"""Flask dashboard alternative to the Streamlit interface."""

from __future__ import annotations

import hashlib
from datetime import date, datetime
from functools import lru_cache
import logging
import os
from pathlib import Path
from time import perf_counter
from typing import Any

from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request
import pandas as pd

from src.cloud_connector import clear_cloud_export_cache, download_sheet_as_xlsx_cached
from src.data_loader import WorkbookData, load_all_data, resolve_workbook_path
from src.insights import (
    average_daily_spending,
    budget_adherence,
    compute_kpis,
    cumulative_savings_investments,
    daily_spending,
    income_breakdown,
    monthly_category_spending,
    monthly_income_trend,
    monthly_totals,
    rolling_spending,
    spending_breakdown,
    top_categories,
)
from src.logging_config import setup_logging
from src.monefy_sync import run_sync


LOG_PATH = setup_logging()
LOGGER = logging.getLogger(__name__)
load_dotenv()
LOGGER.info("Starting Personal Finance Flask app. log_path=%s", LOG_PATH)

AUTO_SOURCE = "auto"
LOCAL_SOURCE = "local"

app = Flask(__name__, template_folder="templates")
_DATA_SNAPSHOTS: dict[str, WorkbookData] = {}


def _elapsed_ms(start_time: float) -> float:
    return round((perf_counter() - start_time) * 1000, 2)


def _project_root() -> Path:
    return Path(__file__).resolve().parent


def _resolve_credentials_path() -> Path:
    raw_path = os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials.json")
    credentials_path = Path(raw_path)
    if credentials_path.is_absolute():
        return credentials_path
    return _project_root() / credentials_path


def _cloud_is_configured() -> bool:
    return bool(os.getenv("SPREADSHEET_NAME", "").strip()) and _resolve_credentials_path().exists()


def _month_bounds(months: pd.Series) -> tuple[pd.Timestamp, pd.Timestamp]:
    month_values = pd.to_datetime(months.dropna().unique())
    if len(month_values) == 0:
        now = pd.Timestamp(datetime.now().replace(day=1))
        return now, now
    return pd.Timestamp(min(month_values)), pd.Timestamp(max(month_values))


def _normalize_data_mode(value: str | None) -> str:
    normalized = (value or AUTO_SOURCE).strip().lower()
    return LOCAL_SOURCE if normalized == LOCAL_SOURCE else AUTO_SOURCE


def _parse_month_arg(value: str | None, fallback: pd.Timestamp) -> pd.Timestamp:
    if not value:
        return fallback
    try:
        parsed = pd.Timestamp(value)
    except Exception:
        LOGGER.warning("Invalid month argument received: %s", value)
        return fallback
    return pd.Timestamp(parsed.year, parsed.month, 1)


def _parse_categories_arg() -> list[str] | None:
    if "categories" not in request.args:
        return None
    values = request.args.getlist("categories")
    categories: list[str] = []
    for value in values:
        for item in value.split(","):
            cleaned = item.strip()
            if cleaned:
                categories.append(cleaned)
    return categories


def _local_data_signature(workbook_path: str) -> str:
    path = Path(workbook_path)
    if not path.exists():
        return f"local:{workbook_path}:missing"
    stat = path.stat()
    return f"local:{path.resolve()}:{int(stat.st_mtime_ns)}:{stat.st_size}"


def _cloud_data_signature(cloud_bytes: bytes) -> str:
    digest = hashlib.blake2b(cloud_bytes, digest_size=12).hexdigest()
    return f"cloud:{digest}:{len(cloud_bytes)}"


def _register_data_snapshot(signature: str, data: WorkbookData) -> None:
    _DATA_SNAPSHOTS[signature] = data
    if len(_DATA_SNAPSHOTS) > 12:
        oldest_key = next(iter(_DATA_SNAPSHOTS))
        _DATA_SNAPSHOTS.pop(oldest_key, None)


def _clear_runtime_caches() -> None:
    _load_cached_local_data.cache_clear()
    _load_cached_cloud_data.cache_clear()
    _build_cached_dashboard_core_payload.cache_clear()
    _build_cached_transactions_payload.cache_clear()
    _DATA_SNAPSHOTS.clear()
    clear_cloud_export_cache()


def _serialize_records(
    frame: pd.DataFrame,
    month_columns: tuple[str, ...] = (),
    date_columns: tuple[str, ...] = (),
) -> list[dict[str, Any]]:
    scoped = frame.copy()
    for column in month_columns:
        if column in scoped.columns:
            scoped[column] = pd.to_datetime(scoped[column], errors="coerce").dt.strftime("%Y-%m")
    for column in date_columns:
        if column in scoped.columns:
            scoped[column] = pd.to_datetime(scoped[column], errors="coerce").dt.strftime("%Y-%m-%d")
    scoped = scoped.where(pd.notna(scoped), None)
    return scoped.to_dict(orient="records")


@lru_cache(maxsize=4)
def _load_cached_local_data(workbook_path: str) -> WorkbookData:
    LOGGER.debug("Loading local workbook. workbook_path=%s", workbook_path)
    return load_all_data(workbook_path)


@lru_cache(maxsize=2)
def _load_cached_cloud_data(workbook_bytes: bytes) -> WorkbookData:
    LOGGER.debug("Loading cloud workbook bytes. size=%s", len(workbook_bytes))
    return load_all_data(workbook_bytes)


def _load_workbook_data(
    data_mode: str,
    *,
    force_cloud_refresh: bool = False,
) -> tuple[WorkbookData, str, str | None, str, dict[str, float]]:
    started_at = perf_counter()
    timings: dict[str, float] = {}
    workbook_path = str(resolve_workbook_path())
    loaded_from = "Local workbook"
    cloud_error: str | None = None
    data_signature = _local_data_signature(workbook_path)
    data: WorkbookData | None = None

    if data_mode != LOCAL_SOURCE and _cloud_is_configured():
        try:
            cloud_download_start = perf_counter()
            cloud_bytes = download_sheet_as_xlsx_cached(
                os.getenv("SPREADSHEET_NAME"),
                force_refresh=force_cloud_refresh,
            )
            timings["cloud_download_ms"] = _elapsed_ms(cloud_download_start)
            cloud_parse_start = perf_counter()
            data = _load_cached_cloud_data(cloud_bytes)
            timings["cloud_parse_ms"] = _elapsed_ms(cloud_parse_start)
            data_signature = _cloud_data_signature(cloud_bytes)
            loaded_from = "Google Sheet (cloud export)"
            LOGGER.info("Cloud workbook loaded successfully.")
        except Exception as exc:
            cloud_error = str(exc)
            LOGGER.warning("Cloud workbook load failed, local fallback active.", exc_info=True)
    elif data_mode != LOCAL_SOURCE:
        LOGGER.info("Cloud mode requested but not configured. Using local workbook.")

    if data is None:
        local_parse_start = perf_counter()
        data = _load_cached_local_data(workbook_path)
        timings["local_parse_ms"] = _elapsed_ms(local_parse_start)
        data_signature = _local_data_signature(workbook_path)
        LOGGER.info("Local workbook loaded successfully.")

    _register_data_snapshot(data_signature, data)
    timings["workbook_load_total_ms"] = _elapsed_ms(started_at)
    return data, loaded_from, cloud_error, data_signature, timings


def _resolve_filters(
    data: WorkbookData,
    start_month: pd.Timestamp | None,
    end_month: pd.Timestamp | None,
    selected_year: str,
    requested_categories: list[str] | None,
) -> tuple[pd.Timestamp, pd.Timestamp, str, list[str]]:
    month_min, month_max = _month_bounds(data.general_summary["month"])
    resolved_start_month = start_month if start_month is not None else month_min
    resolved_end_month = end_month if end_month is not None else month_max
    if resolved_start_month > resolved_end_month:
        resolved_start_month, resolved_end_month = resolved_end_month, resolved_start_month

    resolved_year = selected_year
    if resolved_year != "All":
        try:
            year = int(resolved_year)
            resolved_start_month = max(resolved_start_month, pd.Timestamp(year=year, month=1, day=1))
            resolved_end_month = min(resolved_end_month, pd.Timestamp(year=year, month=12, day=1))
        except ValueError:
            LOGGER.warning("Invalid year filter: %s", resolved_year)
            resolved_year = "All"

    all_categories = sorted(data.expenses_by_category["category"].dropna().unique().tolist())
    if requested_categories:
        categories_set = set(all_categories)
        resolved_categories = [category for category in requested_categories if category in categories_set]
    else:
        resolved_categories = all_categories
    return resolved_start_month, resolved_end_month, resolved_year, resolved_categories


def _build_dashboard_core_payload(
    data: WorkbookData,
    start_month: pd.Timestamp,
    end_month: pd.Timestamp,
    selected_year: str,
    selected_categories: list[str],
) -> dict[str, Any]:
    general_summary = data.general_summary
    expenses_by_category = data.expenses_by_category
    raw_transactions = data.raw_transactions
    budget = data.budget
    income_by_source = data.income_by_source
    category_averages = data.category_averages

    start_date = start_month
    end_date = (end_month + pd.offsets.MonthEnd(1)).normalize()

    monthly_overview = monthly_totals(general_summary)
    monthly_overview = monthly_overview[
        (monthly_overview["month"] >= start_month) & (monthly_overview["month"] <= end_month)
    ]
    # Keep only months with income/expense activity for a readable overview chart.
    monthly_overview = monthly_overview[
        (monthly_overview["income"] != 0)
        | (monthly_overview["expenses"] != 0)
    ]
    kpis = compute_kpis(general_summary, start_month, end_month)

    category_breakdown = spending_breakdown(
        expenses_by_category,
        start_month=start_month,
        end_month=end_month,
        categories=selected_categories,
    )
    monthly_by_category = monthly_category_spending(
        expenses_by_category,
        start_month=start_month,
        end_month=end_month,
        categories=selected_categories,
    )
    top_spend = top_categories(
        expenses_by_category,
        top_n=7,
        start_month=start_month,
        end_month=end_month,
        breakdown=category_breakdown,
    )
    rolling_base = expenses_by_category[expenses_by_category["category"].isin(selected_categories)]
    rolling = rolling_spending(rolling_base)
    rolling = rolling[(rolling["month"] >= start_month) & (rolling["month"] <= end_month)]

    daily = daily_spending(
        raw_transactions,
        start_date=start_date,
        end_date=end_date,
        categories=selected_categories,
    )
    avg_daily = average_daily_spending(
        raw_transactions,
        start_date=start_date,
        end_date=end_date,
        categories=selected_categories,
        daily_totals=daily,
    )

    budget_table = budget_adherence(budget)
    if selected_categories:
        budget_table = budget_table[
            budget_table["category"].isin(selected_categories) | budget_table["is_total"]
        ]

    income_split = income_breakdown(income_by_source, start_month=start_month, end_month=end_month)
    income_trend = monthly_income_trend(income_by_source, start_month=start_month, end_month=end_month)
    savings_curve = cumulative_savings_investments(
        general_summary, start_month=start_month, end_month=end_month
    )

    avg_view = category_averages.copy()
    if selected_year != "All" and not avg_view.empty:
        avg_view = avg_view[avg_view["year"] == int(selected_year)]

    return {
        "kpis": {
            "total_income": kpis.total_income,
            "total_expenses": kpis.total_expenses,
            "total_investments": kpis.total_investments,
            "total_savings": kpis.total_savings,
            "net_cash_flow": kpis.net_cash_flow,
            "savings_rate": kpis.savings_rate,
            "average_daily_spending": avg_daily,
        },
        "overview": {
            "monthly": _serialize_records(monthly_overview, month_columns=("month",)),
        },
        "spending": {
            "breakdown": _serialize_records(category_breakdown),
            "monthly_by_category": _serialize_records(monthly_by_category, month_columns=("month",)),
            "top_categories": _serialize_records(top_spend),
            "rolling": _serialize_records(rolling, month_columns=("month",)),
            "daily": _serialize_records(daily, date_columns=("date",)),
        },
        "budget": {
            "table": _serialize_records(budget_table, month_columns=("month",)),
        },
        "income": {
            "split": _serialize_records(income_split),
            "trend": _serialize_records(income_trend, month_columns=("month",)),
        },
        "savings": {
            "curve": _serialize_records(savings_curve, month_columns=("month",)),
            "category_averages": _serialize_records(avg_view),
        },
    }


def _build_transactions_payload(
    raw_transactions: pd.DataFrame,
    start_month: pd.Timestamp,
    end_month: pd.Timestamp,
    selected_categories: list[str],
    search_query: str,
) -> dict[str, Any]:
    start_date = start_month
    end_date = (end_month + pd.offsets.MonthEnd(1)).normalize()
    transactions = raw_transactions.copy()
    transactions = transactions[
        (transactions["date"] >= start_date)
        & (transactions["date"] <= end_date)
        & (transactions["category"].isin(selected_categories))
    ]
    if search_query:
        query = search_query.lower()
        transactions = transactions[
            transactions["description"].str.lower().str.contains(query, na=False)
            | transactions["category"].str.lower().str.contains(query, na=False)
        ]
    transactions = transactions.sort_values("date", ascending=False)
    return {
        "count": int(len(transactions)),
        "rows": _serialize_records(
            transactions[["date", "category", "amount", "description"]],
            date_columns=("date",),
        ),
    }


@lru_cache(maxsize=128)
def _build_cached_dashboard_core_payload(
    data_signature: str,
    start_month_key: str,
    end_month_key: str,
    selected_year: str,
    categories_key: tuple[str, ...],
) -> dict[str, Any]:
    data = _DATA_SNAPSHOTS.get(data_signature)
    if data is None:
        raise KeyError(f"Data snapshot not found for signature: {data_signature}")
    return _build_dashboard_core_payload(
        data=data,
        start_month=pd.Timestamp(start_month_key),
        end_month=pd.Timestamp(end_month_key),
        selected_year=selected_year,
        selected_categories=list(categories_key),
    )


@lru_cache(maxsize=512)
def _build_cached_transactions_payload(
    data_signature: str,
    start_month_key: str,
    end_month_key: str,
    categories_key: tuple[str, ...],
    search_query: str,
) -> dict[str, Any]:
    data = _DATA_SNAPSHOTS.get(data_signature)
    if data is None:
        raise KeyError(f"Data snapshot not found for signature: {data_signature}")
    return _build_transactions_payload(
        raw_transactions=data.raw_transactions,
        start_month=pd.Timestamp(start_month_key),
        end_month=pd.Timestamp(end_month_key),
        selected_categories=list(categories_key),
        search_query=search_query,
    )


@app.get("/")
def index() -> str:
    return render_template("dashboard.html")


@app.get("/api/data")
def api_data() -> Any:
    request_started = perf_counter()
    data_mode = _normalize_data_mode(request.args.get("source"))
    data, loaded_from, cloud_error, _, load_timings = _load_workbook_data(data_mode)

    month_min, month_max = _month_bounds(data.general_summary["month"])
    categories = sorted(data.expenses_by_category["category"].dropna().unique().tolist())
    years = ["All"] + sorted({str(value.year) for value in pd.date_range(month_min, month_max, freq="MS")})
    timings = {
        **load_timings,
        "endpoint_total_ms": _elapsed_ms(request_started),
    }

    return jsonify(
        {
            "source": {
                "mode": data_mode,
                "loaded_from": loaded_from,
                "cloud_error": cloud_error,
            },
            "filters": {
                "month_min": month_min.strftime("%Y-%m"),
                "month_max": month_max.strftime("%Y-%m"),
                "years": years,
                "categories": categories,
            },
            "timings": timings,
        }
    )


@app.get("/api/dashboard")
def api_dashboard() -> Any:
    request_started = perf_counter()
    data_mode = _normalize_data_mode(request.args.get("source"))
    selected_year = request.args.get("year", "All")
    include_transactions = request.args.get("include_transactions", "false").strip().lower() == "true"
    search_query = request.args.get("search", "").strip() if include_transactions else ""
    requested_categories = _parse_categories_arg()

    data, loaded_from, cloud_error, data_signature, load_timings = _load_workbook_data(data_mode)

    month_min, month_max = _month_bounds(data.general_summary["month"])
    requested_start_month = _parse_month_arg(request.args.get("start_month"), month_min)
    requested_end_month = _parse_month_arg(request.args.get("end_month"), month_max)
    start_month, end_month, selected_year, selected_categories = _resolve_filters(
        data=data,
        start_month=requested_start_month,
        end_month=requested_end_month,
        selected_year=selected_year,
        requested_categories=requested_categories,
    )
    start_month_key = start_month.strftime("%Y-%m-01")
    end_month_key = end_month.strftime("%Y-%m-01")
    categories_key = tuple(sorted(set(selected_categories)))

    compute_started = perf_counter()
    payload = dict(
        _build_cached_dashboard_core_payload(
        data_signature=data_signature,
        start_month_key=start_month_key,
        end_month_key=end_month_key,
        selected_year=selected_year,
        categories_key=categories_key,
        )
    )
    compute_timings = {"dashboard_compute_ms": _elapsed_ms(compute_started)}

    if include_transactions:
        transactions_started = perf_counter()
        payload["transactions"] = _build_cached_transactions_payload(
            data_signature=data_signature,
            start_month_key=start_month_key,
            end_month_key=end_month_key,
            categories_key=categories_key,
            search_query=search_query.lower(),
        )
        compute_timings["transactions_compute_ms"] = _elapsed_ms(transactions_started)

    payload["meta"] = {
        "source": {
            "mode": data_mode,
            "loaded_from": loaded_from,
            "cloud_error": cloud_error,
        },
        "filters": {
            "start_month": start_month.strftime("%Y-%m"),
            "end_month": end_month.strftime("%Y-%m"),
            "year": selected_year,
            "categories": selected_categories,
            "search": search_query,
        },
        "timings": {
            **load_timings,
            **compute_timings,
            "endpoint_total_ms": _elapsed_ms(request_started),
        },
    }
    return jsonify(payload)


@app.get("/api/transactions")
def api_transactions() -> Any:
    request_started = perf_counter()
    data_mode = _normalize_data_mode(request.args.get("source"))
    selected_year = request.args.get("year", "All")
    search_query = request.args.get("search", "").strip()
    requested_categories = _parse_categories_arg()

    data, loaded_from, cloud_error, data_signature, load_timings = _load_workbook_data(data_mode)
    month_min, month_max = _month_bounds(data.general_summary["month"])
    requested_start_month = _parse_month_arg(request.args.get("start_month"), month_min)
    requested_end_month = _parse_month_arg(request.args.get("end_month"), month_max)
    start_month, end_month, selected_year, selected_categories = _resolve_filters(
        data=data,
        start_month=requested_start_month,
        end_month=requested_end_month,
        selected_year=selected_year,
        requested_categories=requested_categories,
    )
    start_month_key = start_month.strftime("%Y-%m-01")
    end_month_key = end_month.strftime("%Y-%m-01")
    categories_key = tuple(sorted(set(selected_categories)))

    compute_started = perf_counter()
    transactions = _build_cached_transactions_payload(
        data_signature=data_signature,
        start_month_key=start_month_key,
        end_month_key=end_month_key,
        categories_key=categories_key,
        search_query=search_query.lower(),
    )
    timings = {
        **load_timings,
        "transactions_compute_ms": _elapsed_ms(compute_started),
        "endpoint_total_ms": _elapsed_ms(request_started),
    }
    return jsonify(
        {
            "transactions": transactions,
            "meta": {
                "source": {
                    "mode": data_mode,
                    "loaded_from": loaded_from,
                    "cloud_error": cloud_error,
                },
                "filters": {
                    "start_month": start_month.strftime("%Y-%m"),
                    "end_month": end_month.strftime("%Y-%m"),
                    "year": selected_year,
                    "categories": selected_categories,
                    "search": search_query,
                },
                "timings": timings,
            },
        }
    )


@app.post("/api/refresh")
def api_refresh() -> Any:
    request_started = perf_counter()
    _clear_runtime_caches()
    data_mode = _normalize_data_mode(request.args.get("source"))
    data, loaded_from, cloud_error, _, load_timings = _load_workbook_data(
        data_mode,
        force_cloud_refresh=True,
    )
    month_min, month_max = _month_bounds(data.general_summary["month"])
    return jsonify(
        {
            "message": "Cache cleared and data reloaded.",
            "source": {
                "mode": data_mode,
                "loaded_from": loaded_from,
                "cloud_error": cloud_error,
            },
            "month_range": {
                "month_min": month_min.strftime("%Y-%m"),
                "month_max": month_max.strftime("%Y-%m"),
            },
            "timings": {
                **load_timings,
                "endpoint_total_ms": _elapsed_ms(request_started),
            },
        }
    )


@app.post("/api/sync-monefy")
def api_sync_monefy() -> Any:
    try:
        request_started = perf_counter()
        summary = run_sync(
            folder=os.getenv("MONEFY_FOLDER"),
            spreadsheet_name=os.getenv("SPREADSHEET_NAME"),
        )
        _clear_runtime_caches()
        return jsonify(
            {
                "message": (
                    "Monefy sync completed: "
                    f"recreated {summary['sheet_name']} from "
                    f"{summary['source_file']} with "
                    f"{summary['imported_rows']} row(s)."
                ),
                "summary": summary,
                "timings": {
                    "endpoint_total_ms": _elapsed_ms(request_started),
                },
            }
        )
    except Exception as exc:
        LOGGER.error("Monefy sync failed.", exc_info=True)
        return jsonify({"message": f"Monefy sync failed: {exc}"}), 500


def run() -> None:
    host = os.getenv("FLASK_HOST", "0.0.0.0")
    port = int(os.getenv("FLASK_PORT", "8080"))
    LOGGER.info("Running Flask app on host=%s port=%s", host, port)
    app.run(host=host, port=port, debug=False)


if __name__ == "__main__":
    run()
