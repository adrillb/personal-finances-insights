"""Flask dashboard alternative to the Streamlit interface."""

from __future__ import annotations

from datetime import date, datetime
from functools import lru_cache
import logging
import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request
import pandas as pd

from src.cloud_connector import download_sheet_as_xlsx
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


def _load_workbook_data(data_mode: str) -> tuple[WorkbookData, str, str | None]:
    workbook_path = str(resolve_workbook_path())
    loaded_from = "Local workbook"
    cloud_error: str | None = None
    data: WorkbookData | None = None

    if data_mode != LOCAL_SOURCE and _cloud_is_configured():
        try:
            cloud_bytes = download_sheet_as_xlsx(os.getenv("SPREADSHEET_NAME"))
            data = _load_cached_cloud_data(cloud_bytes)
            loaded_from = "Google Sheet (cloud export)"
            LOGGER.info("Cloud workbook loaded successfully.")
        except Exception as exc:
            cloud_error = str(exc)
            LOGGER.warning("Cloud workbook load failed, local fallback active.", exc_info=True)
    elif data_mode != LOCAL_SOURCE:
        LOGGER.info("Cloud mode requested but not configured. Using local workbook.")

    if data is None:
        data = _load_cached_local_data(workbook_path)
        LOGGER.info("Local workbook loaded successfully.")
    return data, loaded_from, cloud_error


def _build_dashboard_payload(
    data: WorkbookData,
    start_month: pd.Timestamp,
    end_month: pd.Timestamp,
    selected_year: str,
    selected_categories: list[str],
    search_query: str,
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
    top_spend = top_categories(expenses_by_category, top_n=7, start_month=start_month, end_month=end_month)
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
        "transactions": {
            "count": int(len(transactions)),
            "rows": _serialize_records(
                transactions[["date", "category", "amount", "description"]],
                date_columns=("date",),
            ),
        },
    }


@app.get("/")
def index() -> str:
    return render_template("dashboard.html")


@app.get("/api/data")
def api_data() -> Any:
    data_mode = _normalize_data_mode(request.args.get("source"))
    data, loaded_from, cloud_error = _load_workbook_data(data_mode)

    month_min, month_max = _month_bounds(data.general_summary["month"])
    categories = sorted(data.expenses_by_category["category"].dropna().unique().tolist())
    years = ["All"] + sorted({str(value.year) for value in pd.date_range(month_min, month_max, freq="MS")})

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
        }
    )


@app.get("/api/dashboard")
def api_dashboard() -> Any:
    data_mode = _normalize_data_mode(request.args.get("source"))
    selected_year = request.args.get("year", "All")
    search_query = request.args.get("search", "").strip()
    requested_categories = _parse_categories_arg()

    data, loaded_from, cloud_error = _load_workbook_data(data_mode)

    month_min, month_max = _month_bounds(data.general_summary["month"])
    start_month = _parse_month_arg(request.args.get("start_month"), month_min)
    end_month = _parse_month_arg(request.args.get("end_month"), month_max)
    if start_month > end_month:
        start_month, end_month = end_month, start_month

    if selected_year != "All":
        try:
            year = int(selected_year)
            start_month = max(start_month, pd.Timestamp(year=year, month=1, day=1))
            end_month = min(end_month, pd.Timestamp(year=year, month=12, day=1))
        except ValueError:
            LOGGER.warning("Invalid year filter: %s", selected_year)
            selected_year = "All"

    all_categories = sorted(data.expenses_by_category["category"].dropna().unique().tolist())
    if requested_categories is None:
        selected_categories = all_categories
    else:
        selected_categories = [category for category in requested_categories if category in set(all_categories)]

    payload = _build_dashboard_payload(
        data=data,
        start_month=start_month,
        end_month=end_month,
        selected_year=selected_year,
        selected_categories=selected_categories,
        search_query=search_query,
    )
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
    }
    return jsonify(payload)


@app.post("/api/refresh")
def api_refresh() -> Any:
    _load_cached_local_data.cache_clear()
    _load_cached_cloud_data.cache_clear()
    data_mode = _normalize_data_mode(request.args.get("source"))
    data, loaded_from, cloud_error = _load_workbook_data(data_mode)
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
        }
    )


@app.post("/api/sync-monefy")
def api_sync_monefy() -> Any:
    try:
        summary = run_sync(
            folder=os.getenv("MONEFY_FOLDER"),
            spreadsheet_name=os.getenv("SPREADSHEET_NAME"),
        )
        _load_cached_local_data.cache_clear()
        _load_cached_cloud_data.cache_clear()
        return jsonify(
            {
                "message": (
                    "Monefy sync completed: "
                    f"{summary['processed_files']} file(s), "
                    f"{summary['imported_rows']} row(s), "
                    f"{summary['skipped_files']} skipped."
                ),
                "summary": summary,
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
