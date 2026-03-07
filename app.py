"""Streamlit dashboard for personal finance insights."""

from __future__ import annotations

from datetime import datetime
import logging
import os
from pathlib import Path
from time import perf_counter

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv

from src.data_loader import load_all_data, resolve_workbook_path
from src.cloud_connector import clear_cloud_export_cache, download_sheet_as_xlsx_cached
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
LOGGER.info("Starting Personal Finance Insights app.")

load_dotenv()
LOGGER.debug("Environment loaded. log_path=%s", LOG_PATH)


def _elapsed_ms(start_time: float) -> float:
    return round((perf_counter() - start_time) * 1000, 2)


def _format_currency(value: float) -> str:
    return f"EUR {value:,.2f}"


def _month_bounds(months: pd.Series) -> tuple[pd.Timestamp, pd.Timestamp]:
    month_values = pd.to_datetime(months.dropna().unique())
    if len(month_values) == 0:
        now = pd.Timestamp(datetime.now().replace(day=1))
        return now, now
    return pd.Timestamp(min(month_values)), pd.Timestamp(max(month_values))


def _project_root() -> Path:
    return Path(__file__).resolve().parent


def _resolve_credentials_path() -> Path:
    raw_path = os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials.json")
    credentials_path = Path(raw_path)
    if credentials_path.is_absolute():
        return credentials_path
    return _project_root() / credentials_path


def _cloud_is_configured() -> bool:
    configured = bool(os.getenv("SPREADSHEET_NAME", "").strip()) and _resolve_credentials_path().exists()
    if not configured:
        LOGGER.debug("Cloud configuration missing spreadsheet name or credentials.")
    return configured


@st.cache_data(show_spinner=False)
def _load_cached_local_data(workbook_path: str):
    LOGGER.debug("Loading local workbook. workbook_path=%s", workbook_path)
    return load_all_data(workbook_path)


@st.cache_data(show_spinner=False)
def _load_cached_cloud_data(workbook_bytes: bytes):
    LOGGER.debug("Loading cloud workbook bytes. size=%s", len(workbook_bytes))
    return load_all_data(workbook_bytes)


st.set_page_config(page_title="Personal Finance Insights", layout="wide")
st.title("Personal Finance Insights")
script_started = perf_counter()
timings: dict[str, float] = {}

st.sidebar.header("Data Source")
data_mode = st.sidebar.selectbox(
    "Workbook source",
    options=["Auto (Cloud + Local fallback)", "Local only"],
    index=0,
)
LOGGER.debug("Selected workbook source mode: %s", data_mode)

if st.sidebar.button("Refresh from Cloud"):
    LOGGER.info("User clicked Refresh from Cloud.")
    clear_cloud_export_cache()
    st.cache_data.clear()
    st.session_state["cloud_message"] = "Cloud cache cleared. Data refreshed."
    st.rerun()

if st.sidebar.button("Sync Monefy"):
    LOGGER.info("User clicked Sync Monefy.")
    try:
        sync_summary = run_sync(
            folder=os.getenv("MONEFY_FOLDER"),
            spreadsheet_name=os.getenv("SPREADSHEET_NAME"),
        )
        clear_cloud_export_cache()
        st.cache_data.clear()
        st.session_state["cloud_message"] = (
            "Monefy sync completed: "
            f"recreated {sync_summary['sheet_name']} from "
            f"{sync_summary['source_file']} with "
            f"{sync_summary['imported_rows']} row(s)."
        )
        LOGGER.info(
            "Monefy sync done. sheet=%s source_file=%s imported_rows=%s",
            sync_summary["sheet_name"],
            sync_summary["source_file"],
            sync_summary["imported_rows"],
        )
        st.rerun()
    except Exception as exc:
        LOGGER.error("Monefy sync failed.", exc_info=True)
        st.sidebar.error(f"Monefy sync failed: {exc}")

if "cloud_message" in st.session_state:
    st.sidebar.success(st.session_state.pop("cloud_message"))

workbook_path = str(resolve_workbook_path())
loaded_from = "Local workbook"
cloud_error: str | None = None
LOGGER.debug("Resolved local workbook path: %s", workbook_path)

data = None
data_load_started = perf_counter()
if data_mode != "Local only" and _cloud_is_configured():
    LOGGER.info("Loading workbook from cloud export.")
    try:
        cloud_bytes = download_sheet_as_xlsx_cached(os.getenv("SPREADSHEET_NAME"))
        data = _load_cached_cloud_data(cloud_bytes)
        loaded_from = "Google Sheet (cloud export)"
        LOGGER.info("Cloud workbook loaded.")
    except Exception as exc:
        cloud_error = str(exc)
        LOGGER.warning("Cloud load failed; using local workbook. reason=%s", exc)
elif data_mode != "Local only":
    LOGGER.warning("Cloud mode selected but cloud is not configured. Using local workbook.")

if data is None:
    LOGGER.info("Loading local workbook.")
    try:
        data = _load_cached_local_data(workbook_path)
        LOGGER.info("Local workbook loaded.")
    except Exception:
        LOGGER.error("Local workbook load failed.", exc_info=True)
        raise
timings["data_load_ms"] = _elapsed_ms(data_load_started)

st.sidebar.caption(f"Loaded from: {loaded_from}")
if cloud_error:
    LOGGER.debug("Cloud fallback details: %s", cloud_error)
    st.sidebar.warning(f"Cloud fallback used local file: {cloud_error}")

general_summary = data.general_summary
expenses_by_category = data.expenses_by_category
raw_transactions = data.raw_transactions
budget = data.budget
income_by_source = data.income_by_source
category_averages = data.category_averages
LOGGER.debug(
    (
        "Dataset sizes loaded. general_summary=%sx%s expenses_by_category=%sx%s "
        "raw_transactions=%sx%s budget=%sx%s income_by_source=%sx%s category_averages=%sx%s"
    ),
    len(general_summary),
    len(general_summary.columns),
    len(expenses_by_category),
    len(expenses_by_category.columns),
    len(raw_transactions),
    len(raw_transactions.columns),
    len(budget),
    len(budget.columns),
    len(income_by_source),
    len(income_by_source.columns),
    len(category_averages),
    len(category_averages.columns),
)

month_min, month_max = _month_bounds(general_summary["month"])

st.sidebar.header("Filters")
date_range = st.sidebar.slider(
    "Month range",
    min_value=month_min.to_pydatetime(),
    max_value=month_max.to_pydatetime(),
    value=(month_min.to_pydatetime(), month_max.to_pydatetime()),
    format="YYYY-MM",
)

year_options = ["All"] + sorted({str(m.year) for m in pd.date_range(month_min, month_max, freq="MS")})
selected_year = st.sidebar.selectbox("Year", options=year_options, index=0)

all_categories = sorted(expenses_by_category["category"].dropna().unique().tolist())
selected_categories = st.sidebar.multiselect(
    "Categories",
    options=all_categories,
    default=all_categories,
)

start_month = pd.Timestamp(date_range[0]).replace(day=1)
end_month = pd.Timestamp(date_range[1]).replace(day=1)
if selected_year != "All":
    year = int(selected_year)
    start_month = max(start_month, pd.Timestamp(year=year, month=1, day=1))
    end_month = min(end_month, pd.Timestamp(year=year, month=12, day=1))

start_date = start_month
end_date = (end_month + pd.offsets.MonthEnd(1)).normalize()

aggregation_started = perf_counter()
monthly_overview = monthly_totals(general_summary)
monthly_overview = monthly_overview[
    (monthly_overview["month"] >= start_month) & (monthly_overview["month"] <= end_month)
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
rolling = rolling_spending(
    expenses_by_category[
        expenses_by_category["category"].isin(selected_categories)
    ]
)
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

transactions = raw_transactions.copy()
transactions = transactions[
    (transactions["date"] >= start_date)
    & (transactions["date"] <= end_date)
    & (transactions["category"].isin(selected_categories))
]
timings["aggregation_ms"] = _elapsed_ms(aggregation_started)

tabs = st.tabs(
    [
        "Overview",
        "Spending Analysis",
        "Budget vs Actual",
        "Income",
        "Savings & Investments",
        "Transaction Explorer",
    ]
)

with tabs[0]:
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Income", _format_currency(kpis.total_income))
    col2.metric("Total Expenses", _format_currency(kpis.total_expenses))
    col3.metric("Net Cash Flow", _format_currency(kpis.net_cash_flow))
    col4.metric("Savings Rate", f"{kpis.savings_rate * 100:.1f}%")

    fig_overview = go.Figure()
    fig_overview.add_trace(
        go.Scatter(
            x=monthly_overview["month"],
            y=monthly_overview["income"],
            mode="lines+markers",
            name="Income",
            line=dict(color="green"),
            marker=dict(color="green"),
        )
    )
    fig_overview.add_trace(
        go.Scatter(
            x=monthly_overview["month"],
            y=monthly_overview["expenses"],
            mode="lines+markers",
            name="Expenses",
            line=dict(color="red"),
            marker=dict(color="red"),
        )
    )
    fig_overview.add_trace(
        go.Scatter(
            x=monthly_overview["month"],
            y=monthly_overview["savings"]+monthly_overview["investments"],
            mode="lines+markers",
            name="Investments + Savings",
            line=dict(color="blue"),
            marker=dict(color="blue"),
        )
    )
    fig_overview.update_layout(title="Monthly Income vs Expenses", xaxis_title="Month", yaxis_title="Amount (EUR)")
    st.plotly_chart(fig_overview, width="stretch")

    col_a, col_b = st.columns(2)
    with col_a:
        st.metric("Average Daily Spending", _format_currency(avg_daily))
    with col_b:
        st.metric("Total Investments", _format_currency(kpis.total_investments))

with tabs[1]:
    col1, col2 = st.columns(2)
    with col1:
        fig_pie = px.pie(
            category_breakdown,
            names="category",
            values="amount",
            title="Spending Breakdown by Category",
        )
        st.plotly_chart(fig_pie, width="stretch")
    with col2:
        fig_top = px.bar(
            top_spend,
            x="amount",
            y="category",
            orientation="h",
            title="Top Spending Categories",
        )
        fig_top.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig_top, width="stretch")

    fig_stacked = px.bar(
        monthly_by_category,
        x="month",
        y="amount",
        color="category",
        title="Monthly Spending by Category",
    )
    st.plotly_chart(fig_stacked, width="stretch")

    fig_roll = go.Figure()
    fig_roll.add_trace(
        go.Scatter(x=rolling["month"], y=rolling["amount"], mode="lines+markers", name="Monthly spend")
    )
    fig_roll.add_trace(
        go.Scatter(x=rolling["month"], y=rolling["rolling_avg"], mode="lines", name="3-month average")
    )
    fig_roll.update_layout(title="Spending Trend and Rolling Average", xaxis_title="Month", yaxis_title="Amount")
    st.plotly_chart(fig_roll, width="stretch")

    if not daily.empty:
        heatmap_df = daily.copy()
        heatmap_df["day_of_month"] = heatmap_df["date"].dt.day
        heatmap_df["month_label"] = heatmap_df["date"].dt.strftime("%Y-%m")
        fig_heatmap = px.density_heatmap(
            heatmap_df,
            x="day_of_month",
            y="month_label",
            z="amount",
            histfunc="sum",
            title="Daily Spending Heatmap",
            color_continuous_scale="Blues",
        )
        st.plotly_chart(fig_heatmap, width="stretch")
    else:
        st.info("No daily transactions available for the selected filters.")

with tabs[2]:
    budget_no_total = budget_table[~budget_table["is_total"]].copy()
    fig_budget = px.bar(
        budget_no_total,
        x="category",
        y=["projected", "actual"],
        barmode="group",
        title="Projected vs Actual by Category",
    )
    st.plotly_chart(fig_budget, width="stretch")

    fig_delta = px.bar(
        budget_no_total,
        x="category",
        y="delta",
        color=budget_no_total["delta"].apply(lambda x: "Over budget" if x > 0 else "Under budget"),
        title="Budget Difference (Actual - Projected)",
    )
    st.plotly_chart(fig_delta, width="stretch")

    st.dataframe(
        budget_table[["category", "projected", "actual", "difference", "delta", "pct_used", "is_total"]],
        width="stretch",
    )

with tabs[3]:
    col1, col2 = st.columns(2)
    with col1:
        fig_income_pie = px.pie(
            income_split,
            names="source",
            values="amount",
            title="Income by Source",
        )
        st.plotly_chart(fig_income_pie, width="stretch")
    with col2:
        fig_income_line = px.line(
            income_trend,
            x="month",
            y="amount",
            markers=True,
            title="Monthly Income Trend",
        )
        st.plotly_chart(fig_income_line, width="stretch")

    st.dataframe(income_split, width="stretch")

with tabs[4]:
    col1, col2 = st.columns(2)
    with col1:
        fig_si = go.Figure()
        fig_si.add_trace(go.Bar(x=savings_curve["month"], y=savings_curve["savings"], name="Savings"))
        fig_si.add_trace(
            go.Bar(x=savings_curve["month"], y=savings_curve["investments"], name="Investments")
        )
        fig_si.update_layout(barmode="group", title="Monthly Savings and Investments")
        st.plotly_chart(fig_si, width="stretch")
    with col2:
        fig_cum = go.Figure()
        fig_cum.add_trace(
            go.Scatter(
                x=savings_curve["month"],
                y=savings_curve["savings_cum"],
                mode="lines+markers",
                name="Savings cumulative",
            )
        )
        fig_cum.add_trace(
            go.Scatter(
                x=savings_curve["month"],
                y=savings_curve["investments_cum"],
                mode="lines+markers",
                name="Investments cumulative",
            )
        )
        fig_cum.update_layout(title="Cumulative Savings & Investments")
        st.plotly_chart(fig_cum, width="stretch")

    if not category_averages.empty:
        avg_view = category_averages.copy()
        if selected_year != "All":
            avg_view = avg_view[avg_view["year"] == int(selected_year)]
        fig_avg = px.bar(
            avg_view,
            x="category",
            y="average_amount",
            color="year",
            title="Category Averages by Year",
        )
        st.plotly_chart(fig_avg, width="stretch")

with tabs[5]:
    search = st.text_input("Search description")
    view = transactions.copy()
    if search.strip():
        query = search.strip().lower()
        view = view[
            view["description"].str.lower().str.contains(query, na=False)
            | view["category"].str.lower().str.contains(query, na=False)
        ]

    view = view.sort_values("date", ascending=False)
    st.caption(f"{len(view)} transactions")
    st.dataframe(
        view[["date", "category", "amount", "description"]],
        width="stretch",
        hide_index=True,
    )

timings["script_total_ms"] = _elapsed_ms(script_started)
st.sidebar.caption(
    "Timing | load: "
    f"{timings.get('data_load_ms', 0.0):.1f}ms | "
    f"aggregate: {timings.get('aggregation_ms', 0.0):.1f}ms | "
    f"total: {timings.get('script_total_ms', 0.0):.1f}ms"
)
LOGGER.debug(
    "Streamlit timings: data_load_ms=%.2f aggregation_ms=%.2f script_total_ms=%.2f",
    timings.get("data_load_ms", 0.0),
    timings.get("aggregation_ms", 0.0),
    timings.get("script_total_ms", 0.0),
)
