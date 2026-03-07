"""Metrics and aggregation helpers for the finance dashboard."""

from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Iterable

import pandas as pd

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class KPISet:
    total_income: float
    total_expenses: float
    total_investments: float
    total_savings: float
    net_cash_flow: float
    savings_rate: float


def _normalize_month_start(value: object) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    return pd.Timestamp(ts.year, ts.month, 1)


def _apply_month_filter(
    frame: pd.DataFrame,
    start_month: pd.Timestamp | None = None,
    end_month: pd.Timestamp | None = None,
    month_col: str = "month",
) -> pd.DataFrame:
    LOGGER.debug(
        "Applying month filter. rows=%s start_month=%s end_month=%s month_col=%s",
        len(frame),
        start_month,
        end_month,
        month_col,
    )
    if frame.empty:
        LOGGER.warning("Month filter received an empty dataframe.")
        return frame.copy()
    result = frame.copy()
    month_values = pd.to_datetime(result[month_col], errors="coerce")
    result[month_col] = month_values.dt.to_period("M").dt.to_timestamp()
    if start_month is not None:
        result = result[result[month_col] >= _normalize_month_start(start_month)]
    if end_month is not None:
        result = result[result[month_col] <= _normalize_month_start(end_month)]
    if result.empty:
        LOGGER.warning("Month filter returned no rows for selected range.")
    LOGGER.debug("Month filter completed. rows=%s cols=%s", len(result), len(result.columns))
    return result


def monthly_totals(general_summary: pd.DataFrame) -> pd.DataFrame:
    """Return monthly totals with one column per source and net cash flow."""
    LOGGER.debug("monthly_totals started. rows=%s cols=%s", len(general_summary), len(general_summary.columns))
    if general_summary.empty:
        LOGGER.warning("monthly_totals received an empty dataframe.")
        return pd.DataFrame(
            columns=[
                "month",
                "income",
                "expenses",
                "investments",
                "savings",
                "net_cash_flow",
                "savings_rate",
            ]
        )

    summary = general_summary.copy()
    summary["source"] = summary["source"].str.upper()
    pivot = (
        summary.pivot_table(
            index="month",
            columns="source",
            values="amount",
            aggfunc="sum",
            fill_value=0.0,
        )
        .sort_index()
        .reset_index()
    )
    for col in ["INCOME", "EXPENSES", "INVESTMENTS", "SAVINGS"]:
        if col not in pivot.columns:
            pivot[col] = 0.0

    pivot["net_cash_flow"] = (
        pivot["INCOME"] - pivot["EXPENSES"] - pivot["INVESTMENTS"] - pivot["SAVINGS"]
    )
    pivot["savings_rate"] = pivot["SAVINGS"].div(pivot["INCOME"]).replace(
        [pd.NA, pd.NaT, float("inf"), -float("inf")], 0.0
    )
    pivot["savings_rate"] = pivot["savings_rate"].fillna(0.0)

    result = pivot.rename(
        columns={
            "INCOME": "income",
            "EXPENSES": "expenses",
            "INVESTMENTS": "investments",
            "SAVINGS": "savings",
        }
    )
    LOGGER.debug("monthly_totals completed. rows=%s cols=%s", len(result), len(result.columns))
    return result


def compute_kpis(
    general_summary: pd.DataFrame,
    start_month: pd.Timestamp | None = None,
    end_month: pd.Timestamp | None = None,
) -> KPISet:
    """Compute top-line KPI totals for the selected month range."""
    LOGGER.debug(
        "compute_kpis started. rows=%s start_month=%s end_month=%s",
        len(general_summary),
        start_month,
        end_month,
    )
    scoped = _apply_month_filter(general_summary, start_month, end_month)
    if scoped.empty:
        LOGGER.warning("compute_kpis has no data after month filtering.")
    totals = scoped.groupby("source", as_index=False)["amount"].sum()
    data = {row["source"].upper(): float(row["amount"]) for _, row in totals.iterrows()}

    income = data.get("INCOME", 0.0)
    expenses = data.get("EXPENSES", 0.0)
    investments = data.get("INVESTMENTS", 0.0)
    savings = data.get("SAVINGS", 0.0)
    net = income - expenses - investments - savings
    savings_rate = (savings / income) if income else 0.0

    kpis = KPISet(
        total_income=income,
        total_expenses=expenses,
        total_investments=investments,
        total_savings=savings,
        net_cash_flow=net,
        savings_rate=savings_rate,
    )
    LOGGER.debug(
        "KPI summary computed. income=%s expenses=%s investments=%s savings=%s net=%s savings_rate=%s",
        kpis.total_income,
        kpis.total_expenses,
        kpis.total_investments,
        kpis.total_savings,
        kpis.net_cash_flow,
        kpis.savings_rate,
    )
    return kpis


def spending_breakdown(
    expenses_by_category: pd.DataFrame,
    start_month: pd.Timestamp | None = None,
    end_month: pd.Timestamp | None = None,
    categories: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Aggregate expenses by category with percentage contribution."""
    categories_list = list(categories) if categories is not None else None
    LOGGER.debug(
        "spending_breakdown started. rows=%s start_month=%s end_month=%s categories_count=%s",
        len(expenses_by_category),
        start_month,
        end_month,
        len(categories_list) if categories_list else 0,
    )
    scoped = _apply_month_filter(expenses_by_category, start_month, end_month)
    if categories_list:
        scoped = scoped[scoped["category"].isin(categories_list)]
    if scoped.empty:
        LOGGER.warning("spending_breakdown has no data for selected filters.")
        return pd.DataFrame(columns=["category", "amount", "percentage"])

    grouped = scoped.groupby("category", as_index=False)["amount"].sum()
    total = grouped["amount"].sum()
    grouped["percentage"] = grouped["amount"] / total if total else 0.0
    result = grouped.sort_values("amount", ascending=False).reset_index(drop=True)
    LOGGER.debug("spending_breakdown completed. rows=%s cols=%s", len(result), len(result.columns))
    return result


def monthly_category_spending(
    expenses_by_category: pd.DataFrame,
    start_month: pd.Timestamp | None = None,
    end_month: pd.Timestamp | None = None,
    categories: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Return monthly spending by category for stacked charts."""
    categories_list = list(categories) if categories is not None else None
    LOGGER.debug(
        "monthly_category_spending started. rows=%s start_month=%s end_month=%s categories_count=%s",
        len(expenses_by_category),
        start_month,
        end_month,
        len(categories_list) if categories_list else 0,
    )
    scoped = _apply_month_filter(expenses_by_category, start_month, end_month)
    if categories_list:
        scoped = scoped[scoped["category"].isin(categories_list)]
    if scoped.empty:
        LOGGER.warning("monthly_category_spending has no data for selected filters.")
        return pd.DataFrame(columns=["month", "category", "amount"])
    result = (
        scoped.groupby(["month", "category"], as_index=False)["amount"]
        .sum()
        .sort_values(["month", "amount"], ascending=[True, False])
        .reset_index(drop=True)
    )
    LOGGER.debug("monthly_category_spending completed. rows=%s cols=%s", len(result), len(result.columns))
    return result


def top_categories(
    expenses_by_category: pd.DataFrame,
    top_n: int = 5,
    start_month: pd.Timestamp | None = None,
    end_month: pd.Timestamp | None = None,
    breakdown: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Return the highest-spend categories for the selected period."""
    LOGGER.debug(
        "top_categories started. rows=%s top_n=%s start_month=%s end_month=%s",
        len(expenses_by_category),
        top_n,
        start_month,
        end_month,
    )
    resolved_breakdown = breakdown
    if resolved_breakdown is None:
        resolved_breakdown = spending_breakdown(expenses_by_category, start_month, end_month)
    if resolved_breakdown.empty:
        LOGGER.warning("top_categories has no categories to return.")
    result = resolved_breakdown.head(top_n).reset_index(drop=True)
    LOGGER.debug("top_categories completed. rows=%s cols=%s", len(result), len(result.columns))
    return result


def daily_spending(
    raw_transactions: pd.DataFrame,
    start_date: pd.Timestamp | None = None,
    end_date: pd.Timestamp | None = None,
    categories: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Return daily total spending from transaction-level data."""
    categories_list = list(categories) if categories is not None else None
    LOGGER.debug(
        "daily_spending started. rows=%s start_date=%s end_date=%s categories_count=%s",
        len(raw_transactions),
        start_date,
        end_date,
        len(categories_list) if categories_list else 0,
    )
    if raw_transactions.empty:
        LOGGER.warning("daily_spending received an empty transactions dataframe.")
        return pd.DataFrame(columns=["date", "amount"])

    scoped = raw_transactions.copy()
    scoped["date"] = pd.to_datetime(scoped["date"])
    if start_date is not None:
        scoped = scoped[scoped["date"] >= pd.Timestamp(start_date)]
    if end_date is not None:
        scoped = scoped[scoped["date"] <= pd.Timestamp(end_date)]
    if categories_list:
        scoped = scoped[scoped["category"].isin(categories_list)]
    if scoped.empty:
        LOGGER.warning("daily_spending has no data for selected filters.")
        return pd.DataFrame(columns=["date", "amount"])

    scoped["day"] = scoped["date"].dt.normalize()
    result = (
        scoped.groupby("day", as_index=False)["amount"]
        .sum()
        .rename(columns={"day": "date"})
        .sort_values("date")
        .reset_index(drop=True)
    )
    LOGGER.debug("daily_spending completed. rows=%s cols=%s", len(result), len(result.columns))
    return result


def average_daily_spending(
    raw_transactions: pd.DataFrame,
    start_date: pd.Timestamp | None = None,
    end_date: pd.Timestamp | None = None,
    categories: Iterable[str] | None = None,
    daily_totals: pd.DataFrame | None = None,
) -> float:
    """Return average daily spending over days with transactions."""
    categories_list = list(categories) if categories is not None else None
    LOGGER.debug(
        "average_daily_spending started. rows=%s start_date=%s end_date=%s categories_count=%s",
        len(raw_transactions),
        start_date,
        end_date,
        len(categories_list) if categories_list else 0,
    )
    daily = daily_totals
    if daily is None:
        daily = daily_spending(raw_transactions, start_date, end_date, categories_list)
    if daily.empty:
        LOGGER.warning("average_daily_spending computed from empty daily dataset.")
        return 0.0
    value = float(daily["amount"].mean())
    LOGGER.debug("average_daily_spending completed. average=%s", value)
    return value


def budget_adherence(budget: pd.DataFrame) -> pd.DataFrame:
    """Return budget-vs-actual table with over/under budget metrics."""
    LOGGER.debug("budget_adherence started. rows=%s cols=%s", len(budget), len(budget.columns))
    if budget.empty:
        LOGGER.warning("budget_adherence received an empty dataframe.")
        return pd.DataFrame(columns=["category", "projected", "actual", "delta", "pct_used", "is_total"])

    scoped = budget.copy()
    scoped["delta"] = scoped["actual"] - scoped["projected"]
    scoped["pct_used"] = scoped["actual"].div(scoped["projected"]).replace(
        [pd.NA, pd.NaT, float("inf"), -float("inf")], 0.0
    )
    scoped["pct_used"] = scoped["pct_used"].fillna(0.0)
    LOGGER.debug("budget_adherence completed. rows=%s cols=%s", len(scoped), len(scoped.columns))
    return scoped


def rolling_spending(
    expenses_by_category: pd.DataFrame,
    window: int = 3,
) -> pd.DataFrame:
    """Return monthly total spending and rolling average."""
    LOGGER.debug("rolling_spending started. rows=%s window=%s", len(expenses_by_category), window)
    if expenses_by_category.empty:
        LOGGER.warning("rolling_spending received an empty dataframe.")
        return pd.DataFrame(columns=["month", "amount", "rolling_avg"])

    monthly = (
        expenses_by_category.groupby("month", as_index=False)["amount"]
        .sum()
        .sort_values("month")
        .reset_index(drop=True)
    )
    monthly["rolling_avg"] = monthly["amount"].rolling(window=window, min_periods=1).mean()
    LOGGER.debug("rolling_spending completed. rows=%s cols=%s", len(monthly), len(monthly.columns))
    return monthly


def income_breakdown(
    income_by_source: pd.DataFrame,
    start_month: pd.Timestamp | None = None,
    end_month: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Return source-level income totals excluding TOTAL helper row."""
    LOGGER.debug(
        "income_breakdown started. rows=%s start_month=%s end_month=%s",
        len(income_by_source),
        start_month,
        end_month,
    )
    if income_by_source.empty:
        LOGGER.warning("income_breakdown received an empty dataframe.")
        return pd.DataFrame(columns=["source", "amount"])

    scoped = _apply_month_filter(income_by_source, start_month, end_month)
    scoped = scoped[~scoped["is_total"]]
    if scoped.empty:
        LOGGER.warning("income_breakdown has no non-total data for selected filters.")
    grouped = scoped.groupby("source", as_index=False)["amount"].sum()
    result = grouped.sort_values("amount", ascending=False).reset_index(drop=True)
    LOGGER.debug("income_breakdown completed. rows=%s cols=%s", len(result), len(result.columns))
    return result


def monthly_income_trend(
    income_by_source: pd.DataFrame,
    start_month: pd.Timestamp | None = None,
    end_month: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Return monthly income totals excluding TOTAL helper row."""
    LOGGER.debug(
        "monthly_income_trend started. rows=%s start_month=%s end_month=%s",
        len(income_by_source),
        start_month,
        end_month,
    )
    if income_by_source.empty:
        LOGGER.warning("monthly_income_trend received an empty dataframe.")
        return pd.DataFrame(columns=["month", "amount"])

    scoped = _apply_month_filter(income_by_source, start_month, end_month)
    scoped = scoped[~scoped["is_total"]]
    if scoped.empty:
        LOGGER.warning("monthly_income_trend has no non-total data for selected filters.")
    grouped = scoped.groupby("month", as_index=False)["amount"].sum()
    result = grouped.sort_values("month").reset_index(drop=True)
    LOGGER.debug("monthly_income_trend completed. rows=%s cols=%s", len(result), len(result.columns))
    return result


def cumulative_savings_investments(
    general_summary: pd.DataFrame,
    start_month: pd.Timestamp | None = None,
    end_month: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Return cumulative savings and investments over time."""
    LOGGER.debug(
        "cumulative_savings_investments started. rows=%s start_month=%s end_month=%s",
        len(general_summary),
        start_month,
        end_month,
    )
    scoped = _apply_month_filter(general_summary, start_month, end_month)
    scoped = scoped[scoped["source"].isin(["SAVINGS", "INVESTMENTS"])]
    if scoped.empty:
        LOGGER.warning("cumulative_savings_investments has no savings/investments data.")
        return pd.DataFrame(columns=["month", "savings", "investments", "savings_cum", "investments_cum"])

    monthly = (
        scoped.pivot_table(index="month", columns="source", values="amount", aggfunc="sum", fill_value=0.0)
        .sort_index()
        .reset_index()
    )
    for col in ["SAVINGS", "INVESTMENTS"]:
        if col not in monthly.columns:
            monthly[col] = 0.0

    monthly["savings_cum"] = monthly["SAVINGS"].cumsum()
    monthly["investments_cum"] = monthly["INVESTMENTS"].cumsum()
    result = monthly.rename(columns={"SAVINGS": "savings", "INVESTMENTS": "investments"})
    LOGGER.debug("cumulative_savings_investments completed. rows=%s cols=%s", len(result), len(result.columns))
    return result
