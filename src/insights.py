"""Metrics and aggregation helpers for the finance dashboard."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import pandas as pd


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
    if frame.empty:
        return frame.copy()
    result = frame.copy()
    result[month_col] = result[month_col].map(_normalize_month_start)
    if start_month is not None:
        result = result[result[month_col] >= _normalize_month_start(start_month)]
    if end_month is not None:
        result = result[result[month_col] <= _normalize_month_start(end_month)]
    return result


def monthly_totals(general_summary: pd.DataFrame) -> pd.DataFrame:
    """Return monthly totals with one column per source and net cash flow."""
    if general_summary.empty:
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

    return pivot.rename(
        columns={
            "INCOME": "income",
            "EXPENSES": "expenses",
            "INVESTMENTS": "investments",
            "SAVINGS": "savings",
        }
    )


def compute_kpis(
    general_summary: pd.DataFrame,
    start_month: pd.Timestamp | None = None,
    end_month: pd.Timestamp | None = None,
) -> KPISet:
    """Compute top-line KPI totals for the selected month range."""
    scoped = _apply_month_filter(general_summary, start_month, end_month)
    totals = scoped.groupby("source", as_index=False)["amount"].sum()
    data = {row["source"].upper(): float(row["amount"]) for _, row in totals.iterrows()}

    income = data.get("INCOME", 0.0)
    expenses = data.get("EXPENSES", 0.0)
    investments = data.get("INVESTMENTS", 0.0)
    savings = data.get("SAVINGS", 0.0)
    net = income - expenses - investments - savings
    savings_rate = (savings / income) if income else 0.0

    return KPISet(
        total_income=income,
        total_expenses=expenses,
        total_investments=investments,
        total_savings=savings,
        net_cash_flow=net,
        savings_rate=savings_rate,
    )


def spending_breakdown(
    expenses_by_category: pd.DataFrame,
    start_month: pd.Timestamp | None = None,
    end_month: pd.Timestamp | None = None,
    categories: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Aggregate expenses by category with percentage contribution."""
    scoped = _apply_month_filter(expenses_by_category, start_month, end_month)
    if categories:
        scoped = scoped[scoped["category"].isin(list(categories))]
    if scoped.empty:
        return pd.DataFrame(columns=["category", "amount", "percentage"])

    grouped = scoped.groupby("category", as_index=False)["amount"].sum()
    total = grouped["amount"].sum()
    grouped["percentage"] = grouped["amount"] / total if total else 0.0
    return grouped.sort_values("amount", ascending=False).reset_index(drop=True)


def monthly_category_spending(
    expenses_by_category: pd.DataFrame,
    start_month: pd.Timestamp | None = None,
    end_month: pd.Timestamp | None = None,
    categories: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Return monthly spending by category for stacked charts."""
    scoped = _apply_month_filter(expenses_by_category, start_month, end_month)
    if categories:
        scoped = scoped[scoped["category"].isin(list(categories))]
    if scoped.empty:
        return pd.DataFrame(columns=["month", "category", "amount"])
    return (
        scoped.groupby(["month", "category"], as_index=False)["amount"]
        .sum()
        .sort_values(["month", "amount"], ascending=[True, False])
        .reset_index(drop=True)
    )


def top_categories(
    expenses_by_category: pd.DataFrame,
    top_n: int = 5,
    start_month: pd.Timestamp | None = None,
    end_month: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Return the highest-spend categories for the selected period."""
    breakdown = spending_breakdown(expenses_by_category, start_month, end_month)
    return breakdown.head(top_n).reset_index(drop=True)


def daily_spending(
    raw_transactions: pd.DataFrame,
    start_date: pd.Timestamp | None = None,
    end_date: pd.Timestamp | None = None,
    categories: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Return daily total spending from transaction-level data."""
    if raw_transactions.empty:
        return pd.DataFrame(columns=["date", "amount"])

    scoped = raw_transactions.copy()
    scoped["date"] = pd.to_datetime(scoped["date"])
    if start_date is not None:
        scoped = scoped[scoped["date"] >= pd.Timestamp(start_date)]
    if end_date is not None:
        scoped = scoped[scoped["date"] <= pd.Timestamp(end_date)]
    if categories:
        scoped = scoped[scoped["category"].isin(list(categories))]
    if scoped.empty:
        return pd.DataFrame(columns=["date", "amount"])

    scoped["day"] = scoped["date"].dt.normalize()
    return (
        scoped.groupby("day", as_index=False)["amount"]
        .sum()
        .rename(columns={"day": "date"})
        .sort_values("date")
        .reset_index(drop=True)
    )


def average_daily_spending(
    raw_transactions: pd.DataFrame,
    start_date: pd.Timestamp | None = None,
    end_date: pd.Timestamp | None = None,
    categories: Iterable[str] | None = None,
) -> float:
    """Return average daily spending over days with transactions."""
    daily = daily_spending(raw_transactions, start_date, end_date, categories)
    if daily.empty:
        return 0.0
    return float(daily["amount"].mean())


def budget_adherence(budget: pd.DataFrame) -> pd.DataFrame:
    """Return budget-vs-actual table with over/under budget metrics."""
    if budget.empty:
        return pd.DataFrame(columns=["category", "projected", "actual", "delta", "pct_used", "is_total"])

    scoped = budget.copy()
    scoped["delta"] = scoped["actual"] - scoped["projected"]
    scoped["pct_used"] = scoped["actual"].div(scoped["projected"]).replace(
        [pd.NA, pd.NaT, float("inf"), -float("inf")], 0.0
    )
    scoped["pct_used"] = scoped["pct_used"].fillna(0.0)
    return scoped


def rolling_spending(
    expenses_by_category: pd.DataFrame,
    window: int = 3,
) -> pd.DataFrame:
    """Return monthly total spending and rolling average."""
    if expenses_by_category.empty:
        return pd.DataFrame(columns=["month", "amount", "rolling_avg"])

    monthly = (
        expenses_by_category.groupby("month", as_index=False)["amount"]
        .sum()
        .sort_values("month")
        .reset_index(drop=True)
    )
    monthly["rolling_avg"] = monthly["amount"].rolling(window=window, min_periods=1).mean()
    return monthly


def income_breakdown(
    income_by_source: pd.DataFrame,
    start_month: pd.Timestamp | None = None,
    end_month: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Return source-level income totals excluding TOTAL helper row."""
    if income_by_source.empty:
        return pd.DataFrame(columns=["source", "amount"])

    scoped = _apply_month_filter(income_by_source, start_month, end_month)
    scoped = scoped[~scoped["is_total"]]
    grouped = scoped.groupby("source", as_index=False)["amount"].sum()
    return grouped.sort_values("amount", ascending=False).reset_index(drop=True)


def monthly_income_trend(
    income_by_source: pd.DataFrame,
    start_month: pd.Timestamp | None = None,
    end_month: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Return monthly income totals excluding TOTAL helper row."""
    if income_by_source.empty:
        return pd.DataFrame(columns=["month", "amount"])

    scoped = _apply_month_filter(income_by_source, start_month, end_month)
    scoped = scoped[~scoped["is_total"]]
    grouped = scoped.groupby("month", as_index=False)["amount"].sum()
    return grouped.sort_values("month").reset_index(drop=True)


def cumulative_savings_investments(
    general_summary: pd.DataFrame,
    start_month: pd.Timestamp | None = None,
    end_month: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Return cumulative savings and investments over time."""
    scoped = _apply_month_filter(general_summary, start_month, end_month)
    scoped = scoped[scoped["source"].isin(["SAVINGS", "INVESTMENTS"])]
    if scoped.empty:
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
    return monthly.rename(columns={"SAVINGS": "savings", "INVESTMENTS": "investments"})
