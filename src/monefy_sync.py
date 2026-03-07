"""Synchronize exported Monefy CSV files into Google Sheets."""

from __future__ import annotations

import csv
from datetime import datetime
import logging
import os
from pathlib import Path
import re

import pandas as pd
from dotenv import load_dotenv
import gspread

from src.cloud_connector import get_spreadsheet


load_dotenv()

NORMALIZED_COLUMNS = [
    "date",
    "account",
    "category",
    "amount",
    "currency",
    "converted_amount",
    "description",
]
LOGGER = logging.getLogger(__name__)


def _resolve_monefy_folder(folder: str | Path | None = None) -> Path:
    value = folder or os.getenv("MONEFY_FOLDER", "")
    if not str(value).strip():
        LOGGER.warning("MONEFY_FOLDER is missing or empty.")
        raise ValueError("Missing MONEFY_FOLDER. Set it in .env or pass folder explicitly.")
    path = Path(value)
    if not path.exists():
        LOGGER.warning("Monefy folder does not exist: %s", path)
        raise FileNotFoundError(f"Monefy folder not found: {path}")
    LOGGER.debug("Resolved Monefy folder: %s", path)
    return path


def _resolve_single_csv(folder_path: Path) -> Path:
    csv_files = sorted(folder_path.glob("*.csv"), key=lambda path: path.stat().st_mtime)
    LOGGER.debug("Discovered CSV files in MONEFY_FOLDER. count=%s", len(csv_files))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in MONEFY_FOLDER: {folder_path}")
    if len(csv_files) > 1:
        raise ValueError(
            f"Expected exactly one CSV in MONEFY_FOLDER but found {len(csv_files)}: {folder_path}"
        )
    return csv_files[0]


def load_monefy_csv_rows(path: str | Path) -> tuple[list[str], list[list[str]]]:
    """Load CSV rows preserving original text format (dates, numbers, etc.)."""
    csv_path = Path(path)
    LOGGER.debug("Loading raw Monefy CSV rows: %s", csv_path)
    try:
        with csv_path.open("r", encoding="utf-8-sig", newline="") as csv_file:
            sample = csv_file.read(4096)
            csv_file.seek(0)
            delimiter_candidates = [",", ";", "\t", "|"]
            delimiter = max(delimiter_candidates, key=sample.count) if sample else ","
            if sample and sample.count(delimiter) == 0:
                delimiter = ","

            reader = csv.reader(csv_file, delimiter=delimiter)
            raw_rows = [row for row in reader]
    except Exception:
        LOGGER.error("Failed loading CSV rows: %s", csv_path, exc_info=True)
        raise

    if not raw_rows:
        LOGGER.warning("CSV file has no rows: %s", csv_path)
        return [], []

    header = raw_rows[0]
    data_rows = [row for row in raw_rows[1:] if any(str(cell).strip() for cell in row)]
    LOGGER.debug(
        "Raw CSV loaded. header_cols=%s data_rows=%s delimiter=%r",
        len(header),
        len(data_rows),
        delimiter,
    )
    return header, data_rows


def _pick_column(columns: list[str], candidates: list[str]) -> str | None:
    candidate_set = {value.strip().lower() for value in candidates}
    for column in columns:
        if column in candidate_set:
            return column
    for column in columns:
        if any(token in column for token in candidate_set):
            return column
    return None


def _parse_ddmmyyyy_to_iso(value: str) -> str | None:
    text = str(value).strip()
    if not text:
        return None
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return text

    match = re.fullmatch(r"(\d{1,2})[\/\.-](\d{1,2})[\/\.-](\d{4})", text)
    if not match:
        return None
    day = int(match.group(1))
    month = int(match.group(2))
    year = int(match.group(3))
    try:
        return datetime(year, month, day).strftime("%Y-%m-%d")
    except ValueError:
        return None


def _normalize_date_columns(header: list[str], rows: list[list[str]]) -> list[list[str]]:
    """Normalize date-like values to ISO format to avoid locale ambiguity in Sheets."""
    if not header or not rows:
        return rows

    normalized_header = [str(col).strip().lower() for col in header]
    date_col = _pick_column(normalized_header, ["date", "transaction date"])
    if date_col is None:
        LOGGER.debug("No date column found in CSV header; skipping date normalization.")
        return rows

    date_index = normalized_header.index(date_col)
    normalized_rows: list[list[str]] = []
    converted_count = 0

    for row in rows:
        updated_row = list(row)
        if date_index < len(updated_row):
            normalized = _parse_ddmmyyyy_to_iso(str(updated_row[date_index]))
            if normalized is not None:
                updated_row[date_index] = normalized
                converted_count += 1
        normalized_rows.append(updated_row)

    LOGGER.debug("Date normalization completed. converted_rows=%s", converted_count)
    return normalized_rows


def _to_number(series: pd.Series) -> pd.Series:
    cleaned = (
        series.astype(str)
        .str.replace(r"[^\d,\.\-]", "", regex=True)
        .str.replace(",", ".", regex=False)
    )
    return pd.to_numeric(cleaned, errors="coerce")


def parse_monefy_csv(path: str | Path) -> pd.DataFrame:
    """Parse and normalize a Monefy CSV export to stable columns."""
    csv_path = Path(path)
    LOGGER.debug("Parsing Monefy CSV: %s", csv_path)
    try:
        frame = pd.read_csv(csv_path, sep=None, engine="python", dtype=str)
        if frame.empty:
            LOGGER.warning("CSV file has no rows: %s", csv_path)
            return pd.DataFrame(columns=NORMALIZED_COLUMNS)

        frame.columns = [str(column).strip() for column in frame.columns]
        normalized_cols = [column.lower() for column in frame.columns]
        col_map = dict(zip(normalized_cols, frame.columns))
        available = list(col_map.keys())

        date_col = _pick_column(available, ["date", "transaction date"])
        account_col = _pick_column(available, ["account", "wallet"])
        category_col = _pick_column(available, ["category"])
        amount_col = _pick_column(available, ["amount", "sum", "value"])
        currency_col = _pick_column(available, ["currency"])
        converted_col = _pick_column(
            available,
            ["converted amount", "amount converted", "amount in base currency"],
        )
        description_col = _pick_column(available, ["description", "note", "notes", "comment"])
        type_col = _pick_column(available, ["type", "transaction type", "income/expense"])

        if date_col is None or category_col is None or amount_col is None:
            raise ValueError(
                "CSV format not recognized. Required columns: date, category, amount."
            )

        parsed_dates = pd.to_datetime(frame[col_map[date_col]], dayfirst=True, errors="coerce")
        if parsed_dates.isna().all():
            parsed_dates = pd.to_datetime(frame[col_map[date_col]], dayfirst=False, errors="coerce")

        amounts = _to_number(frame[col_map[amount_col]]).fillna(0.0)
        if type_col is not None:
            txn_type = frame[col_map[type_col]].astype(str).str.lower()
            expense_mask = txn_type.str.contains("expense|gasto", regex=True, na=False)
            income_mask = txn_type.str.contains("income|ingreso", regex=True, na=False)
            amounts = amounts.abs()
            amounts = amounts.where(~expense_mask, -amounts.abs())
            amounts = amounts.where(~income_mask, amounts.abs())

        out = pd.DataFrame(
            {
                "date": parsed_dates,
                "account": frame[col_map[account_col]] if account_col else "",
                "category": frame[col_map[category_col]].astype(str).str.strip(),
                "amount": amounts,
                "currency": frame[col_map[currency_col]] if currency_col else "",
                "converted_amount": _to_number(frame[col_map[converted_col]])
                if converted_col
                else pd.Series([pd.NA] * len(frame)),
                "description": frame[col_map[description_col]].fillna("") if description_col else "",
            }
        )

        out = out.dropna(subset=["date"])
        out = out[out["category"].astype(str).str.strip() != ""]
        out = out.sort_values("date").reset_index(drop=True)
        LOGGER.debug("CSV parsed successfully. rows=%s cols=%s", len(out), len(out.columns))
        if out.empty:
            LOGGER.warning("CSV parsing produced no valid rows after normalization: %s", csv_path)
        return out[NORMALIZED_COLUMNS]
    except Exception:
        LOGGER.error("Failed to parse CSV file: %s", csv_path, exc_info=True)
        raise


def _create_unique_temp_title(spreadsheet: gspread.Spreadsheet) -> str:
    base = "__tmp_monefy_sync__"
    existing_titles = {worksheet.title for worksheet in spreadsheet.worksheets()}
    counter = 1
    while f"{base}{counter}" in existing_titles:
        counter += 1
    return f"{base}{counter}"


def _recreate_worksheet(
    spreadsheet: gspread.Spreadsheet,
    sheet_name: str,
    header: list[str],
) -> gspread.Worksheet:
    temp_worksheet: gspread.Worksheet | None = None
    try:
        existing_worksheet = spreadsheet.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        existing_worksheet = None

    if existing_worksheet is not None:
        worksheets = spreadsheet.worksheets()
        if len(worksheets) == 1:
            temp_worksheet = spreadsheet.add_worksheet(
                title=_create_unique_temp_title(spreadsheet),
                rows=1,
                cols=1,
            )
        spreadsheet.del_worksheet(existing_worksheet)

    worksheet = spreadsheet.add_worksheet(
        title=sheet_name,
        rows=max(2000, len(header) + 10),
        cols=max(26, len(header) + 5),
    )
    worksheet.append_row(header, value_input_option="USER_ENTERED")

    if temp_worksheet is not None:
        spreadsheet.del_worksheet(temp_worksheet)
    return worksheet


def _to_sheet_value(value: object) -> object:
    if pd.isna(value):
        return ""
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, float):
        return round(value, 2)
    return value


def _replace_worksheet_rows(
    worksheet: gspread.Worksheet,
    frame: pd.DataFrame,
) -> None:
    values = [[_to_sheet_value(value) for value in row] for row in frame.to_numpy()]
    if values:
        worksheet.append_rows(values, value_input_option="USER_ENTERED")


def _get_or_create_worksheet(
    spreadsheet: gspread.Spreadsheet,
    sheet_name: str,
    expected_rows: int,
    expected_cols: int,
) -> gspread.Worksheet:
    try:
        return spreadsheet.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        LOGGER.info(
            "Worksheet '%s' not found in '%s'. Creating it.",
            sheet_name,
            spreadsheet.title,
        )
        return spreadsheet.add_worksheet(
            title=sheet_name,
            rows=max(2000, expected_rows + 10),
            cols=max(26, expected_cols + 5),
        )


def _clear_and_replace_worksheet_rows(
    worksheet: gspread.Worksheet,
    header: list[str],
    rows: list[list[str]],
) -> None:
    worksheet.clear()

    max_columns = max(
        len(header),
        max((len(row) for row in rows), default=0),
    )
    if max_columns == 0:
        return

    values: list[list[str]] = []
    if header:
        padded_header = header + ([""] * (max_columns - len(header)))
        values.append(padded_header)
    values.extend(row + ([""] * (max_columns - len(row))) for row in rows)
    if not values:
        return
    worksheet.update("A1", values, value_input_option="USER_ENTERED")


def sync_to_sheet(
    header: list[str],
    rows: list[list[str]],
    spreadsheet_name: str,
    sheet_name: str = "MonefyCSV",
) -> int:
    """Clear worksheet values and write raw CSV rows; return imported data row count."""
    LOGGER.debug(
        "sync_to_sheet started. rows=%s spreadsheet=%s sheet=%s",
        len(rows),
        spreadsheet_name,
        sheet_name,
    )
    try:
        spreadsheet = get_spreadsheet(spreadsheet_name)
        worksheet = _get_or_create_worksheet(
            spreadsheet,
            sheet_name,
            expected_rows=len(rows) + 1,
            expected_cols=max(len(header), max((len(row) for row in rows), default=0)),
        )
        _clear_and_replace_worksheet_rows(worksheet, header, rows)
        row_count = len(rows)
        LOGGER.info(
            "Cleared and updated worksheet '%s' in spreadsheet '%s' with %s row(s).",
            sheet_name,
            spreadsheet_name,
            row_count,
        )
        return row_count
    except Exception:
        LOGGER.error("Failed writing rows to sheet=%s", sheet_name, exc_info=True)
        raise


def run_sync(
    folder: str | Path | None = None,
    spreadsheet_name: str | None = None,
    sheet_name: str = "MonefyCSV",
) -> dict[str, int | str]:
    """Clear Monefy sheet values and copy the single CSV in MONEFY_FOLDER."""
    LOGGER.debug(
        "run_sync started. folder=%s spreadsheet_name=%s sheet_name=%s",
        folder,
        spreadsheet_name,
        sheet_name,
    )
    folder_path = _resolve_monefy_folder(folder)

    if spreadsheet_name is None:
        spreadsheet_name = os.getenv("SPREADSHEET_NAME", "").strip()
    if not spreadsheet_name:
        raise ValueError("Missing SPREADSHEET_NAME. Set it in .env or pass spreadsheet_name.")

    csv_path = _resolve_single_csv(folder_path)
    header, rows = load_monefy_csv_rows(csv_path)
    rows = _normalize_date_columns(header, rows)
    imported_rows = sync_to_sheet(
        header,
        rows,
        spreadsheet_name=spreadsheet_name,
        sheet_name=sheet_name,
    )
    summary = {
        "processed_files": 1,
        "imported_rows": imported_rows,
        "source_file": csv_path.name,
        "sheet_name": sheet_name,
    }
    LOGGER.info(
        "Monefy sync completed. sheet=%s source_file=%s imported_rows=%s",
        sheet_name,
        csv_path.name,
        imported_rows,
    )
    return summary
