"""Synchronize exported Monefy CSV files into Google Sheets."""

from __future__ import annotations

from datetime import datetime
import hashlib
import json
import logging
import os
from pathlib import Path

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


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


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


def _resolve_processed_log(processed_log: str | Path | None = None) -> Path:
    if processed_log is not None:
        resolved = Path(processed_log)
        LOGGER.debug("Resolved processed log path from argument: %s", resolved)
        return resolved
    resolved = _project_root() / "data" / ".monefy_processed.json"
    LOGGER.debug("Resolved processed log path from default: %s", resolved)
    return resolved


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    result = digest.hexdigest()
    LOGGER.debug("Computed SHA256 for %s: %s", path.name, result)
    return result


def _load_processed_entries(log_path: Path) -> dict[str, dict[str, object]]:
    if not log_path.exists():
        LOGGER.debug("Processed log not found. Starting with empty entries: %s", log_path)
        return {}
    with log_path.open("r", encoding="utf-8") as handle:
        raw_data = json.load(handle)
    if not isinstance(raw_data, dict):
        LOGGER.warning("Processed log has unexpected format. Resetting entries. path=%s", log_path)
        return {}
    entries = raw_data.get("entries", {})
    if isinstance(entries, dict):
        LOGGER.debug("Loaded processed entries. count=%s", len(entries))
        return entries
    LOGGER.warning("Processed log entries value is not a dictionary. path=%s", log_path)
    return {}


def _save_processed_entries(log_path: Path, entries: dict[str, dict[str, object]]) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"entries": entries}
    with log_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
    LOGGER.debug("Saved processed entries. count=%s path=%s", len(entries), log_path)


def _pick_column(columns: list[str], candidates: list[str]) -> str | None:
    candidate_set = {value.strip().lower() for value in candidates}
    for column in columns:
        if column in candidate_set:
            return column
    for column in columns:
        if any(token in column for token in candidate_set):
            return column
    return None


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


def get_unprocessed_csvs(
    folder: str | Path,
    processed_log: str | Path | None = None,
) -> list[Path]:
    """Return CSV files not present in the processed log (by file hash)."""
    folder_path = _resolve_monefy_folder(folder)
    log_path = _resolve_processed_log(processed_log)
    processed_entries = _load_processed_entries(log_path)

    candidates = sorted(folder_path.glob("*.csv"), key=lambda path: path.stat().st_mtime)
    LOGGER.debug("Discovered CSV files for processing. count=%s folder=%s", len(candidates), folder_path)
    unprocessed: list[Path] = []
    for csv_path in candidates:
        digest = _file_sha256(csv_path)
        if digest not in processed_entries:
            unprocessed.append(csv_path)
    LOGGER.debug("Unprocessed CSV files resolved. count=%s", len(unprocessed))
    return unprocessed


def _ensure_worksheet(spreadsheet: gspread.Spreadsheet, sheet_name: str, header: list[str]) -> gspread.Worksheet:
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=2000, cols=max(26, len(header) + 5))
        worksheet.append_row(header, value_input_option="USER_ENTERED")
        return worksheet

    existing_header = worksheet.row_values(1)
    if not existing_header:
        worksheet.append_row(header, value_input_option="USER_ENTERED")
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


def sync_to_sheet(
    df: pd.DataFrame,
    spreadsheet_name: str,
    sheet_name: str = "MonefyCSV",
) -> int:
    """Append normalized rows into a worksheet and return inserted row count."""
    LOGGER.debug(
        "sync_to_sheet started. rows=%s spreadsheet=%s sheet=%s",
        len(df),
        spreadsheet_name,
        sheet_name,
    )
    if df.empty:
        LOGGER.warning("sync_to_sheet received no data rows to sync.")
        return 0
    try:
        spreadsheet = get_spreadsheet(spreadsheet_name)
        worksheet = _ensure_worksheet(spreadsheet, sheet_name, NORMALIZED_COLUMNS)
        target_header = worksheet.row_values(1) or NORMALIZED_COLUMNS

        aligned = df.copy()
        for column in target_header:
            if column not in aligned.columns:
                aligned[column] = ""
        aligned = aligned[target_header]

        values = [[_to_sheet_value(value) for value in row] for row in aligned.to_numpy()]
        if not values:
            LOGGER.warning("sync_to_sheet produced no values after alignment.")
            return 0
        worksheet.append_rows(values, value_input_option="USER_ENTERED")
        LOGGER.info("Synced rows to sheet. sheet=%s rows=%s", sheet_name, len(values))
        return len(values)
    except Exception:
        LOGGER.error("Failed writing rows to sheet=%s", sheet_name, exc_info=True)
        raise


def run_sync(
    folder: str | Path | None = None,
    spreadsheet_name: str | None = None,
    sheet_name: str = "MonefyCSV",
    processed_log: str | Path | None = None,
) -> dict[str, int]:
    """Sync all new CSV files to Google Sheets and update processed log."""
    LOGGER.debug(
        "run_sync started. folder=%s spreadsheet_name=%s sheet_name=%s processed_log=%s",
        folder,
        spreadsheet_name,
        sheet_name,
        processed_log,
    )
    folder_path = _resolve_monefy_folder(folder)
    log_path = _resolve_processed_log(processed_log)
    processed_entries = _load_processed_entries(log_path)
    LOGGER.debug("Loaded existing processed entries. count=%s", len(processed_entries))

    if spreadsheet_name is None:
        spreadsheet_name = os.getenv("SPREADSHEET_NAME", "").strip()
    if not spreadsheet_name:
        raise ValueError("Missing SPREADSHEET_NAME. Set it in .env or pass spreadsheet_name.")

    csv_files = sorted(folder_path.glob("*.csv"), key=lambda path: path.stat().st_mtime)
    LOGGER.debug("Discovered CSV files for sync. count=%s", len(csv_files))
    if not csv_files:
        LOGGER.warning("No CSV files found in Monefy folder: %s", folder_path)
    processed_files = 0
    skipped_files = 0
    imported_rows = 0

    try:
        for csv_path in csv_files:
            digest = _file_sha256(csv_path)
            if digest in processed_entries:
                skipped_files += 1
                continue

            frame = parse_monefy_csv(csv_path)
            rows_added = sync_to_sheet(frame, spreadsheet_name=spreadsheet_name, sheet_name=sheet_name)
            imported_rows += rows_added
            processed_files += 1
            processed_entries[digest] = {
                "file_name": csv_path.name,
                "processed_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                "rows_imported": rows_added,
            }
            LOGGER.info(
                "Synced CSV file. file=%s rows_added=%s digest=%s",
                csv_path.name,
                rows_added,
                digest,
            )
    except Exception:
        LOGGER.error("Monefy sync failed.", exc_info=True)
        raise

    _save_processed_entries(log_path, processed_entries)
    summary = {
        "processed_files": processed_files,
        "skipped_files": skipped_files,
        "imported_rows": imported_rows,
    }
    LOGGER.info(
        "Monefy sync completed. processed_files=%s skipped_files=%s imported_rows=%s",
        processed_files,
        skipped_files,
        imported_rows,
    )
    return summary
