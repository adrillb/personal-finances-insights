"""Google Sheets helpers for cloud-based workbook access."""

from __future__ import annotations

from functools import lru_cache
import logging
import os
from pathlib import Path
from time import perf_counter, time
from urllib.parse import urlencode
from urllib.request import Request as UrlRequest, urlopen

import gspread
from dotenv import load_dotenv
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.oauth2.service_account import Credentials


load_dotenv()

SHEETS_SCOPE = "https://www.googleapis.com/auth/spreadsheets"
DRIVE_READ_SCOPE = "https://www.googleapis.com/auth/drive.readonly"
XLSX_MIME_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
LOGGER = logging.getLogger(__name__)


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _resolve_credentials_path(credentials_path: str | Path | None = None) -> Path:
    value = credentials_path or os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials.json")
    path = Path(value)
    if not path.is_absolute():
        path = _project_root() / path
    LOGGER.debug("Resolved Google credentials path: %s", path)
    if not path.exists():
        raise FileNotFoundError(f"Google credentials file not found: {path}")
    return path


def _resolve_spreadsheet_name(spreadsheet_name: str | None = None) -> str:
    name = (spreadsheet_name or os.getenv("SPREADSHEET_NAME", "")).strip()
    LOGGER.debug("Resolved spreadsheet name: %s", name)
    if not name:
        raise ValueError("Missing spreadsheet name. Set SPREADSHEET_NAME or pass spreadsheet_name.")
    return name


def get_service_account_credentials(
    credentials_path: str | Path | None = None,
    scopes: list[str] | None = None,
) -> Credentials:
    path = _resolve_credentials_path(credentials_path)
    auth_scopes = scopes or [SHEETS_SCOPE, DRIVE_READ_SCOPE]
    try:
        credentials = Credentials.from_service_account_file(str(path), scopes=auth_scopes)
        return credentials
    except Exception:
        LOGGER.error("Failed to authenticate service account credentials.", exc_info=True)
        raise


def get_gspread_client(credentials_path: str | Path | None = None) -> gspread.Client:
    try:
        credentials = get_service_account_credentials(credentials_path)
        client = gspread.authorize(credentials)
        LOGGER.info("Google Sheets auth client created successfully.")
        return client
    except Exception:
        LOGGER.error("Failed to create Google Sheets auth client.", exc_info=True)
        raise


def get_spreadsheet(
    spreadsheet_name: str | None = None,
    credentials_path: str | Path | None = None,
) -> gspread.Spreadsheet:
    try:
        client = get_gspread_client(credentials_path)
        name = _resolve_spreadsheet_name(spreadsheet_name)
        spreadsheet = client.open(name)
        LOGGER.info("Opened spreadsheet successfully: %s", name)
        return spreadsheet
    except Exception:
        LOGGER.error("Failed to open spreadsheet.", exc_info=True)
        raise


def download_sheet_as_xlsx(
    spreadsheet_name: str | None = None,
    credentials_path: str | Path | None = None,
) -> bytes:
    """Export a Google Sheet as XLSX bytes."""
    try:
        spreadsheet = get_spreadsheet(spreadsheet_name, credentials_path)
        credentials = get_service_account_credentials(credentials_path, scopes=[DRIVE_READ_SCOPE])
        credentials.refresh(GoogleAuthRequest())
        if not credentials.token:
            raise RuntimeError("Failed to obtain access token for Google Drive export.")

        base_url = f"https://www.googleapis.com/drive/v3/files/{spreadsheet.id}/export"
        query = urlencode({"mimeType": XLSX_MIME_TYPE})
        request = UrlRequest(
            url=f"{base_url}?{query}",
            headers={"Authorization": f"Bearer {credentials.token}"},
        )
        with urlopen(request, timeout=90) as response:
            payload = response.read()
            LOGGER.info("Downloaded XLSX export successfully. bytes=%s", len(payload))
            return payload
    except Exception:
        LOGGER.error("Failed to download spreadsheet as XLSX.", exc_info=True)
        raise


def _cache_ttl_seconds() -> int:
    raw_ttl = os.getenv("CLOUD_EXPORT_CACHE_TTL_SECONDS", "180")
    try:
        ttl = int(raw_ttl)
    except ValueError:
        ttl = 180
    return max(ttl, 1)


def _ttl_bucket(ttl_seconds: int) -> int:
    return int(time() // ttl_seconds)


@lru_cache(maxsize=8)
def _download_sheet_as_xlsx_cached(
    spreadsheet_name: str,
    credentials_path: str,
    ttl_bucket: int,
) -> bytes:
    del ttl_bucket
    return download_sheet_as_xlsx(
        spreadsheet_name=spreadsheet_name,
        credentials_path=credentials_path,
    )


def download_sheet_as_xlsx_cached(
    spreadsheet_name: str | None = None,
    credentials_path: str | Path | None = None,
    *,
    force_refresh: bool = False,
) -> bytes:
    """Export a Google Sheet as XLSX bytes with a short-lived cache."""
    if force_refresh:
        _download_sheet_as_xlsx_cached.cache_clear()
    name = _resolve_spreadsheet_name(spreadsheet_name)
    resolved_credentials = str(_resolve_credentials_path(credentials_path))
    ttl_seconds = _cache_ttl_seconds()
    start_time = perf_counter()
    payload = _download_sheet_as_xlsx_cached(
        name,
        resolved_credentials,
        _ttl_bucket(ttl_seconds),
    )
    elapsed_ms = (perf_counter() - start_time) * 1000
    LOGGER.debug(
        "download_sheet_as_xlsx_cached completed in %.2fms (ttl=%ss).",
        elapsed_ms,
        ttl_seconds,
    )
    return payload


def clear_cloud_export_cache() -> None:
    """Clear cached cloud export payloads."""
    _download_sheet_as_xlsx_cached.cache_clear()
