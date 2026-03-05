"""Google Sheets helpers for cloud-based workbook access."""

from __future__ import annotations

import os
from pathlib import Path
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


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _resolve_credentials_path(credentials_path: str | Path | None = None) -> Path:
    value = credentials_path or os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials.json")
    path = Path(value)
    if not path.is_absolute():
        path = _project_root() / path
    if not path.exists():
        raise FileNotFoundError(f"Google credentials file not found: {path}")
    return path


def _resolve_spreadsheet_name(spreadsheet_name: str | None = None) -> str:
    name = (spreadsheet_name or os.getenv("SPREADSHEET_NAME", "")).strip()
    if not name:
        raise ValueError("Missing spreadsheet name. Set SPREADSHEET_NAME or pass spreadsheet_name.")
    return name


def get_service_account_credentials(
    credentials_path: str | Path | None = None,
    scopes: list[str] | None = None,
) -> Credentials:
    path = _resolve_credentials_path(credentials_path)
    auth_scopes = scopes or [SHEETS_SCOPE, DRIVE_READ_SCOPE]
    return Credentials.from_service_account_file(str(path), scopes=auth_scopes)


def get_gspread_client(credentials_path: str | Path | None = None) -> gspread.Client:
    credentials = get_service_account_credentials(credentials_path)
    return gspread.authorize(credentials)


def get_spreadsheet(
    spreadsheet_name: str | None = None,
    credentials_path: str | Path | None = None,
) -> gspread.Spreadsheet:
    client = get_gspread_client(credentials_path)
    return client.open(_resolve_spreadsheet_name(spreadsheet_name))


def download_sheet_as_xlsx(
    spreadsheet_name: str | None = None,
    credentials_path: str | Path | None = None,
) -> bytes:
    """Export a Google Sheet as XLSX bytes."""
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
        return response.read()
