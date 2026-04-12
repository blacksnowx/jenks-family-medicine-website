"""
Google Sheets sync for VA 201 Bills data.

Reads the "201s" tab from the configured Google Spreadsheet and returns a
DataFrame with the same column names that data_loader.load_va_data() expects
from '201 Bills and Payments.csv'.

Column mapping (Sheets → output DataFrame):
  A  → Case ID          (hashed with cid_ prefix — dedup key)
  B  → Provider         (staff name, not patient PII)
  D  → Date of Service
  E  → Focused DBQs
  F  → Routine IMOs
  G  → TBI
  H  → Gen Med DBQs
  P  → No Show

PII RULES:
  - Case IDs are hashed IMMEDIATELY upon retrieval.
  - Only row counts and timestamps are logged — never data values.
  - Provider column contains staff names (not patients) — not hashed.

Required env vars:
  GOOGLE_SERVICE_ACCOUNT_JSON — Full service account JSON as a string
  GOOGLE_SHEET_ID             — Spreadsheet ID to read from
  PII_HASH_SECRET             — Secret for deterministic PII hashing

Optional env vars:
  SHEETS_TAB_NAME — Worksheet tab name (default: '201s')
"""

import json
import logging
import os

import pandas as pd

try:
    from .pii_utils import hash_case_id
except ImportError:
    from pii_utils import hash_case_id

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_TAB_NAME = "201s"

# Column indices (0-based) in the "201s" sheet
COL_CASE_ID       = 0   # A
COL_PROVIDER      = 1   # B
COL_DATE_SERVICE  = 3   # D
COL_FOCUSED_DBQS  = 4   # E
COL_ROUTINE_IMOS  = 5   # F
COL_TBI           = 6   # G
COL_GEN_MED_DBQS  = 7   # H
COL_NO_SHOW       = 15  # P

# Output column names — must match what load_va_data() expects
OUTPUT_COLUMNS = [
    "Case ID",          # hashed
    "Provider",
    "Date of Service",
    "Focused DBQs",
    "Routine IMOs",
    "TBI",
    "Gen Med DBQs",
    "No Show",
]


# ---------------------------------------------------------------------------
# Auth helper
# ---------------------------------------------------------------------------

def _get_gspread_client():
    """
    Build and return an authenticated gspread client using service account
    credentials from the GOOGLE_SERVICE_ACCOUNT_JSON env var.

    Raises:
        RuntimeError: If the env var is missing or the JSON is invalid.
    """
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError as exc:
        raise RuntimeError(
            "gspread and google-auth are required. Run: pip install gspread google-auth"
        ) from exc

    raw_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    if not raw_json:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON env var is not set.")

    try:
        creds_dict = json.loads(raw_json)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            "GOOGLE_SERVICE_ACCOUNT_JSON is not valid JSON."
        ) from exc

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]

    try:
        credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        client = gspread.authorize(credentials)
    except Exception as exc:
        raise RuntimeError(f"Failed to authenticate with Google: {exc}") from exc

    return client


# ---------------------------------------------------------------------------
# Main sync function
# ---------------------------------------------------------------------------

def fetch_201s() -> pd.DataFrame:
    """
    Read the '201s' tab from the configured Google Spreadsheet.

    Returns:
        DataFrame with OUTPUT_COLUMNS, matching the format expected by
        data_loader.load_va_data(). Returns an empty DataFrame on failure.

    Raises:
        RuntimeError: If GOOGLE_SHEET_ID or auth credentials are missing.
    """
    sheet_id = os.environ.get("GOOGLE_SHEET_ID", "")
    if not sheet_id:
        raise RuntimeError("GOOGLE_SHEET_ID env var is not set.")

    tab_name = os.environ.get("SHEETS_TAB_NAME", DEFAULT_TAB_NAME)

    client = _get_gspread_client()

    try:
        spreadsheet = client.open_by_key(sheet_id)
    except Exception as exc:
        raise RuntimeError(f"Failed to open spreadsheet: {exc}") from exc

    try:
        worksheet = spreadsheet.worksheet(tab_name)
    except Exception as exc:
        raise RuntimeError(
            f"Worksheet '{tab_name}' not found in spreadsheet. "
            f"Check SHEETS_TAB_NAME env var. Error: {exc}"
        ) from exc

    logger.info("Fetching data from Google Sheets tab '%s'", tab_name)

    try:
        all_values = worksheet.get_all_values()
    except Exception as exc:
        raise RuntimeError(f"Failed to read worksheet data: {exc}") from exc

    if not all_values:
        logger.warning("Google Sheets returned empty data for tab '%s'", tab_name)
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    # Skip header row
    data_rows = all_values[1:]
    logger.info("Google Sheets: %d data rows fetched (excluding header)", len(data_rows))

    def safe_get(row: list, idx: int) -> str:
        """Return cell value or empty string if column doesn't exist."""
        if idx < len(row):
            return str(row[idx]).strip()
        return ""

    records = []
    skipped = 0
    for row in data_rows:
        raw_case_id = safe_get(row, COL_CASE_ID)

        # Skip blank rows
        if not raw_case_id:
            skipped += 1
            continue

        # Hash Case ID immediately — never store or log the raw value
        try:
            hashed_case_id = hash_case_id(raw_case_id)
        except Exception:
            skipped += 1
            continue

        record = {
            "Case ID":          hashed_case_id,
            "Provider":         safe_get(row, COL_PROVIDER),
            "Date of Service":  safe_get(row, COL_DATE_SERVICE),
            "Focused DBQs":     safe_get(row, COL_FOCUSED_DBQS),
            "Routine IMOs":     safe_get(row, COL_ROUTINE_IMOS),
            "TBI":              safe_get(row, COL_TBI),
            "Gen Med DBQs":     safe_get(row, COL_GEN_MED_DBQS),
            "No Show":          safe_get(row, COL_NO_SHOW),
        }
        records.append(record)

    if skipped > 0:
        logger.info("Sheets sync: skipped %d blank/invalid rows", skipped)

    if not records:
        logger.warning("Sheets sync produced 0 valid records after filtering")
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    df = pd.DataFrame(records, columns=OUTPUT_COLUMNS)
    logger.info("Google Sheets sync complete: %d records", len(df))
    return df
