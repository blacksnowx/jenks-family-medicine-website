"""
Data ingestion module for automated pulling of:
  1. VA C&P Exam data from Google Sheets
  2. Primary Care charges from Tebra/Kareo SOAP API

The pipeline stores clean data in the ReferenceData table using the same
filenames ('201 Bills and Payments.csv', 'Charges Export.csv') that
data_loader.py already reads — so the dashboard immediately picks up fresh
data without any other code changes.

Usage (Flask CLI):
    flask pull-data              # pull both sources
    flask pull-data --source va  # pull VA only
    flask pull-data --source pc  # pull primary care only

Environment variables required:
    GOOGLE_SHEETS_CREDENTIALS  — JSON string for a GCP service account
    TEBRAKEY                   — Tebra/Kareo customer API key
    TEBRA_USER                 — (optional) Tebra username
    TEBRA_PASSWORD             — (optional) Tebra password
    DATABASE_URL               — PostgreSQL URL (falls back to local SQLite)
"""

import datetime
import io
import json
import logging
import os
import re
import xml.etree.ElementTree as ET

import pandas as pd
import requests

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

TEBRA_SOAP_URL = "https://webservice.kareo.com/services/soap/2.1/"
SHEETS_SPREADSHEET_ID = "195fdZnKs1SrWd9EqV9CRXF3kt6a71mFtXWPNu6bpCLM"

# PII fields that must be stripped from Tebra data immediately after retrieval.
# This list is intentionally broad — any field name containing patient/guarantor
# demographics must not survive past _strip_pii_from_charge().
TEBRA_PII_FIELDS = {
    "PatientFirstName", "PatientLastName", "PatientFullName", "PatientName",
    "PatientDOB", "PatientDateOfBirth", "PatientSSN", "PatientSocialSecurity",
    "PatientAddress", "PatientAddress1", "PatientAddress2",
    "PatientCity", "PatientState", "PatientZip", "PatientZipCode",
    "PatientPhone", "PatientPhoneNumber", "PatientCellPhone",
    "PatientEmail", "PatientEmailAddress",
    "GuarantorFirstName", "GuarantorLastName", "GuarantorAddress",
    "GuarantorPhone", "GuarantorEmail",
    "PatientChartNumber", "PatientAccountNumber",
}

# The only fields we want to keep from a Tebra charge record.
TEBRA_SAFE_FIELDS = {
    "ServiceDate",
    "ProcedureCode",
    "Modifiers",
    "RenderingProviderName",
    "FacilityName",
    "ChargeAmount",
    "Units",
    "PrimaryInsuranceName",
    "SecondaryInsuranceName",
    "InsuranceContractAdjustment",
    "InsurancePaymentAmount",
    "PatientPaymentAmount",
    "OtherAdjustment",
}

# CPT code → Work RVU lookup table.
# Values sourced from CMS Physician Fee Schedule (2024).
# Descriptions are kept as inline comments for maintainability.
CPT_RVU_MAP: dict[str, float] = {
    # Office/outpatient E&M — established patient
    "99211": 0.18,  "99212": 0.70,  "99213": 1.30,  "99214": 1.92,  "99215": 2.80,
    # Office/outpatient E&M — new patient
    "99202": 0.93,  "99203": 1.42,  "99204": 2.43,  "99205": 3.17,
    # Annual wellness visits
    "G0438": 1.50,  "G0439": 1.30,
    "99385": 1.75,  "99386": 1.92,  "99387": 2.00,
    "99395": 1.50,  "99396": 1.75,  "99397": 1.92,
    # Telehealth (audio-only E&M)
    "99441": 0.48,  "99442": 0.97,  "99443": 1.50,
    # Prolonged services
    "99354": 1.76,  "99355": 1.76,  "99417": 0.61,
    # Vaccine administration
    "90471": 0.17,  "90472": 0.15,  "90473": 0.17,  "90474": 0.15,
    # Common vaccines (administration component only)
    "90686": 0.17,  "90714": 0.17,  "90718": 0.17,  "90707": 0.17,
    # DOT / occupational physicals
    "99455": 1.77,  "99456": 1.77,
    # Medical opinion / nexus letter (miscellaneous service)
    "99080": 2.50,
    # Complex chronic care management
    "99487": 1.00,  "99489": 0.50,  "99490": 0.61,  "99491": 1.05,
    # Transitional care management
    "99495": 2.11,  "99496": 3.13,
}

# ── Google Sheets ─────────────────────────────────────────────────────────────

def _get_sheets_service():
    """
    Build and return an authenticated Google Sheets API service object.

    Reads service account credentials from the GOOGLE_SHEETS_CREDENTIALS
    environment variable (JSON string).

    Raises:
        RuntimeError: If required Google API libraries are not installed.
        ValueError:   If GOOGLE_SHEETS_CREDENTIALS is not set.
    """
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
    except ImportError as exc:
        raise RuntimeError(
            "Google API libraries not installed. "
            "Run: pip install google-api-python-client google-auth"
        ) from exc

    creds_json = os.environ.get("GOOGLE_SHEETS_CREDENTIALS")
    if not creds_json:
        raise ValueError("GOOGLE_SHEETS_CREDENTIALS environment variable not set")

    creds_dict = json.loads(creds_json)
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    credentials = service_account.Credentials.from_service_account_info(
        creds_dict, scopes=scopes
    )
    return build("sheets", "v4", credentials=credentials, cache_discovery=False)


def pull_google_sheets_va_data(spreadsheet_id: str = SHEETS_SPREADSHEET_ID) -> bytes:
    """
    Pull VA C&P exam data from a Google Sheets spreadsheet.

    Returns the sheet contents as UTF-8 CSV bytes whose columns match the
    '201 Bills and Payments.csv' schema that data_loader.load_va_data() expects.

    Args:
        spreadsheet_id: Google Sheets document ID.

    Returns:
        CSV bytes ready to be stored in the ReferenceData table.

    Raises:
        ValueError: If the sheet is empty.
        RuntimeError: If the Google API call fails.
    """
    logger.info("Pulling VA data from Google Sheets (id=%s)", spreadsheet_id)

    service = _get_sheets_service()

    try:
        result = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range="A:Z")
            .execute()
        )
    except Exception as exc:
        raise RuntimeError(f"Google Sheets API call failed: {exc}") from exc

    values = result.get("values", [])
    if not values:
        raise ValueError("Google Sheets returned empty data")

    headers = values[0]
    rows = values[1:]

    # Pad short rows to header length so DataFrame construction doesn't fail
    rows = [r + [""] * (len(headers) - len(r)) for r in rows]
    df = pd.DataFrame(rows, columns=headers)

    # Drop completely blank rows
    df = df[df.apply(lambda r: r.str.strip().ne("").any(), axis=1)].reset_index(drop=True)

    if df.empty:
        raise ValueError("Google Sheets returned no data rows")

    logger.info("Pulled %d VA rows from Google Sheets", len(df))
    return df.to_csv(index=False).encode("utf-8")


# ── Tebra / Kareo SOAP API ────────────────────────────────────────────────────

def _build_get_charges_request(
    api_key: str,
    from_date: str,
    to_date: str,
    username: str = "",
    password: str = "",
) -> str:
    """
    Construct the SOAP XML envelope for a GetCharges request.

    Args:
        api_key:   Tebra customer API key (TEBRAKEY).
        from_date: Start date as 'YYYY-MM-DD'.
        to_date:   End date as 'YYYY-MM-DD'.
        username:  Optional Tebra username.
        password:  Optional Tebra password.

    Returns:
        SOAP XML string (UTF-8).
    """
    # Escape XML special characters in credentials
    def _esc(s: str) -> str:
        return (
            s.replace("&", "&amp;")
             .replace("<", "&lt;")
             .replace(">", "&gt;")
             .replace('"', "&quot;")
             .replace("'", "&apos;")
        )

    return (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"'
        ' xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"'
        ' xmlns:xsd="http://www.w3.org/2001/XMLSchema">'
        "<soap:Body>"
        '<GetCharges xmlns="http://www.kareo.com/api/schemas/">'
        "<request>"
        "<RequestHeader>"
        f"<CustomerKey>{_esc(api_key)}</CustomerKey>"
        f"<User>{_esc(username)}</User>"
        f"<Password>{_esc(password)}</Password>"
        "</RequestHeader>"
        f"<FromServiceDate>{from_date}</FromServiceDate>"
        f"<ToServiceDate>{to_date}</ToServiceDate>"
        "</request>"
        "</GetCharges>"
        "</soap:Body>"
        "</soap:Envelope>"
    )


def _strip_pii_from_charge(charge_dict: dict) -> dict:
    """
    Remove all PII fields from a charge record dictionary.

    Only fields present in TEBRA_SAFE_FIELDS are retained.  Any field not
    explicitly listed as safe is dropped, even if it is not in the known PII
    list — this is a whitelist approach for maximum safety.

    Args:
        charge_dict: Raw charge record as a flat dict.

    Returns:
        New dict containing only safe, non-PII fields.
    """
    return {k: v for k, v in charge_dict.items() if k in TEBRA_SAFE_FIELDS}


def _parse_charges_response(xml_text: str) -> list[dict]:
    """
    Parse a Tebra GetCharges SOAP response and return PII-stripped records.

    PII is stripped immediately upon parsing each element — raw patient data
    never exists as a complete dict in memory after this function runs.

    Args:
        xml_text: Raw SOAP response body as a string.

    Returns:
        List of clean charge dicts (PII removed).

    Raises:
        ValueError: If the XML cannot be parsed.
    """
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        raise ValueError(f"Failed to parse Tebra SOAP response: {exc}") from exc

    _ns = re.compile(r"\{[^}]+\}")

    def _tag(el) -> str:
        return _ns.sub("", el.tag)

    charges = []
    for el in root.iter():
        if _tag(el) in ("Charge", "ChargeDTO"):
            raw: dict = {}
            for child in el:
                raw[_tag(child)] = (child.text or "").strip()
            # Strip PII immediately — never accumulate patient demographics
            charges.append(_strip_pii_from_charge(raw))

    return charges


def _normalize_tebra_charges(charges: list[dict]) -> pd.DataFrame:
    """
    Convert stripped Tebra charge dicts into a DataFrame whose columns match
    the 'Charges Export.csv' schema that data_loader.load_pc_data() expects.

    Args:
        charges: List of PII-free charge dicts from _parse_charges_response().

    Returns:
        DataFrame with standardised column names.
    """
    df = pd.DataFrame(charges) if charges else pd.DataFrame()

    column_map = {
        "ServiceDate":                   "Date Of Service",
        "RenderingProviderName":         "Rendering Provider",
        "ProcedureCode":                 "Procedure Codes with Modifiers",
        "Modifiers":                     "Modifiers",
        "ChargeAmount":                  "Service Charge Amount",
        "Units":                         "Units",
        "FacilityName":                  "Facility Name",
        "PrimaryInsuranceName":          "Pri Ins Name",
        "SecondaryInsuranceName":        "Sec Ins Name",
        "InsuranceContractAdjustment":   "Pri Ins Insurance Contract Adjustment",
        "InsurancePaymentAmount":        "Pri Ins Insurance Payment",
        "PatientPaymentAmount":          "Pat Payment Amount",
        "OtherAdjustment":              "Other Adjustment",
    }
    df = df.rename(columns={k: v for k, v in column_map.items() if k in df.columns})

    # Guarantee the columns load_pc_data() requires exist
    required = [
        "Date Of Service",
        "Rendering Provider",
        "Procedure Codes with Modifiers",
        "Service Charge Amount",
        "Units",
    ]
    for col in required:
        if col not in df.columns:
            df[col] = ""

    return df


def pull_tebra_charges(
    from_date: str | None = None,
    to_date: str | None = None,
    timeout: int = 60,
) -> bytes:
    """
    Pull charges from the Tebra/Kareo SOAP API with immediate PII stripping.

    Returns CSV bytes compatible with the 'Charges Export.csv' format.

    Args:
        from_date: Start date as 'YYYY-MM-DD'. Defaults to 90 days ago.
        to_date:   End date as 'YYYY-MM-DD'. Defaults to today.
        timeout:   HTTP request timeout in seconds.

    Returns:
        CSV bytes ready to be stored in the ReferenceData table.

    Raises:
        ValueError:  If TEBRAKEY is not set.
        RuntimeError: If the HTTP request fails.
    """
    api_key = os.environ.get("TEBRAKEY")
    if not api_key:
        raise ValueError("TEBRAKEY environment variable not set")

    username = os.environ.get("TEBRA_USER", "")
    password = os.environ.get("TEBRA_PASSWORD", "")

    if not from_date:
        from_date = (datetime.datetime.today() - datetime.timedelta(days=90)).strftime("%Y-%m-%d")
    if not to_date:
        to_date = datetime.datetime.today().strftime("%Y-%m-%d")

    logger.info("Pulling Tebra charges from %s to %s", from_date, to_date)

    soap_body = _build_get_charges_request(api_key, from_date, to_date, username, password)
    headers = {
        "Content-Type": "text/xml; charset=utf-8",
        "SOAPAction": "http://www.kareo.com/api/schemas/GetCharges",
    }

    try:
        response = requests.post(
            TEBRA_SOAP_URL,
            data=soap_body.encode("utf-8"),
            headers=headers,
            timeout=timeout,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(f"Tebra API request failed: {exc}") from exc

    charges = _parse_charges_response(response.text)

    if not charges:
        logger.warning("Tebra API returned 0 charges for the requested date range")
        empty = pd.DataFrame(
            columns=["Date Of Service", "Rendering Provider",
                     "Procedure Codes with Modifiers", "Service Charge Amount", "Units"]
        )
        return empty.to_csv(index=False).encode("utf-8")

    df = _normalize_tebra_charges(charges)
    logger.info("Pulled and sanitised %d charge records from Tebra", len(df))
    return df.to_csv(index=False).encode("utf-8")


# ── CPT → RVU mapping ────────────────────────────────────────────────────────

def cpt_to_rvu(cpt_code: str, procedure_description: str = "") -> float:
    """
    Map a CPT code to a Work RVU value.

    Lookup order:
      1. Exact match in CPT_RVU_MAP
      2. Regex pattern matching for E&M and vaccine ranges
      3. Description-based keyword fallback (mirrors rvu_analytics logic)

    Args:
        cpt_code:              CPT code string (e.g. '99213').
        procedure_description: Optional free-text description for fallback.

    Returns:
        RVU value as float. Returns 0.0 if the code is unknown.
    """
    clean = re.sub(r"\s+", "", str(cpt_code)).upper()

    # 1. Exact lookup
    if clean in CPT_RVU_MAP:
        return CPT_RVU_MAP[clean]

    # 2. Pattern ranges
    if re.search(r"992[01][1-5]", clean):   # 99211-99215 / 99201-99205
        return 1.30
    if re.search(r"993[89][0-9]", clean):   # 99381-99397 annual/preventive
        return 1.30
    if re.search(r"90[4-7][0-9]{2}", clean):  # 904xx-907xx vaccine admin
        return 0.50

    # 3. Description fallback
    desc = procedure_description.upper()
    if "FUNCTIONAL" in desc and "INITIAL" in desc:
        return 3.75
    if "FUNCTIONAL" in desc:
        return 2.50
    if "NEXUS" in desc:
        return 2.50
    if "DOT" in desc and "PHYSICAL" in desc:
        return 1.0
    if "ANNUAL" in desc or "PHYSICAL" in desc:
        return 1.30
    if "VACCINE" in desc or "ADMIN" in desc:
        return 0.50

    return 0.0


# ── Deduplication ────────────────────────────────────────────────────────────

#: Column subset used to decide whether two PC charge rows are the same charge.
#: Patient-identifying columns (name, chart #) are intentionally excluded —
#: deduplication relies solely on clinical and billing fields.
PC_DEDUP_KEYS = [
    "Date Of Service",
    "Rendering Provider",
    "Procedure Codes with Modifiers",
    "Service Charge Amount",
    "Units",
]


def _read_existing_csv_from_db(filename: str) -> pd.DataFrame:
    """
    Load an existing CSV blob from the ReferenceData table.

    Returns an empty DataFrame if the file is not found or the table doesn't
    exist yet (e.g., fresh deployment with no uploaded data).
    """
    from sqlalchemy import create_engine, text

    database_url = os.environ.get("DATABASE_URL", "")
    if not database_url:
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        db_path = os.path.join(base_dir, "instance", "site.db")
        database_url = f"sqlite:///{db_path}"

    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql://", 1)

    try:
        engine = create_engine(database_url)
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT data FROM reference_data WHERE filename = :f"),
                {"f": filename},
            ).fetchone()
            if row and row[0]:
                return pd.read_csv(io.BytesIO(bytes(row[0])))
    except Exception as exc:
        logger.warning("Could not load existing '%s' from DB: %s", filename, exc)
    return pd.DataFrame()


def _dedup_charges(new_df: pd.DataFrame, existing_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge freshly pulled charges with the existing dataset and remove duplicates.

    Deduplication key: PC_DEDUP_KEYS (date, provider, CPT, charge, units).
    When a row appears in both DataFrames the version from *new_df* is kept so
    that corrected charges (e.g. updated charge amounts) replace stale ones.

    Args:
        new_df:      Newly pulled charges (already PII-stripped).
        existing_df: Charges already stored in the database.

    Returns:
        Merged, deduplicated DataFrame.
    """
    if existing_df.empty:
        return new_df.reset_index(drop=True)
    if new_df.empty:
        return existing_df.reset_index(drop=True)

    available_keys = [
        k for k in PC_DEDUP_KEYS
        if k in new_df.columns and k in existing_df.columns
    ]

    if not available_keys:
        logger.warning("No common dedup keys found — appending without deduplication")
        return pd.concat([existing_df, new_df], ignore_index=True)

    # Stack new rows first so keep='first' retains the newer version on conflict
    combined = pd.concat([new_df, existing_df], ignore_index=True)
    deduped = combined.drop_duplicates(subset=available_keys, keep="first")
    logger.info(
        "Dedup: %d new + %d existing → %d unique rows (%d duplicates removed)",
        len(new_df), len(existing_df), len(deduped),
        len(new_df) + len(existing_df) - len(deduped),
    )
    return deduped.reset_index(drop=True)


# ── Database storage ──────────────────────────────────────────────────────────

def store_csv_to_db(filename: str, csv_bytes: bytes) -> None:
    """
    Upsert CSV bytes into the ReferenceData table.

    Mirrors the manual upload behaviour in app.py (Owner upload form) but
    works outside a Flask application context so the CLI command can call it
    without spinning up the full web application.

    Args:
        filename:  One of '201 Bills and Payments.csv', 'Charges Export.csv',
                   or 'bank.csv'.
        csv_bytes: UTF-8 encoded CSV content.
    """
    from sqlalchemy import create_engine, text

    database_url = os.environ.get("DATABASE_URL", "")
    if not database_url:
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        db_path = os.path.join(base_dir, "instance", "site.db")
        database_url = f"sqlite:///{db_path}"

    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql://", 1)

    engine = create_engine(database_url)
    now = datetime.datetime.now(datetime.timezone.utc)

    with engine.begin() as conn:
        existing = conn.execute(
            text("SELECT id FROM reference_data WHERE filename = :f"),
            {"f": filename},
        ).fetchone()

        if existing:
            conn.execute(
                text(
                    "UPDATE reference_data SET data = :data, updated_at = :now "
                    "WHERE filename = :f"
                ),
                {"data": csv_bytes, "now": now, "f": filename},
            )
        else:
            conn.execute(
                text(
                    "INSERT INTO reference_data (filename, data, updated_at) "
                    "VALUES (:f, :data, :now)"
                ),
                {"f": filename, "data": csv_bytes, "now": now},
            )

    logger.info("Stored %d bytes as '%s' in ReferenceData", len(csv_bytes), filename)


# ── Full ingestion pipeline ───────────────────────────────────────────────────

def run_ingestion_pipeline(sources: list[str] | None = None) -> dict:
    """
    Run the full data ingestion pipeline.

    Pulls from each requested source and upserts the result into the
    ReferenceData table so the dashboard immediately sees fresh data.

    Args:
        sources: Which sources to pull. Accepts any combination of 'va' and
                 'pc'. Defaults to both.

    Returns:
        Dict keyed by source name. Each value has:
          - 'status': 'ok' or 'error'
          - 'rows':   Row count (on success)
          - 'error':  Error message string (on failure)
    """
    if sources is None:
        sources = ["va", "pc"]

    results: dict = {}

    if "va" in sources:
        try:
            csv_bytes = pull_google_sheets_va_data()
            store_csv_to_db("201 Bills and Payments.csv", csv_bytes)
            df = pd.read_csv(io.BytesIO(csv_bytes))
            results["va"] = {"status": "ok", "rows": len(df)}
            logger.info("VA ingestion complete: %d rows", len(df))
        except Exception as exc:
            logger.error("VA ingestion failed: %s", exc)
            results["va"] = {"status": "error", "error": str(exc)}

    if "pc" in sources:
        try:
            csv_bytes = pull_tebra_charges()
            new_df = pd.read_csv(io.BytesIO(csv_bytes))
            existing_df = _read_existing_csv_from_db("Charges Export.csv")
            merged_df = _dedup_charges(new_df, existing_df)
            final_csv = merged_df.to_csv(index=False).encode("utf-8")
            store_csv_to_db("Charges Export.csv", final_csv)
            results["pc"] = {
                "status": "ok",
                "rows": len(merged_df),
                "new_rows": len(new_df),
            }
            logger.info("PC ingestion complete: %d total rows (%d new)", len(merged_df), len(new_df))
        except Exception as exc:
            logger.error("PC ingestion failed: %s", exc)
            results["pc"] = {"status": "error", "error": str(exc)}

    return results
