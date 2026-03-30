"""
Tebra (Kareo) SOAP API sync for charge data.

Pulls charge records from the Tebra SOAP API and returns a DataFrame
with the same column names that data_loader.load_pc_data() expects from
'Charges Export.csv'.

PII RULES:
  - NEVER request patient names, DOB, addresses, SSNs, or any other PHI.
  - Patient ID and Encounter ID are hashed IMMEDIATELY upon retrieval.
  - Only row counts and timestamps are logged — never data values.

Required env vars:
  TEBRAKEY          — CustomerKey for the Tebra/Kareo API
  TEBRA_USER        — API username (practice admin account)
  TEBRA_PASSWORD    — API password
  PII_HASH_SECRET   — Secret for deterministic PII hashing (see pii_utils.py)

Optional env vars:
  TEBRA_PRACTICE_ID — Practice ID (default: 100713); logged for diagnostics
"""

import logging
import os
import re
import time
import xml.etree.ElementTree as ET
from datetime import date, timedelta

import pandas as pd
import requests

try:
    from .pii_utils import hash_encounter_id, hash_patient_id
except ImportError:
    from pii_utils import hash_encounter_id, hash_patient_id

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SOAP_ENDPOINT = "https://webservice.kareo.com/services/soap/2.1/KareoServices.svc"

# Bug 2 fix: correct SOAPAction URLs (no /2.1, no IKareoServices)
SOAP_ACTION_GET_CHARGES = "http://www.kareo.com/api/schemas/KareoServices/GetCharges"
SOAP_ACTION_GET_PRACTICES = "http://www.kareo.com/api/schemas/KareoServices/GetPractices"

# Bug 1 fix: correct namespace (no /2.1 suffix)
NS_ENVELOPE = "http://schemas.xmlsoap.org/soap/envelope/"
NS_KAREO = "http://www.kareo.com/api/schemas/"

# ClientVersion required by the Kareo WSDL RequestHeader schema
CLIENT_VERSION = "4.0"

# Columns in the output DataFrame — must match Charges Export.csv column names
# exactly as expected by data_loader.load_pc_data()
OUTPUT_COLUMNS = [
    "Date Of Service",
    "Rendering Provider",
    "Service Charge Amount",
    "Pri Ins Insurance Contract Adjustment",
    "Sec Ins Insurance Contract Adjustment",
    "Other Ins Insurance Contract Adjustment",
    "Pri Ins Insurance Payment",
    "Sec Ins Insurance Payment",
    "Other Ins Insurance Payment",
    "Pat Payment Amount",
    "Other Adjustment",
    "Procedure Code",
    "Procedure Codes with Modifiers",
    "Encounter ID",   # hashed with eid_ prefix
    "Patient ID",     # hashed with pid_ prefix
]

# ---------------------------------------------------------------------------
# Practice discovery
# ---------------------------------------------------------------------------

def get_practice_ids(customer_key: str, username: str, password: str) -> list[dict]:
    """
    Call GetPractices to retrieve all practices accessible with these credentials.

    Returns a list of dicts with 'ID' and 'Name' keys (practice info is not PHI).
    Logs available practices so the admin can verify credentials are working.
    """
    # Bug 1 fix: correct namespace; Bug 8 fix: no PracticeFields wrapper
    # RequestHeader element order must match WSDL xs:sequence exactly:
    # ClientVersion → CustomerKey → Password → User
    logger.info(
        "Tebra GetPractices RequestHeader: ClientVersion=%s CustomerKey=%s User=%s Password=***",
        CLIENT_VERSION, customer_key, username,
    )
    envelope = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope
    xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
    xmlns:kar="http://www.kareo.com/api/schemas/">
  <soap:Header/>
  <soap:Body>
    <kar:GetPractices>
      <kar:request>
        <kar:RequestHeader>
          <kar:ClientVersion>{CLIENT_VERSION}</kar:ClientVersion>
          <kar:CustomerKey>{customer_key}</kar:CustomerKey>
          <kar:Password>{password}</kar:Password>
          <kar:User>{username}</kar:User>
        </kar:RequestHeader>
        <kar:Fields>
          <kar:ID>true</kar:ID>
          <kar:Name>true</kar:Name>
        </kar:Fields>
      </kar:request>
    </kar:GetPractices>
  </soap:Body>
</soap:Envelope>"""

    try:
        resp = requests.post(
            SOAP_ENDPOINT,
            data=envelope.encode("utf-8"),
            headers={
                "Content-Type": "text/xml; charset=utf-8",
                "SOAPAction": f'"{SOAP_ACTION_GET_PRACTICES}"',
            },
            timeout=30,
        )
    except requests.RequestException as exc:
        logger.warning("GetPractices HTTP request failed: %s", exc)
        return []

    if resp.status_code != 200:
        logger.warning("GetPractices returned HTTP %d: %s", resp.status_code, resp.text[:500])
        return []

    try:
        root = ET.fromstring(resp.text)
    except ET.ParseError as exc:
        logger.warning("GetPractices XML parse error: %s", exc)
        return []

    # Check ErrorResponse and SecurityResponse
    result = root.find(f".//{{{NS_KAREO}}}GetPracticesResult")
    if result is not None:
        error_el = result.find(f"{{{NS_KAREO}}}ErrorResponse")
        if error_el is not None:
            is_error = error_el.findtext(f"{{{NS_KAREO}}}IsError", "").strip().lower()
            if is_error == "true":
                msg = error_el.findtext(f"{{{NS_KAREO}}}ErrorMessage", "").strip()
                logger.warning("GetPractices API error: %s", msg)
                return []

        sec_el = result.find(f"{{{NS_KAREO}}}SecurityResponse")
        if sec_el is not None:
            authenticated = sec_el.findtext(f"{{{NS_KAREO}}}Authenticated", "").strip().lower()
            if authenticated != "true":
                logger.warning("GetPractices: authentication failed")
                return []

    practices = []
    for p in root.findall(f".//{{{NS_KAREO}}}PracticeData"):
        pid = p.findtext(f"{{{NS_KAREO}}}ID", "").strip()
        name = p.findtext(f"{{{NS_KAREO}}}Name", "").strip()
        if pid:
            practices.append({"ID": pid, "Name": name})

    if practices:
        for p in practices:
            logger.info("Tebra practice available: ID=%s Name=%s", p["ID"], p["Name"])
    else:
        logger.warning("GetPractices returned no practices. Raw snippet: %s", resp.text[:500])

    return practices


# ---------------------------------------------------------------------------
# SOAP request builder
# ---------------------------------------------------------------------------

def _xml_structure_only(xml_text: str, max_chars: int = 1000) -> str:
    """Return XML with all text node content stripped — only tags and attributes remain."""
    stripped = re.sub(r'>([^<]+)<', '><', xml_text)
    return stripped[:max_chars]


def _build_soap_envelope(customer_key: str, username: str, password: str,
                          start_date: str, end_date: str,
                          practice_name: str = "") -> str:
    """
    Build the SOAP XML envelope for the GetCharges request.

    Only non-PII fields are requested. Patient names, DOBs, addresses, etc.
    are deliberately excluded from the Fields specification.

    Fixes applied:
      - Bug 1: correct xmlns (no /2.1)
      - Bug 3: FromServiceDate/ToServiceDate (not ServiceStartDate/ServiceEndDate)
      - Bug 4: no <Paging> element (API returns all records in one response)
      - Bug 5: field flags directly in <Fields>, no <ChargeFields> wrapper
      - Bug 6: no <PracticeID> in Filter (not valid in ChargeFilter schema)
      - RequestHeader xs:sequence order: ClientVersion → CustomerKey → Password → User
      - ChargeFilter xs:sequence order: FromServiceDate → PracticeName → ToServiceDate
    """
    # ChargeFilter xs:sequence order per WSDL: FromServiceDate, then PracticeName, then ToServiceDate
    practice_name_el = f"          <kar:PracticeName>{practice_name}</kar:PracticeName>\n" if practice_name else ""
    envelope = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope
    xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
    xmlns:kar="http://www.kareo.com/api/schemas/">
  <soap:Header/>
  <soap:Body>
    <kar:GetCharges>
      <kar:request>
        <kar:RequestHeader>
          <kar:ClientVersion>{CLIENT_VERSION}</kar:ClientVersion>
          <kar:CustomerKey>{customer_key}</kar:CustomerKey>
          <kar:Password>{password}</kar:Password>
          <kar:User>{username}</kar:User>
        </kar:RequestHeader>
        <kar:Fields>
          <kar:ServiceStartDate>true</kar:ServiceStartDate>
          <kar:RenderingProviderName>true</kar:RenderingProviderName>
          <kar:TotalCharges>true</kar:TotalCharges>
          <kar:PrimaryInsuranceInsuranceContractAdjustment>true</kar:PrimaryInsuranceInsuranceContractAdjustment>
          <kar:SecondaryInsuranceInsuranceContractAdjustment>true</kar:SecondaryInsuranceInsuranceContractAdjustment>
          <kar:TertiaryInsuranceInsuranceContractAdjustment>true</kar:TertiaryInsuranceInsuranceContractAdjustment>
          <kar:PrimaryInsuranceInsurancePayment>true</kar:PrimaryInsuranceInsurancePayment>
          <kar:SecondaryInsuranceInsurancePayment>true</kar:SecondaryInsuranceInsurancePayment>
          <kar:TertiaryInsuranceInsurancePayment>true</kar:TertiaryInsuranceInsurancePayment>
          <kar:PatientPaymentAmount>true</kar:PatientPaymentAmount>
          <kar:OtherAdjustment>true</kar:OtherAdjustment>
          <kar:ProcedureCode>true</kar:ProcedureCode>
          <kar:ProcedureModifier1>true</kar:ProcedureModifier1>
          <kar:ProcedureModifier2>true</kar:ProcedureModifier2>
          <kar:ProcedureModifier3>true</kar:ProcedureModifier3>
          <kar:ProcedureModifier4>true</kar:ProcedureModifier4>
          <kar:EncounterID>true</kar:EncounterID>
          <kar:PatientID>true</kar:PatientID>
        </kar:Fields>
        <kar:Filter>
          <kar:FromServiceDate>{start_date}</kar:FromServiceDate>
{practice_name_el}          <kar:ToServiceDate>{end_date}</kar:ToServiceDate>
        </kar:Filter>
      </kar:request>
    </kar:GetCharges>
  </soap:Body>
</soap:Envelope>"""
    return envelope


# ---------------------------------------------------------------------------
# Response parser
# ---------------------------------------------------------------------------

def _parse_text(element, tag: str, ns: str = NS_KAREO) -> str:
    """Safely extract text from a child element."""
    child = element.find(f"{{{ns}}}{tag}")
    if child is not None and child.text:
        return child.text.strip()
    return ""


def _normalize_service_date(raw: str) -> str:
    """
    Convert a date string from the Tebra API to M/D/YYYY format (no leading
    zeros), matching the Charges Export CSV convention.

    Handles ISO timestamps (2023-01-15T00:00:00), zero-padded (01/15/2023),
    and already-normalized (1/15/2023) inputs.  Returns the raw string
    unchanged if parsing fails.
    """
    if not raw:
        return raw
    try:
        dt = pd.to_datetime(raw, errors="raise")
        return f"{dt.month}/{dt.day}/{dt.year}"
    except Exception:
        return raw


def _parse_charges_response(xml_text: str) -> list[dict]:
    """
    Parse the GetCharges SOAP response XML.

    Returns a list of dicts (one per charge).
    Response structure: GetChargesResult > ErrorResponse + SecurityResponse + Charges

    PII SAFETY: Patient ID and Encounter ID are hashed before inclusion.
    """
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        logger.warning("Failed to parse SOAP response XML: %s", exc)
        return []

    body = root.find(f"{{{NS_ENVELOPE}}}Body")
    if body is None:
        logger.warning("SOAP response has no Body element")
        return []

    result = body.find(f".//{{{NS_KAREO}}}GetChargesResult")
    if result is None:
        body_tags = [child.tag for child in body]
        fault_el = body.find(f"{{{NS_ENVELOPE}}}Fault")
        if fault_el is not None:
            faultcode = fault_el.findtext("faultcode") or fault_el.findtext(f"{{{NS_ENVELOPE}}}Code") or ""
            faultstring = fault_el.findtext("faultstring") or fault_el.findtext(f"{{{NS_ENVELOPE}}}Reason") or ""
            detail_el = fault_el.find("detail")
            detail = ET.tostring(detail_el, encoding="unicode") if detail_el is not None else ""
            logger.warning(
                "Tebra SOAP Fault: faultcode=%r faultstring=%r detail=%s",
                faultcode, faultstring, _xml_structure_only(detail, 500),
            )
        else:
            logger.warning(
                "GetChargesResult not found in SOAP response. Body child tags: %s", body_tags
            )
        return []

    # Bug 7 fix: check ErrorResponse.IsError (no TotalCount in this API)
    error_el = result.find(f"{{{NS_KAREO}}}ErrorResponse")
    if error_el is not None:
        is_error = error_el.findtext(f"{{{NS_KAREO}}}IsError", "").strip().lower()
        if is_error == "true":
            msg = error_el.findtext(f"{{{NS_KAREO}}}ErrorMessage", "").strip()
            logger.warning("Tebra API error: %s", msg)
            return []

    # Check SecurityResponse.Authenticated
    sec_el = result.find(f"{{{NS_KAREO}}}SecurityResponse")
    if sec_el is not None:
        authenticated = sec_el.findtext(f"{{{NS_KAREO}}}Authenticated", "").strip().lower()
        if authenticated != "true":
            logger.warning("Tebra API: authentication failed")
            return []

    charges_el = result.find(f"{{{NS_KAREO}}}Charges")
    if charges_el is None:
        logger.warning("Charges element not found in GetChargesResult")
        return []

    charge_count = len(charges_el.findall(f"{{{NS_KAREO}}}ChargeData"))
    logger.info("Tebra: %d ChargeData elements in response", charge_count)

    rows = []
    for charge in charges_el.findall(f"{{{NS_KAREO}}}ChargeData"):
        raw_patient_id = _parse_text(charge, "PatientID")
        raw_encounter_id = _parse_text(charge, "EncounterID")

        # Hash PII immediately — never store or log raw values
        try:
            hashed_patient_id = hash_patient_id(raw_patient_id) if raw_patient_id else ""
            hashed_encounter_id = hash_encounter_id(raw_encounter_id) if raw_encounter_id else ""
        except Exception:
            hashed_patient_id = ""
            hashed_encounter_id = ""

        proc_code = _parse_text(charge, "ProcedureCode")
        modifiers = [_parse_text(charge, f"ProcedureModifier{i}") for i in range(1, 5)]
        modifiers = [m for m in modifiers if m]
        proc_with_mods = (proc_code + " " + " ".join(modifiers)).strip() if modifiers else proc_code

        row = {
            "Date Of Service":                           _normalize_service_date(_parse_text(charge, "ServiceStartDate")),
            "Rendering Provider":                        _parse_text(charge, "RenderingProviderName"),
            "Service Charge Amount":                     _parse_text(charge, "TotalCharges"),
            "Pri Ins Insurance Contract Adjustment":     _parse_text(charge, "PrimaryInsuranceInsuranceContractAdjustment"),
            "Sec Ins Insurance Contract Adjustment":     _parse_text(charge, "SecondaryInsuranceInsuranceContractAdjustment"),
            "Other Ins Insurance Contract Adjustment":   _parse_text(charge, "TertiaryInsuranceInsuranceContractAdjustment"),
            "Pri Ins Insurance Payment":                 _parse_text(charge, "PrimaryInsuranceInsurancePayment"),
            "Sec Ins Insurance Payment":                 _parse_text(charge, "SecondaryInsuranceInsurancePayment"),
            "Other Ins Insurance Payment":               _parse_text(charge, "TertiaryInsuranceInsurancePayment"),
            "Pat Payment Amount":                        _parse_text(charge, "PatientPaymentAmount"),
            "Other Adjustment":                          _parse_text(charge, "OtherAdjustment"),
            "Procedure Code":                            proc_code,
            "Procedure Codes with Modifiers":            proc_with_mods,
            "Encounter ID":                              hashed_encounter_id,
            "Patient ID":                                hashed_patient_id,
        }
        rows.append(row)

    return rows


# ---------------------------------------------------------------------------
# Main sync function
# ---------------------------------------------------------------------------

_CHUNK_DAYS = 55          # stay safely under the API's 60-day limit
_RATE_LIMIT_DELAY = 1.1  # seconds between chunk requests


def _fetch_chunk(
    customer_key: str,
    username: str,
    password: str,
    chunk_start: date,
    chunk_end: date,
    practice_name: str,
) -> list[dict]:
    """Make a single GetCharges API call for a ≤55-day window."""
    start_str = chunk_start.strftime("%m/%d/%Y")
    end_str = chunk_end.strftime("%m/%d/%Y")
    logger.info("Tebra: fetching chunk %s to %s", start_str, end_str)

    envelope = _build_soap_envelope(
        customer_key=customer_key,
        username=username,
        password=password,
        start_date=start_str,
        end_date=end_str,
        practice_name=practice_name,
    )

    logger.info(
        "Tebra GetCharges RequestHeader: ClientVersion=%s CustomerKey=%s User=%s Password=*** PracticeName=%s",
        CLIENT_VERSION, customer_key, username, practice_name,
    )

    try:
        response = requests.post(
            SOAP_ENDPOINT,
            data=envelope.encode("utf-8"),
            headers={
                "Content-Type": "text/xml; charset=utf-8",
                "SOAPAction": f'"{SOAP_ACTION_GET_CHARGES}"',
            },
            timeout=60,
        )
    except requests.RequestException as exc:
        logger.warning("Tebra HTTP request failed for chunk %s-%s: %s", start_str, end_str, exc)
        return []

    if response.status_code != 200:
        logger.warning(
            "Tebra non-200 response (status %d) for chunk %s-%s",
            response.status_code, start_str, end_str,
        )
        return []

    rows = _parse_charges_response(response.text)
    logger.info("Tebra: chunk %s to %s → %d records", start_str, end_str, len(rows))
    return rows


def fetch_charges(start_date: date, end_date: date) -> pd.DataFrame:
    """
    Fetch charge records from the Tebra API for the given date range.

    Automatically splits the range into ≤55-day chunks to respect the API's
    60-day limit, with a 1.1-second delay between requests.

    Args:
        start_date: First date of service to include (inclusive).
        end_date:   Last date of service to include (inclusive).

    Returns:
        DataFrame with OUTPUT_COLUMNS, ready to merge with Charges Export data.
        Returns an empty DataFrame on failure (logs the error).

    Raises:
        RuntimeError: If required env vars (TEBRAKEY, TEBRA_USER, TEBRA_PASSWORD) are missing.
        TEBRA_PRACTICE_ID is optional (defaults to 100713) and is logged for diagnostics.
    """
    customer_key = os.environ.get("TEBRAKEY", "")
    username = os.environ.get("TEBRA_USER", "")
    password = os.environ.get("TEBRA_PASSWORD", "")
    practice_id = os.environ.get("TEBRA_PRACTICE_ID", "100713")
    practice_name = "Jenks Family Medicine, PLLC"

    if not customer_key:
        raise RuntimeError("TEBRAKEY env var is not set.")
    if not username or not password:
        raise RuntimeError("TEBRA_USER and TEBRA_PASSWORD env vars are required.")

    logger.info(
        "Tebra charge sync: %s to %s (PracticeID=%s PracticeName=%s)",
        start_date.strftime("%m/%d/%Y"), end_date.strftime("%m/%d/%Y"),
        practice_id, practice_name,
    )

    # Split the full range into ≤55-day chunks
    all_rows: list[dict] = []
    chunk_start = start_date
    first_chunk = True

    while chunk_start <= end_date:
        chunk_end = min(chunk_start + timedelta(days=_CHUNK_DAYS - 1), end_date)

        if not first_chunk:
            time.sleep(_RATE_LIMIT_DELAY)
        first_chunk = False

        rows = _fetch_chunk(customer_key, username, password, chunk_start, chunk_end, practice_name)
        all_rows.extend(rows)
        chunk_start = chunk_end + timedelta(days=1)

    if not all_rows:
        logger.warning(
            "Tebra sync returned 0 records for %s to %s",
            start_date.strftime("%m/%d/%Y"), end_date.strftime("%m/%d/%Y"),
        )
        get_practice_ids(customer_key, username, password)
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    df = pd.DataFrame(all_rows, columns=OUTPUT_COLUMNS)

    # Deduplicate in case chunk boundaries overlap
    before = len(df)
    df = df.drop_duplicates()
    if len(df) < before:
        logger.info("Tebra: dropped %d duplicate rows after chunk merge", before - len(df))

    logger.info("Tebra sync complete: %d records fetched", len(df))
    return df


def fetch_charges_incremental(last_sync_date: date | None = None) -> pd.DataFrame:
    """
    Convenience wrapper for incremental sync.

    Pulls from last_sync_date through yesterday. When last_sync_date is None
    (initial sync), falls back to TEBRA_LOOKBACK_DAYS env var (default 365)
    to capture a full year of historical data.
    """
    today = date.today()
    end_date = today - timedelta(days=1)  # don't pull today's partial data

    if last_sync_date:
        start_date = last_sync_date
    else:
        lookback_days = int(os.environ.get("TEBRA_LOOKBACK_DAYS", "1280"))
        start_date = today - timedelta(days=lookback_days)
        logger.info("Tebra: initial sync, looking back %d days to %s", lookback_days, start_date)

    if start_date > end_date:
        logger.warning("Tebra: nothing to sync (start_date %s > end_date %s)", start_date, end_date)
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    return fetch_charges(start_date, end_date)
