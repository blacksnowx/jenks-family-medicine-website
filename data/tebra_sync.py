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
  TEBRAKEY       — CustomerKey for the Tebra/Kareo API
  TEBRA_USER     — API username (practice admin account)
  TEBRA_PASSWORD — API password
  PII_HASH_SECRET — Secret for deterministic PII hashing (see pii_utils.py)
"""

import logging
import os
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
    envelope = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope
    xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
    xmlns:kar="http://www.kareo.com/api/schemas/">
  <soap:Header/>
  <soap:Body>
    <kar:GetPractices>
      <kar:request>
        <kar:RequestHeader>
          <kar:CustomerKey>{customer_key}</kar:CustomerKey>
          <kar:User>{username}</kar:User>
          <kar:Password>{password}</kar:Password>
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
            logger.warning("TEBRA practice available: ID=%s Name=%s", p["ID"], p["Name"])
    else:
        logger.warning("GetPractices returned no practices. Raw snippet: %s", resp.text[:500])

    return practices


# ---------------------------------------------------------------------------
# SOAP request builder
# ---------------------------------------------------------------------------

def _build_soap_envelope(customer_key: str, username: str, password: str,
                          start_date: str, end_date: str) -> str:
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
    """
    envelope = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope
    xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
    xmlns:kar="http://www.kareo.com/api/schemas/">
  <soap:Header/>
  <soap:Body>
    <kar:GetCharges>
      <kar:request>
        <kar:RequestHeader>
          <kar:CustomerKey>{customer_key}</kar:CustomerKey>
          <kar:User>{username}</kar:User>
          <kar:Password>{password}</kar:Password>
        </kar:RequestHeader>
        <kar:Filter>
          <kar:FromServiceDate>{start_date}</kar:FromServiceDate>
          <kar:ToServiceDate>{end_date}</kar:ToServiceDate>
        </kar:Filter>
        <kar:Fields>
          <kar:ServiceDate>true</kar:ServiceDate>
          <kar:RenderingProvider>true</kar:RenderingProvider>
          <kar:ServiceAmount>true</kar:ServiceAmount>
          <kar:PrimaryInsuranceContractAdjustment>true</kar:PrimaryInsuranceContractAdjustment>
          <kar:SecondaryInsuranceContractAdjustment>true</kar:SecondaryInsuranceContractAdjustment>
          <kar:OtherInsuranceContractAdjustment>true</kar:OtherInsuranceContractAdjustment>
          <kar:PrimaryInsurancePayment>true</kar:PrimaryInsurancePayment>
          <kar:SecondaryInsurancePayment>true</kar:SecondaryInsurancePayment>
          <kar:OtherInsurancePayment>true</kar:OtherInsurancePayment>
          <kar:PatientPaymentAmount>true</kar:PatientPaymentAmount>
          <kar:OtherAdjustment>true</kar:OtherAdjustment>
          <kar:ProcedureCode>true</kar:ProcedureCode>
          <kar:ProcedureCodeWithModifiers>true</kar:ProcedureCodeWithModifiers>
          <kar:EncounterID>true</kar:EncounterID>
          <kar:PatientID>true</kar:PatientID>
        </kar:Fields>
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
        logger.warning(
            "GetChargesResult not found in SOAP response. Body child tags: %s",
            [child.tag for child in body],
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

        row = {
            "Date Of Service":                           _parse_text(charge, "ServiceDate"),
            "Rendering Provider":                        _parse_text(charge, "RenderingProvider"),
            "Service Charge Amount":                     _parse_text(charge, "ServiceAmount"),
            "Pri Ins Insurance Contract Adjustment":     _parse_text(charge, "PrimaryInsuranceContractAdjustment"),
            "Sec Ins Insurance Contract Adjustment":     _parse_text(charge, "SecondaryInsuranceContractAdjustment"),
            "Other Ins Insurance Contract Adjustment":   _parse_text(charge, "OtherInsuranceContractAdjustment"),
            "Pri Ins Insurance Payment":                 _parse_text(charge, "PrimaryInsurancePayment"),
            "Sec Ins Insurance Payment":                 _parse_text(charge, "SecondaryInsurancePayment"),
            "Other Ins Insurance Payment":               _parse_text(charge, "OtherInsurancePayment"),
            "Pat Payment Amount":                        _parse_text(charge, "PatientPaymentAmount"),
            "Other Adjustment":                          _parse_text(charge, "OtherAdjustment"),
            "Procedure Code":                            _parse_text(charge, "ProcedureCode"),
            "Procedure Codes with Modifiers":            _parse_text(charge, "ProcedureCodeWithModifiers"),
            "Encounter ID":                              hashed_encounter_id,
            "Patient ID":                                hashed_patient_id,
        }
        rows.append(row)

    return rows


# ---------------------------------------------------------------------------
# Main sync function
# ---------------------------------------------------------------------------

def fetch_charges(start_date: date, end_date: date) -> pd.DataFrame:
    """
    Fetch charge records from the Tebra API for the given date range.

    Args:
        start_date: First date of service to include (inclusive).
        end_date:   Last date of service to include (inclusive).

    Returns:
        DataFrame with OUTPUT_COLUMNS, ready to merge with Charges Export data.
        Returns an empty DataFrame on failure (logs the error).

    Raises:
        RuntimeError: If required env vars (TEBRAKEY, TEBRA_USER, TEBRA_PASSWORD) are missing.
    """
    customer_key = os.environ.get("TEBRAKEY", "")
    username = os.environ.get("TEBRA_USER", "")
    password = os.environ.get("TEBRA_PASSWORD", "")

    if not customer_key:
        raise RuntimeError("TEBRAKEY env var is not set.")
    if not username or not password:
        raise RuntimeError("TEBRA_USER and TEBRA_PASSWORD env vars are required.")

    start_str = start_date.strftime("%m/%d/%Y")
    end_str = end_date.strftime("%m/%d/%Y")

    logger.warning("Starting Tebra charge sync: %s to %s", start_str, end_str)

    # Bug 4 fix: single request — no pagination loop
    envelope = _build_soap_envelope(
        customer_key=customer_key,
        username=username,
        password=password,
        start_date=start_str,
        end_date=end_str,
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
        logger.warning("HTTP request to Tebra API failed: %s", exc)
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    logger.warning(
        "Tebra API response: status=%d content_length=%d",
        response.status_code, len(response.content),
    )

    if response.status_code != 200:
        snippet = response.text[:2000] if response.text else "(empty)"
        logger.warning(
            "Tebra API non-200 response (status %d). Body snippet: %s",
            response.status_code, snippet,
        )
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    rows = _parse_charges_response(response.text)

    if not rows:
        logger.warning("Tebra sync returned 0 records for %s to %s", start_str, end_str)
        # Run practice discovery to help diagnose auth/config issues
        get_practice_ids(customer_key, username, password)
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    df = pd.DataFrame(rows, columns=OUTPUT_COLUMNS)
    logger.warning("Tebra sync complete: %d records fetched", len(df))
    return df


def fetch_charges_incremental(last_sync_date: date | None = None) -> pd.DataFrame:
    """
    Convenience wrapper for incremental sync.

    Pulls from last_sync_date (or 90 days ago if None) through yesterday.
    """
    today = date.today()
    end_date = today - timedelta(days=1)  # don't pull today's partial data

    if last_sync_date:
        start_date = last_sync_date
    else:
        start_date = today - timedelta(days=90)

    if start_date > end_date:
        logger.warning("Tebra: nothing to sync (start_date %s > end_date %s)", start_date, end_date)
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    return fetch_charges(start_date, end_date)
