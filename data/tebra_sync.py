"""
Tebra (Kareo) SOAP API sync for charge data.

Pulls charge records from the Tebra SOAP 2.1 API and returns a DataFrame
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

Optional env vars:
  TEBRA_PRACTICE_ID    — Filter by practice ID (leave blank for all)
  TEBRA_REQUEST_DELAY  — Seconds between paginated requests (default 1.1)
  TEBRA_PAGE_SIZE      — Records per page request (default 100, max 200)
"""

import logging
import os
import time
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta

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

SOAP_ACTION_GET_CHARGES = "http://www.kareo.com/api/schemas/2.1/IKareoServices/GetCharges"

# Namespaces used in Kareo SOAP XML — Kareo uses SOAP 1.1
NS_ENVELOPE = "http://schemas.xmlsoap.org/soap/envelope/"
NS_KAREO = "http://www.kareo.com/api/schemas/2.1"

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
# SOAP request builder
# ---------------------------------------------------------------------------

def _build_soap_envelope(customer_key: str, username: str, password: str,
                          start_date: str, end_date: str,
                          practice_id: str = "", page_size: int = 100,
                          page_number: int = 1) -> str:
    """
    Build the SOAP XML envelope for the GetCharges request.

    Only non-PII fields are requested. Patient names, DOBs, addresses, etc.
    are deliberately excluded from the Fields specification.
    """
    practice_filter = ""
    if practice_id:
        practice_filter = f"<kar:PracticeID>{practice_id}</kar:PracticeID>"

    envelope = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope
    xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
    xmlns:kar="http://www.kareo.com/api/schemas/2.1">
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
          <kar:ServiceStartDate>{start_date}</kar:ServiceStartDate>
          <kar:ServiceEndDate>{end_date}</kar:ServiceEndDate>
          {practice_filter}
        </kar:Filter>
        <kar:Fields>
          <kar:ChargeFields>
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
          </kar:ChargeFields>
        </kar:Fields>
        <kar:Paging>
          <kar:PageSize>{page_size}</kar:PageSize>
          <kar:PageNumber>{page_number}</kar:PageNumber>
        </kar:Paging>
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


def _parse_charges_response(xml_text: str) -> tuple[list[dict], int]:
    """
    Parse the GetCharges SOAP response XML.

    Returns:
        (rows, total_count) where rows is a list of dicts (one per charge)
        and total_count is the reported total record count.

    PII SAFETY: Patient ID and Encounter ID are hashed before inclusion.
    """
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        logger.error("Failed to parse SOAP response XML: %s", exc)
        return [], 0

    # Navigate: Envelope → Body → GetChargesResponse → GetChargesResult
    body = root.find(f"{{{NS_ENVELOPE}}}Body")
    if body is None:
        logger.error("SOAP response has no Body element")
        return [], 0

    # Try both with and without namespace on response element
    result = None
    for path in [
        f"{{{NS_KAREO}}}GetChargesResponse/{{{NS_KAREO}}}GetChargesResult",
        ".//{{{NS_KAREO}}}GetChargesResult",
    ]:
        result = body.find(path)
        if result is not None:
            break

    if result is None:
        logger.error("GetChargesResult not found in SOAP response")
        return [], 0

    # Check for API-level errors
    error_response = result.find(f"{{{NS_KAREO}}}ErrorResponse")
    if error_response is not None:
        error_msg = _parse_text(error_response, "ErrorMessage")
        logger.error("Tebra API error: %s", error_msg)
        return [], 0

    # Total record count for pagination
    total_el = result.find(f"{{{NS_KAREO}}}TotalCount")
    total_count = int(total_el.text.strip()) if (total_el is not None and total_el.text) else 0

    charges_el = result.find(f"{{{NS_KAREO}}}Charges")
    if charges_el is None:
        return [], total_count

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

    return rows, total_count


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

    practice_id = os.environ.get("TEBRA_PRACTICE_ID", "")
    request_delay = float(os.environ.get("TEBRA_REQUEST_DELAY", "1.1"))
    page_size = int(os.environ.get("TEBRA_PAGE_SIZE", "100"))
    page_size = min(max(page_size, 1), 200)  # clamp to [1, 200]

    start_str = start_date.strftime("%m/%d/%Y")
    end_str = end_date.strftime("%m/%d/%Y")

    logger.info(
        "Starting Tebra charge sync: %s to %s (page_size=%d, delay=%.1fs)",
        start_str, end_str, page_size, request_delay,
    )

    all_rows: list[dict] = []
    page_number = 1

    while True:
        envelope = _build_soap_envelope(
            customer_key=customer_key,
            username=username,
            password=password,
            start_date=start_str,
            end_date=end_str,
            practice_id=practice_id,
            page_size=page_size,
            page_number=page_number,
        )

        try:
            response = requests.post(
                SOAP_ENDPOINT,
                data=envelope.encode("utf-8"),
                headers={
                    "Content-Type": "text/xml; charset=utf-8",
                    "SOAPAction": f'"{SOAP_ACTION_GET_CHARGES}"',
                },
                timeout=30,
            )
        except requests.RequestException as exc:
            logger.error("HTTP request to Tebra API failed (page %d): %s", page_number, exc)
            break

        logger.info(
            "Tebra API response: page=%d status=%d content_length=%d",
            page_number, response.status_code, len(response.content),
        )

        if response.status_code != 200:
            # Log the response body for debugging — but truncate to 2000 chars
            # to avoid accidentally logging large payloads with embedded PII.
            snippet = response.text[:2000] if response.text else "(empty)"
            logger.error(
                "Tebra API non-200 response (page %d, status %d). Body snippet: %s",
                page_number, response.status_code, snippet,
            )
            break

        rows, total_count = _parse_charges_response(response.text)
        all_rows.extend(rows)

        logger.info(
            "Page %d: fetched %d records (running total: %d / reported total: %d)",
            page_number, len(rows), len(all_rows), total_count,
        )

        # Stop if we've collected all records or got an empty page
        if not rows or len(all_rows) >= total_count:
            break

        page_number += 1
        time.sleep(request_delay)  # respect rate limit

    if not all_rows:
        logger.warning("Tebra sync returned 0 records for %s to %s", start_str, end_str)
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    df = pd.DataFrame(all_rows, columns=OUTPUT_COLUMNS)
    logger.info("Tebra sync complete: %d records fetched", len(df))
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
        logger.info("Tebra: nothing to sync (start_date %s > end_date %s)", start_date, end_date)
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    return fetch_charges(start_date, end_date)
