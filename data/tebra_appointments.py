"""
Tebra (Kareo) SOAP API integration for appointment scheduling.

Provides functions to query available appointment slots and create
tentative appointments for new-patient online booking.

PII RULES:
  - Patient names and contact info are only sent to CreateAppointment (required).
  - Appointment queries return only start/end times and status (no patient data).
  - Nothing is logged that constitutes PHI.

Required env vars (same as tebra_sync.py):
  TEBRAKEY          — CustomerKey for the Tebra/Kareo API
  TEBRA_USER        — API username (practice admin account)
  TEBRA_PASSWORD    — API password

Optional env vars:
  TEBRA_PRACTICE_ID — Practice ID (default: 100713)
"""

import logging
import os
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta

import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants  (same pattern as tebra_sync.py)
# ---------------------------------------------------------------------------

SOAP_ENDPOINT = "https://webservice.kareo.com/services/soap/2.1/KareoServices.svc"

SOAP_ACTION_GET_APPOINTMENTS      = "http://www.kareo.com/api/schemas/KareoServices/GetAppointments"
SOAP_ACTION_CREATE_APPOINTMENT    = "http://www.kareo.com/api/schemas/KareoServices/CreateAppointment"
SOAP_ACTION_GET_APPOINTMENT_REASONS = "http://www.kareo.com/api/schemas/KareoServices/GetAppointmentReasons"

# Correct namespace — no /2.1 suffix
NS_ENVELOPE = "http://schemas.xmlsoap.org/soap/envelope/"
NS_KAREO    = "http://www.kareo.com/api/schemas/"

# RequestHeader ClientVersion (same as tebra_sync.py)
CLIENT_VERSION = "4.0"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_credentials() -> tuple[str, str, str]:
    """
    Return (customer_key, username, password) from env vars.
    Raises RuntimeError if any are missing.
    """
    customer_key = os.environ.get("TEBRAKEY", "")
    username     = os.environ.get("TEBRA_USER", "")
    password     = os.environ.get("TEBRA_PASSWORD", "")
    if not customer_key:
        raise RuntimeError("TEBRAKEY env var is not set.")
    if not username or not password:
        raise RuntimeError("TEBRA_USER and TEBRA_PASSWORD env vars are required.")
    return customer_key, username, password


def _request_header_xml(customer_key: str, username: str, password: str) -> str:
    """Build the RequestHeader XML block.
    xs:sequence order: ClientVersion → CustomerKey → Password → User
    """
    return (
        f"        <kar:RequestHeader>\n"
        f"          <kar:ClientVersion>{CLIENT_VERSION}</kar:ClientVersion>\n"
        f"          <kar:CustomerKey>{customer_key}</kar:CustomerKey>\n"
        f"          <kar:Password>{password}</kar:Password>\n"
        f"          <kar:User>{username}</kar:User>\n"
        f"        </kar:RequestHeader>"
    )


def _parse_text(element, tag: str, ns: str = NS_KAREO) -> str:
    """Safely extract text from a child element."""
    child = element.find(f"{{{ns}}}{tag}")
    if child is not None and child.text:
        return child.text.strip()
    return ""


def _check_error_and_auth(result_el) -> str | None:
    """
    Check ErrorResponse and SecurityResponse elements.
    Returns an error message string if there is a problem, else None.
    """
    error_el = result_el.find(f"{{{NS_KAREO}}}ErrorResponse")
    if error_el is not None:
        is_error = error_el.findtext(f"{{{NS_KAREO}}}IsError", "").strip().lower()
        if is_error == "true":
            msg = error_el.findtext(f"{{{NS_KAREO}}}ErrorMessage", "unknown API error").strip()
            return msg

    sec_el = result_el.find(f"{{{NS_KAREO}}}SecurityResponse")
    if sec_el is not None:
        authenticated = sec_el.findtext(f"{{{NS_KAREO}}}Authenticated", "").strip().lower()
        if authenticated != "true":
            return "Authentication failed"

    return None


def _post_soap(action: str, envelope: str) -> ET.Element | None:
    """
    POST a SOAP envelope and return the parsed root XML element.
    Returns None and logs a warning on any error.
    """
    try:
        resp = requests.post(
            SOAP_ENDPOINT,
            data=envelope.encode("utf-8"),
            headers={
                "Content-Type": "text/xml; charset=utf-8",
                "SOAPAction": f'"{action}"',
            },
            timeout=30,
        )
    except requests.RequestException as exc:
        logger.warning("Tebra SOAP request failed (%s): %s", action, exc)
        print(f"[TEBRA DEBUG] SOAP request exception for {action}: {exc}")
        return None

    logger.info("Tebra HTTP status %d for %s", resp.status_code, action)
    print(f"[TEBRA DEBUG] {action} HTTP status={resp.status_code} len={len(resp.text)} body[:200]={resp.text[:200]!r}")
    if resp.status_code != 200:
        logger.warning(
            "Tebra non-200 response for %s; body prefix: %.200s",
            action, resp.text,
        )
        return None

    try:
        root = ET.fromstring(resp.text)
        print(f"[TEBRA DEBUG] {action} parsed OK; root tag={root.tag}")
        return root
    except ET.ParseError as exc:
        logger.warning(
            "Tebra XML parse error for %s: %s; body prefix: %.200s",
            action, exc, resp.text,
        )
        print(f"[TEBRA DEBUG] {action} XML parse error: {exc}")
        return None


# ---------------------------------------------------------------------------
# Public API functions
# ---------------------------------------------------------------------------

def get_appointments(provider_name: str, start_date: date, end_date: date) -> list[dict]:
    """
    GetAppointments filtered by ResourceName and date range.

    Returns a list of dicts with keys: start, end, status.
    NO patient data is requested or returned.

    AppointmentFilter xs:sequence (alphabetical from XSD):
      AppointmentReason, ConfirmationStatus, EndDate, FromCreatedDate,
      FromLastModifiedDate, PatientCasePayerScenario, PatientFullName,
      PatientID, PracticeName, ResourceName, ServiceLocationName, StartDate,
      TimeZoneOffsetFromGMT, ToCreatedDate, ToLastModifiedDate, Type
    """
    customer_key, username, password = _get_credentials()

    start_str = start_date.strftime("%m/%d/%Y")
    end_str   = end_date.strftime("%m/%d/%Y")

    logger.info(
        "Tebra GetAppointments: provider=%s start=%s end=%s",
        provider_name, start_str, end_str,
    )

    envelope = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope
    xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
    xmlns:kar="http://www.kareo.com/api/schemas/">
  <soap:Header/>
  <soap:Body>
    <kar:GetAppointments>
      <kar:request>
{_request_header_xml(customer_key, username, password)}
        <kar:Fields>
          <kar:ConfirmationStatus>true</kar:ConfirmationStatus>
          <kar:EndDate>true</kar:EndDate>
          <kar:ID>true</kar:ID>
          <kar:StartDate>true</kar:StartDate>
        </kar:Fields>
        <kar:Filter>
          <kar:EndDate>{end_str}</kar:EndDate>
          <kar:ResourceName>{provider_name}</kar:ResourceName>
          <kar:StartDate>{start_str}</kar:StartDate>
        </kar:Filter>
      </kar:request>
    </kar:GetAppointments>
  </soap:Body>
</soap:Envelope>"""

    root = _post_soap(SOAP_ACTION_GET_APPOINTMENTS, envelope)
    if root is None:
        return []

    result_el = root.find(f".//{{{NS_KAREO}}}GetAppointmentsResult")
    if result_el is None:
        logger.warning("GetAppointmentsResult not found in response")
        print(f"[TEBRA DEBUG] GetAppointmentsResult not found; all tags: {[el.tag for el in root.iter()][:20]}")
        return []

    err = _check_error_and_auth(result_el)
    if err:
        logger.warning("GetAppointments error: %s", err)
        return []

    appointments = []
    for appt in root.findall(f".//{{{NS_KAREO}}}AppointmentData"):
        start_raw  = _parse_text(appt, "StartDate")
        end_raw    = _parse_text(appt, "EndDate")
        status_raw = _parse_text(appt, "ConfirmationStatus")

        try:
            start_dt = datetime.fromisoformat(start_raw) if start_raw else None
            end_dt   = datetime.fromisoformat(end_raw)   if end_raw   else None
        except ValueError:
            # Try pandas-style parsing as fallback
            try:
                import pandas as pd
                start_dt = pd.to_datetime(start_raw).to_pydatetime() if start_raw else None
                end_dt   = pd.to_datetime(end_raw).to_pydatetime()   if end_raw   else None
            except Exception:
                start_dt = None
                end_dt   = None

        if start_dt and end_dt:
            appointments.append({
                "start":  start_dt,
                "end":    end_dt,
                "status": status_raw,
            })

    logger.info("GetAppointments returned %d appointments", len(appointments))
    return appointments


def get_appointment_reasons() -> list[dict]:
    """
    GetAppointmentReasons.

    Returns a list of dicts with keys: id, name, duration_minutes.
    """
    customer_key, username, password = _get_credentials()

    logger.info("Tebra GetAppointmentReasons")

    envelope = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope
    xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
    xmlns:kar="http://www.kareo.com/api/schemas/">
  <soap:Header/>
  <soap:Body>
    <kar:GetAppointmentReasons>
      <kar:request>
{_request_header_xml(customer_key, username, password)}
        <kar:Fields>
          <kar:Duration>true</kar:Duration>
          <kar:ID>true</kar:ID>
          <kar:Name>true</kar:Name>
        </kar:Fields>
      </kar:request>
    </kar:GetAppointmentReasons>
  </soap:Body>
</soap:Envelope>"""

    root = _post_soap(SOAP_ACTION_GET_APPOINTMENT_REASONS, envelope)
    if root is None:
        return []

    result_el = root.find(f".//{{{NS_KAREO}}}GetAppointmentReasonsResult")
    if result_el is None:
        logger.warning("GetAppointmentReasonsResult not found in response")
        return []

    err = _check_error_and_auth(result_el)
    if err:
        logger.warning("GetAppointmentReasons error: %s", err)
        return []

    reasons = []
    for reason in root.findall(f".//{{{NS_KAREO}}}AppointmentReasonData"):
        reason_id   = _parse_text(reason, "ID")
        name        = _parse_text(reason, "Name")
        duration_raw = _parse_text(reason, "Duration")

        try:
            duration_minutes = int(duration_raw)
        except (ValueError, TypeError):
            duration_minutes = 30  # sensible default

        if reason_id:
            reasons.append({
                "id":               reason_id,
                "name":             name,
                "duration_minutes": duration_minutes,
            })

    logger.info("GetAppointmentReasons returned %d reasons", len(reasons))
    return reasons


def calculate_available_slots(
    provider_name: str,
    target_date: date,
    start_hour: int = 8,
    end_hour: int = 17,
    slot_minutes: int = 30,
    start_minute: int = 0,
    end_minute: int = 0,
    break_start_hour: int = None,
    break_end_hour: int = None,
) -> list[dict]:
    """
    Calculate available appointment slots for a provider on a given day.

    Fetches existing appointments (gracefully handles API failures), then
    removes occupied time from the provider's working hours, returning
    a list of free slots.

    Returns a list of dicts: {start: datetime, end: datetime, label: str}
    """
    try:
        existing = get_appointments(provider_name, target_date, target_date)
    except Exception as exc:
        logger.warning("Tebra appointment fetch failed (non-fatal): %s", exc)
        existing = []

    # Build a set of occupied (start, end) intervals
    occupied: list[tuple[datetime, datetime]] = [
        (a["start"], a["end"])
        for a in existing
        if a["status"] not in ("Cancelled", "No Show")
    ]

    # Add break time as occupied if configured
    if break_start_hour is not None and break_end_hour is not None:
        break_start = datetime(target_date.year, target_date.month, target_date.day, break_start_hour, 0)
        break_end = datetime(target_date.year, target_date.month, target_date.day, break_end_hour, 0)
        occupied.append((break_start, break_end))

    # Generate all candidate slots for the day
    slot_start = datetime(target_date.year, target_date.month, target_date.day, start_hour, start_minute)
    day_end    = datetime(target_date.year, target_date.month, target_date.day, end_hour, end_minute)
    delta      = timedelta(minutes=slot_minutes)

    available = []
    current = slot_start
    while current + delta <= day_end:
        slot_end = current + delta

        # Check overlap with any occupied interval
        is_free = True
        for occ_start, occ_end in occupied:
            # Overlap if current < occ_end AND slot_end > occ_start
            # Strip timezone info for comparison if present
            s = occ_start.replace(tzinfo=None) if occ_start.tzinfo else occ_start
            e = occ_end.replace(tzinfo=None)   if occ_end.tzinfo   else occ_end
            if current < e and slot_end > s:
                is_free = False
                break

        if is_free:
            label = current.strftime("%-I:%M %p")
            available.append({
                "start": current,
                "end":   slot_end,
                "label": label,
            })

        current = current + delta

    logger.info(
        "calculate_available_slots: %s %s → %d available / %d occupied",
        provider_name, target_date, len(available), len(occupied),
    )
    return available


def create_tentative_appointment(
    provider_name: str,
    start_time: datetime,
    end_time: datetime,
    reason_id: str,
    patient_name: str,
    patient_phone: str,
    patient_email: str,
    notes: str = "",
) -> str | None:
    """
    CreateAppointment with AppointmentStatus=Tentative.

    AppointmentCreate xs:sequence (alphabetical from XSD):
      AppointmentId, AppointmentMode, AppointmentName, AppointmentReasonId,
      AppointmentStatus, AppointmentType, AppointmentUUID, AttendeesCount,
      CreatedAt, CreatedBy, CustomerId, EndTime, ForRecare,
      InsurancePolicyAuthorizationId, IsDeleted, IsGroupAppointment,
      IsRecurring, MaxAttendees, Notes, OccurrenceId, PatientCaseId,
      PatientSummaries, PatientSummary, PracticeId, ProviderId,
      RecurrenceRule, ResourceId, ResourceIds, ServiceLocationId, StartTime,
      UpdatedAt, UpdatedBy, WasCreatedOnline

    Returns the new appointment ID string, or None on failure.
    """
    customer_key, username, password = _get_credentials()

    # Format datetimes as ISO 8601 (required by Kareo)
    start_iso = start_time.strftime("%Y-%m-%dT%H:%M:%S")
    end_iso   = end_time.strftime("%Y-%m-%dT%H:%M:%S")

    # Notes: include contact info since this is a tentative online booking
    full_notes = f"Online booking request. Patient: {patient_name}. Phone: {patient_phone}. Email: {patient_email}."
    if notes:
        full_notes += f" Notes: {notes}"

    # Escape XML special chars in user-supplied fields
    def _esc(s: str) -> str:
        return (s.replace("&", "&amp;")
                 .replace("<", "&lt;")
                 .replace(">", "&gt;")
                 .replace('"', "&quot;")
                 .replace("'", "&apos;"))

    logger.info(
        "Tebra CreateAppointment: provider=%s start=%s reason_id=%s",
        provider_name, start_iso, reason_id,
    )

    # Only send the minimal required fields.
    # AppointmentCreate xs:sequence order (alphabetical):
    envelope = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope
    xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
    xmlns:kar="http://www.kareo.com/api/schemas/">
  <soap:Header/>
  <soap:Body>
    <kar:CreateAppointment>
      <kar:request>
{_request_header_xml(customer_key, username, password)}
        <kar:Appointment>
          <kar:AppointmentName>{_esc(patient_name)}</kar:AppointmentName>
          <kar:AppointmentReasonId>{_esc(str(reason_id))}</kar:AppointmentReasonId>
          <kar:AppointmentStatus>Tentative</kar:AppointmentStatus>
          <kar:EndTime>{end_iso}</kar:EndTime>
          <kar:Notes>{_esc(full_notes)}</kar:Notes>
          <kar:StartTime>{start_iso}</kar:StartTime>
          <kar:WasCreatedOnline>true</kar:WasCreatedOnline>
        </kar:Appointment>
      </kar:request>
    </kar:CreateAppointment>
  </soap:Body>
</soap:Envelope>"""

    root = _post_soap(SOAP_ACTION_CREATE_APPOINTMENT, envelope)
    if root is None:
        return None

    result_el = root.find(f".//{{{NS_KAREO}}}CreateAppointmentResult")
    if result_el is None:
        logger.warning("CreateAppointmentResult not found in response")
        print(f"[TEBRA DEBUG] CreateAppointmentResult not found; all tags: {[el.tag for el in root.iter()][:20]}")
        return None

    err = _check_error_and_auth(result_el)
    if err:
        logger.warning("CreateAppointment error: %s", err)
        return None

    # Look for the returned appointment ID
    appt_id = _parse_text(result_el, "AppointmentId")
    if not appt_id:
        # Some API versions return it inside an Appointment element
        appt_el = result_el.find(f"{{{NS_KAREO}}}Appointment")
        if appt_el is not None:
            appt_id = _parse_text(appt_el, "AppointmentId") or _parse_text(appt_el, "ID")

    if appt_id:
        logger.info("CreateAppointment succeeded: ID=%s", appt_id)
    else:
        logger.warning("CreateAppointment: could not extract appointment ID from response")

    return appt_id or None
