"""
Tests for the Tebra scheduling integration.

Routes tested:
  GET  /api/schedule/providers
  GET  /api/schedule/available
  POST /api/schedule/book

Unit tests for calculate_available_slots() are also included.
"""

import json
from datetime import date, datetime
from unittest.mock import patch

import pytest

from conftest import TEST_DATE, TEST_PROVIDER, TEST_START_TIME, TEST_END_TIME
from models import AppointmentRequest, TebraBooking


# ---------------------------------------------------------------------------
# GET /api/schedule/providers
# ---------------------------------------------------------------------------

def test_providers_returns_200(client):
    resp = client.get("/api/schedule/providers")
    assert resp.status_code == 200


def test_providers_returns_json_list(client):
    resp = client.get("/api/schedule/providers")
    data = resp.get_json()
    assert "providers" in data
    assert isinstance(data["providers"], list)


def test_providers_includes_seeded_provider(client):
    resp = client.get("/api/schedule/providers")
    providers = resp.get_json()["providers"]
    assert TEST_PROVIDER in providers


# ---------------------------------------------------------------------------
# GET /api/schedule/available
# ---------------------------------------------------------------------------

def _mock_appointments(return_value=None):
    """Return a patch context for Tebra get_appointments."""
    return patch(
        "data.tebra_appointments.get_appointments",
        return_value=return_value or [],
    )


def test_available_missing_params_returns_400(client):
    resp = client.get("/api/schedule/available")
    assert resp.status_code == 400


def test_available_missing_provider_returns_400(client):
    resp = client.get(f"/api/schedule/available?date={TEST_DATE}")
    assert resp.status_code == 400


def test_available_missing_date_returns_400(client):
    resp = client.get(f"/api/schedule/available?provider={TEST_PROVIDER}")
    assert resp.status_code == 400


def test_available_past_date_returns_400(client):
    resp = client.get(
        f"/api/schedule/available?provider={TEST_PROVIDER}&date=2020-01-01"
    )
    assert resp.status_code == 400


def test_available_invalid_date_format_returns_400(client):
    resp = client.get(
        f"/api/schedule/available?provider={TEST_PROVIDER}&date=07-01-2030"
    )
    assert resp.status_code == 400


def test_available_returns_slot_list(client):
    with _mock_appointments():
        resp = client.get(
            f"/api/schedule/available?provider={TEST_PROVIDER}&date={TEST_DATE}"
        )
    assert resp.status_code == 200
    data = resp.get_json()
    assert "slots" in data
    assert isinstance(data["slots"], list)


def test_available_slots_have_required_fields(client):
    with _mock_appointments():
        resp = client.get(
            f"/api/schedule/available?provider={TEST_PROVIDER}&date={TEST_DATE}"
        )
    slots = resp.get_json()["slots"]
    assert len(slots) > 0, "Expected at least one available slot on a weekday with no bookings"
    for slot in slots:
        assert "start" in slot, "Slot missing 'start' field"
        assert "end" in slot, "Slot missing 'end' field"
        assert "label" in slot, "Slot missing 'label' field"


def test_available_slots_dont_overlap(client):
    with _mock_appointments():
        resp = client.get(
            f"/api/schedule/available?provider={TEST_PROVIDER}&date={TEST_DATE}"
        )
    slots = resp.get_json()["slots"]
    for i in range(len(slots) - 1):
        end_of_current = slots[i]["end"]
        start_of_next = slots[i + 1]["start"]
        assert end_of_current <= start_of_next, (
            f"Slots overlap: slot {i} ends {end_of_current}, slot {i+1} starts {start_of_next}"
        )


def test_available_slots_respect_schedule_hours(client):
    """All slots must start at or after 08:00 and end at or before 17:00."""
    with _mock_appointments():
        resp = client.get(
            f"/api/schedule/available?provider={TEST_PROVIDER}&date={TEST_DATE}"
        )
    slots = resp.get_json()["slots"]
    assert len(slots) > 0
    for slot in slots:
        start_dt = datetime.fromisoformat(slot["start"])
        end_dt = datetime.fromisoformat(slot["end"])
        assert start_dt.hour >= 8, f"Slot starts before 8am: {slot['start']}"
        assert end_dt.hour < 17 or (end_dt.hour == 17 and end_dt.minute == 0), (
            f"Slot ends after 5pm: {slot['end']}"
        )


def test_available_unscheduled_provider_returns_no_slots(client):
    with _mock_appointments():
        resp = client.get(
            f"/api/schedule/available?provider=Unknown+Provider&date={TEST_DATE}"
        )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["slots"] == []


# ---------------------------------------------------------------------------
# POST /api/schedule/book
# ---------------------------------------------------------------------------

VALID_BOOK_PAYLOAD = {
    "provider": TEST_PROVIDER,
    "start_time": TEST_START_TIME,
    "end_time": TEST_END_TIME,
    "reason_id": "reason_001",
    "patient_name": "Test Patient",
    "patient_phone": "423-555-0199",
    "patient_email": "patient@example.com",
    "notes": "First visit",
}


def test_book_with_valid_data_returns_success(client):
    with patch("data.tebra_appointments.create_tentative_appointment", return_value="appt_001"):
        resp = client.post("/api/schedule/book", json=VALID_BOOK_PAYLOAD)
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["success"] is True


def test_book_with_missing_fields_returns_400(client):
    payload = {k: v for k, v in VALID_BOOK_PAYLOAD.items() if k != "patient_name"}
    resp = client.post("/api/schedule/book", json=payload)
    assert resp.status_code == 400
    assert "Missing fields" in resp.get_json().get("error", "")


def test_book_with_past_date_returns_400(client):
    payload = {
        **VALID_BOOK_PAYLOAD,
        "start_time": "2020-01-01T09:00:00",
        "end_time": "2020-01-01T09:30:00",
    }
    resp = client.post("/api/schedule/book", json=payload)
    assert resp.status_code == 400


def test_book_creates_tebra_booking_record(client, db):
    with patch("data.tebra_appointments.create_tentative_appointment", return_value="appt_002"):
        client.post("/api/schedule/book", json=VALID_BOOK_PAYLOAD)
    booking = TebraBooking.query.filter_by(patient_phone="423-555-0199").first()
    assert booking is not None
    assert booking.provider_name == TEST_PROVIDER
    assert booking.reason_id == "reason_001"


def test_book_sets_status_booked_when_tebra_succeeds(client, db):
    with patch("data.tebra_appointments.create_tentative_appointment", return_value="appt_003"):
        client.post("/api/schedule/book", json=VALID_BOOK_PAYLOAD)
    booking = TebraBooking.query.filter_by(patient_phone="423-555-0199").first()
    assert booking.status == "booked"
    assert booking.tebra_appt_id == "appt_003"


def test_book_sets_status_pending_when_tebra_unavailable(client, db):
    """When Tebra is unavailable the booking is still saved locally as 'pending'."""
    with patch(
        "data.tebra_appointments.create_tentative_appointment",
        side_effect=RuntimeError("TEBRAKEY env var is not set."),
    ):
        resp = client.post("/api/schedule/book", json=VALID_BOOK_PAYLOAD)
    assert resp.status_code == 200
    booking = TebraBooking.query.filter_by(patient_phone="423-555-0199").first()
    assert booking is not None
    assert booking.status == "pending"


# ---------------------------------------------------------------------------
# Unit tests — calculate_available_slots() slot logic
# ---------------------------------------------------------------------------

def test_calculate_slots_empty_schedule_returns_all_slots():
    """With no existing bookings, all 18 half-hour slots (8am–5pm) should be free."""
    from data.tebra_appointments import calculate_available_slots

    target = date(2030, 1, 7)
    with patch("data.tebra_appointments.get_appointments", return_value=[]):
        slots = calculate_available_slots("Any Provider", target, start_hour=8, end_hour=17)

    assert len(slots) == 18  # (17 - 8) * 2 = 18 slots


def test_calculate_slots_occupied_slot_excluded():
    from data.tebra_appointments import calculate_available_slots

    target = date(2030, 1, 7)
    occupied = [
        {
            "start": datetime(2030, 1, 7, 9, 0),
            "end": datetime(2030, 1, 7, 9, 30),
            "status": "Confirmed",
        }
    ]
    with patch("data.tebra_appointments.get_appointments", return_value=occupied):
        slots = calculate_available_slots("Any Provider", target, start_hour=8, end_hour=17)

    # 18 total - 1 occupied = 17
    assert len(slots) == 17
    starts = [s["start"].strftime("%H:%M") for s in slots]
    assert "09:00" not in starts


def test_calculate_slots_cancelled_not_excluded():
    """Cancelled appointments do not block time slots."""
    from data.tebra_appointments import calculate_available_slots

    target = date(2030, 1, 7)
    cancelled = [
        {
            "start": datetime(2030, 1, 7, 10, 0),
            "end": datetime(2030, 1, 7, 10, 30),
            "status": "Cancelled",
        }
    ]
    with patch("data.tebra_appointments.get_appointments", return_value=cancelled):
        slots = calculate_available_slots("Any Provider", target, start_hour=8, end_hour=17)

    # Cancelled should not block — still 18 slots
    assert len(slots) == 18


def test_calculate_slots_have_start_end_label():
    from data.tebra_appointments import calculate_available_slots

    target = date(2030, 1, 7)
    with patch("data.tebra_appointments.get_appointments", return_value=[]):
        slots = calculate_available_slots("Any Provider", target, start_hour=8, end_hour=17)

    for slot in slots:
        assert "start" in slot
        assert "end" in slot
        assert "label" in slot
        assert isinstance(slot["start"], datetime)
        assert isinstance(slot["end"], datetime)
        assert isinstance(slot["label"], str)


def test_calculate_slots_no_overlap():
    from data.tebra_appointments import calculate_available_slots

    target = date(2030, 1, 7)
    with patch("data.tebra_appointments.get_appointments", return_value=[]):
        slots = calculate_available_slots("Any Provider", target, start_hour=8, end_hour=17)

    for i in range(len(slots) - 1):
        assert slots[i]["end"] <= slots[i + 1]["start"], (
            f"Slots {i} and {i+1} overlap"
        )
