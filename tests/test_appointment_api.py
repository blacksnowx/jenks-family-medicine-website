"""
Tests for the appointment request API endpoints.

  POST /api/appointment-request      — public, no auth
  GET  /api/appointment-requests     — Owner/Admin only
  POST /api/appointment-request/<id>/status — Owner/Admin only
"""

import json
import pytest

from models import AppointmentRequest


VALID_REQUEST_PAYLOAD = {
    "name": "Jane Test",
    "phone": "423-555-0100",
    "email": "jane@example.com",
    "preferred_time": "Morning",
    "reason": "Annual physical",
    "source": "primary-care",
}


# ---------------------------------------------------------------------------
# POST /api/appointment-request — valid data
# ---------------------------------------------------------------------------

def test_valid_appointment_request_returns_200(client):
    resp = client.post(
        "/api/appointment-request",
        json=VALID_REQUEST_PAYLOAD,
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["success"] is True


def test_valid_appointment_request_stored_in_db(client, db):
    client.post("/api/appointment-request", json=VALID_REQUEST_PAYLOAD)
    req = AppointmentRequest.query.filter_by(phone="423-555-0100").first()
    assert req is not None
    assert req.name == "Jane Test"
    assert req.email == "jane@example.com"
    assert req.status == "new"


def test_appointment_request_source_field_primary_care(client, db):
    payload = {**VALID_REQUEST_PAYLOAD, "source": "primary-care"}
    client.post("/api/appointment-request", json=payload)
    req = AppointmentRequest.query.filter_by(phone="423-555-0100").first()
    assert req.source == "primary-care"


def test_appointment_request_source_field_functional_medicine(client, db):
    payload = {**VALID_REQUEST_PAYLOAD, "phone": "423-555-0200", "source": "functional-medicine"}
    client.post("/api/appointment-request", json=payload)
    req = AppointmentRequest.query.filter_by(phone="423-555-0200").first()
    assert req.source == "functional-medicine"


def test_appointment_request_default_status_is_new(client, db):
    client.post("/api/appointment-request", json=VALID_REQUEST_PAYLOAD)
    req = AppointmentRequest.query.filter_by(phone="423-555-0100").first()
    assert req.status == "new"


# ---------------------------------------------------------------------------
# POST /api/appointment-request — missing required fields
# ---------------------------------------------------------------------------

def test_missing_name_returns_400(client):
    payload = {k: v for k, v in VALID_REQUEST_PAYLOAD.items() if k != "name"}
    resp = client.post("/api/appointment-request", json=payload)
    assert resp.status_code == 400
    assert resp.get_json()["success"] is False


def test_missing_phone_returns_400(client):
    payload = {k: v for k, v in VALID_REQUEST_PAYLOAD.items() if k != "phone"}
    resp = client.post("/api/appointment-request", json=payload)
    assert resp.status_code == 400


def test_empty_name_returns_400(client):
    payload = {**VALID_REQUEST_PAYLOAD, "name": "   "}
    resp = client.post("/api/appointment-request", json=payload)
    assert resp.status_code == 400


def test_empty_phone_returns_400(client):
    payload = {**VALID_REQUEST_PAYLOAD, "phone": ""}
    resp = client.post("/api/appointment-request", json=payload)
    assert resp.status_code == 400


# ---------------------------------------------------------------------------
# GET /api/appointment-requests — requires Owner/Admin auth
# ---------------------------------------------------------------------------

def test_appointment_requests_unauthenticated_redirects(client):
    """Unauthenticated access must redirect to login (302) or return 401."""
    resp = client.get("/api/appointment-requests")
    assert resp.status_code in (302, 401)


def test_appointment_requests_as_owner(owner_client):
    resp = owner_client.get("/api/appointment-requests")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "requests" in data


def test_appointment_requests_as_admin(admin_client):
    resp = admin_client.get("/api/appointment-requests")
    assert resp.status_code == 200


def test_appointment_requests_as_provider_returns_403(provider_client):
    resp = provider_client.get("/api/appointment-requests")
    assert resp.status_code == 403


# ---------------------------------------------------------------------------
# POST /api/appointment-request/<id>/status — Owner/Admin only
# ---------------------------------------------------------------------------

def _create_appointment(client):
    """Helper: create an appointment and return its DB id."""
    client.post("/api/appointment-request", json=VALID_REQUEST_PAYLOAD)
    req = AppointmentRequest.query.filter_by(phone="423-555-0100").first()
    return req.id


def test_update_status_requires_auth(client):
    req_id = _create_appointment(client)
    resp = client.post(
        f"/api/appointment-request/{req_id}/status",
        json={"status": "contacted"},
    )
    assert resp.status_code in (302, 401)


def test_update_status_as_owner(owner_client, db):
    req_id = _create_appointment(owner_client)
    resp = owner_client.post(
        f"/api/appointment-request/{req_id}/status",
        json={"status": "contacted"},
    )
    assert resp.status_code == 200
    req = db.session.get(AppointmentRequest, req_id)
    assert req.status == "contacted"


def test_update_status_as_provider_returns_403(provider_client, db):
    req_id = _create_appointment(provider_client)
    resp = provider_client.post(
        f"/api/appointment-request/{req_id}/status",
        json={"status": "contacted"},
    )
    assert resp.status_code == 403


def test_appointment_requests_returns_recent_records(owner_client, db):
    """List endpoint returns up to 20 requests ordered newest-first."""
    # Create 3 requests
    for i in range(3):
        owner_client.post(
            "/api/appointment-request",
            json={**VALID_REQUEST_PAYLOAD, "phone": f"423-555-01{i:02d}"},
        )
    resp = owner_client.get("/api/appointment-requests")
    data = resp.get_json()
    assert len(data["requests"]) >= 3
