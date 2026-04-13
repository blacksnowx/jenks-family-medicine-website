"""
TDD tests for /new-patient redirect and Tebra scheduling CTAs.

Requirements:
  - GET /new-patient must redirect (301) to Tebra provider URL
  - base.html nav "Schedule Appointment" must link to Tebra URL
  - index.html hero "Schedule Appointment" must link to Tebra URL
  - No public-facing template should use /new-patient as an href
"""

import pytest

TEBRA_URL = "https://www.tebra.com/care/provider/ehrin-irvin-fnp-1558838680"


# ---------------------------------------------------------------------------
# /new-patient redirect
# ---------------------------------------------------------------------------

def test_new_patient_redirects(client):
    resp = client.get("/new-patient")
    assert resp.status_code in (301, 302), (
        f"/new-patient should redirect, got {resp.status_code}"
    )


def test_new_patient_redirects_to_tebra(client):
    resp = client.get("/new-patient")
    location = resp.headers.get("Location", "")
    assert TEBRA_URL in location, (
        f"/new-patient redirect target should be Tebra URL, got: {location}"
    )


def test_new_patient_redirect_is_permanent(client):
    resp = client.get("/new-patient")
    assert resp.status_code == 301, (
        f"/new-patient should be a permanent (301) redirect, got {resp.status_code}"
    )


# ---------------------------------------------------------------------------
# base.html nav — Schedule Appointment CTA
# ---------------------------------------------------------------------------

def test_base_nav_schedule_links_to_tebra(client):
    """The nav 'Schedule Appointment' button must link directly to Tebra."""
    resp = client.get("/")
    assert TEBRA_URL.encode() in resp.data, (
        "base.html nav 'Schedule Appointment' should link to Tebra URL"
    )


def test_base_nav_schedule_does_not_link_to_new_patient(client):
    """The nav must NOT send users to the old /new-patient route."""
    resp = client.get("/")
    assert b'href="/new-patient"' not in resp.data, (
        "base.html nav should not link to /new-patient"
    )


# ---------------------------------------------------------------------------
# index.html hero — Schedule Appointment CTA
# ---------------------------------------------------------------------------

def test_index_hero_schedule_links_to_tebra(client):
    """The index hero 'Schedule Appointment' button must link to Tebra."""
    resp = client.get("/")
    assert TEBRA_URL.encode() in resp.data, (
        "index.html hero 'Schedule Appointment' should link to Tebra URL"
    )


def test_index_does_not_link_to_new_patient(client):
    resp = client.get("/")
    assert b'href="/new-patient"' not in resp.data, (
        "index.html should not link to /new-patient"
    )
