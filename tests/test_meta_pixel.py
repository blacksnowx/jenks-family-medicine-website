"""
Tests for Meta Pixel integration on landing pages.

The pixel fires on /welcome/primary-care and /welcome/functional-medicine
when META_PIXEL_ID is set. Admin pages must never include pixel code.
"""

import pytest


LANDING_ROUTES = [
    "/welcome/primary-care",
    "/welcome/functional-medicine",
]

PIXEL_ID = "987654321098765"


# ---------------------------------------------------------------------------
# Pixel present when META_PIXEL_ID is set
# ---------------------------------------------------------------------------

def test_primary_care_includes_fbq_when_pixel_id_set(client, monkeypatch):
    monkeypatch.setenv("META_PIXEL_ID", PIXEL_ID)
    resp = client.get("/welcome/primary-care")
    assert b"fbq" in resp.data
    assert PIXEL_ID.encode() in resp.data


def test_functional_medicine_includes_fbq_when_pixel_id_set(client, monkeypatch):
    monkeypatch.setenv("META_PIXEL_ID", PIXEL_ID)
    resp = client.get("/welcome/functional-medicine")
    assert b"fbq" in resp.data
    assert PIXEL_ID.encode() in resp.data


def test_pixel_script_tag_is_present_when_set(client, monkeypatch):
    """The fbevents.js script tag must be loaded when META_PIXEL_ID is set."""
    monkeypatch.setenv("META_PIXEL_ID", PIXEL_ID)
    resp = client.get("/welcome/primary-care")
    assert b"fbevents.js" in resp.data


def test_fbq_init_uses_correct_pixel_id(client, monkeypatch):
    """fbq('init', ...) must use the configured pixel ID."""
    monkeypatch.setenv("META_PIXEL_ID", PIXEL_ID)
    resp = client.get("/welcome/primary-care")
    assert f"fbq('init', '{PIXEL_ID}')".encode() in resp.data


# ---------------------------------------------------------------------------
# ViewContent event fires on page load
# ---------------------------------------------------------------------------

def test_primary_care_viewcontent_event_fires(client, monkeypatch):
    """The ViewContent event must be tracked for the primary-care landing page."""
    monkeypatch.setenv("META_PIXEL_ID", PIXEL_ID)
    resp = client.get("/welcome/primary-care")
    assert b"ViewContent" in resp.data
    assert b"Primary Care Landing Page" in resp.data


def test_functional_medicine_viewcontent_event_fires(client, monkeypatch):
    """The ViewContent event must be tracked for the functional-medicine landing page."""
    monkeypatch.setenv("META_PIXEL_ID", PIXEL_ID)
    resp = client.get("/welcome/functional-medicine")
    assert b"ViewContent" in resp.data
    assert b"Functional Medicine Landing Page" in resp.data


# ---------------------------------------------------------------------------
# Pixel absent when META_PIXEL_ID is not set
# ---------------------------------------------------------------------------

def test_primary_care_no_fbq_when_pixel_id_unset(client, monkeypatch):
    monkeypatch.delenv("META_PIXEL_ID", raising=False)
    resp = client.get("/welcome/primary-care")
    assert b"fbq" not in resp.data


def test_functional_medicine_no_fbq_when_pixel_id_unset(client, monkeypatch):
    monkeypatch.delenv("META_PIXEL_ID", raising=False)
    resp = client.get("/welcome/functional-medicine")
    assert b"fbq" not in resp.data


def test_no_pixel_script_when_id_unset(client, monkeypatch):
    monkeypatch.delenv("META_PIXEL_ID", raising=False)
    resp = client.get("/welcome/primary-care")
    assert b"fbevents.js" not in resp.data


# ---------------------------------------------------------------------------
# Admin pages must NOT include Meta pixel
# ---------------------------------------------------------------------------

def test_admin_login_page_no_pixel(client, monkeypatch):
    monkeypatch.setenv("META_PIXEL_ID", PIXEL_ID)
    resp = client.get("/admin")
    assert b"fbq" not in resp.data, "Admin login page must not include Meta pixel"


def test_admin_dashboard_no_pixel(owner_client, monkeypatch):
    monkeypatch.setenv("META_PIXEL_ID", PIXEL_ID)
    resp = owner_client.get("/admin/dashboard")
    assert b"fbq" not in resp.data, "Admin dashboard must not include Meta pixel"


# ---------------------------------------------------------------------------
# Pixel renders correctly with different pixel IDs
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "pixel_id",
    ["111111111111111", "999999999999999", "123456789012345"],
)
def test_various_pixel_ids_rendered(client, monkeypatch, pixel_id):
    monkeypatch.setenv("META_PIXEL_ID", pixel_id)
    resp = client.get("/welcome/primary-care")
    assert pixel_id.encode() in resp.data


# ---------------------------------------------------------------------------
# Lead event tracking (JavaScript in template)
# ---------------------------------------------------------------------------

def test_primary_care_has_lead_event_js_or_form_submission_handler(client, monkeypatch):
    """
    The page should either fire a Lead event on form submission or have
    the JS in place to do so.  We check for the pattern in the template source.
    """
    monkeypatch.setenv("META_PIXEL_ID", PIXEL_ID)
    resp = client.get("/welcome/primary-care")
    body = resp.data
    # Either a Lead event or the appointment-request form submit handler is present
    has_lead = b"Lead" in body
    has_form_submit = b"appointment-request" in body or b"appt-form" in body
    assert has_lead or has_form_submit, (
        "primary-care page must have Lead event tracking or appointment form submit handler"
    )
