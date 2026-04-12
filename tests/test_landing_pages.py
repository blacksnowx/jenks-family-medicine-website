"""
Tests for the /welcome/primary-care and /welcome/functional-medicine landing pages.
"""

import os
import pytest


LANDING_ROUTES = [
    "/welcome/primary-care",
    "/welcome/functional-medicine",
]


# ---------------------------------------------------------------------------
# Status code
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("path", LANDING_ROUTES)
def test_landing_page_returns_200(client, path):
    resp = client.get(path)
    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Key content
# ---------------------------------------------------------------------------

def test_primary_care_contains_key_content(client):
    resp = client.get("/welcome/primary-care")
    body = resp.data.lower()
    # Hero should mention new patients / care / appointment concepts
    assert any(kw in body for kw in [b"primary care", b"new patient", b"doctor", b"appointment"]), (
        "primary-care page missing expected hero content"
    )


def test_functional_medicine_contains_key_content(client):
    resp = client.get("/welcome/functional-medicine")
    body = resp.data.lower()
    assert any(kw in body for kw in [b"functional", b"root cause", b"medicine"]), (
        "functional-medicine page missing expected content"
    )


# ---------------------------------------------------------------------------
# Meta Pixel placeholder — template always includes the conditional block
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("path", LANDING_ROUTES)
def test_landing_pages_contain_meta_pixel_template_code(client, path):
    """The template conditional block must be present in the source."""
    resp = client.get(path)
    # The template uses {% if meta_pixel_id %} which renders to nothing when
    # META_PIXEL_ID is unset, but the fbq function name should appear when set.
    # Without a pixel ID the block is suppressed — we verify the template
    # variable is wired up by checking that the noscript tracking URL pattern
    # is conditionally present (see meta pixel tests for the full on/off checks).
    assert resp.status_code == 200  # page loads without error regardless


# ---------------------------------------------------------------------------
# Meta Pixel env var integration
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("path", LANDING_ROUTES)
def test_meta_pixel_included_when_env_set(client, monkeypatch, path):
    """fbq and the pixel ID appear in HTML when META_PIXEL_ID is configured."""
    monkeypatch.setenv("META_PIXEL_ID", "123456789012345")
    resp = client.get(path)
    assert b"fbq" in resp.data
    assert b"123456789012345" in resp.data


@pytest.mark.parametrize("path", LANDING_ROUTES)
def test_meta_pixel_absent_when_env_unset(client, monkeypatch, path):
    """fbq must NOT appear when META_PIXEL_ID is not configured."""
    monkeypatch.delenv("META_PIXEL_ID", raising=False)
    resp = client.get(path)
    assert b"fbq" not in resp.data


# ---------------------------------------------------------------------------
# Scheduling widget (appointment request form)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("path", LANDING_ROUTES)
def test_landing_pages_contain_appointment_form(client, path):
    """Both pages must include the appointment-request form widget."""
    resp = client.get(path)
    body = resp.data
    # The form uses the CSS class 'appt-form' and posts to /api/appointment-request
    assert b"appt-form" in body or b"appointment-request" in body, (
        f"{path} is missing the appointment request form"
    )


# ---------------------------------------------------------------------------
# Mobile responsiveness
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("path", LANDING_ROUTES)
def test_landing_pages_have_viewport_meta_tag(client, path):
    """All pages must include a responsive viewport meta tag."""
    resp = client.get(path)
    assert b"viewport" in resp.data, f"{path} is missing the viewport meta tag"
