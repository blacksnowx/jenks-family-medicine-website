"""
Tests for authentication and role-based access control.

Role hierarchy:
  Owner  — full access
  Admin  — appointment requests, new patients report, basic dashboard
  Provider — view-only dashboard, blocked from Anne Jenks data and bonus reports
"""

import pytest


# ---------------------------------------------------------------------------
# Unauthenticated access
# ---------------------------------------------------------------------------

def test_admin_dashboard_requires_login(client):
    resp = client.get("/admin/dashboard")
    assert resp.status_code in (302, 401)
    if resp.status_code == 302:
        assert "/admin" in resp.headers["Location"] or "login" in resp.headers["Location"].lower()


def test_admin_sync_requires_login(client):
    resp = client.post("/admin/sync", json={"sync_type": "all"})
    assert resp.status_code in (302, 401)


def test_appointment_requests_requires_login(client):
    resp = client.get("/api/appointment-requests")
    assert resp.status_code in (302, 401)


# ---------------------------------------------------------------------------
# Owner access
# ---------------------------------------------------------------------------

def test_owner_can_access_dashboard(owner_client):
    resp = owner_client.get("/admin/dashboard")
    assert resp.status_code == 200


def test_owner_sees_owner_only_section(owner_client):
    resp = owner_client.get("/admin/dashboard")
    assert b"Owner Only Features" in resp.data


def test_owner_can_access_bonus_report_route(owner_client):
    from unittest.mock import patch
    import pandas as pd

    empty = pd.DataFrame()
    with (
        patch("data.data_loader.load_pc_data", return_value=empty),
        patch("data.data_loader.load_va_data", return_value=empty),
    ):
        resp = owner_client.get("/admin/reports/bonus")
    assert resp.status_code == 200


def test_owner_can_access_owner_analytics(owner_client):
    from unittest.mock import patch
    import pandas as pd

    empty = pd.DataFrame()
    with (
        patch("data.data_loader.load_pc_data", return_value=empty),
        patch("data.data_loader.load_va_data", return_value=empty),
    ):
        resp = owner_client.get("/admin/reports/owner_analytics")
    # 200 or 500 (if analytics need data), but NOT 403
    assert resp.status_code != 403


def test_owner_can_access_appointment_requests(owner_client):
    resp = owner_client.get("/api/appointment-requests")
    assert resp.status_code == 200


def test_owner_can_see_data_sync_tab(owner_client):
    resp = owner_client.get("/admin/dashboard")
    assert b"Sync" in resp.data or b"sync" in resp.data


def test_owner_can_trigger_sync(owner_client):
    from unittest.mock import patch
    with patch("data.sync_manager.run_all_syncs"):
        resp = owner_client.post("/admin/sync", json={"sync_type": "all"})
    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Admin access
# ---------------------------------------------------------------------------

def test_admin_can_access_dashboard(admin_client):
    resp = admin_client.get("/admin/dashboard")
    assert resp.status_code == 200


def test_admin_sees_admin_section(admin_client):
    resp = admin_client.get("/admin/dashboard")
    assert b"Admin Features" in resp.data


def test_admin_does_not_see_owner_section(admin_client):
    resp = admin_client.get("/admin/dashboard")
    assert b"Owner Only Features" not in resp.data


def test_admin_can_access_new_patients_report(admin_client):
    from unittest.mock import patch
    import pandas as pd

    with (
        patch("data.data_loader.load_pc_data", return_value=pd.DataFrame()),
        patch("data.data_loader.get_csv_from_db", return_value=None),
    ):
        resp = admin_client.get("/admin/reports/new_patients")
    assert resp.status_code == 200


def test_admin_can_access_appointment_requests(admin_client):
    resp = admin_client.get("/api/appointment-requests")
    assert resp.status_code == 200


def test_admin_cannot_access_bonus_report(admin_client):
    resp = admin_client.get("/admin/reports/bonus")
    assert resp.status_code == 403


def test_admin_cannot_access_owner_analytics(admin_client):
    resp = admin_client.get("/admin/reports/owner_analytics")
    assert resp.status_code == 403


def test_admin_cannot_trigger_sync(admin_client):
    resp = admin_client.post("/admin/sync", json={"sync_type": "all"})
    assert resp.status_code == 403


# ---------------------------------------------------------------------------
# Provider access
# ---------------------------------------------------------------------------

def test_provider_can_access_dashboard(provider_client):
    resp = provider_client.get("/admin/dashboard")
    assert resp.status_code == 200


def test_provider_does_not_see_owner_section(provider_client):
    resp = provider_client.get("/admin/dashboard")
    assert b"Owner Only Features" not in resp.data


def test_provider_does_not_see_admin_section(provider_client):
    resp = provider_client.get("/admin/dashboard")
    assert b"Admin Features" not in resp.data


def test_provider_cannot_access_bonus_report(provider_client):
    resp = provider_client.get("/admin/reports/bonus")
    assert resp.status_code == 403


def test_provider_cannot_access_owner_analytics(provider_client):
    resp = provider_client.get("/admin/reports/owner_analytics")
    assert resp.status_code == 403


def test_provider_cannot_access_appointment_requests(provider_client):
    resp = provider_client.get("/api/appointment-requests")
    assert resp.status_code == 403


def test_provider_cannot_trigger_sync(provider_client):
    resp = provider_client.post("/admin/sync", json={"sync_type": "all"})
    assert resp.status_code == 403


def test_provider_cannot_view_anne_jenks_rvu_chart(provider_client):
    resp = provider_client.get("/admin/reports/rvu_image?view_type=Anne+Jenks")
    assert resp.status_code == 403


# ---------------------------------------------------------------------------
# Login / Logout flow
# ---------------------------------------------------------------------------

def test_login_with_invalid_credentials_stays_on_login(client):
    resp = client.post(
        "/admin",
        data={"email": "nobody@test.com", "password": "wrongpassword"},
        follow_redirects=True,
    )
    # Should stay on login page or show error — not redirect to dashboard
    assert b"Invalid" in resp.data or resp.request.path == "/admin" or b"incorrect" in resp.data.lower()


def test_logout_clears_session(owner_client, client):
    # After logout, dashboard must require re-login
    owner_client.get("/admin/logout", follow_redirects=True)
    resp = client.get("/admin/dashboard")
    assert resp.status_code in (302, 401)
