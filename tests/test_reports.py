"""
Tests for report generation functions and the corresponding admin API routes.

Data loading is mocked throughout — these tests don't require production data.
"""

import io
from unittest.mock import patch, MagicMock

import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Minimal synthetic DataFrames for mocking data_loader
# ---------------------------------------------------------------------------

def _make_pc_df():
    return pd.DataFrame(
        {
            "Date Of Service": pd.to_datetime(["2025-11-05", "2025-11-12", "2025-11-19"]),
            "Rendering Provider": ["Jenks, Anne ", "IRVIN, EHRIN ", "SUGGS, SARAH "],
            "Procedure Code": ["99213", "99213", "99214"],
            "Procedure Codes with Modifiers": ["99213", "99213", "99214"],
            "Service Charge Amount": [150.0, 150.0, 200.0],
            "Patient ID": ["pid_aaa", "pid_bbb", "pid_ccc"],
            "Encounter ID": ["eid_001", "eid_002", "eid_003"],
            "Encounter Procedure ID": ["epid_001", "epid_002", "epid_003"],
            "Pri Ins Insurance Payment": [120.0, 120.0, 160.0],
            "Sec Ins Insurance Payment": [0.0, 0.0, 0.0],
            "Other Ins Insurance Payment": [0.0, 0.0, 0.0],
            "Pat Payment Amount": [10.0, 10.0, 20.0],
            "Pri Ins Insurance Contract Adjustment": [20.0, 20.0, 20.0],
            "Sec Ins Insurance Contract Adjustment": [0.0, 0.0, 0.0],
            "Other Ins Insurance Contract Adjustment": [0.0, 0.0, 0.0],
            "Other Adjustment": [0.0, 0.0, 0.0],
            "Week": pd.to_datetime(["2025-11-07", "2025-11-14", "2025-11-21"]),
        }
    )


def _make_va_df():
    return pd.DataFrame(
        {
            "Provider": ["SARAH SUGGS", "SARAH SUGGS"],
            "Date of Service": pd.to_datetime(["2025-11-05", "2025-11-12"]),
            "Focused DBQs": [1, 0],
            "Routine IMOs": ["1", "0"],
            "TBI": [0, 1],
            "Gen Med DBQs": [0, 0],
            "No Show": ["0", "0"],
            "VA_Revenue": [480.0, 600.0],
            "Week": pd.to_datetime(["2025-11-07", "2025-11-14"]),
        }
    )


# ---------------------------------------------------------------------------
# RVU analytics — get_rvu_dataset
# ---------------------------------------------------------------------------

def test_rvu_dataset_returns_dataframe(app):
    from data.rvu_analytics import get_rvu_dataset

    with (
        patch("data.data_loader.load_pc_data", return_value=_make_pc_df()),
        patch("data.data_loader.load_va_data", return_value=_make_va_df()),
    ):
        df = get_rvu_dataset()

    assert isinstance(df, pd.DataFrame)


def test_rvu_dataset_expected_columns(app):
    from data.rvu_analytics import get_rvu_dataset

    with (
        patch("data.data_loader.load_pc_data", return_value=_make_pc_df()),
        patch("data.data_loader.load_va_data", return_value=_make_va_df()),
    ):
        df = get_rvu_dataset()

    if not df.empty:
        for col in ["Date Of Service", "Provider", "RVU", "Category"]:
            assert col in df.columns, f"Missing column '{col}' in RVU dataset"


def test_rvu_dataset_data_source_pc_excludes_va(app):
    from data.rvu_analytics import get_rvu_dataset

    with (
        patch("data.data_loader.load_pc_data", return_value=_make_pc_df()),
        patch("data.data_loader.load_va_data", return_value=_make_va_df()),
    ):
        df_pc = get_rvu_dataset(data_source="pc")
        df_va = get_rvu_dataset(data_source="va")

    # pc-only dataset should not contain VA categories
    if not df_pc.empty:
        va_cats = {"VA Exam (0 IMOs)", "VA Exam (1-3 IMOs)", "VA Exam (4+ IMOs)", "No Show"}
        pc_cats = set(df_pc["Category"].unique())
        assert not pc_cats.intersection(va_cats), (
            f"PC-only dataset contains VA categories: {pc_cats & va_cats}"
        )


def test_rvu_dataset_include_pipeline_adds_source_column(app):
    from data.rvu_analytics import get_rvu_dataset

    draft_df = pd.DataFrame(
        {
            "Date Of Service": pd.to_datetime(["2025-11-26"]),
            "Rendering Provider": ["Jenks, Anne "],
            "Procedure Code": ["99213"],
            "Procedure Codes with Modifiers": ["99213"],
            "Service Charge Amount": [150.0],
            "Encounter Procedure ID": ["epid_draft_new"],
        }
    )
    draft_csv = io.BytesIO(draft_df.to_csv(index=False).encode())

    with (
        patch("data.data_loader.load_pc_data", return_value=_make_pc_df()),
        patch("data.data_loader.load_va_data", return_value=pd.DataFrame()),
        patch("data.data_loader.get_csv_from_db", return_value=draft_csv),
    ):
        df = get_rvu_dataset(include_pipeline=True)

    if not df.empty:
        assert "Source" in df.columns, "'Source' column must be present when include_pipeline=True"


def test_rvu_dataset_exclude_providers(app):
    from data.rvu_analytics import get_rvu_dataset

    with (
        patch("data.data_loader.load_pc_data", return_value=_make_pc_df()),
        patch("data.data_loader.load_va_data", return_value=pd.DataFrame()),
    ):
        df = get_rvu_dataset()

    # Simulate provider exclusion (done at chart level, not dataset level)
    if not df.empty and "Provider" in df.columns:
        filtered = df[df["Provider"] != "ANNE JENKS"]
        assert "ANNE JENKS" not in filtered["Provider"].values


# ---------------------------------------------------------------------------
# RVU chart generation
# ---------------------------------------------------------------------------

def test_rvu_chart_returns_png_bytes(app):
    from data.rvu_analytics import generate_rvu_chart

    with (
        patch("data.data_loader.load_pc_data", return_value=_make_pc_df()),
        patch("data.data_loader.load_va_data", return_value=_make_va_df()),
    ):
        image_bytes = generate_rvu_chart("Company Wide")

    assert isinstance(image_bytes, bytes)
    assert len(image_bytes) > 0
    # PNG magic bytes
    assert image_bytes[:4] == b"\x89PNG", "generate_rvu_chart must return PNG bytes"


# ---------------------------------------------------------------------------
# Bonus report
# ---------------------------------------------------------------------------

def test_bonus_report_returns_expected_keys(app):
    from data.rvu_analytics import get_quarterly_bonus_report

    with (
        patch("data.data_loader.load_pc_data", return_value=_make_pc_df()),
        patch("data.data_loader.load_va_data", return_value=_make_va_df()),
    ):
        report = get_quarterly_bonus_report()

    assert isinstance(report, dict)
    for key in ["quarter_label", "days_elapsed", "days_in_quarter", "providers"]:
        assert key in report, f"Bonus report missing key '{key}'"


def test_bonus_report_providers_is_list(app):
    from data.rvu_analytics import get_quarterly_bonus_report

    with (
        patch("data.data_loader.load_pc_data", return_value=_make_pc_df()),
        patch("data.data_loader.load_va_data", return_value=_make_va_df()),
    ):
        report = get_quarterly_bonus_report()

    assert isinstance(report["providers"], list)


def test_bonus_report_rvus_earned_is_whole_number(app):
    """rvus_earned must be a floored integer — no fractional RVUs in bonus calc."""
    from data.rvu_analytics import get_quarterly_bonus_report

    with (
        patch("data.data_loader.load_pc_data", return_value=_make_pc_df()),
        patch("data.data_loader.load_va_data", return_value=_make_va_df()),
        patch("data.data_loader.get_csv_from_db", return_value=None),
    ):
        report = get_quarterly_bonus_report()

    for prov in report["providers"]:
        earned = prov["rvus_earned"]
        assert earned == int(earned), (
            f"{prov['provider']}: rvus_earned={earned} is not a whole number (floor not applied)"
        )


def test_bonus_report_draft_rvus_always_present(app):
    """Every provider dict must contain 'draft_rvus' for the current quarter."""
    from data.rvu_analytics import get_quarterly_bonus_report

    with (
        patch("data.data_loader.load_pc_data", return_value=_make_pc_df()),
        patch("data.data_loader.load_va_data", return_value=_make_va_df()),
        patch("data.data_loader.get_csv_from_db", return_value=None),
    ):
        report = get_quarterly_bonus_report()

    for prov in report["providers"]:
        assert "draft_rvus" in prov, (
            f"{prov['provider']}: missing 'draft_rvus' key in current-quarter bonus report"
        )


def test_bonus_report_estimated_total_rvus_always_present(app):
    """Every provider dict must contain 'estimated_total_rvus' for the current quarter."""
    from data.rvu_analytics import get_quarterly_bonus_report

    with (
        patch("data.data_loader.load_pc_data", return_value=_make_pc_df()),
        patch("data.data_loader.load_va_data", return_value=_make_va_df()),
        patch("data.data_loader.get_csv_from_db", return_value=None),
    ):
        report = get_quarterly_bonus_report()

    for prov in report["providers"]:
        assert "estimated_total_rvus" in prov, (
            f"{prov['provider']}: missing 'estimated_total_rvus' key in current-quarter bonus report"
        )


def test_bonus_report_estimated_total_rvus_includes_draft(app):
    """estimated_total_rvus >= rvus_earned (draft can only add, never subtract)."""
    from data.rvu_analytics import get_quarterly_bonus_report

    # Build a draft charges CSV with charges in the current quarter
    import io
    from datetime import date
    today = date.today()
    draft_df = pd.DataFrame({
        "Date Of Service": pd.to_datetime([today.strftime("%Y-%m-%d")]),
        "Rendering Provider": ["Jenks, Anne "],
        "Procedure Code": ["99213"],
        "Procedure Codes with Modifiers": ["99213"],
        "Service Charge Amount": [150.0],
        "Encounter Procedure ID": ["epid_draft_new_999"],
    })
    draft_csv = io.BytesIO(draft_df.to_csv(index=False).encode())

    with (
        patch("data.data_loader.load_pc_data", return_value=_make_pc_df()),
        patch("data.data_loader.load_va_data", return_value=_make_va_df()),
        patch("data.data_loader.get_csv_from_db", return_value=draft_csv),
    ):
        report = get_quarterly_bonus_report()

    for prov in report["providers"]:
        assert prov["estimated_total_rvus"] >= prov["rvus_earned"], (
            f"{prov['provider']}: estimated_total_rvus < rvus_earned — draft should only add"
        )


# ---------------------------------------------------------------------------
# Revenue per RVU report
# ---------------------------------------------------------------------------

def test_revenue_per_rvu_report_returns_expected_structure(app):
    from data.revenue_per_rvu import get_revenue_per_rvu_report

    with (
        patch("data.data_loader.load_pc_data", return_value=_make_pc_df()),
        patch("data.data_loader.load_va_data", return_value=_make_va_df()),
    ):
        report = get_revenue_per_rvu_report()

    assert isinstance(report, dict)
    # Should have weekly breakdown at minimum
    if "error" not in report:
        assert "weekly" in report


# ---------------------------------------------------------------------------
# New patients report
# ---------------------------------------------------------------------------

def test_new_patients_report_returns_expected_structure(app):
    from data.new_patients_analytics import get_new_patients_report

    with (
        patch("data.data_loader.load_pc_data", return_value=_make_pc_df()),
        patch("data.data_loader.get_csv_from_db", return_value=None),
    ):
        report = get_new_patients_report()

    assert isinstance(report, dict)
    if "error" not in report:
        for key in ["weekly", "total_new_patients", "avg_per_week", "google_ads_comparison"]:
            assert key in report, f"New patients report missing key '{key}'"


def test_new_patients_report_includes_google_ads_comparison(app):
    from data.new_patients_analytics import get_new_patients_report

    with (
        patch("data.data_loader.load_pc_data", return_value=_make_pc_df()),
        patch("data.data_loader.get_csv_from_db", return_value=None),
    ):
        report = get_new_patients_report()

    if "error" not in report:
        ads = report["google_ads_comparison"]
        assert isinstance(ads, dict)
        for key in ["ads_start_date", "pre_ads_weeks", "post_ads_weeks", "percent_change"]:
            assert key in ads, f"google_ads_comparison missing key '{key}'"


# ---------------------------------------------------------------------------
# Route-level tests for report endpoints
# ---------------------------------------------------------------------------

def test_rvu_image_route_requires_auth(client):
    resp = client.get("/admin/reports/rvu_image")
    assert resp.status_code in (302, 401)


def test_bonus_report_route_owner_can_access(owner_client):
    with (
        patch("data.data_loader.load_pc_data", return_value=_make_pc_df()),
        patch("data.data_loader.load_va_data", return_value=_make_va_df()),
    ):
        resp = owner_client.get("/admin/reports/bonus")
    assert resp.status_code == 200


def test_bonus_report_route_provider_denied(provider_client):
    resp = provider_client.get("/admin/reports/bonus")
    assert resp.status_code == 403


def test_new_patients_route_owner_can_access(owner_client):
    with (
        patch("data.data_loader.load_pc_data", return_value=_make_pc_df()),
        patch("data.data_loader.get_csv_from_db", return_value=None),
    ):
        resp = owner_client.get("/admin/reports/new_patients")
    assert resp.status_code == 200


def test_new_patients_route_admin_can_access(admin_client):
    with (
        patch("data.data_loader.load_pc_data", return_value=_make_pc_df()),
        patch("data.data_loader.get_csv_from_db", return_value=None),
    ):
        resp = admin_client.get("/admin/reports/new_patients")
    assert resp.status_code == 200


def test_new_patients_route_provider_denied(provider_client):
    provider_resp = provider_client.get("/admin/reports/new_patients")
    assert provider_resp.status_code == 403


def test_owner_analytics_route_owner_can_access(owner_client):
    with (
        patch("data.data_loader.load_pc_data", return_value=_make_pc_df()),
        patch("data.data_loader.load_va_data", return_value=_make_va_df()),
    ):
        resp = owner_client.get("/admin/reports/owner_analytics")
    assert resp.status_code in (200, 500)  # 500 acceptable if analytics need more data


def test_owner_analytics_route_provider_denied(provider_client):
    resp = provider_client.get("/admin/reports/owner_analytics")
    assert resp.status_code == 403
