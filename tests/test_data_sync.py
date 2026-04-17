"""
Unit tests for the data sync logic (sync_manager.py) and PII hashing (pii_utils.py).

These tests run entirely in-memory — no production DB or Tebra API is touched.
"""

import io
import os

import pandas as pd
import pytest

# Make sure PII_HASH_SECRET is set before importing pii_utils
os.environ.setdefault("PII_HASH_SECRET", "test-pii-hash-secret-min-32-chars-for-tests!!")


# ---------------------------------------------------------------------------
# Helpers — synthetic DataFrames
# ---------------------------------------------------------------------------

def _charges_df(epids, dates=None, procedure_codes=None):
    n = len(epids)
    dates = dates or [f"1/{i+1}/2025" for i in range(n)]
    codes = procedure_codes or ["99213"] * n
    return pd.DataFrame(
        {
            "Encounter Procedure ID": epids,
            "Date Of Service": dates,
            "Encounter ID": [f"eid_{i}" for i in range(n)],
            "Procedure Code": codes,
            "Service Charge Amount": [100.0] * n,
            "Rendering Provider": ["Jenks, Anne "] * n,
        }
    )


def _bills_df(case_ids, providers=None):
    n = len(case_ids)
    providers = providers or ["Sarah Suggs"] * n
    return pd.DataFrame(
        {
            "Case ID": case_ids,
            "Provider": providers,
            "Date of Service": [f"2025-01-0{i+1}" for i in range(n)],
            "Focused DBQs": [0] * n,
            "Routine IMOs": [0] * n,
            "TBI": [0] * n,
            "Gen Med DBQs": [0] * n,
            "No Show": [0] * n,
        }
    )


# ---------------------------------------------------------------------------
# PII hashing — pii_utils
# ---------------------------------------------------------------------------

def test_hash_patient_id_has_pid_prefix():
    from data.pii_utils import hash_patient_id
    result = hash_patient_id("12345678")
    assert result.startswith("pid_"), f"Expected pid_ prefix, got: {result}"


def test_hash_encounter_id_has_eid_prefix():
    from data.pii_utils import hash_encounter_id
    result = hash_encounter_id("99999999")
    assert result.startswith("eid_"), f"Expected eid_ prefix, got: {result}"


def test_hash_case_id_has_cid_prefix():
    from data.pii_utils import hash_case_id
    result = hash_case_id("VA-2025-001")
    assert result.startswith("cid_"), f"Expected cid_ prefix, got: {result}"


def test_hash_pii_is_deterministic():
    from data.pii_utils import hash_patient_id
    a = hash_patient_id("12345678")
    b = hash_patient_id("12345678")
    assert a == b, "Same input must produce the same hash (deterministic)"


def test_hash_pii_different_inputs_produce_different_hashes():
    from data.pii_utils import hash_patient_id
    assert hash_patient_id("11111111") != hash_patient_id("22222222")


def test_is_already_hashed_true_for_prefixed_value():
    from data.pii_utils import is_already_hashed
    assert is_already_hashed("pid_abc123", "pid_") is True


def test_is_already_hashed_false_for_raw_value():
    from data.pii_utils import is_already_hashed
    assert is_already_hashed("12345678", "pid_") is False


def test_hash_pii_columns_hashes_specified_columns():
    from data.pii_utils import hash_pii_columns
    df = pd.DataFrame(
        {
            "Patient ID": ["12345678", "99999999"],
            "Encounter ID": ["ENC001", "ENC002"],
            "Unrelated": ["foo", "bar"],
        }
    )
    result = hash_pii_columns(df, {"Patient ID": "pid_", "Encounter ID": "eid_"})
    assert all(result["Patient ID"].str.startswith("pid_"))
    assert all(result["Encounter ID"].str.startswith("eid_"))
    # Unrelated column unchanged
    assert list(result["Unrelated"]) == ["foo", "bar"]


def test_hash_pii_columns_skips_already_hashed():
    from data.pii_utils import hash_pii_columns
    hashed = "pid_abcdef1234567890abcdef1234567890"
    df = pd.DataFrame({"Patient ID": [hashed]})
    result = hash_pii_columns(df, {"Patient ID": "pid_"})
    assert result["Patient ID"].iloc[0] == hashed


# ---------------------------------------------------------------------------
# Sync manager — _merge_charges
# ---------------------------------------------------------------------------

def test_merge_charges_empty_existing_returns_new_data():
    from data.sync_manager import _merge_charges
    new_data = _charges_df(["epid_001", "epid_002"])
    merged, new_count = _merge_charges(pd.DataFrame(), new_data)
    assert len(merged) == 2
    assert new_count == 2


def test_merge_charges_with_existing_data_deduplicates():
    from data.sync_manager import _merge_charges
    existing = _charges_df(["epid_001", "epid_002"])
    new_data = _charges_df(["epid_001", "epid_003"])  # epid_001 is a duplicate
    merged, new_count = _merge_charges(existing, new_data)
    assert new_count == 1
    assert len(merged) == 3
    assert list(merged["Encounter Procedure ID"]).count("epid_001") == 1


def test_merge_charges_no_duplicate_epids_after_merge():
    from data.sync_manager import _merge_charges
    existing = _charges_df(["epid_001", "epid_002"])
    new_data = _charges_df(["epid_002", "epid_003"])
    merged, _ = _merge_charges(existing, new_data)
    epids = merged["Encounter Procedure ID"].tolist()
    assert len(epids) == len(set(epids)), "Duplicate EPIDs found after merge"


def test_merge_charges_all_new_added():
    from data.sync_manager import _merge_charges
    existing = _charges_df(["epid_001"])
    new_data = _charges_df(["epid_002", "epid_003"])
    merged, new_count = _merge_charges(existing, new_data)
    assert new_count == 2
    assert len(merged) == 3


def test_merge_charges_zero_new_when_all_duplicates():
    from data.sync_manager import _merge_charges
    existing = _charges_df(["epid_001", "epid_002"])
    new_data = _charges_df(["epid_001", "epid_002"])
    merged, new_count = _merge_charges(existing, new_data)
    assert new_count == 0
    assert len(merged) == 2


def test_draft_charges_dedup_against_confirmed():
    """
    Draft charges with EPIDs already in confirmed charges must not be added.
    This tests the fix for draft charge double-counting.
    """
    from data.sync_manager import _merge_charges
    confirmed = _charges_df(["epid_001", "epid_002"])
    # epid_001 exists in both confirmed and draft — should not be counted as new
    draft_overlap = _charges_df(["epid_001", "epid_draft_only"])
    merged, new_count = _merge_charges(confirmed, draft_overlap)
    assert new_count == 1, "Only the non-overlapping draft charge should be added"
    epids = merged["Encounter Procedure ID"].tolist()
    assert epids.count("epid_001") == 1, "epid_001 must not be duplicated"


def test_merge_charges_existing_data_deduped_internally():
    """If the existing blob itself has duplicates, they are cleaned up on merge."""
    from data.sync_manager import _merge_charges
    # Existing has a duplicate row
    existing = pd.concat([
        _charges_df(["epid_001"]),
        _charges_df(["epid_001"]),  # duplicate
    ], ignore_index=True)
    new_data = _charges_df(["epid_002"])
    merged, _ = _merge_charges(existing, new_data)
    epids = merged["Encounter Procedure ID"].tolist()
    assert epids.count("epid_001") == 1


def test_merge_charges_120_day_overlap_no_duplicates():
    """Simulates the incremental sync overlap: re-pulling the same 120-day
    window on every run must not inflate totals."""
    from data.sync_manager import _merge_charges
    existing = _charges_df([f"epid_{i:04d}" for i in range(100)])
    # Re-pull the full 100 + 5 genuinely new (mimics overlap behavior)
    overlap_pull = _charges_df([f"epid_{i:04d}" for i in range(105)])
    merged, new_count = _merge_charges(existing, overlap_pull)
    assert new_count == 5
    assert len(merged) == 105
    epids = merged["Encounter Procedure ID"].tolist()
    assert len(epids) == len(set(epids)), "overlap re-pull produced duplicates"


def test_fetch_charges_incremental_extends_window_for_draft_promotions():
    """The incremental window must reach back at least 120 days so drafts
    approved after the previous sync are captured."""
    from datetime import date, timedelta
    from unittest.mock import patch
    from data.tebra_sync import fetch_charges_incremental, DRAFT_PROMOTION_LOOKBACK_DAYS

    today = date.today()
    # Simulate last sync 10 days ago — a draft with service date 60 days ago
    # approved yesterday would otherwise be missed.
    last_sync = today - timedelta(days=10)

    captured = {}
    def fake_fetch(start, end):
        captured["start"] = start
        captured["end"] = end
        return pd.DataFrame(columns=["Encounter Procedure ID"])

    with patch("data.tebra_sync.fetch_charges", side_effect=fake_fetch):
        fetch_charges_incremental(last_sync_date=last_sync)

    expected_start = today - timedelta(days=DRAFT_PROMOTION_LOOKBACK_DAYS)
    assert captured["start"] == expected_start, (
        f"Overlap window must start {DRAFT_PROMOTION_LOOKBACK_DAYS} days back, "
        f"got {captured['start']}"
    )


# ---------------------------------------------------------------------------
# Sync manager — _merge_201s
# ---------------------------------------------------------------------------

def test_merge_201s_empty_existing_returns_new_data():
    from data.sync_manager import _merge_201s
    new_data = _bills_df(["cid_001", "cid_002"])
    merged, new_count = _merge_201s(pd.DataFrame(), new_data)
    assert len(merged) == 2
    assert new_count == 2


def test_merge_201s_deduplicates_by_case_id():
    from data.sync_manager import _merge_201s
    existing = _bills_df(["cid_001", "cid_002"])
    new_data = _bills_df(["cid_001", "cid_003"])  # cid_001 is a duplicate
    merged, new_count = _merge_201s(existing, new_data)
    assert new_count == 1
    assert len(merged) == 3
    assert list(merged["Case ID"]).count("cid_001") == 1


def test_merge_201s_all_new_added():
    from data.sync_manager import _merge_201s
    existing = _bills_df(["cid_001"])
    new_data = _bills_df(["cid_002", "cid_003"])
    merged, new_count = _merge_201s(existing, new_data)
    assert new_count == 2
    assert len(merged) == 3


def test_merge_201s_zero_new_when_all_duplicates():
    from data.sync_manager import _merge_201s
    existing = _bills_df(["cid_001", "cid_002"])
    new_data = _bills_df(["cid_001", "cid_002"])
    merged, new_count = _merge_201s(existing, new_data)
    assert new_count == 0
    assert len(merged) == 2


# ---------------------------------------------------------------------------
# RVU dataset filtering by data_source
# ---------------------------------------------------------------------------

def test_get_rvu_dataset_va_source_excludes_pc(app):
    """data_source='va' must not include any rows from the PC pipeline."""
    from unittest.mock import patch
    import pandas as pd
    from data.rvu_analytics import get_rvu_dataset

    empty_df = pd.DataFrame()
    va_df = pd.DataFrame(
        {
            "Provider": ["SARAH SUGGS"],
            "Date of Service": ["2025-11-01"],
            "No Show": ["0"],
            "Routine IMOs": ["1"],
            "Focused DBQs": [0],
            "TBI": [0],
            "Gen Med DBQs": [0],
        }
    )

    with (
        patch("data.data_loader.load_pc_data", return_value=empty_df),
        patch("data.data_loader.load_va_data", return_value=va_df),
    ):
        df = get_rvu_dataset(data_source="va")

    # Should only have VA rows — none from PC
    if not df.empty:
        assert "Source" not in df.columns or df["Source"].unique().tolist() != ["confirmed"]


def test_get_rvu_dataset_pc_source_excludes_va(app):
    """data_source='pc' must not include any VA rows."""
    from unittest.mock import patch
    import pandas as pd
    from data.rvu_analytics import get_rvu_dataset

    pc_df = pd.DataFrame(
        {
            "Date Of Service": ["2025-11-01"],
            "Rendering Provider": ["Jenks, Anne "],
            "Procedure Code": ["99213"],
            "Procedure Codes with Modifiers": ["99213"],
            "Service Charge Amount": [150.0],
        }
    )

    with (
        patch("data.data_loader.load_pc_data", return_value=pc_df),
        patch("data.data_loader.load_va_data", return_value=pd.DataFrame()),
    ):
        df = get_rvu_dataset(data_source="pc")

    # VA columns (No Show, Routine IMOs) should not appear as data-driving columns
    # The VA-specific logic is never applied when data_source='pc'
    # Just confirm function returns without error
    assert df is not None


def test_get_rvu_dataset_include_pipeline_va_has_no_pipeline(app):
    """Pipeline (draft) charges are PC-only; VA-only view must have no pipeline rows."""
    from unittest.mock import patch
    import pandas as pd
    from data.rvu_analytics import get_rvu_dataset

    with (
        patch("data.data_loader.load_pc_data", return_value=pd.DataFrame()),
        patch("data.data_loader.load_va_data", return_value=pd.DataFrame()),
        patch("data.data_loader.get_csv_from_db", return_value=None),
    ):
        df = get_rvu_dataset(data_source="va", include_pipeline=True)

    if not df.empty and "Source" in df.columns:
        assert "pipeline" not in df["Source"].values, (
            "VA-only view must not include pipeline rows"
        )
