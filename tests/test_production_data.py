"""
Production data validation tests.

Designed to run against the live Heroku database to verify data integrity
after syncs. These tests SKIP automatically when no production data is present.

Run against production:
    heroku run python -m pytest tests/test_production_data.py -v --app jenks-family-medicine-site

Run locally (with DATABASE_URL set):
    DATABASE_URL=<heroku-url> python -m pytest tests/test_production_data.py -v

These extend the quick_data_check.py diagnostics with pytest assertions.
"""

import io
import os
import sys

import pandas as pd
import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from data.data_loader import (
    VALID_PROVIDERS,
    get_csv_from_db,
    load_pc_data,
    load_va_data,
)


# ---------------------------------------------------------------------------
# Session-scoped fixtures — load data once for the entire test run
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def charges_raw() -> pd.DataFrame:
    blob = get_csv_from_db("Charges Export.csv")
    if blob is None:
        pytest.skip("Charges Export.csv not found in database — run against production")
    blob.seek(0)
    return pd.read_csv(blob)


@pytest.fixture(scope="session")
def bills_raw() -> pd.DataFrame:
    blob = get_csv_from_db("201 Bills and Payments.csv")
    if blob is None:
        pytest.skip("201 Bills and Payments.csv not found in database — run against production")
    blob.seek(0)
    return pd.read_csv(blob)


@pytest.fixture(scope="session")
def draft_raw() -> pd.DataFrame:
    blob = get_csv_from_db("Draft Charges.csv")
    if blob is None:
        pytest.skip("Draft Charges.csv not found — run after a draft sync")
    blob.seek(0)
    return pd.read_csv(blob)


@pytest.fixture(scope="session")
def charges_processed() -> pd.DataFrame:
    df = load_pc_data()
    if df is None or df.empty:
        pytest.skip("load_pc_data() returned empty — run against production")
    return df


@pytest.fixture(scope="session")
def bills_processed() -> pd.DataFrame:
    df = load_va_data()
    if df is None or df.empty:
        pytest.skip("load_va_data() returned empty — run against production")
    return df


# ---------------------------------------------------------------------------
# 1. Deduplication
# ---------------------------------------------------------------------------

class TestDeduplication:
    def test_no_duplicate_encounter_procedure_ids(self, charges_raw):
        if "Encounter Procedure ID" not in charges_raw.columns:
            pytest.skip("Encounter Procedure ID column not present")
        non_null = charges_raw["Encounter Procedure ID"].dropna()
        dupes = non_null.duplicated().sum()
        assert dupes == 0, (
            f"Charges Export has {dupes} duplicate Encounter Procedure IDs — "
            "dedup logic may have a bug"
        )

    def test_no_exact_duplicate_rows_in_charges(self, charges_raw):
        dupes = charges_raw.duplicated().sum()
        assert dupes == 0, f"Charges Export has {dupes} exact duplicate rows"

    def test_no_exact_duplicate_rows_in_bills(self, bills_raw):
        dupes = bills_raw.duplicated().sum()
        assert dupes == 0, f"201 Bills has {dupes} exact duplicate rows"

    def test_draft_charges_dont_overlap_confirmed(self, charges_raw, draft_raw):
        """
        Charges that have been approved must not also appear in Draft Charges.
        If they do, include_pipeline=True would double-count them.
        """
        if "Encounter Procedure ID" not in charges_raw.columns:
            pytest.skip("No Encounter Procedure ID in charges")
        if "Encounter Procedure ID" not in draft_raw.columns:
            pytest.skip("No Encounter Procedure ID in draft charges")

        confirmed_epids = set(
            charges_raw["Encounter Procedure ID"].dropna().astype(str)
        )
        draft_epids = set(
            draft_raw["Encounter Procedure ID"].dropna().astype(str)
        )
        overlap = confirmed_epids & draft_epids
        assert not overlap, (
            f"Found {len(overlap)} EPIDs in both confirmed and draft charges — "
            f"pipeline dedup may be failing. Sample: {list(overlap)[:5]}"
        )


# ---------------------------------------------------------------------------
# 2. Provider validation
# ---------------------------------------------------------------------------

class TestProviders:
    def test_all_providers_recognized_in_charges(self, charges_raw):
        """Rendering Provider values must map to known providers after normalization."""
        from data.data_loader import normalize_provider

        providers = charges_raw["Rendering Provider"].dropna().astype(str)
        unrecognized = [
            v for v in providers.unique()
            if normalize_provider(v) not in VALID_PROVIDERS
        ]
        # Allow up to 5% unrecognized (e.g. locums, billing artifacts)
        pct = len(unrecognized) / len(providers.unique()) if len(providers.unique()) > 0 else 0
        assert pct <= 0.05, (
            f"{len(unrecognized)}/{len(providers.unique())} ({pct:.1%}) provider names "
            f"are unrecognized: {unrecognized[:10]}"
        )

    def test_known_providers_present_in_charges(self, charges_processed):
        if "Provider" not in charges_processed.columns:
            if "Rendering Provider" not in charges_processed.columns:
                pytest.skip("No provider column in charges")
        providers = charges_processed.get("Provider", charges_processed.get("Rendering Provider", pd.Series()))
        known = {"ANNE JENKS", "SARAH SUGGS"}
        found = set(str(v).upper() for v in providers.unique())
        missing = known - found
        assert not missing, (
            f"Expected providers not found in Charges Export: {missing}"
        )


# ---------------------------------------------------------------------------
# 3. Date range validation
# ---------------------------------------------------------------------------

class TestDateRanges:
    def test_charges_date_range_reasonable(self, charges_raw):
        dates = pd.to_datetime(charges_raw["Date Of Service"], errors="coerce").dropna()
        assert len(dates) > 0
        # No dates before practice opened (use 2024-01-01 as a safe lower bound)
        too_old = (dates < "2024-01-01").sum()
        assert too_old == 0, (
            f"{too_old} Charges Export rows have Date Of Service before 2024-01-01 — "
            "may be data artifacts or normalization errors"
        )
        # No dates in the far future (more than 90 days from today)
        too_future = (dates > pd.Timestamp.now() + pd.Timedelta(days=90)).sum()
        assert too_future == 0, (
            f"{too_future} rows have future Date Of Service (>90 days ahead)"
        )

    def test_charges_date_range_spans_multiple_months(self, charges_raw):
        dates = pd.to_datetime(charges_raw["Date Of Service"], errors="coerce").dropna()
        span = (dates.max() - dates.min()).days
        assert span >= 60, (
            f"Date range is only {span} days — expected at least 60"
        )

    def test_bills_date_range_reasonable(self, bills_raw):
        dates = pd.to_datetime(bills_raw["Date of Service"], errors="coerce").dropna()
        assert len(dates) > 0
        span = (dates.max() - dates.min()).days
        assert span >= 60, (
            f"201 Bills date range is only {span} days — expected at least 60"
        )


# ---------------------------------------------------------------------------
# 4. PII hashing
# ---------------------------------------------------------------------------

class TestPIIHashing:
    def test_patient_ids_all_hashed(self, charges_raw):
        if "Patient ID" not in charges_raw.columns:
            pytest.skip("No Patient ID column")
        non_null = charges_raw["Patient ID"].dropna().astype(str)
        non_null = non_null[~non_null.isin(["", "nan"])]
        unhashed = [v for v in non_null if not v.startswith("pid_")]
        assert not unhashed, (
            f"{len(unhashed)} Patient IDs are not hashed. Sample: {unhashed[:5]}"
        )

    def test_encounter_ids_all_hashed(self, charges_raw):
        if "Encounter ID" not in charges_raw.columns:
            pytest.skip("No Encounter ID column")
        non_null = charges_raw["Encounter ID"].dropna().astype(str)
        non_null = non_null[~non_null.isin(["", "nan"])]
        unhashed = [v for v in non_null if not v.startswith("eid_")]
        assert not unhashed, (
            f"{len(unhashed)} Encounter IDs are not hashed. Sample: {unhashed[:5]}"
        )

    def test_no_raw_numeric_patient_ids(self, charges_raw):
        if "Patient ID" not in charges_raw.columns:
            pytest.skip("No Patient ID column")
        non_null = charges_raw["Patient ID"].dropna().astype(str)
        raw_numeric = [v for v in non_null if v.isdigit() and len(v) >= 5]
        assert not raw_numeric, (
            f"Found {len(raw_numeric)} raw numeric Patient IDs — PII hashing failed. "
            f"Sample: {raw_numeric[:5]}"
        )

    def test_no_raw_numeric_encounter_ids(self, charges_raw):
        if "Encounter ID" not in charges_raw.columns:
            pytest.skip("No Encounter ID column")
        non_null = charges_raw["Encounter ID"].dropna().astype(str)
        raw_numeric = [v for v in non_null if v.isdigit() and len(v) >= 5]
        assert not raw_numeric, (
            f"Found {len(raw_numeric)} raw numeric Encounter IDs — PII hashing failed. "
            f"Sample: {raw_numeric[:5]}"
        )


# ---------------------------------------------------------------------------
# 5. Data volume sanity checks
# ---------------------------------------------------------------------------

class TestDataVolume:
    def test_charges_row_count_above_minimum(self, charges_raw):
        assert len(charges_raw) > 1000, (
            f"Charges Export has only {len(charges_raw)} rows — expected > 1000"
        )

    def test_bills_row_count_above_minimum(self, bills_raw):
        assert len(bills_raw) > 100, (
            f"201 Bills has only {len(bills_raw)} rows — expected > 100"
        )

    def test_no_critical_charges_column_mostly_null(self, charges_raw):
        critical = ["Date Of Service", "Rendering Provider", "Service Charge Amount"]
        for col in critical:
            if col not in charges_raw.columns:
                continue
            pct_null = charges_raw[col].isna().mean()
            assert pct_null <= 0.10, (
                f"Column '{col}' is {pct_null:.1%} null — exceeds 10% threshold"
            )
