"""
Data Integrity Tests for Charges Export CSV and 201 Bills CSV.

These tests validate the production data stored in the Heroku database after
automated sync operations merged API data with manually-uploaded CSVs.

Run with:
    python -m pytest tests/test_data_integrity.py -v

Requires DATABASE_URL env var pointing to the Heroku Postgres instance.
"""

import io
import os
import sys

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Path setup — allow importing from the project root
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from data.data_loader import (
    VALID_PROVIDERS,
    get_csv_from_db,
    load_pc_data,
    load_va_data,
)
from data.rvu_analytics import get_rvu_dataset

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def charges_raw() -> pd.DataFrame:
    """Raw Charges Export CSV as loaded directly from the database (no processing)."""
    blob = get_csv_from_db("Charges Export.csv")
    if blob is None:
        pytest.skip("Charges Export.csv not found in database")
    blob.seek(0)
    return pd.read_csv(blob)


@pytest.fixture(scope="session")
def bills_raw() -> pd.DataFrame:
    """Raw 201 Bills CSV as loaded directly from the database (no processing)."""
    blob = get_csv_from_db("201 Bills and Payments.csv")
    if blob is None:
        pytest.skip("201 Bills and Payments.csv not found in database")
    blob.seek(0)
    return pd.read_csv(blob)


@pytest.fixture(scope="session")
def charges_processed() -> pd.DataFrame:
    """Charges Export loaded through the full load_pc_data() pipeline."""
    df = load_pc_data()
    if df is None or df.empty:
        pytest.skip("load_pc_data() returned empty DataFrame")
    return df


@pytest.fixture(scope="session")
def bills_processed() -> pd.DataFrame:
    """201 Bills loaded through the full load_va_data() pipeline."""
    df = load_va_data()
    if df is None or df.empty:
        pytest.skip("load_va_data() returned empty DataFrame")
    return df


@pytest.fixture(scope="session")
def rvu_dataset() -> pd.DataFrame:
    """RVU master dataset from get_rvu_dataset()."""
    df = get_rvu_dataset()
    if df is None or df.empty:
        pytest.skip("get_rvu_dataset() returned empty DataFrame")
    return df


# ===========================================================================
# 1. Column Integrity Tests
# ===========================================================================

CHARGES_REQUIRED_COLUMNS = [
    "Date Of Service",
    "Rendering Provider",
    "Service Charge Amount",
    "Pri Ins Insurance Payment",
    "Sec Ins Insurance Payment",
    "Other Ins Insurance Payment",
    "Pri Ins Insurance Contract Adjustment",
    "Sec Ins Insurance Contract Adjustment",
    "Other Ins Insurance Contract Adjustment",
    "Pat Payment Amount",
    "Other Adjustment",
    "Procedure Code",
    "Procedure Codes with Modifiers",
    "Patient ID",
    "Encounter ID",
]

BILLS_REQUIRED_COLUMNS_OPTIONS = [
    ["Date of Service", "Focused DBQs", "Routine IMOs", "TBI", "Gen Med DBQs", "No Show"],
]

BILLS_PROVIDER_COLUMN_OPTIONS = ["Provider", "Sarah Suggs", "Sarah Suggs "]


class TestChargesColumnIntegrity:
    def test_all_required_columns_present(self, charges_raw):
        missing = [c for c in CHARGES_REQUIRED_COLUMNS if c not in charges_raw.columns]
        assert not missing, f"Charges Export is missing required columns: {missing}"

    def test_no_duplicate_column_names(self, charges_raw):
        dupes = list(charges_raw.columns[charges_raw.columns.duplicated(keep=False)].unique())
        assert not dupes, f"Charges Export has duplicate column names: {dupes}"

    def test_no_unnamed_artifact_columns(self, charges_raw):
        unnamed = [c for c in charges_raw.columns if str(c).startswith("Unnamed:")]
        assert not unnamed, f"Charges Export has pandas artifact columns: {unnamed}"


class TestBillsColumnIntegrity:
    def test_required_columns_present(self, bills_raw):
        # Must have all core columns
        required = ["Date of Service", "Focused DBQs", "Routine IMOs", "TBI", "Gen Med DBQs", "No Show"]
        missing = [c for c in required if c not in bills_raw.columns]
        assert not missing, f"201 Bills is missing required columns: {missing}"

    def test_provider_column_present(self, bills_raw):
        has_provider = any(c in bills_raw.columns for c in BILLS_PROVIDER_COLUMN_OPTIONS)
        assert has_provider, (
            f"201 Bills has no provider column. Expected one of: {BILLS_PROVIDER_COLUMN_OPTIONS}. "
            f"Got: {list(bills_raw.columns)}"
        )

    def test_no_duplicate_column_names(self, bills_raw):
        dupes = list(bills_raw.columns[bills_raw.columns.duplicated(keep=False)].unique())
        assert not dupes, f"201 Bills has duplicate column names: {dupes}"

    def test_no_unnamed_artifact_columns(self, bills_raw):
        unnamed = [c for c in bills_raw.columns if str(c).startswith("Unnamed:")]
        assert not unnamed, f"201 Bills has pandas artifact columns: {unnamed}"


# ===========================================================================
# 2. Data Type Tests
# ===========================================================================

class TestChargesDataTypes:
    def test_date_of_service_parses(self, charges_raw):
        dates = pd.to_datetime(charges_raw["Date Of Service"], errors="coerce")
        bad = dates.isna().sum()
        total = len(dates)
        pct_bad = bad / total if total > 0 else 0
        assert pct_bad <= 0.05, (
            f"{bad}/{total} ({pct_bad:.1%}) Date Of Service values failed to parse as dates"
        )

    def test_service_charge_amount_numeric(self, charges_processed):
        col = charges_processed["Service Charge Amount"]
        assert pd.api.types.is_numeric_dtype(col), (
            f"Service Charge Amount should be numeric after processing, got {col.dtype}"
        )

    def test_payment_columns_numeric(self, charges_processed):
        payment_cols = [
            "Pri Ins Insurance Payment",
            "Sec Ins Insurance Payment",
            "Other Ins Insurance Payment",
            "Pat Payment Amount",
        ]
        for col in payment_cols:
            if col in charges_processed.columns:
                assert pd.api.types.is_numeric_dtype(charges_processed[col]), (
                    f"{col} should be numeric after processing"
                )

    def test_adjustment_columns_numeric(self, charges_processed):
        adj_cols = [
            "Pri Ins Insurance Contract Adjustment",
            "Sec Ins Insurance Contract Adjustment",
            "Other Ins Insurance Contract Adjustment",
        ]
        for col in adj_cols:
            if col in charges_processed.columns:
                assert pd.api.types.is_numeric_dtype(charges_processed[col]), (
                    f"{col} should be numeric after processing"
                )

    def test_procedure_code_not_all_nan(self, charges_raw):
        pct_nan = charges_raw["Procedure Code"].isna().mean()
        assert pct_nan <= 0.10, (
            f"Procedure Code is {pct_nan:.1%} NaN — too many missing values"
        )

    def test_rendering_provider_not_all_nan(self, charges_raw):
        pct_nan = charges_raw["Rendering Provider"].isna().mean()
        assert pct_nan <= 0.10, (
            f"Rendering Provider is {pct_nan:.1%} NaN — too many missing values"
        )


class TestBillsDataTypes:
    def test_date_of_service_parses(self, bills_raw):
        dates = pd.to_datetime(bills_raw["Date of Service"], errors="coerce")
        bad = dates.isna().sum()
        total = len(dates)
        pct_bad = bad / total if total > 0 else 0
        assert pct_bad <= 0.05, (
            f"{bad}/{total} ({pct_bad:.1%}) Date of Service values failed to parse as dates"
        )

    def test_provider_values_recognizable(self, bills_raw):
        # Find the provider column
        provider_col = next(
            (c for c in BILLS_PROVIDER_COLUMN_OPTIONS if c in bills_raw.columns), None
        )
        if provider_col is None:
            pytest.skip("No provider column found")
        non_null = bills_raw[provider_col].dropna()
        if len(non_null) == 0:
            pytest.fail("Provider column is entirely null")
        # At least some values should map to known providers
        known = sum(
            1 for v in non_null
            if any(prov.lower() in str(v).lower() for prov in ["jenks", "suggs", "irvin", "mayo"])
        )
        pct_known = known / len(non_null)
        assert pct_known >= 0.50, (
            f"Only {pct_known:.1%} of provider values appear to be from known providers"
        )


# ===========================================================================
# 3. Deduplication Tests
# ===========================================================================

class TestChargesDeduplication:
    def test_no_exact_duplicate_rows(self, charges_raw):
        dupe_count = charges_raw.duplicated().sum()
        assert dupe_count == 0, (
            f"Charges Export has {dupe_count} exact duplicate rows"
        )

    def test_no_duplicate_composite_key(self, charges_raw):
        dedup_cols = ["Date Of Service", "Encounter ID", "Procedure Code"]
        available = [c for c in dedup_cols if c in charges_raw.columns]
        if len(available) < 3:
            pytest.skip(f"Not all dedup columns present, found: {available}")
        dupes = charges_raw.duplicated(subset=dedup_cols).sum()
        assert dupes == 0, (
            f"Charges Export has {dupes} duplicate (Date Of Service, Encounter ID, Procedure Code) combinations"
        )


class TestBillsDeduplication:
    def test_no_duplicate_case_ids(self, bills_raw):
        if "Case ID" not in bills_raw.columns:
            pytest.skip("Case ID column not present in 201 Bills")
        dupes = bills_raw["Case ID"].duplicated().sum()
        assert dupes == 0, f"201 Bills has {dupes} duplicate Case IDs"

    def test_no_exact_duplicate_rows(self, bills_raw):
        dupe_count = bills_raw.duplicated().sum()
        assert dupe_count == 0, (
            f"201 Bills has {dupe_count} exact duplicate rows"
        )


# ===========================================================================
# 4. PII Hashing Tests
# ===========================================================================

class TestChargesPIIHashing:
    def test_patient_id_hashed(self, charges_raw):
        if "Patient ID" not in charges_raw.columns:
            pytest.skip("Patient ID column not present")
        non_null = charges_raw["Patient ID"].dropna().astype(str)
        if len(non_null) == 0:
            pytest.skip("Patient ID column is entirely null")
        unhashed = [v for v in non_null if not v.startswith("pid_") and v not in ("", "nan")]
        pct_unhashed = len(unhashed) / len(non_null) if len(non_null) > 0 else 0
        assert pct_unhashed == 0, (
            f"{len(unhashed)}/{len(non_null)} Patient ID values are NOT hashed (don't start with 'pid_'). "
            f"Sample raw values: {unhashed[:5]}"
        )

    def test_encounter_id_hashed(self, charges_raw):
        if "Encounter ID" not in charges_raw.columns:
            pytest.skip("Encounter ID column not present")
        non_null = charges_raw["Encounter ID"].dropna().astype(str)
        if len(non_null) == 0:
            pytest.skip("Encounter ID column is entirely null")
        unhashed = [v for v in non_null if not v.startswith("eid_") and v not in ("", "nan")]
        pct_unhashed = len(unhashed) / len(non_null) if len(non_null) > 0 else 0
        assert pct_unhashed == 0, (
            f"{len(unhashed)}/{len(non_null)} Encounter ID values are NOT hashed (don't start with 'eid_'). "
            f"Sample raw values: {unhashed[:5]}"
        )

    def test_no_raw_numeric_patient_ids(self, charges_raw):
        if "Patient ID" not in charges_raw.columns:
            pytest.skip("Patient ID column not present")
        non_null = charges_raw["Patient ID"].dropna().astype(str)
        # Raw Tebra patient IDs are purely numeric (7-10 digits)
        raw_numeric = [v for v in non_null if v.isdigit() and len(v) >= 5]
        assert not raw_numeric, (
            f"Found {len(raw_numeric)} raw numeric Patient IDs — PII hashing may have failed: {raw_numeric[:5]}"
        )

    def test_no_raw_numeric_encounter_ids(self, charges_raw):
        if "Encounter ID" not in charges_raw.columns:
            pytest.skip("Encounter ID column not present")
        non_null = charges_raw["Encounter ID"].dropna().astype(str)
        raw_numeric = [v for v in non_null if v.isdigit() and len(v) >= 5]
        assert not raw_numeric, (
            f"Found {len(raw_numeric)} raw numeric Encounter IDs — PII hashing may have failed: {raw_numeric[:5]}"
        )


class TestBillsPIIHashing:
    def test_case_id_hashed(self, bills_raw):
        if "Case ID" not in bills_raw.columns:
            pytest.skip("Case ID column not present")
        non_null = bills_raw["Case ID"].dropna().astype(str)
        if len(non_null) == 0:
            pytest.skip("Case ID column is entirely null")
        unhashed = [v for v in non_null if not v.startswith("cid_") and v not in ("", "nan")]
        assert not unhashed, (
            f"{len(unhashed)} Case ID values are NOT hashed (don't start with 'cid_'). "
            f"Sample: {unhashed[:5]}"
        )


# ===========================================================================
# 5. Data Completeness Tests
# ===========================================================================

class TestChargesCompleteness:
    def test_row_count_above_minimum(self, charges_raw):
        assert len(charges_raw) > 1000, (
            f"Charges Export has only {len(charges_raw)} rows — expected > 1000 "
            f"(original manual upload had thousands)"
        )

    def test_key_providers_present(self, charges_processed):
        if "Provider" not in charges_processed.columns:
            # Provider comes from normalizing Rendering Provider — check the raw column
            pytest.skip("Processed DataFrame has no Provider column")
        providers = set(charges_processed["Provider"].dropna().unique())
        for expected in ["ANNE JENKS", "SARAH SUGGS"]:
            assert expected in providers, (
                f"Expected provider '{expected}' not found in Charges Export. "
                f"Found providers: {providers}"
            )

    def test_rendering_provider_has_known_names(self, charges_raw):
        known_fragments = ["jenks", "suggs", "irvin", "mayo", "redd"]
        providers = charges_raw["Rendering Provider"].dropna().astype(str).str.lower()
        has_known = providers.apply(
            lambda v: any(frag in v for frag in known_fragments)
        )
        pct = has_known.mean()
        assert pct >= 0.80, (
            f"Only {pct:.1%} of Rendering Provider values match known provider names"
        )

    def test_date_range_spans_multiple_months(self, charges_raw):
        dates = pd.to_datetime(charges_raw["Date Of Service"], errors="coerce").dropna()
        if len(dates) == 0:
            pytest.fail("No valid Date Of Service values found")
        date_range_days = (dates.max() - dates.min()).days
        assert date_range_days >= 60, (
            f"Date range is only {date_range_days} days — expected at least 60 (2 months). "
            f"Min: {dates.min().date()}, Max: {dates.max().date()}"
        )

    def test_no_critical_column_mostly_nan(self, charges_raw):
        critical = ["Date Of Service", "Rendering Provider", "Service Charge Amount", "Procedure Code"]
        for col in critical:
            if col not in charges_raw.columns:
                continue
            pct_nan = charges_raw[col].isna().mean()
            assert pct_nan <= 0.10, (
                f"Critical column '{col}' is {pct_nan:.1%} NaN — exceeds 10% threshold"
            )


class TestBillsCompleteness:
    def test_row_count_above_minimum(self, bills_raw):
        assert len(bills_raw) > 2000, (
            f"201 Bills has only {len(bills_raw)} rows — expected > 2000"
        )

    def test_key_providers_present(self, bills_processed):
        provider_col = next(
            (c for c in ["Provider"] + BILLS_PROVIDER_COLUMN_OPTIONS if c in bills_processed.columns), None
        )
        if provider_col is None:
            pytest.skip("No provider column found in processed bills")
        providers_raw = bills_processed[provider_col].dropna().astype(str)
        providers_lower = providers_raw.str.lower()
        assert providers_lower.str.contains("suggs").any(), (
            "SARAH SUGGS not found in 201 Bills provider column"
        )

    def test_date_range_spans_multiple_months(self, bills_raw):
        dates = pd.to_datetime(bills_raw["Date of Service"], errors="coerce").dropna()
        if len(dates) == 0:
            pytest.fail("No valid Date of Service values found in 201 Bills")
        date_range_days = (dates.max() - dates.min()).days
        assert date_range_days >= 60, (
            f"201 Bills date range is only {date_range_days} days — expected at least 60. "
            f"Min: {dates.min().date()}, Max: {dates.max().date()}"
        )

    def test_no_critical_column_mostly_nan(self, bills_raw):
        critical = ["Date of Service", "Focused DBQs", "Routine IMOs"]
        for col in critical:
            if col not in bills_raw.columns:
                continue
            pct_nan = bills_raw[col].isna().mean()
            assert pct_nan <= 0.10, (
                f"Critical column '{col}' is {pct_nan:.1%} NaN — exceeds 10% threshold"
            )


# ===========================================================================
# 6. Merge Integrity Tests
# ===========================================================================

class TestChargesMergeIntegrity:
    def test_no_duplicate_columns_after_load(self, charges_processed):
        dupes = list(
            charges_processed.columns[charges_processed.columns.duplicated(keep=False)].unique()
        )
        assert not dupes, (
            f"load_pc_data() output has duplicate column names: {dupes}"
        )

    def test_required_output_columns_present(self, charges_processed):
        for col in ["Provider", "Date Of Service", "Week"]:
            # Provider is derived inside get_rvu_dataset, not load_pc_data, so skip it here
            if col == "Provider":
                continue
            assert col in charges_processed.columns, (
                f"load_pc_data() output is missing expected column '{col}'"
            )

    def test_rendering_provider_column_present(self, charges_processed):
        assert "Rendering Provider" in charges_processed.columns, (
            "load_pc_data() output is missing 'Rendering Provider' column"
        )

    def test_week_column_is_datetime(self, charges_processed):
        assert pd.api.types.is_datetime64_any_dtype(charges_processed["Week"]), (
            f"Week column should be datetime dtype, got {charges_processed['Week'].dtype}"
        )


class TestBillsMergeIntegrity:
    def test_no_duplicate_columns_after_load(self, bills_processed):
        dupes = list(
            bills_processed.columns[bills_processed.columns.duplicated(keep=False)].unique()
        )
        assert not dupes, (
            f"load_va_data() output has duplicate column names: {dupes}"
        )

    def test_required_output_columns_present(self, bills_processed):
        for col in ["Provider", "Date of Service", "Week", "VA_Revenue"]:
            assert col in bills_processed.columns, (
                f"load_va_data() output is missing expected column '{col}'"
            )

    def test_no_both_provider_and_sarah_suggs_columns(self, bills_processed):
        """After processing, there should not be BOTH 'Provider' and 'Sarah Suggs ' columns."""
        has_provider = "Provider" in bills_processed.columns
        has_sarah = any(c in bills_processed.columns for c in ["Sarah Suggs ", "Sarah Suggs"])
        assert not (has_provider and has_sarah), (
            "load_va_data() output has both 'Provider' and 'Sarah Suggs'/'Sarah Suggs ' columns — "
            "the merge left a duplicate provider column"
        )

    def test_week_column_is_datetime(self, bills_processed):
        assert pd.api.types.is_datetime64_any_dtype(bills_processed["Week"]), (
            f"Week column should be datetime dtype, got {bills_processed['Week'].dtype}"
        )

    def test_va_revenue_is_numeric(self, bills_processed):
        assert pd.api.types.is_numeric_dtype(bills_processed["VA_Revenue"]), (
            "VA_Revenue column should be numeric"
        )


# ===========================================================================
# 7. Analytics Pipeline Tests
# ===========================================================================

VALID_CATEGORIES = {
    "Primary Care Visit",
    "VA Exam",
    "VA Exam (0 IMOs)",
    "VA Exam (1-3 IMOs)",
    "VA Exam (4+ IMOs)",
    "No Show",
    "Vaccine Only",
    "DOT Physical",
    "Nexus Letter",
    "Func Med Initial",
    "Func Med Subsequent",
    "Other/Unclassified",
    "Manual Adjustment",
}


class TestRVUAnalyticsPipeline:
    def test_rvu_dataset_not_empty(self, rvu_dataset):
        assert len(rvu_dataset) > 0, "get_rvu_dataset() returned an empty DataFrame"

    def test_required_columns_present(self, rvu_dataset):
        for col in ["Date Of Service", "Provider", "RVU", "Category"]:
            assert col in rvu_dataset.columns, (
                f"get_rvu_dataset() output is missing required column '{col}'"
            )

    def test_rvu_values_mostly_positive(self, rvu_dataset):
        non_zero = (rvu_dataset["RVU"] > 0).mean()
        assert non_zero >= 0.50, (
            f"Only {non_zero:.1%} of RVU values are positive — expected at least 50%"
        )

    def test_category_values_from_known_set(self, rvu_dataset):
        unknown = set(rvu_dataset["Category"].dropna().unique()) - VALID_CATEGORIES
        assert not unknown, (
            f"get_rvu_dataset() contains unexpected Category values: {unknown}"
        )

    def test_provider_values_normalized(self, rvu_dataset):
        providers = set(rvu_dataset["Provider"].dropna().unique())
        non_canonical = providers - set(VALID_PROVIDERS)
        pct_non_canonical = len(non_canonical) / len(providers) if providers else 0
        assert pct_non_canonical == 0, (
            f"RVU dataset has non-canonical provider values: {non_canonical}"
        )

    def test_date_column_is_datetime(self, rvu_dataset):
        assert pd.api.types.is_datetime64_any_dtype(rvu_dataset["Date Of Service"]), (
            f"Date Of Service in RVU dataset should be datetime, got {rvu_dataset['Date Of Service'].dtype}"
        )

    def test_no_duplicate_columns(self, rvu_dataset):
        dupes = list(rvu_dataset.columns[rvu_dataset.columns.duplicated(keep=False)].unique())
        assert not dupes, f"get_rvu_dataset() output has duplicate column names: {dupes}"
