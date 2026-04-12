#!/usr/bin/env python3
"""
Quick data integrity check for Charges Export and 201 Bills CSVs.

Prints a clear PASS/FAIL report showing the state of the production data.
Includes targeted diagnostics for:
  - Provider name normalization failures (Ehrin Irvin missing from Tebra rows)
  - Duplicate rows from merge (Heather's inflated RVUs)
  - Historical data gaps (Anne Jenks' Dec-Feb gap)

Run with:
    heroku run python tests/quick_data_check.py --app jenks-family-medicine-site

Or locally (with DATABASE_URL set):
    python tests/quick_data_check.py
"""

import io
import os
import sys
from collections import Counter

import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from data.data_loader import (
    PROVIDER_ALIASES,
    VALID_PROVIDERS,
    get_csv_from_db,
    load_pc_data,
    load_va_data,
    normalize_provider,
)

# ---------------------------------------------------------------------------
# Report helpers
# ---------------------------------------------------------------------------

PASS = "\033[92mPASS\033[0m"
FAIL = "\033[91mFAIL\033[0m"
WARN = "\033[93mWARN\033[0m"
INFO = "\033[94mINFO\033[0m"

_results = []


def check(label: str, condition: bool, detail: str = "", warn_only: bool = False) -> bool:
    status = PASS if condition else (WARN if warn_only else FAIL)
    line = f"  [{status}] {label}"
    if detail:
        for dline in detail.split("\n"):
            line += f"\n         {dline}"
    print(line)
    _results.append(condition)
    return condition


def info(label: str, value: str = ""):
    line = f"  [{INFO}] {label}"
    if value:
        for vline in str(value).split("\n"):
            line += f"\n         {vline}"
    print(line)


def section(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------

def check_charges_export():
    section("CHARGES EXPORT CSV")

    blob = get_csv_from_db("Charges Export.csv")
    if blob is None:
        check("File exists in database", False, "Charges Export.csv not found in reference_data table")
        return None

    check("File exists in database", True)
    blob.seek(0)
    df = pd.read_csv(blob)

    # Row count
    check(
        f"Row count > 1000 (got {len(df):,})",
        len(df) > 1000,
        f"Only {len(df):,} rows — original manual upload had thousands",
    )

    # Duplicate columns
    dupes = list(df.columns[df.columns.duplicated(keep=False)].unique())
    check(
        "No duplicate column names",
        not dupes,
        f"Duplicate columns: {dupes}" if dupes else "",
    )

    # Unnamed artifact columns
    unnamed = [c for c in df.columns if str(c).startswith("Unnamed:")]
    check(
        "No 'Unnamed:' artifact columns",
        not unnamed,
        f"Artifact columns: {unnamed}" if unnamed else "",
    )

    # Required columns
    required = [
        "Date Of Service", "Rendering Provider", "Service Charge Amount",
        "Pri Ins Insurance Payment", "Procedure Code", "Procedure Codes with Modifiers",
        "Patient ID", "Encounter ID",
    ]
    missing = [c for c in required if c not in df.columns]
    check(
        "Required columns present",
        not missing,
        f"Missing: {missing}" if missing else "",
    )

    # Dates parse
    dates = pd.to_datetime(df.get("Date Of Service", pd.Series(dtype=str)), errors="coerce")
    pct_bad_dates = dates.isna().mean()
    check(
        f"Date Of Service parses ({pct_bad_dates:.1%} bad)",
        pct_bad_dates <= 0.05,
        f"{pct_bad_dates:.1%} of dates failed to parse",
    )
    if dates.notna().any():
        info("Date range", f"{dates.min().date()} → {dates.max().date()}")

    # Date range spans multiple months
    valid_dates = dates.dropna()
    if len(valid_dates) > 0:
        date_range_days = (valid_dates.max() - valid_dates.min()).days
        check(
            f"Date range spans >= 2 months ({date_range_days} days)",
            date_range_days >= 60,
        )

    # Exact duplicate rows
    dupe_rows = df.duplicated().sum()
    check(
        f"No exact duplicate rows (found {dupe_rows:,})",
        dupe_rows == 0,
        f"{dupe_rows:,} exact duplicate rows — possible double-merge" if dupe_rows else "",
    )

    # Composite key duplicates
    dedup_cols = ["Date Of Service", "Encounter ID", "Procedure Code"]
    if all(c in df.columns for c in dedup_cols):
        dupe_keys = df.duplicated(subset=dedup_cols).sum()
        check(
            f"No duplicate composite keys (found {dupe_keys:,})",
            dupe_keys == 0,
            "Duplicate (Date, EncounterID, ProcedureCode) found — merge may have added duplicates" if dupe_keys else "",
        )

    # PII hashing
    if "Patient ID" in df.columns:
        non_null_pids = df["Patient ID"].dropna().astype(str)
        non_null_pids = non_null_pids[non_null_pids != "nan"]
        if len(non_null_pids) > 0:
            pct_hashed = non_null_pids.str.startswith("pid_").mean()
            raw_numeric = [v for v in non_null_pids if v.isdigit() and len(v) >= 5]
            check(
                f"Patient IDs hashed with pid_ prefix ({pct_hashed:.1%} hashed)",
                pct_hashed == 1.0,
                f"{len(raw_numeric)} raw numeric IDs found: {raw_numeric[:3]}" if raw_numeric else "",
            )
        else:
            check("Patient ID column has values", False, "Patient ID column is entirely null/empty")

    if "Encounter ID" in df.columns:
        non_null_eids = df["Encounter ID"].dropna().astype(str)
        non_null_eids = non_null_eids[non_null_eids != "nan"]
        if len(non_null_eids) > 0:
            pct_hashed = non_null_eids.str.startswith("eid_").mean()
            raw_numeric = [v for v in non_null_eids if v.isdigit() and len(v) >= 5]
            check(
                f"Encounter IDs hashed with eid_ prefix ({pct_hashed:.1%} hashed)",
                pct_hashed == 1.0,
                f"{len(raw_numeric)} raw numeric IDs found" if raw_numeric else "",
            )

    # ===========================================================================
    # PROVIDER DIAGNOSTICS (targeted for Ehrin/Anne/Heather issues)
    # ===========================================================================
    section("  PROVIDER DIAGNOSTICS — Charges Export")

    if "Rendering Provider" not in df.columns:
        info("Rendering Provider column missing — skipping provider diagnostics")
        return df

    # Keep full-length series (aligned with df index) for boolean indexing
    raw_providers_full = df["Rendering Provider"].fillna("").astype(str)
    raw_providers = raw_providers_full[raw_providers_full != ""]

    # Count of each unique raw provider name
    provider_counts = raw_providers.value_counts()
    info(f"Raw 'Rendering Provider' unique values ({len(provider_counts)})",
         "\n".join(f"  {cnt:>6,}  {repr(name)}" for name, cnt in provider_counts.items()))

    # Map each raw name to its canonical form
    canonical_map = {}
    for raw_name in provider_counts.index:
        canonical_map[raw_name] = normalize_provider(raw_name)

    print()
    info("Raw → Canonical mapping",
         "\n".join(f"  {repr(raw):40s} → {repr(can)}" for raw, can in canonical_map.items()))

    # Check for Ehrin Irvin specifically
    ehrin_raw = [r for r in provider_counts.index if "irvin" in r.lower() or "ehrin" in r.lower()]
    check(
        f"Ehrin Irvin found in raw provider names ({ehrin_raw})",
        bool(ehrin_raw),
        "Ehrin Irvin NOT found in any raw Rendering Provider value — API may use a different name format",
    )
    if ehrin_raw:
        total_ehrin = sum(provider_counts[r] for r in ehrin_raw)
        info(f"Ehrin Irvin rows: {total_ehrin:,}")
        # Check if any of Ehrin's rows are from the API (have eid_ Encounter IDs)
        ehrin_df = df[raw_providers_full.isin(ehrin_raw)]
        if "Encounter ID" in ehrin_df.columns:
            api_rows = ehrin_df["Encounter ID"].astype(str).str.startswith("eid_").sum()
            csv_rows = (~ehrin_df["Encounter ID"].astype(str).str.startswith("eid_")).sum()
            info(f"  Ehrin rows from API (eid_ hashed): {api_rows:,}")
            info(f"  Ehrin rows from original CSV (non-eid_): {csv_rows:,}")

    # Check for Anne Jenks
    anne_raw = [r for r in provider_counts.index if "jenks" in r.lower() or "redd" in r.lower()]
    check(
        f"Anne Jenks found in raw provider names ({anne_raw})",
        bool(anne_raw),
        "Anne Jenks NOT found in any raw Rendering Provider value",
    )

    # Check for Heather Mayo
    heather_raw = [r for r in provider_counts.index if "mayo" in r.lower() or "heather" in r.lower()]
    if heather_raw:
        total_heather = sum(provider_counts[r] for r in heather_raw)
        info(f"Heather Mayo rows: {total_heather:,}")

    # Count canonical providers after normalization
    canonical_counts = raw_providers.apply(normalize_provider).value_counts()
    print()
    info("Canonical provider row counts",
         "\n".join(f"  {cnt:>6,}  {repr(name)}" for name, cnt in canonical_counts.items()))

    # Check for unrecognized providers (would appear as non-canonical in VALID_PROVIDERS)
    unrecognized = {name for name in canonical_counts.index if name not in VALID_PROVIDERS}
    check(
        "All providers normalize to known canonical names",
        not unrecognized,
        f"Unrecognized after normalization: {unrecognized}\n"
        f"  These will be DROPPED from RVU calculations!" if unrecognized else "",
    )

    # Rows from API vs CSV (based on Encounter ID format)
    if "Encounter ID" in df.columns:
        print()
        eids = df["Encounter ID"].fillna("").astype(str)
        api_rows = (eids.str.startswith("eid_")).sum()
        csv_rows = len(df) - api_rows
        info(f"Rows from Tebra API sync (eid_ hashed): {api_rows:,} ({api_rows/len(df):.1%})")
        info(f"Rows from original CSV upload (non-eid_): {csv_rows:,} ({csv_rows/len(df):.1%})")

        # Provider breakdown for API vs CSV rows
        api_df = df[eids.str.startswith("eid_")]
        csv_df = df[~eids.str.startswith("eid_")]

        if len(api_df) > 0:
            api_provider_counts = api_df["Rendering Provider"].fillna("(null)").value_counts()
            info("API row provider names",
                 "\n".join(f"  {cnt:>6,}  {repr(n)}" for n, cnt in api_provider_counts.items()))
        if len(csv_df) > 0:
            csv_provider_counts = csv_df["Rendering Provider"].fillna("(null)").value_counts()
            info("Original CSV row provider names",
                 "\n".join(f"  {cnt:>6,}  {repr(n)}" for n, cnt in csv_provider_counts.items()))

    # Per-provider row count by month (to detect Anne's gap and Heather's spike)
    if "Date Of Service" in df.columns:
        print()
        df["_date"] = pd.to_datetime(df["Date Of Service"], errors="coerce")
        df["_canonical"] = raw_providers.reindex(df.index).apply(
            lambda x: normalize_provider(x) if pd.notna(x) else "(null)"
        )
        df["_month"] = df["_date"].dt.to_period("M").astype(str)

        pivot = (
            df[df["_canonical"].isin(VALID_PROVIDERS)]
            .groupby(["_month", "_canonical"])
            .size()
            .unstack(fill_value=0)
            .tail(12)  # last 12 months
        )
        info("Rows per provider per month (last 12 months)\n" + pivot.to_string())

        # Drop temp columns
        df.drop(columns=["_date", "_canonical", "_month"], inplace=True, errors="ignore")

    return df


def check_201_bills():
    section("201 BILLS AND PAYMENTS CSV")

    blob = get_csv_from_db("201 Bills and Payments.csv")
    if blob is None:
        check("File exists in database", False, "201 Bills and Payments.csv not found in reference_data table")
        return None

    check("File exists in database", True)
    blob.seek(0)
    df = pd.read_csv(blob)

    # Row count
    check(
        f"Row count > 2000 (got {len(df):,})",
        len(df) > 2000,
        f"Only {len(df):,} rows",
    )

    # Duplicate columns
    dupes = list(df.columns[df.columns.duplicated(keep=False)].unique())
    check(
        "No duplicate column names",
        not dupes,
        f"Duplicate columns: {dupes}" if dupes else "",
    )

    # Provider column
    provider_options = ["Provider", "Sarah Suggs ", "Sarah Suggs"]
    found_provider = next((c for c in provider_options if c in df.columns), None)
    check(
        f"Provider column present (found: {repr(found_provider)})",
        found_provider is not None,
        f"Expected one of: {provider_options}, got columns: {list(df.columns[:15])}" if not found_provider else "",
    )

    # Both provider columns present (bad state)
    has_provider = "Provider" in df.columns
    has_sarah = any(c in df.columns for c in ["Sarah Suggs ", "Sarah Suggs"])
    check(
        "No conflicting provider columns (Provider + Sarah Suggs)",
        not (has_provider and has_sarah),
        "Both 'Provider' and 'Sarah Suggs'/'Sarah Suggs ' columns present — merge may have created duplicates",
        warn_only=True,
    )

    # Required columns
    required = ["Date of Service", "Focused DBQs", "Routine IMOs", "TBI", "Gen Med DBQs", "No Show"]
    missing = [c for c in required if c not in df.columns]
    check(
        "Required columns present",
        not missing,
        f"Missing: {missing}" if missing else "",
    )

    # Unnamed artifact columns
    unnamed = [c for c in df.columns if str(c).startswith("Unnamed:")]
    check(
        "No 'Unnamed:' artifact columns",
        not unnamed,
        f"Artifact columns: {unnamed}" if unnamed else "",
    )

    # Case ID hashing
    if "Case ID" in df.columns:
        non_null_cids = df["Case ID"].dropna().astype(str)
        non_null_cids = non_null_cids[non_null_cids != "nan"]
        if len(non_null_cids) > 0:
            pct_hashed = non_null_cids.str.startswith("cid_").mean()
            check(
                f"Case IDs hashed with cid_ prefix ({pct_hashed:.1%} hashed)",
                pct_hashed == 1.0,
            )
            dupe_ids = df["Case ID"].duplicated().sum()
            check(
                f"No duplicate Case IDs (found {dupe_ids})",
                dupe_ids == 0,
            )
        else:
            check("Case ID column has values", False, "Case ID column is entirely null/empty")
    else:
        check("Case ID column present", False, "No Case ID column — deduplication key missing")

    # Dates
    dates = pd.to_datetime(df.get("Date of Service", pd.Series(dtype=str)), errors="coerce")
    pct_bad_dates = dates.isna().mean()
    check(
        f"Date of Service parses ({pct_bad_dates:.1%} bad)",
        pct_bad_dates <= 0.05,
    )
    if dates.notna().any():
        info("Date range", f"{dates.min().date()} → {dates.max().date()}")

    # Date range
    valid_dates = dates.dropna()
    if len(valid_dates) > 0:
        date_range_days = (valid_dates.max() - valid_dates.min()).days
        check(
            f"Date range spans >= 2 months ({date_range_days} days)",
            date_range_days >= 60,
        )

    # Exact duplicate rows
    dupe_rows = df.duplicated().sum()
    check(
        f"No exact duplicate rows (found {dupe_rows})",
        dupe_rows == 0,
    )

    # Provider distribution
    if found_provider:
        provider_counts = df[found_provider].value_counts()
        info(f"Provider distribution\n" +
             "\n".join(f"  {cnt:>6,}  {repr(name)}" for name, cnt in provider_counts.items()))

    return df


def check_processed_pipeline():
    section("PROCESSED DATA PIPELINE (load_pc_data / load_va_data)")

    # Charges pipeline
    print("\n  load_pc_data():")
    try:
        pc = load_pc_data()
        if pc is None or pc.empty:
            check("load_pc_data() returns data", False, "Returned empty DataFrame")
        else:
            check(f"load_pc_data() returns data ({len(pc):,} rows)", True)
            dupes = list(pc.columns[pc.columns.duplicated(keep=False)].unique())
            check("No duplicate columns in output", not dupes, f"{dupes}" if dupes else "")
            check("'Rendering Provider' column present", "Rendering Provider" in pc.columns)
            check("'Date Of Service' column present", "Date Of Service" in pc.columns)
            check("'Week' column present", "Week" in pc.columns)
            if "Week" in pc.columns:
                check(
                    "Week column is datetime",
                    pd.api.types.is_datetime64_any_dtype(pc["Week"]),
                    f"dtype={pc['Week'].dtype}",
                )
            if "Service Charge Amount" in pc.columns:
                check(
                    "Service Charge Amount is numeric",
                    pd.api.types.is_numeric_dtype(pc["Service Charge Amount"]),
                    f"dtype={pc['Service Charge Amount'].dtype}",
                )
    except Exception as e:
        check("load_pc_data() runs without exception", False, str(e))

    # VA pipeline
    print("\n  load_va_data():")
    try:
        va = load_va_data()
        if va is None or va.empty:
            check("load_va_data() returns data", False, "Returned empty DataFrame")
        else:
            check(f"load_va_data() returns data ({len(va):,} rows)", True)
            dupes = list(va.columns[va.columns.duplicated(keep=False)].unique())
            check("No duplicate columns in output", not dupes, f"{dupes}" if dupes else "")
            check("'Provider' column present", "Provider" in va.columns)
            check("'Date of Service' column present", "Date of Service" in va.columns)
            check("'Week' column present", "Week" in va.columns)
            check("'VA_Revenue' column present", "VA_Revenue" in va.columns)

            has_provider = "Provider" in va.columns
            has_sarah = any(c in va.columns for c in ["Sarah Suggs ", "Sarah Suggs"])
            check(
                "No conflicting provider columns after load",
                not (has_provider and has_sarah),
                "Both 'Provider' and 'Sarah Suggs' columns present",
                warn_only=True,
            )
    except Exception as e:
        check("load_va_data() runs without exception", False, str(e))


def check_rvu_pipeline():
    section("RVU ANALYTICS PIPELINE (get_rvu_dataset)")

    try:
        from data.rvu_analytics import get_rvu_dataset
        df = get_rvu_dataset()

        if df is None or df.empty:
            check("get_rvu_dataset() returns data", False, "Returned empty DataFrame")
            return

        check(f"get_rvu_dataset() returns data ({len(df):,} rows)", True)

        required = ["Date Of Service", "Provider", "RVU", "Category", "Week"]
        missing = [c for c in required if c not in df.columns]
        check("Required columns present", not missing, f"Missing: {missing}" if missing else "")

        pct_positive_rvu = (df["RVU"] > 0).mean() if "RVU" in df.columns else 0
        check(
            f"RVU values mostly positive ({pct_positive_rvu:.1%} > 0)",
            pct_positive_rvu >= 0.50,
        )

        if "Provider" in df.columns:
            providers = set(df["Provider"].dropna().unique())
            non_canonical = providers - set(VALID_PROVIDERS)
            check(
                "All providers are canonical",
                not non_canonical,
                f"Non-canonical providers: {non_canonical}" if non_canonical else "",
            )
            # Per-provider RVU totals
            provider_rvu = df.groupby("Provider")["RVU"].sum().sort_values(ascending=False)
            info("RVU totals per provider\n" +
                 "\n".join(f"  {rvu:>8.1f}  {prov}" for prov, rvu in provider_rvu.items()))

            # Check Ehrin specifically
            ehrin_rvu = provider_rvu.get("EHRIN IRVIN", 0.0)
            check(
                f"EHRIN IRVIN has > 10 total RVUs (got {ehrin_rvu:.1f})",
                ehrin_rvu > 10,
                "Ehrin's RVUs are near-zero — possible provider name mismatch in Tebra API data",
            )

            # Check Anne for gap
            anne_rvu = provider_rvu.get("ANNE JENKS", 0.0)
            check(
                f"ANNE JENKS has > 10 total RVUs (got {anne_rvu:.1f})",
                anne_rvu > 10,
                "Anne's RVUs are near-zero — possible data gap or merge issue",
            )

        if "Category" in df.columns:
            cats = set(df["Category"].dropna().unique())
            info("Categories", str(sorted(cats)))

        # Per-provider RVU per week (to detect Heather's spike)
        if "Week" in df.columns and "Provider" in df.columns:
            weekly = (
                df[df["Provider"].isin(VALID_PROVIDERS)]
                .groupby(["Week", "Provider"])["RVU"]
                .sum()
                .unstack(fill_value=0)
                .tail(16)
            )
            info("Weekly RVU per provider (last 16 weeks)\n" + weekly.to_string())

    except Exception as e:
        check("get_rvu_dataset() runs without exception", False, str(e))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("\n" + "=" * 60)
    print("  JENKS FAMILY MEDICINE — DATA INTEGRITY QUICK CHECK")
    print("=" * 60)

    db_url = os.environ.get("DATABASE_URL", "(not set — using local SQLite)")
    db_display = db_url[:50] + "..." if len(db_url) > 50 else db_url
    print(f"\n  Database: {db_display}")

    check_charges_export()
    check_201_bills()
    check_processed_pipeline()
    check_rvu_pipeline()

    # Summary
    passed = sum(_results)
    total = len(_results)
    failed = total - passed

    section("SUMMARY")
    print(f"  Total checks: {total}")
    print(f"  Passed:       {passed}")
    print(f"  Failed:       {failed}")
    print()

    if failed == 0:
        print(f"  [{PASS}] All checks passed!")
    else:
        print(f"  [{FAIL}] {failed} check(s) failed — see details above")
    print()

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
