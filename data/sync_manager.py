"""
Sync Manager — orchestrates Tebra and Google Sheets data ingestion.

For each sync type, the manager:
  1. Creates a SyncLog record (status='running')
  2. Pulls new data from the source
  3. Loads existing CSV blob from the reference_data table
  4. Merges new data with existing, deduplicates
  5. Writes the updated CSV blob back to reference_data
  6. Updates the SyncLog record (status='success' or 'error')

Deduplication keys:
  Charges Export (Tebra):  composite (Date Of Service, Encounter ID, Procedure Code)
  201 Bills (Sheets):      Case ID (hashed)

IMPORTANT: This module must only be called from within a Flask app context
so that db.session is available.
"""

import io
import logging
from datetime import datetime, timezone

import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _load_existing_csv(filename: str) -> pd.DataFrame:
    """
    Load an existing CSV blob from the reference_data table.
    Returns an empty DataFrame if the file doesn't exist or can't be parsed.
    """
    from data.data_loader import get_csv_from_db

    blob = get_csv_from_db(filename)
    if blob is None:
        logger.info("No existing '%s' in database — will create fresh.", filename)
        return pd.DataFrame()

    try:
        df = pd.read_csv(blob)
        # Deduplicate column names — a manually-uploaded CSV may have repeated
        # headers; keeping only the first occurrence prevents downstream errors
        # where row.get(col) returns a Series instead of a scalar.
        if df.columns.duplicated().any():
            dupes = list(df.columns[df.columns.duplicated(keep=False)].unique())
            logger.warning(
                "Removing duplicate column names from '%s': %s", filename, dupes
            )
            df = df.loc[:, ~df.columns.duplicated()]
        logger.info("Loaded existing '%s': %d rows", filename, len(df))
        return df
    except Exception as exc:
        logger.error("Failed to parse existing '%s': %s", filename, exc)
        return pd.DataFrame()


def _save_csv_to_db(filename: str, df: pd.DataFrame) -> None:
    """Write a DataFrame as a CSV blob into the reference_data table."""
    from models import db, ReferenceData

    csv_bytes = df.to_csv(index=False).encode("utf-8")

    ref = ReferenceData.query.filter_by(filename=filename).first()
    if ref is None:
        ref = ReferenceData(filename=filename, data=csv_bytes)
        db.session.add(ref)
    else:
        ref.data = csv_bytes
        ref.updated_at = datetime.now(timezone.utc)

    db.session.commit()
    logger.info("Saved '%s' to database: %d bytes", filename, len(csv_bytes))


def _normalize_date_str(val) -> str:
    """Normalize any date representation to M/D/YYYY (no leading zeros) for dedup key comparison."""
    s = str(val).strip()
    if not s or s in ("nan", "NaT", "None", ""):
        return s
    try:
        dt = pd.to_datetime(s, errors="raise")
        return f"{dt.month}/{dt.day}/{dt.year}"
    except Exception:
        return s


def _merge_charges(existing: pd.DataFrame, new_data: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """
    Merge new Tebra charge rows into the existing Charges Export DataFrame.

    Dedup strategy:
      - Primary: "Encounter Procedure ID" (unique charge line ID from Tebra) when present.
      - Fallback: composite (Date Of Service, Encounter ID, Procedure Code, Service Charge Amount)
        for rows where Encounter Procedure ID is absent (e.g. manually-uploaded CSVs).

    Returns:
        (merged_df, new_record_count)
    """
    if existing.empty:
        logger.info("No existing charges — using fetched data as baseline.")
        return new_data.copy(), len(new_data)

    if new_data.empty:
        return existing.copy(), 0

    # Dedup the existing blob in place — guards against historical duplicates
    # that may have accumulated before this logic was in place.
    epid_col_check = "Encounter Procedure ID"
    has_epid_existing_pre = (
        epid_col_check in existing.columns
        and existing[epid_col_check].astype(str).str.strip().ne("").any()
    )
    before_existing = len(existing)
    if has_epid_existing_pre:
        existing = existing.drop_duplicates(subset=[epid_col_check], keep="first")
    else:
        existing = existing.drop_duplicates()
    if len(existing) < before_existing:
        logger.warning(
            "Charges merge: removed %d duplicate rows from existing blob",
            before_existing - len(existing),
        )

    # ---- Primary dedup: by Encounter Procedure ID (when present) ----
    epid_col = "Encounter Procedure ID"
    has_epid_new = epid_col in new_data.columns and new_data[epid_col].astype(str).str.strip().ne("").any()
    has_epid_existing = epid_col in existing.columns and existing[epid_col].astype(str).str.strip().ne("").any()

    if has_epid_new and has_epid_existing:
        existing_epids = set(
            existing[epid_col].astype(str).str.strip()
        ) - {"", "nan"}
        new_mask = ~new_data[epid_col].astype(str).str.strip().isin(existing_epids)
    else:
        # ---- Fallback: composite key ----
        dedup_cols = ["Date Of Service", "Encounter ID", "Procedure Code", "Service Charge Amount"]
        for col in dedup_cols:
            if col not in existing.columns:
                existing[col] = ""
            if col not in new_data.columns:
                new_data[col] = ""

        # Normalize dates to M/D/YYYY so CSV dates (1/15/2023) and API ISO dates
        # (2023-01-15T00:00:00) compare as equal.
        existing_keys = set(
            zip(
                existing["Date Of Service"].apply(_normalize_date_str),
                existing["Encounter ID"].astype(str),
                existing["Procedure Code"].astype(str),
                existing["Service Charge Amount"].astype(str),
            )
        )
        new_mask = ~new_data.apply(
            lambda r: (
                _normalize_date_str(r["Date Of Service"]),
                str(r["Encounter ID"]),
                str(r["Procedure Code"]),
                str(r["Service Charge Amount"]),
            ) in existing_keys,
            axis=1,
        )

    truly_new = new_data[new_mask]
    new_count = len(truly_new)

    if new_count == 0:
        logger.info("Charges merge: 0 new records (all %d fetched already existed)", len(new_data))
        return existing.copy(), 0

    # Align columns between existing and new before concat.
    # Use dict.fromkeys to preserve order while deduplicating.
    all_cols = list(dict.fromkeys(list(existing.columns) + [
        col for col in truly_new.columns if col not in existing.columns
    ]))

    merged = pd.concat(
        [existing.reindex(columns=all_cols), truly_new.reindex(columns=all_cols)],
        ignore_index=True,
    )

    # Final safety dedup on the merged result
    epid_col_final = "Encounter Procedure ID"
    if epid_col_final in merged.columns and merged[epid_col_final].astype(str).str.strip().ne("").any():
        dup_count = merged.duplicated(subset=[epid_col_final], keep="first").sum()
        if dup_count > 0:
            logger.warning(
                "Charges merge: final safety dedup found %d duplicate Encounter Procedure ID(s) "
                "after concat — keeping first occurrence only",
                dup_count,
            )
        merged = merged.drop_duplicates(subset=[epid_col_final], keep="first")
    else:
        merged = merged.drop_duplicates()

    logger.info("Charges merge: %d new records added (total: %d)", new_count, len(merged))
    return merged, new_count


def _merge_201s(existing: pd.DataFrame, new_data: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """
    Merge new Google Sheets 201 rows into the existing 201 Bills DataFrame.

    Dedup key: Case ID (hashed)

    Returns:
        (merged_df, new_record_count)
    """
    dedup_col = "Case ID"

    # Normalize column names: the original CSV uses 'Sarah Suggs ' as the
    # provider column header, while sheets_sync outputs 'Provider'.  Align
    # them so the merged CSV is consistent and load_va_data() doesn't create
    # duplicate 'Provider' columns after its own rename step.
    if not existing.empty:
        # Figure out which provider column name the existing data uses
        existing_provider_col = None
        for candidate in ['Sarah Suggs ', 'Sarah Suggs', 'Provider']:
            if candidate in existing.columns:
                existing_provider_col = candidate
                break

        if existing_provider_col and existing_provider_col != 'Provider':
            # Rename sheets data to match existing CSV format
            if 'Provider' in new_data.columns and existing_provider_col not in new_data.columns:
                new_data = new_data.rename(columns={'Provider': existing_provider_col})
        elif existing_provider_col == 'Provider' or existing_provider_col is None:
            # Existing already uses 'Provider' or has no provider col — keep as-is
            pass

    if existing.empty:
        logger.info("No existing 201s — using fetched data as baseline.")
        return new_data.copy(), len(new_data)

    if new_data.empty:
        return existing.copy(), 0

    if dedup_col not in existing.columns:
        existing[dedup_col] = ""
    if dedup_col not in new_data.columns:
        new_data[dedup_col] = ""

    before_existing = len(existing)
    existing_has_ids = existing[dedup_col].astype(str).str.strip().ne("").any()
    if existing_has_ids:
        existing = existing.drop_duplicates(subset=[dedup_col], keep="first")
    else:
        existing = existing.drop_duplicates()
    if len(existing) < before_existing:
        logger.warning(
            "201s merge: removed %d duplicate rows from existing blob",
            before_existing - len(existing),
        )

    existing_ids = set(existing[dedup_col].astype(str))
    new_mask = ~new_data[dedup_col].astype(str).isin(existing_ids)
    truly_new = new_data[new_mask]
    new_count = len(truly_new)

    if new_count == 0:
        logger.info("201s merge: 0 new records (all %d fetched already existed)", len(new_data))
        return existing.copy(), 0

    all_cols = list(existing.columns)
    for col in truly_new.columns:
        if col not in all_cols:
            all_cols.append(col)

    merged = pd.concat(
        [existing.reindex(columns=all_cols), truly_new.reindex(columns=all_cols)],
        ignore_index=True,
    )

    if existing_has_ids:
        dup_count = merged.duplicated(subset=[dedup_col], keep="first").sum()
        if dup_count > 0:
            logger.warning(
                "201s merge: final safety dedup found %d duplicate Case ID(s); keeping first only",
                dup_count,
            )
        merged = merged.drop_duplicates(subset=[dedup_col], keep="first")

    logger.info("201s merge: %d new records added (total: %d)", new_count, len(merged))
    return merged, new_count


# ---------------------------------------------------------------------------
# Public sync entry points
# ---------------------------------------------------------------------------

def run_tebra_sync(last_sync_date=None) -> dict:
    """
    Run a full Tebra charge sync cycle.

    Args:
        last_sync_date: datetime or date — pull records since this date.
                        If None, defaults to 90 days ago (tebra_sync default).

    Returns:
        dict with keys: status, records_fetched, records_new, error_message
    """
    from models import db, SyncLog
    from data.tebra_sync import fetch_charges_incremental

    log = SyncLog(
        sync_type="tebra",
        status="running",
        started_at=datetime.now(timezone.utc),
    )
    db.session.add(log)
    db.session.commit()

    result = {"status": "error", "records_fetched": 0, "records_new": 0, "error_message": None}

    try:
        sync_from = last_sync_date.date() if hasattr(last_sync_date, "date") else last_sync_date
        new_df = fetch_charges_incremental(last_sync_date=sync_from)

        result["records_fetched"] = len(new_df)

        existing_df = _load_existing_csv("Charges Export.csv")
        merged_df, new_count = _merge_charges(existing_df, new_df)
        result["records_new"] = new_count

        _save_csv_to_db("Charges Export.csv", merged_df)

        log.status = "success"
        log.records_fetched = result["records_fetched"]
        log.records_new = result["records_new"]
        log.completed_at = datetime.now(timezone.utc)
        log.last_sync_date = datetime.now(timezone.utc)
        db.session.commit()

        result["status"] = "success"

    except Exception as exc:
        error_msg = str(exc)
        logger.error("Tebra sync failed: %s", error_msg)

        log.status = "error"
        log.error_message = error_msg[:2000]  # truncate for DB column
        log.completed_at = datetime.now(timezone.utc)
        db.session.commit()

        result["error_message"] = error_msg

    return result


def run_sheets_sync() -> dict:
    """
    Run a full Google Sheets 201s sync cycle.

    Returns:
        dict with keys: status, records_fetched, records_new, error_message
    """
    from models import db, SyncLog
    from data.sheets_sync import fetch_201s

    log = SyncLog(
        sync_type="sheets",
        status="running",
        started_at=datetime.now(timezone.utc),
    )
    db.session.add(log)
    db.session.commit()

    result = {"status": "error", "records_fetched": 0, "records_new": 0, "error_message": None}

    try:
        new_df = fetch_201s()
        result["records_fetched"] = len(new_df)

        existing_df = _load_existing_csv("201 Bills and Payments.csv")
        merged_df, new_count = _merge_201s(existing_df, new_df)
        result["records_new"] = new_count

        _save_csv_to_db("201 Bills and Payments.csv", merged_df)

        log.status = "success"
        log.records_fetched = result["records_fetched"]
        log.records_new = result["records_new"]
        log.completed_at = datetime.now(timezone.utc)
        log.last_sync_date = datetime.now(timezone.utc)
        db.session.commit()

        result["status"] = "success"

    except Exception as exc:
        error_msg = str(exc)
        logger.error("Sheets sync failed: %s", error_msg)

        log.status = "error"
        log.error_message = error_msg[:2000]
        log.completed_at = datetime.now(timezone.utc)
        db.session.commit()

        result["error_message"] = error_msg

    return result


def run_draft_sync() -> dict:
    """
    Fetch draft/pending charges and store them in 'Draft Charges.csv'.

    Draft charges are transient (they get promoted to approved charges
    eventually), so this always overwrites the existing file rather than
    merging.  The data is kept separate from 'Charges Export.csv' so it
    never contaminates the primary confirmed-charge dataset.

    Returns:
        dict with keys: status, records_fetched, error_message
    """
    from models import db, SyncLog
    from data.tebra_sync import fetch_draft_charges

    log = SyncLog(
        sync_type="draft",
        status="running",
        started_at=datetime.now(timezone.utc),
    )
    db.session.add(log)
    db.session.commit()

    result = {"status": "error", "records_fetched": 0, "error_message": None}

    try:
        draft_df = fetch_draft_charges()
        result["records_fetched"] = len(draft_df)

        # Always overwrite — drafts are transient, no merge needed
        _save_csv_to_db("Draft Charges.csv", draft_df)

        log.status = "success"
        log.records_fetched = result["records_fetched"]
        log.records_new = result["records_fetched"]
        log.completed_at = datetime.now(timezone.utc)
        log.last_sync_date = datetime.now(timezone.utc)
        db.session.commit()

        result["status"] = "success"

    except Exception as exc:
        error_msg = str(exc)
        logger.error("Draft charge sync failed: %s", error_msg)

        log.status = "error"
        log.error_message = error_msg[:2000]
        log.completed_at = datetime.now(timezone.utc)
        db.session.commit()

        result["error_message"] = error_msg

    return result


def run_all_syncs() -> dict:
    """
    Run both Tebra and Sheets syncs sequentially.

    Passes the last successful Tebra sync date for incremental pulls.
    """
    from models import SyncLog

    # Find last successful Tebra sync date for incremental pull
    last_tebra = (
        SyncLog.query
        .filter_by(sync_type="tebra", status="success")
        .order_by(SyncLog.completed_at.desc())
        .first()
    )
    last_sync_date = last_tebra.last_sync_date if last_tebra else None

    tebra_result = run_tebra_sync(last_sync_date=last_sync_date)
    draft_result = run_draft_sync()
    sheets_result = run_sheets_sync()

    return {
        "tebra": tebra_result,
        "draft": draft_result,
        "sheets": sheets_result,
        "overall_status": (
            "success"
            if tebra_result["status"] == "success"
            and draft_result["status"] == "success"
            and sheets_result["status"] == "success"
            else "partial_error"
        ),
    }
