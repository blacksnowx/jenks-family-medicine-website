"""
PII hashing utilities for Jenks Family Medicine.

All patient identifiers must be hashed before storage or processing.
Uses HMAC-SHA256 with a site-specific secret so hashes are:
  - Deterministic  (same input → same hash every time, enables dedup)
  - Non-reversible (cannot recover original value without the secret)
  - Prefixed       (makes the identifier type visible in logs/data)

Required env var: PII_HASH_SECRET — a strong random string, min 32 chars.
"""

import hashlib
import hmac
import os

import pandas as pd

# 16-byte hex digest — long enough to be collision-resistant for this dataset
_DIGEST_LENGTH = 32  # hex chars = 16 bytes


def _get_secret() -> bytes:
    """Return the HMAC secret from env. Raises if not set."""
    secret = os.environ.get("PII_HASH_SECRET", "")
    if not secret:
        raise RuntimeError(
            "PII_HASH_SECRET env var is not set. "
            "Set it to a strong random string before using pii_utils."
        )
    return secret.encode("utf-8")


def hash_pii(value: str, prefix: str) -> str:
    """
    Hash a PII identifier and return a prefixed, non-reversible token.

    Args:
        value:  The raw identifier (patient ID, encounter ID, case ID, etc.)
        prefix: Short label for the identifier type, e.g. 'pid_', 'eid_', 'cid_'

    Returns:
        A string like 'pid_a3f8b2c1d4e5f678' — safe to store and log.

    Raises:
        RuntimeError: If PII_HASH_SECRET is not configured.
        ValueError:   If value is empty.
    """
    if not value or not str(value).strip():
        raise ValueError("Cannot hash an empty identifier.")

    raw = str(value).strip().encode("utf-8")
    digest = hmac.new(_get_secret(), raw, hashlib.sha256).hexdigest()
    return f"{prefix}{digest[:_DIGEST_LENGTH]}"


def hash_patient_id(patient_id: str) -> str:
    """Hash a Tebra patient ID. Returns 'pid_<hex>'."""
    return hash_pii(patient_id, "pid_")


def hash_encounter_id(encounter_id: str) -> str:
    """Hash a Tebra encounter ID. Returns 'eid_<hex>'."""
    return hash_pii(encounter_id, "eid_")


def hash_case_id(case_id: str) -> str:
    """Hash a VA/Sheets case ID. Returns 'cid_<hex>'."""
    return hash_pii(case_id, "cid_")


def is_already_hashed(value: str, prefix: str) -> bool:
    """Return True if the value already starts with the given prefix."""
    return str(value).startswith(prefix)


def hash_pii_columns(df: pd.DataFrame, column_map: dict) -> pd.DataFrame:
    """
    Hash PII columns in a DataFrame using HMAC-SHA256 and return a copy.

    Args:
        df:         DataFrame to process.
        column_map: dict of {column_name: prefix}, e.g.
                    {"Patient ID": "pid_", "Encounter ID": "eid_"}

    Returns:
        A new DataFrame with the specified columns hashed.

    Raises:
        RuntimeError: If PII_HASH_SECRET is not configured.
    """
    df = df.copy()
    for col, prefix in column_map.items():
        if col not in df.columns:
            continue
        df[col] = df[col].apply(
            lambda v, p=prefix: v
            if (pd.isna(v) or is_already_hashed(str(v), p))
            else hash_pii(str(v), p)
        )
    return df
