"""
Migration script: hash PII columns in existing reference_data blobs.

Idempotent — values already prefixed with "pid_" or "eid_" are skipped.

Usage (from project root):
    python scripts/hash_existing_pii.py
"""

import io
import os
import sys

# Make sure project root is on the path so we can import app modules.
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

import pandas as pd
from sqlalchemy import create_engine, text

from data.data_loader import get_database_url
from data.pii_utils import hash_pii_columns

# ---------------------------------------------------------------------------
# PII column maps per file
# ---------------------------------------------------------------------------
FILE_CONFIGS = {
    "Charges Export.csv": {
        "Patient ID":   "pid_",
        "Encounter ID": "eid_",
    },
    "201 Bills and Payments.csv": {
        "Patient_ID": "pid_",
    },
}


def process_blob(engine, filename: str, column_map: dict) -> None:
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT data FROM reference_data WHERE filename = :f"),
            {"f": filename},
        ).fetchone()

    if row is None or not row[0]:
        print(f"  [SKIP] '{filename}' not found in reference_data.")
        return

    df = pd.read_csv(io.BytesIO(row[0]))

    # Report which columns are present / absent
    for col in column_map:
        if col not in df.columns:
            print(f"  [WARN] Column '{col}' not found in '{filename}' — skipping that column.")

    present = {col: pfx for col, pfx in column_map.items() if col in df.columns}
    if not present:
        print(f"  [SKIP] No target columns found in '{filename}'.")
        return

    # Check if already fully hashed (sample first non-null value)
    all_hashed = all(
        df[col].dropna().apply(lambda v: str(v).startswith(pfx)).all()
        for col, pfx in present.items()
        if not df[col].dropna().empty
    )
    if all_hashed and all(df[col].dropna().empty is False for col in present):
        print(f"  [OK]   '{filename}' already hashed — no changes needed.")
        return

    df_hashed = hash_pii_columns(df, present)

    buf = io.BytesIO()
    df_hashed.to_csv(buf, index=False)
    new_blob = buf.getvalue()

    with engine.begin() as conn:
        conn.execute(
            text("UPDATE reference_data SET data = :d WHERE filename = :f"),
            {"d": new_blob, "f": filename},
        )

    print(f"  [DONE] '{filename}' — hashed columns: {list(present.keys())}")


def main():
    engine = create_engine(get_database_url())

    for filename, column_map in FILE_CONFIGS.items():
        print(f"Processing '{filename}' ...")
        process_blob(engine, filename, column_map)

    print("Migration complete.")


if __name__ == "__main__":
    main()
