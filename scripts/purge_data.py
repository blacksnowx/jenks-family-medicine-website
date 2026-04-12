"""
Purge reference_data and sync_log tables to allow a clean re-sync.

CRITICAL: This script ONLY deletes from reference_data and sync_log.
It does NOT touch the users or banner_settings tables.

Usage:
    heroku run python scripts/purge_data.py --app jenks-family-medicine-site
"""

import os
import sys

from sqlalchemy import create_engine, text


def get_database_url():
    url = os.environ.get("DATABASE_URL", "")
    if not url:
        print("ERROR: DATABASE_URL is not set.", file=sys.stderr)
        sys.exit(1)
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return url


def main():
    engine = create_engine(get_database_url())

    with engine.begin() as conn:
        result = conn.execute(text("DELETE FROM reference_data"))
        ref_deleted = result.rowcount
        print(f"Deleted {ref_deleted} row(s) from reference_data.")

        result = conn.execute(text("DELETE FROM sync_log"))
        log_deleted = result.rowcount
        print(f"Deleted {log_deleted} row(s) from sync_log.")

    print("Purge complete. users and banner_settings tables were NOT touched.")


if __name__ == "__main__":
    main()
