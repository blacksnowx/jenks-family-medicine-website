"""
Heroku Scheduler entry point for automated data sync.

Usage (configure in Heroku Scheduler):
    python scripts/run_sync.py
    python scripts/run_sync.py tebra
    python scripts/run_sync.py sheets
    python scripts/run_sync.py all

The script hits the /api/sync/trigger endpoint on this app's own dyno using
the SYNC_SECRET env var for authentication. This keeps all sync logic inside
the web process and its Flask app context, avoiding the need to duplicate
database connection logic here.

Required env vars:
    SYNC_SECRET   — shared secret that authenticates the trigger endpoint
    APP_BASE_URL  — base URL of the deployed app (e.g. https://your-app.herokuapp.com)

Optional env vars:
    SYNC_TYPE     — 'tebra', 'sheets', or 'all' (default: 'all')
"""

import os
import sys

import requests


def main():
    sync_type = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("SYNC_TYPE", "all")

    if sync_type not in ("tebra", "sheets", "all"):
        print(f"ERROR: Invalid sync_type '{sync_type}'. Must be 'tebra', 'sheets', or 'all'.")
        sys.exit(1)

    secret = os.environ.get("SYNC_SECRET", "")
    if not secret:
        print("ERROR: SYNC_SECRET env var is not set.")
        sys.exit(1)

    base_url = os.environ.get("APP_BASE_URL", "").rstrip("/")
    if not base_url:
        print("ERROR: APP_BASE_URL env var is not set.")
        sys.exit(1)

    url = f"{base_url}/api/sync/trigger"
    params = {"secret": secret, "sync_type": sync_type}

    print(f"Triggering sync: type={sync_type} url={url}")

    try:
        response = requests.get(url, params=params, timeout=300)
    except requests.RequestException as exc:
        print(f"ERROR: HTTP request failed: {exc}")
        sys.exit(1)

    print(f"Response status: {response.status_code}")

    try:
        data = response.json()
    except Exception:
        print(f"ERROR: Could not parse response as JSON. Body: {response.text[:500]}")
        sys.exit(1)

    if response.status_code != 200:
        print(f"ERROR: Sync endpoint returned {response.status_code}: {data.get('error', 'unknown')}")
        sys.exit(1)

    # Print summary (counts only — never data values)
    if sync_type == "all":
        tebra = data.get("tebra", {})
        sheets = data.get("sheets", {})
        overall = data.get("overall_status", "unknown")
        print(
            f"Tebra:  status={tebra.get('status')}  "
            f"fetched={tebra.get('records_fetched', 0)}  "
            f"new={tebra.get('records_new', 0)}"
        )
        print(
            f"Sheets: status={sheets.get('status')}  "
            f"fetched={sheets.get('records_fetched', 0)}  "
            f"new={sheets.get('records_new', 0)}"
        )
        print(f"Overall: {overall}")
        if overall != "success":
            sys.exit(1)
    else:
        status = data.get("status", "unknown")
        print(
            f"status={status}  "
            f"fetched={data.get('records_fetched', 0)}  "
            f"new={data.get('records_new', 0)}"
        )
        if status == "error":
            print(f"Error: {data.get('error_message', 'unknown')}")
            sys.exit(1)

    print("Sync completed successfully.")


if __name__ == "__main__":
    main()
