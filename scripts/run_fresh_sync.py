"""Trigger a full fresh sync from Tebra (1280 day lookback) and Google Sheets."""
import os
os.environ.setdefault("TEBRA_LOOKBACK_DAYS", "1280")

from app import app

with app.app_context():
    from data.sync_manager import run_tebra_sync, run_sheets_sync

    print("Starting Tebra sync (lookback 1280 days)...")
    r = run_tebra_sync()
    print(f"Tebra result: {r}")

    print("Starting Sheets sync...")
    r = run_sheets_sync()
    print(f"Sheets result: {r}")
