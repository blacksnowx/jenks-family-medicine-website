#!/usr/bin/env python3
"""
Seed ProviderSchedule entries for Sarah Suggs Mon-Fri 8am-5pm 30-min slots.

Usage:
    python scripts/seed_schedules.py
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import app, db
from models import ProviderSchedule  # noqa: E402

PROVIDER_NAME = "SARAH F SUGGS, NP"

with app.app_context():
    existing = ProviderSchedule.query.filter_by(provider_name=PROVIDER_NAME).count()
    if existing:
        print(f"Schedule already exists for {PROVIDER_NAME} ({existing} rows) — skipping")
    else:
        for dow in range(5):
            db.session.add(ProviderSchedule(
                provider_name=PROVIDER_NAME,
                day_of_week=dow,
                start_hour=8,
                end_hour=17,
                slot_duration=30,
                is_active=True,
            ))
        db.session.commit()
        print(f"Seeded {PROVIDER_NAME} schedule: Mon-Fri 8am-5pm 30-min slots")
