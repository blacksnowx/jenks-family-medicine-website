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
    # Clear any existing entries for this provider (both old and new name)
    ProviderSchedule.query.filter(
        ProviderSchedule.provider_name.in_(["SARAH SUGGS", PROVIDER_NAME])
    ).delete(synchronize_session=False)
    db.session.commit()

    # Seed Mon-Fri (0=Mon, 4=Fri)
    for dow in range(5):
        schedule = ProviderSchedule(
            provider_name=PROVIDER_NAME,
            day_of_week=dow,
            start_hour=8,
            end_hour=17,
            slot_duration=30,
            is_active=True,
        )
        db.session.add(schedule)

    db.session.commit()
    print(f"Seeded {PROVIDER_NAME} schedule: Mon-Fri 8am-5pm 30-min slots")
