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

with app.app_context():
    # Clear existing entries for this provider
    ProviderSchedule.query.filter_by(provider_name="SARAH SUGGS").delete()
    db.session.commit()

    # Seed Mon-Fri (0=Mon, 4=Fri)
    for dow in range(5):
        schedule = ProviderSchedule(
            provider_name="SARAH SUGGS",
            day_of_week=dow,
            start_hour=8,
            end_hour=17,
            slot_duration=30,
            is_active=True,
        )
        db.session.add(schedule)

    db.session.commit()
    print("Seeded Sarah Suggs schedule: Mon-Fri 8am-5pm 30-min slots")
