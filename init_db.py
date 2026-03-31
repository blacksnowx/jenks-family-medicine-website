"""Initialise the database and seed admin accounts.

Run with:  python init_db.py

Idempotent — existing users are left untouched.
"""

import os
import sys

# Ensure the project root is on the path so we can import app / models.
sys.path.insert(0, os.path.dirname(__file__))

from app import create_app  # noqa: E402
from models import BannerSettings, ProviderSchedule, User, db  # noqa: E402

from datetime import datetime, timedelta, timezone

SEED_USERS = [
    {"email": "anne@jenksfamilymedicine.com", "password": "anne123", "role": "Owner"},
    {"email": "tyler@jenksfamilymedicine.com", "password": "tyler123", "role": "Owner"},
    {"email": "brittany@jenksfamilymedicine.com", "password": "brittany123", "role": "Admin"},
    {"email": "sarah@jenksfamilymedicine.com", "password": "sarah123", "role": "Provider"},
    {"email": "heather@jenksfamilymedicine.com", "password": "heather123", "role": "Provider"},
    {"email": "ehrin@jenksfamilymedicine.com", "password": "ehrin123", "role": "Provider"},
]


def _seed_provider_schedule() -> None:
    """Seed Sarah Suggs Mon-Fri 8am-5pm, 30-min slots, noon-1pm break."""
    if ProviderSchedule.query.first():
        print("  ✓ Provider schedule already seeded")
        return
    for dow in range(5):  # 0=Mon … 4=Fri
        db.session.add(ProviderSchedule(
            provider_name="SARAH SUGGS",
            day_of_week=dow,
            start_hour=8,
            start_minute=0,
            end_hour=17,
            end_minute=0,
            slot_duration=30,
            break_start_hour=12,
            break_end_hour=13,
            is_active=True,
        ))
    print("  + Seeded Sarah Suggs schedule (Mon-Fri 8am-5pm, noon break)")


def seed() -> None:
    app = create_app()
    with app.app_context():
        db.create_all()

        for user_data in SEED_USERS:
            existing = User.query.filter_by(email=user_data["email"]).first()
            if existing:
                print(f"  ✓ User already exists: {user_data['email']}")
                continue
            user = User(email=user_data["email"], role=user_data["role"])
            user.set_password(user_data["password"])
            # Expire password immediately upon creation
            user.password_changed_at = datetime.now(timezone.utc) - timedelta(days=31)
            db.session.add(user)
            print(f"  + Created {user_data['role']}: {user_data['email']}")

        # Ensure a BannerSettings row exists.
        if not BannerSettings.query.first():
            db.session.add(BannerSettings(is_active=False, message=""))
            print("  + Created default banner settings")
        else:
            print("  ✓ Banner settings already exist")

        _seed_provider_schedule()

        db.session.commit()
        print("\nDone.")


if __name__ == "__main__":
    seed()
