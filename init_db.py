"""Initialise the database and seed admin accounts.

Run with:  python init_db.py

Idempotent — existing users are left untouched.
"""

import os
import sys

# Ensure the project root is on the path so we can import app / models.
sys.path.insert(0, os.path.dirname(__file__))

from app import create_app  # noqa: E402
from models import BannerSettings, User, db  # noqa: E402

SEED_USERS = [
    {"email": "brittany@jenksfamilymedicine.com", "password": "brittany123"},
    {"email": "tyler@jenksfamilymedicine.com", "password": "tyler123"},
    {"email": "anne@jenksfamilymedicine.com", "password": "anne123"},
]


def seed() -> None:
    app = create_app()
    with app.app_context():
        db.create_all()

        for user_data in SEED_USERS:
            existing = User.query.filter_by(email=user_data["email"]).first()
            if existing:
                print(f"  ✓ User already exists: {user_data['email']}")
                continue
            user = User(email=user_data["email"])
            user.set_password(user_data["password"])
            db.session.add(user)
            print(f"  + Created user: {user_data['email']}")

        # Ensure a BannerSettings row exists.
        if not BannerSettings.query.first():
            db.session.add(BannerSettings(is_active=False, message=""))
            print("  + Created default banner settings")
        else:
            print("  ✓ Banner settings already exist")

        db.session.commit()
        print("\nDone.")


if __name__ == "__main__":
    seed()
