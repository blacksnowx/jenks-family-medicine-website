"""
Pytest configuration and shared fixtures for the Jenks Family Medicine test suite.

All tests that modify the database use function-scoped fixtures so each test
starts with a clean, seeded in-memory SQLite database.
"""

import os
import sys

import pytest

# Set required env vars before any project module is imported.
os.environ.setdefault("PII_HASH_SECRET", "test-pii-hash-secret-min-32-chars-for-tests!!")
os.environ.setdefault("SECRET_KEY", "test-flask-secret-key")
# Force SQLite for the module-level app = create_app() that runs on import.
# Without this, the module tries to open instance/site.db which may not exist.
os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")
# Ensure Tebra creds are absent by default so tests don't accidentally hit the real API.
os.environ.pop("TEBRAKEY", None)

# Make sure the project root is on the path.
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from datetime import datetime, timezone, timedelta

from app import create_app
from models import db as _db, User, BannerSettings, ProviderSchedule, AppointmentRequest


# ---------------------------------------------------------------------------
# Test date constants — far enough in the future to avoid "past date" errors
# ---------------------------------------------------------------------------

# 2030-01-07 is a Monday (weekday 0)
TEST_DATE = "2030-01-07"
TEST_START_TIME = f"{TEST_DATE}T09:00:00"
TEST_END_TIME = f"{TEST_DATE}T09:30:00"
TEST_PROVIDER = "Dr. Test Provider"


# ---------------------------------------------------------------------------
# App / DB fixtures
# ---------------------------------------------------------------------------

def _seed_db():
    """Seed test users, banner, and provider schedules."""
    # Users
    owner = User(email="owner@test.com", role="Owner")
    owner.set_password("Ownerpwd1!")
    owner.password_changed_at = datetime.now(timezone.utc)

    admin = User(email="admin@test.com", role="Admin")
    admin.set_password("Adminpwd1!")
    admin.password_changed_at = datetime.now(timezone.utc)

    provider = User(email="provider@test.com", role="Provider")
    provider.set_password("Provider1!")
    provider.password_changed_at = datetime.now(timezone.utc)

    expired = User(email="expired@test.com", role="Owner")
    expired.set_password("Expiredpw1!")
    expired.password_changed_at = datetime.now(timezone.utc) - timedelta(days=31)

    _db.session.add_all([owner, admin, provider, expired])

    # Banner
    banner = BannerSettings(is_active=False, message="")
    _db.session.add(banner)

    # Provider schedule — Mon–Fri 8am–5pm in 30-min slots for TEST_PROVIDER
    for dow in range(5):
        schedule = ProviderSchedule(
            provider_name=TEST_PROVIDER,
            day_of_week=dow,
            start_hour=8,
            end_hour=17,
            slot_duration=30,
            is_active=True,
        )
        _db.session.add(schedule)

    _db.session.commit()


@pytest.fixture
def app():
    """Fresh Flask application with an in-memory SQLite DB, seeded for each test."""
    application = create_app()
    application.config.update(
        {
            "TESTING": True,
            "SQLALCHEMY_DATABASE_URI": "sqlite:///:memory:",
            "WTF_CSRF_ENABLED": False,
            "SECRET_KEY": "test-flask-secret-key",
        }
    )
    with application.app_context():
        _db.create_all()
        _seed_db()
        yield application
        _db.session.remove()
        _db.drop_all()


@pytest.fixture
def client(app):
    """Flask test client."""
    return app.test_client()


@pytest.fixture
def db(app):
    """Database session bound to the test app context."""
    return _db


# ---------------------------------------------------------------------------
# Authenticated client fixtures
# ---------------------------------------------------------------------------

def _login(client, email, password):
    client.post(
        "/admin",
        data={"email": email, "password": password},
        follow_redirects=True,
    )


@pytest.fixture
def owner_client(client):
    _login(client, "owner@test.com", "Ownerpwd1!")
    yield client
    client.get("/admin/logout", follow_redirects=True)


@pytest.fixture
def admin_client(client):
    _login(client, "admin@test.com", "Adminpwd1!")
    yield client
    client.get("/admin/logout", follow_redirects=True)


@pytest.fixture
def provider_client(client):
    _login(client, "provider@test.com", "Provider1!")
    yield client
    client.get("/admin/logout", follow_redirects=True)
