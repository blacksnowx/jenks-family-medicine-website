"""Database models for admin authentication and site settings."""

from datetime import datetime, timezone

from flask_login import UserMixin
from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import check_password_hash, generate_password_hash

db = SQLAlchemy()


class User(UserMixin, db.Model):
    """Admin user account."""

    __tablename__ = "users"

    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(120), unique=True, nullable=False)
    password_hash = db.Column(db.String(256), nullable=False)
    role = db.Column(db.String(20), nullable=False, default="Provider")
    password_changed_at = db.Column(db.DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))

    @staticmethod
    def validate_password(password: str) -> tuple[bool, str]:
        """
        Validates password meets the complexity requirements:
        10-18 length, alphanumeric characters allowed, plus spaces and special characters.
        """
        if len(password) < 10 or len(password) > 18:
            return False, "Password must be between 10 and 18 characters long."
            
        import re
        # Allow alphanumeric (a-z, A-Z, 0-9), spaces, and special characters.
        # This basically means anything EXCEPT control characters or non-ASCII if you want to be strict,
        # but the prompt specifies "a-zA-Z0-9 + special chars + spaces". We don't need the hex check anymore.
        # If there are any characters that are NOT in the allowed set, reject it.
        # \w covers a-zA-Z0-9_, \s covers spaces, and string.punctuation covers specials.
        # Alternatively, checking for non-allowed chars is easier:
        if re.search(r'[^a-zA-Z0-9\s!"#$%&\'()*+,\-./:;<=>?@\[\\\]^_`{|}~]', password):
             return False, "Password may only contain alphanumeric characters, spaces, and special characters."
             
        return True, ""

    def set_password(self, password: str) -> None:
        self.password_hash = generate_password_hash(password)
        self.password_changed_at = datetime.now(timezone.utc)

    def check_password(self, password: str) -> bool:
        return check_password_hash(self.password_hash, password)

    def __repr__(self) -> str:
        return f"<User {self.email}>"


class BannerSettings(db.Model):
    """Singleton row that stores the homepage banner state."""

    __tablename__ = "banner_settings"

    id = db.Column(db.Integer, primary_key=True)
    is_active = db.Column(db.Boolean, default=False, nullable=False)
    message = db.Column(db.Text, default="", nullable=False)
    updated_at = db.Column(
        db.DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc)
    )

    def __repr__(self) -> str:
        return f"<BannerSettings active={self.is_active}>"


class ReferenceData(db.Model):
    """Stores the reference CSV files securely in the database."""

    __tablename__ = "reference_data"

    id = db.Column(db.Integer, primary_key=True)
    filename = db.Column(db.String(100), unique=True, nullable=False)
    data = db.Column(db.LargeBinary, nullable=False)
    updated_at = db.Column(
        db.DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc)
    )

    def __repr__(self) -> str:
        return f"<ReferenceData {self.filename}>"


class AppointmentRequest(db.Model):
    """Appointment requests submitted via the marketing landing pages."""

    __tablename__ = "appointment_requests"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    phone = db.Column(db.String(20), nullable=False)
    email = db.Column(db.String(120), nullable=False)
    preferred_time = db.Column(db.String(50))
    reason = db.Column(db.Text)
    source = db.Column(db.String(50))  # 'primary-care' or 'functional-medicine'
    status = db.Column(db.String(20), default="new")  # new / contacted / scheduled
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    notes = db.Column(db.Text)

    def __repr__(self) -> str:
        return f"<AppointmentRequest {self.name} ({self.source}) [{self.status}]>"


class SyncLog(db.Model):
    """Audit log for automated data sync runs (Tebra and Google Sheets)."""

    __tablename__ = "sync_log"

    id = db.Column(db.Integer, primary_key=True)
    sync_type = db.Column(db.String(50), nullable=False)   # 'tebra' or 'sheets'
    status = db.Column(db.String(20), nullable=False)      # 'running', 'success', 'error'
    records_fetched = db.Column(db.Integer, default=0)
    records_new = db.Column(db.Integer, default=0)
    started_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    completed_at = db.Column(db.DateTime, nullable=True)
    error_message = db.Column(db.Text, nullable=True)
    last_sync_date = db.Column(db.DateTime, nullable=True)  # Upper bound of the synced date range

    def __repr__(self) -> str:
        return f"<SyncLog {self.sync_type} {self.status} @ {self.started_at}>"


class ProviderSchedule(db.Model):
    """Defines schedulable hours for a provider (used by the online booking widget)."""

    __tablename__ = "provider_schedule"

    id            = db.Column(db.Integer, primary_key=True)
    provider_name = db.Column(db.String(100))
    day_of_week   = db.Column(db.Integer)   # 0=Mon, 6=Sun
    start_hour    = db.Column(db.Integer, default=8)
    end_hour      = db.Column(db.Integer, default=17)
    slot_duration = db.Column(db.Integer, default=30)
    is_active     = db.Column(db.Boolean, default=True)

    def __repr__(self) -> str:
        return f"<ProviderSchedule {self.provider_name} dow={self.day_of_week}>"


class TebraBooking(db.Model):
    """Tebra-integrated booking requests from the slot-picker scheduling widget."""

    __tablename__ = "tebra_bookings"

    id              = db.Column(db.Integer, primary_key=True)
    provider_name   = db.Column(db.String(100), nullable=False)
    start_time      = db.Column(db.DateTime, nullable=False)
    end_time        = db.Column(db.DateTime, nullable=False)
    reason_id       = db.Column(db.String(50))
    patient_name    = db.Column(db.String(200), nullable=False)
    patient_phone   = db.Column(db.String(30), nullable=False)
    patient_email   = db.Column(db.String(200), nullable=False)
    notes           = db.Column(db.Text, default="")
    tebra_appt_id   = db.Column(db.String(50))   # ID returned by CreateAppointment (may be None)
    status          = db.Column(db.String(20), default="pending")  # pending / booked / error
    created_at      = db.Column(
        db.DateTime, default=lambda: datetime.now(timezone.utc)
    )

    def __repr__(self) -> str:
        return f"<TebraBooking {self.patient_name} @ {self.start_time}>"
