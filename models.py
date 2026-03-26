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

