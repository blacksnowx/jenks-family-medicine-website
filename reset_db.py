"""Reset the database: drop all tables, recreate, and seed."""
import os, sys
sys.path.insert(0, os.path.dirname(__file__))

from app import create_app
from models import BannerSettings, User, db

app = create_app()
with app.app_context():
    print("Dropping all tables...")
    db.drop_all()
    print("Creating all tables...")
    db.create_all()
    print("Tables recreated.")

    # Seed users
    for data in [
        {"email": "brittany@jenksfamilymedicine.com", "password": "brittany123"},
        {"email": "tyler@jenksfamilymedicine.com", "password": "tyler123"},
        {"email": "anne@jenksfamilymedicine.com", "password": "anne123"},
    ]:
        user = User(email=data["email"])
        user.set_password(data["password"])
        db.session.add(user)
        print(f"  + Created user: {data['email']}")

    db.session.add(BannerSettings(is_active=False, message=""))
    print("  + Created default banner settings")
    db.session.commit()
    print("\nDone — database reset and seeded.")
