"""Diagnose DB issues on Heroku."""
import os, sys, traceback
sys.path.insert(0, os.path.dirname(__file__))

print("=== DATABASE DIAGNOSTICS ===")
url = os.environ.get("DATABASE_URL", "")
print(f"DATABASE_URL starts with: {url[:20]}...")
if url.startswith("postgres://"):
    print("NOTE: URL uses postgres://, app.py converts to postgresql://")

try:
    from app import create_app
    from models import User, BannerSettings, db
    app = create_app()
    with app.app_context():
        print(f"DB URI starts with: {app.config['SQLALCHEMY_DATABASE_URI'][:25]}...")
        # Check tables
        from sqlalchemy import inspect
        inspector = inspect(db.engine)
        tables = inspector.get_table_names()
        print(f"Tables: {tables}")
        
        if "users" in tables:
            cols = [c["name"] for c in inspector.get_columns("users")]
            print(f"Users columns: {cols}")
            count = User.query.count()
            print(f"User count: {count}")
            if count > 0:
                for u in User.query.all():
                    print(f"  User: {u.email}, hash_len={len(u.password_hash)}")
        else:
            print("ERROR: 'users' table does not exist!")
            print("Running db.create_all()...")
            db.create_all()
            print("Tables after create_all:", inspector.get_table_names())
            
        if "banner_settings" in tables:
            b = BannerSettings.query.first()
            print(f"Banner: active={b.is_active if b else 'NO ROW'}")
        else:
            print("ERROR: 'banner_settings' table does not exist!")
except Exception as e:
    print(f"EXCEPTION: {e}")
    traceback.print_exc()

print("=== DONE ===")
