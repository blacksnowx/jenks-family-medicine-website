from datetime import timedelta

from flask import Flask, render_template, url_for, redirect, jsonify, make_response, request, flash
from flask_login import LoginManager, login_user, logout_user, login_required, current_user
import datetime
import os
import time
import threading

import requests

from models import db, User, BannerSettings, ReferenceData, SyncLog
from datetime import timezone
from data import rvu_analytics
from data.data_loader import get_database_url
from data import owner_analytics
from data.pii_utils import hash_pii_columns

def create_app():
    app = Flask(__name__)

    # --- Configuration ---
    app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "dev-secret-change-me")

    # Database — prefer DATABASE_URL (Heroku Postgres), fall back to local SQLite.
    app.config["SQLALCHEMY_DATABASE_URI"] = get_database_url()
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

    # Session timeout — 5 minutes of inactivity
    app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(minutes=5)

    # --- Extensions ---
    db.init_app(app)
    login_manager = LoginManager()
    login_manager.init_app(app)
    login_manager.login_view = "admin_login"

    @login_manager.user_loader
    def load_user(user_id):
        return db.session.get(User, int(user_id))

    # Ensure tables exist on first request (safe for production; init_db.py handles seeding).
    with app.app_context():
        db.create_all()

    # -------------------------------------------------------------------
    #  Helpers
    # -------------------------------------------------------------------

    def get_current_year():
        return datetime.datetime.now().year

    @app.context_processor
    def inject_globals():
        banner = BannerSettings.query.first()
        return {
            "current_year": get_current_year(),
            "page_url": request.base_url,
            "banner": banner,
        }

    @app.before_request
    def refresh_session():
        """Keep the session alive on each request (rolling expiry)."""
        from flask import session
        session.permanent = True

    # -------------------------------------------------------------------
    #  Google Reviews cache (unchanged)
    # -------------------------------------------------------------------

    REVIEWS_CACHE = {"timestamp": 0, "payload": None}

    def get_reviews_cache_ttl():
        """Return the cache TTL for Google review responses."""
        try:
            return int(os.getenv("GOOGLE_REVIEWS_CACHE_TTL", "3600"))
        except ValueError:
            return 3600

    def fetch_google_reviews(force_refresh: bool = False):
        """Fetch five-star Google reviews for the configured place."""

        api_key = os.getenv("GOOGLE_PLACES_API_KEY")
        place_id = os.getenv("GOOGLE_PLACES_PLACE_ID")

        if not api_key or not place_id:
            raise RuntimeError(
                "Google Places API configuration is missing. Set GOOGLE_PLACES_API_KEY and GOOGLE_PLACES_PLACE_ID."
            )

        cache_ttl = get_reviews_cache_ttl()
        now = time.time()

        if not force_refresh and REVIEWS_CACHE["payload"] is not None:
            if now - REVIEWS_CACHE["timestamp"] < cache_ttl:
                return REVIEWS_CACHE["payload"]

        endpoint = "https://maps.googleapis.com/maps/api/place/details/json"
        params = {
            "place_id": place_id,
            "fields": "rating,reviews,user_ratings_total",
            "reviews_sort": "newest",
            "language": "en",
            "key": api_key,
        }

        try:
            response = requests.get(endpoint, params=params, timeout=10)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise RuntimeError("Unable to reach the Google Places API.") from exc

        payload = response.json()
        status = payload.get("status")
        if status != "OK":
            message = payload.get("error_message") or status or "Unknown error"
            raise RuntimeError(f"Google Places API error: {message}")

        result = payload.get("result", {})
        raw_reviews = result.get("reviews", [])

        five_star_reviews = []
        for review in raw_reviews:
            if review.get("rating") != 5:
                continue

            text = (review.get("text") or "").strip()
            if not text:
                continue

            five_star_reviews.append(
                {
                    "author_name": review.get("author_name"),
                    "author_url": review.get("author_url"),
                    "profile_photo_url": review.get("profile_photo_url"),
                    "rating": review.get("rating"),
                    "text": text,
                    "time": review.get("time"),
                    "relative_time_description": review.get("relative_time_description"),
                }
            )

        five_star_reviews.sort(key=lambda item: item.get("time") or 0, reverse=True)

        payload_summary = {
            "reviews": five_star_reviews,
            "rating": result.get("rating"),
            "total_ratings": result.get("user_ratings_total"),
            "fetched_at": datetime.datetime.utcnow().isoformat() + "Z",
        }

        REVIEWS_CACHE["payload"] = payload_summary
        REVIEWS_CACHE["timestamp"] = now

        return payload_summary

    # -------------------------------------------------------------------
    #  Public Page Routes
    # -------------------------------------------------------------------

    @app.route('/')
    def index():
        return render_template('index.html', page_title='Welcome')

    @app.route('/about')
    def about():
        return render_template('about.html', page_title='About Us')

    @app.route('/services-insurance')
    def services():
        return render_template('services.html', page_title='Services & Insurance')

    @app.route('/functional-medicine')
    def functional_medicine():
        return render_template('functional-medicine.html', page_title='Functional Medicine')

    @app.route('/veteran-resources')
    def veteran_resources():
        return render_template('veteran-resources.html', page_title='Veteran Resources')

    @app.route('/contact')
    def contact():
        return render_template('contact.html', page_title='Contact Us')

    @app.route('/patient-portal')
    def patient_portal():
        return render_template('patient-portal.html', page_title='Patient Portal')

    @app.route('/privacy-policy')
    def privacy_policy():
        return render_template('privacy-policy.html', page_title='Privacy Policy')

    @app.route('/landing')
    def landing():
        return render_template('landing.html', page_title='Schedule Appointment')

    @app.route('/new-patient')
    def new_patient():
        return render_template('new-patient.html', page_title='Choose Your Provider')

    @app.route('/summer_flyer')
    def summer_flyer_promo():
        return render_template('summer_flyer.html', page_title='Summer Flyer')

    # -------------------------------------------------------------------
    #  API Routes
    # -------------------------------------------------------------------

    @app.route('/api/google-reviews')
    def google_reviews():
        try:
            payload = fetch_google_reviews()
        except RuntimeError as exc:
            app.logger.warning("Google reviews unavailable: %s", exc)
            return (
                jsonify(
                    {
                        "reviews": [],
                        "error": "Google reviews are not available right now.",
                    }
                ),
                503,
            )

        reviews = payload.get("reviews", [])[:10]

        return jsonify(
            {
                "reviews": reviews,
                "rating": payload.get("rating"),
                "total_ratings": payload.get("total_ratings"),
                "fetched_at": payload.get("fetched_at"),
            }
        )

    # -------------------------------------------------------------------
    #  Admin Routes
    # -------------------------------------------------------------------

    @app.route('/admin', methods=['GET', 'POST'])
    def admin_login():
        if current_user.is_authenticated:
            return redirect(url_for('admin_dashboard'))

        if request.method == 'POST':
            email = request.form.get('email', '').strip().lower()
            password = request.form.get('password', '')
            user = User.query.filter_by(email=email).first()

            if user and user.check_password(password):
                login_user(user)
                return redirect(url_for('admin_dashboard'))
            else:
                flash('Invalid email or password.', 'error')

        return render_template('admin/login.html', page_title='Admin Login')

    @app.route('/admin/dashboard', methods=['GET', 'POST'])
    @login_required
    def admin_dashboard():
        # Check if password needs to be changed (older than 30 days)
        now = datetime.datetime.now(timezone.utc)
        needs_password_change = False
        if current_user.password_changed_at:
            pwd_changed = current_user.password_changed_at
            # Make sure pwd_changed is timezone aware before subtracting
            if pwd_changed.tzinfo is None:
                pwd_changed = pwd_changed.replace(tzinfo=timezone.utc)
            password_age = now - pwd_changed
            if password_age.days >= 30:
                needs_password_change = True
        else:
             # Just in case there is no timestamp
             needs_password_change = True

        banner = BannerSettings.query.first()
        if not banner:
            banner = BannerSettings(is_active=False, message="")
            db.session.add(banner)
            db.session.commit()

        if request.method == 'POST':
            action = request.form.get('action')

            if action == 'update_banner':
                if needs_password_change:
                     flash('You must change your password before performing any actions.', 'error')
                elif current_user.role not in ['Owner', 'Admin']:
                     flash('You do not have permission to perform this action.', 'error')
                else:
                    banner.is_active = request.form.get('is_active') == 'on'
                    banner.message = request.form.get('message', '').strip()
                    db.session.commit()
                    flash('Banner updated successfully.', 'success')

            elif action == 'upload_reference_data':
                if needs_password_change:
                     flash('You must change your password before performing any actions.', 'error')
                elif current_user.role != 'Owner':
                     flash('Only Owners can upload reference data.', 'error')
                else:
                    file = request.files.get('reference_file')
                    file_type = request.form.get('reference_file_type')
                    if not file or not file.filename:
                        flash('No file selected.', 'error')
                    elif not file_type:
                        flash('No file type selected.', 'error')
                    else:
                        import io
                        import pandas as pd

                        # PII columns to hash per file type
                        PII_COLUMN_MAPS = {
                            'Charges Export.csv': {
                                'Patient ID':   'pid_',
                                'Encounter ID': 'eid_',
                            },
                            '201 Bills and Payments.csv': {
                                'Patient_ID': 'pid_',
                            },
                        }

                        raw_bytes = file.read()
                        column_map = PII_COLUMN_MAPS.get(file_type)
                        if column_map:
                            try:
                                df = pd.read_csv(io.BytesIO(raw_bytes))
                                df = hash_pii_columns(df, column_map)
                                buf = io.BytesIO()
                                df.to_csv(buf, index=False)
                                file_data = buf.getvalue()
                            except Exception:
                                file_data = raw_bytes
                        else:
                            file_data = raw_bytes

                        ref = ReferenceData.query.filter_by(filename=file_type).first()
                        if not ref:
                            ref = ReferenceData(filename=file_type, data=file_data)
                            db.session.add(ref)
                        else:
                            ref.data = file_data
                            ref.updated_at = datetime.datetime.now(timezone.utc)
                        db.session.commit()
                        flash(f'Successfully uploaded and replaced {file_type}.', 'success')

            elif action == 'change_password':
                current_password = request.form.get('current_password', '')
                new_password = request.form.get('new_password', '')
                confirm_password = request.form.get('confirm_password', '')

                if not current_user.check_password(current_password):
                    flash('Current password is incorrect.', 'error')
                elif new_password != confirm_password:
                    flash('New passwords do not match.', 'error')
                else:
                    is_valid, error_msg = User.validate_password(new_password)
                    if not is_valid:
                        flash(error_msg, 'error')
                    else:
                        current_user.set_password(new_password)
                        db.session.commit()
                        flash('Password changed successfully.', 'success')
                        needs_password_change = False

            elif action == 'generate_rvu_report':
                if needs_password_change:
                    flash('You must change your password before performing any actions.', 'error')
                else:
                    selected_view = request.form.get('rvu_view', 'Company Wide')
                    
                    # Store selected view in session or pass directly to template
                    # For simplicity, we just render the template with the selection
                    return render_template('admin/dashboard.html', 
                                           page_title='Admin Dashboard', 
                                           banner=banner, 
                                           needs_password_change=needs_password_change,
                                           active_tab='section-reports',
                                           rvu_view=selected_view)

            return redirect(url_for('admin_dashboard'))

        return render_template('admin/dashboard.html', page_title='Admin Dashboard', banner=banner, needs_password_change=needs_password_change)

    @app.route('/admin/reports/rvu_image')
    @login_required
    def rvu_image():
        view_type = request.args.get('view_type', 'Company Wide')
        source = request.args.get('source', 'all')
        if source not in ('all', 'pc', 'va'):
            source = 'all'
        pipeline = request.args.get('pipeline', 'false').lower() == 'true'
        try:
            image_bytes = rvu_analytics.generate_rvu_chart(view_type, data_source=source, include_pipeline=pipeline)
            response = make_response(image_bytes)
            response.headers.set('Content-Type', 'image/png')
            return response
        except Exception as e:
            app.logger.error("Error generating RVU chart: %s", e, exc_info=True)
            return "Error generating chart", 500

    @app.route('/admin/reports/bonus')
    @login_required
    def bonus_report():
        if current_user.role != 'Owner':
            return jsonify({'error': 'Forbidden'}), 403
        source = request.args.get('source', 'all')
        if source not in ('all', 'pc', 'va'):
            source = 'all'
        pipeline = request.args.get('pipeline', 'false').lower() == 'true'
        try:
            data = rvu_analytics.get_quarterly_bonus_report(data_source=source, include_pipeline=pipeline)
            return jsonify(data)
        except Exception as e:
            app.logger.error("Error generating bonus report: %s", e, exc_info=True)
            return jsonify({'error': 'Failed to generate report'}), 500

    @app.route('/admin/reports/owner_analytics')
    @login_required
    def owner_analytics_data():
        if current_user.role != 'Owner':
            return jsonify({'error': 'Owner access required'}), 403
        try:
            data = owner_analytics.get_all_analytics()
            return jsonify(data)
        except Exception as e:
            app.logger.error("Error generating owner analytics: %s", e, exc_info=True)
            return jsonify({'error': 'Failed to generate analytics data'}), 500

    # -------------------------------------------------------------------
    #  Sync Routes
    # -------------------------------------------------------------------

    @app.route('/admin/sync', methods=['POST'])
    @login_required
    def admin_sync():
        """Owner-only endpoint to trigger a manual data sync."""
        if current_user.role != 'Owner':
            return jsonify({'error': 'Owner access required'}), 403

        sync_type = request.json.get('sync_type', 'all') if request.is_json else request.form.get('sync_type', 'all')

        if sync_type not in ('tebra', 'sheets', 'all', 'draft'):
            return jsonify({'error': f"Invalid sync_type '{sync_type}'. Must be 'tebra', 'sheets', 'draft', or 'all'."}), 400

        try:
            from data import sync_manager

            def _run_sync():
                with app.app_context():
                    try:
                        if sync_type == 'tebra':
                            sync_manager.run_tebra_sync()
                        elif sync_type == 'sheets':
                            sync_manager.run_sheets_sync()
                        elif sync_type == 'draft':
                            sync_manager.run_draft_sync()
                        else:
                            sync_manager.run_all_syncs()
                    except Exception as exc:
                        app.logger.error("Background sync error (%s): %s", sync_type, exc)

            t = threading.Thread(target=_run_sync, daemon=True)
            t.start()
            return jsonify({'status': 'started', 'message': f'{sync_type} sync started in background. Check status for progress.'})

        except Exception as exc:
            app.logger.error("Sync endpoint error (%s): %s", sync_type, exc)
            return jsonify({'error': 'Sync failed to start. Check server logs.'}), 500

    @app.route('/admin/sync/status')
    @login_required
    def admin_sync_status():
        """Return the latest sync log entries for both sync types."""
        if current_user.role != 'Owner':
            return jsonify({'error': 'Owner access required'}), 403

        logs = (
            SyncLog.query
            .order_by(SyncLog.started_at.desc())
            .limit(20)
            .all()
        )

        entries = []
        for log in logs:
            entries.append({
                'id':              log.id,
                'sync_type':       log.sync_type,
                'status':          log.status,
                'records_fetched': log.records_fetched,
                'records_new':     log.records_new,
                'started_at':      log.started_at.isoformat() + 'Z' if log.started_at else None,
                'completed_at':    log.completed_at.isoformat() + 'Z' if log.completed_at else None,
                'error_message':   log.error_message,
                'last_sync_date':  log.last_sync_date.isoformat() + 'Z' if log.last_sync_date else None,
            })

        return jsonify({'logs': entries})

    @app.route('/api/sync/trigger')
    def api_sync_trigger():
        """
        Unauthenticated endpoint for Heroku Scheduler (or similar cron runners).
        Requires the SYNC_SECRET env var to be passed as ?secret=<value>.
        """
        expected_secret = os.environ.get('SYNC_SECRET', '')
        if not expected_secret:
            return jsonify({'error': 'SYNC_SECRET is not configured on this server.'}), 503

        provided_secret = request.args.get('secret', '')
        if not provided_secret or provided_secret != expected_secret:
            return jsonify({'error': 'Unauthorized'}), 401

        sync_type = request.args.get('sync_type', 'all')
        if sync_type not in ('tebra', 'sheets', 'all', 'draft'):
            sync_type = 'all'

        try:
            from data import sync_manager
            if sync_type == 'tebra':
                result = sync_manager.run_tebra_sync()
            elif sync_type == 'sheets':
                result = sync_manager.run_sheets_sync()
            elif sync_type == 'draft':
                result = sync_manager.run_draft_sync()
            else:
                result = sync_manager.run_all_syncs()

            return jsonify(result)

        except Exception as exc:
            app.logger.error("Scheduled sync error (%s): %s", sync_type, exc)
            return jsonify({'error': 'Sync failed. Check server logs.'}), 500

    @app.route('/admin/logout')
    @login_required
    def admin_logout():
        logout_user()
        flash('You have been logged out.', 'info')
        return redirect(url_for('admin_login'))

    # -------------------------------------------------------------------
    #  SEO Routes
    # -------------------------------------------------------------------

    @app.route('/sitemap.xml')
    def sitemap():
        """Generate sitemap.xml dynamically."""
        host_components = url_for("index", _external=True).split("/")
        base_url = "/".join(host_components[:3])

        pages = [
            {"loc": f"{base_url}{url_for('index')}", "changefreq": "weekly", "priority": "1.0"},
            {"loc": f"{base_url}{url_for('about')}", "changefreq": "monthly", "priority": "0.8"},
            {"loc": f"{base_url}{url_for('services')}", "changefreq": "monthly", "priority": "0.8"},
            {"loc": f"{base_url}{url_for('functional_medicine')}", "changefreq": "monthly", "priority": "0.8"},
            {"loc": f"{base_url}{url_for('veteran_resources')}", "changefreq": "monthly", "priority": "0.8"},
            {"loc": f"{base_url}{url_for('patient_portal')}", "changefreq": "monthly", "priority": "0.6"},
            {"loc": f"{base_url}{url_for('contact')}", "changefreq": "monthly", "priority": "0.7"},
            {"loc": f"{base_url}{url_for('privacy_policy')}", "changefreq": "yearly", "priority": "0.3"},
            {"loc": f"{base_url}{url_for('new_patient')}", "changefreq": "monthly", "priority": "0.8"},
        ]

        sitemap_xml = render_template('sitemap_template.xml', pages=pages)
        response = make_response(sitemap_xml)
        response.headers["Content-Type"] = "application/xml"
        return response

    @app.route('/robots.txt')
    def robots():
        """Generate robots.txt dynamically."""
        base_url = request.url_root.rstrip('/')
        lines = [
            "User-agent: *",
            "Allow: /",
            f"Sitemap: {base_url}/sitemap.xml"
        ]
        response = make_response("\n".join(lines))
        response.headers["Content-Type"] = "text/plain"
        return response

    return app


# --- Application entry-point ---
app = create_app()

if __name__ == '__main__':
    app.run(debug=False)
