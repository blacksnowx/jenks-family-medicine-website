from datetime import timedelta

from flask import Flask, render_template, url_for, redirect, jsonify, make_response, request, flash
from flask_login import LoginManager, login_user, logout_user, login_required, current_user
import datetime
import os
import time
import threading

import requests

from models import db, User, BannerSettings, ReferenceData, SyncLog, AppointmentRequest, ProviderSchedule, TebraBooking
from datetime import timezone
from data import rvu_analytics
from data import revenue_per_rvu
from data import new_patients_analytics
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

    @app.route('/welcome/primary-care')
    def welcome_primary_care():
        meta_pixel_id = os.environ.get('META_PIXEL_ID', '')
        return render_template('welcome/primary-care.html',
                               page_title='Your New Doctor is Ready to See You',
                               meta_pixel_id=meta_pixel_id)

    @app.route('/welcome/functional-medicine')
    def welcome_functional_medicine():
        meta_pixel_id = os.environ.get('META_PIXEL_ID', '')
        return render_template('welcome/functional-medicine.html',
                               page_title='Get to the Root Cause — Functional Medicine',
                               meta_pixel_id=meta_pixel_id)

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

    @app.route('/api/appointment-request', methods=['POST'])
    def appointment_request():
        """Accept appointment requests from the marketing landing pages."""
        data = request.get_json(silent=True) or request.form

        name = (data.get('name') or '').strip()
        phone = (data.get('phone') or '').strip()
        email = (data.get('email') or '').strip()
        preferred_time = (data.get('preferred_time') or '').strip()
        reason = (data.get('reason') or '').strip()
        source = (data.get('source') or '').strip()

        if not name or not phone:
            return jsonify({'success': False, 'error': 'Name and phone are required.'}), 400

        req = AppointmentRequest(
            name=name,
            phone=phone,
            email=email,
            preferred_time=preferred_time,
            reason=reason,
            source=source,
            status='new',
        )
        db.session.add(req)
        db.session.commit()

        app.logger.info(
            "New appointment request from %s (%s) via %s — phone: %s",
            name, email, source, phone
        )

        return jsonify({'success': True, 'message': 'Request received! We will contact you shortly.'})

    @app.route('/api/appointment-requests')
    @login_required
    def get_appointment_requests():
        """Return recent appointment requests for the admin dashboard."""
        if current_user.role not in ('Owner', 'Admin'):
            return jsonify({'error': 'Access denied'}), 403

        requests_list = (
            AppointmentRequest.query
            .order_by(AppointmentRequest.created_at.desc())
            .limit(20)
            .all()
        )

        return jsonify({'requests': [
            {
                'id': r.id,
                'name': r.name,
                'phone': r.phone,
                'email': r.email,
                'preferred_time': r.preferred_time,
                'reason': r.reason,
                'source': r.source,
                'status': r.status,
                'created_at': r.created_at.isoformat() + 'Z' if r.created_at else None,
            }
            for r in requests_list
        ]})

    @app.route('/api/appointment-request/<int:req_id>/status', methods=['POST'])
    @login_required
    def update_appointment_status(req_id):
        """Owner/Admin can update the status of an appointment request."""
        if current_user.role not in ('Owner', 'Admin'):
            return jsonify({'error': 'Access denied'}), 403

        req = db.session.get(AppointmentRequest, req_id)
        if not req:
            return jsonify({'error': 'Not found'}), 404

        data = request.get_json(silent=True) or request.form
        new_status = (data.get('status') or '').strip()
        if new_status not in ('new', 'contacted', 'scheduled'):
            return jsonify({'error': 'Invalid status'}), 400

        req.status = new_status
        db.session.commit()
        return jsonify({'success': True})

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
                    appt_requests_early = []
                    if current_user.role in ('Owner', 'Admin'):
                        appt_requests_early = (
                            AppointmentRequest.query
                            .order_by(AppointmentRequest.created_at.desc())
                            .limit(20)
                            .all()
                        )
                    return render_template('admin/dashboard.html',
                                           page_title='Admin Dashboard',
                                           banner=banner,
                                           needs_password_change=needs_password_change,
                                           active_tab='section-reports',
                                           rvu_view=selected_view,
                                           appt_requests=appt_requests_early)

            return redirect(url_for('admin_dashboard'))

        appt_requests = []
        if current_user.role in ('Owner', 'Admin'):
            appt_requests = (
                AppointmentRequest.query
                .order_by(AppointmentRequest.created_at.desc())
                .limit(20)
                .all()
            )

        return render_template('admin/dashboard.html', page_title='Admin Dashboard', banner=banner,
                               needs_password_change=needs_password_change, appt_requests=appt_requests)

    @app.route('/admin/reports/rvu_image')
    @login_required
    def rvu_image():
        view_type = request.args.get('view_type', 'Company Wide')
        source = request.args.get('source', 'all')
        if source not in ('all', 'pc', 'va'):
            source = 'all'
        pipeline = request.args.get('pipeline', 'false').lower() == 'true'

        # Providers must not see Anne Jenks' individual data
        is_provider = current_user.role == 'Provider'
        if is_provider and view_type.lower() in ('anne jenks',):
            return "Access denied", 403
        exclude_providers = ['ANNE JENKS'] if is_provider else None

        try:
            image_bytes = rvu_analytics.generate_rvu_chart(view_type, data_source=source, include_pipeline=pipeline, exclude_providers=exclude_providers)
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

    @app.route('/admin/reports/revenue_per_rvu')
    @login_required
    def revenue_per_rvu_report():
        if current_user.role != 'Owner':
            return jsonify({'error': 'Owner access required'}), 403
        try:
            data = revenue_per_rvu.get_revenue_per_rvu_report()
            return jsonify(data)
        except Exception as e:
            app.logger.error("Error generating revenue/RVU report: %s", e, exc_info=True)
            return jsonify({'error': 'Failed to generate report'}), 500

    @app.route('/admin/reports/revenue_per_rvu_chart')
    @login_required
    def revenue_per_rvu_chart():
        if current_user.role != 'Owner':
            return 'Forbidden', 403
        try:
            image_bytes = revenue_per_rvu.generate_revenue_per_rvu_chart()
            response = make_response(image_bytes)
            response.headers.set('Content-Type', 'image/png')
            return response
        except Exception as e:
            app.logger.error("Error generating revenue/RVU chart: %s", e, exc_info=True)
            return 'Error generating chart', 500

    @app.route('/admin/reports/new_patients')
    @login_required
    def new_patients_report():
        if current_user.role not in ('Owner', 'Admin'):
            return jsonify({'error': 'Access denied'}), 403
        try:
            data = new_patients_analytics.get_new_patients_report()
            return jsonify(data)
        except Exception as e:
            app.logger.error("Error generating new patients report: %s", e, exc_info=True)
            return jsonify({'error': 'Failed to generate report'}), 500

    @app.route('/admin/reports/new_patients_chart')
    @login_required
    def new_patients_chart():
        if current_user.role not in ('Owner', 'Admin'):
            return 'Forbidden', 403
        try:
            image_bytes = new_patients_analytics.generate_new_patients_chart()
            response = make_response(image_bytes)
            response.headers.set('Content-Type', 'image/png')
            return response
        except Exception as e:
            app.logger.error("Error generating new patients chart: %s", e, exc_info=True)
            return 'Error generating chart', 500

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
    #  Scheduling API Routes (public, no auth required)
    # -------------------------------------------------------------------

    @app.route('/api/schedule/providers')
    def schedule_providers():
        """Return distinct active providers from ProviderSchedule."""
        try:
            rows = (
                ProviderSchedule.query
                .filter_by(is_active=True)
                .with_entities(ProviderSchedule.provider_name)
                .distinct()
                .all()
            )
            providers = [r.provider_name for r in rows]
            return jsonify({'providers': providers})
        except Exception as exc:
            app.logger.error("schedule_providers error: %s", exc)
            return jsonify({'error': 'Unable to fetch providers.'}), 500

    @app.route('/api/schedule/reasons')
    def schedule_reasons():
        """Return appointment reasons from Tebra."""
        try:
            from data.tebra_appointments import get_appointment_reasons
            reasons = get_appointment_reasons()
            return jsonify({'reasons': reasons})
        except RuntimeError as exc:
            app.logger.error("schedule_reasons RuntimeError: %s", exc)
            return jsonify({'reasons': [], 'error': 'Scheduling service unavailable.'}), 503
        except Exception as exc:
            app.logger.error("schedule_reasons error: %s", exc)
            return jsonify({'reasons': [], 'error': 'Unable to fetch reasons.'}), 500

    @app.route('/api/schedule/available')
    def schedule_available():
        """
        Return available appointment slots for a provider on a given date.
        Query params: provider (str), date (YYYY-MM-DD)
        """
        provider = request.args.get('provider', '').strip()
        date_str = request.args.get('date', '').strip()

        if not provider or not date_str:
            return jsonify({'error': 'provider and date are required'}), 400

        try:
            target_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        except ValueError:
            return jsonify({'error': 'date must be YYYY-MM-DD format'}), 400

        # Reject requests for dates in the past
        if target_date < datetime.now().date():
            return jsonify({'error': 'date must be today or in the future'}), 400

        # Look up provider schedule for this day of week
        dow = target_date.weekday()  # 0=Mon, 6=Sun
        schedule = (
            ProviderSchedule.query
            .filter_by(provider_name=provider, day_of_week=dow, is_active=True)
            .first()
        )

        if not schedule:
            # Provider not scheduled on this day
            return jsonify({'slots': [], 'message': 'No availability on this day.'})

        try:
            from data.tebra_appointments import calculate_available_slots
            slots = calculate_available_slots(
                provider_name=provider,
                target_date=target_date,
                start_hour=schedule.start_hour,
                end_hour=schedule.end_hour,
                slot_minutes=schedule.slot_duration,
                start_minute=schedule.start_minute or 0,
                end_minute=schedule.end_minute or 0,
                break_start_hour=schedule.break_start_hour,
                break_end_hour=schedule.break_end_hour,
            )
        except RuntimeError as exc:
            app.logger.error("calculate_available_slots RuntimeError: %s", exc)
            return jsonify({'error': 'Scheduling service unavailable.'}), 503
        except Exception as exc:
            app.logger.error("calculate_available_slots error: %s", exc)
            return jsonify({'error': 'Unable to fetch available slots.'}), 500

        payload = [
            {
                'start': s['start'].strftime('%Y-%m-%dT%H:%M:%S'),
                'end':   s['end'].strftime('%Y-%m-%dT%H:%M:%S'),
                'label': s['label'],
            }
            for s in slots
        ]
        return jsonify({'slots': payload})

    @app.route('/api/schedule/book', methods=['POST'])
    def schedule_book():
        """
        Book a tentative appointment.
        Request JSON: provider, start_time, end_time, reason_id,
                      patient_name, patient_phone, patient_email, notes (optional)
        """
        data = request.get_json(silent=True) or {}

        required_fields = ['provider', 'start_time', 'end_time',
                            'patient_name', 'patient_phone', 'patient_email']
        missing = [f for f in required_fields if not data.get(f, '').strip()]
        if missing:
            return jsonify({'error': f"Missing fields: {', '.join(missing)}"}), 400

        provider      = data['provider'].strip()
        start_time_str = data['start_time'].strip()
        end_time_str   = data['end_time'].strip()
        reason_id     = data['reason_id'].strip()
        patient_name  = data['patient_name'].strip()
        patient_phone = data['patient_phone'].strip()
        patient_email = data['patient_email'].strip()
        notes         = data.get('notes', '').strip()

        try:
            start_dt = datetime.strptime(start_time_str, '%Y-%m-%dT%H:%M:%S')
            end_dt   = datetime.strptime(end_time_str,   '%Y-%m-%dT%H:%M:%S')
        except ValueError:
            return jsonify({'error': 'start_time and end_time must be YYYY-MM-DDTHH:MM:SS'}), 400

        # Reject bookings in the past
        if start_dt.date() < datetime.now().date():
            return jsonify({'error': 'Cannot book appointments in the past'}), 400

        tebra_id = None
        booking_status = 'pending'

        # Attempt to create a tentative appointment in Tebra
        try:
            from data.tebra_appointments import create_tentative_appointment
            tebra_id = create_tentative_appointment(
                provider_name=provider,
                start_time=start_dt,
                end_time=end_dt,
                reason_id=reason_id,
                patient_name=patient_name,
                patient_phone=patient_phone,
                patient_email=patient_email,
                notes=notes,
            )
            booking_status = 'booked' if tebra_id else 'pending'
        except RuntimeError as exc:
            app.logger.error("create_tentative_appointment RuntimeError: %s", exc)
            # Continue — still record the request locally even if Tebra is unavailable
        except Exception as exc:
            app.logger.error("create_tentative_appointment error: %s", exc)

        # Always persist the request in the local database
        try:
            booking = TebraBooking(
                provider_name=provider,
                start_time=start_dt,
                end_time=end_dt,
                reason_id=reason_id,
                patient_name=patient_name,
                patient_phone=patient_phone,
                patient_email=patient_email,
                notes=notes,
                tebra_appt_id=tebra_id,
                status=booking_status,
            )
            db.session.add(booking)
            db.session.commit()
        except Exception as exc:
            app.logger.error("TebraBooking DB save error: %s", exc)
            db.session.rollback()
            return jsonify({'error': 'Unable to save appointment request.'}), 500

        return jsonify({
            'success': True,
            'message': 'Your appointment request has been received. We will confirm shortly.',
            'appointment_id': tebra_id,
            'status': booking_status,
        })

    # -------------------------------------------------------------------
    #  Admin Schedule Management Routes (Owner only)
    # -------------------------------------------------------------------

    @app.route('/admin/schedule/templates')
    @login_required
    def admin_schedule_templates():
        if current_user.role != 'Owner':
            return jsonify({'error': 'Owner access required'}), 403
        templates = ProviderSchedule.query.order_by(
            ProviderSchedule.provider_name, ProviderSchedule.day_of_week
        ).all()
        return jsonify({'templates': [t.to_dict() for t in templates]})

    @app.route('/admin/schedule/templates/<int:template_id>', methods=['POST'])
    @login_required
    def admin_schedule_update_template(template_id):
        if current_user.role != 'Owner':
            return jsonify({'error': 'Owner access required'}), 403
        tmpl = db.session.get(ProviderSchedule, template_id)
        if not tmpl:
            return jsonify({'error': 'Not found'}), 404
        data = request.get_json(silent=True) or {}
        for field in ('start_hour', 'start_minute', 'end_hour', 'end_minute',
                      'slot_duration', 'break_start_hour', 'break_end_hour'):
            if field in data:
                val = data[field]
                setattr(tmpl, field, int(val) if val is not None else None)
        if 'is_active' in data:
            tmpl.is_active = bool(data['is_active'])
        if 'provider_tebra_id' in data:
            tmpl.provider_tebra_id = str(data['provider_tebra_id']).strip() or None
        db.session.commit()
        return jsonify({'success': True})

    @app.route('/admin/schedule/bookings')
    @login_required
    def admin_schedule_bookings():
        """Return recent Tebra booking requests (Owner only)."""
        if current_user.role != 'Owner':
            return jsonify({'error': 'Owner access required'}), 403
        bookings = (
            TebraBooking.query
            .order_by(TebraBooking.created_at.desc())
            .limit(50)
            .all()
        )
        result = []
        for b in bookings:
            result.append({
                'id': b.id,
                'provider_name': b.provider_name,
                'start_time': b.start_time.isoformat() if b.start_time else None,
                'end_time': b.end_time.isoformat() if b.end_time else None,
                'patient_name': b.patient_name,
                'patient_phone': b.patient_phone,
                'patient_email': b.patient_email,
                'reason_id': b.reason_id,
                'status': b.status,
                'tebra_appt_id': b.tebra_appt_id,
                'created_at': b.created_at.isoformat() + 'Z' if b.created_at else None,
            })
        return jsonify({'bookings': result})

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
