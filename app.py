from flask import Flask, render_template, url_for, redirect, jsonify, make_response, request
import datetime
import os
import time

import requests

app = Flask(__name__)


# Function to get the current year for the footer

def get_current_year():
    return datetime.datetime.now().year


# Register the function to be available in all templates


@app.context_processor
def inject_globals():
    return {
        "current_year": get_current_year(),
        "page_url": request.base_url
    }


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


# --- Page Routes ---


@app.route('/')
def index():
    # Passing page_title explicitly for the homepage
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
    # Option 1: Info page (as designed below)
    return render_template('patient-portal.html', page_title='Patient Portal')

    # Option 2: Directly redirect to Kareo (uncomment if preferred)
    # return redirect("https://portal.kareo.com", code=302)


@app.route('/privacy-policy')
def privacy_policy():
    return render_template('privacy-policy.html', page_title='Privacy Policy')


@app.route('/landing')
def landing():
    return render_template('landing.html', page_title='Schedule Appointment')


# --- Add other potential routes if needed (e.g., for form submissions) ---


@app.route('/summer_flyer')
def summer_flyer_promo():
    return render_template('summer_flyer.html', page_title='Summer Flyer')


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


@app.route('/sitemap.xml')
def sitemap():
    """Generate sitemap.xml dynamically."""
    host_components = url_for("index", _external=True).split("/")
    base_url = "/".join(host_components[:3])  # simple way to get protocol + domain

    # Static pages
    pages = [
        {"loc": f"{base_url}{url_for('index')}", "changefreq": "weekly", "priority": "1.0"},
        {"loc": f"{base_url}{url_for('about')}", "changefreq": "monthly", "priority": "0.8"},
        {"loc": f"{base_url}{url_for('services')}", "changefreq": "monthly", "priority": "0.8"},
        {"loc": f"{base_url}{url_for('functional_medicine')}", "changefreq": "monthly", "priority": "0.8"},
        {"loc": f"{base_url}{url_for('veteran_resources')}", "changefreq": "monthly", "priority": "0.8"},
        {"loc": f"{base_url}{url_for('patient_portal')}", "changefreq": "monthly", "priority": "0.6"},
        {"loc": f"{base_url}{url_for('contact')}", "changefreq": "monthly", "priority": "0.7"},
        {"loc": f"{base_url}{url_for('privacy_policy')}", "changefreq": "yearly", "priority": "0.3"},
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


if __name__ == '__main__':
    # Set debug=False when deploying to production (Heroku sets this via env var)
    # For local testing, you can use debug=True
    app.run(debug=False)  # Set debug=False for Heroku

