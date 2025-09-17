from flask import Flask, render_template, url_for, redirect, jsonify
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
def inject_current_year():
    return {"current_year": get_current_year()}


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


if __name__ == '__main__':
    # Set debug=False when deploying to production (Heroku sets this via env var)
    # For local testing, you can use debug=True
    app.run(debug=False)  # Set debug=False for Heroku

