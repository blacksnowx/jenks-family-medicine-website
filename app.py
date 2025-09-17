import datetime
import os
import time
from typing import Optional

import requests
from flask import Flask, render_template, url_for, redirect, jsonify

app = Flask(__name__)


# Function to get the current year for the footer

def get_current_year():
    return datetime.datetime.now().year


# Register the function to be available in all templates


@app.context_processor
def inject_current_year():
    return {"current_year": get_current_year()}


REVIEWS_CACHE = {"timestamp": 0, "payload": None, "place_id": None}
PLACE_ID_CACHE = {"timestamp": 0, "value": None, "query": None}


def get_reviews_cache_ttl():
    """Return the cache TTL for Google review responses."""

    try:
        return int(os.getenv("GOOGLE_REVIEWS_CACHE_TTL", "3600"))
    except ValueError:
        return 3600


def get_place_id_cache_ttl():
    """Return the cache TTL for resolved Google Place IDs."""

    try:
        return int(os.getenv("GOOGLE_PLACES_PLACE_ID_CACHE_TTL", "86400"))
    except ValueError:
        return 86400


def resolve_google_place_id(api_key: str, configured_place_id: Optional[str] = None) -> str:
    """Resolve the Google Place ID either from config or via a text search."""

    if not api_key:
        raise RuntimeError("Google Places API key is not configured.")

    if configured_place_id:
        candidate = configured_place_id.strip()
        if candidate:
            return candidate

    query = (os.getenv("GOOGLE_PLACES_SEARCH_TEXT") or "Jenks Family Medicine, Chattanooga TN").strip()
    if not query:
        raise RuntimeError(
            "Google Places search text is not configured."
        )

    now = time.time()
    cache_ttl = get_place_id_cache_ttl()

    if (
        PLACE_ID_CACHE["value"]
        and PLACE_ID_CACHE.get("query") == query
        and now - PLACE_ID_CACHE["timestamp"] < cache_ttl
    ):
        return PLACE_ID_CACHE["value"]

    endpoint = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
    params = {
        "input": query,
        "inputtype": "textquery",
        "fields": "place_id",
        "key": api_key,
        "language": "en",
    }

    try:
        response = requests.get(endpoint, params=params, timeout=10)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError("Unable to resolve Google Place ID.") from exc

    payload = response.json()
    status = payload.get("status")
    if status != "OK":
        if status == "ZERO_RESULTS":
            raise RuntimeError(
                f"No Google Place ID found for query '{query}'."
            )
        message = payload.get("error_message") or status or "Unknown error"
        raise RuntimeError(f"Google Place ID lookup error: {message}")

    candidates = payload.get("candidates") or []
    if not candidates:
        raise RuntimeError(
            f"No Google Place ID found for query '{query}'."
        )

    place_id = candidates[0].get("place_id")
    if not place_id:
        raise RuntimeError("Google Place ID lookup did not return a place_id.")

    PLACE_ID_CACHE["value"] = place_id
    PLACE_ID_CACHE["timestamp"] = now
    PLACE_ID_CACHE["query"] = query

    return place_id


def fetch_google_reviews(force_refresh: bool = False):
    """Fetch five-star Google reviews for the configured place."""

    api_key = (os.getenv("GOOGLE_PLACES_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError(
            "Google Places API key is not configured. Set GOOGLE_PLACES_API_KEY."
        )

    place_id = resolve_google_place_id(api_key, os.getenv("GOOGLE_PLACES_PLACE_ID"))
    if not place_id:
        raise RuntimeError(
            "Google Place ID could not be determined."
        )

    cache_ttl = get_reviews_cache_ttl()
    now = time.time()

    if not force_refresh and REVIEWS_CACHE["payload"] is not None:
        if (
            REVIEWS_CACHE.get("place_id") == place_id
            and now - REVIEWS_CACHE["timestamp"] < cache_ttl
        ):
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
        "place_id": place_id,
    }

    REVIEWS_CACHE["payload"] = payload_summary
    REVIEWS_CACHE["timestamp"] = now
    REVIEWS_CACHE["place_id"] = place_id

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
            "place_id": payload.get("place_id"),
        }
    )


if __name__ == '__main__':
    # Set debug=False when deploying to production (Heroku sets this via env var)
    # For local testing, you can use debug=True
    app.run(debug=False)  # Set debug=False for Heroku

