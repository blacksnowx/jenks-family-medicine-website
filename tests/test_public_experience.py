"""Public-site contract tests for the refreshed clinic experience."""

from models import BannerSettings


PUBLIC_ROUTES = (
    "/",
    "/about",
    "/services-insurance",
    "/veteran-resources",
    "/contact",
    "/patient-portal",
    "/privacy-policy",
    "/new-patient",
)


def test_core_public_routes_render(client):
    for path in PUBLIC_ROUTES:
        response = client.get(path)
        assert response.status_code == 200, f"{path} returned {response.status_code}"


def test_amanda_rice_is_featured_on_key_patient_journeys(client):
    for path in ("/", "/about", "/new-patient"):
        body = client.get(path).data
        assert b"Amanda Rice, NP" in body, f"Amanda is missing from {path}"
        assert b"September 2026" in body, f"Amanda's start timing is missing from {path}"


def test_amanda_profile_does_not_invent_a_practitioner_id(client):
    body = client.get("/new-patient").data
    assert b"amanda-rice" in body
    assert b"practitionerId=amanda" not in body


def test_public_layout_has_accessible_navigation_landmarks(client):
    body = client.get("/").data
    assert b'href="#main-content"' in body
    assert b'id="main-content"' in body
    assert b'aria-label="Primary navigation"' in body
    assert b'aria-current="page"' in body


def test_homepage_metadata_matches_published_hours(client):
    body = client.get("/").data
    assert b'"dayOfWeek": ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]' in body
    assert b'"closes": "16:00"' in body
    assert b'"closes": "23:00"' not in body


def test_expired_scheduling_banner_is_removed(client):
    body = client.get("/").data
    assert b"Online scheduling is coming soon" not in body
    assert b"site-banner-temp" not in body


def test_active_message_banner_renders_on_homepage(client, db):
    banner = BannerSettings.query.first()
    banner.is_active = True
    banner.message = "Clinic message banner test"
    db.session.commit()

    home = client.get("/").data
    contact = client.get("/contact").data

    assert b'id="site-banner"' in home
    assert b"Clinic message banner test" in home
    assert b"Clinic message banner test" not in contact
