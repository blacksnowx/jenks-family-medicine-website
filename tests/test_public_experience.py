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


def test_public_contact_email_uses_brittany_inbox(client):
    for path in ("/", "/contact", "/welcome/primary-care"):
        body = client.get(path).data.lower()
        assert b"brittany@jenksfamilymedicine.com" in body, f"Brittany's email is missing from {path}"
        assert b"office@jenksfamilymedicine.com" not in body, f"Old office email remains on {path}"


def test_amanda_rice_is_featured_on_key_patient_journeys(client):
    for path in ("/", "/about", "/new-patient"):
        body = client.get(path).data
        assert b"Amanda Rice, NP" in body, f"Amanda is missing from {path}"
        assert b"Accepting new patients" in body, f"Amanda's availability is missing from {path}"
        assert b"September 2026" not in body
        assert b"images/Amanda.png" in body
        assert b"Photo coming soon" not in body


def test_amanda_profile_does_not_invent_a_practitioner_id(client):
    body = client.get("/new-patient").data
    assert b"amanda-rice" in body
    assert b"practitionerId=amanda" not in body


def test_amanda_profile_includes_certification_and_education(client):
    body = client.get("/about").data
    amanda_card = body.split(b'id="amanda-rice"', 1)[1].split(b"</article>", 1)[0]

    assert b"AANP Board Certified" in amanda_card
    assert b"Doctor of Nursing Practice, University of Tennessee at Chattanooga</li>" in amanda_card
    assert b"University of Tennessee at Chattanooga, 2026" not in amanda_card
    assert b"Bachelor of Science in Nursing, University of West Georgia, 2011" in amanda_card
    assert b"Associate of Science in Nursing, Dalton State College, 2010" in amanda_card


def test_ehrin_lists_compensation_and_pension_examiner_last(client):
    body = client.get("/about").data
    ehrin_card = body.split(b'id="ehrin-irvin"', 1)[1].split(b"</article>", 1)[0]
    highlights = ehrin_card.split(b'<ul class="check-list">', 1)[1].split(b"</ul>", 1)[0]

    assert highlights.rstrip().endswith(b"<li>Certified Compensation &amp; Pension Examiner</li>")


def test_ehrin_and_amanda_show_the_accepting_new_patients_badge(client):
    badge = b'<p class="availability-badge">Accepting new patients</p>'

    for path in ("/about", "/new-patient"):
        body = client.get(path).data
        for slug in (b"ehrin-irvin", b"amanda-rice"):
            card = body.split(b'id="' + slug + b'"', 1)[1].split(b"</article>", 1)[0]
            assert badge in card, f"Badge missing from {slug!r} on {path}"
        anne_card = body.split(b'id="anne-jenks"', 1)[1].split(b"</article>", 1)[0]
        assert b"availability-badge" not in anne_card, f"Unexpected badge on Anne's card on {path}"


def test_contact_photo_grid_shows_the_four_clinicians(client):
    body = client.get("/contact").data
    collage = body.split(b'class="team-collage', 1)[1].split(b"</div>", 1)[0]

    assert collage.count(b"<img") == 4
    for portrait in (b"Ehrin.jpg", b"Anne.jpg", b"Heather.jpg", b"Amanda.png"):
        assert portrait in collage, f"{portrait!r} missing from the contact collage"
    assert b"Brittany.jpg" not in body


def test_home_hero_shows_the_office_photo_instead_of_the_team_collage(client):
    body = client.get("/").data
    hero = body.split(b'class="hero-photo-frame"', 1)[1].split(b"</div>", 1)[0]

    assert hero.count(b"<img") == 1
    assert b"images/Office.jpg" in hero
    assert b"team-collage" not in body
    for portrait in (b"Ehrin.jpg", b"Anne.jpg", b"Heather.jpg", b"Brittany.jpg"):
        assert portrait not in body, f"{portrait!r} remains on the home page"
    assert client.get("/static/images/Office.jpg").status_code == 200


def test_supervising_physician_is_a_text_note_beneath_the_support_team(client):
    body = client.get("/about").data
    provider_grid = body.split(b'class="provider-grid"', 1)[1].split(b'id="support-team-title"', 1)[0]

    assert b"Redd" not in provider_grid
    assert provider_grid.count(b'<article class="provider-card') == 4

    # The note follows the support team photos (Tyler and Brittany).
    after_support_photos = body.split(b'id="brittany-griffith"', 1)[1]
    note = after_support_photos.split(b'id="supervising-physician"', 1)[1].split(b"</aside>", 1)[0]
    assert b">Supervising Physician</h3>" in note
    assert (
        b"Dr. David Redd serves as the supervising physician for Jenks Family Medicine's "
        b"nurse practitioners. Patient care at our office is provided by our nurse "
        b"practitioner team. Dr. Redd does not see patients at this location."
    ) in note
    assert b"<img" not in note


def test_supervising_physician_photo_and_bio_are_removed(client):
    body = client.get("/about").data

    for retired in (
        b"Redd.png",
        b'id="david-redd"',
        b"David Redd, MD",
        b"More than 30 years of experience",
        b"Carraway Methodist",
        b"California Polytechnic",
    ):
        assert retired not in body
    assert client.get("/static/images/Redd.png").status_code == 404


def test_fax_number_is_replaced_with_call_for_number(client):
    for path in ("/", "/contact", "/welcome/primary-care"):
        body = client.get(path).data
        assert b"380-7154" not in body, f"Fax number remains on {path}"
        assert b"Call for number" in body, f"Fax placeholder is missing from {path}"


def test_vso_contacts_list_offices_without_personal_names(client):
    body = client.get("/veteran-resources").data

    for name in (b"Chuck", b"Alsobrook", b"George Davis"):
        assert name not in body
    assert b"<h3>Hamilton County VSO</h3>" in body
    assert b"<h3>Tennessee State VSO</h3>" in body
    assert b'href="tel:4232097884"' in body
    assert b'href="tel:6153393796"' in body


def test_maps_point_at_the_clinic_business_listing(client):
    home = client.get("/").data
    contact = client.get("/contact").data

    for body in (home, contact):
        assert b"Jenks%20Family%20Medicine" in body, "Map embed should show the clinic's own pin"
        assert b"query_place_id=ChIJI4puYMVdYIgR9opqcCbon5c" in body
        assert b"maps.google.com/?q=3908" not in body, "Address-only map link remains"
    assert b"location-preview__art" not in home


def test_parking_instructions_no_longer_mention_validation(client):
    contact = client.get("/contact").data

    assert (
        b"Free parking is available during office hours of 8:00 AM - 4:00 PM "
        b"for patients and their families."
    ) in contact
    for retired in (b"QR code", b"validation code", b"license plate"):
        assert retired not in contact
    assert b"Free with validation" not in client.get("/").data


def test_secure_communication_points_to_phone_and_portal(client):
    body = client.get("/contact").data
    section = body.split(b"<h2>Secure communication</h2>", 1)[1].split(b"</div>", 1)[0]

    assert b"contact us via phone using the information provided on this page" in section
    assert b"phone or email" not in section
    assert b"please call our office or use our secure" in section
    assert b"Patient Portal</a>" in section


def test_dot_follow_up_pricing(client):
    body = client.get("/services-insurance").data

    assert b"Certified FMCSA exams ($120*)." in body
    assert b'DOT Follow-Up After "Determination Pending" Status:</strong> ($40*).' in body
    assert b"complete re-exam per FMCSA regulations, which is a $120* fee." in body
    assert b"$35" not in body


def test_betsy_is_removed_from_the_public_team(client):
    body = client.get("/about").data

    assert b'id="betsy"' not in body
    assert b"Betsy.jpg" not in body
    assert b"Betsy, receptionist" not in body
    assert client.get("/static/images/Betsy.jpg").status_code == 404

    for path in ("/", "/contact"):
        assert b"Team.jpg" not in client.get(path).data

    assert client.get("/static/images/Team.jpg").status_code == 404


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
