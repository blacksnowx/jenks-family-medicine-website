"""Public clinic content shared across the Flask routes and templates.

This module deliberately contains presentation-safe facts only. Scheduling IDs,
clinical analytics aliases, and compensation settings remain in their own
systems so a website copy change cannot alter operational data.
"""

ATHENA_LOCATION_URL = "https://consumer.scheduling.athena.io/?locationId=34992-1"

# The clinic's Google Maps business listing. Map links and embeds point at the
# listing itself (not the bare street address) so visitors see the Jenks Family
# Medicine pin with its hours, reviews, and directions.
GOOGLE_MAPS_PLACE_ID = "ChIJI4puYMVdYIgR9opqcCbon5c"
GOOGLE_MAPS_URL = (
    "https://www.google.com/maps/search/?api=1"
    "&query=Jenks+Family+Medicine+Chattanooga+TN"
    f"&query_place_id={GOOGLE_MAPS_PLACE_ID}"
)
# From Google Maps: listing > Share > Embed a map.
GOOGLE_MAPS_EMBED_URL = (
    "https://www.google.com/maps/embed?pb=!1m18!1m12!1m3!1d3267.849585875899"
    "!2d-85.32772709999999!3d35.0104702!2m3!1f0!2f0!3f0!3m2!1i1024!2i768!4f13.1"
    "!3m3!1m2!1s0x88605dc5606e8a23%3A0x979fe826706a8af6!2sJenks%20Family%20Medicine"
    "!5e0!3m2!1sen!2sus!4v1794400044885!5m2!1sen!2sus"
)


CLINICAL_TEAM = (
    {
        "slug": "ehrin-irvin",
        "name": "Ehrin Irvin, NP",
        "short_name": "Ehrin",
        "role": "Family Nurse Practitioner",
        "credential": "AANP Board Certified",
        "image": "images/Ehrin.jpg",
        "image_alt": "Ehrin Irvin, nurse practitioner at Jenks Family Medicine",
        "initials": "EI",
        "highlights": (
            "Certified FMCSA DOT Examiner",
            "MSN, University of Tennessee at Chattanooga, 2018",
            "BSN, University of North Carolina at Chapel Hill, 2007",
            "Certified Compensation & Pension Examiner",
        ),
        "booking_url": f"{ATHENA_LOCATION_URL}&practitionerId=34992-4",
        "booking_label": "Schedule with Ehrin",
        "availability": "Accepting new patients",
        "is_new": False,
    },
    {
        "slug": "anne-jenks",
        "name": "Anne Jenks, NP",
        "short_name": "Anne",
        "role": "Family Nurse Practitioner",
        "credential": "ANCC Board Certified",
        "image": "images/Anne.jpg",
        "image_alt": "Anne Jenks, nurse practitioner at Jenks Family Medicine",
        "initials": "AJ",
        "highlights": (
            "Certified Compensation & Pension Examiner",
            "Certified FMCSA DOT Examiner",
            "MSN, University of Maryland, Baltimore, 2014",
            "BSN, University of Tennessee at Chattanooga, 2011",
        ),
        "booking_url": f"{ATHENA_LOCATION_URL}&practitionerId=34992-5",
        "booking_label": "Schedule with Anne",
        "availability": None,
        "is_new": False,
    },
    {
        "slug": "heather-mayo",
        "name": "Heather Mayo, NP",
        "short_name": "Heather",
        "role": "Family Nurse Practitioner",
        "credential": "AANP Board Certified",
        "image": "images/Heather.jpg",
        "image_alt": "Heather Mayo, nurse practitioner at Jenks Family Medicine",
        "initials": "HM",
        "highlights": (
            "Certified Compensation & Pension Examiner",
            "MSN, University of Cincinnati, 2015",
            "BSN, University of Arkansas for Medical Sciences, 2008",
        ),
        "booking_url": None,
        "booking_label": None,
        "availability": None,
        "is_new": False,
    },
    {
        "slug": "amanda-rice",
        "name": "Amanda Rice, NP",
        "short_name": "Amanda",
        "role": "Nurse Practitioner",
        "credential": "AANP Board Certified",
        "image": "images/Amanda.png",
        "image_alt": "Amanda Rice, nurse practitioner at Jenks Family Medicine",
        "initials": "AR",
        "highlights": (
            "Doctor of Nursing Practice, University of Tennessee at Chattanooga",
            "Bachelor of Science in Nursing, University of West Georgia, 2011",
            "Associate of Science in Nursing, Dalton State College, 2010",
        ),
        "booking_url": None,
        "booking_label": "Schedule with Amanda",
        "availability": "Accepting new patients",
        "is_new": True,
    },
)


# The supervising physician is not listed here or in CLINICAL_TEAM on purpose:
# he does not see patients at this location, so the Our Team page carries a
# short text note about him (templates/about.html) instead of a profile.
SUPPORT_TEAM = (
    {
        "slug": "tyler-jenks",
        "name": "Tyler Jenks",
        "role": "Business Manager",
        "image": "images/Tyler.jpg",
        "image_alt": "Tyler Jenks, business manager",
        "initials": "TJ",
    },
    {
        "slug": "brittany-griffith",
        "name": "Brittany Griffith",
        "role": "Clinic Manager",
        "image": "images/Brittany.jpg",
        "image_alt": "Brittany Griffith, clinic manager",
        "initials": "BG",
    },
)


# Photo collage on the contact page: the four clinicians. (The home page hero
# shows the office photo, static/images/Office.jpg, instead.)
TEAM_PORTRAITS = tuple(
    member
    for member in CLINICAL_TEAM
    if member["slug"] in {"ehrin-irvin", "anne-jenks", "heather-mayo", "amanda-rice"}
)


BOOKING_PROVIDERS = tuple(
    member for member in CLINICAL_TEAM if member["slug"] in {"ehrin-irvin", "anne-jenks", "amanda-rice"}
)

AMANDA_RICE = next(member for member in CLINICAL_TEAM if member["slug"] == "amanda-rice")


PAGE_META = {
    "index": {
        "title": "Primary Care in Chattanooga",
        "description": (
            "Personal primary care, sick visits, physicals, veteran services, and DOT exams "
            "from Jenks Family Medicine in Chattanooga, Tennessee."
        ),
    },
    "about": {
        "title": "Meet Our Care Team",
        "description": "Meet the nurse practitioners and staff caring for patients at Jenks Family Medicine.",
    },
    "services": {
        "title": "Services & Insurance",
        "description": "Explore primary care, veteran exams, specialty physicals, telehealth, and insurance options.",
    },
    "veteran_resources": {
        "title": "Veteran Care & Resources",
        "description": "Veteran exams, Chattanooga-area VA contacts, VSO information, and trusted online resources.",
    },
    "contact": {
        "title": "Contact & Location",
        "description": "Call, visit, or get directions to Jenks Family Medicine in Chattanooga, Tennessee.",
    },
    "patient_portal": {
        "title": "Patient Portal",
        "description": "Securely view records, message the clinic, request refills, and manage your care.",
    },
    "privacy_policy": {
        "title": "Notice of Privacy Practices",
        "description": "Read the Jenks Family Medicine Notice of Privacy Practices.",
    },
    "new_patient": {
        "title": "Schedule an Appointment",
        "description": "Choose a Jenks Family Medicine provider and schedule your visit online or by phone.",
    },
}
