"""Scheduling journey tests for the public provider-selection page."""


EHRIN_ATHENA_URL = (
    "https://consumer.scheduling.athena.io/"
    "?locationId=34992-1&practitionerId=34992-4"
)
ANNE_ATHENA_URL = (
    "https://consumer.scheduling.athena.io/"
    "?locationId=34992-1&practitionerId=34992-5"
)


def test_new_patient_renders_provider_choice(client):
    response = client.get("/new-patient")
    assert response.status_code == 200
    assert b"Choose your provider" in response.data


def test_verified_provider_links_use_athena(client):
    body = client.get("/new-patient").data
    assert b"consumer.scheduling.athena.io" in body
    assert b"practitionerId=34992-4" in body
    assert b"practitionerId=34992-5" in body


def test_amanda_is_listed_without_an_invented_online_id(client):
    body = client.get("/new-patient").data
    assert b"Amanda Rice, NP" in body
    assert b"Appointments beginning September 2026" in body
    assert b"practitionerId=amanda" not in body
    assert b'href="tel:4233735457"' in body


def test_primary_schedule_ctas_use_provider_choice_page(client):
    home = client.get("/").data
    assert b'href="/new-patient"' in home


def test_provider_links_open_in_a_new_tab_safely(client):
    body = client.get("/new-patient").data
    assert body.count(b'target="_blank"') >= 3
    assert body.count(b'rel="noopener noreferrer"') >= 3
