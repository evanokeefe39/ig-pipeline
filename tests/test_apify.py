"""Tests for apify.py — uses httpx mocks."""
from ig_pipeline.apify import search_actors


def test_search_actors(respx_mock):
    respx_mock.get(
        "https://api.apify.com/v2/acts?search=instagram&token=abc&limit=5"
    ).respond(
        json={
            "data": [
                {
                    "name": "apify/instagram-scraper",
                    "title": "Instagram Scraper",
                    "description": "Scrapes IG",
                }
            ]
        }
    )
    results = search_actors("instagram", token="abc", limit=5)
    assert len(results) == 1
    assert results[0].name == "apify/instagram-scraper"
