"""Tests for profile_scrape.py — batch profile backfill orchestrator."""
import json

import httpx

from ig_pipeline.db import get_db


# ── Mock helpers ──────────────────────────────────────────────────────────

_MOCK_TOKEN = "test-token"
_ACTOR = "apify/instagram-scraper"
_API = "https://api.apify.com/v2"


def _mock_apify_trigger(respx_mock, run_id="test-run", dataset_id="test-ds"):
    """Set up respx mock for trigger_run POST."""
    respx_mock.post(
        f"{_API}/acts/{_ACTOR}/runs?token={_MOCK_TOKEN}"
    ).respond(json={
        "data": {
            "id": run_id,
            "defaultDatasetId": dataset_id,
            "stats": {"estimatedTotalPriceUsd": 1.23},
        }
    })


def _mock_apify_poll(respx_mock, run_id="test-run", dataset_id="test-ds"):
    """Set up respx mock for poll_run GET.  poll_run uses GET /v2/actor-runs/{id}."""
    respx_mock.get(
        f"{_API}/actor-runs/{run_id}?token={_MOCK_TOKEN}"
    ).respond(json={
        "data": {
            "id": run_id,
            "status": "SUCCEEDED",
            "defaultDatasetId": dataset_id,
        }
    })


def _mock_apify_stream(respx_mock, dataset_id="test-ds", items=None):
    """Set up respx mock for stream_dataset GET (first page, no pagination cursor)."""
    if items is None:
        items = [
            {"id": "p1", "shortCode": "sc1", "url": "https://ig.com/1",
             "ownerUsername": "alice", "ownerId": 1,
             "metaData": {"followersCount": 5000, "biography": "Designer"}},
            {"id": "p2", "shortCode": "sc2", "url": "https://ig.com/2",
             "ownerUsername": "bob", "ownerId": 2},
        ]
    respx_mock.get(
        f"{_API}/datasets/{dataset_id}/items?format=jsonl&token={_MOCK_TOKEN}&limit=500"
    ).respond(
        content="\n".join(json.dumps(it) for it in items) + "\n",
        headers={},  # no pagination cursor → stream stops after one page
    )


# ── Discovery tests ───────────────────────────────────────────────────────

def test_query_profiles_empty_db():
    """Empty DB returns empty list."""
    db = get_db(":memory:")
    from ig_pipeline.profile_scrape import query_profiles_needing_meta
    result = query_profiles_needing_meta(db=db)
    assert result == []


def test_query_profiles_all_null():
    """Profiles with follower_count IS NULL are returned, sorted by post_count desc."""
    db = get_db(":memory:")

    # Seed silver_posts: alice (3 posts, no metaData), bob (1 post, no metaData)
    for i in range(3):
        db.execute("""
            INSERT OR REPLACE INTO silver_posts
                (post_id, owner_id, owner_username, media_files, source_dataset)
            VALUES (?, ?, ?, '[]', 'ds1')
        """, (f"a{i}", "1", "alice"))
    db.execute("""
        INSERT OR REPLACE INTO silver_posts
            (post_id, owner_id, owner_username, media_files, source_dataset)
        VALUES ('b0', '2', 'bob', '[]', 'ds1')
    """)

    # Seed dim_profile with NULL follower_count
    db.execute("""
        INSERT INTO dim_profile (profile_key, owner_id, owner_username, is_current)
        VALUES (1, '1', 'alice', true), (2, '2', 'bob', true)
    """)
    db.commit()

    from ig_pipeline.profile_scrape import query_profiles_needing_meta
    result = query_profiles_needing_meta(db=db)

    assert len(result) == 2
    assert result[0]["owner_username"] == "alice"
    assert result[0]["post_count"] == 3
    assert result[1]["owner_username"] == "bob"
    assert result[1]["post_count"] == 1


def test_query_profiles_some_filled():
    """Profiles with follower_count NOT NULL are excluded from results."""
    db = get_db(":memory:")

    db.execute("""
        INSERT OR REPLACE INTO silver_posts
            (post_id, owner_id, owner_username, media_files, source_dataset)
        VALUES ('a0', '1', 'alice', '[]', 'ds1'),
               ('b0', '2', 'bob', '[]', 'ds1')
    """)
    db.execute("""
        INSERT INTO dim_profile (profile_key, owner_id, owner_username, follower_count, is_current)
        VALUES (1, '1', 'alice', NULL, true),
               (2, '2', 'bob', 1000, true)
    """)
    db.commit()

    from ig_pipeline.profile_scrape import query_profiles_needing_meta
    result = query_profiles_needing_meta(db=db)

    assert len(result) == 1
    assert result[0]["owner_username"] == "alice"


# ── Batch execution tests ─────────────────────────────────────────────────

def test_run_batch_happy_path(tmp_path, monkeypatch, respx_mock):
    """run_batch triggers, polls, ingests and returns a summary dict."""
    db = get_db(":memory:")
    dest_dir = tmp_path / "bronze" / "datasets"
    dest_dir.mkdir(parents=True)
    monkeypatch.setattr("ig_pipeline.db.BRONZE_DIR", dest_dir)

    _mock_apify_trigger(respx_mock, run_id="r1", dataset_id="ds1")
    _mock_apify_poll(respx_mock, run_id="r1", dataset_id="ds1")
    _mock_apify_stream(respx_mock, dataset_id="ds1")

    from ig_pipeline.profile_scrape import run_batch
    result = run_batch(
        _ACTOR, ["alice", "bob"], results_limit=80,
        token=_MOCK_TOKEN, db=db, poll_secs=0,
    )

    assert result["run_id"] == "r1"
    assert result["dataset_id"] == "ds1"
    assert result["profiles"] == 2
    assert result["results_limit"] == 80
    assert result["items_ingested"] == 2
    assert result["estimated_cost_usd"] == 1.23
    assert result["elapsed_secs"] >= 0


def test_run_batch_cleans_usernames(tmp_path, monkeypatch, respx_mock):
    """URLs and bare usernames both produce valid Instagram profile URLs."""
    db = get_db(":memory:")
    dest_dir = tmp_path / "bronze" / "datasets"
    dest_dir.mkdir(parents=True)
    monkeypatch.setattr("ig_pipeline.db.BRONZE_DIR", dest_dir)

    # Capture the URLs sent to Apify.  respx passes httpx.Request as first arg.
    sent_urls = []

    def capture_trigger(request):
        body = json.loads(request.content)
        sent_urls.extend(body.get("directUrls", []))
        return httpx.Response(200, json={
            "data": {
                "id": "r2",
                "defaultDatasetId": "ds2",
                "stats": {"estimatedTotalPriceUsd": 0.5},
            }
        })

    respx_mock.post(
        f"{_API}/acts/{_ACTOR}/runs?token={_MOCK_TOKEN}"
    ).mock(side_effect=capture_trigger)
    _mock_apify_poll(respx_mock, run_id="r2", dataset_id="ds2")
    _mock_apify_stream(respx_mock, dataset_id="ds2")

    from ig_pipeline.profile_scrape import run_batch
    run_batch(
        _ACTOR,
        ["https://www.instagram.com/alice/", "bob"],
        results_limit=60,
        token=_MOCK_TOKEN, db=db, poll_secs=0,
    )

    assert len(sent_urls) == 2
    assert sent_urls[0] == "https://www.instagram.com/alice/"
    assert sent_urls[1] == "https://www.instagram.com/bob/"


# ── run_all_batches tests ─────────────────────────────────────────────────

def test_run_all_batches_calls_each_batch(tmp_path, monkeypatch, respx_mock):
    """run_all_batches executes multiple batches and returns per-batch summaries."""
    db = get_db(":memory:")
    dest_dir = tmp_path / "bronze" / "datasets"
    dest_dir.mkdir(parents=True)
    monkeypatch.setattr("ig_pipeline.db.BRONZE_DIR", dest_dir)

    _mock_apify_trigger(respx_mock, run_id="rA", dataset_id="dsA")
    _mock_apify_poll(respx_mock, run_id="rA", dataset_id="dsA")
    _mock_apify_stream(respx_mock, dataset_id="dsA", items=[
        {"id": "p1", "url": "https://ig.com/1", "ownerUsername": "alice", "ownerId": 1},
    ])

    from ig_pipeline.profile_scrape import run_all_batches

    batches = [
        (["alice", "bob"], 80),
    ]

    result = run_all_batches(
        batches, _MOCK_TOKEN, actor=_ACTOR, db=db,
    )

    assert len(result) == 1
    assert result[0]["profiles"] == 2
    assert result[0]["results_limit"] == 80


# ── backfill_after_batches tests ──────────────────────────────────────────

def test_backfill_after_batches(tmp_path, monkeypatch):
    """backfill_after_batches runs silver, dim_profile, and refresh_views."""
    db = get_db(":memory:")
    dest_dir = tmp_path / "bronze" / "datasets"
    dest_dir.mkdir(parents=True)
    silv_dir = tmp_path / "silver" / "posts"
    silv_dir.mkdir(parents=True)
    gold_dir = tmp_path / "gold" / "posts"
    gold_dir.mkdir(parents=True)

    monkeypatch.setattr("ig_pipeline.db.BRONZE_DIR", dest_dir)
    monkeypatch.setattr("ig_pipeline.db.SILVER_DIR", silv_dir)
    monkeypatch.setattr("ig_pipeline.gold._gold_dir", lambda: gold_dir)
    monkeypatch.setattr("ig_pipeline.gold._silver_dir", lambda: silv_dir)

    # Seed a bronze dataset with profile metadata.
    # metaData is a *dict* in raw Apify output — silver will json.dumps it into meta_data.
    dest = dest_dir / "ds1.jsonl"
    posts = [
        {
            "id": "p1", "shortCode": "sc1", "url": "https://ig.com/1",
            "ownerId": 1, "ownerUsername": "alice",
            "likesCount": 100, "commentsCount": 10,
            "timestamp": "2025-06-15T12:00:00.000Z",
            "metaData": {"followersCount": 5000, "biography": "Designer"},
        },
        {
            "id": "p2", "shortCode": "sc2", "url": "https://ig.com/2",
            "ownerId": 2, "ownerUsername": "bob",
            "likesCount": 50, "commentsCount": 2,
            "timestamp": "2025-06-16T12:00:00.000Z",
            "metaData": {"followersCount": 200, "biography": "Dev"},
        },
    ]
    with open(dest, "w", encoding="utf-8") as f:
        for p in posts:
            f.write(json.dumps(p) + "\n")

    # Record bronze ingest
    db.execute("""
        INSERT INTO bronze_ingests (dataset_id, run_id, actor, item_count, file_path)
        VALUES ('ds1', 'run-1', 'apify/instagram-scraper', 2, ?)
    """, (str(dest),))
    db.commit()

    from ig_pipeline.profile_scrape import backfill_after_batches
    result = backfill_after_batches(db=db)

    assert result["posts_silvered"] == 2
    assert result["profiles_in_dim"] == 2
    assert result["profiles_with_followers"] == 2

    # Verify dim_profile has follower counts
    rows = db.execute(
        "SELECT owner_username, follower_count, bio FROM dim_profile ORDER BY owner_username"
    ).fetchall()
    assert rows[0] == ("alice", 5000, "Designer")
    assert rows[1] == ("bob", 200, "Dev")

    # Verify views exist
    views = db.execute(
        "SELECT view_name FROM duckdb_views() WHERE view_name IN "
        "('fact_post', 'profile_stats', 'topic_stats', "
        "'profile_topic_edges', 'profile_resource_edges')"
    ).fetchall()
    assert len(views) == 5


def test_backfill_after_batches_follower_count_progress(tmp_path, monkeypatch):
    """follower_count NOT NULL count increases after backfill with metaData."""
    db = get_db(":memory:")
    dest_dir = tmp_path / "bronze" / "datasets"
    dest_dir.mkdir(parents=True)
    silv_dir = tmp_path / "silver" / "posts"
    silv_dir.mkdir(parents=True)

    monkeypatch.setattr("ig_pipeline.db.BRONZE_DIR", dest_dir)
    monkeypatch.setattr("ig_pipeline.db.SILVER_DIR", silv_dir)
    monkeypatch.setattr("ig_pipeline.gold._gold_dir", lambda: tmp_path / "gold" / "posts")
    monkeypatch.setattr("ig_pipeline.gold._silver_dir", lambda: silv_dir)

    # Run 1: posts without metaData → NULL follower_count
    dest1 = dest_dir / "ds1.jsonl"
    posts_no_meta = [
        {"id": "p1", "shortCode": "sc1", "url": "https://ig.com/1",
         "ownerId": 1, "ownerUsername": "alice",
         "likesCount": 100, "commentsCount": 10,
         "timestamp": "2025-06-15T12:00:00.000Z"},
    ]
    with open(dest1, "w", encoding="utf-8") as f:
        for p in posts_no_meta:
            f.write(json.dumps(p) + "\n")

    db.execute("""
        INSERT INTO bronze_ingests (dataset_id, run_id, actor, item_count, file_path)
        VALUES ('ds1', 'run-1', 'apify/instagram-scraper', 1, ?)
    """, (str(dest1),))
    db.commit()

    from ig_pipeline.profile_scrape import backfill_after_batches
    result1 = backfill_after_batches(db=db)
    assert result1["profiles_with_followers"] == 0

    # Run 2: same profile, now WITH metaData (dict, not JSON-encoded string)
    dest2 = dest_dir / "ds2.jsonl"
    posts_with_meta = [
        {"id": "p2", "shortCode": "sc2", "url": "https://ig.com/2",
         "ownerId": 1, "ownerUsername": "alice",
         "likesCount": 200, "commentsCount": 5,
         "timestamp": "2025-06-16T12:00:00.000Z",
         "metaData": {"followersCount": 5000, "biography": "Designer"}},
    ]
    with open(dest2, "w", encoding="utf-8") as f:
        for p in posts_with_meta:
            f.write(json.dumps(p) + "\n")

    db.execute("""
        INSERT INTO bronze_ingests (dataset_id, run_id, actor, item_count, file_path)
        VALUES ('ds2', 'run-2', 'apify/instagram-scraper', 1, ?)
    """, (str(dest2),))
    db.commit()

    result2 = backfill_after_batches(db=db)
    assert result2["profiles_with_followers"] == 1

    row = db.execute(
        "SELECT follower_count, bio FROM dim_profile WHERE owner_username = 'alice'"
    ).fetchone()
    assert row == (5000, "Designer")
