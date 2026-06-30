"""Tests for DuckDB views: fact_post, profile_stats, topic_stats, edges."""

import json
import pytest

from ig_pipeline.db import get_db


def _seed_silver_with_gold(db, tmp_path, monkeypatch):
    """Helper: insert bronze + silver + gold data for view testing."""
    import time

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

    posts = [
        {
            "id": "111", "shortCode": "a", "url": "https://x.com/1",
            "caption": "comment GUIDE",
            "ownerId": 1, "ownerUsername": "alice",
            "likesCount": 100, "commentsCount": 10,
            "videoPlayCount": 1000, "videoViewCount": 500,
            "timestamp": "2025-06-15T12:00:00.000Z", "hashtags": ["design"],
        },
        {
            "id": "222", "shortCode": "b", "url": "https://x.com/2",
            "caption": "normal post",
            "ownerId": 2, "ownerUsername": "bob",
            "likesCount": 200, "commentsCount": 5,
            "videoPlayCount": 0, "videoViewCount": 0,
            "timestamp": "2025-06-16T12:00:00.000Z", "hashtags": [],
        },
        {
            "id": "333", "shortCode": "c", "url": "https://x.com/3",
            "caption": "another one",
            "ownerId": 1, "ownerUsername": "alice",
            "likesCount": 300, "commentsCount": 15,
            "videoPlayCount": 2000, "videoViewCount": 800,
            "timestamp": "2025-07-01T12:00:00.000Z", "hashtags": ["ui"],
        },
    ]

    dest = dest_dir / "ds1.jsonl"
    with open(dest, "w", encoding="utf-8") as f:
        for p in posts:
            f.write(json.dumps(p) + "\n")

    db.execute(
        "INSERT INTO bronze_ingests (dataset_id, run_id, actor, item_count, file_path) "
        "VALUES ('ds1', 'run-1', 'apify/ig', 3, ?)",
        (str(dest),),
    )
    db.commit()

    from ig_pipeline.silver import deduplicate_all
    deduplicate_all(db=db)

    # Insert gold_analyses for posts 111 and 222 (333 not analysed)
    for pid, adm, domain, topic, ct in [
        ("111", "E5", "ai_tools", "image_gen", "tutorial"),
        ("222", "A1", "frontend", "react", "showcase"),
    ]:
        enriched = {
            "post_id": pid,
            "url": f"https://x.com/{pid}",
            "schema_version": 3,
            "analysed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "analysis": {
                "is_educational": True,
                "is_actionable": pid == "111",
                "admirality": adm,
                "domain": domain,
                "subdomain": "",
                "topic": topic,
                "subtopic": "",
                "content_type": ct,
                "style": "minimal",
                "format": "carousel",
                "educational_json": {"summary": "learns", "workflow": [], "concepts": [], "principles": [], "techniques": []},
                "actionable_json": {
                    "summary": "does",
                    "resources": [
                        {"name": "ToolX", "url": "toolx.com", "type": "tool", "purpose": "stuff"},
                        {"name": "SiteY", "url": "sitey.com", "type": "site", "purpose": "things"},
                    ] if pid == "111" else [],
                    "tools": ["ToolX"] if pid == "111" else [],
                    "guides": [],
                    "downloads": [],
                },
                "transcript": "test transcript",
            },
        }
        (gold_dir / pid).mkdir(parents=True, exist_ok=True)
        (gold_dir / pid / "enriched.json").write_text(
            json.dumps(enriched, ensure_ascii=False, indent=2), encoding="utf-8",
        )
        db.execute(
            "INSERT OR REPLACE INTO gold_analyses "
            "(post_id, schema_version, status, result_json, attempts, analysed_at) "
            "VALUES (?, ?, 'analysed', ?, 1, CURRENT_TIMESTAMP)",
            (pid, 3, json.dumps(enriched)),
        )

    db.commit()


# ── fact_post ────────────────────────────────────────────────────────────────


def test_fact_post_all_rows(tmp_path, monkeypatch):
    """fact_post contains all silver_posts rows (LEFT JOIN on gold)."""
    db = get_db(":memory:")
    _seed_silver_with_gold(db, tmp_path, monkeypatch)

    from ig_pipeline.gold import populate_dim_time, populate_dim_profile, refresh_views
    populate_dim_time(db=db)
    populate_dim_profile(db=db)
    refresh_views(db=db)

    rows = db.execute("SELECT post_id FROM fact_post ORDER BY post_id").fetchall()
    assert [r[0] for r in rows] == ["111", "222", "333"]


def test_fact_post_gold_fields(tmp_path, monkeypatch):
    """Gold fields are populated for analysed posts, NULL for others."""
    db = get_db(":memory:")
    _seed_silver_with_gold(db, tmp_path, monkeypatch)

    from ig_pipeline.gold import populate_dim_time, populate_dim_profile, refresh_views
    populate_dim_time(db=db)
    populate_dim_profile(db=db)
    refresh_views(db=db)

    # Post 111: analysed, E5 admirality
    r = db.execute("SELECT admirality, is_educational, is_actionable, domain, topic FROM fact_post WHERE post_id='111'").fetchone()
    assert r == ("E5", True, True, "ai_tools", "image_gen")

    # Post 222: analysed, A1 admirality
    r = db.execute("SELECT admirality, domain, topic FROM fact_post WHERE post_id='222'").fetchone()
    assert r == ("A1", "frontend", "react")

    # Post 333: not analysed — gold fields NULL
    r = db.execute("SELECT admirality, domain FROM fact_post WHERE post_id='333'").fetchone()
    assert r == (None, None)


def test_fact_post_engagement_ratios(tmp_path, monkeypatch):
    """Derived ratios are computed correctly."""
    db = get_db(":memory:")
    _seed_silver_with_gold(db, tmp_path, monkeypatch)

    from ig_pipeline.gold import populate_dim_time, populate_dim_profile, refresh_views
    populate_dim_time(db=db)
    populate_dim_profile(db=db)
    refresh_views(db=db)

    # Post 111: 1000 plays / 500 views = 2.0
    r = db.execute("SELECT play_view_ratio, comment_like_ratio FROM fact_post WHERE post_id='111'").fetchone()
    assert r[0] == 2.0
    assert r[1] == pytest.approx(0.1)

    # Post 222: 0 plays / 0 views → NULL (CASE WHEN > 0)
    r = db.execute("SELECT play_view_ratio, comment_like_ratio FROM fact_post WHERE post_id='222'").fetchone()
    assert r[0] is None
    assert r[1] == pytest.approx(0.025)


# ── profile_stats ────────────────────────────────────────────────────────────


def test_profile_stats_aggregations(tmp_path, monkeypatch):
    """profile_stats computes per-owner aggregations correctly."""
    db = get_db(":memory:")
    _seed_silver_with_gold(db, tmp_path, monkeypatch)

    from ig_pipeline.gold import populate_dim_time, populate_dim_profile, refresh_views
    populate_dim_time(db=db)
    populate_dim_profile(db=db)
    refresh_views(db=db)

    # Alice: posts 111 + 333 = 2 posts
    r = db.execute(
        "SELECT post_count, avg_likes, bait_post_count FROM profile_stats WHERE owner_id='1'"
    ).fetchone()
    assert r[0] == 2
    assert r[1] == 200.0  # (100+300)/2
    assert r[2] == 1      # post 111 has bait

    # Bob: 1 post
    r = db.execute(
        "SELECT post_count, avg_likes FROM profile_stats WHERE owner_id='2'"
    ).fetchone()
    assert r[0] == 1
    assert r[1] == 200.0


def test_profile_stats_no_gold_needed(tmp_path, monkeypatch):
    """profile_stats works on silver data alone (no gold enrichment required)."""
    db = get_db(":memory:")
    _seed_silver_with_gold(db, tmp_path, monkeypatch)

    from ig_pipeline.gold import populate_dim_time, populate_dim_profile, refresh_views
    populate_dim_time(db=db)
    populate_dim_profile(db=db)
    refresh_views(db=db)

    assert db.execute("SELECT count(*) FROM profile_stats").fetchone()[0] == 2


# ── topic_stats ──────────────────────────────────────────────────────────────


def test_topic_stats_counts(tmp_path, monkeypatch):
    """topic_stats groups by domain/topic from analysed posts."""
    db = get_db(":memory:")
    _seed_silver_with_gold(db, tmp_path, monkeypatch)

    from ig_pipeline.gold import populate_dim_time, populate_dim_profile, refresh_views
    populate_dim_time(db=db)
    populate_dim_profile(db=db)
    refresh_views(db=db)

    rows = db.execute(
        "SELECT domain, topic, post_count, profile_count FROM topic_stats ORDER BY domain"
    ).fetchall()
    assert len(rows) == 2
    assert rows[0] == ("ai_tools", "image_gen", 1, 1)
    assert rows[1] == ("frontend", "react", 1, 1)


# ── edge views ───────────────────────────────────────────────────────────────


def test_profile_topic_edges(tmp_path, monkeypatch):
    """profile_topic_edges maps profiles to topics with engagement."""
    db = get_db(":memory:")
    _seed_silver_with_gold(db, tmp_path, monkeypatch)

    from ig_pipeline.gold import populate_dim_time, populate_dim_profile, refresh_views
    populate_dim_time(db=db)
    populate_dim_profile(db=db)
    refresh_views(db=db)

    rows = db.execute(
        "SELECT owner_username, domain, topic, post_count FROM profile_topic_edges ORDER BY owner_username"
    ).fetchall()
    assert len(rows) == 2
    assert rows[0] == ("alice", "ai_tools", "image_gen", 1)
    assert rows[1] == ("bob", "frontend", "react", 1)


def test_profile_resource_edges(tmp_path, monkeypatch):
    """profile_resource_edges extracts resources from actionable_json."""
    db = get_db(":memory:")
    _seed_silver_with_gold(db, tmp_path, monkeypatch)

    from ig_pipeline.gold import populate_dim_time, populate_dim_profile, refresh_views
    populate_dim_time(db=db)
    populate_dim_profile(db=db)
    refresh_views(db=db)

    rows = db.execute(
        "SELECT owner_username, resource_name, resource_url, resource_type FROM profile_resource_edges ORDER BY resource_name"
    ).fetchall()
    assert len(rows) == 2
    assert rows[0] == ("alice", "SiteY", "sitey.com", "site")
    assert rows[1] == ("alice", "ToolX", "toolx.com", "tool")


def test_profile_resource_edges_empty(tmp_path, monkeypatch):
    """Post 222 has empty resources array → no edges."""
    db = get_db(":memory:")
    _seed_silver_with_gold(db, tmp_path, monkeypatch)

    from ig_pipeline.gold import populate_dim_time, populate_dim_profile, refresh_views
    populate_dim_time(db=db)
    populate_dim_profile(db=db)
    refresh_views(db=db)

    # Bob (post 222) has no resources → should not appear
    rows = db.execute(
        "SELECT owner_username FROM profile_resource_edges WHERE owner_username='bob'"
    ).fetchall()
    assert len(rows) == 0


# ── dimension tables ─────────────────────────────────────────────────────────


def test_dim_time_populated(tmp_path, monkeypatch):
    """dim_time contains dates from silver_posts timestamps."""
    db = get_db(":memory:")
    _seed_silver_with_gold(db, tmp_path, monkeypatch)

    from ig_pipeline.gold import populate_dim_time
    n = populate_dim_time(db=db)
    assert n == 3  # 3 distinct dates: 2025-06-15, 2025-06-16, 2025-07-01

    rows = db.execute("SELECT date, month, quarter, year FROM dim_time ORDER BY date").fetchall()
    assert len(rows) == 3
def test_dim_profile_populated(tmp_path, monkeypatch):
    """dim_profile seeds unique owners from silver_posts."""
    db = get_db(":memory:")
    _seed_silver_with_gold(db, tmp_path, monkeypatch)

    from ig_pipeline.gold import populate_dim_profile
    n = populate_dim_profile(db=db)
    assert n == 2  # alice + bob

    rows = db.execute(
        "SELECT owner_id, owner_username, is_current FROM dim_profile ORDER BY owner_id"
    ).fetchall()
    assert rows[0] == ("1", "alice", True)
    assert rows[1] == ("2", "bob", True)


def test_dim_time_idempotent(tmp_path, monkeypatch):
    """populate_dim_time is idempotent with INSERT OR REPLACE."""
    db = get_db(":memory:")
    _seed_silver_with_gold(db, tmp_path, monkeypatch)

    from ig_pipeline.gold import populate_dim_time
    n1 = populate_dim_time(db=db)
    n2 = populate_dim_time(db=db)
    assert n1 == n2
