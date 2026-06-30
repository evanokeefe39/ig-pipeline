"""Tests for silver.py."""
import json

from ig_pipeline.db import get_db


def test_deduplicate_empty():
    """deduplicate_all returns zero when nothing to silver."""
    db = get_db(":memory:")

    from ig_pipeline.silver import deduplicate_all
    result = deduplicate_all(db=db)
    assert result.datasets_processed == 0
    assert result.posts_silvered == 0


def test_deduplicate_new_dataset(tmp_path, monkeypatch):
    """Posts from a new bronze dataset are upserted into silver."""
    db = get_db(":memory:")

    dest_dir = tmp_path / "bronze" / "datasets"
    dest_dir.mkdir(parents=True)
    silv_dir = tmp_path / "silver" / "posts"
    silv_dir.mkdir(parents=True)

    monkeypatch.setattr("ig_pipeline.db.BRONZE_DIR", dest_dir)
    monkeypatch.setattr("ig_pipeline.db.SILVER_DIR", silv_dir)

    dest = dest_dir / "ds1.jsonl"
    posts = [
        {"id": "111", "shortCode": "sc1", "url": "https://ig.com/1", "caption": "hello"},
        {"id": "222", "shortCode": "sc2", "url": "https://ig.com/2"},
    ]
    with open(dest, "w", encoding="utf-8") as f:
        for p in posts:
            f.write(json.dumps(p) + "\n")

    db.execute(
        "INSERT INTO bronze_ingests (dataset_id, run_id, actor, item_count, file_path) "
        "VALUES ('ds1', 'run-1', 'apify/ig', 2, ?)",
        (str(dest),),
    )
    db.commit()

    from ig_pipeline.silver import deduplicate_all
    result = deduplicate_all(db=db)
    assert result.datasets_processed == 1
    assert result.posts_silvered == 2

    rows = db.execute("SELECT post_id FROM silver_posts").fetchall()
    assert {r[0] for r in rows} == {"111", "222"}

    for pid in ("111", "222"):
        pj = silv_dir / pid / "post.json"
        assert pj.exists()


def test_upsert_replaces_existing(tmp_path, monkeypatch):
    """INSERT OR REPLACE overwrites when post_id already exists."""
    db = get_db(":memory:")

    dest_dir = tmp_path / "bronze" / "datasets"
    dest_dir.mkdir(parents=True)
    silv_dir = tmp_path / "silver" / "posts"
    silv_dir.mkdir(parents=True)

    monkeypatch.setattr("ig_pipeline.db.BRONZE_DIR", dest_dir)
    monkeypatch.setattr("ig_pipeline.db.SILVER_DIR", silv_dir)

    # First dataset
    dest1 = dest_dir / "ds1.jsonl"
    with open(dest1, "w") as f:
        post_data = {"id": "111", "shortCode": "sc1", "url": "https://ig.com/1"}
        post_data["caption"] = "old"
        f.write(json.dumps(post_data) + "\n")

    db.execute(
        "INSERT INTO bronze_ingests (dataset_id, run_id, actor, item_count, file_path) "
        "VALUES ('ds1', 'run-1', 'apify/ig', 1, ?)",
        (str(dest1),),
    )
    db.commit()

    from ig_pipeline.silver import deduplicate_all
    deduplicate_all(db=db)

    # Caption should be "old"
    row = db.execute(
        "SELECT caption FROM silver_posts WHERE post_id='111'"
    ).fetchone()
    assert row[0] == "old"

    # Second dataset — same post_id, different caption
    dest2 = dest_dir / "ds2.jsonl"
    with open(dest2, "w") as f:
        post_data = {"id": "111", "shortCode": "sc1", "url": "https://ig.com/1"}
        post_data["caption"] = "new"
        f.write(json.dumps(post_data) + "\n")

    db.execute(
        "INSERT INTO bronze_ingests (dataset_id, run_id, actor, item_count, file_path) "
        "VALUES ('ds2', 'run-2', 'apify/ig', 1, ?)",
        (str(dest2),),
    )
    db.commit()

    # Re-run — this should upsert (overwrite) the old caption
    result = deduplicate_all(db=db)
    assert result.posts_silvered == 1  # processed 1 post (upsert)

    row = db.execute(
        "SELECT caption, source_dataset FROM silver_posts WHERE post_id='111'"
    ).fetchone()
    assert row[0] == "new"    # caption overwritten
    assert row[1] == "ds2"    # source_dataset updated


def test_new_columns_populated(tmp_path, monkeypatch):
    """Engagement and profile columns are populated from post JSON."""
    db = get_db(":memory:")

    dest_dir = tmp_path / "bronze" / "datasets"
    dest_dir.mkdir(parents=True)
    silv_dir = tmp_path / "silver" / "posts"
    silv_dir.mkdir(parents=True)

    monkeypatch.setattr("ig_pipeline.db.BRONZE_DIR", dest_dir)
    monkeypatch.setattr("ig_pipeline.db.SILVER_DIR", silv_dir)

    dest = dest_dir / "ds1.jsonl"
    post = {
        "id": "111",
        "shortCode": "sc1",
        "url": "https://ig.com/1",
        "caption": "Check this out",
        "ownerId": 12345,
        "ownerUsername": "testuser",
        "likesCount": 500,
        "commentsCount": 25,
        "videoPlayCount": 10000,
        "videoViewCount": 5000,
        "timestamp": "2025-06-15T12:00:00.000Z",
        "hashtags": ["design", "ui"],
    }
    with open(dest, "w", encoding="utf-8") as f:
        f.write(json.dumps(post) + "\n")

    db.execute(
        "INSERT INTO bronze_ingests (dataset_id, run_id, actor, item_count, file_path) "
        "VALUES ('ds1', 'run-1', 'apify/ig', 1, ?)",
        (str(dest),),
    )
    db.commit()

    from ig_pipeline.silver import deduplicate_all
    deduplicate_all(db=db)

    row = db.execute("""
        SELECT owner_id, owner_username, likes_count, comments_count,
               video_play_count, video_view_count, timestamp, hashtags, has_engagement_bait
        FROM silver_posts WHERE post_id = '111'
    """).fetchone()

    assert row[0] == "12345"
    assert row[1] == "testuser"
    assert row[2] == 500
    assert row[3] == 25
    assert row[4] == 10000
    assert row[5] == 5000
    assert row[6] is not None
    assert "design" in row[7]
    assert row[8] == False  # "Check this out" is not bait


def test_engagement_bait_detection():
    """_detect_engagement_bait catches known patterns."""
    from ig_pipeline.silver import _detect_engagement_bait

    assert _detect_engagement_bait("comment GUIDE to get the link")
    assert _detect_engagement_bait("DM me for details")
    assert _detect_engagement_bait("link in bio")
    assert _detect_engagement_bait("drop a comment below")
    assert _detect_engagement_bait("message me for the template")
    assert _detect_engagement_bait("COMMENT STEAL")

    assert not _detect_engagement_bait("Here is a normal caption with useful info")
    assert not _detect_engagement_bait("")


def test_engagement_bait_in_silver(tmp_path, monkeypatch):
    """has_engagement_bait is correctly set during silvering."""
    db = get_db(":memory:")

    dest_dir = tmp_path / "bronze" / "datasets"
    dest_dir.mkdir(parents=True)
    silv_dir = tmp_path / "silver" / "posts"
    silv_dir.mkdir(parents=True)

    monkeypatch.setattr("ig_pipeline.db.BRONZE_DIR", dest_dir)
    monkeypatch.setattr("ig_pipeline.db.SILVER_DIR", silv_dir)

    dest = dest_dir / "ds1.jsonl"
    posts = [
        {"id": "1", "shortCode": "a", "url": "https://x.com/1", "caption": "comment GUIDE for link"},
        {"id": "2", "shortCode": "b", "url": "https://x.com/2", "caption": "normal caption"},
    ]
    with open(dest, "w", encoding="utf-8") as f:
        for p in posts:
            f.write(json.dumps(p) + "\n")

    db.execute(
        "INSERT INTO bronze_ingests (dataset_id, run_id, actor, item_count, file_path) "
        "VALUES ('ds1', 'run-1', 'apify/ig', 2, ?)",
        (str(dest),),
    )
    db.commit()

    from ig_pipeline.silver import deduplicate_all
    deduplicate_all(db=db)

    rows = db.execute(
        "SELECT post_id, has_engagement_bait FROM silver_posts ORDER BY post_id"
    ).fetchall()
    assert rows[0] == ("1", True)
    assert rows[1] == ("2", False)
