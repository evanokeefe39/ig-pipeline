"""Tests for gold.py."""
import json

from ig_pipeline.db import get_db


def test_enrich_no_pending():
    """enrich_posts returns zero when nothing needs analysis."""
    db = get_db(":memory:")

    from ig_pipeline.gold import enrich_posts
    result = enrich_posts(db=db)
    assert result.analysed == 0


def test_enrich_with_post_ids(tmp_path, monkeypatch):
    """enrich_posts processes specified post_ids and writes results."""
    db = get_db(":memory:")
    gold_dir = tmp_path / "gold" / "posts"
    gold_dir.mkdir(parents=True)
    monkeypatch.setattr("ig_pipeline.db.GOLD_DIR", gold_dir)
    monkeypatch.setattr("ig_pipeline.db.SILVER_DIR", tmp_path / "silver" / "posts")

    # Seed silver_posts
    db.execute(
        "INSERT INTO silver_posts (post_id, url, source_dataset) "
        "VALUES ('p1', 'https://ig.com/p1', 'ds1')"
    )
    db.commit()

    # Mock _enrich_async to simulate successful analysis
    import ig_pipeline.gold as gold_mod

    async def fake_enrich_async(_client, pending, _db):
        assert len(pending) == 1
        assert pending[0][0] == "p1"
        # Write what the real async pipeline would write
        enriched = {
            "post_id": "p1",
            "url": "https://ig.com/p1",
            "schema_version": 2,
            "analysed_at": "2026-01-01T00:00:00Z",
            "analysis": {"is_educational": True, "value_score": 4, "content_type": "tip"},
        }
        out_dir = gold_mod._gold_dir() / "p1"
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "enriched.json").write_text(
            json.dumps(enriched, ensure_ascii=False, indent=2), encoding="utf-8",
        )
        _db.execute(
            "INSERT OR REPLACE INTO gold_analyses "
            "(post_id, schema_version, status, result_json, attempts, analysed_at) "
            "VALUES ('p1', 2, 'analysed', ?, 1, CURRENT_TIMESTAMP)",
            (json.dumps(enriched),),
        )
        _db.commit()
        from ig_pipeline.models import GoldResult
        r = GoldResult(analysed=1, failed=0, skipped=0, duration_secs=0.0)
        return r

    monkeypatch.setattr(gold_mod, "_enrich_async", fake_enrich_async)
    monkeypatch.setattr(gold_mod, "_silver_dir",
                        lambda: tmp_path / "silver" / "posts")

    from ig_pipeline.gold import enrich_posts
    result = enrich_posts(post_ids=["p1"], db=db)
    assert result.analysed == 1
    assert result.failed == 0

    # Verify gold_analyses row
    row = db.execute(
        "SELECT status, result_json FROM gold_analyses WHERE post_id='p1'"
    ).fetchone()
    assert row is not None
    assert row[0] == "analysed"

    # Verify enriched.json on disk
    assert (gold_dir / "p1" / "enriched.json").exists()


def test_enrich_skips_analysed(tmp_path, monkeypatch):
    """Posts already in gold_analyses with status='analysed' are skipped."""
    db = get_db(":memory:")
    monkeypatch.setattr("ig_pipeline.db.SILVER_DIR", tmp_path / "silver" / "posts")

    db.execute(
        "INSERT INTO silver_posts (post_id, url, source_dataset) "
        "VALUES ('p1', 'https://ig.com/p1', 'ds1')"
    )
    db.execute(
        "INSERT INTO gold_analyses (post_id, schema_version, status, result_json) "
        "VALUES ('p1', 2, 'analysed', '{}')"
    )
    db.commit()

    from ig_pipeline.gold import enrich_posts
    result = enrich_posts(post_ids=["p1"], db=db)
    assert result.analysed == 0  # skipped


def test_enrich_retries_failed(tmp_path, monkeypatch):
    """Posts with status='failed' are eligible for re-analysis."""
    db = get_db(":memory:")
    monkeypatch.setattr("ig_pipeline.db.SILVER_DIR", tmp_path / "silver" / "posts")
    gold_dir = tmp_path / "gold" / "posts"
    gold_dir.mkdir(parents=True)
    monkeypatch.setattr("ig_pipeline.db.GOLD_DIR", gold_dir)

    db.execute(
        "INSERT INTO silver_posts (post_id, url, source_dataset) "
        "VALUES ('p1', 'https://ig.com/p1', 'ds1')"
    )
    db.execute(
        "INSERT INTO gold_analyses (post_id, schema_version, status, error, attempts) "
        "VALUES ('p1', 2, 'failed', 'timeout', 1)"
    )
    db.commit()

    import ig_pipeline.gold as gold_mod

    async def fake_enrich_async(_client, pending, _db):
        enriched = {
            "post_id": "p1", "url": "https://ig.com/p1",
            "schema_version": 2, "analysed_at": "2026-01-01T00:00:00Z",
            "analysis": {"is_educational": False, "value_score": 2, "content_type": "showcase"},
        }
        out_dir = gold_mod._gold_dir() / "p1"
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "enriched.json").write_text(json.dumps(enriched))
        _db.execute(
            "INSERT OR REPLACE INTO gold_analyses "
            "(post_id, schema_version, status, result_json, attempts, analysed_at) "
            "VALUES ('p1', 2, 'analysed', ?, 2, CURRENT_TIMESTAMP)",
            (json.dumps(enriched),),
        )
        _db.commit()
        from ig_pipeline.models import GoldResult
        return GoldResult(analysed=1)

    monkeypatch.setattr(gold_mod, "_enrich_async", fake_enrich_async)

    from ig_pipeline.gold import enrich_posts
    result = enrich_posts(post_ids=["p1"], db=db)
    assert result.analysed == 1

    row = db.execute(
        "SELECT status, attempts FROM gold_analyses WHERE post_id='p1'"
    ).fetchone()
    assert row[0] == "analysed"
    assert row[1] == 2  # attempts incremented


def test_enrich_max_posts(tmp_path, monkeypatch):
    """enrich_posts respects the max_posts limit."""
    db = get_db(":memory:")
    monkeypatch.setattr("ig_pipeline.db.SILVER_DIR", tmp_path / "silver" / "posts")
    gold_dir = tmp_path / "gold" / "posts"
    gold_dir.mkdir(parents=True)
    monkeypatch.setattr("ig_pipeline.db.GOLD_DIR", gold_dir)

    for pid in ("p1", "p2", "p3"):
        db.execute(
            "INSERT INTO silver_posts (post_id, url, source_dataset) "
            "VALUES (?, ?, 'ds1')", (pid, f"https://ig.com/{pid}")
        )
    db.commit()

    import ig_pipeline.gold as gold_mod

    async def fake_enrich_async(_client, pending, _db):
        from ig_pipeline.models import GoldResult
        return GoldResult(analysed=len(pending))

    monkeypatch.setattr(gold_mod, "_enrich_async", fake_enrich_async)

    from ig_pipeline.gold import enrich_posts
    result = enrich_posts(max_posts=2, db=db)
    assert result.analysed == 2


def test_enrich_max_posts_zero_returns_immediately():
    """max_posts=0 returns immediately with no work."""
    db = get_db(":memory:")

    from ig_pipeline.gold import enrich_posts
    result = enrich_posts(max_posts=0, db=db)
    assert result.analysed == 0
    assert result.failed == 0
