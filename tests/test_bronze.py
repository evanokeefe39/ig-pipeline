"""Tests for bronze.py."""
import json
from pathlib import Path

from ig_pipeline.db import get_db


def test_bronze_skips_already_ingested(tmp_path):
    """ingest_dataset returns skipped=True for a known dataset_id."""
    db = get_db(":memory:")

    db.execute(
        "INSERT INTO bronze_ingests (dataset_id, run_id, actor, item_count, file_path) "
        "VALUES ('test-ds', 'run-1', 'apify/ig', 5, 'fake/path')"
    )
    db.commit()

    from ig_pipeline.bronze import ingest_dataset
    result = ingest_dataset("test-ds", token="fake", db=db)
    assert result.skipped is True
    assert result.dataset_id == "test-ds"


def test_bronze_ingests_new_dataset(tmp_path, monkeypatch):
    """ingest_dataset writes NDJSON and records state for a new dataset."""
    db = get_db(":memory:")

    dest_dir = tmp_path / "bronze" / "datasets"
    dest_dir.mkdir(parents=True)

    def fake_stream(dataset_id, dest, *, token):
        items = [
            {"id": "123", "shortCode": "abc", "url": "https://example.com/1"},
            {"id": "456", "shortCode": "def", "url": "https://example.com/2"},
        ]
        with open(dest, "w", encoding="utf-8") as f:
            for item in items:
                f.write(json.dumps(item) + "\n")
        return 2

    monkeypatch.setattr("ig_pipeline.db.BRONZE_DIR", dest_dir)

    import ig_pipeline.bronze as bronze_mod
    from ig_pipeline.bronze import ingest_dataset
    monkeypatch.setattr(bronze_mod, "stream_dataset", fake_stream)

    result = ingest_dataset("new-ds", token="fake", db=db)
    assert result.skipped is False
    assert result.item_count == 2
    assert Path(result.path).exists()

    row = db.execute(
        "SELECT item_count FROM bronze_ingests WHERE dataset_id='new-ds'"
    ).fetchone()
    assert row is not None
    assert row[0] == 2
