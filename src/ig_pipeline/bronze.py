"""Bronze layer — ingest Apify datasets to immutable local storage."""

from __future__ import annotations

import hashlib
import logging

import duckdb

from . import db as _db
from .apify import stream_dataset
from .models import BronzeResult


def _bronze_dir():
    return _db.BRONZE_DIR

log = logging.getLogger(__name__)


def ingest_dataset(dataset_id: str, *, token: str, run_id: str = "", actor: str = "",
                   db: duckdb.DuckDBPyConnection | None = None) -> BronzeResult:
    """Stream an Apify dataset to bronze storage. Idempotent — skips if done."""
    if db is None:
        db = _db.get_db()

    # Check if already ingested
    row = db.execute(
        "SELECT dataset_id, file_path FROM bronze_ingests WHERE dataset_id = ?",
        (dataset_id,),
    ).fetchone()
    if row:
        log.info("Dataset %s already ingested, skipping", dataset_id)
        return BronzeResult(
            dataset_id=dataset_id,
            path=row[1],
            item_count=0,
            skipped=True,
        )

    dest = _bronze_dir() / f"{dataset_id}.jsonl"
    item_count = stream_dataset(dataset_id, dest=dest, token=token)

    # Compute checksum
    checksum = hashlib.sha256()
    with open(dest, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            checksum.update(chunk)

    # Record in state
    db.execute(
        "INSERT INTO bronze_ingests "
        "(dataset_id, run_id, actor, item_count, file_path, checksum_sha256) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (dataset_id, run_id, actor, item_count, str(dest), checksum.hexdigest()),
    )
    db.commit()

    return BronzeResult(
        dataset_id=dataset_id, path=str(dest), item_count=item_count, skipped=False
    )
