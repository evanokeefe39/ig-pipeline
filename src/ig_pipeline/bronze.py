"""Bronze layer — download Apify datasets to immutable local storage.

Download is file-based: a ``.jsonl`` data file plus a ``.jsonl.meta`` sidecar
with run_id, actor, item_count, downloaded_at. No DuckDB writes.
"""

from __future__ import annotations

import json as _json
import logging
import time
from pathlib import Path

from . import db as _db
from .apify import stream_dataset
from .models import BronzeResult


def _bronze_dir() -> Path:
    return _db.BRONZE_DIR

log = logging.getLogger(__name__)


def download_dataset(dataset_id: str, *, token: str, run_id: str = "", actor: str = "") -> BronzeResult:
    """Download dataset to bronze storage. Idempotent — skips if file exists.

    Writes ``{dataset_id}.jsonl`` (NDJSON data) and ``{dataset_id}.jsonl.meta``
    (JSON metadata: run_id, actor, item_count, downloaded_at).
    No DuckDB writes — bronze state is purely file-based.
    """
    dest = _bronze_dir() / f"{dataset_id}.jsonl"
    meta_path = Path(str(dest) + ".meta")

    if dest.exists():
        log.info("Dataset %s already downloaded, skipping", dataset_id)
        try:
            meta = _json.loads(meta_path.read_text())
            item_count = meta.get("item_count", 0)
        except Exception:
            item_count = sum(1 for _ in open(dest, encoding="utf-8") if _.strip())
        return BronzeResult(dataset_id=dataset_id, path=str(dest), item_count=item_count, skipped=True)

    item_count = stream_dataset(dataset_id, dest=dest, token=token)

    meta = {
        "dataset_id": dataset_id,
        "run_id": run_id,
        "actor": actor,
        "item_count": item_count,
        "downloaded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    meta_path.write_text(_json.dumps(meta, indent=2))

    log.info("Downloaded %d items from %s", item_count, dataset_id)
    return BronzeResult(dataset_id=dataset_id, path=str(dest), item_count=item_count)


def ingest_dataset(dataset_id: str, *, token: str, run_id: str = "", actor: str = "") -> BronzeResult:
    """Deprecated alias for download_dataset. Will be removed with Dagster migration."""
    return download_dataset(dataset_id, token=token, run_id=run_id, actor=actor)
