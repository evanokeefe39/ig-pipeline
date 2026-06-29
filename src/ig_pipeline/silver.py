"""Silver layer — deduplicate bronze datasets into canonical post directories."""

from __future__ import annotations

import json
import logging
import os
import shutil
from pathlib import Path

import duckdb

from . import db as _db
from .models import SilverResult


def _silver_dir():
    return _db.SILVER_DIR

log = logging.getLogger(__name__)


def deduplicate_all(*, db: duckdb.DuckDBPyConnection | None = None) -> SilverResult:
    """Find un-silvered bronze datasets and deduplicate into silver.

    Idempotent — uses silver_progress to track which datasets are fully processed.
    On re-run after a crash, incomplete datasets are reprocessed entirely (INSERT OR
    REPLACE handles already-written posts idempotently).
    Datasets are processed in ingested_at order so the latest dataset's writes win.
    Media files are hardlinked (zero-copy on same filesystem).
    """
    if db is None:
        db = _db.get_db()

    # Find un-silvered bronze datasets, ordered oldest-first so the latest
    # dataset processes last and its INSERT OR REPLACE wins for shared post_ids.
    rows = db.execute("""
        SELECT b.dataset_id, b.file_path
        FROM bronze_ingests b
        WHERE b.dataset_id NOT IN (
            SELECT source_dataset FROM silver_progress
        )
        ORDER BY b.ingested_at ASC
    """).fetchall()

    if not rows:
        log.info("Nothing to silver")
        return SilverResult()

    posts_silvered = 0

    for dataset_id, file_path in rows:
        log.info("Silvering dataset %s from %s", dataset_id, file_path)
        src = Path(file_path)
        if not src.exists():
            log.warning("Bronze file missing: %s", src)
            continue

        # Read posts from NDJSON
        posts = []
        with open(src, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    posts.append(json.loads(line))
                except json.JSONDecodeError:
                    continue

        dataset_post_count = 0
        for post in posts:
            post_id = str(post.get("id") or post.get("shortCode") or "")
            if not post_id:
                continue

            # Write canonical post.json (overwrite if exists — latest wins)
            post_dir = _silver_dir() / post_id
            post_dir.mkdir(parents=True, exist_ok=True)
            (post_dir / "post.json").write_text(
                json.dumps(post, ensure_ascii=False, indent=2), encoding="utf-8"
            )

            # Hardlink media if available
            media_dir = post_dir / "media"
            media_dir.mkdir(exist_ok=True)
            posted_media = _link_media(post_id, str(dataset_id), media_dir)

            # DuckDB upsert — INSERT OR REPLACE by primary key (post_id).
            files_json = json.dumps(
                sorted(f.name for f in media_dir.iterdir()) if posted_media else []
            )
            db.execute(
                """INSERT OR REPLACE INTO silver_posts
                   (post_id, shortcode, url, caption, media_files, media_count, source_dataset)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    post_id,
                    post.get("shortCode") or "",
                    (post.get("url") or "").strip(),
                    (post.get("caption") or "")[:5000],
                    files_json,
                    posted_media,
                    dataset_id,
                ),
            )
            dataset_post_count += 1

        db.commit()
        # Record dataset as fully processed — only after all posts committed.
        db.execute(
            "INSERT OR REPLACE INTO silver_progress (source_dataset, post_count) VALUES (?, ?)",
            (dataset_id, dataset_post_count),
        )
        db.commit()
        posts_silvered += dataset_post_count
        log.info("  %d posts silvered from %s", dataset_post_count, dataset_id)

    return SilverResult(
        datasets_processed=len(rows),
        posts_silvered=posts_silvered,
    )




def _link_media(post_id: str, source_dataset: str, dest_dir: Path) -> int:
    """Hardlink media files from the source data directory into silver."""
    src_dir = _db.DATA_DIR / source_dataset / post_id
    if not src_dir.is_dir():
        return 0

    exts = {".mp4", ".mov", ".mpeg", ".webm", ".jpg", ".jpeg", ".png", ".gif", ".webp"}
    count = 0
    for src in sorted(p for p in src_dir.iterdir() if p.is_file() and p.suffix.lower() in exts):
        dest = dest_dir / src.name
        if not dest.exists():
            try:
                os.link(str(src), str(dest))
            except OSError:
                shutil.copy2(str(src), str(dest))
        count += 1
    return count
