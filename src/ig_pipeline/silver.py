"""Silver layer — deduplicate bronze datasets into canonical post directories."""

from __future__ import annotations

import json
import logging
import os
import re
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

    # Discover bronze datasets by scanning the filesystem — no bronze_ingests table needed.
    # Filter out datasets already recorded in silver_progress.
    from . import db as _db
    bronze_dir = _db.BRONZE_DIR
    processed = {
        r[0] for r in db.execute(
            "SELECT source_dataset FROM silver_progress"
        ).fetchall()
    }
    bronze_files = sorted(bronze_dir.glob("*.jsonl"),
                          key=lambda p: p.stat().st_mtime)
    rows = []
    for f in bronze_files:
        ds_id = f.stem
        if ds_id not in processed:
            rows.append((ds_id, str(f)))
    log.info("Found %d un-silvered bronze datasets", len(rows))
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
            hashtags_json = json.dumps(post.get("hashtags") or [])
            has_bait = _detect_engagement_bait(post.get("caption") or "")
            meta_json = json.dumps(post.get("metaData") or {}) if post.get("metaData") else None
            db.execute(
                """INSERT OR REPLACE INTO silver_posts
                   (post_id, shortcode, url, caption, owner_id, owner_username,
                    likes_count, comments_count, video_play_count, video_view_count,
                    timestamp, hashtags, meta_data, has_engagement_bait,
                    media_files, media_count, source_dataset)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    post_id,
                    post.get("shortCode") or "",
                    (post.get("url") or "").strip(),
                    (post.get("caption") or "")[:5000],
                    str(post.get("ownerId") or ""),
                    post.get("ownerUsername") or "",
                    post.get("likesCount") or 0,
                    post.get("commentsCount") or 0,
                    post.get("videoPlayCount") or 0,
                    post.get("videoViewCount") or 0,
                    post.get("timestamp") or None,
                    hashtags_json,
                    meta_json,
                    has_bait,
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


_ENGAGEMENT_BAIT_PATTERNS = [
    r"\bcomment\s+\w+",
    r"\bDM\s+(me\s+)?\w+",
    r"\bDM\b",
    r"\blink\s+in\s+bio\b",
    r"\bcheck\s+(my|the)\s+link\b",
    r"\bdrop\s+a\s+\w+\s+below\b",
    r"\bmessage\s+me\b",
]

_BAIT_RE = re.compile("|".join(_ENGAGEMENT_BAIT_PATTERNS), re.IGNORECASE)


def _detect_engagement_bait(caption: str) -> bool:
    """Return True if the caption contains known engagement-bait patterns.

    Detects phrases like "comment GUIDE", "DM me for link", "link in bio",
    "drop a comment below" — common funnel/gate tactics.
    Runs on every silver post at zero LLM cost.
    """
    return bool(_BAIT_RE.search(caption))
