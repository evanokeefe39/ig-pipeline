"""Profile-scraping batch orchestrator.

Backfills dim_profile metadata (follower counts, bio, etc.) by running the
Apify Instagram scraper against profile URLs instead of post URLs.  Profile
URLs trigger the actor to include a ``metaData`` field in each result.

Design
------
The plan tiers profiles by quality/coverage into explicit batches:
  Whales (80 posts), Top (60), Mid (20), Tail (10).
Batches are hand-curated — this module DOES NOT auto-classify.
It accepts explicit profile lists and executes the trigger/poll/ingest cycle.

Usage
-----
    from ig_pipeline.profile_scrape import (
        query_profiles_needing_meta,
        run_batch,
        backfill_after_batches,
    )

    token = os.environ["APIFY_API_TOKEN"]
    profiles = query_profiles_needing_meta()  # discovery only

    # Feed hand-curated batches:
    whales_batch1 = ["starter_story", "sabrina_ramonov", ...]
    run_batch("apify/instagram-scraper", whales_batch1, 80, token)

    backfill_after_batches()  # silver + dim_profile + views
"""

from __future__ import annotations

import logging
import time

import duckdb

from . import db as _db
from .apify import poll_run, trigger_run
from .bronze import ingest_dataset
from .gold import populate_dim_profile, refresh_views

log = logging.getLogger(__name__)

# Default batch size: Apify free tier comfortably handles ~8-9 profiles per
# run.  The plan uses 6-9 for higher-limit tiers and larger batches for
# low-limit tiers.
DEFAULT_BATCH_SIZE = 9
DEFAULT_ACTOR = "shu8hvrXbJbY3Eb9W"  # apify/instagram-scraper (ID form)


# ── Discovery helper ──────────────────────────────────────────────────────

def query_profiles_needing_meta(
    *, db: duckdb.DuckDBPyConnection | None = None
) -> list[dict[str, object]]:
    """Return profiles that have posts but no metaData in dim_profile.

    Read-only helper for planning — does not drive batch construction.
    Result: ``[{"owner_username": str, "post_count": int}, ...]`` sorted
    by post_count descending.
    """
    if db is None:
        db = _db.get_db()

    rows = db.execute("""
        SELECT sp.owner_username, COUNT(*) AS post_count
        FROM silver_posts sp
        LEFT JOIN dim_profile dp
          ON sp.owner_id = dp.owner_id AND dp.is_current
        WHERE sp.owner_username IS NOT NULL
          AND sp.owner_username != ''
          AND dp.follower_count IS NULL
        GROUP BY sp.owner_username
        ORDER BY post_count DESC, sp.owner_username
    """).fetchall()

    return [{"owner_username": r[0], "post_count": r[1]} for r in rows]


# ── Batch execution ───────────────────────────────────────────────────────

def run_batch(
    actor: str,
    usernames: list[str],
    results_limit: int,
    token: str,
    *,
    results_type: str = "posts",
    poll_secs: int = 5,
    timeout: int = 600,
    db: duckdb.DuckDBPyConnection | None = None,
) -> dict[str, object]:
    """Trigger, poll, and ingest one batch of profile URLs.

    Returns a summary dict:
        {"run_id": str, "dataset_id": str, "estimated_cost_usd": float,
         "profiles": int, "results_limit": int, "items_ingested": int,
         "elapsed_secs": float}
    """
    if db is None:
        db = _db.get_db()

    urls = [f"https://www.instagram.com/{u.strip('/').split('/')[-1]}/" for u in usernames]
    log.info("Batch: %d profiles, %d results each", len(urls), results_limit)

    t0 = time.monotonic()

    run = trigger_run(
        actor,
        urls,
        token=token,
        results_limit=results_limit,
        results_type=results_type,
    )

    dataset_id = poll_run(run.run_id, token=token, poll_secs=poll_secs, timeout=timeout)

    result = ingest_dataset(
        dataset_id, token=token, run_id=run.run_id, actor=actor, db=db,
    )

    elapsed = time.monotonic() - t0

    summary: dict[str, object] = {
        "run_id": run.run_id,
        "dataset_id": dataset_id,
        "estimated_cost_usd": run.estimated_cost_usd,
        "profiles": len(urls),
        "results_limit": results_limit,
        "items_ingested": result.item_count,
        "elapsed_secs": round(elapsed, 1),
    }
    log.info(
        "Batch done: %d items in %.1fs (est $%.4f)",
        result.item_count, elapsed, run.estimated_cost_usd,
    )
    return summary


def run_all_batches(
    batches: list[tuple[list[str], int]],
    token: str,
    *,
    actor: str = DEFAULT_ACTOR,
    db: duckdb.DuckDBPyConnection | None = None,
) -> list[dict[str, object]]:
    """Execute multiple batches sequentially.

    ``batches`` is a list of ``(usernames, results_limit)`` tuples — one
    tuple per Apify run.  Returns a summary dict per batch.
    """
    results: list[dict[str, object]] = []
    for i, (usernames, results_limit) in enumerate(batches, start=1):
        log.info("=== Batch %d/%d ===", i, len(batches))
        summary = run_batch(actor, usernames, results_limit, token, db=db)
        results.append(summary)
    return results


# ── Post-backfill steps ───────────────────────────────────────────────────

def backfill_after_batches(
    *, db: duckdb.DuckDBPyConnection | None = None
) -> dict[str, object]:
    """Run silver dedup, populate dim_profile, and refresh views.

    Call once after all profile-scraping batches complete.
    Returns counts for each step.
    """
    if db is None:
        db = _db.get_db()

    sr = deduplicate_all(db=db)
    n_profiles = populate_dim_profile(db=db)
    refresh_views(db=db)

    n_filled = db.execute(
        "SELECT COUNT(*) FROM dim_profile WHERE follower_count IS NOT NULL"
    ).fetchone()[0]

    summary: dict[str, object] = {
        "posts_silvered": sr.posts_silvered,
        "profiles_in_dim": n_profiles,
        "profiles_with_followers": n_filled,
    }
    log.info(
        "Backfill complete: %d posts silvered, %d profiles (%d with followers)",
        sr.posts_silvered, n_profiles, n_filled,
    )
    return summary


def populate_dim_profile_from_details(
    dataset_id: str,
    *,
    db: duckdb.DuckDBPyConnection | None = None,
) -> int:
    """Read a bronze "details" dataset and upsert profile metadata into dim_profile.

    The "details" resultsType returns one record per profile with fields:
    id, username, fullName, biography, followersCount, followsCount,
    postsCount, verified, businessCategoryName, externalUrl, etc.

    Maps directly to dim_profile columns.  Only updates profiles that exist
    in silver_posts (owner_id match) — ignores profiles we have no posts for.

    Returns count of profiles updated.
    """
    import json
    from pathlib import Path

    if db is None:
        db = _db.get_db()

    # Find the bronze file
    row = db.execute(
        "SELECT file_path FROM bronze_ingests WHERE dataset_id = ?",
        (dataset_id,),
    ).fetchone()
    if not row:
        raise ValueError(f"Dataset {dataset_id} not found in bronze_ingests")

    bronze_path = Path(row[0])
    if not bronze_path.exists():
        raise FileNotFoundError(f"Bronze file not found: {bronze_path}")

    # Collect profile metadata
    updates: list[tuple] = []
    skipped = 0
    with open(bronze_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)

            # Skip non-successful profiles
            status = record.get("ig_status") or record.get("status")
            if status and status != "ok":
                skipped += 1
                continue

            owner_id = str(record.get("id", ""))
            username = record.get("username", "")
            if not owner_id or not username:
                skipped += 1
                continue

            updates.append((
                owner_id,
                username,
                record.get("followersCount"),
                record.get("postsCount"),
                record.get("biography"),
                bool(record.get("verified", False)),
                record.get("businessCategoryName"),
                record.get("externalUrl"),
            ))

    log.info(
        "Profile details: %d profiles from bronze, %d skipped",
        len(updates), skipped,
    )

    if not updates:
        return 0

    # Multi-row UPSERT: mark existing rows superseded, insert new
    now = db.execute("SELECT CURRENT_TIMESTAMP").fetchone()[0]

    # Build a temp table for batch update
    db.execute("""
        CREATE OR REPLACE TEMP TABLE _profile_detail_import (
            owner_id VARCHAR PRIMARY KEY,
            owner_username VARCHAR,
            follower_count INTEGER,
            posts_count_ig INTEGER,
            bio VARCHAR,
            is_verified BOOLEAN,
            profile_category VARCHAR,
            external_url VARCHAR
        )
    """)

    for u in updates:
        db.execute(
            "INSERT OR REPLACE INTO _profile_detail_import VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            u,
        )

    # Expire existing rows for profiles we're updating
    db.execute("""
        UPDATE dim_profile
        SET is_current = false, effective_to = ?
        WHERE owner_id IN (SELECT owner_id FROM _profile_detail_import)
          AND is_current
    """, (now,))

    # Insert new rows
    db.execute("""
        INSERT INTO dim_profile (
            profile_key, owner_id, owner_username,
            follower_count, posts_count_ig, bio, is_verified,
            profile_category, external_url,
            is_current, effective_from
        )
        SELECT
            (SELECT COALESCE(MAX(profile_key), 0) FROM dim_profile)
                + ROW_NUMBER() OVER (ORDER BY i.owner_id),
            i.owner_id, i.owner_username,
            i.follower_count, i.posts_count_ig, i.bio, i.is_verified,
            i.profile_category, i.external_url,
            true, ?
        FROM _profile_detail_import i
        WHERE i.owner_id NOT IN (SELECT owner_id FROM dim_profile WHERE is_current)
    """, (now,))

    db.execute("DROP TABLE IF EXISTS _profile_detail_import")
    db.commit()

    count = db.execute(
        "SELECT COUNT(*) FROM dim_profile WHERE follower_count IS NOT NULL"
    ).fetchone()[0]
    log.info("dim_profile updated: %d profiles now have follower counts", count)
    return count
