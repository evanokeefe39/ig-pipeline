#!/usr/bin/env python3
"""Profile scraping CLI — triggers Apify batches to backfill dim_profile metadata.

Usage:
  uv run python scripts/profile_scrape.py whales    # Phase 1 (80 posts each)
  uv run python scripts/profile_scrape.py top        # Phase 2a (60 posts each)
  uv run python scripts/profile_scrape.py mid        # Phase 2b (20 posts each)
  uv run python scripts/profile_scrape.py tail       # Phase 3 (10 posts each)
  uv run python scripts/profile_scrape.py all        # All phases in order
  uv run python scripts/profile_scrape.py check      # Show backfill progress

Each phase reads profiles needing metaData from the DB, chunks them into
batches sized for Apify's free tier, and runs trigger → poll → ingest.
After all batches in a phase complete, silver dedup + dim_profile + views
are refreshed automatically.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

sys.path.insert(0, "src")

from dotenv import load_dotenv

from ig_pipeline.db import get_db
from ig_pipeline.profile_scrape import (
    backfill_after_batches,
    query_profiles_needing_meta,
    run_batch,
)

ACTOR = "shu8hvrXbJbY3Eb9W"  # apify/instagram-scraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
log = logging.getLogger("profile_scrape")


def _load_token() -> str:
    load_dotenv()
    token = os.environ.get("APIFY_API_TOKEN")
    if not token:
        print("APIFY_API_TOKEN not set in .env or environment", file=sys.stderr)
        sys.exit(1)
    return token


# ── Tier definitions ─────────────────────────────────────────────────────

# results_limit per tier, and max profiles per batch (Apify free tier limit)
TIERS: dict[str, dict] = {
    "whales": {"results_limit": 80, "batch_size": 7, "label": "Phase 1 — Whales"},
    "top": {"results_limit": 60, "batch_size": 8, "label": "Phase 2a — Top"},
    "mid": {"results_limit": 20, "batch_size": 50, "label": "Phase 2b — Mid"},
    "tail": {"results_limit": 10, "batch_size": 300, "label": "Phase 3 — Tail"},
}

# Which profiles fall into which tier (by post_count threshold)
TIER_THRESHOLDS = [
    ("whales", 4),
    ("top", 3),
    ("mid", 2),
    ("tail", 1),
]


def _classify_profiles():
    """Query the DB and classify profiles needing backfill into tiers."""
    profiles = query_profiles_needing_meta()
    tiered: dict[str, list[str]] = {t: [] for t in TIERS}
    for p in profiles:
        username = str(p["owner_username"])
        post_count = int(p["post_count"])  # type: ignore[arg-type]
        for tier_name, threshold in TIER_THRESHOLDS:
            if post_count >= threshold:
                tiered[tier_name].append(username)
                break
    return tiered


def _chunk(seq: list[str], size: int) -> list[list[str]]:
    return [seq[i : i + size] for i in range(0, len(seq), size)]


# ── Commands ──────────────────────────────────────────────────────────────

def cmd_check() -> None:
    """Show backfill progress."""
    db = get_db()
    total = db.execute(
        "SELECT COUNT(*) FROM dim_profile WHERE is_current"
    ).fetchone()[0]
    filled = db.execute(
        "SELECT COUNT(*) FROM dim_profile WHERE follower_count IS NOT NULL"
    ).fetchone()[0]
    missing = db.execute(
        "SELECT COUNT(*) FROM dim_profile WHERE follower_count IS NULL AND is_current"
    ).fetchone()[0]
    print(f"Profiles: {total} total, {filled} with followers, {missing} remaining")
    print(f"Progress: {filled}/{total} ({filled/total*100:.0f}%)" if total else "No profiles")


def cmd_scrape(tier_name: str, token: str, dry_run: bool = False) -> None:
    """Scrape one tier of profiles."""
    if tier_name == "all":
        for t in ["whales", "top", "mid", "tail"]:
            cmd_scrape(t, token, dry_run)
        return

    tier = TIERS[tier_name]
    tiered = _classify_profiles()
    usernames = tiered[tier_name]

    if not usernames:
        print(f"{tier['label']}: no profiles need backfill")
        return

    batches = _chunk(usernames, tier["batch_size"])
    results_limit = tier["results_limit"]
    total_results = len(usernames) * results_limit
    est_cost = total_results * 0.0023

    print(f"\n{tier['label']}: {len(usernames)} profiles × {results_limit} posts")
    print(f"  Batches: {len(batches)} ({', '.join(str(len(b)) for b in batches)} profiles each)")
    print(f"  Total results: ~{total_results}, est cost: ~${est_cost:.2f}")

    if dry_run:
        print("  DRY RUN — no batches triggered")
        return

    for i, batch_usernames in enumerate(batches, start=1):
        print(f"\n  Batch {i}/{len(batches)}: {len(batch_usernames)} profiles")
        summary = run_batch(ACTOR, batch_usernames, results_limit, token)
        print(f"  ✓ {summary['items_ingested']} items in {summary['elapsed_secs']:.0f}s "
              f"(est ${summary['estimated_cost_usd']:.4f})")

    # After all batches in this tier: silver + dim + views
    print("\n  Running backfill after batches...")
    result = backfill_after_batches()
    print(f"  ✓ {result['posts_silvered']} posts silvered, "
          f"{result['profiles_with_followers']} profiles now have follower counts")


# ── CLI ───────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill dim_profile metadata via batch Apify profile scraping",
    )
    parser.add_argument(
        "command",
        choices=["check", "whales", "top", "mid", "tail", "all"],
        help="Tier to scrape, 'all' for every tier, 'check' for progress",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be scraped without triggering runs",
    )
    args = parser.parse_args()

    if args.command == "check":
        cmd_check()
        return

    token = _load_token()
    cmd_scrape(args.command, token, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
