#!/usr/bin/env python3
"""Post scraping CLI — scrape Instagram posts by URL and ingest into the pipeline.

Usage:
  uv run python scripts/post_scrape.py --urls https://ig.com/p/abc/ https://ig.com/p/def/
  uv run python scripts/post_scrape.py --urls-file urls.txt
  uv run python scripts/post_scrape.py --profile starter_story --limit 20
  uv run python scripts/post_scrape.py --profile starter_story --limit 20 --no-enrich

The script triggers an Apify run, polls for completion, ingests to bronze,
deduplicates into silver, and optionally enriches new posts via Gemini.
After enrichment, refreshes dim_profile and analytics views.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

sys.path.insert(0, "src")

from dotenv import load_dotenv

from ig_pipeline.apify import poll_run, trigger_run
from ig_pipeline.bronze import download_dataset, ingest_dataset
from ig_pipeline.db import get_db
from ig_pipeline.gold import enrich_posts, populate_dim_profile, refresh_views
from ig_pipeline.silver import deduplicate_all

ACTOR = "shu8hvrXbJbY3Eb9W"  # apify/instagram-scraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
log = logging.getLogger("post_scrape")


def _load_token() -> str:
    load_dotenv()
    token = os.environ.get("APIFY_API_TOKEN")
    if not token:
        print("APIFY_API_TOKEN not set in .env or environment", file=sys.stderr)
        sys.exit(1)
    return token


def _build_urls_from_profiles(usernames: list[str]) -> list[str]:
    """Convert usernames to Instagram profile URLs."""
    return [f"https://www.instagram.com/{u.strip('/').split('/')[-1]}/" for u in usernames]


def cmd_scrape_posts(
    urls: list[str],
    token: str,
    *,
    results_limit: int = 12,
    enrich: bool = False,
    dry_run: bool = False,
    scrape_only: bool = False,
) -> None:
    """Scrape posts from the given URLs, ingest, and optionally enrich."""
    actor = ACTOR
    print(f"\nScraping {len(urls)} URLs \u00d7 {results_limit} posts each")
    print(f"  Est cost: ~${len(urls) * results_limit * 0.0023:.2f}")

    if dry_run:
        print("  DRY RUN \u2014 no run triggered")
        return

    # Step 1: Trigger
    run = trigger_run(actor, urls, token=token, results_limit=results_limit, results_type="posts")
    print(f"  Run: {run.run_id}")
    print(f"  Dataset: {run.dataset_id}")
    print(f"  Monitor: https://console.apify.com/actors/{actor}/runs/{run.run_id}")

    # Step 2: Poll
    print("  Polling for completion...")
    dataset_id = poll_run(run.run_id, token=token, timeout=1200)
    print(f"  Run complete, dataset: {dataset_id}")

    if scrape_only:
        result = download_dataset(dataset_id, token=token, run_id=run.run_id, actor=actor)
        print(f"  Downloaded: {result.item_count} items -> {result.path}")
        print(f"  Dataset ID: {dataset_id}")
        return

    # Step 3: Bronze
    result = ingest_dataset(
        dataset_id, token=token, run_id=run.run_id, actor=actor,
    )
    print(f"  Bronze: {result.item_count} items ingested")

    # Step 4: Silver
    db = get_db()
    sr = deduplicate_all(db=db)
    print(f"  Silver: {sr.posts_silvered} posts upserted")

    # Step 5: Gold (optional)
    if enrich:
        gr = enrich_posts(db=db)
        print(f"  Gold: {gr.analysed} analysed, {gr.failed} failed, {gr.skipped} skipped")

    # Step 6: Refresh dimensions and views
    populate_dim_profile(db=db)
    refresh_views(db=db)
    print("  Dimensions + views refreshed")
# ── CLI ───────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scrape Instagram posts and ingest into the pipeline",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--urls", nargs="+", help="Instagram post or profile URLs")
    group.add_argument("--urls-file", help="File with one URL per line")
    group.add_argument("--profile", nargs="+", help="Instagram usernames")
    group.add_argument("--profiles-file", help="File with one username per line")

    parser.add_argument("--limit", type=int, default=12, help="Results limit per URL (default: 12)")
    parser.add_argument("--enrich", action="store_true", help="Run Gemini enrichment after silver")
    parser.add_argument("--dry-run", action="store_true", help="Preview without triggering")
    parser.add_argument("--scrape-only", action="store_true", help="Download only, no DuckDB steps")

    args = parser.parse_args()
    if args.urls:
        urls = args.urls
    elif args.urls_file:
        with open(args.urls_file) as f:
            urls = [line.strip() for line in f if line.strip()]
        print(f"Read {len(urls)} URLs from {args.urls_file}")
    elif args.profiles_file:
        with open(args.profiles_file) as f:
            usernames = [line.strip() for line in f if line.strip()]
        urls = _build_urls_from_profiles(usernames)
        print(f"Built {len(urls)} profile URLs from {len(usernames)} usernames in {args.profiles_file}")
    else:
        urls = _build_urls_from_profiles(args.profile)
        print(f"Built {len(urls)} profile URLs from {len(args.profile)} usernames")

    if not urls:
        print("No URLs to scrape", file=sys.stderr)
        sys.exit(1)

    token = _load_token()
    cmd_scrape_posts(
        urls, token,
        results_limit=args.limit,
        enrich=args.enrich,
        dry_run=args.dry_run,
        scrape_only=args.scrape_only,
    )


if __name__ == "__main__":
    main()
