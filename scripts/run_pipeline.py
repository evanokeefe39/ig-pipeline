#!/usr/bin/env python3
"""Cron entry point. Runs all pipeline layers, idempotently.

Safe to run repeatedly — each layer checks state before acting.
"""

import sys
import logging

sys.path.insert(0, "src")

from ig_pipeline.apify import trigger_run, poll_run, list_runs
from ig_pipeline.bronze import ingest_dataset
from ig_pipeline.silver import deduplicate_all
from ig_pipeline.gold import enrich_posts, refresh_views

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
log = logging.getLogger("pipeline")


def run():
    log.info("Pipeline start")

    token = _load_token()

    # Find recent completed Apify runs we haven't ingested yet
    from ig_pipeline.db import get_db
    db = get_db()
    ingested = {
        r[0] for r in db.execute(
            "SELECT dataset_id FROM bronze_ingests"
        ).fetchall()
    }

    runs = list_runs("apify/instagram-scraper", token=token, limit=10)
    for run in runs:
        if run.status == "SUCCEEDED" and run.dataset_id and run.dataset_id not in ingested:
            result = ingest_dataset(
                run.dataset_id,
                token=token,
                run_id=run.run_id,
                actor="apify/instagram-scraper",
            )
            log.info("  bronze: %s → %d items", result.dataset_id, result.item_count)

    # Silver — upsert new bronze data
    sr = deduplicate_all()
    log.info("  silver: %d posts", sr.posts_silvered)

    # Gold — enrich un-analysed posts
    gr = enrich_posts()
    log.info("  gold: %d analysed, %d failed", gr.analysed, gr.failed)

    refresh_views()
    log.info("Pipeline complete")


def _load_token() -> str:
    import os
    from dotenv import load_dotenv
    load_dotenv()
    return os.environ["APIFY_API_TOKEN"]


if __name__ == "__main__":
    run()
