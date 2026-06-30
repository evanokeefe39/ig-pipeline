"""Cron entry point. Processes local bronze files through all pipeline layers.

Safe to run repeatedly — each layer checks state before acting.
No Apify API calls — operates entirely on local bronze files.
"""

import sys
import logging

sys.path.insert(0, "src")

from ig_pipeline.silver import deduplicate_all
from ig_pipeline.gold import enrich_posts, populate_dim_profile, refresh_views
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
log = logging.getLogger("pipeline")


def run():
    log.info("Pipeline start")
    sr = deduplicate_all()
    log.info("  silver: %d posts", sr.posts_silvered)
    gr = enrich_posts()
    log.info("  gold: %d analysed, %d failed", gr.analysed, gr.failed)
    n_profiles = populate_dim_profile()
    log.info("  dim_profile: %d profiles updated", n_profiles)
    refresh_views()
    log.info("Pipeline complete")

if __name__ == "__main__":
    run()
