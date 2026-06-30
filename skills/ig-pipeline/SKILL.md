---
name: ig-pipeline
description: Instagram saved-posts pipeline — Apify extract, medallion lake, Gemini enrich
trigger: /ig
---

# /ig — Instagram Pipeline

All pipeline operations through `eval` imports from `ig_pipeline`. NEVER call the Apify API directly. All ingest streams to disk — no large JSON in tool responses.

## Available functions

### Explore (read-only, ad-hoc)
```
from ig_pipeline.apify import search_actors, inspect_actor, list_runs
search_actors("instagram")          → [{name, title, description, score}]
inspect_actor("apify/instagram-scraper") → {name, input_schema, pricing}
list_runs("apify/instagram-scraper", limit=10) → [{run_id, status, dataset_id}]
```

### Scrape (ad-hoc or batch)
```
from ig_pipeline.apify import trigger_run, poll_run
from ig_pipeline.bronze import ingest_dataset
trigger_run("apify/instagram-scraper", urls)   → {run_id, dataset_id, estimated_cost}
poll_run(run_id)                               → dataset_id
ingest_dataset(dataset_id, run_id=run_id, actor=actor, token=token) → {path, item_count, skipped}
```
All Apify API calls have retries (4 attempts, exponential backoff on 429/5xx).

### Silver — upsert (idempotent, DuckDB INSERT OR REPLACE)
```
from ig_pipeline.silver import deduplicate_all
deduplicate_all()  → {datasets_processed, posts_silvered}
```
Crash-safe: `silver_progress` table tracks dataset-level completion. Datasets are processed in `ingested_at ASC` order so the latest dataset's writes win.

### Enrich (resumable, skips analysed, retries failed)
```
from ig_pipeline.gold import enrich_posts, refresh_views
from ig_pipeline.gold import populate_dim_time, populate_dim_profile

enrich_posts()                           # all un-analysed posts
enrich_posts(max_posts=50)               # first 50 un-analysed
enrich_posts(post_ids=["id1","id2"])     # specific posts

# After enrichment, seed dimensions and refresh views:
populate_dim_time()                      → int (date count)
populate_dim_profile()                   → int (profile count)
refresh_views()                          → {views_created: 5}
```
- Gemini Flash model `gemini-3.1-flash-lite`
- Gold v3 schema (SCHEMA_VERSION=3): Admiralty Code (A1-F6), freeform taxonomy,
  educational_json + actionable_json
- Silver detects engagement bait (regex on caption) — zero LLM cost
- Async pipeline: TokenBucket (14 RPM), upload worker, generate worker with retry/backoff
- `status='analysed'`: skipped on re-run
- `status='failed'`: retried on re-run
- Previous `enriched.json` archived to `data/archive/<post_id>/` before overwrite

### Analytics (DuckDB views)
```
from ig_pipeline.gold import refresh_views
refresh_views()
# Then query via DuckDB:
#   fact_post               — star schema (silver + gold + dims)
#   profile_stats           — per-owner engagement aggregations
#   topic_stats             — per-topic cross-profile aggregations
#   profile_topic_edges     — graph export: profile → topic
#   profile_resource_edges  — graph export: profile → resource
```
### Batch: weekly scheduled pipeline
Runs `scripts/run_pipeline.py` — queries recent Apify runs, skips ingested, upserts to silver, enriches.

### Resuming after a crash
- **Silver**: `deduplicate_all()` reprocesses any dataset not in `silver_progress`. INSERT OR REPLACE is idempotent — no duplicates.
- **Gold**: `enrich_posts()` skips `status='analysed'` posts. Failed posts with `status='failed'` are retried automatically.

## Constraints
- NEVER call Apify API directly. Use package functions.
- NEVER load datasets into memory. `ingest_dataset()` streams to disk.
- Tool responses are small structured JSON (<1 KB). Data lives on disk.
- NEVER f-string SQL queries. Always parameterized `?` placeholders.
- GEMINI_API_KEY must be set in environment for enrichment.
