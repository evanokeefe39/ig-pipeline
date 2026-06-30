# ig-pipeline — Agent operating context

This repo is designed to be operated by Claude via the `/ig` skill.
Keep this file current — Claude reads it on every session.

## Skill

See `skills/ig-pipeline/SKILL.md` for the full function reference.
Trigger: `/ig` — Claude imports `ig_pipeline` functions directly in `eval` cells.

## Key rules for Claude

- Never call Apify API directly. Use `ig_pipeline.apify` functions.
- Never load datasets into chat context. `bronze.ingest()` streams to disk via cursor pagination.
- Tool responses are small structured JSON (< 1 KB). Data lives on disk.
- Never use `pip`. Always use `uv` for Python package management.
- Use `.venv` in the project root.

- **PIPELINE FIRST**: Never manually chain `trigger_run`/`poll_run`/`ingest_dataset`
  in eval cells. Use the pipeline scripts (`scripts/post_scrape.py`,
  `scripts/profile_scrape.py`). If the pipeline can't express the workflow,
  that IS the gap — extend the pipeline, don't work around it.

## Layer contract

- **Bronze** — immutable raw ingest. Watermarked by run_id + actor. Never modifies after write.
- **Silver** — DuckDB `INSERT OR REPLACE` by post_id. Latest dataset wins. Media hardlinked.
- **Gold** — Gemini enrichment. Idempotent and resumable — skips already-analysed posts.
- **Query** — DuckDB views (fact_post, profile_stats, topic_stats, edge views).
  Never query gold JSON directly.

All layer directories are **env-overridable** for testing and ad-hoc analysis:
``IG_DATA_DIR``, ``IG_BRONZE_DIR``, ``IG_SILVER_DIR``, ``IG_GOLD_DIR``,
``IG_DB_PATH``. Set before import or call ``db._reset_dirs()`` to pick up
changes mid-session. Default: ``data/`` under project root.

## State tracking

DuckDB at `data/pipeline.db`. Tables:
- `bronze_ingests`, `silver_posts`, `silver_progress`, `gold_analyses`
- `dim_time` (date spine), `dim_profile` (SCD2, profile metadata), `taxonomy_terms` (SCD2 stub for Phase 3 MDM)

DuckDB views:
- `fact_post` — star schema fact view (silver + gold + dims)
- `profile_stats` — per-owner engagement aggregations
- `topic_stats` — per-topic cross-profile aggregations
- `profile_topic_edges` — graph export: profile → topic
- `profile_resource_edges` — graph export: profile → resource (tool co-occurrence)

All operations are idempotent. Run `populate_dim_time()`, `populate_dim_profile()`,
then `refresh_views()` after enrichment.

## Gold v3 schema (SCHEMA_VERSION=3)

- **Admiralty Code** (A1-F6): source reliability × information credibility
- **Freeform taxonomy**: domain, subdomain, topic, subtopic, content_type, style, format
- **educational_json**: {summary, workflow[{step,tool,detail}], concepts[{term,explanation}], principles[], techniques[]}
- **actionable_json**: {summary, resources[{name,url,type,purpose}], tools[], guides[], downloads[]}
- **has_engagement_bait**: regex on caption in silver (no LLM cost)

## Profile metadata

Profile metadata (follower count, bio, category) is NOT available in post-scrape
output (`resultsType: "posts"`). To get it, use the same Instagram Scraper actor
with `resultsType: "details"` — returns one record per profile with `followersCount`,
`biography`, `businessCategoryName`, `verified`, `externalUrl`, etc.

Profile metadata is ingested directly into `dim_profile` via
`populate_dim_profile_from_details()` (SCD2 — each scrape creates a new row with
`effective_from`/`effective_to`). The post-scraping pipeline does NOT need to run
for profile metadata — `resultsType: "details"` costs ~$0.99 for 368 profiles.

**Current state (2026-06-29):** 368/371 profiles have follower counts (99%).
Total reach: ~63M followers. 3 profiles missing (private/deleted).

## Known issues / technical debt

- **`stream_dataset` corrupts NDJSON**: large records with special characters in
  bios produce truncated/invalid JSON lines. The bronze file for dataset
  `o44ZGN3WOEuMzCgcf` had 154/498 lines broken. Workaround: fetch clean JSON
  from Apify API directly and rewrite the bronze file.
- **`deduplicate_all()` is monolithic**: processes ALL un-silvered datasets.
  Cannot silver a single dataset ad-hoc. Needs decomposition into
  `silver_dataset(dataset_id)`.
- **No ETL orchestration framework**: tasks are manually chained in scripts.
  No `PipelineRun` tracking, no retry, no scheduling. Dagster migration planned
  (see `issues.md` for migration plan). Surgical changes made to decouple
  bronze downloads from DuckDB — the codebase is now Dagster-ready:
  - `download_dataset()` writes files + sidecar, no DuckDB
  - `bronze_ingests` table removed
  - `--scrape-only` for parallel downloads
  - `run_pipeline.py` is pure medallion processor
- **No per-profile watermarks**: rescraping fetches all posts each time.
  Evaluating dlt (data load tool) for incremental loading with cursor-based
  watermarks per profile source.

- **Pipeline UX gaps**: The pipeline should accept flexible input configs
  rather than requiring manual eval-cell orchestration:
  - Post scraping: `post_scrape.py` already handles ad-hoc lists via `--urls`,
    `--urls-file`, or `--profile`. No gap.
  - Profile scraping: module supports ad-hoc lists (`run_batch`) but CLI is
    tier-only. Low priority.
  - `resultsType: "details"` scraping: no CLI script chains trigger→poll→
    ingest→populate. Deferred.

## Decisions (2026-06-29)

- **Profile metadata via `resultsType: "details"`**: the Instagram Scraper
  actor supports this — cheaper and simpler than the separate profile-scraper
  actor. One run handles all profiles (~$0.99 for 368).
- **Profile data is SCD2**: follower counts change over time. Each scrape
  creates a new `dim_profile` row with `effective_from`/`effective_to`.
- **dlt evaluation**: dlt provides built-in incremental loading, watermarks,
  and DuckDB support. Consider adopting for extraction layer, keeping the
  medallion architecture underneath.

## Decisions (2026-06-30) — surgical changes

- **Bronze layer decoupled from DuckDB**: `download_dataset()` writes `.jsonl`
  + `.jsonl.meta` sidecar (run_id, actor, item_count). No DuckDB writes during
  download. Enables parallel scraping without lock contention.
- **`bronze_ingests` table removed**: bronze state is file-based. `silver.py`
  scans `data/bronze/*.jsonl` instead of querying `bronze_ingests`.
- **`--scrape-only` flag on `post_scrape.py`**: downloads files + sidecar only,
  zero DuckDB. Multiple batches run in parallel.
- **`run_pipeline.py` is pure medallion processor**: dedup → enrich → dim →
  views. No Apify API calls (was mixing runs from other projects).

## Dagster migration plan

See `issues.md` for full migration plan (asset mapping, IO manager, config,
execution model, and 8-step migration path). Key points:

- Each current pipeline layer maps to one Dagster asset
- DuckDB IO manager serializes writes per Dagster run process
- Config class drives scraping parameters
- Bronze file sidecars carry metadata through the asset graph
- Migration is split into scoped steps — no big-bang rewrite

## Test conventions
