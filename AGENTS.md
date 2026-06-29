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

## Layer contract

- **Bronze** — immutable raw ingest. Watermarked by run_id + actor. Never modifies after write.
- **Silver** — DuckDB `INSERT OR REPLACE` by post_id. Latest dataset wins. Media hardlinked.
- **Gold** — Gemini enrichment. Idempotent and resumable — skips already-analysed posts.
- **Query** — DuckDB views (fact_post, profile_stats, topic_stats, edge views).
  Never query gold JSON directly.

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

Raw Apify post data includes `ownerId`, `ownerUsername`, `ownerFullName` ONLY.
Follower count, bio, profile category are NOT in post-scrape output.
Phase 5 will require a separate Apify profile-scraper actor.
`profile_stats` view computes aggregate stats from our scraped posts.

## Test conventions
