# Session Summary — 2026-06-29

## What we built

Phases 1-2 of the competitive analysis rollout plan.  The `ig-pipeline`
repo now has a thin star schema, v3 gold extraction with Admiralty Code
credibility signals, and 5 analytics views ready for querying.

### Repo setup
- Git initialized, public repo: https://github.com/evanokeefe39/ig-pipeline
- Branch protection: PR + approval + squash-merge on `master`
- `.gitignore` covers `.env`, `.env.*`, `data/`, `*.db`, `*.log`, IDE/OS files
- `.gitattributes` for line ending normalization

### Data migration
- 828 posts (3.8 GB) migrated from `scrape-ig-saved-list` to `ig-pipeline/data/`
- 4 bronze JSONL datasets, media hardlinked into silver

### Schema evolution
- `silver_posts` expanded: engagement counts, owner info, hashtags, timestamp,
  `has_engagement_bait` (regex on caption — zero LLM cost)
- `dim_time` — date spine (293 dates)
- `dim_profile` — SCD2 (368 profiles, follower_count NULL until Phase 5)
- `taxonomy_terms` — SCD2 stub for Phase 3 MDM

### Gold v3 (SCHEMA_VERSION=3)
- **Admiralty Code** (A1-F6): source reliability × information credibility
- **Freeform taxonomy**: domain, subdomain, topic, subtopic, content_type, style, format
- **Two-JSON**: `educational_json` + `actionable_json` replace 6 extraction arrays
- 5 posts enriched, output verified

### Analytics views (5)
`fact_post`, `profile_stats`, `topic_stats`, `profile_topic_edges`, `profile_resource_edges`
All resolve via DuckDB.  Graph edge views exportable to Neo4j when needed.

### Tests
18 tests, all passing.  Coverage: silver columns, bait detection, dim tables,
all 5 views.  In-memory DuckDB via DI pattern.

## Key decisions

1. **Thin star schema** (3 dims + fact view + edge views).  DuckDB handles all current queries.
   Graph DB (Neo4j) deferred until we need PageRank / community detection.
2. **Taxonomy Registry is MDM, not a pipeline layer.**  Gold writes raw terms.
   Normalization (embedding → cluster → merge) is a separate offline process.
   SCD2 for audit trail.  Review queue for cos 0.85-0.95 merges.
3. **No follower count in raw Apify post data.**  Only `ownerId`, `ownerUsername`,
   `ownerFullName`.  Phase 5 will need a separate Apify profile-scraper actor.
4. **`value_score` dropped.**  Replaced by Admiralty Code (objective criteria)
   + `is_actionable` (can you apply this?) + `has_engagement_bait` (no LLM cost).

## Git state

Branch: `feat/competitive-analysis` (behind PR #1)
4 commits ahead of `master`:
- `f67c5cd` — Plan document
- `453b6c7` — V3 gold prompt
- `ffb5566` — Silver + dims + views + tests
- `d1cf603` — Docs + changelog

PR #1 is open and ready for review.

## Next session — Phase 3

Taxonomy Registry MDM:
1. `normalize_taxonomy.py` — extract unique terms → embeddings → cluster → merge
2. `taxonomy_review` view — pending review queue for cos 0.85-0.95
3. `/normalize-taxonomy` skill — interactive accept/reject workflow
4. Wire `dim_topic` to resolve through `taxonomy_terms WHERE is_current = true`
5. Run on v3 data, refresh views with canonical terms

The `taxonomy_terms` SCD2 table is ready.  `dim_topic` table needs to be
created and seeded from taxonomy_terms.  Views need to JOIN through canonical
terms.

## Key files

| File | What |
|---|---|
| `tasks/plans/competitive-analysis.md` | Full requirements, architecture, gap analysis, 7-phase plan |
| `src/ig_pipeline/gold.py` | V3 prompt, populate_*, refresh_views, enrich_posts |
| `src/ig_pipeline/silver.py` | Engagement extraction, _detect_engagement_bait, _link_media |
| `src/ig_pipeline/db.py` | All table schemas (silver_posts, dims, taxonomy_terms) |
| `tests/test_silver.py` | 6 tests (new columns, bait detection, bait in silver) |
| `tests/test_views.py` | 12 tests (all views, dim tables, idempotence) |
| `data/pipeline.db` | DuckDB with 812 posts, 368 profiles, 5 enriched |