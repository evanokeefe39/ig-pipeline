# Changelog

## 2.1.0 (2026-06-29) — Pipeline hardening

### Correctness fixes
- **SQL injection fixed**: `enrich_posts()` uses parameterized queries for `post_ids` and `max_posts`. F-string interpolation removed.
- **Silver crash-safety**: Added `silver_progress` table. Incomplete datasets are reprocessed on re-run — no silent data loss.
- **Silver recency**: Datasets processed in `ingested_at ASC` order so latest writes win.
- **Duplicate post handling**: `deduplicate_all()` now processes duplicate `post_id` across datasets correctly via ordering.
- **Hardcoded path removed**: `_link_media()` uses `_db.DATA_DIR` instead of `Path("data")`.

### Gold layer — real Gemini pipeline
- **No more stub**: `enrich_posts()` calls Gemini Flash (model `gemini-3.1-flash-lite`) via the `google-genai` SDK.
- **Async pipeline**: TokenBucket rate limiter (14 RPM), upload worker, generate worker with exponential backoff (429 → 60/120/240/480s, 503 → 10/20/30/40s).
- **Resumable**: Analysed posts are skipped; failed posts (status='failed') are retried on re-run.
- **Archive-on-overwrite**: Previous `enriched.json` moved to `data/archive/<post_id>/` before overwrite.
- **SDK migration**: Ported from `google-generativeai` to `google-genai` (new unified SDK).

### Removed
- **publish/unpublish**: Removed entirely — overkill for current pipeline. `published` column dropped from `gold_analyses`. `PublishResult` model deleted.
- **`published_posts` DuckDB view**: Removed.
- **`watermarks` table**: Never used — removed from schema.
- **`run_history` table**: Never used — removed from schema.
- **Orphaned SQL files**: `sql/` directory removed.

### Resilience
- **HTTP retries**: All Apify API calls (`_get`, `_post`, `stream_dataset`) wrapped with `tenacity` retries. Exponential backoff on 429, 5xx, and network errors. 4 attempts, 2-30s wait.
- **Edge case guards**: `enrich_posts()` handles empty `post_ids` list and `max_posts=0`.

### CI
- **uv in GitHub Actions**: Replaced `pip install` with `astral-sh/setup-uv@v5` + `uv sync`.
- **Environment variables**: Secrets passed via `env:` instead of `.env` file.
- **Test gate**: `pipeline` job depends on `test` job passing.

### Testing
- 12 tests (was 9 → 7 after publish removal + 5 new gold tests)
- Gold enrichment tests: post_ids filter, resumability (analysed skip, failed retry), max_posts limit, max_posts=0 guard.
- All tests use in-memory DuckDB via DI pattern.

### Housekeeping
- Import ordering fixed across all modules — ruff `I` rules pass.
- All ruff diagnostics resolved (0 warnings).
- Stale `serve.cpython-314.pyc` cleaned.

## 2.0.0 (2026-06-29)

### Architecture
- Complete rewrite with medallion architecture (bronze/silver/gold)
- Single DuckDB database for pipeline state and analytical queries
- Dependency injection pattern for all pipeline functions (`db=None`)
- Serving is out of scope — query DuckDB directly or via `read_json()`

### Extraction (Apify)
- `apify.py`: trigger_run, poll_run, stream_dataset (cursor pagination)
- `apify.py`: search_actors, inspect_actor, list_runs (actor exploration)
- Dataset streaming via `?format=jsonl` — never holds data in memory
- SHA-256 checksums on bronze ingest
- Watermarking: `run_id` + `actor` recorded in `bronze_ingests`

### Deduplication (Silver)
- DuckDB `INSERT OR REPLACE` by post_id — native upsert, no SELECT-check pattern
- Latest dataset wins automatically
- Media hardlinking (zero-copy on same filesystem)
- Idempotent — safe to run repeatedly

### Enrichment (Gold)
- Gemini Flash enrichment (async, rate-limited, resumable)
- DuckDB views: `posts`

### Testing
- 7 tests, in-memory DuckDB (`:memory:`), zero `get_db` monkeypatching
- DI pattern: `function_under_test(db=test_db)`
- `respx` for Apify API mocking, `tmp_path` for filesystem isolation
- Upsert test proves `INSERT OR REPLACE` overwrites on conflict
