# Lessons learned — ig-pipeline v2 build

## 2026-06-29

### Dependency injection for DB connections
**Problem**: Monkeypatching `get_db` across test modules caused state leakage.
**Fix**: All pipeline functions accept `db=None`. Tests pass `get_db(":memory:")` directly.
**Rule**: Never monkeypatch `get_db`. Inject via the function parameter.

### DuckDB vs SQLite syntax
**Problem**: `datetime('now')` works in SQLite but not DuckDB.
**Fix**: Use `CURRENT_TIMESTAMP`.
**Rule**: Always test DDL against DuckDB, not SQLite.

### Module-level import capture
**Problem**: `from .db import BRONZE_DIR` captures value at import time. Monkeypatches don't propagate.
**Fix**: `from . import db as _db` then `_db.BRONZE_DIR` (attribute access on module ref).
**Rule**: Import the module, not the attribute, when testability matters.

### DuckDB upsert for dedup
**Problem**: SELECT-then-INSERT pattern for dedup is verbose and requires two round-trips.
**Fix**: `INSERT OR REPLACE INTO silver_posts (...) VALUES (...)` — DuckDB native upsert by primary key. Latest dataset wins automatically.
**Rule**: Use `INSERT OR REPLACE` for idempotent writes with a natural key.

### Mixed carousels
**Problem**: `post_media()` returned only video slides for any carousel with a video child.
**Fix**: Return all slides in order. Interleave uploaded File objects + inline image bytes.
**Rule**: Iterate all media, handle each by type, preserve order.

### uv over pip
**Problem**: `pip install -e .` failed on Python 3.14 with setuptools backend issues.
**Fix**: Use `hatchling` build backend + `uv venv && uv pip install -e .`.
**Rule**: Always `uv`, never `pip`. `.venv` in every Python project.

### Watermarking
**Problem**: Bronze ingest didn't record which Apify run produced the data.
**Fix**: `ingest_dataset()` accepts `run_id` + `actor` params. `scripts/run_pipeline.py` queries `list_runs()` and skips ingested datasets.
**Rule**: Record source identity (run_id, actor) at ingest time for incremental processing.

## 2026-06-29 — Pipeline hardening

### Never f-string SQL queries
**Problem**: `enrich_posts()` built SQL with f-string interpolation: `f"WHERE s.post_id IN ({placeholders})"`. A post_id containing a single quote breaks the query. An externally-sourced post_id is SQL injection.
**Fix**: Use parameterized queries: `"WHERE s.post_id IN (" + ", ".join("?" * len(post_ids)) + ")"` with params list.
**Rule**: Never f-string SQL. Always parameterized placeholders. Validate scalar inputs (LIMIT, max_posts) before binding.

### Stubs disguised as working code are data footguns
**Problem**: `enrich_posts()` wrote hardcoded dummy JSON and marked posts as `status='analysed'`. The pipeline "succeeded" but produced garbage that would never be re-processed (resumability check skips `status='analysed'`).
**Fix**: Either implement the real pipeline or raise `NotImplementedError`. Never ship stubs that lie about working.
**Rule**: Placeholders write no state. If a function can't do its job, it must fail loudly.

### Silver dedup status is a dataset-level concern, not post-level
**Problem**: `deduplicate_all()` inferred dataset completion from `DISTINCT source_dataset FROM silver_posts`. If a dataset had 100 posts and the process crashed after 50, the remaining 50 were silently lost because `source_dataset` already appeared in `silver_posts`.
**Fix**: Add `silver_progress` table. Write to it ONLY after ALL posts in a dataset are committed. On re-run, dataset not in `silver_progress` → reprocess entirely. INSERT OR REPLACE handles already-written posts idempotently.
**Rule**: Track multi-item batches at the batch level. Never infer batch completion from individual item state.

### DuckDB execute() is always truthy — never check UPDATE rowcount with it
**Problem**: `r = db.execute("UPDATE ..."); count += 1 if r else 0` — `db.execute()` returns DuckDBPyConnection (always truthy), so count was always incremented regardless of whether the UPDATE matched. `r.rowcount` returns -1 for DuckDB.
**Fix**: Use `RETURNING` clause: `UPDATE ... RETURNING post_id` then `len(r.fetchall())` gives actual affected rows.
**Rule**: Never rely on DuckDB execute return value for affected-row counts. Use RETURNING.

### HTTP wrappers need retries on transient errors
**Problem**: `apify.py` `_get()`/`_post()`/`stream_dataset()` had zero retry logic. A single 429 or 503 killed the pipeline. `tenacity` was a dependency but never imported.
**Fix**: Add `@retry` decorator with `retry_if_exception(_is_retryable)` — retry on 429, 5xx, network errors with exponential backoff. Do NOT retry on 4xx (permanent).
**Rule**: Every external HTTP call gets retries. Use tenacity with a specific retry condition function.

### Dead infrastructure rots
**Problem**: `watermarks` and `run_history` tables were defined in schema but never written to or read by any code. `sql/silver_deduplicate.sql` and `sql/gold_views.sql` were never loaded. `serve.pyc` lingered after `serve.py` was removed.
**Fix**: Delete all unexercised schema, SQL files, and stale artifacts. If it's not wired in, it's not real.
**Rule**: Schema tables that aren't read or written are speculative overproduction. Ship only what's wired.
