# Pipeline hardening — get v2 working end-to-end

## Intent

The v2 ig-pipeline has sound architecture but several correctness bugs, a stub
disguised as working code, dead artifacts, and reliability gaps. This plan makes
the pipeline actually function end-to-end: bronze ingest → silver dedup → gold
Gemini enrichment, with idempotency, resilience, and no footguns.

## Context

### Relevant existing code
- `src/ig_pipeline/gold.py` — stub `enrich_posts()` writes hardcoded dummy JSON, marks as analysed, never re-runs. SQL injection via f-string. `publish()`/`unpublish()` have count bugs and are being removed.
- `src/ig_pipeline/silver.py` — partial-crash data loss: dataset marked done after first post. `_link_media()` hardcodes `Path("data")`. Import of `os` inside function body.
- `src/ig_pipeline/apify.py` — no retries on any HTTP call despite `tenacity` dependency. Pagination header unverified against real API.
- `src/ig_pipeline/db.py` — `watermarks` and `run_history` tables defined but never written to. Singleton `_conn` not thread-safe.
- `src/ig_pipeline/models.py` — `PublishResult` only used by the functions being removed.
- `scripts/run_pipeline.py` — no error handling, no run_history writes, no watermarks usage.
- `sql/*.sql` — two SQL files never loaded by any code. Orphaned dead artifacts.
- `.github/workflows/pipeline.yml` — uses `pip` not `uv`, writes secrets to `.env` file.
- `tests/test_gold.py` — tests publish/unpublish (being removed), doesn't test enrichment.
- v1 `quick_analyze.py` — working async Gemini pipeline to port: TokenBucket, upload_file, build_media_parts, generate (with retry/backoff), process_post, post_media, PROMPT.

### Architectural constraints
- DuckDB single-file state + analytical queries. Not changing this.
- DI pattern (db=None). Not changing this.
- INSERT OR REPLACE for upserts. Not changing this.
- Medallion layers (bronze/silver/gold). Not changing this.
- `uv` for all Python package management. Not changing this.

### Prior decisions
- DuckDB over SQLite — `read_json()` reads gold files directly.
- DI over monkeypatching — confirmed by three rounds of monkeypatch bugs in v1.
- INSERT OR REPLACE over SELECT-then-INSERT — eliminates manual dedup.
- No serve.py — serving is out of scope.
- No dbt, no Airbyte, no Prefect — 527 posts don't warrant orchestration frameworks.

### Anti-patterns to avoid
- Stubs/placeholders marked as "done" — the gold stub is the canonical example.
- F-string SQL interpolation — use parameterized queries.
- Global mutable state for test isolation — the singleton `_conn` already caused leakage.
- Speculative infrastructure — tables/code that aren't used yet "for later."

## Scope of this plan

### What's in
1. Remove publish functionality entirely.
2. Fix all correctness bugs (SQL injection, silver data loss, gold stub).
3. Port the v1 async Gemini pipeline into gold.py.
4. Add HTTP retries to apify.py.
5. Clean up dead artifacts and code hygiene.
6. Fix CI to use uv and proper secret handling.
7. Update tests for all changes.

### What's out of scope
- Migrating v1 data into v2 format (separate task after pipeline works).
- Serving/query layer (separate concern, deliberately excluded).
- Bronze `trigger_run`/`poll_run` end-to-end test (requires Apify token + real run — smoke test separately).
- Replacing the DuckDB singleton with connection pooling (not needed at this scale).

## Phases

### Phase 1: Remove publish functionality

Remove all publish/unpublish code and schema. It's overkill for a pipeline that
isn't even working yet. If visibility gating is needed later, it can be added
as a simple WHERE clause on a view.

Files:
- `gold.py` — delete `publish()`, `unpublish()`, remove `published` param from
  `enrich_posts()` signature. Remove `published` from the INSERT in
  `enrich_posts()`. Remove `published_posts` view from `refresh_views()`.
- `db.py` — drop `published` column from `gold_analyses` schema. Drop
  `PublishResult`-related schema (none, but the column goes).
- `models.py` — delete `PublishResult` class.
- `run_pipeline.py` — remove `published=True` from `enrich_posts()` call.
- `sql/gold_views.sql` — delete `published_posts` view.
- `tests/test_gold.py` — delete `test_publish_and_unpublish`.
- `README.md` — remove publish from usage examples and design decisions.
- `AGENTS.md` — remove published flag references.

### Phase 2: Fix correctness bugs

**2a. SQL injection in gold.py `enrich_posts()`**

Replace f-string interpolation with parameterized queries:
- `post_ids` IN clause: build `?, ?, ?` placeholders from `len(post_ids)`, pass
  as params list.
- `max_posts` LIMIT: validate `isinstance(max_posts, int)` and pass as param, or
  use `LIMIT ?` with the int value.

**2b. Silver partial-crash data loss**

Add a `silver_progress` table to track dataset-level completion:
```
CREATE TABLE IF NOT EXISTS silver_progress (
    source_dataset TEXT PRIMARY KEY,
    post_count     INTEGER NOT NULL,
    completed_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
```
- `deduplicate_all()` writes to `silver_progress` only after ALL posts in a
  dataset are processed and committed.
- The "find un-silvered datasets" query changes to check `silver_progress`
  instead of inferring from `silver_posts` rows.
- On re-run after a crash: dataset not in `silver_progress` → reprocess entirely.
  INSERT OR REPLACE handles the already-written posts idempotently.

**2c. Silver dedup doesn't respect recency across datasets**

Add `ORDER BY b.ingested_at DESC` to the dataset query so the most recently
ingested dataset is processed last (its INSERT OR REPLACE wins for shared
post_ids). Alternatively, process all datasets but ensure the upsert order
matches recency. The simplest correct approach: query datasets ordered by
`bronze_ingests.ingested_at ASC`, so the latest one's writes land last.

**2d. `_link_media()` hardcodes `Path("data")`**

Change to `_db.DATA_DIR / source_dataset / post_id`.

**2e. `Sequence` not imported in gold.py**

Add `from collections.abc import Sequence` to imports.

**2f. Bronze.py dead conditional**

Remove `row[1] if len(row) > 1 else ""` — always a 2-tuple. Use `row[1]`.

**2g. Silver.py inline `import os`**

Move to module top.

### Phase 3: Port v1 Gemini pipeline into gold.py

This is the core work — replacing the stub with the real async enrichment.

**What to port from v1 `quick_analyze.py`:**
- `PROMPT` (lines 116-164) — the extraction prompt. Copy verbatim.
- `TokenBucket` class (lines 236-273) — rate limiter.
- `upload_file()` (lines 280-317) — upload videos to Gemini Files API.
- `build_media_parts()` (lines 324-344) — interleave uploaded videos + inline images.
- `generate()` (lines 347-406) — call Gemini with retry/backoff.
- `post_media()` (lines 707-724) — enumerate media files for a post in order.
- `mime_type()`, `is_video()`, `media_type_label()` — helpers.
- `archive_existing_analysis()` (lines 419-449) — archive before overwrite.
- `process_post()` (lines 467-560) — the per-post async flow.

**Adaptations needed for v2:**
- Media lives in `silver/posts/{post_id}/media/` not `data/{dataset}/{post_id}/`.
- `post_media()` reads from `silver_dir / post_id / media/` instead of the v1
  post directory.
- State is DuckDB `gold_analyses` table, not SQLite `files` table.
- Write `enriched.json` to `gold/posts/{post_id}/enriched.json` (already the
  v2 convention).
- Archive to `data/archive/{post_id}/` before overwrite (same pattern as v1).
- Use `google-genai` SDK (`from google import genai`) — v1 uses `google.generativeai`
  (the older SDK). Check which SDK `pyproject.toml` declares. It says
  `google-genai` (the new SDK). The v1 code uses `google.generativeai` (old SDK).
  This is a breaking difference — the API calls differ. Need to adapt the v1
  code to the new SDK's API surface.
- `enrich_posts()` becomes the sync entry point that calls `asyncio.run()` on
  the async pipeline, similar to v1's `main()`.
- Remove the redundant `INSERT OR IGNORE` before `INSERT OR REPLACE`.
- Write `gold_analyses` row only after successful analysis. On failure, write
  with `status='failed'` and `error` message, increment `attempts`.

**Schema change for gold_analyses:**
- Keep `published` removed (Phase 1).
- Ensure `error` column is used for failed analyses.
- `attempts` column is incremented on each attempt.

**Test strategy for gold:**
- Test that `enrich_posts()` calls Gemini (mock `genai.GenerativeModel` or
  the new SDK equivalent).
- Test that successful analysis writes `enriched.json` + updates DuckDB.
- Test that failed analysis sets `status='failed'` with error message.
- Test resumability: posts with `status='analysed'` are skipped.
- Test `max_posts` limit is respected.
- Test `post_ids` filter works.

### Phase 4: Add HTTP retries to apify.py

Use `tenacity` (already a dependency) to wrap `_get()` and `_post()`:
- Retry on `httpx.HTTPStatusError` with 429, 500, 502, 503, 504 status codes.
- Exponential backoff: 2s, 4s, 8s, 16s (max 4 attempts).
- No retry on 4xx (except 429) — those are permanent errors.
- Use `@retry` decorator or a wrapper function.

Also add retries to `stream_dataset()` pagination loop — same transient errors.

**Test:** Mock httpx to return 503 then 200, verify retry succeeds.

### Phase 5: Smoke-test stream_dataset pagination

Per the External Integration Gate, verify the pagination header against the real
Apify API before relying on it. This requires an Apify token and a real
dataset_id.

- Run `stream_dataset()` against a small known dataset.
- Verify item count matches the Apify dataset count.
- Verify the pagination cursor header name is correct.
- If the header is wrong, fix the cursor extraction to use the response body
  (Apify may return pagination info in JSON, not headers).

This is a manual verification step, not an automated test. Document the result
in `tasks/lessons.md`.

### Phase 6: Clean up dead artifacts and hygiene

**6a. Delete orphaned SQL files**
- `sql/silver_deduplicate.sql` — never loaded, Python dedup is inline. Delete.
- `sql/gold_views.sql` — duplicated by `refresh_views()` in gold.py. Delete.
- Remove `sql/` directory if empty.

**6b. Remove unused DB tables**
- `watermarks` table — never read or written. Remove from `_init_schema()`.
- `run_history` table — never read or written. Remove from `_init_schema()`.
- (If run tracking is needed later, add it then. Not now.)

**6c. Clean stale pycache**
- Delete `src/ig_pipeline/__pycache__/serve.cpython-314.pyc` (stale — no serve.py exists).

**6d. Fix import ordering**
- `silver.py`: move `from .models import SilverResult` to top with other imports.
- `gold.py`: move `from .models import GoldResult, PublishResult` (now just `GoldResult`) to top.

**6e. Batch publish removal from views**
- Already handled in Phase 1 — `refresh_views()` drops `published_posts` view.
- The `posts` view drops the `published` column from SELECT.

**6f. Separate exploration functions (optional, low priority)**
- Move `search_actors()`, `inspect_actor()` to `explore.py`.
- Move `ActorSummary`, `ActorDetail`, `InputField`, `RunSummary` models with them
  or to a shared models file.
- Keep `list_runs()` in apify.py (used by run_pipeline.py).
- Defer if it adds churn without value.

### Phase 7: Fix CI

**7a. Use uv in GitHub Actions**
- Replace `pip install -e .` with:
  ```yaml
  - uses: astral-sh/setup-uv@v6
  - run: uv venv && uv pip install -e ".[dev]"
  - run: uv run pytest tests/ -v
  ```
- Replace `cache: pip` with uv's caching.

**7b. Use env vars for secrets, not .env file**
- Replace `.env` file writing with:
  ```yaml
  - run: uv run python scripts/run_pipeline.py
    env:
      APIFY_API_TOKEN: ${{ secrets.APIFY_API_TOKEN }}
      GEMINI_API_KEY: ${{ secrets.GEMINI_API_KEY }}
  ```

**7c. Add test step to CI**
- Run `uv run pytest tests/ -v` before the pipeline step.
- Pipeline only runs if tests pass.

### Phase 8: Update documentation

- `README.md` — remove publish from examples, update schema docs, update design decisions.
- `AGENTS.md` — remove published flag references, update layer contract.
- `tasks/lessons.md` — add entries for: SQL injection pattern, partial-crash data loss pattern, stub-disguised-as-working pattern.

## Edge case inventory

1. **Post with no media files** — `post_media()` returns empty list. Gemini gets
   prompt only (text from caption). Should this be skipped or analysed? v1
   analyses it (caption text alone). Keep this behavior.
2. **Post with no id and no shortCode** — silver.py skips these (line 69-70).
   v1 had the `unknown/` folder collision bug. Confirm silver.py handles it.
3. **Carousel with >12 slides** — v1 caps at MAX_SLIDES=12. Keep this guard.
4. **Gemini returns malformed JSON** — v1 retries with short sleep. Keep this.
5. **Gemini 429 rate limit** — v1 backs off 60/120/240/480s. Keep this.
6. **Gemini 503/unavailable** — v1 retries with 10*attempt seconds. Keep this.
7. **Upload fails for a video slide** — v1 marks post as failed, skips. Keep this.
8. **Dataset file missing on disk** — silver.py logs warning and continues. Keep this.
9. **Duplicate post_ids across datasets** — INSERT OR REPLACE handles this. But
   the dedup query must process datasets in recency order (Phase 2c).
10. **Re-run after partial silver crash** — Phase 2b fixes this with silver_progress.
11. **enrich_posts called with empty post_ids list** — should return immediately
    with no work. Currently builds `IN ()` which is invalid SQL. Guard against this.
12. **enrich_posts called with max_posts=0** — should return immediately. Guard
    against this (LIMIT 0 works in DuckDB but is wasteful).

## Definition of done

- [ ] All publish/unpublish code and schema removed
- [ ] SQL injection in gold.py fixed (parameterized queries)
- [ ] Silver partial-crash data loss fixed (silver_progress table)
- [ ] Silver dedup respects dataset recency
- [ ] `_link_media()` uses `_db.DATA_DIR` not hardcoded path
- [ ] `enrich_posts()` calls Gemini (real SDK, not stub)
- [ ] Failed analyses recorded with status='failed' + error message
- [ ] Resumability works (analysed posts skipped, failed posts retried)
- [ ] v1 async patterns ported: TokenBucket, upload, generate, process_post
- [ ] Archive-on-overwrite works for re-analysis
- [ ] HTTP retries on apify.py _get/_post/stream_dataset
- [ ] Orphaned SQL files deleted
- [ ] Unused watermarks/run_history tables removed
- [ ] Stale serve.pyc cleaned
- [ ] Import ordering fixed (ruff clean)
- [ ] CI uses uv, not pip
- [ ] CI uses env vars for secrets, not .env file
- [ ] CI runs tests before pipeline
- [ ] Tests cover: gold enrichment (success + failure + resumable), silver
      progress tracking, silver dedup order, SQL injection guard
- [ ] `uv run pytest tests/ -v` passes with zero failures
- [ ] `uv run ruff check src/ tests/` passes with zero warnings
- [ ] README + AGENTS.md updated to reflect changes
- [ ] lessons.md updated with new patterns

## Negative space

What must not change:
- DuckDB as the single state store.
- DI pattern for DB connections.
- INSERT OR REPLACE for upserts.
- Medallion architecture (bronze/silver/gold).
- uv for package management.
- No serving layer.

What is explicitly out of scope:
- v1 data migration.
- Serving/query layer.
- dbt, Airbyte, Prefect, Dagster.
- Connection pooling / async DB.
- Bronze trigger_run/poll_run end-to-end test (requires Apify token).

What decisions are reserved for human review:
- Whether to use `google-genai` (new SDK) vs `google-generativeai` (old SDK) —
  pyproject.toml says `google-genai` but v1 code uses `google-generativeai`.
  Need to confirm which is installed and adapt the port accordingly.

## Open questions

1. **Which Gemini SDK?** `pyproject.toml` lists `google-genai` (the new unified
   SDK). v1 `quick_analyze.py` uses `google.generativeai` (the older SDK). The
   API surfaces differ (`genai.GenerativeModel` vs `genai.Client`). Need to
   resolve: install `google-genai`, check its API, adapt the v1 code. This
   should be resolved during Phase 3 implementation, not blocking the plan.

## Review section

To be filled in after implementation.
