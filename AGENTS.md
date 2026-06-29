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
- **Query** — DuckDB reads gold JSON directly: `SELECT * FROM read_json('data/gold/posts/*/enriched.json')`.

## State tracking

DuckDB at `data/pipeline.db`. Tables: `bronze_ingests`, `silver_posts`,
`silver_progress`, `gold_analyses`. All operations are idempotent.

## Watermarking

- `bronze_ingests` records `run_id` and `actor` for every ingested Apify dataset.
- `scripts/run_pipeline.py` queries `list_runs()` and skips already-ingested datasets.
- `silver_posts.source_dataset` tracks which bronze dataset produced each silver record.

## Test conventions

```python
def test_something():
    db = get_db(":memory:")
    db.execute("INSERT INTO ...")       # seed
    result = function_under_test(db=db)  # inject
    assert result.expected == actual     # verify
```

No `monkeypatch` for DB access. Monkeypatch only for external APIs and filesystem paths.

## Patterns learned (v2 build + hardening)

- **DI for DB**: All pipeline functions accept `db=None`. Tests inject `:memory:`.
- **DuckDB timestamps**: `CURRENT_TIMESTAMP`, not `datetime('now')`.
- **Module imports**: `from .db import BRONZE_DIR` captures value at import time. Use `_db.BRONZE_DIR`.
- **DuckDB upsert**: `INSERT OR REPLACE` by primary key for dedup. No SELECT-check pattern needed.
- **Mixed carousels**: Return all slides, interleave uploaded videos + inline images.
- **Parameterized SQL**: Never f-string SQL. Use `?` placeholders with params tuples.
- **Crash-safe batch tracking**: Track multi-item batches at the batch level (`silver_progress`). Never infer completion from individual item state.
- **DuckDB row counts**: `db.execute()` is always truthy — use `RETURNING` clause, never check execute result.
- **HTTP retries**: Every external HTTP call gets retries. Use `tenacity` with a specific retry condition function (`_is_retryable`).
- **Dead code removal**: Schema tables, SQL files, and pycache artifacts that aren't wired in are speculative overproduction. Ship only what's exercised.
