# ig-pipeline

[![Tests](https://github.com/evanods/ig-pipeline/actions/workflows/pipeline.yml/badge.svg)](https://github.com/evanods/ig-pipeline/actions/workflows/pipeline.yml)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

Medallion-architecture ETL pipeline for scraping Instagram saved posts via Apify,
deduplicating into a local data lake, and enriching with Gemini.

## Architecture

```
Instagram URLs    -->  Apify actor  -->  bronze/*.jsonl    (immutable raw dumps)
                                                  |
                                    DuckDB upsert │  INSERT OR REPLACE (post_id)
                                                  v
                                          silver/posts/{id}/  (canonical + media)
                                                  |
                                    Gemini Flash  │  async, rate-limited
                                                  v
                                           gold/posts/{id}/   (enriched + views)
```

**Layers:**
- **Bronze** — raw Apify datasets streamed via cursor pagination. Watermarked by run_id.
- **Silver** — upserted by Instagram `post_id`. DuckDB `INSERT OR REPLACE`. Latest dataset wins.
- **Gold** — Gemini enrichment results, idempotent and resumable.

## Quick start

```bash
uv venv
uv pip install -e ".[dev]"
cp .env.example .env   # add APIFY_API_TOKEN + GEMINI_API_KEY
python scripts/run_pipeline.py
```

## Usage

### Batch pipeline (cron / GitHub Actions)
```bash
python scripts/run_pipeline.py
```
Runs all layers idempotently. Watermarking ensures only new Apify runs are ingested.

### Ad-hoc (Claude / interactive)
```python
from ig_pipeline.apify import trigger_run, poll_run
from ig_pipeline.bronze import ingest_dataset
from ig_pipeline.silver import deduplicate_all
from ig_pipeline.gold import enrich_posts

run = trigger_run("apify/instagram-scraper", ["https://www.instagram.com/p/..."])
dataset_id = poll_run(run.run_id)
ingest_dataset(dataset_id, run_id=run.run_id, actor=run.actor)
deduplicate_all()
enrich_posts()
```

Full function reference in `skills/ig-pipeline/SKILL.md`.

## Data layout

```
data/
├── bronze/datasets/{dataset_id}.jsonl      # raw Apify dumps (immutable)
├── silver/posts/{post_id}/
│   ├── post.json                           # canonical metadata
│   └── media/                              # hardlinked media files
├── gold/posts/{post_id}/
│   └── enriched.json                       # Gemini analysis result
└── pipeline.db                             # DuckDB (state + views)
```

## State tracking

DuckDB at `data/pipeline.db`. Schema:

- `bronze_ingests` — which datasets have been downloaded, from which run
- `silver_posts` — canonical post records with media manifests (upserted by post_id)
- `silver_progress` — tracks which bronze datasets have been fully processed (crash-safe)
- `gold_analyses` — enrichment status, results, and error tracking

## Design decisions

- **DuckDB over SQLite**: Reads JSON files directly (`read_json()`), no import step. Same file for pipeline state and analytical queries.
- **INSERT OR REPLACE for dedup**: DuckDB's native upsert by primary key. No SELECT-check-then-INSERT pattern. Latest dataset wins automatically.
- **No dbt, no Airbyte, no Prefect**: 5 functions, 828 posts. A cron job + DuckDB + filesystem is the whole infrastructure.
- **Serving is separate**: This repo is ETL only. Query the DuckDB file directly, or use `read_json('data/gold/posts/*/enriched.json')`.

## Development

```bash
uv venv
uv pip install -e ".[dev]"
pytest tests/ -v
```

Tests use in-memory DuckDB (`:memory:`) via dependency injection. All pipeline functions accept `db=None`. No monkeypatching for database access.

## License

MIT — see [LICENSE](LICENSE).
