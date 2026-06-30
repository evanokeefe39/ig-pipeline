---
name: scrape-posts
description: >
  Scrape Instagram posts into the pipeline by URL or profile username.
  Triggers an Apify run, polls for completion, ingests to bronze, deduplicates
  to silver, and optionally enriches with Gemini. Use when the user asks to
  scrape posts, fetch Instagram content, add new posts to the pipeline, or
  ingest specific posts or profiles. Trigger on phrases like "scrape posts",
  "fetch Instagram", "add posts to pipeline", "ingest posts", "scrape this
  profile".
---

# Scrape Posts

Scrapes Instagram posts and ingests them into the medallion pipeline:
Bronze (raw ingest) → Silver (deduplicate) → Gold (Gemini enrich, optional).

## Script

```bash
uv run python scripts/post_scrape.py {--urls URLS | --profile USERNAMES | --urls-file FILE} [--limit N] [--enrich] [--dry-run]
```

### Examples

Scrape specific post URLs:
```bash
uv run python scripts/post_scrape.py --urls https://www.instagram.com/p/abc123/ https://www.instagram.com/p/def456/
```

Scrape a profile's recent posts:
```bash
uv run python scripts/post_scrape.py --profile starter_story --limit 20
```

Scrape multiple profiles:
```bash
uv run python scripts/post_scrape.py --profile starter_story sabrina_ramonov --limit 12
```

Scrape from a file (one URL per line):
```bash
uv run python scripts/post_scrape.py --urls-file urls.txt --limit 50
```

Scrape and enrich with Gemini:
```bash
uv run python scripts/post_scrape.py --profile starter_story --limit 12 --enrich
```

### Options

| Flag | Description |
|------|-------------|
| `--urls` | One or more Instagram post or profile URLs |
| `--profile` | Instagram usernames (builds profile URLs automatically) |
| `--urls-file` | File with one URL per line |
| `--limit N` | Results limit per URL (default: 12) |
| `--enrich` | Run Gemini enrichment after silver |
| `--dry-run` | Preview without triggering |

### Workflow

1. Triggers an Apify run with the given URLs
2. Polls until the run completes (up to 20 minutes)
3. Ingests the dataset into bronze (immutable, watermarked NDJSON)
4. Deduplicates into silver (INSERT OR REPLACE by post_id)
5. If `--enrich`: runs Gemini enrichment on new posts
6. Refreshes `dim_profile` and all analytical views

## Requirements

- `APIFY_API_TOKEN` in `.env`
- `GEMINI_API_KEY` in `.env` (only if using `--enrich`)
- Working directory: project root

## Cost

Post scraping at the BRONZE tier: ~$0.0023 per result. A single profile
at the default limit of 12 posts costs ~$0.03.

## Module

For programmatic use:

```python
from ig_pipeline.apify import trigger_run, poll_run
from ig_pipeline.bronze import ingest_dataset
from ig_pipeline.silver import deduplicate_all
from ig_pipeline.gold import enrich_posts, refresh_views
```
