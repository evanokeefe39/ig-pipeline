---
name: scrape-profile
description: >
  Batch-scrape Instagram profile metadata (follower counts, bio, category)
  via the Apify Instagram scraper. Backfills dim_profile with metaData that
  is only available when scraping profile URLs, not individual post URLs.
  Use when the user asks to scrape profiles, backfill follower counts, run
  profile batches, or execute the profile-scraping plan. Trigger on phrases
  like "scrape profiles", "backfill dim_profile", "get follower counts",
  "run profile batches", "profile metadata".
---

# Scrape Profiles

Backfills `dim_profile` metadata (follower counts, bio, verified status,
business category, external URL, related profiles) by running the Apify
Instagram scraper against profile URLs.

Profile URLs trigger the actor to include a `metaData` field in each result.
Post URLs do NOT include this field — this is why profile scraping exists as
a separate workflow.

## Script

```bash
uv run python scripts/profile_scrape.py {whales|top|mid|tail|all|check} [--dry-run]
```

### Commands

| Command | Profiles | Posts each | Batches | Est cost |
|---------|----------|------------|---------|----------|
| `whales` | ~13 | 80 | 2 | ~$2.39 |
| `top` | ~16 | 60 | 2 | ~$2.21 |
| `mid` | ~49 | 20 | 1 | ~$2.25 |
| `tail` | ~287 | 10 | 1 | ~$6.60 |
| `all` | ~365 | varies | 6 | ~$13.44 |
| `check` | — | — | — | $0 (read-only) |

### Workflow

Each command:
1. Queries the DB for profiles in that tier that need backfill
2. Chunks them into batches sized for Apify's free tier
3. Triggers an Apify run per batch
4. Polls until each run completes
5. Ingests the dataset into bronze
6. After all batches: runs silver dedup, populates dim_profile, refreshes views

### Dry run

```bash
uv run python scripts/profile_scrape.py whales --dry-run
```

Shows the batch plan and cost estimate without triggering any runs.

### Check progress

```bash
uv run python scripts/profile_scrape.py check
```

Shows total profiles, how many have follower counts, and how many remain.

### Module

For programmatic use, import the module instead:

```python
from ig_pipeline.profile_scrape import run_batch, backfill_after_batches
```

## Requirements

- `APIFY_API_TOKEN` in `.env`
- Working directory: project root

## Cost

Profile scraping at the BRONZE tier: ~$0.0023 per result. The 6-batch
plan totals ~$13.44 against a $36.50 budget.
