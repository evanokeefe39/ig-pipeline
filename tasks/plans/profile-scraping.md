# Profile Scraping Plan — Batch Apify Runs

## Goal

Scrape recent posts from all 366 profiles missing `metaData` (follower counts,
bio, related profiles) to backfill `dim_profile`.  Use the same Apify actor
(`apify/instagram-scraper`) pointed at profile URLs instead of post URLs.

## Actor config

```
actor: apify/instagram-scraper
resultsType: "posts"
```

The `resultsLimit` is global per run — all profiles in a run share the same
limit.  Re-scraping a profile with a higher limit repeats the first N posts,
so each profile must be scraped once at its final depth.

## Budget

Available: **$36.50 USD**.  Cost at BRONZE tier: ~$0.0023/result.

## Batched runs (6 total, ~$13.69)

| Phase | Profiles | Posts each | Runs | Results | Cost | Cumulative |
|---|---|---|---|---|---|---|
| 1 — Whales | 13 (4+ saved) | 80 | 2 (7+6) | 1,040 | $2.39 | $2.39 |
| 2a — Top | 17 (3 saved) | 60 | 2 (9+8) | 1,020 | $2.35 | $4.74 |
| 2b — Mid | 53 (2 saved) | 20 | 1 | 1,060 | $2.44 | $7.18 |
| 3 — Tail | 283 (1 saved) | 10 | 1 | 2,830 | $6.51 | $13.69 |

Headroom: $22.81 for re-runs, deeper pulls, or Phase 3 expansion to 15 posts.

## Phase 1 — Whales (13 profiles × 80 posts)

### Batch 1a (7 profiles, 560 results, $1.29)

```
starter_story
sabrina_ramonov
chase.h.ai
angus.sewell
aitickerdaily
marc.kaz
mrnotion.co
```

### Batch 1b (6 profiles, 480 results, $1.10)

```
howtogofreelance
londonthefriend
ericosiu
evolving.ai
daniel.margu
kerem.tech
```

## Phase 2a — Top (17 profiles × 60 posts)

### Batch 2a-1 (9 profiles, 540 results, $1.24)

```
steven.builds
zigzagprompts
hueminart
jason.yslai
logodesigntools
kiraknowsai
letbuild.xyz
ceozac
gustsngfx
```

### Batch 2a-2 (8 profiles, 480 results, $1.10)

```
lukas.mullen
blakeandersonw
marvintaylor.design
digitalwebjunkie
notamusement
alfred.ddd
yoan_b
stephgorton__
```

## Phase 2b — Mid (53 profiles × 20 posts, 1 run)

Single run: 53 profiles, 1,060 results, $2.44.

```
3drenderlifestyle
aaronmwanderson
advanceddesign
aiconsumer
aiforlifereview
amittrainer_
arakotvfx
bevels.xyz
bigfaces.design
bm_aiart
bonesdesign.co
cache_bunny
carterpresents
codegrids
creation.visuals
digitalprocess
edsby.og
epicmotion
factory__joe
geometricgoods
ghost.io
graemenpeter
holke79
hunterhammonds
iamhaydenjames
imdannyclark
imjakefurie
imrinaas
intravoidai
its.joelwatson
jaxxonmedia
jays_far_out
johnnyholland
jonathan_mwand
kevin_twohy
khatcher.io
konstantinosdoubt
kristian.creates
lena.ai
lukas.mullen
madebyjamesuk
maj0rvisuals
marc.kaz
marvintaylor.design
mattthomas.png
maxschon
mero.creates
mk.ngu
mr.lukasjames
mrdavedigital
notalllne
philipwallage
```

## Phase 3 — Tail (283 profiles × 10 posts, 1 run)

Single run: 283 profiles, 2,830 results, $6.51.

Full list available via:
```python
from ig_pipeline.db import get_db
db = get_db()
rows = db.execute("""
    SELECT sp.owner_username, COUNT(*) as n
    FROM silver_posts sp
    LEFT JOIN dim_profile dp ON sp.owner_id = dp.owner_id AND dp.is_current
    WHERE dp.follower_count IS NULL
    GROUP BY sp.owner_username
    HAVING COUNT(*) = 1
    ORDER BY sp.owner_username
""").fetchall()
profiles = [r[0] for r in rows]
```

## Execution

### Prerequisites
```bash
cd C:/Users/evano/repos/ig-pipeline
```

### Per-run workflow
```python
from ig_pipeline.apify import trigger_run, poll_run
from ig_pipeline.bronze import ingest_dataset
import os
from dotenv import load_dotenv
load_dotenv()
token = os.environ["APIFY_API_TOKEN"]

actor = "apify/instagram-scraper"
urls = [
    "https://www.instagram.com/starter_story/",
    "https://www.instagram.com/sabrina_ramonov/",
    # ...
]

run = trigger_run(
    actor,
    urls,
    token=token,
    results_limit=80,
    results_type="posts",
)
print(f"Run: {run.run_id}, Dataset: {run.dataset_id}, Cost: ~${run.estimated_cost_usd:.2f}")

dataset_id = poll_run(run.run_id, token=token)
result = ingest_dataset(dataset_id, token=token, run_id=run.run_id, actor=actor)
print(f"Ingested: {result.item_count} items")
```

### After all runs complete
```python
from ig_pipeline.silver import deduplicate_all
from ig_pipeline.gold import populate_dim_profile, refresh_views

deduplicate_all()
populate_dim_profile()
refresh_views()
```

Then check:
```sql
SELECT count(*) FROM dim_profile WHERE follower_count IS NOT NULL;
-- Should increase from 3 to ~366
```

## After ingestion — get metaData

Once posts are ingested and silvered, the `meta_data` column in `silver_posts`
will contain profile metadata.  `populate_dim_profile` extracts the latest
per owner already.

## Automation module

`src/ig_pipeline/profile_scrape.py` wraps the workflow above:

- `query_profiles_needing_meta(db)` — read-only discovery: which profiles
  lack follower_count in dim_profile
- `run_batch(actor, usernames, results_limit, token)` — trigger, poll,
  ingest a single batch of profile URLs
- `run_all_batches(batches, token)` — run multiple (username_list,
  results_limit) tuples in sequence
- `backfill_after_batches(db)` — silver dedup + populate_dim_profile +
  refresh_views; returns counts for each step

Tests: `tests/test_profile_scrape.py` (8 tests, all passing).
