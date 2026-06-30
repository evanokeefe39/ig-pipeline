# Issues & deferred work

## ETL orchestration framework

**Status:** Deferred to feature branch `feat/dagster-orchestration`.

### Research (2026-06-30)

Evaluated lightweight ETL orchestrators for local, single-developer use with DAG
visualization, error logging, and observability. Criteria: self-hostable, free,
Python-friendly, minimal infrastructure.

| Tool | HQ | Local DB | Deploy | Notes |
|---|---|---|---|---|
| **Dagster** | US (SF) | SQLite | `pip install dagster` + `dg dev` | Asset-centric, strong lineage. Steep learning curve but richer observability. |
| **Prefect** | US (DC) | SQLite | `pip install prefect` + `prefect server start` | Task-centric. Lightest to adopt — decorators on existing functions. |
| **Kestra** | France | H2 (embedded) | Single JAR or Docker | YAML-defined workflows. EU-based (GDPR-native). JVM dependency. |
| **Mage** | US | — | `pip install mage-ai` | Notebook-style blocks. Less mature for production. |

**Decision:** Dagster selected for exploration. Its asset-centric model maps
naturally to the medallion architecture (bronze dataset → silver posts → gold
analyses → views), and it provides column-level lineage and per-partition
materialization tracking that Prefect's task-centric model doesn't.

Deferred to a separate branch to avoid scope creep on the current hardening work.
The current scripts (`post_scrape.py`, `profile_scrape.py`, `run_pipeline.py`)
remain the operational pipeline until Dagster is integrated.

### EU sovereignty notes

Self-hosting pinned OSS versions of US tools (Prefect, Dagster) has effectively
zero Cloud Act exposure — the law compels companies to hand over data they hold,
not to push backdoors into code already on your server. Telemetry is the real
practical concern: Prefect 3.x OSS server is local-only (no phone-home), same
for Dagster OSS. Disable analytics env vars as a precaution:

- `PREFECT_SERVER_ANALYTICS_ENABLED=false`
- `DAGSTER_TELEMETRY_ENABLED=false`

The EU-vendor preference (Kestra) is procurement theater for self-hosted OSS,
but legitimate for regulated institutions concerned about supply-chain
jurisdiction (US sanctions could affect PyPI/GitHub access).

## Ad-hoc pipeline UX

The PIPELINE FIRST rule remains in force. Assessment (2026-06-30):

- **Post scraping (`post_scrape.py`):** Already handles ad-hoc lists via
  `--urls`, `--urls-file`, or `--profile`. Chains trigger→poll→bronze→silver→gold.
  No gap.
- **Profile metadata (`profile_scrape.py`):** Tier-based only in CLI. Module
  functions (`run_batch`) support arbitrary username lists but CLI doesn't
  expose `--profiles`. Low-priority gap.
- **`resultsType: "details"` scraping:** Module has `populate_dim_profile_from_details()`
  but no CLI script chains trigger→poll→ingest→populate for this flow. Deferred.

## Surgical changes (2026-06-30)

Decoupled bronze downloads from DuckDB to enable parallel scraping and clean
Dagster migration path:

### Changes made

- **`bronze.py`** — `download_dataset()` writes `.jsonl` data + `.jsonl.meta`
  sidecar (run_id, actor, item_count, downloaded_at). No DuckDB writes.
  `ingest_dataset()` kept as deprecated alias for backward compat.
- **`db.py`** — `bronze_ingests` table removed from schema. Bronze state is
  purely file-based — a `.jsonl` exists = dataset downloaded.
- **`silver.py`** — `deduplicate_all()` scans `data/bronze/*.jsonl` via
  glob + `silver_progress` instead of querying `bronze_ingests`.
- **`post_scrape.py`** — added `--scrape-only` flag. Downloads files only,
  zero DuckDB interaction. Multiple `--scrape-only` commands can run in
  parallel without lock contention.
- **`run_pipeline.py`** — stripped down to pure medallion processor:
  dedup → enrich → dim_profile → views. No Apify API calls (was mixing
  runs from other projects). No `bronze_ingests` queries.
- **`profile_scrape.py`** — `run_batch()` calls `download_dataset()` directly
  (was passing `db=` param). `populate_dim_profile_from_details()` finds
  bronze files via filesystem path instead of bronze_ingests query.

### Rationale

DuckDB is single-writer by design. The old pattern coupled file downloads to
DuckDB writes, preventing parallel execution. Separating concerns:
1. **Download** (Apify → disk) — runs in parallel, no shared state
2. **Medallion processing** (bronze → silver → gold) — sequential, single
   DuckDB writer, reads from filesystem

This exactly matches Dagster's asset model, making migration straightforward.

## Dagster migration plan

### Asset mapping

| Current layer | Dagster asset | Produces | Dependencies |
|---|---|---|---|
| `download_dataset()` | `bronze_dataset` | `data/bronze/{ds_id}.jsonl` (file) | Apify API external resource |
| `deduplicate_all()` | `silver_posts` | `silver_posts` table (DuckDB) | bronze files on disk |
| `enrich_posts()` | `gold_analyses` | `gold_analyses` table (DuckDB) | silver_posts, Gemini API |
| `populate_dim_profile()` | `dim_profile` | `dim_profile` table (DuckDB) | silver_posts |
| `refresh_views()` | `analytics_views` | DuckDB views (DuckDB) | all DuckDB tables |

### Key infrastructure

- **DuckDB IO manager** — single Dagster-run process serializes all DuckDB
  writes. Multiple assets can declare `DuckDB` as their I/O manager; Dagster
  ensures only one writes at a time.
- **Sidecar `.meta.json` files** — the `bronze_dataset` asset writes these
  alongside `.jsonl` data, storing run_id, actor, item_count, downloaded_at.
  Dagster's own event log tracks materialization separately.
- **`bronze_ingests` table is gone** — no migration needed from data side.
  All existing bronze files are discoverable by scanning the directory.

### Config

```python
class ScrapeConfig(Config):
    urls: list[str]
    profile_usernames: list[str] | None = None
    results_limit: int = 12
    results_type: str = "posts"
```

Config drives the `bronze_dataset` asset. Other assets (silver, gold) have
no config — they process whatever data exists.

### Execution

A single Dagster job schedules the chain:

```
bronze_dataset -> silver_posts -> gold_analyses
                                  -> dim_profile
                                  -> analytics_views
```

All three DuckDB assets share one IO manager, serializing writes.
`bronze_dataset` runs independently (file write, no DuckDB).

### Migration path

1. Create feature branch: `feat/dagster-orchestration`
2. `uv add dagster dagster-webserver`
3. Define `assets.py` with the 5 assets above
4. Define DuckDB IO manager in `resources.py`
5. Test with existing bronze files on disk
6. Replace `post_scrape.py` with Dagster config-driven job
7. Replace `run_pipeline.py` with scheduled Dagster job
8. Remove `scripts/` code that now lives in Dagster

### Parallel batch optimization (future)

The Instagram scraper actor serializes profile scraping within a single run.
A run with 290 profiles × 10 posts takes much longer than 10 runs of 29
profiles × 10 posts — each actor run has its own Instagram throttling window.

Apify starter plan likely supports ~6-10 concurrent actor runs. With 64 GB
RAM, memory is not a bottleneck (each run uses ~512 MB-1 GB).

Optimization for Dagster migration: split tier batches into smaller groups
(~30 profiles each) and farm them to parallel Apify runs. Each profile
appears in exactly one batch — re-scraping the same profile returns the
same posts (idempotent, wasteful).

Config-driven batch splitting would live in the `bronze_dataset` asset,
parameterized by profile list + max concurrent runs.

This is a runtime optimization only — no impact on correctness or cost.
