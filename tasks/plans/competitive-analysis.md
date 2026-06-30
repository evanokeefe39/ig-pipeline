# Competitive Analysis — Requirements & Rollout Plan

## Intent

Enable competitive analysis and idea research across Instagram profiles:
single-profile benchmarking, cross-profile topic comparison, and
profile→topic + profile→resource graph edges.  Design repeatable patterns
that transfer to TikTok, YouTube, X, and LinkedIn with platform-native
schemas.

## Architecture

Two independent concerns with different cadences.  The ETL pipeline
(Bronze → Silver → Gold) runs on batch triggers.  The Taxonomy Registry
(MDM) runs asynchronously — a separate process with its own tables,
its own cadence, and optional human review via an interactive skill.

```
  Bronze ──→ Silver ──→ Gold (writes raw freeform taxonomy)
                              │
                              ▼
  ┌──────────────────────────────────────────────┐
  │              Thin Star Schema                 │
  │                                               │
  │  dim_profile (SCD2)  ─┐                       │
  │  dim_topic (SCD2) ────┤── fact_post (view)    │
  │  dim_time            ─┘                       │
  │                                               │
  │  profile_topic_edges (view)  ──→ graph export │
  │  profile_resource_edges (view) ──→ graph exp. │
  └──────────────┬───────────────────────────────┘
                 │
    ┌────────────▼──────────────────┐
    │    Taxonomy Registry (MDM)    │
    │    ─────────────────────      │
    │    pass 1: exact dedup        │
    │      (lower → trim → dedup)   │
    │                               │
    │    pass 2: embedding cluster  │
    │      cos ≥ 0.95  auto-merge   │
    │      cos 0.85–0.95  review    │
    │      cos < 0.85  distinct     │
    │                               │
    │    taxonomy_terms (SCD2)      │
    │    taxonomy_review (view)     │
    │                               │
    │    Managed via skill:         │
    │    /normalize-taxonomy        │
    └───────────────────────────────┘
```

**Key principle:** The hot path (Gold) never normalizes.  Gold writes
whatever the LLM emits.  Normalization is an offline MDM process.  Views
always resolve through `taxonomy_terms WHERE is_current = true`.

---

## Requirements

### R1 — Freeform topic extraction (emergent taxonomy)

`domain`, `subdomain`, `topic`, `subtopic` — all freeform strings.
No controlled vocabulary.  Topic groups emerge from the data.

`content_type` — freeform string (not the current 8-value enum).

`style` — freeform string.  Emotional/aesthetic dimension.  Captures
vibe: "dark mode", "brutalist", "minimalist", "cinematic", "playful",
"corporate", "retro".

`format` — freeform string.  Structural presentation.  "faceless video",
"talking head", "listicle", "screen recording", "carousel", "slide deck",
"before/after", "storytime".

`hashtags` — pulled from the original post's `hashtags` array, not
LLM-generated.  The enrichment prompt ignores hashtags entirely.

### R2 — Credibility signals (replaces `value_score`)

`value_score` (1-5) is too subjective — no shared criteria.  Replace
with signals that have objective definitions:

| Field | Type | Source | Description |
|---|---|---|---|
| `is_actionable` | boolean | Gold | Can you apply this? Contains a tool, step, resource, or method. |
| `admirality` | string (2-char) | Gold | Intelligence-grade rating: source reliability × information credibility. |
| `has_engagement_bait` | boolean | Silver | Regex on caption: "comment X", "DM for", "link in bio". No LLM cost. |

**Admiralty Code** — adapted from the NATO intelligence convention
(A1-F6) for social media content assessment.  LLM extracts both
dimensions from the post content alone — no profile history needed.

**Source reliability** (who is speaking — assessed from the post itself):

| Code | Label | Criteria |
|---|---|---|
| A | Practitioner | Creator shows something they built/ran/designed. First-hand demonstrator. |
| B | Expert | Knowledgeable observer or industry insider. Credible curator with domain knowledge. |
| C | Curator | Aggregator sharing others' work (with or without credit). |
| D | Unknown | No track record or expertise visible in the post. |
| E | Engagement baiter | Withholds content behind an engagement gate with no preview of substance. |
| F | Cannot assess | Post provides no basis to judge source reliability. |

**Information credibility** (what is claimed):

| Code | Label | Criteria |
|---|---|---|
| 1 | Demonstrated | Output shown working. You can see the result. |
| 2 | Probably true | Consistent with known facts. Logical, coherent, specific. |
| 3 | Possibly true | Plausible but unverified. Reasonable but no supporting evidence in the post. |
| 4 | Opinion | Subjective take, aesthetic preference, personal philosophy. |
| 5 | Improbable | Extraordinary claims without evidence. "I made $X with this one trick." |
| 6 | Cannot assess | Claim is too vague or content-free to evaluate. |

Combined rating examples:
- `"A1"` — practitioner demonstrating verified results
- `"B2"` — expert sharing probably-true information
- `"D4"` — unknown source with opinions
- `"E5"` — engagement bait with improbable claims

Trivially filterable in SQL: `WHERE admirality IN ('A1','A2','B1','B2')`
for high-confidence posts.

The old `gated_content` and `gated_trigger` fields are removed — source
reliability `E` (engagement baiter) combined with `has_engagement_bait`
in silver captures this more precisely.

### R3 — Engagement metrics

v1: absolute counts (`likesCount`, `commentsCount`, `videoPlayCount`,
`videoViewCount`) plus derived ratios (play/view, comment/like).

v2: engagement rate (engagement / follower_count) after Phase 5 profile
scraping.

### R4 — Single-profile benchmarking

- Post vs profile average: `post_likes / owner_avg_likes`.
- Profile's best/worst posts by engagement.
- Filterable by domain, topic, content_type, admirality, format.

### R5 — Cross-profile topic analysis

- Per domain/topic: which profiles get highest avg engagement?
- Engagement distribution (quartiles) across profiles for a topic.
- Topic overlap: shared domain/topic between two profiles.

### R6 — Graph edges (export surface)

- `profile_topic_edges` — one row per profile-topic pair.  Exportable to
  Neo4j/NetworkX when graph algorithms are needed.
- `profile_resource_edges` — one row per profile-resource pair.  Enables
  tool co-occurrence queries.

### R7 — Taxonomy Registry (MDM, SCD2)

Pass 1: exact dedup — lowercase, trim, dedup identical strings.

Pass 2: Gemini text-embedding-004 → cosine similarity clustering:
cos ≥ 0.95 auto-merge, cos 0.85-0.95 review queue, cos < 0.85 distinct.

`taxonomy_terms` table: SCD2 with `effective_from`, `effective_to`,
`is_current`, `similarity_score`, `review_status`.

Managed interactively via a skill (`/normalize-taxonomy`) that queries
the registry, shows clusters, and accepts/rejects merges.

Idempotent — re-runnable without touching the ETL pipeline.

### R8 — Time dimension (deferred to v2)

Aggregate stats only for v1.  Time-series in v2.

### R9 — Multi-platform (deferred)

Platform-native schemas.  Cross-platform views when second platform lands.

---

## Data Model — Thin Star Schema

Three dimension tables, one fact view, two edge-table views.  ~200 lines
of SQL.  SCD2 on dimensions that evolve (profile metadata, taxonomy terms).

### Dimension tables

```
dim_profile
  profile_key       INTEGER PK (surrogate)
  owner_id          TEXT (natural key)
  owner_username    TEXT
  follower_count    INTEGER          -- NULL until Phase 5
  profile_category  TEXT             -- NULL until Phase 5
  effective_from    TIMESTAMP
  effective_to      TIMESTAMP
  is_current        BOOLEAN

dim_topic
  topic_key         INTEGER PK (surrogate)
  domain            TEXT
  subdomain         TEXT
  topic             TEXT
  subtopic          TEXT
  content_type      TEXT
  style             TEXT
  format            TEXT
  -- SCD2: effective_from/to/is_current managed by Taxonomy Registry

dim_time
  time_key          INTEGER PK
  date              DATE
  month             INTEGER
  quarter           INTEGER
  year              INTEGER
```

### Fact view

```
fact_post (view over silver_posts + gold_analyses)
  post_id           TEXT
  profile_key       INTEGER FK → dim_profile
  topic_key         INTEGER FK → dim_topic (resolved via taxonomy_terms)
  time_key          INTEGER FK → dim_time
  shortcode         TEXT
  url               TEXT
  likes_count       INTEGER
  comments_count    INTEGER
  video_play_count  INTEGER
  video_view_count  INTEGER
  play_view_ratio   FLOAT
  comment_like_ratio FLOAT
  is_actionable     BOOLEAN
  admirality        TEXT         -- 2-char Admiralty Code
  has_engagement_bait BOOLEAN
  is_educational    BOOLEAN
  educational_json  JSON         -- {summary, workflow[{step,tool,detail}], concepts[{term,explanation}], principles[], techniques[]}
  actionable_json   JSON         -- {summary, resources[{name,url,type,purpose}], tools[], guides[], downloads[{name,url}]}
```

### Edge-table views

```
profile_topic_edges
  owner_id          TEXT
  owner_username    TEXT
  domain            TEXT
  topic             TEXT
  post_count        INTEGER
  avg_likes         FLOAT
  avg_comments      FLOAT
  avg_plays         FLOAT
  last_post_at      TIMESTAMP

profile_resource_edges
  owner_id          TEXT
  owner_username    TEXT
  resource_name     TEXT        -- from actionable_json.resources[].name
  resource_url      TEXT        -- from actionable_json.resources[].url
  resource_type     TEXT        -- from actionable_json.resources[].type
  post_count        INTEGER
```

### Taxonomy registry tables

```
taxonomy_terms (SCD2)
  term_id           INTEGER PK
  term_type         TEXT      -- domain, subdomain, topic, subtopic, content_type, style, format
  raw_value         TEXT      -- as extracted by LLM
  canonical_value   TEXT      -- resolved canonical term
  similarity_score  FLOAT     -- cosine similarity to canonical (NULL for self-mapped)
  review_status     TEXT      -- auto_merged, pending_review, reviewed_approved, reviewed_rejected
  effective_from    TIMESTAMP
  effective_to      TIMESTAMP
  is_current        BOOLEAN
  merged_into_id    INTEGER   -- FK → taxonomy_terms.term_id (for deprecated terms)

taxonomy_review (view)
  -- Pending merge proposals for human review (cos 0.85–0.95)
  -- Shows cluster: canonical_value + all raw_values proposed for merge + similarity scores
```

---

## Schema Change: `gold_analyses.result_json` v2 → v3

| Field | v2 | v3 |
|---|---|---|
| `content_type` | enum (8 values) | freeform string |
| `domains` | array from 12-value vocab | replaced by `domain` (singular) |
| `domain` | — | freeform string |
| `subdomain` | — | freeform string |
| `topic` | — | freeform string |
| `subtopic` | — | freeform string |
| `style` | — | freeform string |
| `format` | — | freeform string |
| `tags` | LLM-generated | removed (use bronze hashtags) |
| `value_score` | integer 1–5 | replaced by `admirality` + `is_actionable` |
| `admirality` | — | 2-char code: source reliability (A-F) + info credibility (1-6) |
| `is_actionable` | — | boolean |
| `gated_content` | boolean | removed (captured by admirality E + has_engagement_bait) |
| `gated_trigger` | string | removed |
| `is_educational` | boolean | kept |
| `resources` | array of objects | replaced by `educational_json` + `actionable_json` |
| `workflow_steps` | array of strings | moved into `educational_json.workflow` |
| `tips` | array of strings | moved into `educational_json` or `actionable_json` as appropriate |
| `concepts` | array of objects | moved into `educational_json.concepts` |
| `tools_apps` | array of strings | moved into `actionable_json.tools` |
| `people` | array of strings | removed (low signal; can be extracted from transcript if needed) |
| `educational_json` | — | {summary, workflow[{step,tool?,detail?}], concepts[{term,explanation}], principles[], techniques[]} |
| `actionable_json` | — | {summary, resources[{name,url,type,purpose}], tools[], guides[], downloads[{name,url}]} |
| `transcript` | string | kept |

## New Columns: `silver_posts`

| Column | Source | Type |
|---|---|---|
| `owner_id` | post.json.ownerId | TEXT |
| `owner_username` | post.json.ownerUsername | TEXT |
| `likes_count` | post.json.likesCount | INTEGER |
| `comments_count` | post.json.commentsCount | INTEGER |
| `video_play_count` | post.json.videoPlayCount | INTEGER |
| `video_view_count` | post.json.videoViewCount | INTEGER |
| `timestamp` | post.json.timestamp | TIMESTAMP |
| `hashtags` | post.json.hashtags | TEXT[] |
| `has_engagement_bait` | regex on caption | BOOLEAN |

---

## Gap Analysis

| # | Requirement | Current State | Gap | Effort |
|---|---|---|---|---|
| R1 | Freeform taxonomy + style/format | Controlled vocab, no style/format | Rewrite gold prompt + schema v3 | Medium |
| R2 | Credibility signals | value_score only | Add admirality + is_actionable to prompt + schema | Medium |
| R3 | Engagement in DB | Buried in post.json | Add columns to silver_posts; re-silver; has_engagement_bait regex | Small |
| R4 | Profile benchmarking | No owner aggregation | dim_profile + profile_stats view | Small |
| R5 | Cross-profile topics | No topic aggregation | dim_topic + topic_stats view | Medium |
| R6 | Graph edges | None | Two edge-table views | Small |
| R7 | Taxonomy Registry | No MDM at all | taxonomy_terms SCD2 + normalize_taxonomy + skill | Large |
| R8 | Time dimension | timestamp not surfaced | dim_time table | Small |
| R9 | Multi-platform | IG only | Deferred | — |

---

## Rollout Phases

### Phase 1 — Schema Evolution (now)

1. Add engagement/profile/hashtag columns to `silver_posts` (ALTER TABLE)
2. Add `has_engagement_bait` column — populated via regex on caption during re-silver
3. Re-silver all 826 posts to populate new columns
4. Create `dim_time` table — static, populate once
5. Create `dim_profile` table — seed with unique owners from silver_posts
6. Create `taxonomy_terms` table (SCD2 empty stub — populated in Phase 3)
7. Rewrite gold prompt: freeform taxonomy + style/format + admirality + is_actionable;
   remove tag extraction, value_score, gated_content, gated_trigger
8. Bump `SCHEMA_VERSION` to 3
9. Run gold on 5 posts → validate new schema
10. Run gold on all 826 posts → full v3 enrichment
11. Create `dim_topic` table — seed from raw gold_analyses (self-mapped, not yet normalized)

**Deliverable:** v3 enrichment on all posts.  Engagement queryable.  All
dimension tables seeded.

### Phase 2 — Analytics Views (now)

1. Create `fact_post` view
2. Create `profile_stats` view
3. Create `topic_stats` view
4. Create `profile_topic_edges` view
5. Create `profile_resource_edges` view
6. Update `refresh_views()` to create all views
7. Refresh views on full dataset
8. Run benchmarking queries against real data → validate

**Deliverable:** All competitive analysis queries work.  Graph edge
export surface ready.

### Phase 3 — Taxonomy Registry (MDM)

1. Implement `normalize_taxonomy.py`:
   - Extract all unique raw values from `gold_analyses.result_json`
   - Insert into `taxonomy_terms` as self-mapped rows
   - Pass 1: exact dedup -> merge identical-after-normalize terms
   - Cluster with cosine similarity
   - cos ≥ 0.95 → auto-merge, SCD2 effective dates
   - cos 0.85–0.95 → review_status = 'pending_review'
2. Create `taxonomy_review` view for interactive workflow
3. Create `/normalize-taxonomy` skill:
   - Step 1: show pending review clusters with similarity scores
   - Step 2: accept/reject/split merge proposals interactively
   - Step 3: apply decisions → update taxonomy_terms SCD2 records
   - Step 4: refresh dim_topic from canonical taxonomy_terms
4. Wire `dim_topic` to resolve through `taxonomy_terms WHERE is_current = true`
5. Run normalization + review on v3 dataset
6. Refresh all views with canonical terms

**Deliverable:** MDM process live.  Taxonomy deduplicated.  Review
queue operational.  All views resolve canonical terms.

### Phase 4 — Exploration & Iteration (follow-up)

- Run benchmarking queries against real data
- Identify topic clusters that need refinement
- Feed findings back into gold prompt tuning
- Tune cosine thresholds based on real false-positive/negative rates

### Phase 5 — Profile Scraping (deferred)

- The Apify Instagram scraper includes `metaData` (followersCount, postsCount,
  biography, verified, businessCategoryName, relatedProfiles) when given
  **profile URLs** (e.g. `instagram.com/bywaviboy`), but NOT when given
  **individual post URLs**.
- Strategy: feed all 369 unique owner profile URLs to the Apify actor.
  Extract `metaData` from the resulting posts into `silver_posts.meta_data`.
  `populate_dim_profile` already reads the latest metaData per owner.
- We already have coverage for 3 profiles (bywaviboy, vinny_creative,
  electroformaint) from the existing profile-scraped datasets.
- `relatedProfiles` from metaData is Instagram's built-in profile similarity
  graph — could seed `profile_profile_edges` view in Phase 6.

### Phase 6 — Graph DB (deferred)

- Export `profile_topic_edges` + `profile_resource_edges` to Neo4j/NetworkX
- Community detection, PageRank, profile similarity clustering
- Only when DuckDB window functions can't handle the query

### Phase 7 — Second Platform (deferred)

- Add TikTok-specific bronze/silver tables (platform-native schema)
- Feed taxonomy registry from TikTok gold
- Evaluate cross-platform `dim_profile` merging (same creator on both)

---

## Open Questions

- [x] How many unique owners? → 369 (top 2: bywaviboy 155, vinny_creative 147)
- [x] value_score → Admiralty Code (A1-F6) + is_actionable + has_engagement_bait
- [x] style / format → both freeform, added to gold schema
- [x] gated_content → removed; captured by admirality E + has_engagement_bait
- [x] Taxonomy management → /normalize-taxonomy skill with interactive review
- [x] is_educational kept — actionable ≠ educational (investment tips are actionable not educational; color theory is educational not actionable). Independent signals, both stay.
