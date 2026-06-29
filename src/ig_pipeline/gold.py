"""Gold layer — Gemini enrichment and DuckDB views."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import pathlib
import random
import time
from collections.abc import Sequence
from typing import TYPE_CHECKING

import duckdb

from . import db as _db
from .models import GoldResult

if TYPE_CHECKING:
    import google.genai as genai

log = logging.getLogger(__name__)

# ── Config ──────────────────────────────────────────────────────────────────

GEMINI_MODEL = "gemini-3.1-flash-lite"
SCHEMA_VERSION = 3

# Max carousel slides sent in a single multi-image request (cost / payload guard).
MAX_SLIDES = 12

# Token bucket config
RPM_LIMIT: int = 14  # stay 1 below Google's hard ceiling
RPM_JITTER: float = 0.10  # +/- 10% jitter on refill intervals

# Concurrency limits
UPLOAD_CONCURRENCY: int = int(os.environ.get("UPLOAD_CONCURRENCY", "4"))
GENERATE_CONCURRENCY: int = int(os.environ.get("GENERATE_CONCURRENCY", "15"))

SUPPORTED_EXTENSIONS = {
    ".mp4", ".mov", ".mpeg", ".webm", ".jpg", ".jpeg", ".png", ".gif", ".webp",
}

# ── Extraction prompt ───────────────────────────────────────────────────────

PROMPT = """
You are a knowledge extractor analyzing social media posts. You are given a
post's media: a video, OR all slides of a carousel in order.  Extract
structured knowledge from the post.

Return ONLY a valid JSON object with exactly these fields. No markdown
fences, no explanation.

{
  "is_educational":          boolean,
  "is_actionable":           boolean,
  "admirality":              string  (2 characters),
  "domain":                  string,
  "subdomain":               string,
  "topic":                   string,
  "subtopic":                string,
  "content_type":            string,
  "style":                   string,
  "format":                  string,
  "educational_json":        { ... },
  "actionable_json":         { ... },
  "transcript":              string
}

Field notes:

is_educational — true if the post teaches something reusable: a method,
principle, concept, or technique.  Pure showcase or hype = false.

is_actionable — true if the post gives you something you can go do:
install a tool, follow a guide, download an asset, apply a step.

admirality — 2-character Admiralty Code.
  First char = source reliability (who is speaking):
    A = Practitioner — shows something they built/ran/designed
    B = Expert — knowledgeable observer, industry insider
    C = Curator — aggregator sharing others' work
    D = Unknown — no track record visible in the post
    E = Engagement baiter — content withheld behind a gate
    F = Cannot assess
  Second char = information credibility (what is claimed):
    1 = Demonstrated — output shown working, you can see the result
    2 = Probably true — consistent with known facts, logical
    3 = Possibly true — plausible but unverified
    4 = Opinion — subjective take, aesthetic preference
    5 = Improbable — big claims without proof
    6 = Cannot assess
  Examples: "A1" = practitioner demonstrating verified results.
            "E5" = engagement baiter with improbable claims.

domain, subdomain, topic, subtopic — freeform strings.  No controlled
vocabulary.  Be consistent across posts — reuse the same terms for
similar content.

content_type — freeform.  Describe the format naturally: "tutorial",
"case study", "listicle", "product review", "behind the scenes".
Not controlled.

style — freeform.  Emotional/aesthetic vibe: "minimalist", "dark mode",
"cinematic", "brutalist", "playful", "retro".

format — freeform.  Structural presentation: "faceless video",
"talking head", "carousel", "screen recording", "before/after".

educational_json — what the post teaches.  Include only if
is_educational is true.  Otherwise set to empty object {}.

{
  "summary":    string,
  "workflow":   [{"step": string, "tool": string, "detail": string}],
  "concepts":   [{"term": string, "explanation": string}],
  "principles": [string],
  "techniques": [string]
}

- summary: 1-2 sentences on what it teaches.  No hype.
- workflow: ordered steps, if the post describes a process.
  Each step can name a tool and give a detail.
- concepts: key terms/principles explained.  One per entry.
- principles: design/creative/technical principles taught.
- techniques: specific techniques demonstrated.

actionable_json — things the viewer can go do, get, or apply.
Include only if is_actionable is true.  Otherwise set to empty
object {}.

{
  "summary":    string,
  "resources":  [{"name": string, "url": string, "type": string,
                   "purpose": string}],
  "tools":      [string],
  "guides":     [string],
  "downloads":  [{"name": string, "url": string}]
}

- summary: 1 sentence on what you can do.
- resources: every named tool/site/repo/asset.  Read URLs exactly as
  shown (e.g. "github.com/user/repo").  If no URL is shown, set to "".
  type should be one of: "tool|site|repo|library|framework|course|book|
  font|template|asset|newsletter|community|other".
  purpose = what it's for, in a few words.  This is the highest-value
  field — be exhaustive.
- tools: tool/app names extracted from resources, for quick filtering.
- guides: actionable instructions.
- downloads: downloadable assets with their URLs.

transcript — full enriched transcript.  For video, transcribe speech
and interleave bracketed scene notes: "words [scene: ...] words".
For a carousel, concatenate the text of every slide in order, prefixed
"[slide N] ".  Be thorough — search the full content.

Empty string for missing strings, [] for missing arrays.
""".strip()


# ── Helpers ─────────────────────────────────────────────────────────────────

def _silver_dir() -> pathlib.Path:
    return _db.SILVER_DIR


def _gold_dir() -> pathlib.Path:
    return _db.GOLD_DIR


def _mime_type(path: pathlib.Path) -> str:
    ext = path.suffix.lower()
    return {
        ".mp4": "video/mp4",
        ".mov": "video/quicktime",
        ".mpeg": "video/mpeg",
        ".webm": "video/webm",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".png": "image/png",
        ".gif": "image/gif",
        ".webp": "image/webp",
    }.get(ext, "application/octet-stream")


def _is_video(path: pathlib.Path) -> bool:
    return path.suffix.lower() in {".mp4", ".mov", ".mpeg", ".webm"}


def _post_media(post_dir: pathlib.Path) -> list[pathlib.Path]:
    """All media for a post, in analysis order.

    Video post -> [video.mp4]. Single image -> [image.jpg]. Carousel -> every
    slide (media_00.jpg, media_01.mp4, ... in order, interleaving image and
    video children). Capped at MAX_SLIDES.
    """
    media_dir = post_dir / "media"
    if not media_dir.is_dir():
        return []
    video = media_dir / "video.mp4"
    if video.exists():
        return [video]
    single = media_dir / "image.jpg"
    if single.exists():
        return [single]
    slides = sorted(
        q for q in media_dir.iterdir()
        if q.is_file() and q.suffix.lower() in SUPPORTED_EXTENSIONS
    )
    return slides[:MAX_SLIDES]


def _media_type_label(media: list[pathlib.Path]) -> str:
    """Classify a post's media for DB bookkeeping."""
    if len(media) == 0:
        return "none"
    if len(media) == 1:
        return "video" if _is_video(media[0]) else "image"
    return "carousel"


def _media_videos(media: list[pathlib.Path]) -> list[pathlib.Path]:
    """Video files within a media list that need Files API upload."""
    return [m for m in media if _is_video(m)]


def _archive_existing_analysis(out_dir: pathlib.Path) -> None:
    """Move an existing analysis.json to data/archive/<post_id>/ before overwrite.

    The post folder keeps only the latest analysis; prior versions are preserved
    under data/archive/<post_id>/ for recovery.
    """
    existing = out_dir / "enriched.json"
    if not existing.exists():
        return
    archive_dir = _db.DATA_DIR / "archive" / out_dir.name
    archive_dir.mkdir(parents=True, exist_ok=True)
    ver: object = "x"
    ts = ""
    with contextlib.suppress(Exception):
        old = json.loads(existing.read_text(encoding="utf-8"))
        ver = old.get("schema_version", "x")
        ts = (old.get("analysed_at") or "")
    ts = "".join(c for c in ts if c.isalnum() or c == "T")[:15]
    if not ts:
        ts = time.strftime("%Y%m%dT%H%M%S", time.localtime(existing.stat().st_mtime))
    dest = archive_dir / f"analysis_v{ver}_{ts}.json"
    n = 1
    while dest.exists():
        dest = archive_dir / f"analysis_v{ver}_{ts}_{n}.json"
        n += 1
    existing.replace(dest)
    log.debug("  archived previous analysis -> %s", dest)


# ── Token bucket — async, thread-safe ───────────────────────────────────────

class TokenBucket:
    """Leaky-bucket rate limiter.

    Tokens refill continuously at `rate` per second. Each acquire()
    call takes one token, blocking until one is available. Jitter is
    applied to refill intervals so concurrent waiters don't all unblock
    at exactly the same moment (thundering herd).
    """

    def __init__(self, rate_per_minute: int, jitter: float = 0.10) -> None:
        self._rate = rate_per_minute / 60.0  # tokens per second
        self._jitter = jitter
        self._tokens: float = float(rate_per_minute)  # start full
        self._max_tokens: float = float(rate_per_minute)
        self._last_refill: float = asyncio.get_event_loop().time()
        self._lock = asyncio.Lock()

    def _refill(self) -> None:
        now = asyncio.get_event_loop().time()
        elapsed = now - self._last_refill
        jittered_rate = self._rate * (1 + random.uniform(-self._jitter, self._jitter))
        self._tokens = min(self._max_tokens, self._tokens + elapsed * jittered_rate)
        self._last_refill = now

    async def acquire(self) -> None:
        while True:
            async with self._lock:
                self._refill()
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                wait = (1.0 - self._tokens) / self._rate
                wait *= 1 + random.uniform(0, self._jitter)
            await asyncio.sleep(wait)


# ── Upload worker ───────────────────────────────────────────────────────────

async def _upload_file(client: genai.Client, path: pathlib.Path) -> genai.types.File | None:
    """Upload a file to the Gemini Files API and wait until ACTIVE.

    Runs in a thread pool since the SDK is synchronous.
    Returns the File object or None on failure.
    """
    from google.genai.types import UploadFileConfig

    loop = asyncio.get_event_loop()
    mime = _mime_type(path)

    def _do_upload() -> genai.types.File:
        return client.files.upload(
            file=str(path),
            config=UploadFileConfig(mime_type=mime),
        )

    def _do_get(name: str) -> genai.types.File:
        return client.files.get(name=name)

    def _do_delete(name: str) -> None:
        with contextlib.suppress(Exception):
            client.files.delete(name=name)

    try:
        log.debug("  Uploading %s...", path.name)
        upload = await loop.run_in_executor(None, _do_upload)

        # Poll until ACTIVE (usually instant for short clips)
        deadline = time.monotonic() + 120
        while upload.state.name == "PROCESSING":
            if time.monotonic() > deadline:
                raise TimeoutError(f"{path.name} stuck in PROCESSING >120s")
            await asyncio.sleep(2)
            upload = await loop.run_in_executor(None, _do_get, upload.name)

        if upload.state.name != "ACTIVE":
            raise RuntimeError(f"{path.name} file state: {upload.state.name}")

        return upload

    except Exception as e:
        log.error("  Upload failed for %s: %s", path.name, e)
        return None


# ── Media parts builder ─────────────────────────────────────────────────────

async def _build_media_parts(
    media: list[pathlib.Path],
    uploads: dict[pathlib.Path, genai.types.File] | None = None,
) -> list[genai.types.Part]:
    """Build Gemini content parts for a post's media.

    Videos that have been uploaded to the Files API are sent as File references.
    Images are sent inline. Parts are ordered to match the media list so
    carousel slides stay in sequence.
    """
    from google.genai.types import Blob, FileData, Part

    uploads = uploads or {}
    loop = asyncio.get_event_loop()
    parts: list[Part] = []
    for m in media:
        uf = uploads.get(m)
        if uf is not None:
            parts.append(Part(file_data=FileData(file_uri=uf.uri, mime_type=uf.mime_type)))
        else:
            img_bytes = await loop.run_in_executor(None, m.read_bytes)
            parts.append(Part(inline_data=Blob(mime_type=_mime_type(m), data=img_bytes)))
    return parts


# ── Generate worker ─────────────────────────────────────────────────────────

async def _generate(
    client: genai.Client,
    label: str,
    media_parts: list[genai.types.Part],
    bucket: TokenBucket,
    max_retries: int = 4,
) -> dict | None:
    """Call generate_content with prebuilt media parts + the extraction prompt,
    respecting the token bucket. Retries with backoff on 429 / 5xx.
    Returns parsed JSON dict or None on exhausted retries.
    """
    from google.genai.types import GenerateContentConfig

    loop = asyncio.get_event_loop()
    parts = [*media_parts, PROMPT]

    for attempt in range(1, max_retries + 1):
        await bucket.acquire()

        try:
            log.debug("  Generating for %s (attempt %d)...", label, attempt)

            def _do_generate() -> genai.types.GenerateContentResponse:
                return client.models.generate_content(
                    model=GEMINI_MODEL,
                    contents=parts,
                    config=GenerateContentConfig(
                        response_mime_type="application/json",
                        temperature=0.0,
                    ),
                )

            response = await loop.run_in_executor(None, _do_generate)
            return json.loads(response.text.strip())

        except json.JSONDecodeError as e:
            log.warning("  %s attempt %d — bad JSON: %s", label, attempt, e)
            await asyncio.sleep(3)

        except Exception as e:
            msg = str(e).lower()
            if "429" in msg or "resource_exhausted" in msg or "quota" in msg:
                backoff = 60 * (2 ** (attempt - 1))  # 60, 120, 240, 480s
                log.warning(
                    "  %s attempt %d — rate limited, backing off %ds",
                    label, attempt, backoff,
                )
                await asyncio.sleep(backoff)
            elif "503" in msg or "unavailable" in msg or "timeout" in msg:
                backoff = 10 * attempt
                log.warning("  %s attempt %d — transient error, retry in %ds: %s",
                            label, attempt, backoff, e)
                await asyncio.sleep(backoff)
            else:
                log.error("  %s attempt %d — non-retryable: %s", label, attempt, e)
                break

    log.error("  %s failed after %d attempts", label, max_retries)
    return None


# ── Per-post processor ──────────────────────────────────────────────────────

async def _process_post(
    client: genai.Client,
    post_id: str,
    url: str,
    bucket: TokenBucket,
    upload_sem: asyncio.Semaphore,
    generate_sem: asyncio.Semaphore,
    index: int,
    total: int,
) -> dict | None:
    """End-to-end for one post: gather media → upload videos → generate → return result.

    Returns the parsed analysis dict on success, or None on failure.
    """
    label = post_id
    post_dir = _silver_dir() / post_id
    media = _post_media(post_dir)

    log.info("[%d/%d] %s (%d media)", index, total, label, len(media))

    # Upload every video in the media list (single-video posts AND video
    # slides within mixed carousels) so Gemini gets them as File references.
    uploads: dict[pathlib.Path, genai.types.File] = {}
    for vid in _media_videos(media):
        async with upload_sem:
            uf = await _upload_file(client, vid)
            if uf is None:
                return None
            uploads[vid] = uf

    media_parts = await _build_media_parts(media, uploads)

    async with generate_sem:
        result = await _generate(client, label, media_parts, bucket)

    # Clean up every uploaded file after the generate call.
    if uploads:
        loop = asyncio.get_event_loop()
        for uf in uploads.values():
            await loop.run_in_executor(None, lambda f=uf: client.files.delete(name=f.name))

    if result is None:
        return None

    edu = "EDU" if result.get("is_educational") else "skip"
    log.info(
        "  done  v%s %-13s  %s",
        result.get("value_score", "?"),
        f"{result.get('content_type', '?')}/{edu}",
        (result.get("summary") or "")[:60],
    )
    return result


# ── Async enrichment runner ─────────────────────────────────────────────────

async def _enrich_async(
    client: genai.Client,
    pending: list[tuple[str, str]],
    db: duckdb.DuckDBPyConnection,
) -> GoldResult:
    """Run the full async Gemini enrichment pipeline over pending posts."""
    result = GoldResult()
    bucket = TokenBucket(RPM_LIMIT, RPM_JITTER)
    upload_sem = asyncio.Semaphore(UPLOAD_CONCURRENCY)
    generate_sem = asyncio.Semaphore(GENERATE_CONCURRENCY)
    total = len(pending)
    t0 = time.time()

    async def _process_one(idx: int, post_id: str, url: str) -> tuple[str, dict | None]:
        analysis = await _process_post(
            client, post_id, url, bucket, upload_sem, generate_sem, idx, total,
        )
        return post_id, analysis

    # Process sequentially to respect rate limits and simplify error tracking.
    for i, (post_id, url) in enumerate(pending, 1):
        _, analysis = await _process_one(i, post_id, url)

        out_dir = _gold_dir() / post_id
        out_dir.mkdir(parents=True, exist_ok=True)

        if analysis is None:
            # Record failure
            db.execute(
                "INSERT OR REPLACE INTO gold_analyses "
                "(post_id, schema_version, status, error, attempts, analysed_at) "
                "VALUES (?, ?, 'failed', 'gemini_failed', COALESCE("
                "(SELECT attempts + 1 FROM gold_analyses WHERE post_id = ?), 1"
                "), CURRENT_TIMESTAMP)",
                (post_id, SCHEMA_VERSION, post_id),
            )
            result.failed += 1
        else:
            enriched = {
                "post_id": post_id,
                "url": url,
                "schema_version": SCHEMA_VERSION,
                "analysed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "analysis": analysis,
            }

            _archive_existing_analysis(out_dir)
            (out_dir / "enriched.json").write_text(
                json.dumps(enriched, ensure_ascii=False, indent=2), encoding="utf-8",
            )

            db.execute(
                "INSERT OR REPLACE INTO gold_analyses "
                "(post_id, schema_version, status, result_json, attempts, analysed_at) "
                "VALUES (?, ?, 'analysed', ?, 1, CURRENT_TIMESTAMP)",
                (post_id, SCHEMA_VERSION, json.dumps(enriched)),
            )
            result.analysed += 1

    db.commit()
    result.duration_secs = time.time() - t0
    return result


# ── Sync entry point ────────────────────────────────────────────────────────

def enrich_posts(
    *,
    max_posts: int | None = None,
    post_ids: Sequence[str] | None = None,
    db: duckdb.DuckDBPyConnection | None = None,
) -> GoldResult:
    """Run Gemini enrichment on un-analysed silver posts.

    Resumable — skips posts already in gold_analyses with status='analysed'.
    Failed posts (status='failed') are retried on re-run.
    """
    if db is None:
        db = _db.get_db()
    result = GoldResult()

    if post_ids is not None and not post_ids:
        log.info("No post_ids specified")
        return result
    if max_posts is not None and max_posts <= 0:
        log.info("max_posts <= 0, nothing to enrich")
        return result

    if post_ids:
        placeholders = ", ".join("?" * len(post_ids))
        pending = db.execute(
            "SELECT s.post_id, s.url "
            "FROM silver_posts s "
            "LEFT JOIN gold_analyses g ON s.post_id = g.post_id "
            "WHERE s.post_id IN (" + placeholders + ") "
            "  AND (g.post_id IS NULL OR g.status != 'analysed')",
            list(post_ids),
        ).fetchall()
    else:
        pending = db.execute(
            "SELECT s.post_id, s.url "
            "FROM silver_posts s "
            "LEFT JOIN gold_analyses g ON s.post_id = g.post_id "
            "WHERE g.post_id IS NULL OR g.status != 'analysed' "
            "ORDER BY s.silvered_at DESC "
            + ("LIMIT ?" if max_posts else ""),
            (int(max_posts),) if max_posts else (),
        ).fetchall()

    if not pending:
        log.info("No posts to enrich")
        return result

    # Import at runtime to avoid hard dependency on google-genai for
    # non-enrichment operations (apify, bronze, silver, views).
    import google.genai as genai_module

    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY not set in environment")

    client = genai_module.Client(api_key=api_key)
    result = asyncio.run(_enrich_async(client, pending, db))
    log.info("Enriched %d posts (%d failed) in %.1fs",
             result.analysed, result.failed, result.duration_secs)
    return result


# ── Views ───────────────────────────────────────────────────────────────────

def refresh_views(*, db: duckdb.DuckDBPyConnection | None = None) -> dict:
    """Create/replace DuckDB views over gold data."""
    if db is None:
        db = _db.get_db()
    db.execute("""
        CREATE OR REPLACE VIEW posts AS
        SELECT
            g.post_id,
            s.shortcode,
            s.url,
            s.caption,
            g.analysed_at,
            json_extract_string(g.result_json, '$.analysis.is_educational') = 'true' AS is_educational,
            json_extract_string(g.result_json, '$.analysis.is_actionable') = 'true' AS is_actionable,
            json_extract_string(g.result_json, '$.analysis.admirality') AS admirality,
            json_extract_string(g.result_json, '$.analysis.domain') AS domain,
            json_extract_string(g.result_json, '$.analysis.subdomain') AS subdomain,
            json_extract_string(g.result_json, '$.analysis.topic') AS topic,
            json_extract_string(g.result_json, '$.analysis.subtopic') AS subtopic,
            json_extract_string(g.result_json, '$.analysis.content_type') AS content_type,
            json_extract_string(g.result_json, '$.analysis.style') AS style,
            json_extract_string(g.result_json, '$.analysis.format') AS format,
            json(g.result_json) -> '$.analysis.educational_json' AS educational_json,
            json(g.result_json) -> '$.analysis.actionable_json' AS actionable_json,
            json(g.result_json) -> '$.analysis.transcript' AS transcript,
            json(g.result_json) AS result_json
        FROM gold_analyses g
        JOIN silver_posts s USING (post_id)
        WHERE g.status = 'analysed'
    """)
    db.commit()
    log.info("Views refreshed")
    return {"views_created": 1}
