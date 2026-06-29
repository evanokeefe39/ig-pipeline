"""Apify API client — trigger, poll, stream, explore.

All functions take an explicit `token` parameter. Never calls a non-token overload.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

import httpx
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from .models import ActorDetail, ActorSummary, InputField, RunInfo, RunSummary

log = logging.getLogger(__name__)
API_BASE = "https://api.apify.com/v2"
DEFAULT_TIMEOUT = 30


def _is_retryable(exception: BaseException) -> bool:
    """Return True for transient HTTP errors that should be retried.

    Retry on: 429 (rate limit), 5xx (server errors), network/connect/timeout errors.
    Do NOT retry on: 4xx (bad request, not found, unauthorized — permanent).
    """
    if isinstance(exception, httpx.TimeoutException):
        return True
    if isinstance(exception, httpx.NetworkError):
        return True
    if isinstance(exception, httpx.HTTPStatusError):
        status = exception.response.status_code
        if status == 429:
            return True
        if 500 <= status < 600:
            return True
        return False
    return False


# ── Client helpers ────────────────────────────────────────────────────────

def _get(path: str, token: str, **params: Any) -> dict[str, Any]:
    return _get_with_retry(path, token, **params)


@retry(
    retry=retry_if_exception(_is_retryable),
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    reraise=True,
)
def _get_with_retry(path: str, token: str, **params: Any) -> dict[str, Any]:
    url = f"{API_BASE}/{path.lstrip('/')}"
    params["token"] = token
    resp = httpx.get(url, params=params, timeout=DEFAULT_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    if data.get("error"):
        raise RuntimeError(f"Apify API error: {data['error']}")
    return data.get("data", data)


def _post(path: str, token: str, body: Any = None, **params: Any) -> dict[str, Any]:
    return _post_with_retry(path, token, body, **params)


@retry(
    retry=retry_if_exception(_is_retryable),
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    reraise=True,
)
def _post_with_retry(path: str, token: str, body: Any = None, **params: Any) -> dict[str, Any]:
    url = f"{API_BASE}/{path.lstrip('/')}"
    params["token"] = token
    resp = httpx.post(url, params=params, json=body, timeout=DEFAULT_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    if data.get("error"):
        raise RuntimeError(f"Apify API error: {data['error']}")
    return data.get("data", data)


# ── Extraction ────────────────────────────────────────────────────────────

def trigger_run(actor: str, urls: list[str], *, token: str) -> RunInfo:
    """Start an actor run. Returns immediately with run_id and dataset_id.

    The dataset_id is available before the run finishes — Apify creates it
    up front. Use `poll_run()` to wait for completion.
    """
    body = {
        "directUrls": urls,
        "resultsType": "posts",
        "resultsLimit": 1,
        "proxy": {"useApifyProxy": True},
    }
    result = _post(f"acts/{actor}/runs", token, body=body)
    run_id = result["id"]
    dataset_id = result.get("defaultDatasetId")
    cost = result.get("stats", {}).get("estimatedTotalPriceUsd", 0.0)
    log.info("Triggered run %s (dataset %s, est $%.4f)", run_id, dataset_id, cost)
    return RunInfo(run_id=run_id, dataset_id=dataset_id, actor=actor, estimated_cost_usd=cost)


def poll_run(run_id: str, *, token: str, poll_secs: int = 5, timeout: int = 600) -> str:
    """Poll until run completes. Returns dataset_id on SUCCEEDED.

    Raises RuntimeError on failure or timeout.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = _get(f"actor-runs/{run_id}", token)
        status = result.get("status")
        dataset_id = result.get("defaultDatasetId") or result.get("dataset", {}).get("id")
        if status == "SUCCEEDED":
            log.info("Run %s succeeded, dataset %s", run_id, dataset_id)
            return dataset_id
        if status in ("FAILED", "ABORTED", "TIMED-OUT"):
            raise RuntimeError(f"Run {run_id} {status}: {result.get('errorMessage', '')}")
        elapsed = timeout - (deadline - time.time())
        log.debug("Run %s status: %s (%.0fs elapsed)", run_id, status, elapsed)
        time.sleep(poll_secs)
    raise RuntimeError(f"Run {run_id} timed out after {timeout}s")


@retry(
    retry=retry_if_exception(_is_retryable),
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    reraise=True,
)
def _stream_get(url: str, **params: Any) -> httpx.Response:
    """GET with retries, returning the raw response for streaming."""
    resp = httpx.get(url, params=params, timeout=60)
    resp.raise_for_status()
    return resp


def stream_dataset(dataset_id: str, dest: Path, *, token: str) -> int:
    """Stream dataset via cursor pagination to ``dest`` as NDJSON.

    Never holds the full dataset in memory. Returns item count.
    Uses the ``?format=jsonl`` endpoint with cursor-based pagination.
    """
    item_count = 0
    cursor: str | None = None
    with open(dest, "w", encoding="utf-8") as f:
        while True:
            params: dict[str, Any] = {
                "format": "jsonl",
                "token": token,
                "limit": 500,
            }
            if cursor:
                params["cursor"] = cursor
            url = f"{API_BASE}/datasets/{dataset_id}/items"
            resp = _stream_get(url, **params)
            # Response body is NDJSON
            for line in resp.iter_lines():
                if line.strip():
                    f.write(line + "\n")
                    item_count += 1
            pagination = resp.headers.get("x-apify-pagination-cursor") or \
                         resp.headers.get("apify-pagination-cursor")
            if not pagination:
                break
            cursor = pagination
    log.info("Streamed %d items to %s", item_count, dest)
    return item_count

# ── Explore (read-only) ───────────────────────────────────────────────────

def search_actors(query: str, *, token: str, limit: int = 20) -> list[ActorSummary]:
    """Search Apify actor store by name/description."""
    result = _get("acts", token, search=query, limit=limit)
    items = result if isinstance(result, list) else result.get("items", [])
    return [
        ActorSummary(
            name=i["name"],
            title=i.get("title", ""),
            description=i.get("description", ""),
            user_score=i.get("userScore"),
            run_count=i.get("runCount"),
        )
        for i in items
    ]


def inspect_actor(actor_name: str, *, token: str) -> ActorDetail:
    """Get full actor metadata, input schema, and pricing."""
    result = _get(f"acts/{actor_name}", token)
    schema_raw = result.get("inputSchema", {})
    fields_raw = schema_raw.get("properties", {}) if isinstance(schema_raw, dict) else {}
    required = set(schema_raw.get("required", []))
    input_schema = [
        InputField(
            field=k,
            type=v.get("type", "string"),
            required=k in required,
            description=v.get("description", ""),
            default=v.get("default"),
        )
        for k, v in fields_raw.items()
        if isinstance(v, dict)
    ]
    return ActorDetail(
        name=result["name"],
        title=result.get("title", ""),
        description=result.get("description", ""),
        input_schema=input_schema,
        pricing=result.get("pricing", {}),
    )


def list_runs(actor: str, *, token: str, limit: int = 10) -> list[RunSummary]:
    """Recent runs for an actor."""
    result = _get(f"acts/{actor}/runs", token, limit=limit)
    items = result if isinstance(result, list) else result.get("items", [])
    return [
        RunSummary(
            run_id=i["id"],
            status=i.get("status", "UNKNOWN"),
            started_at=i.get("startedAt"),
            finished_at=i.get("finishedAt"),
            dataset_id=i.get("defaultDatasetId"),
            error=i.get("errorMessage"),
        )
        for i in items
    ]
