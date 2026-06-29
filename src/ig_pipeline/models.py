from __future__ import annotations

from typing import Any

from pydantic import BaseModel

# ── Apify models ──────────────────────────────────────────────────────────

class RunInfo(BaseModel):
    run_id: str
    dataset_id: str | None = None
    actor: str
    estimated_cost_usd: float = 0.0


class RunSummary(BaseModel):
    run_id: str
    status: str
    started_at: str | None = None
    finished_at: str | None = None
    dataset_id: str | None = None
    error: str | None = None


class ActorSummary(BaseModel):
    name: str
    title: str
    description: str
    user_score: float | None = None
    run_count: int | None = None


class InputField(BaseModel):
    field: str
    type: str
    required: bool = False
    description: str = ""
    default: Any = None


class ActorDetail(ActorSummary):
    input_schema: list[InputField] = []
    pricing: dict[str, Any] = {}


# ── Layer results ─────────────────────────────────────────────────────────

class BronzeResult(BaseModel):
    dataset_id: str
    path: str
    item_count: int
    skipped: bool = False


class SilverResult(BaseModel):
    datasets_processed: int = 0
    posts_silvered: int = 0


class GoldResult(BaseModel):
    analysed: int = 0
    failed: int = 0
    skipped: int = 0
    duration_secs: float = 0.0


