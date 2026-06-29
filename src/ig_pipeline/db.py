"""Single DuckDB connection for pipeline state + analytical views.

Directory defaults are resolved from environment variables, falling back to
``data/`` under the current working directory. Set these for testing or
custom deployments:

  IG_DATA_DIR      — root data directory (default: ``data``)
  IG_DB_PATH       — DuckDB path (default: ``$IG_DATA_DIR/pipeline.db``)
  IG_BRONZE_DIR    — bronze datasets (default: ``$IG_DATA_DIR/bronze/datasets``)
  IG_SILVER_DIR    — silver posts (default: ``$IG_DATA_DIR/silver/posts``)
  IG_GOLD_DIR      — gold outputs (default: ``$IG_DATA_DIR/gold/posts``)
"""

from __future__ import annotations

import os
from pathlib import Path

import duckdb


def _env_path(key: str, default: str) -> Path:
    return Path(os.environ.get(key, default))


# ── Directory resolution (lazy, env-overridable) ──────────────────────────

_dirs: dict[str, Path] = {}


def _resolve(name: str, env_var: str, default: str) -> Path:
    """Resolve a directory path from env or default, with caching."""
    if name not in _dirs:
        _dirs[name] = _env_path(env_var, default)
    return _dirs[name]


def _data_dir() -> Path:
    """Root data directory (env: ``IG_DATA_DIR``, default: ``data``)."""
    return _resolve("data", "IG_DATA_DIR", "data")


def _db_path() -> Path:
    """DuckDB path (env: ``IG_DB_PATH``, default: ``$DATA_DIR/pipeline.db``)."""
    return _resolve("db", "IG_DB_PATH", str(_data_dir() / "pipeline.db"))


def _bronze_dir() -> Path:
    """Bronze datasets (env: ``IG_BRONZE_DIR``, default: ``$DATA_DIR/bronze/datasets``)."""
    return _resolve("bronze", "IG_BRONZE_DIR", str(_data_dir() / "bronze" / "datasets"))


def _silver_dir() -> Path:
    """Silver posts (env: ``IG_SILVER_DIR``, default: ``$DATA_DIR/silver/posts``)."""
    return _resolve("silver", "IG_SILVER_DIR", str(_data_dir() / "silver" / "posts"))


def _gold_dir() -> Path:
    """Gold outputs (env: ``IG_GOLD_DIR``, default: ``$DATA_DIR/gold/posts``)."""
    return _resolve("gold", "IG_GOLD_DIR", str(_data_dir() / "gold" / "posts"))


def _reset_dirs() -> None:
    """Clear cached directory resolution — picks up env changes on next access."""
    _dirs.clear()


def __getattr__(name: str) -> Path:
    """Attribute access: ``_db.BRONZE_DIR`` resolves via ``_bronze_dir()``."""
    _map = {
        "DATA_DIR": _data_dir,
        "DB_PATH": _db_path,
        "BRONZE_DIR": _bronze_dir,
        "SILVER_DIR": _silver_dir,
        "GOLD_DIR": _gold_dir,
    }
    if name in _map:
        return _map[name]()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


# ── Connection management ─────────────────────────────────────────────────

_conn: duckdb.DuckDBPyConnection | None = None


def get_db(path: str | None = None) -> duckdb.DuckDBPyConnection:
    """Get or create the DuckDB connection. Auto-creates tables on first call.

    When ``path`` is given (e.g. ``:memory:``), returns a fresh uncached
    connection — useful for tests. Otherwise uses the global singleton at
    ``DB_PATH()``.
    """
    if path is not None:
        conn = duckdb.connect(path)
        _init_schema(conn)
        return conn

    global _conn
    if _conn is not None:
        return _conn

    _data_dir().mkdir(parents=True, exist_ok=True)
    _bronze_dir().mkdir(parents=True, exist_ok=True)
    _silver_dir().mkdir(parents=True, exist_ok=True)
    _gold_dir().mkdir(parents=True, exist_ok=True)

    _conn = duckdb.connect(str(_db_path()))
    _init_schema(_conn)
    return _conn


def _init_schema(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze_ingests (
            dataset_id    TEXT PRIMARY KEY,
            run_id        TEXT NOT NULL,
            actor         TEXT NOT NULL,
            item_count    INTEGER NOT NULL DEFAULT 0,
            file_path     TEXT NOT NULL,
            checksum_sha256 TEXT,
            ingested_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS silver_posts (
            post_id       TEXT PRIMARY KEY,
            shortcode     TEXT,
            url           TEXT,
            caption       TEXT,
            owner_id      TEXT,
            owner_username TEXT,
            likes_count   INTEGER,
            comments_count INTEGER,
            video_play_count INTEGER,
            video_view_count INTEGER,
            timestamp     TIMESTAMP,
            hashtags      TEXT NOT NULL DEFAULT '[]',
            meta_data     TEXT,
            has_engagement_bait BOOLEAN NOT NULL DEFAULT FALSE,
            media_files   TEXT NOT NULL DEFAULT '[]',
            media_count   INTEGER NOT NULL DEFAULT 0,
            source_dataset TEXT NOT NULL,
            silvered_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS silver_progress (
            source_dataset TEXT PRIMARY KEY,
            post_count     INTEGER NOT NULL DEFAULT 0,
            completed_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gold_analyses (
            post_id        TEXT PRIMARY KEY REFERENCES silver_posts(post_id),
            schema_version INTEGER NOT NULL DEFAULT 2,
            status         TEXT NOT NULL DEFAULT 'pending',
            result_json    TEXT,
            error          TEXT,
            attempts       INTEGER NOT NULL DEFAULT 0,
            analysed_at    TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dim_time (
            time_key      INTEGER PRIMARY KEY,
            date          DATE NOT NULL,
            month         INTEGER NOT NULL,
            quarter       INTEGER NOT NULL,
            year          INTEGER NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dim_profile (
            profile_key   INTEGER PRIMARY KEY,
            owner_id      TEXT NOT NULL,
            owner_username TEXT NOT NULL,
            follower_count INTEGER,
            posts_count_ig INTEGER,
            bio           TEXT,
            is_verified   BOOLEAN,
            profile_category TEXT,
            external_url  TEXT,
            related_profiles TEXT,
            effective_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            effective_to   TIMESTAMP,
            is_current     BOOLEAN NOT NULL DEFAULT TRUE
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS taxonomy_terms (
            term_id        INTEGER PRIMARY KEY,
            term_type      TEXT NOT NULL,
            raw_value      TEXT NOT NULL,
            canonical_value TEXT NOT NULL,
            similarity_score FLOAT,
            review_status  TEXT NOT NULL DEFAULT 'self_mapped',
            effective_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            effective_to   TIMESTAMP,
            is_current     BOOLEAN NOT NULL DEFAULT TRUE,
            merged_into_id INTEGER
        )
    """)
    conn.commit()

def close() -> None:
    global _conn
    if _conn is not None:
        _conn.close()
        _conn = None
