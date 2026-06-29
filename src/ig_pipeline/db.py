"""Single DuckDB connection for pipeline state + analytical views."""

from __future__ import annotations

from pathlib import Path

import duckdb

DATA_DIR = Path("data")
DB_PATH = DATA_DIR / "pipeline.db"
BRONZE_DIR = DATA_DIR / "bronze" / "datasets"
SILVER_DIR = DATA_DIR / "silver" / "posts"
GOLD_DIR = DATA_DIR / "gold" / "posts"

_conn: duckdb.DuckDBPyConnection | None = None


def get_db(path: str | None = None) -> duckdb.DuckDBPyConnection:
    """Get or create the DuckDB connection. Auto-creates tables on first call.

    When ``path`` is given (e.g. ``:memory:``), returns a fresh uncached
    connection — useful for tests. Otherwise uses the global singleton at
    ``DATA_DIR / pipeline.db``.
    """
    if path is not None:
        conn = duckdb.connect(path)
        _init_schema(conn)
        return conn

    global _conn
    if _conn is not None:
        return _conn

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    _conn = duckdb.connect(str(DB_PATH))
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
