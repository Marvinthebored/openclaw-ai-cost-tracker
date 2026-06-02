#!/usr/bin/env python3
"""
Migration script for AI Cost Tracker v3.
Parses OpenClaw and Codex CLI session logs and populates SQLite database.

Usage:
    python3 migrate_sessions.py [--full] [--incremental]

    --full        Rebuild entire database from scratch
    --incremental Only add new sessions (default)
"""

import argparse
import sqlite3
import json
import os
import re
import sys
import time
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Paths / runtime-configurable inputs
SCRIPT_DIR = Path(__file__).resolve().parent
DB_PATH = SCRIPT_DIR / "cost_tracker.db"
SESSIONS_DIR = Path.home() / ".openclaw" / "agents" / "main" / "sessions"
CODEX_SESSIONS_DIR = Path.home() / ".codex" / "sessions"
CHANNEL_MAPPING_PATH = None
BACKUPS_DIR = SCRIPT_DIR / "backups"

CHANNEL_MAPPING = {"channels": {}}
CHANNEL_ID_TO_NAME = {}


def load_channel_mapping(path: Path | None):
    """Load optional channel ID → name mappings.

    The tracker works without this file. When omitted, channel IDs are still parsed
    from session metadata, but human-friendly names may remain blank.
    """
    global CHANNEL_MAPPING, CHANNEL_ID_TO_NAME
    CHANNEL_MAPPING = {"channels": {}}
    CHANNEL_ID_TO_NAME = {}
    if not path:
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            CHANNEL_MAPPING = json.load(f)
        CHANNEL_ID_TO_NAME = {
            cid: (f"#{data.get('name')}" if data.get("name") and not str(data.get("name")).startswith('#') else str(data.get("name") or f"#{cid}"))
            for cid, data in CHANNEL_MAPPING.get("channels", {}).items()
        }
    except FileNotFoundError:
        print(f"WARNING: Channel mapping file not found: {path}")
    except Exception as exc:
        print(f"WARNING: Failed to load channel mapping {path}: {exc}")


def make_source_config(name: str, root: Path):
    return {
        "name": name,
        "root": root.expanduser().resolve(),
    }


def get_log_sources():
    return [
        make_source_config("openclaw", SESSIONS_DIR),
        make_source_config("codex_cli", CODEX_SESSIONS_DIR),
    ]

# Model reference initial data
MODEL_REFERENCE_DATA = [
    ("openai-codex/gpt-5.2", "codex", "OpenAI", "gpt-5.2", 1.0),
    ("openai-codex/gpt-5.2-codex", "codex", "OpenAI", "gpt-5.2-c", 1.0),
    ("openai-codex/gpt-5.3-codex", "codex", "OpenAI", "gpt-5.3-c", 1.0),

    # GitHub Copilot PRU weighting (0 = free)
    ("github-copilot/gpt-5-mini", "gh-copilot", "OpenAI", "gpt-5-mini", 0.0),
    ("github-copilot/gpt-4.1", "gh-copilot", "OpenAI", "gpt-4.1", 0.0),

    ("github-copilot/gpt-5.2", "gh-copilot", "OpenAI", "gpt-5.2", 1.0),
    ("github-copilot/claude-sonnet-4.6", "gh-copilot", "Anthropic", "sonnet-4.6", 1.0),
    ("github-copilot/claude-opus-4.6", "gh-copilot", "Anthropic", "opus-4.6", 3.0),

    ("github-copilot/claude-opus-4.5", "gh-copilot", "Anthropic", "opus-4.5", 3.0),
    ("github-copilot/claude-sonnet-4.5", "gh-copilot", "Anthropic", "sonnet-4.5", 1.0),
    ("github-copilot/claude-sonnet-4", "gh-copilot", "Anthropic", "sonnet-4", 1.0),
    ("github-copilot/claude-haiku-4.5", "gh-copilot", "Anthropic", "haiku-4.5", 0.33),

    ("anthropic/claude-opus-4-6", "anthropic", "Anthropic", "opus-4.6", 1.0),
    ("anthropic/claude-opus-4-5", "anthropic", "Anthropic", "opus-4.5", 1.0),
    ("anthropic/claude-sonnet-4-6", "anthropic", "Anthropic", "sonnet-4.6", 1.0),
    ("anthropic/claude-sonnet-4-5", "anthropic", "Anthropic", "sonnet-4.5", 1.0),

    ("modal/zai-org/GLM-5-FP8", "modal", "Z.AI", "glm-5", 1.0),
    ("zai/glm-5", "zai", "Z.AI", "glm-5", 1.0),
    ("ollama-cloud/glm-5", "ollama", "Z.AI", "glm-5", 1.0),
    ("opencode/glm-5-free", "opencode", "Z.AI", "glm-5-free", 1.0),

    ("moonshot/kimi-k2.5", "moonshot", "Moonshot", "kimi-k2.5", 1.0),
    ("kimi-coding/k2p5", "kimi-code", "Moonshot", "k2p5", 1.0),

    ("abacus/claude-opus-4-6", "abacus", "Anthropic", "opus-4.6", 1.0),
    ("abacus/claude-opus-4-5-20251101", "abacus", "Anthropic", "opus-4.5", 1.0),
    ("abacus/claude-sonnet-4-5-20250929", "abacus", "Anthropic", "sonnet-4.5", 1.0),

    ("openclaw/delivery-mirror", "openclaw", "System", "mirror", 1.0),
    ("openclaw/gateway-injected", "openclaw", "System", "injected", 1.0),
    ("bailian/qwen3.5-plus", "bailian", "Alibaba", "qwen3.5-plus", 1.0),
]

# Provider cost data
PROVIDER_COST_DATA = [
    # (provider, billing_start, plan_type, monthly_cost, extra_usage, notes)
    # These are seed defaults; manual edits via ref_import.py take precedence.
    ("openai-codex", "2026-02-04", "flat", 200, None, "JPY 30,000/mo approx"),
    ("github-copilot", "2026-02-21", "flat", 39, None, "Pro+ plan, 1,500 PRs/mo"),
    ("anthropic", "2026-02-04", "flat_plus_usage", 20, 10.51, "Pro + extra usage"),
    ("anthropic", "2026-03-04", "flat_plus_usage", 100, 0, "Max plan, upgraded Mar 4"),
    ("kimi-code", "2026-02-04", "flat", 20, None, "Subscription"),
    ("bailian", "2026-02-24", "flat", 50, None, "New sub Feb 24"),
    ("zai", "2026-02-23", "flat", 30, None, "New sub Feb 23"),
    ("moonshot", "2026-02-04", "payg", None, None, "PAYG, console tracking"),
    ("opencode", "2026-02-04", "free", 0, None, "GLM-5-free tier"),
    ("modal", "2026-02-04", "free_trial", 0, None, "Free trial, limited quota"),
]


def init_db(conn):
    """Initialize database schema.

    Note: on legacy DBs, `CREATE INDEX ... ON messages(model_ref_id)` may fail
    before the one-time compaction migration runs. That's expected; the migration
    step will rebuild `messages` and create the new index.
    """
    schema_path = SCRIPT_DIR / "schema.sql"
    with open(schema_path) as f:
        sql = f.read()
    try:
        conn.executescript(sql)
    except sqlite3.OperationalError as e:
        if "no such column: model_ref_id" not in str(e):
            raise
    conn.commit()
    print("✓ Schema created")


def populate_reference_tables(conn):
    """Populate reference tables without clobbering local edits.

    These tables are *project-local configuration*, not something that should be
    re-derived from session logs.

    Rules:
    - Never overwrite existing rows (preserve manual edits in SQLite).
    - Only INSERT missing rows so new models/providers can be introduced safely.
    """
    cur = conn.cursor()

    # Model reference (display aliases + PRU multipliers may be manually edited)
    cur.executemany(
        "INSERT OR IGNORE INTO model_reference (model_raw, endpoint, author, model, pru_multiplier) VALUES (?, ?, ?, ?, ?)",
        MODEL_REFERENCE_DATA
    )

    # Provider costs (manual config; never overwrite existing periods)
    cur.executemany(
        "INSERT OR IGNORE INTO provider_costs (provider, billing_start, plan_type, monthly_cost, extra_usage, notes) VALUES (?, ?, ?, ?, ?, ?)",
        PROVIDER_COST_DATA
    )

    conn.commit()



def ensure_pragmas(conn):
    """Apply pragmas for better concurrency + resilience."""
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
    except Exception:
        pass


def maybe_backup_db(db_path: Path, max_age_hours: int = 24, keep: int = 14):
    """Create a rolling SQLite backup if the newest backup is old.

    Keeps the DB as the archive: corruption -> restore last backup.
    """
    try:
        BACKUPS_DIR.mkdir(parents=True, exist_ok=True)
        backups = sorted(BACKUPS_DIR.glob("cost_tracker.backup-*.db"))
        now = time.time()
        if backups:
            newest = max(backups, key=lambda p: p.stat().st_mtime)
            age_hours = (now - newest.stat().st_mtime) / 3600
            if age_hours < max_age_hours:
                return

        ts = datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')
        out = BACKUPS_DIR / f"cost_tracker.backup-{ts}.db"

        if not db_path.exists():
            return

        src = sqlite3.connect(db_path)
        try:
            ensure_pragmas(src)
            dst = sqlite3.connect(out)
            try:
                src.backup(dst)
            finally:
                dst.close()
        finally:
            src.close()

        # Prune old backups
        backups = sorted(BACKUPS_DIR.glob("cost_tracker.backup-*.db"), key=lambda p: p.stat().st_mtime, reverse=True)
        for b in backups[keep:]:
            try:
                b.unlink()
            except Exception:
                pass
    except Exception:
        # Backup failures should not stop ingestion.
        pass


# --- Model reference helpers ---

def infer_model_reference(model_raw: str):
    """Infer model_reference fields for unknown models.

    The DB now uses model_reference as the canonical model dictionary.
    Messages store a compact integer FK (`model_ref_id`) while segments and
    invoices may still refer to `model_raw` directly.

    Return: (endpoint, author, model, pru_multiplier)
    """
    if not model_raw or "/" not in model_raw:
        return ("unknown", "Unknown", model_raw or "unknown", 1.0)

    provider, model = model_raw.split("/", 1)

    # Endpoint + author heuristics
    endpoint = provider
    author = "Unknown"

    if provider == "openai-codex":
        endpoint, author = "codex", "OpenAI"
    elif provider == "github-copilot":
        endpoint = "gh-copilot"
        ml = model.lower()
        if "claude" in ml:
            author = "Anthropic"
        elif "gemini" in ml:
            author = "Google"
        elif ml.startswith("gpt"):
            author = "OpenAI"
        elif "grok" in ml:
            author = "xAI"
    elif provider == "anthropic":
        endpoint, author = "anthropic", "Anthropic"
    elif provider in ("kimi-coding", "moonshot"):
        endpoint, author = ("kimi-code" if provider == "kimi-coding" else "moonshot"), "Moonshot"
    elif provider == "bailian":
        endpoint, author = "bailian", "Alibaba"
    elif provider in ("zai", "modal"):
        endpoint, author = provider, "Z.AI"
    elif provider in ("opencode", "ollama-cloud"):
        endpoint, author = "opencode", "Z.AI"
    elif provider == "abacus":
        endpoint = "abacus"
        author = "Anthropic" if "claude" in model.lower() else "Unknown"
    elif provider == "codex_cli":
        endpoint, author = "codex_cli", "OpenAI"
    elif provider == "openclaw":
        endpoint, author = "openclaw", "System"

    # PRU heuristics (only really meaningful for Copilot; elsewhere default 1.0)
    pru_multiplier = 1.0
    if provider == "github-copilot":
        ml = model.lower()
        if "opus" in ml:
            pru_multiplier = 3.0
        elif "haiku" in ml:
            pru_multiplier = 0.33
        elif ml.endswith("-mini") or ml == "gpt-4.1":
            pru_multiplier = 0.0

    return (endpoint, author, model, pru_multiplier)


def ensure_model_reference(cur, model_raw: str):
    """Ensure model_reference contains model_raw so FK constraints won't halt ingest."""
    if not model_raw:
        return
    endpoint, author, model, pru_multiplier = infer_model_reference(model_raw)
    cur.execute(
        "INSERT OR IGNORE INTO model_reference (model_raw, endpoint, author, model, pru_multiplier) VALUES (?, ?, ?, ?, ?)",
        (model_raw, endpoint, author, model, pru_multiplier),
    )


def get_model_ref_id(cur, model_raw: str) -> int:
    """Return compact integer FK for a model_raw, creating the reference row if needed."""
    ensure_model_reference(cur, model_raw)
    row = cur.execute("SELECT id FROM model_reference WHERE model_raw = ?", (model_raw,)).fetchone()
    if not row:
        raise RuntimeError(f"model_reference row missing after ensure: {model_raw}")
    return int(row[0])


def migrate_compact_schema(conn):
    """One-time in-place migration to compact reference-heavy tables.

    Goals:
    - add integer PK `model_reference.id`
    - compact `messages` to store `model_ref_id` instead of repeated provider/model_raw/role
    """
    cur = conn.cursor()

    # model_reference: legacy schema had model_raw as the PK and no integer id.
    mr_cols = [row[1] for row in cur.execute("PRAGMA table_info(model_reference)").fetchall()]
    if "id" not in mr_cols:
        conn.commit()
        cur.execute("PRAGMA foreign_keys=OFF")
        cur.execute("ALTER TABLE model_reference RENAME TO model_reference_legacy")
        cur.execute(
            """
            CREATE TABLE model_reference (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                model_raw TEXT NOT NULL UNIQUE,
                endpoint TEXT NOT NULL,
                author TEXT NOT NULL,
                model TEXT NOT NULL,
                pru_multiplier REAL DEFAULT 1.0
            )
            """
        )
        cur.execute(
            """
            INSERT INTO model_reference (model_raw, endpoint, author, model, pru_multiplier)
            SELECT model_raw, endpoint, author, model, pru_multiplier
            FROM model_reference_legacy
            ORDER BY model_raw
            """
        )
        cur.execute("DROP TABLE model_reference_legacy")
        cur.execute("PRAGMA foreign_keys=ON")
        conn.commit()

    # messages: compact legacy schema into FK-by-id form.
    msg_cols = [row[1] for row in cur.execute("PRAGMA table_info(messages)").fetchall()]
    if "model_ref_id" not in msg_cols:
        legacy_has_model_raw = "model_raw" in msg_cols
        if legacy_has_model_raw:
            for (model_raw,) in cur.execute("SELECT DISTINCT model_raw FROM messages WHERE model_raw IS NOT NULL AND model_raw != ''"):
                ensure_model_reference(cur, model_raw)
            conn.commit()

        cur.execute("PRAGMA foreign_keys=OFF")
        cur.execute("ALTER TABLE messages RENAME TO messages_legacy")
        cur.execute(
            """
            CREATE TABLE messages (
                id TEXT PRIMARY KEY,
                session_id TEXT REFERENCES sessions(session_id),
                parent_id TEXT,
                timestamp TEXT,
                model_ref_id INTEGER NOT NULL REFERENCES model_reference(id),
                input_tokens INTEGER,
                output_tokens INTEGER,
                cache_read INTEGER,
                cache_write INTEGER,
                total_tokens INTEGER,
                cost_input REAL,
                cost_output REAL,
                cost_cache_read REAL,
                cost_cache_write REAL,
                cost_total REAL
            )
            """
        )
        if legacy_has_model_raw:
            cur.execute(
                """
                INSERT INTO messages (
                    id, session_id, parent_id, timestamp, model_ref_id,
                    input_tokens, output_tokens, cache_read, cache_write, total_tokens,
                    cost_input, cost_output, cost_cache_read, cost_cache_write, cost_total
                )
                SELECT
                    ml.id, ml.session_id, ml.parent_id, ml.timestamp, mr.id,
                    ml.input_tokens, ml.output_tokens, ml.cache_read, ml.cache_write, ml.total_tokens,
                    ml.cost_input, ml.cost_output, ml.cost_cache_read, ml.cost_cache_write, ml.cost_total
                FROM messages_legacy ml
                INNER JOIN model_reference mr ON mr.model_raw = ml.model_raw
                """
            )
        else:
            cur.execute(
                """
                INSERT INTO messages (
                    id, session_id, parent_id, timestamp, model_ref_id,
                    input_tokens, output_tokens, cache_read, cache_write, total_tokens,
                    cost_input, cost_output, cost_cache_read, cost_cache_write, cost_total
                )
                SELECT
                    id, session_id, parent_id, timestamp, model_ref_id,
                    input_tokens, output_tokens, cache_read, cache_write, total_tokens,
                    cost_input, cost_output, cost_cache_read, cost_cache_write, cost_total
                FROM messages_legacy
                """
            )
        cur.execute("DROP TABLE messages_legacy")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_session ON messages(session_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages(timestamp)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_model_ref ON messages(model_ref_id)")
        cur.execute("PRAGMA foreign_keys=ON")
        conn.commit()

    # segments: preserve model_raw-based storage, but repair any FK that still points
    # at the temporary legacy table name from the model_reference migration.
    seg_fks = cur.execute("PRAGMA foreign_key_list(segments)").fetchall()
    seg_model_fk_targets = [row[2] for row in seg_fks if row[3] == "model_raw"]
    needs_segments_repair = bool(seg_model_fk_targets and any(target != "model_reference" for target in seg_model_fk_targets))
    if needs_segments_repair:
        cur.execute("PRAGMA foreign_keys=OFF")
        cur.execute("ALTER TABLE segments RENAME TO segments_legacy")
        cur.execute(
            """
            CREATE TABLE segments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL REFERENCES sessions(session_id),
                model_raw TEXT NOT NULL REFERENCES model_reference(model_raw),
                segment_index INTEGER NOT NULL,
                first_msg_ts TEXT,
                last_msg_ts TEXT,
                msg_count INTEGER DEFAULT 0,
                input_tokens INTEGER DEFAULT 0,
                output_tokens INTEGER DEFAULT 0,
                cache_read INTEGER DEFAULT 0,
                cache_write INTEGER DEFAULT 0,
                total_tokens INTEGER DEFAULT 0,
                cost_input REAL DEFAULT 0,
                cost_output REAL DEFAULT 0,
                cost_cache_read REAL DEFAULT 0,
                cost_cache_write REAL DEFAULT 0,
                cost_total REAL DEFAULT 0,
                UNIQUE(session_id, segment_index)
            )
            """
        )
        cur.execute(
            """
            INSERT INTO segments (
                id, session_id, model_raw, segment_index, first_msg_ts, last_msg_ts, msg_count,
                input_tokens, output_tokens, cache_read, cache_write, total_tokens,
                cost_input, cost_output, cost_cache_read, cost_cache_write, cost_total
            )
            SELECT
                id, session_id, model_raw, segment_index, first_msg_ts, last_msg_ts, msg_count,
                input_tokens, output_tokens, cache_read, cache_write, total_tokens,
                cost_input, cost_output, cost_cache_read, cost_cache_write, cost_total
            FROM segments_legacy
            """
        )
        cur.execute("DROP TABLE segments_legacy")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_segments_session ON segments(session_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_segments_model ON segments(model_raw)")
        cur.execute("PRAGMA foreign_keys=ON")
        conn.commit()


def get_ingest_state(cur, filename: str):
    cur.execute("SELECT session_id, last_offset, last_size, last_mtime FROM ingest_state WHERE filename = ?", (filename,))
    row = cur.fetchone()
    if not row:
        return None
    return {
        'session_id': row[0],
        'last_offset': row[1] or 0,
        'last_size': row[2],
        'last_mtime': row[3],
    }


def update_ingest_state(cur, filename: str, session_id: str, last_offset: int, st_size: int, st_mtime: float):
    cur.execute(
        """INSERT OR REPLACE INTO ingest_state
           (filename, session_id, last_offset, last_size, last_mtime, updated_at)
           VALUES (?, ?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ','now'))""",
        (filename, session_id, int(last_offset), int(st_size), float(st_mtime))
    )


def make_file_locator(source, filepath: Path) -> str:
    if source["name"] == "openclaw":
        return os.path.basename(filepath)
    rel = filepath.resolve().relative_to(source["root"])
    return f'{source["name"]}:{rel.as_posix()}'


def list_source_files(source):
    root = source["root"]
    if not root.exists():
        return [], f'{source["name"]}: directory missing ({root})'
    if source["name"] == "openclaw":
        active = sorted(root.glob("*.jsonl"))
        deleted = sorted(root.glob("*.jsonl.deleted.*"))
        reset = sorted(root.glob("*.jsonl.reset.*"))
        all_files = active + deleted + reset
        summary = f'{source["name"]}: {len(active)} active + {len(deleted)} deleted + {len(reset)} reset = {len(all_files)} total'
        return all_files, summary
    files = sorted(root.rglob("*.jsonl"))
    return files, f'{source["name"]}: {len(files)} total'


def extract_codex_session_id_from_path(filepath: Path):
    match = re.search(r'([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})', filepath.name)
    return match.group(1) if match else None


def extract_codex_model(data):
    payload = data.get("payload") if isinstance(data.get("payload"), dict) else {}
    model = payload.get("model")
    if isinstance(model, str) and model:
        return model
    collab = payload.get("collaboration_mode")
    if isinstance(collab, dict):
        settings = collab.get("settings")
        if isinstance(settings, dict):
            model = settings.get("model")
            if isinstance(model, str) and model:
                return model
    return None


def get_last_model_for_session(cur, session_id: str):
    row = cur.execute(
        """
        SELECT mr.model_raw
        FROM messages m
        INNER JOIN model_reference mr ON mr.id = m.model_ref_id
        WHERE m.session_id = ?
        ORDER BY m.timestamp DESC, m.id DESC
        LIMIT 1
        """,
        (session_id,),
    ).fetchone()
    if not row or not row[0] or "/" not in row[0]:
        return None
    return row[0].split("/", 1)[1]


def insert_message_row(cur, msg_id, session_id, parent_id, timestamp, model_raw, usage, cost=None):
    model_ref_id = get_model_ref_id(cur, model_raw)
    cost = cost or {}
    cur.execute(
        """INSERT OR REPLACE INTO messages
           (id, session_id, parent_id, timestamp, model_ref_id,
            input_tokens, output_tokens, cache_read, cache_write, total_tokens,
            cost_input, cost_output, cost_cache_read, cost_cache_write, cost_total)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            msg_id,
            session_id,
            parent_id,
            timestamp,
            model_ref_id,
            usage.get("input_tokens", 0),
            usage.get("output_tokens", 0),
            usage.get("cache_read", 0),
            usage.get("cache_write", 0),
            usage.get("total_tokens", 0),
            cost.get("input", 0),
            cost.get("output", 0),
            cost.get("cacheRead", 0),
            cost.get("cacheWrite", 0),
            cost.get("total", 0),
        ),
    )


def ingest_openclaw_messages_from_offset(filepath: Path, conn, start_offset: int, session_id: str):
    """Append-only ingest: read only new bytes and upsert assistant messages.

    Does NOT delete existing messages — DB is the archive.
    Returns count of assistant messages processed.
    """
    cur = conn.cursor()
    processed = 0

    with open(filepath, 'rb') as f:
        f.seek(start_offset)
        while True:
            line = f.readline()
            if not line:
                break
            try:
                data = json.loads(line.decode('utf-8', errors='strict'))
            except Exception:
                continue

            if data.get("type") == "message" and data.get("message", {}).get("role") == "assistant":
                msg = data.get("message", {})
                msg_id = data.get("id")
                parent_id = data.get("parentId")
                timestamp = data.get("timestamp")

                provider = msg.get("provider", "unknown")
                model_raw = f"{provider}/{msg.get('model', 'unknown')}"

                usage = msg.get("usage", {})
                cost = usage.get("cost", {})
                insert_message_row(
                    cur,
                    msg_id,
                    session_id,
                    parent_id,
                    timestamp,
                    model_raw,
                    {
                        "input_tokens": usage.get("input", 0),
                        "output_tokens": usage.get("output", 0),
                        "cache_read": usage.get("cacheRead", 0),
                        "cache_write": usage.get("cacheWrite", 0),
                        "total_tokens": usage.get("totalTokens", 0),
                    },
                    {
                        "input": cost.get("input", 0) if cost else 0,
                        "output": cost.get("output", 0) if cost else 0,
                        "cacheRead": cost.get("cacheRead", 0) if cost else 0,
                        "cacheWrite": cost.get("cacheWrite", 0) if cost else 0,
                        "total": cost.get("total", 0) if cost else 0,
                    },
                )
                processed += 1

    return processed


def ingest_codex_messages_from_offset(filepath: Path, conn, start_offset: int, session_id: str):
    """Append-only ingest for Codex CLI token-count events."""
    cur = conn.cursor()
    processed = 0
    current_model = get_last_model_for_session(cur, session_id)

    with open(filepath, 'rb') as f:
        f.seek(start_offset)
        while True:
            line_offset = f.tell()
            line = f.readline()
            if not line:
                break
            try:
                data = json.loads(line.decode('utf-8', errors='strict'))
            except Exception:
                continue

            current_model = extract_codex_model(data) or current_model
            payload = data.get("payload") if isinstance(data.get("payload"), dict) else {}
            if data.get("type") != "event_msg" or payload.get("type") != "token_count":
                continue

            info = payload.get("info") if isinstance(payload.get("info"), dict) else {}
            last_usage = info.get("last_token_usage")
            if not isinstance(last_usage, dict):
                continue

            model_name = current_model or "unknown"
            model_raw = f"codex_cli/{model_name}"
            msg_id = f"{session_id}:codex:{line_offset}"
            insert_message_row(
                cur,
                msg_id,
                session_id,
                None,
                data.get("timestamp"),
                model_raw,
                {
                    "input_tokens": last_usage.get("input_tokens", 0),
                    "output_tokens": last_usage.get("output_tokens", 0),
                    "cache_read": last_usage.get("cached_input_tokens", 0),
                    "cache_write": 0,
                    "total_tokens": last_usage.get("total_tokens", 0),
                },
            )
            processed += 1

    return processed


def ingest_messages_from_offset(filepath: Path, conn, start_offset: int, session_id: str, source):
    if source["name"] == "codex_cli":
        return ingest_codex_messages_from_offset(filepath, conn, start_offset, session_id)
    return ingest_openclaw_messages_from_offset(filepath, conn, start_offset, session_id)


def recompute_segments(conn, session_id: str):
    """Recompute model segments from archived messages in the DB."""
    cur = conn.cursor()

    # Delete only derived data (segments). Messages are the archive.
    cur.execute("DELETE FROM segments WHERE session_id = ?", (session_id,))

    cur.execute(
        """SELECT m.timestamp, mr.model_raw, m.input_tokens, m.output_tokens, m.cache_read, m.cache_write, m.total_tokens,
                  m.cost_input, m.cost_output, m.cost_cache_read, m.cost_cache_write, m.cost_total
           FROM messages m
           INNER JOIN model_reference mr ON mr.id = m.model_ref_id
           WHERE m.session_id = ?
           ORDER BY m.timestamp ASC""",
        (session_id,)
    )
    rows = cur.fetchall()
    if not rows:
        return

    def is_mirror(model_raw: str):
        return model_raw == 'openclaw/delivery-mirror'

    segment_index = 0
    seg_model = None
    seg_rows = []

    def flush():
        nonlocal segment_index, seg_rows
        if not seg_rows or not seg_model:
            return
        first_ts = min(r[0] for r in seg_rows)
        last_ts = max(r[0] for r in seg_rows)
        msg_count = len(seg_rows)

        agg = {
            'input_tokens': sum(r[2] or 0 for r in seg_rows),
            'output_tokens': sum(r[3] or 0 for r in seg_rows),
            'cache_read': sum(r[4] or 0 for r in seg_rows),
            'cache_write': sum(r[5] or 0 for r in seg_rows),
            'total_tokens': sum(r[6] or 0 for r in seg_rows),
            'cost_input': sum(r[7] or 0 for r in seg_rows),
            'cost_output': sum(r[8] or 0 for r in seg_rows),
            'cost_cache_read': sum(r[9] or 0 for r in seg_rows),
            'cost_cache_write': sum(r[10] or 0 for r in seg_rows),
            'cost_total': sum(r[11] or 0 for r in seg_rows),
        }

        ensure_model_reference(cur, seg_model)
        cur.execute(
            """INSERT OR REPLACE INTO segments
               (session_id, model_raw, segment_index, first_msg_ts, last_msg_ts, msg_count,
                input_tokens, output_tokens, cache_read, cache_write, total_tokens,
                cost_input, cost_output, cost_cache_read, cost_cache_write, cost_total)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (session_id, seg_model, segment_index, first_ts, last_ts, msg_count,
             agg['input_tokens'], agg['output_tokens'], agg['cache_read'], agg['cache_write'], agg['total_tokens'],
             agg['cost_input'], agg['cost_output'], agg['cost_cache_read'], agg['cost_cache_write'], agg['cost_total'])
        )
        seg_rows = []
        segment_index += 1

    for row in rows:
        ts, model_raw = row[0], row[1]
        if not model_raw or is_mirror(model_raw):
            continue
        if seg_model is None:
            seg_model = model_raw
        if model_raw != seg_model:
            flush()
            seg_model = model_raw
        seg_rows.append(row)

    flush()

def extract_channel_info(user_text):
    """Extract channel info from user message text.

    Returns (channel_id, channel_name, surface) where surface is
    'discord', 'telegram', 'gui', 'dm'.
    """
    if not user_text:
        return None, None, "gui"

    # Try to find JSON block with channel info (conversation metadata)
    json_match = re.search(r'\{[^{}]*"(?:group_channel|conversation_label|chat_type)"[^{}]*\}', user_text)
    if json_match:
        try:
            data = json.loads(json_match.group())
            channel_name = data.get("group_channel", "")
            is_group = data.get("is_group_chat", False)
            chat_type = data.get("chat_type", "")

            # Extract channel ID from conversation_label if present
            conv_label = data.get("conversation_label", "")
            id_match = re.search(r'channel id:(\d+)', conv_label)
            channel_id = id_match.group(1) if id_match else None

            # Detect surface from conversation_label
            if "Guild" in conv_label or "discord" in conv_label.lower():
                surface = "discord"
            elif "telegram" in conv_label.lower():
                surface = "telegram"
            elif chat_type == "user" or (not is_group and not channel_name):
                surface = "dm"
            else:
                surface = "gui"

            # If no channel name but we have an ID, look it up
            if channel_id and not channel_name:
                channel_name = CHANNEL_ID_TO_NAME.get(channel_id, f"#{channel_id}")

            # For DMs, label as DM
            if not is_group and surface in ("discord", "telegram") and not channel_name:
                channel_name = "DM"

            return channel_id, channel_name, surface
        except:
            pass

    # Check for [Discord Guild #channel ...] prefix (used in older sessions and subagent prompts)
    discord_prefix = re.search(r'\[Discord Guild (#\S+)\s+channel id:(\d+)', user_text)
    if discord_prefix:
        ch_name = discord_prefix.group(1)
        ch_id = discord_prefix.group(2)
        # Resolve name from mapping if available
        resolved = CHANNEL_ID_TO_NAME.get(ch_id, ch_name)
        return ch_id, resolved, "discord"

    # Check for DM indicators
    if '"is_group_chat": false' in user_text or '"chat_type": "user"' in user_text:
        return None, "DM", "dm"

    return None, None, "gui"


def extract_cron_info(user_text):
    """Extract cron job ID and name from first user message.

    Looks for pattern: [cron:UUID jobName]
    Returns (cron_job_id, cron_job_name) or (None, None).
    """
    if not user_text:
        return None, None
    cron_match = re.match(r'\[cron:([0-9a-f-]+)\s+([^\]]+)\]', user_text)
    if cron_match:
        return cron_match.group(1), cron_match.group(2).strip()
    return None, None


def classify_cron_label(cron_job_name):
    """Turn a cron job name into a short display label."""
    if not cron_job_name:
        return None
    name_lower = cron_job_name.lower()
    if "heartbeat" in name_lower:
        return "heartbeat"
    if "crash" in name_lower or "sentinel" in name_lower:
        return "crash-sentinel"
    if "watchdog" in name_lower:
        return "watchdog"
    if "morning" in name_lower or "briefing" in name_lower:
        return "morning-briefing"
    if "reminder" in name_lower:
        return "reminder"
    if "memory review" in name_lower:
        return "memory-review"
    if "check-in" in name_lower:
        return "check-in"
    if "tdnet" in name_lower or "earnings" in name_lower:
        return "tdnet-watch"
    if "dividend" in name_lower:
        return "dividend-watch"
    if "discord" in name_lower and "check" in name_lower:
        return "discord-check"
    # Generic: use the full name but cap at 30 chars
    short = cron_job_name[:30].strip()
    if len(cron_job_name) > 30:
        short += "…"
    return short


def extract_task_label(user_text):
    """Extract a short label from a subagent's task prompt."""
    if not user_text:
        return None

    # If it's a cron job inside a subagent, use cron label
    cron_id, cron_name = extract_cron_info(user_text)
    if cron_name:
        return classify_cron_label(cron_name)

    # If it's a heartbeat
    if "Read HEARTBEAT.md" in user_text and "HEARTBEAT_OK" in user_text:
        return "heartbeat"

    # Strip envelope layers (order matters — strip outermost first)
    text = user_text

    # System messages (can appear at start)
    text = re.sub(r'System:\s*\[.*?\].*?\n', '', text)

    # JSON metadata blocks
    text = re.sub(r'```json\s*\{[^}]*?\}\s*```', '', text, flags=re.DOTALL)
    text = re.sub(r'Conversation info.*?```', '', text, flags=re.DOTALL)
    text = re.sub(r'Sender \(untrusted.*?```', '', text, flags=re.DOTALL)

    text = text.strip()

    # [Discord Guild #channel channel id:NNN +Ns Day YYYY...] Sender (user): message
    text = re.sub(r'^\[Discord Guild[^\]]*\]\s*\S+\s*\([^)]*\):\s*', '', text)

    # [Telegram Name (@handle) ...] message
    text = re.sub(r'^\[Telegram[^\]]*\]\s*', '', text)

    # [Day YYYY-MM-DD HH:MM TZ] prefix (timestamp envelope)
    text = re.sub(r'^\[\w{3}\s+\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}\s+GMT[^\]]*\]\s*', '', text)

    # Sender line: "Username (handle): "
    text = re.sub(r'^\S+\s*\([^)]*\):\s*', '', text)

    # [from: ...] and [message_id: ...] tags
    text = re.sub(r'\[from:[^\]]*\]', '', text)
    text = re.sub(r'\[message_id:[^\]]*\]', '', text)

    text = text.strip()

    # Known patterns → short labels
    if text.startswith("A new session was started"):
        return "new session"
    if "scheduled reminder" in text.lower():
        return "reminder"

    # Clean up and take first meaningful chunk
    text = text.strip()
    if not text:
        return None

    # Take first line or first 35 chars
    first_line = text.split('\n')[0].strip()
    if len(first_line) > 35:
        return first_line[:32] + "…"
    return first_line or None


def is_heartbeat_text(user_text):
    """Check if this is a heartbeat prompt (even without [cron:] prefix)."""
    if not user_text:
        return False
    return "Read HEARTBEAT.md" in user_text and "HEARTBEAT_OK" in user_text


def infer_source_and_label(first_user_text, filename):
    """Determine session source and label from first user message + filename.

    Returns (source, label) where:
      source: 'discord', 'telegram', 'cron', 'subagent', 'gui'
      label: cron job label, parent reference, or None
    """
    is_subagent = "__" in filename

    # Extract cron info from message text
    cron_id, cron_name = extract_cron_info(first_user_text or "")

    # Check for heartbeat (with or without cron prefix)
    if cron_name and "heartbeat" in cron_name.lower():
        return "cron", "heartbeat"
    if is_heartbeat_text(first_user_text):
        return "cron", "heartbeat"

    # Other cron jobs
    if cron_id:
        return "cron", classify_cron_label(cron_name)

    # Sub-agents (filename has parentPrefix__childUUID pattern)
    if is_subagent:
        # Try to extract a useful task excerpt from the first user message
        task_label = extract_task_label(first_user_text)
        return "subagent", task_label

    # Interactive sessions — detect surface from channel info
    # (will be refined by channel extraction in the caller)
    return "gui", None


def detect_deleted(filename):
    """Detect if session is deleted/reset from filename pattern."""
    # Pattern: uuid.jsonl.deleted.2026-02-25T09-20-51.731Z
    # Also:    uuid.jsonl.reset.2026-02-28T00-38-43.799Z
    for marker in (".deleted.", ".reset."):
        if marker in filename:
            match = re.search(r'\.(?:deleted|reset)\.(\d{4}-\d{2}-\d{2}T[\d:-]+)', filename)
            if match:
                return True, match.group(1).replace("T", " ")
            return True, None
    return False, None


def parse_openclaw_session_file(filepath, conn, logical_filename: str):
    """Parse a single session file and insert into database."""
    filename = logical_filename
    cur = conn.cursor()

    session_id = None
    session_nickname = None
    created_at = None
    channel_id = None
    channel_name = None
    surface = "gui"  # discord, telegram, gui, dm
    message_count = 0
    insert_errors = 0
    deleted_at = None
    first_user_text = None  # Capture first user message for classification

    # Segment tracking - detect model changes from assistant messages
    current_model_raw = None
    segment_index = 0
    segment_msgs = []  # Collect messages per segment

    # Detect deleted status
    is_deleted, deleted_timestamp = detect_deleted(filename)
    if is_deleted and deleted_timestamp:
        deleted_at = deleted_timestamp

    def flush_segment():
        """Write accumulated segment data to DB."""
        nonlocal segment_msgs, segment_index
        if not segment_msgs or not session_id:
            return
        # Aggregate segment data
        first_ts = min(m['timestamp'] for m in segment_msgs)
        last_ts = max(m['timestamp'] for m in segment_msgs)
        agg = {
            'input_tokens': sum(m['input_tokens'] for m in segment_msgs),
            'output_tokens': sum(m['output_tokens'] for m in segment_msgs),
            'cache_read': sum(m['cache_read'] for m in segment_msgs),
            'cache_write': sum(m['cache_write'] for m in segment_msgs),
            'total_tokens': sum(m['total_tokens'] for m in segment_msgs),
            'cost_input': sum(m['cost_input'] for m in segment_msgs),
            'cost_output': sum(m['cost_output'] for m in segment_msgs),
            'cost_cache_read': sum(m['cost_cache_read'] for m in segment_msgs),
            'cost_cache_write': sum(m['cost_cache_write'] for m in segment_msgs),
            'cost_total': sum(m['cost_total'] for m in segment_msgs),
        }
        ensure_model_reference(cur, segment_msgs[0]['model_raw'])
        cur.execute("""
            INSERT OR REPLACE INTO segments
            (session_id, model_raw, segment_index, first_msg_ts, last_msg_ts, msg_count,
             input_tokens, output_tokens, cache_read, cache_write, total_tokens,
             cost_input, cost_output, cost_cache_read, cost_cache_write, cost_total)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (session_id, segment_msgs[0]['model_raw'], segment_index, first_ts, last_ts, len(segment_msgs),
              agg['input_tokens'], agg['output_tokens'], agg['cache_read'], agg['cache_write'], agg['total_tokens'],
              agg['cost_input'], agg['cost_output'], agg['cost_cache_read'], agg['cost_cache_write'], agg['cost_total']))
        segment_msgs = []
        segment_index += 1

    try:
        with open(filepath, 'r') as f:
            for line_num, line in enumerate(f):
                try:
                    data = json.loads(line)

                    # Session header — insert stub immediately so message FK never fails
                    if data.get("type") == "session":
                        session_id = data.get("id")
                        # Use chars 9-13 (second group of 4) for visual scanning
                        session_nickname = session_id[9:13] if session_id and len(session_id) >= 13 else session_id
                        created_at = data.get("timestamp")
                        # Insert stub now so subsequent message INSERTs satisfy the FK constraint.
                        # Full metadata (channel, label, etc.) is written at the end.
                        if session_id:
                            cur.execute(
                                """INSERT OR IGNORE INTO sessions
                                   (session_id, session_nickname, filename, created_at)
                                   VALUES (?, ?, ?, ?)""",
                                (session_id, session_nickname, filename, created_at)
                            )

                    # User message - extract channel info + capture first user text
                    elif data.get("type") == "message" and data.get("message", {}).get("role") == "user":
                        content = data.get("message", {}).get("content", [])
                        if content and content[0].get("type") == "text":
                            user_text = content[0].get("text", "")
                            # Capture first user text for classification
                            if first_user_text is None:
                                first_user_text = user_text
                            cid, cname, csurf = extract_channel_info(user_text)
                            if cid:
                                channel_id = cid
                            if cname:
                                channel_name = cname
                            if csurf != "gui":
                                surface = csurf

                    # Assistant message - token/cost data
                    elif data.get("type") == "message" and data.get("message", {}).get("role") == "assistant":
                        msg = data.get("message", {})
                        msg_id = data.get("id")
                        parent_id = data.get("parentId")
                        timestamp = data.get("timestamp")

                        provider = msg.get("provider", "unknown")
                        model_raw = f"{provider}/{msg.get('model', 'unknown')}"

                        # Skip openclaw/delivery-mirror - don't create segments for these
                        is_mirror = provider == "openclaw" and model_raw.endswith("/delivery-mirror")

                        usage = msg.get("usage", {})
                        input_tokens = usage.get("input", 0)
                        output_tokens = usage.get("output", 0)
                        cache_read = usage.get("cacheRead", 0)
                        cache_write = usage.get("cacheWrite", 0)
                        total_tokens = usage.get("totalTokens", 0)

                        cost = usage.get("cost", {})
                        cost_input = cost.get("input", 0) if cost else 0
                        cost_output = cost.get("output", 0) if cost else 0
                        cost_cache_read = cost.get("cacheRead", 0) if cost else 0
                        cost_cache_write = cost.get("cacheWrite", 0) if cost else 0
                        cost_total = cost.get("total", 0) if cost else 0

                        # Insert message (still record mirror messages, just don't segment on them)
                        insert_message_row(
                            cur,
                            msg_id,
                            session_id,
                            parent_id,
                            timestamp,
                            model_raw,
                            {
                                'input_tokens': input_tokens,
                                'output_tokens': output_tokens,
                                'cache_read': cache_read,
                                'cache_write': cache_write,
                                'total_tokens': total_tokens,
                            },
                            {
                                'input': cost_input,
                                'output': cost_output,
                                'cacheRead': cost_cache_read,
                                'cacheWrite': cost_cache_write,
                                'total': cost_total,
                            },
                        )

                        # Skip mirror messages for segment tracking
                        if is_mirror:
                            continue

                        # Detect model change - flush segment if model changed
                        if current_model_raw is not None and model_raw != current_model_raw:
                            flush_segment()
                        current_model_raw = model_raw

                        # Collect for segment aggregation
                        segment_msgs.append({
                            'timestamp': timestamp,
                            'model_raw': model_raw,
                            'input_tokens': input_tokens,
                            'output_tokens': output_tokens,
                            'cache_read': cache_read,
                            'cache_write': cache_write,
                            'total_tokens': total_tokens,
                            'cost_input': cost_input,
                            'cost_output': cost_output,
                            'cost_cache_read': cost_cache_read,
                            'cost_cache_write': cost_cache_write,
                            'cost_total': cost_total,
                        })

                        message_count += 1

                except json.JSONDecodeError:
                    continue
                except Exception as e:
                    insert_errors += 1
                    print(f"  Error parsing line {line_num} in {filename}: {e}")
                    continue

        # Flush final segment
        flush_segment()

        # Insert session record
        if insert_errors:
            print(f"  WARNING: {insert_errors} line(s) failed during parsing of {filename}")
        if session_id:
            # Look up channel name from ID if we have ID but no name
            if channel_id and not channel_name:
                channel_name = CHANNEL_ID_TO_NAME.get(channel_id, f"#{channel_id}")

            # Classify source and label
            source, label = infer_source_and_label(first_user_text, filename)

            # If source is still "gui" but we detected a surface, upgrade it
            if source == "gui" and surface in ("discord", "telegram", "dm"):
                source = surface

            cur.execute("""
                INSERT OR REPLACE INTO sessions
                (session_id, session_nickname, parent_nickname, filename, created_at,
                 channel_id, channel_name, channel_type, session_type, deleted_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (session_id, session_nickname, label, filename, created_at,
                  channel_id, channel_name, surface, source, deleted_at))

            return True, message_count, session_id

    except Exception as e:
        print(f"  Error reading {filename}: {e}")
        return False, 0, None

    return False, 0, None


def parse_codex_session_file(filepath, conn, logical_filename: str):
    """Parse a Codex CLI transcript and normalize token-count events into assistant usage rows."""
    filename = logical_filename
    cur = conn.cursor()

    session_id = extract_codex_session_id_from_path(filepath)
    session_nickname = session_id[9:13] if session_id and len(session_id) >= 13 else session_id
    created_at = None
    message_count = 0
    insert_errors = 0
    current_model = None
    current_model_raw = None
    segment_index = 0
    segment_msgs = []
    source_label = "codex_cli"

    def ensure_session_stub():
        if not session_id:
            return
        cur.execute(
            """INSERT OR IGNORE INTO sessions
               (session_id, session_nickname, filename, created_at)
               VALUES (?, ?, ?, ?)""",
            (session_id, session_nickname, filename, created_at),
        )

    def flush_segment():
        nonlocal segment_msgs, segment_index
        if not segment_msgs or not session_id:
            return
        first_ts = min(m['timestamp'] for m in segment_msgs)
        last_ts = max(m['timestamp'] for m in segment_msgs)
        agg = {
            'input_tokens': sum(m['input_tokens'] for m in segment_msgs),
            'output_tokens': sum(m['output_tokens'] for m in segment_msgs),
            'cache_read': sum(m['cache_read'] for m in segment_msgs),
            'cache_write': sum(m['cache_write'] for m in segment_msgs),
            'total_tokens': sum(m['total_tokens'] for m in segment_msgs),
        }
        ensure_model_reference(cur, segment_msgs[0]['model_raw'])
        cur.execute(
            """
            INSERT OR REPLACE INTO segments
            (session_id, model_raw, segment_index, first_msg_ts, last_msg_ts, msg_count,
             input_tokens, output_tokens, cache_read, cache_write, total_tokens,
             cost_input, cost_output, cost_cache_read, cost_cache_write, cost_total)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0, 0, 0)
            """,
            (
                session_id,
                segment_msgs[0]['model_raw'],
                segment_index,
                first_ts,
                last_ts,
                len(segment_msgs),
                agg['input_tokens'],
                agg['output_tokens'],
                agg['cache_read'],
                agg['cache_write'],
                agg['total_tokens'],
            ),
        )
        segment_msgs = []
        segment_index += 1

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, start=1):
                try:
                    data = json.loads(line)
                except json.JSONDecodeError:
                    continue
                except Exception as exc:
                    insert_errors += 1
                    print(f"  Error parsing line {line_num} in {filename}: {exc}")
                    continue

                current_model = extract_codex_model(data) or current_model
                payload = data.get("payload") if isinstance(data.get("payload"), dict) else {}

                if data.get("type") == "session_meta":
                    session_id = payload.get("id") or session_id or extract_codex_session_id_from_path(filepath)
                    session_nickname = session_id[9:13] if session_id and len(session_id) >= 13 else session_id
                    created_at = payload.get("timestamp") or data.get("timestamp") or created_at
                    source_label = payload.get("source") or source_label
                    ensure_session_stub()
                    continue

                if data.get("type") != "event_msg" or payload.get("type") != "token_count":
                    continue

                info = payload.get("info") if isinstance(payload.get("info"), dict) else {}
                last_usage = info.get("last_token_usage")
                if not isinstance(last_usage, dict):
                    continue

                if not session_id:
                    session_id = extract_codex_session_id_from_path(filepath)
                    session_nickname = session_id[9:13] if session_id and len(session_id) >= 13 else session_id
                if not created_at:
                    created_at = data.get("timestamp")
                ensure_session_stub()

                model_raw = f"codex_cli/{current_model or 'unknown'}"
                timestamp = data.get("timestamp")
                msg_id = f"{session_id}:codex:{line_num}"
                insert_message_row(
                    cur,
                    msg_id,
                    session_id,
                    None,
                    timestamp,
                    model_raw,
                    {
                        'input_tokens': last_usage.get('input_tokens', 0),
                        'output_tokens': last_usage.get('output_tokens', 0),
                        'cache_read': last_usage.get('cached_input_tokens', 0),
                        'cache_write': 0,
                        'total_tokens': last_usage.get('total_tokens', 0),
                    },
                )

                if current_model_raw is not None and model_raw != current_model_raw:
                    flush_segment()
                current_model_raw = model_raw
                segment_msgs.append({
                    'timestamp': timestamp,
                    'model_raw': model_raw,
                    'input_tokens': last_usage.get('input_tokens', 0),
                    'output_tokens': last_usage.get('output_tokens', 0),
                    'cache_read': last_usage.get('cached_input_tokens', 0),
                    'cache_write': 0,
                    'total_tokens': last_usage.get('total_tokens', 0),
                })
                message_count += 1

        flush_segment()

        if insert_errors:
            print(f"  WARNING: {insert_errors} line(s) failed during parsing of {filename}")
        if session_id:
            cur.execute(
                """
                INSERT OR REPLACE INTO sessions
                (session_id, session_nickname, parent_nickname, filename, created_at,
                 channel_id, channel_name, channel_type, session_type, deleted_at)
                VALUES (?, ?, ?, ?, ?, NULL, NULL, ?, ?, NULL)
                """,
                (session_id, session_nickname, source_label, filename, created_at, "gui", "codex_cli"),
            )
            return True, message_count, session_id
    except Exception as exc:
        print(f"  Error reading {filename}: {exc}")
        return False, 0, None

    return False, 0, None


def parse_session_file(filepath, conn, source):
    logical_filename = make_file_locator(source, filepath)
    if source["name"] == "codex_cli":
        return parse_codex_session_file(filepath, conn, logical_filename)
    return parse_openclaw_session_file(filepath, conn, logical_filename)


def migrate_full(conn):
    """Full migration - rebuild entire database."""
    print("\n=== Full Migration ===")

    # Initialize schema
    init_db(conn)

    # Populate reference tables
    populate_reference_tables(conn)
    migrate_compact_schema(conn)

    sources = [source for source in get_log_sources() if source["root"].exists()]
    if not sources:
        print("ERROR: No session log directories found.")
        return

    source_files = []
    for source in sources:
        files, summary = list_source_files(source)
        print(f"\nFound {summary}")
        source_files.extend((source, filepath) for filepath in files)

    success_count = 0
    total_messages = 0

    cur = conn.cursor()
    for i, (source, filepath) in enumerate(source_files):
        if (i + 1) % 100 == 0:
            print(f"  Processing {i+1}/{len(source_files)}...")
            conn.commit()

        success, msg_count, session_id = parse_session_file(filepath, conn, source)
        if success:
            success_count += 1
            total_messages += msg_count
            # Update ingest_state so incremental runs don't re-parse these files
            st = filepath.stat()
            locator = make_file_locator(source, filepath)
            update_ingest_state(cur, locator, session_id, st.st_size, st.st_size, st.st_mtime)

    conn.commit()
    print(f"\n✓ Migrated {success_count}/{len(source_files)} sessions")
    print(f"✓ Total messages: {total_messages}")

    # Print summary stats
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM sessions")
    print(f"✓ Sessions in DB: {cur.fetchone()[0]}")
    cur.execute("SELECT COUNT(*) FROM messages")
    print(f"✓ Messages in DB: {cur.fetchone()[0]}")
    cur.execute("SELECT COUNT(DISTINCT model_ref_id) FROM messages")
    print(f"✓ Unique models: {cur.fetchone()[0]}")


def get_file_last_timestamp(filepath):
    """Get the last message timestamp from a session file."""
    try:
        with open(filepath, 'rb') as f:
            # Read last few KB to find last message
            f.seek(0, 2)
            file_size = f.tell()
            # Read last 8KB
            read_size = min(8192, file_size)
            f.seek(file_size - read_size)
            lines = f.read().decode('utf-8', errors='ignore').strip().split('\n')
            for line in reversed(lines):
                if line.strip():
                    msg = json.loads(line)
                    if msg.get('type') == 'message' and msg.get('timestamp'):
                        return msg['timestamp']
    except Exception as e:
        pass
    return None


def migrate_incremental(conn):
    """Incremental migration (append-only).

    The SQLite DB is the long-term archive.
    Session logs are an ingest stream which may be deleted/rotated later.

    Rules:
    - NEVER delete messages that have been ingested.
    - Only read appended bytes from each session file based on ingest_state.
    - Recompute derived segments from archived messages.
    """
    print("\n=== Incremental Migration (append-only) ===")

    # Keep reference tables current even without a full rebuild
    populate_reference_tables(conn)

    cur = conn.cursor()

    # If schema changed (new ingest_state), ensure it's present
    init_db(conn)
    ensure_pragmas(conn)
    migrate_compact_schema(conn)

    sources = [source for source in get_log_sources() if source["root"].exists()]
    if not sources:
        print("ERROR: No session log directories found.")
        return

    to_process = []  # (source, filepath, start_offset)

    for source in sources:
        files, summary = list_source_files(source)
        print(f"Found {summary}")
        for filepath in files:
            locator = make_file_locator(source, filepath)
            st = filepath.stat()
            state = get_ingest_state(cur, locator)

            if not state:
                to_process.append((source, filepath, 0))
                continue

            last_offset = int(state.get('last_offset') or 0)
            last_mtime = state.get('last_mtime')

            # File got smaller -> truncated/rewritten; re-ingest from start (upsert is safe)
            if st.st_size < last_offset:
                to_process.append((source, filepath, 0))
                continue

            # Normal append-only path
            if st.st_size > last_offset:
                to_process.append((source, filepath, last_offset))
                continue

            # If size is unchanged but mtime jumped, be conservative: re-scan from start
            if last_mtime is not None and st.st_mtime > float(last_mtime) and st.st_size == last_offset:
                to_process.append((source, filepath, 0))

    print(f"Found {len(to_process)} session files needing ingest")
    if not to_process:
        print("✓ Database is up to date")
        return

    total_processed_msgs = 0
    processed_files = 0

    for source, filepath, start_offset in to_process:
        locator = make_file_locator(source, filepath)
        st = filepath.stat()

        # We need a session_id to attach messages when ingesting from an offset.
        state = get_ingest_state(cur, locator)
        known_session_id = state.get('session_id') if state else None

        # If we don't know session_id, or we're re-ingesting from 0, parse full file.
        if start_offset == 0 or not known_session_id:
            success, msg_count, session_id = parse_session_file(filepath, conn, source)
            if not success or not session_id:
                continue

            # segments in parse_session_file are based only on this pass; recompute from DB to be safe
            recompute_segments(conn, session_id)

            update_ingest_state(cur, locator, session_id, st.st_size, st.st_size, st.st_mtime)
            processed_files += 1
            total_processed_msgs += msg_count
            continue

        # Append-only ingest for existing sessions
        processed = ingest_messages_from_offset(filepath, conn, start_offset, known_session_id, source)
        recompute_segments(conn, known_session_id)
        update_ingest_state(cur, locator, known_session_id, st.st_size, st.st_size, st.st_mtime)

        processed_files += 1
        total_processed_msgs += processed

        # Also update deleted/reset marker purely from filename (no log dependency)
        is_deleted, deleted_ts = detect_deleted(os.path.basename(filepath))
        if is_deleted:
            cur.execute("UPDATE sessions SET deleted_at = COALESCE(deleted_at, ?) WHERE session_id = ?", (deleted_ts, known_session_id))

    conn.commit()
    print(f"✓ Processed {processed_files} files")
    print(f"✓ Assistant messages ingested/upserted: {total_processed_msgs}")



def parse_args():
    ap = argparse.ArgumentParser(description="Ingest OpenClaw and Codex CLI session logs into an AI Cost Tracker SQLite database.")
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--full", action="store_true", help="Rebuild the database from scratch.")
    mode.add_argument("--incremental", action="store_true", help="Append new data into an existing database (default).")
    ap.add_argument("--db", default=str(DB_PATH), help="SQLite database path (default: %(default)s)")
    ap.add_argument("--schema", default=str(SCRIPT_DIR / 'schema.sql'), help="Schema SQL path (default: %(default)s)")
    ap.add_argument("--sessions-dir", default=str(SESSIONS_DIR), help="Directory containing OpenClaw session .jsonl files.")
    ap.add_argument("--codex-sessions-dir", default=str(CODEX_SESSIONS_DIR), help="Directory containing Codex CLI session logs.")
    ap.add_argument("--channel-mapping", default=None, help="Optional JSON file mapping channel IDs to names.")
    ap.add_argument("--backups-dir", default=str(BACKUPS_DIR), help="Directory for rolling SQLite backups.")
    return ap.parse_args()


def main():
    global DB_PATH, SESSIONS_DIR, CODEX_SESSIONS_DIR, BACKUPS_DIR, CHANNEL_MAPPING_PATH

    args = parse_args()
    DB_PATH = Path(args.db).expanduser().resolve()
    SESSIONS_DIR = Path(args.sessions_dir).expanduser().resolve()
    CODEX_SESSIONS_DIR = Path(args.codex_sessions_dir).expanduser().resolve()
    BACKUPS_DIR = Path(args.backups_dir).expanduser().resolve()
    CHANNEL_MAPPING_PATH = Path(args.channel_mapping).expanduser().resolve() if args.channel_mapping else None

    # init_db reads schema.sql relative to SCRIPT_DIR, so keep the public package self-contained.
    schema_path = Path(args.schema).expanduser().resolve()
    if schema_path != (SCRIPT_DIR / 'schema.sql').resolve():
        print(f"WARNING: --schema is currently informational only; using packaged schema at {SCRIPT_DIR / 'schema.sql'}")

    load_channel_mapping(CHANNEL_MAPPING_PATH)

    print("AI Cost Tracker migration")
    print(f"Database: {DB_PATH}")
    print(f"OpenClaw sessions: {SESSIONS_DIR}")
    print(f"Codex sessions: {CODEX_SESSIONS_DIR}")
    if CHANNEL_MAPPING_PATH:
        print(f"Channel mapping: {CHANNEL_MAPPING_PATH}")

    if not any(source["root"].exists() for source in get_log_sources()):
        print("ERROR: No session directories found.")
        print(f"  OpenClaw: {SESSIONS_DIR}")
        print(f"  Codex: {CODEX_SESSIONS_DIR}")
        sys.exit(1)

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    BACKUPS_DIR.mkdir(parents=True, exist_ok=True)

    db_existed = DB_PATH.exists()
    maybe_backup_db(DB_PATH)

    conn = sqlite3.connect(DB_PATH)
    init_db(conn)
    ensure_pragmas(conn)

    try:
        if args.full or (not args.incremental and not db_existed):
            migrate_full(conn)
        else:
            migrate_incremental(conn)
    finally:
        conn.close()

    print("\n✓ Migration complete")


if __name__ == "__main__":
    main()
