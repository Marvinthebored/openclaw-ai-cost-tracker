#!/usr/bin/env python3
"""
Migration script for AI Cost Tracker v3.
Parses OpenClaw session logs and populates SQLite database.

Usage:
    python3 migrate_sessions.py [--full] [--incremental]

    --full        Rebuild entire database from scratch
    --incremental Only add new sessions (default)
"""

from __future__ import annotations

import argparse
import sqlite3
import json
import os
import re
import sys
import time
import shutil
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Paths
SCRIPT_DIR = Path(__file__).resolve().parent
DB_PATH = Path(os.environ.get("COST_TRACKER_DB_PATH", SCRIPT_DIR / "cost_tracker.db"))
SESSIONS_DIR = Path.home() / ".openclaw" / "agents" / "main" / "sessions"
SESSIONS_INDEX_PATH = SESSIONS_DIR / "sessions.json"
CODEX_SESSIONS_DIR = Path.home() / ".codex" / "sessions"
OPENCLAW_AGENTS_DIR = Path.home() / ".openclaw" / "agents"
OPENCLAW_SESSION_SOURCES = []
CODEX_SESSION_SOURCE = {"key": "codex_cli", "label": "Codex CLI", "dir": CODEX_SESSIONS_DIR}
KIMI_CODE_DIR = Path.home() / ".kimi-code"
KIMI_SESSION_SOURCE = {"key": "kimi_code", "label": "Kimi Code", "dir": KIMI_CODE_DIR}

# Codex app-server (OpenClaw harness) homes. OpenClaw runs the app-server with
# CODEX_HOME=<agent>/agent/codex-home, so its rollout-*.jsonl transcripts do NOT
# land in the classic ~/.codex/sessions tree. These rollouts carry the real
# per-turn token usage (event_msg/token_count) that OpenClaw session logs mostly
# record as zeros, so they are the authoritative usage source for codex-harness
# turns.
CODEX_HOME_GLOB = OPENCLAW_AGENTS_DIR

# Rollout ingestion from agent codex-home dirs. Added 2026-07-24 because
# OpenClaw session storage does not yet contain complete usage for every
# codex-harness turn. OpenClaw 7.2 records substantially more real usage, but
# measured 2026-07-30 coverage is still incomplete. Keep both sources enabled;
# reconcile_usage_duplicates() excludes only proved one-to-one twins.
CODEX_HOME_INGEST_ENABLED = True

def codex_app_server_roots():
    """Return [{'agent': name, 'dir': sessions-dir}] for each agent codex-home."""
    roots = []
    if not CODEX_HOME_INGEST_ENABLED:
        return roots
    if CODEX_HOME_GLOB.exists():
        for agent_dir in sorted(CODEX_HOME_GLOB.iterdir()):
            sessions = agent_dir / "agent" / "codex-home" / "sessions"
            if sessions.is_dir():
                roots.append({"agent": agent_dir.name, "dir": sessions})
    return roots

def codex_app_server_root_for(filepath):
    """Return the codex-home root dict containing filepath, else None."""
    fp = Path(filepath).resolve()
    for root in codex_app_server_roots():
        try:
            fp.relative_to(root["dir"].resolve())
            return root
        except ValueError:
            continue
    return None
CLAUDE_PROJECTS_DIR = Path.home() / ".claude" / "projects"
CHANNEL_MAPPING_PATH = None
BACKUPS_DIR = SCRIPT_DIR / "backups"
CODEX_THREAD_ID_CACHE_PATH = SCRIPT_DIR / ".codex_thread_id_cache.json"
CODEX_APP_SERVER_IDEMPOTENCY_RE = re.compile(
    r"^codex-app-server:"
    r"(?P<thread_id>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}):"
    r"(?P<turn_id>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}):"
    r"(?P<role>user|assistant):(?P<index>\d+)$"
)
CODEX_APP_SERVER_THREAD_RE = re.compile(r"^codex-app-server:(?P<thread_id>[0-9a-f-]{36}):")
_OPENCLAW_CODEX_THREAD_IDS_CACHE = None
_OPENCLAW_SESSION_INDEX_CACHE = None

CHANNEL_MAPPING = {"channels": {}}
CHANNEL_ID_TO_NAME = {}


def load_channel_mapping(path):
    """Load an optional channel-id mapping without making it a requirement."""
    global CHANNEL_MAPPING, CHANNEL_ID_TO_NAME
    CHANNEL_MAPPING = {"channels": {}}
    CHANNEL_ID_TO_NAME = {}
    if not path:
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            CHANNEL_MAPPING = json.load(f)
        CHANNEL_ID_TO_NAME = {
            str(cid): (
                str(data.get("name"))
                if str(data.get("name") or "").startswith("#")
                else f"#{data.get('name')}"
            )
            for cid, data in CHANNEL_MAPPING.get("channels", {}).items()
            if data.get("name")
        }
    except FileNotFoundError:
        print(f"WARNING: Channel mapping file not found: {path}")
    except Exception as exc:
        print(f"WARNING: Failed to load channel mapping {path}: {exc}")


def discover_openclaw_session_sources():
    """Return every agent JSONL sessions directory, keeping main compatible."""
    sources = []
    seen = set()

    def add(key, directory):
        directory = Path(directory).expanduser().resolve()
        if directory in seen:
            return
        seen.add(directory)
        sources.append({"key": key, "label": f"OpenClaw {key}", "dir": directory})

    add("main", SESSIONS_DIR)
    if OPENCLAW_AGENTS_DIR.exists():
        for agent_dir in sorted(OPENCLAW_AGENTS_DIR.iterdir()):
            if agent_dir.is_dir():
                add(agent_dir.name, agent_dir / "sessions")
    return sources

# Model reference initial data
MODEL_REFERENCE_DATA = [
    ("openai/gpt-5.2", "openai", "OpenAI", "gpt-5.2", 1.0),
    ("openai/gpt-5.2-codex", "openai", "OpenAI", "gpt-5.2-c", 1.0),
    ("openai/gpt-5.3-codex", "openai", "OpenAI", "gpt-5.3-c", 1.0),
    ("openai/gpt-5.4", "openai", "OpenAI", "gpt-5.4", 1.0),
    ("openai/gpt-5.4-codex", "openai", "OpenAI", "gpt-5.4-codex", 1.0),
    ("openai/gpt-5.4-mini", "openai", "OpenAI", "gpt-5.4-mini", 1.0),
    ("openai/gpt-5.5", "openai", "OpenAI", "gpt-5.5", 1.0),
    ("openai-codex/gpt-5.2", "openai-codex", "OpenAI", "gpt-5.2", 1.0),
    ("openai-codex/gpt-5.2-codex", "openai-codex", "OpenAI", "gpt-5.2-c", 1.0),
    ("openai-codex/gpt-5.3-codex", "openai-codex", "OpenAI", "gpt-5.3-c", 1.0),

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
    ("claude/claude-opus-4-6", "claude", "Anthropic", "opus-4.6", 1.0),
    ("claude/claude-opus-4-5", "claude", "Anthropic", "opus-4.5", 1.0),
    ("claude/claude-sonnet-4-6", "claude", "Anthropic", "sonnet-4.6", 1.0),
    ("claude/claude-sonnet-4-5", "claude", "Anthropic", "sonnet-4.5", 1.0),

    ("modal/zai-org/GLM-5-FP8", "modal", "Z.AI", "glm-5", 1.0),
    ("zai/glm-5", "zai", "Z.AI", "glm-5", 1.0),
    ("ollama-cloud/glm-5", "ollama", "Z.AI", "glm-5", 1.0),
    ("opencode/glm-5-free", "opencode", "Z.AI", "glm-5-free", 1.0),

    ("moonshot/kimi-k2.5", "moonshot", "Moonshot", "kimi-k2.5", 1.0),
    ("kimi-coding/k2p5", "kimi-code", "Moonshot", "k2p5", 1.0),
    ("kimi-code/k3-256k", "kimi-code", "Moonshot", "k3-256k", 1.0),

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

PAYG_RATECARD_PATH = SCRIPT_DIR / "ref" / "payg_model_rates.tsv"
_PAYG_RATECARD_CACHE = None

# Pioneer input handling.
#
# HISTORY: Pioneer's anthropic-compatible streaming reports input_tokens as a
# grossly-inflated gross-prompt count (e.g. ~49k) while real billable
# non-cached input is ~1200/message. We previously OVERRODE input to a flat
# 1200 at INGEST time (destructive: raw was lost).
#
# CHANGE 2026-06-11 : store RAW Pioneer session-log data in the DB so we
# can see whether Pioneer ever fixes their API. The 1200 estimate now lives in
# the VIEWER as a per-model toggle (override applied locally, not persisted).
# These constants are kept so the viewer/ratecard layer can reference the
# canonical estimate value. The ingest-time override is OFF by default.
PIONEER_INPUT_ESTIMATE_START_DATE = "2026-06-05"
PIONEER_ESTIMATED_INPUT_TOKENS = 1200
PIONEER_INPUT_ESTIMATE_SOURCE = "pioneer_input_estimate_1200"
# Master switch: when False, raw Pioneer input/cost is stored unchanged and the
# viewer toggle owns the 1200 override. Do NOT re-enable without removing the
# viewer-side toggle, or the two overrides will compound.
PIONEER_INGEST_OVERRIDE_ENABLED = False


def is_pioneer_estimate_model(model_raw: str) -> bool:
    """True for pioneer/ opus|sonnet|haiku refs that get the flat input estimate.

    Scope is intentionally narrow: only the `pioneer/` provider (NOT
    `pioneer-ai/`), and only Claude opus/sonnet/haiku model families.
    """
    if not model_raw.startswith("pioneer/"):
        return False
    tail = model_raw.split("/", 1)[1].lower()
    return any(fam in tail for fam in ("opus", "sonnet", "haiku"))


def _to_float(value, default=0.0):
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def load_payg_ratecard():
    """Load owned PAYG-equivalent token rates from ref/payg_model_rates.tsv."""
    global _PAYG_RATECARD_CACHE
    if _PAYG_RATECARD_CACHE is not None:
        return _PAYG_RATECARD_CACHE

    rates = {}
    if PAYG_RATECARD_PATH.exists():
        with open(PAYG_RATECARD_PATH, "r", encoding="utf-8") as f:
            header = None
            for raw_line in f:
                line = raw_line.rstrip("\n")
                if not line or line.startswith("#"):
                    continue
                parts = line.split("\t")
                if header is None:
                    header = parts
                    continue
                row = dict(zip(header, parts))
                model_raw = (row.get("model_raw") or "").strip()
                if not model_raw:
                    continue
                rates[model_raw] = {
                    "input": _to_float(row.get("input_per_mtok")),
                    "output": _to_float(row.get("output_per_mtok")),
                    "cache_read": _to_float(row.get("cache_read_per_mtok")),
                    "cache_write": _to_float(row.get("cache_write_per_mtok")),
                }
    _PAYG_RATECARD_CACHE = rates
    return rates


# Terminal-owned (hand-run CLI) sessions carry an unambiguous `cli-` prefix so a
# model_raw says who OWNED the session, not merely which log it came from. `claude/` is genuinely mixed -- the same ~/.claude/projects tree
# holds both hand-run and OpenClaw-driven (sdk-*) transcripts -- so the rewrite
# is conditional on session kind, never a blanket string replace.
CLI_MODEL_PREFIXES = {
    "claude/": "cli-claude/",
    "codex/": "cli-codex/",
    "kimi-code/": "cli-kimi/",
}
# The other half of the same rule (ownership convention): if a Claude-family row is
# NOT terminal-owned it is OpenClaw-owned, and its provider is Anthropic. The
# `(c)` marks that OpenClaw reached Anthropic through the Claude CLI credential
# rather than the API directly -- a routing detail, not an owner.
OC_CLAUDE_PREFIXES = {
    "claude/": "anthropic-c/",
    "claude-cli/": "anthropic/",
}
# Reverse map: a renamed prefix resolves to the ratecard entry it came from.
CLI_RATECARD_FALLBACK = {
    "cli-claude/": "claude/",
    "cli-codex/": "codex_cli/",
    "cli-kimi/": "kimi-code/",
    "anthropic-c/": "claude/",
}


def cli_prefixed_model_raw(model_raw: str, session_kind: str) -> str:
    """Rewrite a model_raw so its prefix names the session's OWNER.

    Terminal-owned -> `cli-<tool>/`; OpenClaw-owned Claude -> `anthropic[(c)]/`.
    `claude/` is genuinely mixed (the same ~/.claude/projects tree holds hand-run
    and OpenClaw-driven transcripts), so ownership comes from session kind, never
    from the string. Idempotent: an already-rewritten prefix matches nothing.
    """
    if not model_raw:
        return model_raw
    table = CLI_MODEL_PREFIXES if session_kind == "cli" else OC_CLAUDE_PREFIXES
    for old, new in table.items():
        if model_raw.startswith(old):
            return new + model_raw[len(old):]
    return model_raw


def calculate_payg_equivalent_cost(model_raw: str, usage: dict):
    """Return token PAYG-equivalent cost for CLI rows, or None if no rate exists.

    This is intentionally separate from the dashboard's subscription/pro-rata
    EST COST fields. It fills the normal cost columns used for token-rate
    normalized usage totals. Existing non-zero provider/OpenClaw costs win.
    """
    rates = load_payg_ratecard()
    rate = rates.get(model_raw)
    if not rate:
        # cli-<x>/ is an ownership label, not a second provider: the token rates
        # are the ones already carded under the pre-rename prefix.
        for cli_prefix, card_prefix in CLI_RATECARD_FALLBACK.items():
            if model_raw.startswith(cli_prefix):
                model_name = model_raw[len(cli_prefix):]
                rate = rates.get(f"{card_prefix}{model_name}")
                if not rate and cli_prefix == "cli-codex/":
                    rate = rates.get(f"openai/{model_name}")
                break
    if not rate and model_raw.startswith(("claude-cli/", "anthropic/")):
        # Anthropic token rates do not depend on how OpenClaw reached them, so
        # both the API and Claude-CLI credential routes price off the same card.
        _, model_name = model_raw.split("/", 1)
        rate = rates.get(f"claude/{model_name}")
    if not rate and model_raw.startswith(("codex/", "openai-codex/")):
        _, model_name = model_raw.split("/", 1)
        rate = rates.get(f"openai/{model_name}") or rates.get(f"codex_cli/{model_name}")
    if not rate and model_raw.startswith("pioneer-ai/"):
        # Legacy alias: pioneer-ai/<model> uses the same Pioneer ratecard entry
        # as pioneer/<model> (Pioneer is ~10% above Anthropic base on
        # input/output/cacheRead, so do NOT fall back to claude/<model>).
        _, model_name = model_raw.split("/", 1)
        rate = rates.get(f"pioneer/{model_name}")
    if not rate:
        return None
    cost = {
        "input": (usage.get("input_tokens", 0) or 0) * rate["input"] / 1_000_000,
        "output": (usage.get("output_tokens", 0) or 0) * rate["output"] / 1_000_000,
        "cacheRead": (usage.get("cache_read", 0) or 0) * rate["cache_read"] / 1_000_000,
        "cacheWrite": (usage.get("cache_write", 0) or 0) * rate["cache_write"] / 1_000_000,
    }
    cost["total"] = cost["input"] + cost["output"] + cost["cacheRead"] + cost["cacheWrite"]
    return cost


def maybe_calculate_cli_cost(model_raw: str, usage: dict, cost=None):
    """Use source/provider cost if non-zero; otherwise calculate CLI PAYG-equivalent cost."""
    cost = cost or {}
    if any((cost.get(k) or 0) for k in ("input", "output", "cacheRead", "cacheWrite", "total")):
        return cost
    if model_raw.startswith(("codex_cli/", "claude/", "claude-cli/", "openai/", "openai-codex/", "codex/", "pioneer/", "pioneer-ai/", "kimi-code/", "zai/", "cli-claude/", "cli-codex/", "cli-kimi/", "anthropic/", "anthropic-c/")):
        calculated = calculate_payg_equivalent_cost(model_raw, usage)
        if calculated is not None:
            return calculated
    return cost


def apply_pioneer_input_estimate(model_raw: str, timestamp: str | None, usage: dict, cost=None):
    """Override Pioneer streaming input tokens with a flat estimate.

    Pioneer's streamed usage reports input tokens unreliably (zero, or grossly
    inflated gross-prompt). For legacy compatibility, for the `pioneer/`
    opus|sonnet|haiku rows we replace input with a flat ~1200/message estimate
    (derived from a day of Pioneer web usage) on EVERY matching line from
    2026-06-05 onward. pioneer-ai/ and other providers are not touched.
    """
    if not PIONEER_INGEST_OVERRIDE_ENABLED:
        # Override now lives in the viewer (per-model toggle). Store raw.
        return usage, cost or {}, None, None, 0
    if not is_pioneer_estimate_model(model_raw):
        return usage, cost or {}, None, None, 0
    if not timestamp or str(timestamp)[:10] < PIONEER_INPUT_ESTIMATE_START_DATE:
        return usage, cost or {}, None, None, 0

    adjusted = dict(usage)
    adjusted["input_tokens"] = PIONEER_ESTIMATED_INPUT_TOKENS
    adjusted["total_tokens"] = (
        (adjusted.get("input_tokens", 0) or 0)
        + (adjusted.get("output_tokens", 0) or 0)
        + (adjusted.get("cache_read", 0) or 0)
        + (adjusted.get("cache_write", 0) or 0)
    )
    calculated = calculate_payg_equivalent_cost(model_raw, adjusted) or cost or {}
    return adjusted, calculated, PIONEER_INPUT_ESTIMATE_SOURCE, "payg_estimated_input", 1


def ensure_cost_metadata_columns(conn):
    """Add optional cost and ingestion provenance columns."""
    for table in ("messages", "segments"):
        existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        if "cost_source" not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN cost_source TEXT")
        if "cost_kind" not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN cost_kind TEXT")
        if "cost_is_estimated" not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN cost_is_estimated INTEGER DEFAULT 0")
    message_columns = {row[1] for row in conn.execute("PRAGMA table_info(messages)")}
    if "ingest_source" not in message_columns:
        conn.execute("ALTER TABLE messages ADD COLUMN ingest_source TEXT")
    if "duplicate_of" not in message_columns:
        conn.execute("ALTER TABLE messages ADD COLUMN duplicate_of TEXT")
    if "excluded_reason" not in message_columns:
        conn.execute("ALTER TABLE messages ADD COLUMN excluded_reason TEXT")
    if "is_excluded" not in message_columns:
        conn.execute(
            "ALTER TABLE messages ADD COLUMN is_excluded INTEGER NOT NULL DEFAULT 0"
        )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_messages_ingest_source ON messages(ingest_source)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_messages_excluded ON messages(is_excluded)"
    )
    conn.commit()


def backfill_cli_payg_costs(conn, session_ids=None, since_timestamp=None):
    """Backfill zero-cost CLI-style messages from token usage + owned ratecard.

    If session_ids is provided (incremental refresh), only those sessions are
    scanned. If since_timestamp is provided, only messages at or after that
    timestamp are scanned. With neither filter, the whole DB is scanned.
    """
    ensure_cost_metadata_columns(conn)
    cur = conn.cursor()
    scope_ids = None
    if session_ids is not None:
        scope_ids = [s for s in session_ids if s]
        if not scope_ids:
            return 0  # nothing changed this run
    base_sql = """SELECT m.id, m.session_id, mr.model_raw, m.input_tokens, m.output_tokens,
                  m.cache_read, m.cache_write, m.total_tokens,
                  m.cost_input, m.cost_output, m.cost_cache_read, m.cost_cache_write, m.cost_total
           FROM effective_messages m
           JOIN model_reference mr ON mr.id = m.model_ref_id
           WHERE (mr.model_raw LIKE 'codex_cli/%' OR mr.model_raw LIKE 'claude/%'
                  OR mr.model_raw LIKE 'claude-cli/%'
                  OR mr.model_raw LIKE 'openai/%' OR mr.model_raw LIKE 'openai-codex/%'
                  OR mr.model_raw LIKE 'codex/%'
                  OR mr.model_raw LIKE 'pioneer/%' OR mr.model_raw LIKE 'pioneer-ai/%'
                  OR mr.model_raw LIKE 'kimi-code/%' OR mr.model_raw LIKE 'zai/%'
                  OR mr.model_raw LIKE 'cli-%'
                  OR mr.model_raw LIKE 'anthropic%')
             AND COALESCE(m.cost_total, 0) = 0"""
    params = []
    if since_timestamp is not None:
        base_sql += " AND m.timestamp >= ?"
        params.append(since_timestamp)
    if scope_ids is not None:
        placeholders = ",".join("?" * len(scope_ids))
        base_sql += f" AND m.session_id IN ({placeholders})"
        params.extend(scope_ids)
    rows = cur.execute(base_sql, params).fetchall()
    updated = 0
    touched_sessions = set()
    missing_rates = set()
    for row in rows:
        msg_id, session_id, model_raw = row[0], row[1], row[2]
        usage = {
            "input_tokens": row[3] or 0,
            "output_tokens": row[4] or 0,
            "cache_read": row[5] or 0,
            "cache_write": row[6] or 0,
            "total_tokens": row[7] or 0,
        }
        cost = calculate_payg_equivalent_cost(model_raw, usage)
        if cost is None:
            missing_rates.add(model_raw)
            continue
        if not cost.get("total"):
            continue
        cur.execute(
            """UPDATE messages
               SET cost_input = ?, cost_output = ?, cost_cache_read = ?, cost_cache_write = ?, cost_total = ?,
                   cost_source = 'ratecard', cost_kind = 'payg_equivalent', cost_is_estimated = 0
               WHERE id = ? AND COALESCE(cost_total, 0) = 0""",
            (cost["input"], cost["output"], cost["cacheRead"], cost["cacheWrite"], cost["total"], msg_id),
        )
        if cur.rowcount:
            updated += 1
            if session_id:
                touched_sessions.add(session_id)

    for session_id in touched_sessions:
        recompute_segments(conn, session_id)
    seg_sql = """UPDATE segments
           SET cost_source = 'ratecard', cost_kind = 'payg_equivalent', cost_is_estimated = 0
           WHERE session_id IN (SELECT DISTINCT m.session_id
                                FROM messages m JOIN model_reference mr ON mr.id = m.model_ref_id
                                WHERE mr.model_raw LIKE 'codex_cli/%' OR mr.model_raw LIKE 'claude/%'
                                   OR mr.model_raw LIKE 'claude-cli/%'
                                   OR mr.model_raw LIKE 'openai/%' OR mr.model_raw LIKE 'openai-codex/%'
                                   OR mr.model_raw LIKE 'codex/%'
                                   OR mr.model_raw LIKE 'pioneer/%' OR mr.model_raw LIKE 'pioneer-ai/%'
                                   OR mr.model_raw LIKE 'kimi-code/%' OR mr.model_raw LIKE 'zai/%'
                                   OR mr.model_raw LIKE 'cli-%'
                                   OR mr.model_raw LIKE 'anthropic%')
             AND COALESCE(cost_total, 0) > 0
             AND cost_source IS NULL"""
    if scope_ids is not None:
        placeholders = ",".join("?" * len(scope_ids))
        cur.execute(seg_sql + f" AND session_id IN ({placeholders})", scope_ids)
    else:
        cur.execute(seg_sql)
    conn.commit()
    if missing_rates:
        print(f"  PAYG ratecard missing for: {', '.join(sorted(missing_rates))}")
    print(f"✓ Backfilled CLI PAYG-equivalent costs for {updated} messages across {len(touched_sessions)} sessions")
    return updated


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
    ensure_cost_metadata_columns(conn)
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

def best_guess_model_display_name(model_raw: str) -> str:
    """Return a compact initial display label for a newly seen raw model.

    This is insertion-time inference only. Once a row exists, model_reference
    is authoritative and its editable aliases must never be rewritten here.
    """
    if not model_raw:
        return "unknown"
    model = model_raw.split("/", 1)[1] if "/" in model_raw else model_raw

    # Aggregators often embed vendor/account/router paths in the identifier.
    # The endpoint column already preserves that route; the model label only
    # needs the terminal product name.
    model = model.rsplit("/", 1)[-1]

    label = model.strip()
    lower = label.lower()

    # Claude route names carry a redundant vendor prefix.
    if lower.startswith("claude-"):
        lower = lower[len("claude-"):]
    lower = re.sub(
        r"^(opus|sonnet|haiku|fable)-(\d+)-(\d+)(.*)$",
        r"\1-\2.\3\4",
        lower,
    )

    # Kimi product/transport suffixes are not useful grouping dimensions.
    if lower.startswith("kimi-for-coding"):
        return lower
    kimi_name = lower.removeprefix("kimi-")
    if re.search(r"(?:^|-)k?2(?:p|[.-])?5(?:-|$)", kimi_name) or kimi_name == "k2p5":
        return "k2.5"
    if re.search(r"(?:^|-)k?2(?:p|[.-])?6(?:-|$)", kimi_name) or kimi_name == "k2p6":
        return "k2.6"
    if re.search(r"(?:^|-)k?2(?:p|[.-])?7(?:-|$)", kimi_name) or kimi_name == "k2p7":
        return "k2.7"
    if re.search(r"(?:^|-)k?3(?:-|$)", kimi_name):
        return "k3"
    if kimi_name.startswith("k2"):
        return "k2"

    # Normalise casing and vendor path residue for the remaining families.
    return lower


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
        endpoint, author = "openai-codex", "OpenAI"
    elif provider in ("codex", "cli-codex"):
        # cli-codex/ is codex/ with terminal ownership made explicit; same endpoint.
        endpoint, author = "codex", "OpenAI"
    elif provider == "openai":
        endpoint, author = "openai", "OpenAI"
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
    elif provider in ("anthropic", "anthropic-c"):
        # anthropic-c is OpenClaw reaching Anthropic through the Claude CLI
        # credential rather than the API. That is a routing detail, so it keeps
        # the same endpoint -- one provider in the dashboard, not two.
        endpoint, author = "anthropic", "Anthropic"
    elif provider in ("claude", "claude-cli", "cli-claude"):
        # claude-cli/X is the OpenClaw wrapper's counters for the same Anthropic
        # model that claude/X reads from Claude's own transcript. The prefix
        # records which log the row came from, not a second provider -- every
        # rate lookup below already folds them together. Without this, the
        # yellow provider column (model_reference.endpoint) split one endpoint
        # into CLAUDE and CLAUDE-CLI for identical OpenClaw-owned work.
        endpoint, author = "claude", "Anthropic"
    elif provider in ("kimi-coding", "kimi-code", "kimi", "moonshot", "cli-kimi"):
        endpoint, author = ("moonshot" if provider == "moonshot" else "kimi-code"), "Moonshot"
    elif provider in ("bailian", "bailian-token-plan"):
        endpoint, author = "bailian", "Alibaba"
    elif provider in ("zai", "modal"):
        endpoint, author = provider, "Z.AI"
    elif provider in ("opencode", "ollama-cloud"):
        endpoint, author = "opencode", "Z.AI"
    elif provider == "opencode-go":
        endpoint = "opencode"
        ml = model.lower()
        if "kimi" in ml:
            author = "Moonshot"
        elif "qwen" in ml:
            author = "Alibaba"
        elif "glm" in ml:
            author = "Z.AI"
    elif provider == "fireworks-ai":
        endpoint = "fireworks-ai"
        model = model.rsplit("/", 1)[-1]
        ml = model.lower()
        if ml.startswith("kimi-"):
            author = "Moonshot"
        elif ml.startswith("glm-"):
            author = "Z.AI"
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

    if provider == "codex_cli":
        endpoint = "openai-codex"
    return (
        endpoint,
        author,
        best_guess_model_display_name(model_raw),
        pru_multiplier,
    )


def ensure_model_reference(cur, model_raw: str):
    """Insert a best-guess row for an unseen model without touching existing aliases."""
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


def extract_codex_session_id_from_path(filepath: Path):
    match = re.search(r'([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})', filepath.name)
    return match.group(1) if match else None


def extract_codex_thread_id_from_idempotency_key(value):
    if not isinstance(value, str):
        return None
    match = CODEX_APP_SERVER_THREAD_RE.match(value)
    return match.group("thread_id") if match else None


def codex_is_openclaw_runtime(originator, in_agent_home=False):
    # An explicit standalone launcher wins over the shared storage directory.
    return originator == "openclaw" if originator else bool(in_agent_home)


def read_codex_session_meta(filepath: Path):
    """Return the first Codex session_meta payload without reading the transcript."""
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    data = json.loads(line)
                except Exception:
                    continue
                if data.get("type") == "session_meta":
                    payload = data.get("payload")
                    return payload if isinstance(payload, dict) else {}
    except Exception:
        pass
    return {}


def read_claude_entrypoint(filepath: Path):
    """Return a Claude transcript's `entrypoint` without reading the whole file.

    Claude records the launcher on every line; the Codex analogue is
    `session_meta.payload.originator`.
    """
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f):
                if line_num > 40:
                    break
                try:
                    data = json.loads(line)
                except Exception:
                    continue
                if isinstance(data, dict) and data.get("entrypoint"):
                    return data["entrypoint"]
    except Exception:
        pass
    return None


CLAUDE_SDK_ENTRYPOINTS = ("sdk-cli", "sdk-ts", "sdk-py")


def claude_session_kind(entrypoint):
    """Claude transcripts record their launcher in `entrypoint`; this is the
    Claude analogue of Codex's `originator`.

    Any `sdk-*` entrypoint is an embedding harness (OpenClaw here) driving
    Claude programmatically -- `sdk-ts` is the older TypeScript agent SDK,
    `sdk-cli` the current one. Bare 'cli' is a hand-run terminal session.
    Unknown/missing stays 'cli': do not promote a session to OpenClaw on a
    guess.
    """
    return "openclaw" if entrypoint in CLAUDE_SDK_ENTRYPOINTS else "cli"


def codex_filepath_from_storage_name(storage_name: str):
    """Resolve a tracker codex_cli:* storage key back to its transcript path."""
    if not isinstance(storage_name, str) or not storage_name.startswith("codex_cli:"):
        return None
    app_prefix = "codex_cli:codex-home/"
    if storage_name.startswith(app_prefix):
        rest = storage_name[len(app_prefix):]
        agent, sep, rel = rest.partition("/")
        if not sep or not agent or not rel:
            return None
        return OPENCLAW_AGENTS_DIR / agent / "agent" / "codex-home" / "sessions" / rel
    return CODEX_SESSIONS_DIR / storage_name[len("codex_cli:"):]


def extract_codex_session_id_from_meta(data):
    """Codex CLI stores its durable session/thread id in session_meta.payload.id."""
    if data.get("type") != "session_meta":
        return None
    payload = data.get("payload") if isinstance(data.get("payload"), dict) else {}
    value = payload.get("id")
    return value if isinstance(value, str) and value else None


def extract_codex_session_id_from_file(filepath: Path):
    """Read just enough of a Codex JSONL file to get its explicit session id."""
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    data = json.loads(line)
                except Exception:
                    continue
                session_id = extract_codex_session_id_from_meta(data)
                if session_id:
                    return session_id
    except Exception:
        pass
    return extract_codex_session_id_from_path(filepath)


def collect_openclaw_codex_thread_ids(force_refresh=False):
    """Explicit Codex thread/session ids already represented in OpenClaw logs.

    OpenClaw's codex-app-server bridge writes the Codex thread id in several
    places:
    - normal session messages: message.idempotencyKey =
      codex-app-server:<threadId>:<turnId>:<role>:<n>
    - trajectory sidecars: data.threadId / data.turnId
    - app-server sidecar JSON: threadId

    If ~/.codex session_meta.payload.id matches one of these thread ids, the
    Codex CLI JSONL is the same run already represented by OpenClaw messages.
    """
    global _OPENCLAW_CODEX_THREAD_IDS_CACHE
    if _OPENCLAW_CODEX_THREAD_IDS_CACHE is not None and not force_refresh:
        return set(_OPENCLAW_CODEX_THREAD_IDS_CACHE)

    roots = [source["dir"] for source in OPENCLAW_SESSION_SOURCES]

    def add_thread_id(dest, value):
        if isinstance(value, str) and re.fullmatch(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", value):
            dest.add(value)

    def scan_json_obj(dest, obj):
        if not isinstance(obj, dict):
            return
        msg = obj.get("message") if isinstance(obj.get("message"), dict) else {}
        add_thread_id(dest, extract_codex_thread_id_from_idempotency_key(msg.get("idempotencyKey")))
        data = obj.get("data") if isinstance(obj.get("data"), dict) else {}
        add_thread_id(dest, data.get("threadId"))
        add_thread_id(dest, obj.get("threadId"))

    def scan_file(filepath):
        """Return the set of thread ids found in one file."""
        found = set()
        try:
            if filepath.name.endswith(".codex-app-server.json"):
                with open(filepath, "r", encoding="utf-8") as f:
                    scan_json_obj(found, json.load(f))
                return found
            with open(filepath, "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        scan_json_obj(found, json.loads(line))
                    except Exception:
                        continue
        except Exception:
            return found
        return found

    # Load persistent per-file cache: { path: [mtime, size, [thread_ids...]] }.
    # Only changed files (by mtime+size) are re-parsed; the rest reuse cached
    # results. Turns a ~40s cold full re-scan into a sub-second refresh.
    disk_cache = {}
    if not force_refresh:
        try:
            with open(CODEX_THREAD_ID_CACHE_PATH, "r", encoding="utf-8") as f:
                disk_cache = json.load(f)
            if not isinstance(disk_cache, dict):
                disk_cache = {}
        except Exception:
            disk_cache = {}

    thread_ids = set()
    new_cache = {}
    for root in roots:
        if not root.exists():
            continue
        candidates = []
        candidates.extend(root.glob("*.jsonl"))
        candidates.extend(root.glob("*.jsonl.deleted.*"))
        candidates.extend(root.glob("*.jsonl.reset.*"))
        candidates.extend(root.glob("*.trajectory.jsonl"))
        candidates.extend(root.glob("*.jsonl.codex-app-server.json"))
        for filepath in candidates:
            key = str(filepath)
            try:
                st = filepath.stat()
            except Exception:
                continue
            cached = disk_cache.get(key)
            if (
                isinstance(cached, list)
                and len(cached) == 3
                and cached[0] == st.st_mtime
                and cached[1] == st.st_size
            ):
                found = set(cached[2])
            else:
                found = scan_file(filepath)
            new_cache[key] = [st.st_mtime, st.st_size, sorted(found)]
            thread_ids |= found

    # Persist refreshed cache (best-effort, atomic replace).
    try:
        tmp_path = CODEX_THREAD_ID_CACHE_PATH.with_suffix(".tmp")
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(new_cache, f)
        os.replace(tmp_path, CODEX_THREAD_ID_CACHE_PATH)
    except Exception:
        pass

    _OPENCLAW_CODEX_THREAD_IDS_CACHE = set(thread_ids)
    return thread_ids


def openclaw_source_connection(path):
    """Read current compressed events and legacy JSON without changing the store."""
    from compression import zstd

    conn = sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True)
    columns = {row[1] for row in conn.execute("PRAGMA main.table_info(transcript_events)")}
    if "event_zstd" in columns:
        conn.create_function("decode_event", 1, lambda blob: zstd.decompress(blob).decode("utf-8") if blob else None)
        selected = ["rowid AS rowid"]
        selected += [
            'COALESCE(event_json, decode_event(event_zstd)) AS event_json' if col == "event_json"
            else '"' + col.replace('"', '""') + '"'
            for col in sorted(columns)
        ]
        conn.execute("CREATE TEMP VIEW transcript_events AS SELECT " + ", ".join(selected) + " FROM main.transcript_events")
    return conn


def resolve_tracker_runtime_path(storage_name, config):
    """Resolve runtime keys under the configured, traversal-checked source roots."""
    roots = [
        ("claude:", config["claude_projects_dir"]),
        ("codex_cli:", config["codex_sessions_dir"]),
        ("kimi_code:", config["kimi_code_dir"]),
    ]
    match = re.match(r"^codex_cli:codex-home/([^/]+)/(.+)$", storage_name or "")
    if match:
        roots.insert(0, ("codex_cli:codex-home/", config["openclaw_agents_dir"]))
        relative = Path(match[1]) / "agent" / "codex-home" / "sessions" / match[2]
        root = Path(config["openclaw_agents_dir"]).resolve()
    else:
        found = next(((prefix, Path(root)) for prefix, root in roots if (storage_name or "").startswith(prefix)), None)
        if not found:
            return None
        prefix, root = found
        root = root.resolve()
        relative = Path(storage_name[len(prefix):])
    path = (root / relative).resolve()
    return str(path) if path.is_relative_to(root) else None


def archived_codex_transcript_lines(source_conn):
    """(session_id, None, line) for Codex-routed lines in archived transcripts."""
    from compression import zstd

    out = []
    try:
        archives = source_conn.execute(
            "SELECT session_id, encoding, archive_blob FROM session_transcript_archives"
        ).fetchall()
    except sqlite3.OperationalError:
        return out  # older agent DB without the table
    for session_id, encoding, blob in archives:
        try:
            data = zstd.decompress(blob) if encoding == "zstd" else blob
            text = data.decode("utf-8")
        except Exception:
            continue
        out.extend(
            (session_id, None, line)
            for line in text.splitlines()
            if "codex-app-server:" in line
        )
    return out


def collect_openclaw_codex_thread_routes(include_unrouted=False):
    """Map Codex thread ids to their OpenClaw session and recorded model pair.

    The idempotency key is the explicit join key. No timestamp-only attribution
    is allowed here: if a thread maps to more than one OpenClaw session, it is
    deliberately omitted and remains an unlinked OpenClaw runtime record.
    With include_unrouted=True such threads are returned with session_id None
    (their OpenClaw message_ids are still exact, and are what
    supersede_routed_openclaw_turn_rows() needs).
    """
    collected = {}

    def add(thread_id, session_id, provider=None, model=None, message_id=None):
        if not thread_id or not session_id:
            return
        item = collected.setdefault(
            thread_id,
            {"session_ids": set(), "model_pairs": set(), "message_ids": set()},
        )
        item["session_ids"].add(session_id)
        if provider and model:
            item["model_pairs"].add((str(provider), str(model)))
        if message_id:
            item["message_ids"].add(message_id)

    for source in collect_openclaw_sqlite_sources():
        db_path = source["path"]
        try:
            source_conn = openclaw_source_connection(db_path)
            try:
                rows = source_conn.execute(
                    """
                    SELECT session_id, seq, event_json
                    FROM transcript_events
                    WHERE event_json LIKE '%codex-app-server:%'
                    """
                ).fetchall()
                # OpenClaw deletes isolated sessions (cron, cleanup=delete
                # subagents) and moves their transcript into an archive. Routes
                # are rebuilt every refresh, so without these the turn row and
                # its rollout calls both count again once the session is gone.
                rows += archived_codex_transcript_lines(source_conn)
                for session_id, seq, raw in rows:
                    try:
                        event = json.loads(raw)
                    except Exception:
                        continue
                    message = event.get("message") if isinstance(event.get("message"), dict) else {}
                    thread_id = extract_codex_thread_id_from_idempotency_key(
                        message.get("idempotencyKey")
                    )
                    message_id = None
                    if message.get("role") == "assistant":
                        # Same id rule as ingest_openclaw_sqlite(). Archived
                        # lines have no seq, so only a real event id counts.
                        message_id = event.get("id") or (
                            f"openclaw-sqlite:{source['agent']}:{session_id}:{seq}"
                            if seq is not None else None
                        )
                    add(
                        thread_id,
                        session_id,
                        message.get("provider"),
                        message.get("model"),
                        message_id,
                    )
            finally:
                source_conn.close()
        except Exception:
            continue

    # Historical JSONL remains useful after older SQLite windows have rotated.
    for source in OPENCLAW_SESSION_SOURCES:
        for filepath in collect_openclaw_session_files(source):
            session_id = None
            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    for line in f:
                        if "codex-app-server:" not in line and session_id is not None:
                            continue
                        try:
                            event = json.loads(line)
                        except Exception:
                            continue
                        if event.get("type") == "session" and event.get("id"):
                            session_id = event.get("id")
                        message = event.get("message") if isinstance(event.get("message"), dict) else {}
                        thread_id = extract_codex_thread_id_from_idempotency_key(
                            message.get("idempotencyKey")
                        )
                        if thread_id:
                            add(
                                thread_id,
                                session_id or filepath.name.split(".jsonl", 1)[0],
                                message.get("provider"),
                                message.get("model"),
                                event.get("id") if message.get("role") == "assistant" else None,
                            )
            except Exception:
                continue

    routes = {}
    for thread_id, item in collected.items():
        if len(item["session_ids"]) != 1 and not include_unrouted:
            continue
        routes[thread_id] = {
            "session_id": next(iter(item["session_ids"])) if len(item["session_ids"]) == 1 else None,
            "model_pairs": set(item["model_pairs"]),
            "message_ids": set(item["message_ids"]),
        }
    return routes


def codex_route_model_raw(route, runtime_model_raw: str):
    """Resolve an OpenClaw-recorded provider/model for one Codex runtime model."""
    pairs = set((route or {}).get("model_pairs") or ())
    runtime_model = runtime_model_raw.split("/", 1)[-1]
    exact = {(provider, model) for provider, model in pairs if model == runtime_model}
    if len(exact) == 1:
        provider, model = next(iter(exact))
        return f"{provider}/{model}"
    if len(pairs) == 1:
        provider, model = next(iter(pairs))
        return f"{provider}/{model}"
    # codex app-server is OpenAI's runtime. This branch is only used when the
    # OpenClaw transcript has rotated before its model-bearing event was read.
    return f"openai/{runtime_model}"


def is_codex_session_mirrored_by_openclaw(filepath: Path):
    session_id = extract_codex_session_id_from_file(filepath)
    return bool(session_id and session_id in collect_openclaw_codex_thread_ids())


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


def normalize_codex_source_label(value):
    """Return a short string label for Codex CLI session metadata."""
    if isinstance(value, str) and value:
        return value
    if isinstance(value, dict):
        subagent = value.get("subagent")
        if isinstance(subagent, dict):
            spawn = subagent.get("thread_spawn")
            if isinstance(spawn, dict):
                nickname = spawn.get("agent_nickname")
                role = spawn.get("agent_role")
                if nickname and role:
                    return f"{nickname}:{role}"
                if nickname:
                    return str(nickname)
        return "codex_cli"
    return "codex_cli"


def normalize_codex_usage(last_usage):
    """Map Codex CLI usage into the tracker schema without double-counting cached input."""
    gross_input_tokens = int(last_usage.get("input_tokens") or 0)
    cached_input_tokens = int(last_usage.get("cached_input_tokens") or 0)
    output_tokens = int(last_usage.get("output_tokens") or 0)
    total_tokens = int(last_usage.get("total_tokens") or 0)
    return {
        "input_tokens": max(gross_input_tokens - cached_input_tokens, 0),
        "output_tokens": output_tokens,
        "cache_read": cached_input_tokens,
        "cache_write": 0,
        "total_tokens": total_tokens,
    }


def _canonical_usage_state(value):
    if not isinstance(value, dict):
        return None
    def canonicalize(item):
        if isinstance(item, dict):
            return tuple((key, canonicalize(item.get(key))) for key in sorted(item))
        if isinstance(item, list):
            return tuple(canonicalize(part) for part in item)
        if item is None:
            return 0
        return item

    return canonicalize(value)


def codex_token_count_dedupe_key(info):
    """Semantic identity for Codex CLI token_count events.

    Codex can emit the same token_count payload more than once with different
    transport metadata, including rate-limit ids. The usage state is the thing
    being measured, so dedupe on running totals plus the last usage delta only.
    """
    if not isinstance(info, dict):
        return None
    last_usage = info.get("last_token_usage")
    if not isinstance(last_usage, dict):
        return None
    return (
        _canonical_usage_state(info.get("total_token_usage")),
        _canonical_usage_state(last_usage),
    )


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


def insert_message_row(
    cur,
    msg_id,
    session_id,
    parent_id,
    timestamp,
    model_raw,
    usage,
    cost=None,
    ingest_source=None,
):
    model_ref_id = get_model_ref_id(cur, model_raw)
    source_cost = cost or {}
    usage, source_cost, estimate_source, estimate_kind, is_estimated = apply_pioneer_input_estimate(
        model_raw, timestamp, usage, source_cost
    )
    had_source_cost = any((source_cost.get(k) or 0) for k in ("input", "output", "cacheRead", "cacheWrite", "total"))
    cost = maybe_calculate_cli_cost(model_raw, usage, source_cost)
    calculated_cost = (not had_source_cost) and model_raw.startswith(("codex/", "codex_cli/", "claude/", "openai/", "kimi-code/", "zai/", "cli-claude/", "cli-codex/", "cli-kimi/", "anthropic/", "anthropic-c/")) and bool(cost.get("total"))
    cost_source = estimate_source
    cost_kind = estimate_kind
    cost_is_estimated = is_estimated
    if not cost_source and calculated_cost:
        cost_source = "ratecard"
        cost_kind = "payg_equivalent"
        cost_is_estimated = 0
    cur.execute(
        """INSERT OR REPLACE INTO messages
           (id, session_id, parent_id, timestamp, model_ref_id,
            input_tokens, output_tokens, cache_read, cache_write, total_tokens,
            cost_input, cost_output, cost_cache_read, cost_cache_write, cost_total,
            cost_source, cost_kind, cost_is_estimated, ingest_source,
            duplicate_of, excluded_reason, is_excluded)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, 0)""",
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
            cost_source,
            cost_kind,
            cost_is_estimated,
            ingest_source,
        ),
    )


def ingest_openclaw_messages_from_offset(
    filepath: Path, conn, start_offset: int, session_id: str, source_key: str
):
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
                    ingest_source=f"openclaw_jsonl:{source_key}",
                )
                processed += 1

    return processed


def ingest_codex_messages_from_offset(filepath: Path, conn, start_offset: int, session_id: str):
    """Append-only ingest for Codex CLI token-count events."""
    cur = conn.cursor()
    processed = 0
    current_model = get_last_model_for_session(cur, session_id)
    seen_token_count_states = set()
    session_row = cur.execute(
        "SELECT session_type FROM sessions WHERE session_id = ?",
        (session_id,),
    ).fetchone()
    session_meta = read_codex_session_meta(Path(filepath))
    is_openclaw_runtime = codex_is_openclaw_runtime(
        session_meta.get("originator"), codex_app_server_root_for(filepath)
        or (session_row and session_row[0] == "openclaw"))

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
            dedupe_key = codex_token_count_dedupe_key(info)
            if dedupe_key in seen_token_count_states:
                continue
            seen_token_count_states.add(dedupe_key)

            model_name = current_model or "unknown"
            provider = "openai" if is_openclaw_runtime else "cli-codex"
            model_raw = f"{provider}/{model_name}"
            msg_id = f"{session_id}:codex:{line_offset}"
            normalized_usage = normalize_codex_usage(last_usage)
            insert_message_row(
                cur,
                msg_id,
                session_id,
                None,
                data.get("timestamp"),
                model_raw,
                normalized_usage,
                ingest_source=(
                    "codex_app_server"
                    if is_openclaw_runtime
                    else "codex_cli"
                ),
            )
            processed += 1

    return processed


def kimi_session_dir_for_wire(filepath: Path):
    filepath = Path(filepath)
    # ~/.kimi-code/sessions/<workspace>/session_<id>/agents/<agent>/wire.jsonl
    try:
        return filepath.parents[2]
    except IndexError:
        return filepath.parent


def read_kimi_state(filepath: Path):
    state_path = kimi_session_dir_for_wire(filepath) / "state.json"
    try:
        with open(state_path, "r", encoding="utf-8") as f:
            state = json.load(f)
            return state if isinstance(state, dict) else {}
    except Exception:
        return {}


def extract_kimi_session_id_from_path(filepath: Path):
    session_dir = kimi_session_dir_for_wire(filepath)
    return session_dir.name or Path(filepath).stem


def normalize_kimi_usage(value):
    """Map Kimi Code wire usage into the tracker schema."""
    if not isinstance(value, dict):
        value = {}
    input_tokens = int(value.get("inputOther") or value.get("input") or 0)
    output_tokens = int(value.get("output") or 0)
    cache_read = int(value.get("inputCacheRead") or 0)
    cache_write = int(value.get("inputCacheCreation") or 0)
    return {
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cache_read": cache_read,
        "cache_write": cache_write,
        "total_tokens": input_tokens + output_tokens + cache_read + cache_write,
    }


def insert_kimi_session_stub(cur, filepath: Path, session_id: str, storage_name: str, created_at: str | None):
    state = read_kimi_state(filepath)
    work_dir = state.get("workDir")
    if isinstance(work_dir, str) and work_dir:
        label = f"kimi:{Path(work_dir).name}"
    else:
        label = "kimi-code"
    session_nickname = session_id[9:13] if session_id and len(session_id) >= 13 else session_id
    cur.execute(
        """
        INSERT OR REPLACE INTO sessions
        (session_id, session_nickname, parent_nickname, filename, created_at,
         channel_id, channel_name, channel_type, session_type, deleted_at)
        VALUES (?, ?, ?, ?, ?, NULL, NULL, ?, ?, NULL)
        """,
        (session_id, session_nickname, label, storage_name, created_at, "cli", "cli"),
    )


def parse_kimi_session_file(filepath, conn):
    """Parse a Kimi Code wire transcript and normalize usage.record rows."""
    filepath = Path(filepath)
    storage_name = get_session_storage_name(filepath, "kimi_code")
    session_id = extract_kimi_session_id_from_path(filepath)
    agent_id = filepath.parent.name or "main"
    cur = conn.cursor()
    state = read_kimi_state(filepath)
    created_at = state.get("createdAt") if isinstance(state.get("createdAt"), str) else None
    message_count = 0

    try:
        insert_kimi_session_stub(cur, filepath, session_id, storage_name, created_at)
        with open(filepath, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, start=1):
                try:
                    data = json.loads(line)
                except Exception:
                    continue
                if data.get("type") != "usage.record":
                    continue
                model_raw = data.get("model")
                if not isinstance(model_raw, str) or not model_raw:
                    model_raw = "kimi-code/unknown"
                # Kimi Code is always terminal-owned (session stub hardcodes cli).
                model_raw = cli_prefixed_model_raw(model_raw, "cli")
                usage = normalize_kimi_usage(data.get("usage"))
                timestamp = datetime.fromtimestamp((data.get("time") or 0) / 1000, tz=timezone.utc).isoformat() if data.get("time") else created_at
                msg_id = f"{session_id}:kimi:{agent_id}:{line_num}"
                insert_message_row(
                    cur,
                    msg_id,
                    session_id,
                    None,
                    timestamp,
                    model_raw,
                    usage,
                    ingest_source="kimi_cli",
                )
                message_count += 1
        recompute_segments(conn, session_id)
        return True, message_count, session_id
    except Exception as e:
        print(f"  Error reading {storage_name}: {e}")
        return False, 0, None


def ingest_kimi_messages_from_offset(filepath: Path, conn, start_offset: int, session_id: str):
    """Append-only ingest for Kimi Code usage.record rows."""
    cur = conn.cursor()
    processed = 0
    filepath = Path(filepath)
    agent_id = filepath.parent.name or "main"
    storage_name = get_session_storage_name(filepath, "kimi_code")
    state = read_kimi_state(filepath)
    created_at = state.get("createdAt") if isinstance(state.get("createdAt"), str) else None
    insert_kimi_session_stub(cur, filepath, session_id, storage_name, created_at)

    with open(filepath, "rb") as f:
        f.seek(start_offset)
        while True:
            line_offset = f.tell()
            line = f.readline()
            if not line:
                break
            try:
                data = json.loads(line.decode("utf-8", errors="strict"))
            except Exception:
                continue
            if data.get("type") != "usage.record":
                continue
            model_raw = data.get("model")
            if not isinstance(model_raw, str) or not model_raw:
                model_raw = "kimi-code/unknown"
            # Kimi Code is always terminal-owned (session stub hardcodes cli).
            model_raw = cli_prefixed_model_raw(model_raw, "cli")
            usage = normalize_kimi_usage(data.get("usage"))
            timestamp = datetime.fromtimestamp((data.get("time") or 0) / 1000, tz=timezone.utc).isoformat() if data.get("time") else created_at
            msg_id = f"{session_id}:kimi:{agent_id}:{line_offset}"
            insert_message_row(
                cur,
                msg_id,
                session_id,
                None,
                timestamp,
                model_raw,
                usage,
                ingest_source="kimi_cli",
            )
            processed += 1

    return processed


def ingest_messages_from_offset(filepath: Path, conn, start_offset: int, session_id: str, source_key: str):
    if source_key == "codex_cli":
        return ingest_codex_messages_from_offset(filepath, conn, start_offset, session_id)
    if source_key == "kimi_code":
        return ingest_kimi_messages_from_offset(filepath, conn, start_offset, session_id)
    return ingest_openclaw_messages_from_offset(
        filepath, conn, start_offset, session_id, source_key
    )


def recompute_segments(conn, session_id: str):
    """Recompute model/reset-generation segments from archived messages."""
    cur = conn.cursor()

    # Delete only derived data (segments). Messages are the archive.
    cur.execute("DELETE FROM segments WHERE session_id = ?", (session_id,))

    cur.execute(
        """SELECT m.timestamp, mr.model_raw, m.input_tokens, m.output_tokens, m.cache_read, m.cache_write, m.total_tokens,
                  m.cost_input, m.cost_output, m.cost_cache_read, m.cost_cache_write, m.cost_total,
                  m.cost_is_estimated
           FROM effective_messages m
           INNER JOIN model_reference mr ON mr.id = m.model_ref_id
           WHERE m.session_id = ?
             AND COALESCE(m.is_excluded, 0) = 0
           ORDER BY m.timestamp ASC""",
        (session_id,)
    )
    rows = cur.fetchall()
    if not rows:
        return

    boundary_rows = cur.execute(
        """
        SELECT timestamp
        FROM session_boundaries
        WHERE session_id = ?
        ORDER BY timestamp, source, event_seq
        """,
        (session_id,),
    ).fetchall()
    reset_timestamps = [row[0] for row in boundary_rows if row[0]]

    def is_mirror(model_raw: str):
        return model_raw == 'openclaw/delivery-mirror'

    segment_index = 0
    seg_model = None
    seg_rows = []
    boundary_index = 0

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
        segment_cost_is_estimated = 1 if any(r[12] for r in seg_rows) else 0
        segment_cost_source = PIONEER_INPUT_ESTIMATE_SOURCE if segment_cost_is_estimated else ("ratecard" if agg['cost_total'] else None)
        segment_cost_kind = "payg_estimated_input" if segment_cost_is_estimated else ("payg_equivalent" if agg['cost_total'] else None)

        ensure_model_reference(cur, seg_model)
        cur.execute(
            """INSERT OR REPLACE INTO segments
               (session_id, model_raw, segment_index, first_msg_ts, last_msg_ts, msg_count,
                input_tokens, output_tokens, cache_read, cache_write, total_tokens,
                cost_input, cost_output, cost_cache_read, cost_cache_write, cost_total,
                cost_source, cost_kind, cost_is_estimated)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (session_id, seg_model, segment_index, first_ts, last_ts, msg_count,
             agg['input_tokens'], agg['output_tokens'], agg['cache_read'], agg['cache_write'], agg['total_tokens'],
             agg['cost_input'], agg['cost_output'], agg['cost_cache_read'], agg['cost_cache_write'], agg['cost_total'],
             segment_cost_source, segment_cost_kind, segment_cost_is_estimated)
        )
        seg_rows = []
        segment_index += 1

    for row in rows:
        ts, model_raw = row[0], row[1]
        if not model_raw or is_mirror(model_raw):
            continue
        while boundary_index < len(reset_timestamps) and reset_timestamps[boundary_index] <= ts:
            boundary_ts = reset_timestamps[boundary_index]
            if seg_rows and boundary_ts > seg_rows[-1][0]:
                flush()
                seg_model = None
            boundary_index += 1
        if seg_model is None:
            seg_model = model_raw
        if model_raw != seg_model:
            flush()
            seg_model = model_raw
        seg_rows.append(row)

    flush()


def backfill_message_ingest_sources(conn):
    """Label archived rows using stable IDs and session storage paths.

    Older tracker rows predate message-level provenance. The labels are
    intentionally coarse but deterministic; new rows receive exact labels at
    ingest time.
    """
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE messages
        SET ingest_source = 'codex_app_server'
        WHERE ingest_source IS NULL
          AND id LIKE '%:codex:%'
          AND EXISTS (
              SELECT 1
              FROM sessions s
              WHERE s.session_id = messages.session_id
                AND s.filename LIKE 'codex_cli:codex-home/%'
          )
        """
    )
    cur.execute(
        """
        UPDATE messages
        SET ingest_source = 'codex_cli'
        WHERE ingest_source IS NULL
          AND id LIKE '%:codex:%'
        """
    )
    cur.execute(
        """
        UPDATE messages
        SET ingest_source = 'kimi_cli'
        WHERE ingest_source IS NULL
          AND id LIKE '%:kimi:%'
        """
    )
    cur.execute(
        """
        UPDATE messages
        SET ingest_source = 'claude_cli'
        WHERE ingest_source IS NULL
          AND EXISTS (
              SELECT 1
              FROM sessions s
              WHERE s.session_id = messages.session_id
                AND s.filename LIKE 'claude:%'
          )
        """
    )
    cur.execute(
        """
        UPDATE messages
        SET ingest_source = 'openclaw_legacy'
        WHERE ingest_source IS NULL
        """
    )
    conn.commit()


def backfill_cli_model_prefixes(conn):
    """Repoint messages/segments so a model_raw prefix names the session's OWNER.

    Terminal-owned -> `cli-<tool>/`; everything else Claude-family is
    OpenClaw-owned -> `anthropic[(c)]/` (ownership convention). This cannot be a
    blanket string rewrite: `claude/` is genuinely mixed, holding both hand-run
    and OpenClaw-driven transcripts, so ownership is read from
    `sessions.session_type`, never guessed from the prefix. Idempotent -- rows
    already carrying a rewritten prefix do not match the source patterns.

    Note the endpoint column is carried across unchanged: `cli-codex/` is an
    ownership label, and the dashboard's provider column stays `codex`.
    """
    cur = conn.cursor()
    moved = 0
    # (prefix table, ownership predicate). 'cli' is terminal-owned; anything
    # else reaching a Claude-family model is OpenClaw driving it.
    for table, owned_sql, endpoint_override in (
        (CLI_MODEL_PREFIXES, "s.session_type = 'cli'", None),
        # Ownership moves the provider too: these are Anthropic's, whichever
        # credential route OpenClaw used. Matches the pre-existing anthropic/*
        # rows so the dashboard shows one provider, not two.
        (OC_CLAUDE_PREFIXES, "s.session_type != 'cli'", "anthropic"),
    ):
        sess_filter = owned_sql.replace("s.session_type", "session_type")
        for old_prefix, new_prefix in table.items():
            pairs = cur.execute(
                f"""
                SELECT DISTINCT mr.id, mr.model_raw, mr.endpoint, mr.author,
                       mr.model, mr.pru_multiplier
                FROM messages m
                JOIN model_reference mr ON mr.id = m.model_ref_id
                JOIN sessions s ON s.session_id = m.session_id
                WHERE mr.model_raw LIKE ? AND {owned_sql}
                """,
                (old_prefix + "%",),
            ).fetchall()
            for old_id, old_raw, endpoint, author, model, pru in pairs:
                new_raw = new_prefix + old_raw[len(old_prefix):]
                endpoint = endpoint_override or endpoint
                row = cur.execute(
                    "SELECT id FROM model_reference WHERE model_raw = ?", (new_raw,)
                ).fetchone()
                if row:
                    new_id = int(row[0])
                else:
                    cur.execute(
                        """
                        INSERT INTO model_reference
                        (model_raw, endpoint, author, model, pru_multiplier)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (new_raw, endpoint, author, model, pru),
                    )
                    new_id = int(cur.lastrowid)
                cur.execute(
                    f"""
                    UPDATE messages SET model_ref_id = ?
                    WHERE model_ref_id = ?
                      AND session_id IN (SELECT session_id FROM sessions
                                         WHERE {sess_filter})
                    """,
                    (new_id, old_id),
                )
                moved += cur.rowcount
                cur.execute(
                    f"""
                    UPDATE segments SET model_raw = ?
                    WHERE model_raw = ?
                      AND session_id IN (SELECT session_id FROM sessions
                                         WHERE {sess_filter})
                    """,
                    (new_raw, old_raw),
                )
    conn.commit()
    if moved:
        print(f"✓ Ownership prefix: {moved} messages repointed")
    return moved


def reclassify_runtime_session_identities(conn):
    """Separate direct CLI sessions from OpenClaw-owned runtime evidence.

    `codex_cli` was an importer label, never a source provider. Codex transcripts
    explicitly record their originator, so classify direct use as codex/* + CLI
    and OpenClaw app-server use as openai/* + OpenClaw runtime evidence.
    """
    cur = conn.cursor()
    touched = set()

    cur.execute("CREATE TEMP TABLE IF NOT EXISTS direct_claude_sessions(session_id TEXT PRIMARY KEY)")
    cur.execute("DELETE FROM direct_claude_sessions")
    # Kimi files are direct CLI. Claude transcripts record their launcher in
    # `entrypoint` (sdk-cli = OpenClaw wrapper), so classify them the same way
    # Codex is classified by `originator`; never blanket-label them CLI. The
    # channel columns are still cleared: do not mutate a runtime session into
    # Discord.
    for session_id, filename, session_type, channel_type, channel_id, channel_name in cur.execute(
        """
        SELECT session_id, filename, session_type, channel_type, channel_id, channel_name
        FROM sessions
        WHERE filename LIKE 'claude:%' OR filename LIKE 'kimi_code:%'
        """
    ).fetchall():
        if filename.startswith("claude:"):
            filepath = CLAUDE_PROJECTS_DIR / filename[len("claude:"):]
            entrypoint = read_claude_entrypoint(filepath)
            new_type = claude_session_kind(entrypoint)
            if entrypoint == 'cli':
                cur.execute("INSERT OR IGNORE INTO direct_claude_sessions VALUES (?)", (session_id,))
        else:
            new_type = "cli"
        if (
            session_type != new_type
            or channel_type != new_type
            or channel_id is not None
            or channel_name is not None
        ):
            cur.execute(
                """
                UPDATE sessions
                SET session_type = ?, channel_type = ?,
                    channel_id = NULL, channel_name = NULL
                WHERE session_id = ?
                """,
                (new_type, new_type, session_id),
            )

    codex_sessions = cur.execute(
        """
        SELECT session_id, filename, session_type, channel_type, parent_nickname
        FROM sessions
        WHERE filename LIKE 'codex_cli:%'
        """
    ).fetchall()
    for session_id, filename, old_type, old_channel_type, old_label in codex_sessions:
        filepath = codex_filepath_from_storage_name(filename)
        meta = read_codex_session_meta(filepath) if filepath and filepath.exists() else {}
        is_openclaw = codex_is_openclaw_runtime(
            meta.get("originator"), filename.startswith("codex_cli:codex-home/"))
        # Identity uses the proven owner; standalone execution keeps codex.
        provider = "openai" if is_openclaw else "cli-codex"
        endpoint = "openai" if is_openclaw else "codex"
        ingest_source = "codex_app_server" if is_openclaw else "codex_cli"
        new_type = "openclaw" if is_openclaw else "cli"
        new_channel_type = "openclaw" if is_openclaw else "cli"
        new_label = None if is_openclaw and old_label == "vscode" else old_label

        if (
            old_type != new_type
            or old_channel_type != new_channel_type
            or old_label != new_label
        ):
            cur.execute(
                """
                UPDATE sessions
                SET session_type = ?, channel_type = ?, parent_nickname = ?
                WHERE session_id = ?
                """,
                (new_type, new_channel_type, new_label, session_id),
            )

        rows = cur.execute(
            """
            SELECT model_raw FROM (
                SELECT DISTINCT mr.model_raw AS model_raw
                FROM messages m
                JOIN model_reference mr ON mr.id = m.model_ref_id
                WHERE m.session_id = ?
                  AND (mr.model_raw LIKE 'codex_cli/%' OR (? = 0 AND mr.model_raw LIKE 'openai/%'))
                UNION
                SELECT DISTINCT seg.model_raw AS model_raw
                FROM segments seg
                WHERE seg.session_id = ?
                  AND (seg.model_raw LIKE 'codex_cli/%' OR (? = 0 AND seg.model_raw LIKE 'openai/%'))
            )
            """,
            (session_id, int(is_openclaw), session_id, int(is_openclaw)),
        ).fetchall()
        for (old_model_raw,) in rows:
            model_name = old_model_raw.split("/", 1)[1]
            new_model_raw = f"{provider}/{model_name}"
            old_ref = cur.execute(
                """
                SELECT author, model, pru_multiplier
                FROM model_reference
                WHERE model_raw = ?
                """,
                (old_model_raw,),
            ).fetchone()
            existing_ref = cur.execute(
                "SELECT id FROM model_reference WHERE model_raw = ?",
                (new_model_raw,),
            ).fetchone()
            if not existing_ref and old_ref:
                cur.execute(
                    """
                    INSERT INTO model_reference
                    (model_raw, endpoint, author, model, pru_multiplier)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        new_model_raw,
                        endpoint,
                        old_ref[0],
                        old_ref[1],
                        old_ref[2],
                    ),
                )
                new_model_ref_id = cur.lastrowid
            else:
                new_model_ref_id = (
                    existing_ref[0]
                    if existing_ref
                    else get_model_ref_id(cur, new_model_raw)
                )
            cur.execute(
                """
                UPDATE messages
                SET model_ref_id = ?, ingest_source = ?
                WHERE session_id = ?
                  AND model_ref_id = (
                      SELECT id FROM model_reference WHERE model_raw = ?
                  )
                """,
                (new_model_ref_id, ingest_source, session_id, old_model_raw),
            )
            cur.execute(
                """
                UPDATE segments
                SET model_raw = ?
                WHERE session_id = ? AND model_raw = ?
                """,
                (new_model_raw, session_id, old_model_raw),
            )
        cur.execute(
            """
            UPDATE messages
            SET ingest_source = ?
            WHERE session_id = ? AND ingest_source IN ('codex_cli', 'codex_app_server')
              AND ingest_source != ?
            """,
            (ingest_source, session_id, ingest_source),
        )

    conn.commit()
    return touched


def cleanup_obsolete_codex_cli_model_refs(conn):
    """Remove importer-only raw ids after every real reference has moved."""
    cur = conn.cursor()
    invoice_guard = ""
    if cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'provider_pru_invoices'"
    ).fetchone():
        invoice_guard = """
          AND NOT EXISTS (
              SELECT 1 FROM provider_pru_invoices p WHERE p.model_raw = model_reference.model_raw
          )
        """
    cur.execute(
        f"""
        DELETE FROM model_reference
        WHERE model_raw LIKE 'codex_cli/%'
          AND NOT EXISTS (
              SELECT 1 FROM messages m WHERE m.model_ref_id = model_reference.id
          )
          AND NOT EXISTS (
              SELECT 1 FROM segments seg WHERE seg.model_raw = model_reference.model_raw
          )
          {invoice_guard}
        """
    )
    return cur.rowcount


def add_message_attribution(
    cur,
    source_message_id,
    canonical_session_id,
    canonical_model_raw,
    reason,
    canonical_message_id=None,
):
    model_ref_id = get_model_ref_id(cur, canonical_model_raw)
    cur.execute(
        """
        INSERT OR REPLACE INTO message_attributions
        (source_message_id, canonical_session_id, canonical_model_ref_id,
         canonical_message_id, reason)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            source_message_id,
            canonical_session_id,
            model_ref_id,
            canonical_message_id,
            reason,
        ),
    )


def build_runtime_attributions(conn, routes=None):
    """Attribute complete runtime counters to OpenClaw-owned sessions."""
    cur = conn.cursor()
    touched = set()
    pending = []
    model_ref_cache = {
        row[0]: row[1]
        for row in cur.execute("SELECT model_raw, id FROM model_reference")
    }

    def queue_attribution(
        source_message_id,
        canonical_session_id,
        canonical_model_raw,
        reason,
        canonical_message_id=None,
    ):
        model_ref_id = model_ref_cache.get(canonical_model_raw)
        if model_ref_id is None:
            model_ref_id = get_model_ref_id(cur, canonical_model_raw)
            model_ref_cache[canonical_model_raw] = model_ref_id
        pending.append(
            (
                source_message_id,
                canonical_session_id,
                model_ref_id,
                canonical_message_id,
                reason,
            )
        )

    # A proved final-call twin identifies the runtime invocation. Attribute all
    # calls when the invocation has exactly one owner; ambiguous joins stay raw.
    claude_targets = {}
    for source_session, source_id, wrapper_id, wrapper_session, model_raw in cur.execute(
        """SELECT cli.session_id, cli.id, wrapper.id, wrapper.session_id, mr.model_raw
           FROM messages wrapper JOIN messages cli ON cli.id = wrapper.duplicate_of
           JOIN model_reference mr ON mr.id = wrapper.model_ref_id
           WHERE wrapper.excluded_reason = 'duplicate_claude_wrapper_cli'
             AND wrapper.is_excluded = 1"""
    ).fetchall():
        claude_targets.setdefault(source_session, set()).add(wrapper_session)
        queue_attribution(source_id, wrapper_session, model_raw,
            "openclaw_claude_runtime_usage", wrapper_id)
        touched.update((source_session, wrapper_session))
    for source_session, owners in claude_targets.items():
        kind = cur.execute("SELECT session_type FROM sessions WHERE session_id = ?",
                           (source_session,)).fetchone()[0]
        if len(owners) != 1 or kind == "cli":
            continue
        owner = next(iter(owners))
        for source_id, raw in cur.execute(
            """SELECT m.id, mr.model_raw FROM messages m
               JOIN model_reference mr ON mr.id = m.model_ref_id
               WHERE m.session_id = ? AND m.ingest_source = 'claude_cli'
                 AND COALESCE(m.is_excluded, 0) = 0""", (source_session,)
        ).fetchall():
            queue_attribution(source_id, owner, "anthropic/" + raw.split("/", 1)[-1],
                              "openclaw_claude_runtime_usage")

    # A token-identical Codex pair proves the thread -> OpenClaw session join.
    codex_targets = {}
    for source_session_id, wrapper_session_id, model_raw in cur.execute(
        """
        SELECT cli.session_id, oc.session_id, oc_model.model_raw
        FROM messages cli
        JOIN messages oc ON oc.id = cli.duplicate_of
        JOIN model_reference oc_model ON oc_model.id = oc.model_ref_id
        WHERE cli.excluded_reason = 'duplicate_codex_app_server_openclaw'
          AND cli.is_excluded = 1
        """
    ).fetchall():
        codex_targets.setdefault(source_session_id, set()).add(
            (wrapper_session_id, model_raw)
        )

    explicit_routes = routes if routes is not None else collect_openclaw_codex_thread_routes()
    runtime_sessions = [
        row[0]
        for row in cur.execute(
            """
            SELECT DISTINCT session_id
            FROM messages
            WHERE ingest_source = 'codex_app_server'
            """
        )
    ]
    for source_session_id in runtime_sessions:
        target_session_id = None
        target_model_raw = None
        proved = codex_targets.get(source_session_id, set())
        route = explicit_routes.get(source_session_id)
        if len(proved) == 1:
            target_session_id, target_model_raw = next(iter(proved))
        elif route and cur.execute(
            "SELECT 1 FROM sessions WHERE session_id = ?",
            (route["session_id"],),
        ).fetchone():
            target_session_id = route["session_id"]

        if not target_session_id:
            continue

        rows = cur.execute(
            """
            SELECT m.id, mr.model_raw
            FROM messages m
            JOIN model_reference mr ON mr.id = m.model_ref_id
            WHERE m.session_id = ?
              AND m.ingest_source = 'codex_app_server'
              AND COALESCE(m.is_excluded, 0) = 0
            """,
            (source_session_id,),
        ).fetchall()
        for source_id, runtime_model_raw in rows:
            model_raw = (
                codex_route_model_raw(route, runtime_model_raw)
                if route
                else target_model_raw or runtime_model_raw
            )
            queue_attribution(
                source_id,
                target_session_id,
                model_raw,
                "openclaw_codex_runtime_usage",
            )
        if rows:
            touched.update((source_session_id, target_session_id))

    cur.executemany(
        """
        INSERT OR REPLACE INTO message_attributions
        (source_message_id, canonical_session_id, canonical_model_ref_id,
         canonical_message_id, reason)
        VALUES (?, ?, ?, ?, ?)
        """,
        pending,
    )

    return touched


SUPERSEDED_BY_CODEX_RUNTIME_REASON = "superseded_by_codex_runtime_usage"


def supersede_routed_openclaw_turn_rows(cur, routes):
    """Exclude OpenClaw per-turn Codex rows whose turn is covered by per-call rollout rows.

    The Codex harness hands OpenClaw one usage record per turn: the sum of every
    model call in the turn (older app-server builds reported something else
    entirely). The rollout carries one token_count per call. For a thread routed
    by idempotency key, the attributed per-call rows are the accounting record and
    the turn row is redundant. A turn row is excluded only when at least one
    attributed call falls in its window (after the previous turn row of the same
    thread, up to the turn row itself), so a rollout that is not ingested yet
    leaves the turn row counted until the next refresh recomputes this.
    """
    superseded = []
    for thread_id, route in routes.items():
        message_ids = sorted(route.get("message_ids") or ())
        if not message_ids:
            continue
        calls = [
            row[0]
            for row in cur.execute(
                """
                SELECT m.timestamp
                FROM messages m
                JOIN message_attributions a ON a.source_message_id = m.id
                WHERE m.session_id = ?
                  AND a.reason = 'openclaw_codex_runtime_usage'
                  AND COALESCE(m.is_excluded, 0) = 0
                  AND m.timestamp IS NOT NULL
                ORDER BY m.timestamp
                """,
                (thread_id,),
            )
        ]
        if not calls:
            continue
        turns = []
        for start in range(0, len(message_ids), 500):
            chunk = message_ids[start:start + 500]
            turns.extend(
                cur.execute(
                    f"""
                    SELECT id, timestamp
                    FROM messages
                    WHERE id IN ({",".join("?" * len(chunk))})
                      AND ingest_source LIKE 'openclaw%'
                      AND COALESCE(is_excluded, 0) = 0
                      AND timestamp IS NOT NULL
                    """,
                    chunk,
                ).fetchall()
            )
        turns.sort(key=lambda row: (row[1], row[0]))
        call_index = 0
        for turn_id, turn_ts in turns:
            covered = 0
            while call_index < len(calls) and calls[call_index] <= turn_ts:
                covered += 1
                call_index += 1
            if covered:
                superseded.append((turn_id,))
    cur.executemany(
        """
        UPDATE messages
        SET is_excluded = 1,
            excluded_reason = ?,
            duplicate_of = NULL
        WHERE id = ?
        """,
        [(SUPERSEDED_BY_CODEX_RUNTIME_REASON, turn_id) for (turn_id,) in superseded],
    )
    return len(superseded)


CLAUDE_TURN_REASON = "superseded_by_claude_runtime_usage"


def reconcile_claude_turn_usage(conn, since):
    """Replace a uniquely proved whole-turn sum with its original model calls.

    All four counters must match exactly. A nearby final call anchors a
    contiguous suffix in one non-terminal runtime session of the same model.
    Ambiguous matches and overlapping claims stay on wrapper usage.
    """
    cur = conn.cursor()
    fields = ("input_tokens", "output_tokens", "cache_read", "cache_write")
    wrappers = cur.execute("""
        SELECT m.id, m.session_id, mr.model_raw, m.timestamp,
               m.input_tokens, m.output_tokens, m.cache_read, m.cache_write
        FROM messages m JOIN model_reference mr ON mr.id=m.model_ref_id
        WHERE m.ingest_source LIKE 'openclaw%' AND m.timestamp >= ?
          AND COALESCE(m.is_excluded,0)=0
          AND substr(mr.model_raw,1,instr(mr.model_raw,'/')-1)
              IN ('anthropic','anthropic-c','claude-cli')
          AND m.total_tokens > 0
        ORDER BY m.timestamp, m.id
    """, (since,)).fetchall()
    claims = []
    for wid, owner, raw, timestamp, *target in wrappers:
        target = [v or 0 for v in target]
        # The indexed timestamp join keeps the full-history refresh bounded.
        finals = cur.execute("""
            SELECT m.id, m.session_id, m.timestamp
            FROM messages m JOIN model_reference mr ON mr.id=m.model_ref_id
            WHERE m.timestamp >= strftime('%Y-%m-%dT%H:%M:%fZ', ?, '-2 seconds')
              AND m.timestamp <= strftime('%Y-%m-%dT%H:%M:%fZ', ?, '+2 seconds')
              AND m.ingest_source='claude_cli' AND COALESCE(m.is_excluded,0)=0
              AND m.session_id NOT IN (SELECT session_id FROM direct_claude_sessions)
              AND substr(mr.model_raw,instr(mr.model_raw,'/')+1)=?
        """, (timestamp, timestamp, raw.split('/',1)[1])).fetchall()
        hits = []
        for final_id, runtime, final_ts in finals:
            calls = cur.execute("""
                SELECT m.id, m.input_tokens, m.output_tokens, m.cache_read, m.cache_write
                FROM messages m JOIN model_reference mr ON mr.id=m.model_ref_id
                WHERE m.session_id=? AND m.ingest_source='claude_cli'
                  AND COALESCE(m.is_excluded,0)=0 AND m.timestamp <= ?
                  AND substr(mr.model_raw,instr(mr.model_raw,'/')+1)=?
                ORDER BY m.timestamp DESC, m.id DESC
            """, (runtime, final_ts, raw.split('/',1)[1]))
            sums = [0]*len(fields)
            ids = []
            for cid, *usage in calls:
                if not ids and cid != final_id:
                    break  # equal timestamps cannot prove a unique suffix
                ids.append(cid)
                sums = [a+(b or 0) for a,b in zip(sums,usage)]
                if sums == target:
                    hits.append(ids)
                    break
                if any(a>b for a,b in zip(sums,target)):
                    break
        if len(hits)==1:
            claims.append((wid, owner, raw, hits[0]))
    counts = {}
    for _,_,_,ids in claims:
        for cid in ids:
            counts[cid] = counts.get(cid,0)+1
    for wid,owner,raw,ids in claims:
        if any(counts[cid]!=1 for cid in ids):
            continue
        cur.execute("""UPDATE messages SET is_excluded=1,excluded_reason=?,duplicate_of=NULL
                       WHERE id=?""", (CLAUDE_TURN_REASON,wid))
        for cid in ids:
            add_message_attribution(cur,cid,owner,raw,"openclaw_claude_runtime_usage",wid)


def reconcile_usage_duplicates(conn, since="2026-07-01T00:00:00"):
    """Exclude only high-confidence duplicate representations.

    Raw message rows remain in the archive. Derived segments and dashboard
    totals omit rows marked here and retain a pointer to the canonical row.
    """
    cur = conn.cursor()
    previous_duplicates = {
        row[0]: (row[1], row[2], row[3])
        for row in cur.execute(
            """
            SELECT id, session_id, duplicate_of, excluded_reason
            FROM messages
            WHERE excluded_reason IN (
                'duplicate_codex_app_server_openclaw',
                'duplicate_claude_wrapper_cli',
                'superseded_by_codex_runtime_usage',
                'superseded_by_claude_runtime_usage'
            )
            """
        )
    }
    previous_attributions = {
        row[0]: (row[1], row[2], row[3], row[4])
        for row in cur.execute(
            """
            SELECT a.source_message_id, m.session_id, a.canonical_session_id,
                   a.canonical_model_ref_id, a.reason
            FROM message_attributions a
            JOIN messages m ON m.id = a.source_message_id
            WHERE a.reason IN (
                'openclaw_codex_runtime_usage',
                'openclaw_claude_runtime_usage',
                'openclaw_session_owner'
            )
            """
        )
    }
    backfill_cli_model_prefixes(conn)
    touched_sessions = reclassify_runtime_session_identities(conn)
    cur.execute("""UPDATE messages SET is_excluded=0,excluded_reason=NULL,duplicate_of=NULL
        WHERE ingest_source='codex_cli' AND excluded_reason='duplicate_codex_app_server_openclaw'""")
    cur.execute("""UPDATE messages SET is_excluded=0,excluded_reason=NULL,duplicate_of=NULL
        WHERE excluded_reason='duplicate_claude_wrapper_cli'
          AND duplicate_of IN (SELECT r.id FROM messages r
              JOIN direct_claude_sessions d ON d.session_id=r.session_id)""")
    cur.execute("UPDATE messages SET is_excluded=0,excluded_reason=NULL,duplicate_of=NULL WHERE excluded_reason=?", (CLAUDE_TURN_REASON,))
    reasons = (
        CLAUDE_TURN_REASON,
        "duplicate_codex_app_server_openclaw",
        "duplicate_claude_wrapper_cli",
        SUPERSEDED_BY_CODEX_RUNTIME_REASON,
    )
    # Threads joined by idempotency key are accounted per rollout call. Their
    # earlier twin/turn exclusions are cleared here and recomputed below.
    all_routes = collect_openclaw_codex_thread_routes(include_unrouted=True)
    routes = {
        thread_id: route
        for thread_id, route in all_routes.items()
        if route["session_id"]
    }
    cur.execute(
        "CREATE TEMP TABLE IF NOT EXISTS routed_codex_threads (thread_id TEXT PRIMARY KEY)"
    )
    cur.execute("DELETE FROM routed_codex_threads")
    cur.executemany(
        "INSERT INTO routed_codex_threads (thread_id) VALUES (?)",
        [(thread_id,) for thread_id in routes],
    )
    cur.execute(
        """
        UPDATE messages
        SET is_excluded = 0, excluded_reason = NULL, duplicate_of = NULL
        WHERE excluded_reason = ?
           OR (
                excluded_reason = 'duplicate_codex_app_server_openclaw'
                AND session_id IN (SELECT thread_id FROM routed_codex_threads)
           )
        """,
        (SUPERSEDED_BY_CODEX_RUNTIME_REASON,),
    )
    attribution_reasons = (
        "openclaw_codex_runtime_usage",
        "openclaw_claude_runtime_usage",
    )
    cur.execute(
        """
        WITH candidates AS (
            SELECT
                cli.id AS duplicate_id,
                oc.id AS canonical_id,
                ABS((julianday(cli.timestamp) - julianday(oc.timestamp)) * 86400) AS time_gap
            FROM messages cli
            JOIN model_reference cli_model ON cli_model.id = cli.model_ref_id
            JOIN messages oc INDEXED BY idx_messages_timestamp
              ON oc.timestamp >= max(?, strftime('%Y-%m-%dT%H:%M:%fZ', cli.timestamp, '-3 seconds'))
             AND oc.timestamp <= strftime('%Y-%m-%dT%H:%M:%fZ', cli.timestamp, '+3 seconds')
             AND ABS((julianday(cli.timestamp) - julianday(oc.timestamp)) * 86400) < 2
             AND COALESCE(cli.input_tokens, 0) = COALESCE(oc.input_tokens, 0)
             AND COALESCE(cli.output_tokens, 0) = COALESCE(oc.output_tokens, 0)
             AND COALESCE(cli.cache_read, 0) = COALESCE(oc.cache_read, 0)
             AND COALESCE(cli.cache_write, 0) = COALESCE(oc.cache_write, 0)
             AND COALESCE(cli.total_tokens, 0) = COALESCE(oc.total_tokens, 0)
            JOIN model_reference oc_model ON oc_model.id = oc.model_ref_id
            WHERE cli.ingest_source = 'codex_app_server'
              AND cli.session_id NOT IN (SELECT thread_id FROM routed_codex_threads)
              AND cli.timestamp >= ?
              AND COALESCE(cli.is_excluded, 0) = 0
              AND NOT EXISTS (
                  SELECT 1 FROM message_attributions a
                  WHERE a.source_message_id = cli.id
              )
              AND NOT EXISTS (
                  SELECT 1 FROM messages proved
                  WHERE proved.session_id = cli.session_id
                    AND proved.excluded_reason = 'duplicate_codex_app_server_openclaw'
              )
              AND oc.ingest_source LIKE 'openclaw%'
              AND cli_model.model_raw LIKE 'openai/%'
              AND oc_model.model_raw = cli_model.model_raw
        ),
        ranked AS (
            SELECT
                duplicate_id,
                canonical_id,
                ROW_NUMBER() OVER (
                    PARTITION BY duplicate_id
                    ORDER BY time_gap, canonical_id
                ) AS duplicate_rank,
                ROW_NUMBER() OVER (
                    PARTITION BY canonical_id
                    ORDER BY time_gap, duplicate_id
                ) AS canonical_rank
            FROM candidates
        ),
        matches AS MATERIALIZED (
            SELECT duplicate_id, canonical_id
            FROM ranked
            WHERE duplicate_rank = 1 AND canonical_rank = 1
        )
        UPDATE messages
        SET duplicate_of = matches.canonical_id,
            excluded_reason = 'duplicate_codex_app_server_openclaw',
            is_excluded = 1
        FROM matches
        WHERE messages.id = matches.duplicate_id
        """,
        (since, since),
    )

    cur.execute(
        """
        WITH candidates AS (
            SELECT
                oc.id AS duplicate_id,
                cli.id AS canonical_id,
                ABS(
                    COALESCE(cli.total_tokens, 0) -
                    (
                        COALESCE(oc.input_tokens, 0) +
                        COALESCE(oc.output_tokens, 0) +
                        COALESCE(oc.cache_read, 0) +
                        COALESCE(oc.cache_write, 0)
                    )
                ) AS token_gap,
                ABS((julianday(cli.timestamp) - julianday(oc.timestamp)) * 86400) AS time_gap
            FROM messages oc
            JOIN model_reference oc_model ON oc_model.id = oc.model_ref_id
            JOIN messages cli
              ON cli.timestamp >= max(?, strftime('%Y-%m-%dT%H:%M:%fZ', oc.timestamp, '-3 seconds'))
             AND cli.timestamp <= strftime('%Y-%m-%dT%H:%M:%fZ', oc.timestamp, '+3 seconds')
             AND ABS((julianday(cli.timestamp) - julianday(oc.timestamp)) * 86400) < 2
            JOIN model_reference cli_model ON cli_model.id = cli.model_ref_id
            WHERE oc.ingest_source LIKE 'openclaw%'
              AND oc.timestamp >= ?
              AND COALESCE(oc.is_excluded, 0) = 0
              AND cli.ingest_source = 'claude_cli'
              AND cli.session_id NOT IN (SELECT session_id FROM direct_claude_sessions)
              -- Both sides were renamed by the ownership convention: the OpenClaw wrapper's counters are now
              -- anthropic/X, the Claude CLI importer's are cli-claude/X. They
              -- still describe one Anthropic call, so they still pair here.
              AND substr(oc_model.model_raw, 1, instr(oc_model.model_raw, '/') - 1)
                    IN ('anthropic', 'claude-cli', 'anthropic-c')
              AND substr(cli_model.model_raw, instr(cli_model.model_raw, '/') + 1) =
                  substr(oc_model.model_raw, instr(oc_model.model_raw, '/') + 1)
              AND COALESCE(cli.input_tokens, 0) = COALESCE(oc.input_tokens, 0)
              AND COALESCE(cli.cache_read, 0) = COALESCE(oc.cache_read, 0)
              AND COALESCE(cli.cache_write, 0) = COALESCE(oc.cache_write, 0)
              AND (
                    COALESCE(oc.input_tokens, 0) +
                    COALESCE(oc.output_tokens, 0) +
                    COALESCE(oc.cache_read, 0) +
                    COALESCE(oc.cache_write, 0)
                  ) > 0
              AND (
                    ABS(
                        COALESCE(cli.total_tokens, 0) -
                        (
                            COALESCE(oc.input_tokens, 0) +
                            COALESCE(oc.output_tokens, 0) +
                            COALESCE(oc.cache_read, 0) +
                            COALESCE(oc.cache_write, 0)
                        )
                    ) * 1.0 /
                    (
                        COALESCE(oc.input_tokens, 0) +
                        COALESCE(oc.output_tokens, 0) +
                        COALESCE(oc.cache_read, 0) +
                        COALESCE(oc.cache_write, 0)
                    )
                  ) < 0.10
        ),
        ranked AS (
            SELECT
                duplicate_id,
                canonical_id,
                ROW_NUMBER() OVER (
                    PARTITION BY duplicate_id
                    ORDER BY token_gap, time_gap, canonical_id
                ) AS duplicate_rank,
                ROW_NUMBER() OVER (
                    PARTITION BY canonical_id
                    ORDER BY token_gap, time_gap, duplicate_id
                ) AS canonical_rank
            FROM candidates
        ),
        matches AS MATERIALIZED (
            SELECT duplicate_id, canonical_id
            FROM ranked
            WHERE duplicate_rank = 1 AND canonical_rank = 1
        )
        UPDATE messages
        SET duplicate_of = matches.canonical_id,
            excluded_reason = 'duplicate_claude_wrapper_cli',
            is_excluded = 1
        FROM matches
        WHERE messages.id = matches.duplicate_id
        """,
        (since, since),
    )

    cur.execute(
        "DELETE FROM message_attributions WHERE reason IN (?, ?)",
        attribution_reasons,
    )
    build_runtime_attributions(conn, routes)
    reconcile_claude_turn_usage(conn, since)
    # Threads mirrored into two OpenClaw sessions are not routed, but their
    # per-call rows can still be attributed through a proved twin; the turn
    # rows are identified by message id either way.
    supersede_routed_openclaw_turn_rows(cur, all_routes)
    rows = cur.execute(
        """
        SELECT excluded_reason, COUNT(*), COALESCE(SUM(total_tokens), 0)
        FROM messages
        WHERE is_excluded = 1
          AND excluded_reason IN (?, ?, ?, ?)
        GROUP BY excluded_reason
        ORDER BY excluded_reason
        """,
        reasons,
    ).fetchall()
    current_duplicates = {
        row[0]: (row[1], row[2], row[3])
        for row in cur.execute(
            """
            SELECT id, session_id, duplicate_of, excluded_reason
            FROM messages
            WHERE excluded_reason IN (?, ?, ?, ?)
            """,
            reasons,
        )
    }
    current_attributions = {
        row[0]: (row[1], row[2], row[3], row[4])
        for row in cur.execute(
            """
            SELECT a.source_message_id, m.session_id, a.canonical_session_id,
                   a.canonical_model_ref_id, a.reason
            FROM message_attributions a
            JOIN messages m ON m.id = a.source_message_id
            WHERE a.reason IN (?, ?)
            """,
            attribution_reasons,
        )
    }
    for message_id in set(previous_duplicates) | set(current_duplicates):
        before = previous_duplicates.get(message_id)
        after = current_duplicates.get(message_id)
        if before == after:
            continue
        for value in (before, after):
            if value and value[0]:
                touched_sessions.add(value[0])
    for message_id in set(previous_attributions) | set(current_attributions):
        before = previous_attributions.get(message_id)
        after = current_attributions.get(message_id)
        if before == after:
            continue
        for value in (before, after):
            if not value:
                continue
            source_session_id, canonical_session_id = value[0], value[1]
            if source_session_id:
                touched_sessions.add(source_session_id)
            if canonical_session_id:
                touched_sessions.add(canonical_session_id)
    touched_sessions.update(reconcile_session_owners(conn))
    for session_id in touched_sessions:
        recompute_segments(conn, session_id)
    removed_model_refs = cleanup_obsolete_codex_cli_model_refs(conn)
    conn.commit()

    summary = {reason: {"calls": 0, "tokens": 0} for reason in reasons}
    for reason, calls, tokens in rows:
        summary[reason] = {"calls": int(calls), "tokens": int(tokens)}
    print(
        "✓ Reconciled duplicates: "
        f"{summary['duplicate_codex_app_server_openclaw']['calls']} Codex, "
        f"{summary['duplicate_claude_wrapper_cli']['calls']} Claude wrapper, "
        f"{summary[CLAUDE_TURN_REASON]['calls']} Claude turn rows, "
        f"{summary[SUPERSEDED_BY_CODEX_RUNTIME_REASON]['calls']} Codex turn rows "
        "superseded by per-call usage"
    )
    if removed_model_refs:
        print(f"✓ Removed {removed_model_refs} obsolete codex_cli model references")
    return touched_sessions, summary

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


def extract_text_content(content):
    """Return plain text from OpenClaw message.content across v2/v3 shapes."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, dict) and item.get("type") == "text" and item.get("text"):
                parts.append(str(item["text"]))
        return "\n".join(parts).strip()
    return ""


def load_openclaw_session_index(force_refresh=False):
    """Map OpenClaw session_id -> sessions.json metadata for current v3 logs."""
    global _OPENCLAW_SESSION_INDEX_CACHE
    if _OPENCLAW_SESSION_INDEX_CACHE is not None and not force_refresh:
        return _OPENCLAW_SESSION_INDEX_CACHE

    by_session_id = {}
    if SESSIONS_INDEX_PATH.exists():
        try:
            with open(SESSIONS_INDEX_PATH, "r", encoding="utf-8") as f:
                sessions = json.load(f)
            if isinstance(sessions, dict):
                for session_key, meta in sessions.items():
                    if not isinstance(meta, dict):
                        continue
                    sid = meta.get("sessionId")
                    if isinstance(sid, str) and sid:
                        enriched = dict(meta)
                        enriched["_sessionKey"] = session_key
                        by_session_id[sid] = enriched
        except Exception:
            pass

    _OPENCLAW_SESSION_INDEX_CACHE = by_session_id
    return by_session_id


def extract_route_info_from_session_index(session_id):
    """Return channel metadata from sessions.json when prompt text no longer embeds it."""
    meta = load_openclaw_session_index().get(session_id)
    if not meta:
        return None, None, "gui", None

    channel = meta.get("channel") or meta.get("lastChannel")
    chat_type = meta.get("chatType")
    group_id = meta.get("groupId")
    display_name = meta.get("displayName") or ""
    session_key = meta.get("_sessionKey") or ""

    surface = channel if channel in ("discord", "telegram") else "gui"
    if chat_type == "direct":
        surface = "dm"

    channel_id = str(group_id) if group_id else None
    channel_name = None
    if channel_id:
        channel_name = CHANNEL_ID_TO_NAME.get(channel_id)
    if not channel_name and "#" in display_name:
        channel_name = "#" + display_name.rsplit("#", 1)[-1]
    if not channel_name and ":channel:" in session_key:
        channel_id = session_key.rsplit(":channel:", 1)[-1]
        channel_name = CHANNEL_ID_TO_NAME.get(channel_id, f"#{channel_id}")
    if not channel_name and surface == "dm":
        channel_name = "DM"

    source = surface if surface in ("discord", "telegram", "dm") else None
    return channel_id, channel_name, surface, source


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


def get_session_storage_name(filepath: Path, source: str) -> str:
    """Stable storage key for session files across multiple source roots.

    Main OpenClaw logs deliberately keep their historical bare filename keys so
    existing ingest_state rows remain valid. Other roots get a source prefix to
    avoid basename collisions (for example fastbot:<uuid>.jsonl).
    """
    if source == "claude":
        try:
            rel = filepath.relative_to(CLAUDE_PROJECTS_DIR)
            return f"claude:{rel.as_posix()}"
        except ValueError:
            return f"claude:{filepath.name}"
    if source == "codex_cli":
        app_root = codex_app_server_root_for(filepath)
        if app_root:
            rel = Path(filepath).resolve().relative_to(app_root["dir"].resolve())
            return f"codex_cli:codex-home/{app_root['agent']}/{rel.as_posix()}"
        try:
            rel = filepath.resolve().relative_to(CODEX_SESSIONS_DIR.resolve())
            return f"codex_cli:{rel.as_posix()}"
        except ValueError:
            return f"codex_cli:{filepath.name}"
    if source == "kimi_code":
        try:
            rel = filepath.resolve().relative_to(KIMI_CODE_DIR.resolve())
            return f"kimi_code:{rel.as_posix()}"
        except ValueError:
            return f"kimi_code:{filepath.name}"
    if source == "main":
        return os.path.basename(filepath)
    return f"{source}:{os.path.basename(filepath)}"


def is_openclaw_session_log(filepath: Path) -> bool:
    """True for normal OpenClaw session logs; false for trajectory sidecars."""
    name = filepath.name
    if name.endswith(".trajectory.jsonl") or name.endswith(".trajectory-path.json") or name.endswith(".zst"):
        return False
    return name.endswith(".jsonl") or ".jsonl.deleted." in name or ".jsonl.reset." in name


def collect_openclaw_session_files(source: dict):
    sessions_dir = source["dir"]
    if not sessions_dir.exists():
        return []
    session_files = [p for p in sessions_dir.glob("*.jsonl") if is_openclaw_session_log(p)]
    deleted_files = [p for p in sessions_dir.glob("*.jsonl.deleted.*") if is_openclaw_session_log(p)]
    reset_files = [p for p in sessions_dir.glob("*.jsonl.reset.*") if is_openclaw_session_log(p)]
    return sorted(session_files) + sorted(deleted_files) + sorted(reset_files)


def collect_codex_session_files():
    files = []
    if CODEX_SESSIONS_DIR.exists():
        files.extend(CODEX_SESSIONS_DIR.rglob("*.jsonl"))
    for root in codex_app_server_roots():
        files.extend(root["dir"].rglob("*.jsonl"))
    return sorted(files)


def collect_kimi_session_files():
    sessions_dir = KIMI_CODE_DIR / "sessions"
    if not sessions_dir.exists():
        return []
    return sorted(sessions_dir.glob("*/session_*/agents/*/wire.jsonl"))


def collect_openclaw_sqlite_sources():
    """Return live per-agent OpenClaw SQLite session stores."""
    sources = []
    if not OPENCLAW_AGENTS_DIR.exists():
        return sources
    for agent_dir in sorted(OPENCLAW_AGENTS_DIR.iterdir()):
        db_path = agent_dir / "agent" / "openclaw-agent.sqlite"
        if db_path.is_file():
            sources.append({"agent": agent_dir.name, "path": db_path})
    return sources


def _iso_from_millis(value):
    try:
        return datetime.fromtimestamp(float(value) / 1000, timezone.utc).isoformat(
            timespec="milliseconds"
        ).replace("+00:00", "Z")
    except (TypeError, ValueError, OSError):
        return None


def _openclaw_sqlite_session_metadata(row):
    entry = {}
    try:
        parsed = json.loads(row["entry_json"] or "{}")
        if isinstance(parsed, dict):
            entry = parsed
    except (TypeError, json.JSONDecodeError):
        pass

    session_key = row["session_key"] or ""
    delivery = entry.get("delivery") if isinstance(entry.get("delivery"), dict) else {}
    origin = delivery.get("origin") if isinstance(delivery.get("origin"), dict) else {}
    display_name = entry.get("displayName") or row["display_name"]
    label = row["label"] or entry.get("label")

    channel_id = origin.get("nativeChannelId")
    if not channel_id:
        match = re.search(r":channel:(\d+)$", session_key)
        channel_id = match.group(1) if match else None

    channel_name = CHANNEL_ID_TO_NAME.get(str(channel_id)) if channel_id else None
    if not channel_name and isinstance(display_name, str):
        match = re.search(r"(#[^\s]+)$", display_name)
        channel_name = match.group(1) if match else None

    if ":discord:" in session_key:
        channel_type = "discord"
    elif ":telegram:" in session_key:
        channel_type = "telegram"
    elif origin.get("surface"):
        channel_type = origin.get("surface")
    else:
        channel_type = "gui"

    if ":subagent:" in session_key:
        session_type = "subagent"
    elif ":cron:" in session_key:
        session_type = "cron"
    elif ":acp:" in session_key:
        session_type = "acp"
    elif ":discord:" in session_key:
        session_type = "discord"
    elif ":telegram:" in session_key:
        session_type = "telegram"
    else:
        session_type = "gui"

    return {
        "parent_nickname": label,
        "created_at": _iso_from_millis(entry.get("createdAt") or row["created_at"]),
        "channel_id": str(channel_id) if channel_id else None,
        "channel_name": channel_name,
        "channel_type": channel_type,
        "session_type": session_type,
    }


def collect_scheduled_owner_keys():
    """Read supported scheduler output, not credential-bearing config files."""
    owners = {}
    try:
        result = subprocess.run(["openclaw", "cron", "list", "--all", "--json"],
                                capture_output=True, text=True, timeout=20, check=True)
        payload = json.loads(result.stdout)
        if payload.get("hasMore"):
            raise ValueError("scheduler list is incomplete")
        for job in payload.get("jobs", []):
            policy = job.get("scheduledToolPolicy") or {}
            key = policy.get("ownerSessionKey") or (job.get("owner") or {}).get("sessionKey")
            if key and job.get("sessionTarget") == "current":
                owners[job["id"]] = key
    except (OSError, subprocess.SubprocessError, ValueError) as exc:
        print(f"WARNING: scheduled owner discovery unavailable: {type(exc).__name__}")
    return owners


def reconcile_session_owners(conn, scheduled_owners=None):
    """Collapse explicit continuations and current-target automation evidence."""
    owners = collect_scheduled_owner_keys() if scheduled_owners is None else scheduled_owners
    cur = conn.cursor()
    existing = {r[0] for r in cur.execute("SELECT session_id FROM sessions")}
    # Retain proved historical ownership if a job is retired or discovery fails.
    routes = dict(cur.execute("""SELECT m.session_id, MIN(a.canonical_session_id)
        FROM message_attributions a JOIN messages m ON m.id=a.source_message_id
        WHERE a.reason='openclaw_session_owner' AND m.ingest_source LIKE 'openclaw%'
          AND a.canonical_session_id != m.session_id
        GROUP BY m.session_id HAVING COUNT(DISTINCT a.canonical_session_id)=1"""))
    touched = set()
    for source in collect_openclaw_sqlite_sources():
        db = openclaw_source_connection(source['path'])
        db.row_factory = sqlite3.Row
        try:
            nodes = {r["session_key"]: r for r in db.execute(
                "SELECT session_key, current_session_id, entry_json, created_at, label, display_name FROM session_nodes")}
            windows = db.execute("SELECT session_id, session_key, previous_session_id FROM session_windows").fetchall()
            try:
                archived = db.execute("SELECT DISTINCT session_id, session_key, NULL AS previous_session_id FROM session_transcript_archives").fetchall()
                live_ids = {r["session_id"] for r in windows}
                windows.extend(r for r in archived if r["session_id"] not in live_ids)
            except sqlite3.OperationalError:
                pass
            by_id = {r["session_id"]: r for r in windows}
            for window in windows:
                sid, key = window["session_id"], window["session_key"]
                if sid not in existing:
                    continue
                node = nodes.get(key)
                # Restore historical source metadata without replaying counters.
                metadata_row = dict(node) if node else dict(session_key=key, entry_json='{}',
                    created_at=None, label=None, display_name=None)
                meta = _openclaw_sqlite_session_metadata(metadata_row)
                cur.execute("""UPDATE sessions SET session_type=?, channel_type=?,
                    channel_id=COALESCE(?,channel_id), channel_name=COALESCE(?,channel_name),
                    parent_nickname=COALESCE(?,parent_nickname)
                    WHERE session_id=? AND filename NOT LIKE 'claude:%'
                      AND filename NOT LIKE 'codex_cli:%'""",
                    (meta['session_type'],meta['channel_type'],meta['channel_id'],
                     meta['channel_name'],meta['parent_nickname'],sid))
                match = re.search(r":cron:([^:]+)", key)
                owner_key = owners.get(match.group(1)) if match else None
                target_node = nodes.get(owner_key) if owner_key else None
                if target_node and target_node["current_session_id"] in existing:
                    routes.setdefault(sid, target_node["current_session_id"])
                # Explicit previous-session edges only: no matching-name merges.
                previous = window["previous_session_id"]
                if previous in existing and previous in by_id and by_id[previous]["session_key"] == key:
                    routes[previous] = sid
        except sqlite3.OperationalError:
            pass  # old source stores without durable windows
        finally:
            db.close()
    def resolve(sid):
        seen = set()
        while sid in routes and sid not in seen:
            seen.add(sid)
            sid = routes[sid]
        return None if sid in seen else sid
    cur.execute("""UPDATE message_attributions SET reason='session_model_identity'
        WHERE reason='openclaw_session_owner' AND canonical_session_id=
              (SELECT session_id FROM messages WHERE id=source_message_id)""")
    for mid, sid, raw, kind in cur.execute(
        """SELECT m.id,m.session_id,mr.model_raw,s.session_type FROM messages m
           JOIN sessions s ON s.session_id=m.session_id
           JOIN model_reference mr ON mr.id=m.model_ref_id
           WHERE mr.model_raw LIKE 'anthropic-c/%'
             AND COALESCE(m.is_excluded,0)=0"""
    ).fetchall():
        model_id = get_model_ref_id(cur, ("cli-claude/" if kind == "cli" else "anthropic/") + raw.split("/",1)[1])
        cur.execute("""INSERT INTO message_attributions
            (source_message_id,canonical_session_id,canonical_model_ref_id,reason)
            VALUES (?,?,?,'session_model_identity')
            ON CONFLICT(source_message_id) DO UPDATE SET canonical_model_ref_id=excluded.canonical_model_ref_id""",
            (mid,sid,model_id))
        touched.add(sid)
    # Persist owner routes in the existing attribution relation; raw logs stay put.
    for source_sid in routes:
        owner = resolve(source_sid)
        if not owner or owner == source_sid:
            continue
        for mid, model_id in cur.execute(
            "SELECT id, model_ref_id FROM messages WHERE session_id=? AND ingest_source LIKE 'openclaw%'",
            (source_sid,)).fetchall():
            cur.execute("""INSERT OR REPLACE INTO message_attributions
                (source_message_id,canonical_session_id,canonical_model_ref_id,reason)
                VALUES (?,?,?,'openclaw_session_owner')""", (mid,owner,model_id))
        # Runtime attributions may point to one of those wrapper sessions.
        cur.execute("UPDATE message_attributions SET canonical_session_id=? WHERE canonical_session_id=?",
                    (owner,source_sid))
        touched.update((source_sid,owner))
    return touched


def ingest_openclaw_sqlite(conn):
    """Incrementally import usage-bearing assistant events from OpenClaw SQLite.

    Existing OpenClaw JSONL message ids are preserved in SQLite, so the tracker
    primary key naturally replaces old copies instead of duplicating them.
    """
    tracker_cur = conn.cursor()
    touched_session_ids = set()
    total_events = 0
    total_new_events = 0
    processed_sources = 0

    for source in collect_openclaw_sqlite_sources():
        agent_id = source["agent"]
        db_path = source["path"]
        storage_name = f"openclaw_sqlite:{agent_id}:{db_path}"
        state = get_ingest_state(tracker_cur, storage_name)
        last_rowid = int(state.get("last_offset") or 0) if state else 0

        source_conn = openclaw_source_connection(db_path)
        source_conn.row_factory = sqlite3.Row
        try:
            # OpenClaw may rebuild/compact its SQLite store.  In that case the
            # rowid cursor from the previous store can be greater than the
            # current maximum rowid, causing every newly written event to be
            # skipped forever.  Rewind only this source; message ids and
            # INSERT OR REPLACE make the replay idempotent.
            current_max_rowid = int(
                source_conn.execute(
                    "SELECT COALESCE(MAX(rowid), 0) FROM transcript_events"
                ).fetchone()[0]
            )
            if last_rowid > current_max_rowid:
                print(
                    f"  SQLite rowid regression for {agent_id}: "
                    f"cursor {last_rowid} > max {current_max_rowid}; rewinding"
                )
                last_rowid = 0

            node_metadata = {}
            for node in source_conn.execute(
                """
                SELECT session_key, current_session_id, entry_json, created_at,
                       label, display_name
                FROM session_nodes
                """
            ):
                if node["current_session_id"]:
                    node_metadata[node["current_session_id"]] = _openclaw_sqlite_session_metadata(node)

            # Reset generations are durable transcript events even though the
            # owner session id is intentionally sticky.  Scan them on every
            # refresh so databases created before this table existed are
            # backfilled without rewinding the usage-event ingest cursor.
            reset_rows = source_conn.execute(
                """
                SELECT session_id, seq, event_json
                FROM transcript_events
                WHERE json_extract(event_json, '$.type') = 'reset'
                ORDER BY session_id, seq
                """
            ).fetchall()
            for reset_row in reset_rows:
                try:
                    reset_event = json.loads(reset_row["event_json"])
                except (TypeError, json.JSONDecodeError):
                    continue
                reset_ts = reset_event.get("timestamp")
                if not reset_ts:
                    continue
                reset_session_id = reset_row["session_id"]
                exists = tracker_cur.execute(
                    """
                    SELECT 1 FROM session_boundaries
                    WHERE session_id = ? AND source = ? AND event_seq = ?
                    """,
                    (reset_session_id, agent_id, reset_row["seq"]),
                ).fetchone()
                if exists:
                    continue
                if not tracker_cur.execute(
                    "SELECT 1 FROM sessions WHERE session_id = ?",
                    (reset_session_id,),
                ).fetchone():
                    metadata = node_metadata.get(reset_session_id, {})
                    reset_nickname = (
                        reset_session_id[9:13]
                        if reset_session_id and len(reset_session_id) >= 13
                        else reset_session_id
                    )
                    tracker_cur.execute(
                        """
                        INSERT OR IGNORE INTO sessions
                        (session_id, session_nickname, parent_nickname, filename, created_at,
                         channel_id, channel_name, channel_type, session_type, deleted_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
                        """,
                        (
                            reset_session_id,
                            reset_nickname,
                            metadata.get("parent_nickname"),
                            storage_name,
                            metadata.get("created_at") or reset_ts,
                            metadata.get("channel_id"),
                            metadata.get("channel_name"),
                            metadata.get("channel_type") or "gui",
                            metadata.get("session_type") or "gui",
                        ),
                    )
                tracker_cur.execute(
                    """
                    INSERT INTO session_boundaries
                    (session_id, source, event_seq, timestamp, reason)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        reset_session_id,
                        agent_id,
                        reset_row["seq"],
                        reset_ts,
                        reset_event.get("reason"),
                    ),
                )
                touched_session_ids.add(reset_session_id)

            rows = source_conn.execute(
                """
                SELECT rowid, session_id, seq, event_json, created_at
                FROM transcript_events
                WHERE rowid > ?
                ORDER BY rowid
                """,
                (last_rowid,),
            ).fetchall()
            if not rows:
                for session_id in touched_session_ids:
                    recompute_segments(conn, session_id)
                if touched_session_ids:
                    conn.commit()
                continue

            max_rowid = last_rowid
            for row in rows:
                max_rowid = max(max_rowid, int(row["rowid"]))
                try:
                    event = json.loads(row["event_json"])
                except (TypeError, json.JSONDecodeError):
                    continue
                message = event.get("message") if isinstance(event.get("message"), dict) else {}
                usage = message.get("usage") if isinstance(message.get("usage"), dict) else {}
                if event.get("type") != "message" or message.get("role") != "assistant":
                    continue
                if not any(
                    usage.get(key, 0) or 0
                    for key in ("input", "output", "cacheRead", "cacheWrite", "totalTokens")
                ):
                    continue

                session_id = row["session_id"]
                timestamp = event.get("timestamp") or message.get("timestamp")
                metadata = node_metadata.get(session_id, {})
                created_at = metadata.get("created_at") or timestamp or _iso_from_millis(row["created_at"])
                session_nickname = session_id[9:13] if session_id and len(session_id) >= 13 else session_id
                tracker_cur.execute(
                    """
                    INSERT OR IGNORE INTO sessions
                    (session_id, session_nickname, parent_nickname, filename, created_at,
                     channel_id, channel_name, channel_type, session_type, deleted_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
                    """,
                    (
                        session_id,
                        session_nickname,
                        metadata.get("parent_nickname"),
                        storage_name,
                        created_at,
                        metadata.get("channel_id"),
                        metadata.get("channel_name"),
                        metadata.get("channel_type") or "gui",
                        metadata.get("session_type") or "gui",
                    ),
                )
                tracker_cur.execute(
                    """
                    UPDATE sessions
                    SET parent_nickname = COALESCE(parent_nickname, ?),
                        channel_id = COALESCE(channel_id, ?),
                        channel_name = COALESCE(channel_name, ?),
                        channel_type = COALESCE(channel_type, ?),
                        session_type = COALESCE(session_type, ?)
                    WHERE session_id = ?
                    """,
                    (
                        metadata.get("parent_nickname"),
                        metadata.get("channel_id"),
                        metadata.get("channel_name"),
                        metadata.get("channel_type"),
                        metadata.get("session_type"),
                        session_id,
                    ),
                )

                msg_id = event.get("id") or f"openclaw-sqlite:{agent_id}:{session_id}:{row['seq']}"
                existed = tracker_cur.execute(
                    "SELECT 1 FROM messages WHERE id = ?",
                    (msg_id,),
                ).fetchone() is not None
                provider = message.get("provider") or "unknown"
                model_raw = f"{provider}/{message.get('model') or 'unknown'}"
                source_cost = usage.get("cost") if isinstance(usage.get("cost"), dict) else {}
                insert_message_row(
                    tracker_cur,
                    msg_id,
                    session_id,
                    event.get("parentId"),
                    timestamp,
                    model_raw,
                    {
                        "input_tokens": usage.get("input", 0) or 0,
                        "output_tokens": usage.get("output", 0) or 0,
                        "cache_read": usage.get("cacheRead", 0) or 0,
                        "cache_write": usage.get("cacheWrite", 0) or 0,
                        "total_tokens": usage.get("totalTokens", 0) or 0,
                    },
                    {
                        "input": source_cost.get("input", 0) or 0,
                        "output": source_cost.get("output", 0) or 0,
                        "cacheRead": source_cost.get("cacheRead", 0) or 0,
                        "cacheWrite": source_cost.get("cacheWrite", 0) or 0,
                        "total": source_cost.get("total", 0) or 0,
                    },
                    ingest_source=f"openclaw_sqlite:{agent_id}",
                )
                touched_session_ids.add(session_id)
                total_events += 1
                if not existed:
                    total_new_events += 1

            for session_id in touched_session_ids:
                recompute_segments(conn, session_id)
            stat = db_path.stat()
            update_ingest_state(
                tracker_cur,
                storage_name,
                agent_id,
                max_rowid,
                stat.st_size,
                stat.st_mtime,
            )
            processed_sources += 1
            conn.commit()
        finally:
            source_conn.close()

    print(
        f"✓ OpenClaw SQLite: {total_events} usage events processed "
        f"({total_new_events} new) from {processed_sources} source(s)"
    )
    return touched_session_ids


def extract_claude_user_text(data):
    """Extract plain user text from Claude Code log rows."""
    if data.get("type") != "user":
        return None
    message = data.get("message") or {}
    if message.get("role") != "user":
        return None
    content = message.get("content")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for item in content:
            if item.get("type") == "text" and item.get("text"):
                parts.append(item["text"])
        return "\n".join(parts).strip() or None
    return None


def normalize_claude_assistant_row(data):
    """Map a Claude Code assistant row to the existing DB message shape."""
    if data.get("type") != "assistant":
        return None

    msg = data.get("message") or {}
    if msg.get("role") != "assistant":
        return None

    usage = msg.get("usage") or {}
    request_id = data.get("requestId")
    message_id = msg.get("id")
    row_id = request_id or message_id or data.get("uuid")
    model = msg.get("model") or "unknown"
    timestamp = data.get("timestamp")

    if not row_id or not timestamp:
        return None

    input_tokens = int(usage.get("input_tokens") or 0)
    output_tokens = int(usage.get("output_tokens") or 0)
    cache_read = int(usage.get("cache_read_input_tokens") or 0)
    cache_write = int(usage.get("cache_creation_input_tokens") or 0)
    total_tokens = input_tokens + output_tokens + cache_read + cache_write

    model_raw = f"claude/{model}"
    usage_norm = {
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cache_read": cache_read,
        "cache_write": cache_write,
        "total_tokens": total_tokens,
    }
    cost = maybe_calculate_cli_cost(model_raw, usage_norm, None)

    return {
        "id": row_id,
        "parent_id": data.get("parentUuid"),
        "timestamp": timestamp,
        "model_raw": model_raw,
        **usage_norm,
        "cost_input": cost.get("input", 0),
        "cost_output": cost.get("output", 0),
        "cost_cache_read": cost.get("cacheRead", 0),
        "cost_cache_write": cost.get("cacheWrite", 0),
        "cost_total": cost.get("total", 0),
    }


def parse_claude_session_file(filepath, conn):
    """Parse a Claude Code session JSONL and upsert into the existing schema."""
    storage_name = get_session_storage_name(Path(filepath), "claude")
    cur = conn.cursor()

    session_id = None
    session_nickname = None
    created_at = None
    first_user_text = None
    cwd = None
    entrypoint = None
    message_count = 0
    current_model_raw = None
    segment_index = 0
    segment_msgs = []
    seen_row_ids = set()

    def flush_segment():
        nonlocal segment_msgs, segment_index
        if not segment_msgs or not session_id:
            return
        first_ts = min(m["timestamp"] for m in segment_msgs)
        last_ts = max(m["timestamp"] for m in segment_msgs)
        agg = {
            "input_tokens": sum(m["input_tokens"] for m in segment_msgs),
            "output_tokens": sum(m["output_tokens"] for m in segment_msgs),
            "cache_read": sum(m["cache_read"] for m in segment_msgs),
            "cache_write": sum(m["cache_write"] for m in segment_msgs),
            "total_tokens": sum(m["total_tokens"] for m in segment_msgs),
            "cost_input": sum(m["cost_input"] for m in segment_msgs),
            "cost_output": sum(m["cost_output"] for m in segment_msgs),
            "cost_cache_read": sum(m["cost_cache_read"] for m in segment_msgs),
            "cost_cache_write": sum(m["cost_cache_write"] for m in segment_msgs),
            "cost_total": sum(m["cost_total"] for m in segment_msgs),
        }
        ensure_model_reference(cur, segment_msgs[0]["model_raw"])
        cur.execute("""
            INSERT OR REPLACE INTO segments
            (session_id, model_raw, segment_index, first_msg_ts, last_msg_ts, msg_count,
             input_tokens, output_tokens, cache_read, cache_write, total_tokens,
             cost_input, cost_output, cost_cache_read, cost_cache_write, cost_total,
             cost_source, cost_kind, cost_is_estimated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (session_id, segment_msgs[0]["model_raw"], segment_index, first_ts, last_ts, len(segment_msgs),
              agg["input_tokens"], agg["output_tokens"], agg["cache_read"], agg["cache_write"], agg["total_tokens"],
              agg["cost_input"], agg["cost_output"], agg["cost_cache_read"], agg["cost_cache_write"], agg["cost_total"],
              "ratecard" if agg["cost_total"] else None,
              "payg_equivalent" if agg["cost_total"] else None,
              0 if agg["cost_total"] else None))
        segment_msgs = []
        segment_index += 1

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    data = json.loads(line)
                except json.JSONDecodeError:
                    continue

                session_id = session_id or data.get("sessionId")
                cwd = cwd or data.get("cwd")
                entrypoint = entrypoint or data.get("entrypoint")
                ts = data.get("timestamp")
                if ts and (created_at is None or ts < created_at):
                    created_at = ts

                user_text = extract_claude_user_text(data)
                if user_text and first_user_text is None:
                    first_user_text = user_text

                row = normalize_claude_assistant_row(data)
                if not row or row["id"] in seen_row_ids:
                    continue
                row["id"] = f"{session_id or data.get('sessionId') or 'claude'}:{row['id']}"
                seen_row_ids.add(row["id"])

                if session_id and session_nickname is None:
                    session_nickname = session_id[9:13] if len(session_id) >= 13 else session_id
                    cur.execute(
                        """INSERT OR IGNORE INTO sessions
                           (session_id, session_nickname, filename, created_at, channel_type, session_type)
                           VALUES (?, ?, ?, ?, ?, ?)""",
                        (session_id, session_nickname, storage_name, created_at,
                         claude_session_kind(entrypoint), claude_session_kind(entrypoint))
                    )

                row["model_raw"] = cli_prefixed_model_raw(
                    row["model_raw"], claude_session_kind(entrypoint))
                model_ref_id = get_model_ref_id(cur, row["model_raw"])
                cur.execute("""
                    INSERT OR REPLACE INTO messages
                    (id, session_id, parent_id, timestamp, model_ref_id,
                     input_tokens, output_tokens, cache_read, cache_write, total_tokens,
                     cost_input, cost_output, cost_cache_read, cost_cache_write, cost_total,
                     cost_source, cost_kind, cost_is_estimated, ingest_source,
                     duplicate_of, excluded_reason, is_excluded)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            NULL, NULL, 0)
                """, (row["id"], session_id, row["parent_id"], row["timestamp"], model_ref_id,
                      row["input_tokens"], row["output_tokens"], row["cache_read"], row["cache_write"], row["total_tokens"],
                      row["cost_input"], row["cost_output"], row["cost_cache_read"], row["cost_cache_write"], row["cost_total"],
                      "ratecard" if row["cost_total"] else None,
                      "payg_equivalent" if row["cost_total"] else None,
                      0 if row["cost_total"] else None,
                      "claude_cli"))

                if current_model_raw is not None and row["model_raw"] != current_model_raw:
                    flush_segment()
                current_model_raw = row["model_raw"]
                segment_msgs.append(row)
                message_count += 1

        flush_segment()

        if session_id:
            label = None
            if cwd:
                label = Path(cwd).name or None
            cur.execute("""
                INSERT OR REPLACE INTO sessions
                (session_id, session_nickname, parent_nickname, filename, created_at,
                 channel_id, channel_name, channel_type, session_type, deleted_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (session_id, session_nickname, label, storage_name, created_at,
                  None, None, claude_session_kind(entrypoint),
                  claude_session_kind(entrypoint), None))
            return True, message_count, session_id
    except Exception as e:
        print(f"  Error reading Claude session {filepath}: {e}")
        return False, 0, None

    return False, 0, None


def collect_claude_session_files():
    if not CLAUDE_PROJECTS_DIR.exists():
        return []
    files = []
    for path in CLAUDE_PROJECTS_DIR.rglob("*.jsonl"):
        if "subagents" in path.parts:
            continue
        files.append(path)
    return sorted(files)


def parse_session_file(filepath, conn, source_key="main"):
    """Parse a single OpenClaw session file and insert into database."""
    if source_key == "codex_cli":
        return parse_codex_session_file(filepath, conn)
    if source_key == "kimi_code":
        return parse_kimi_session_file(filepath, conn)
    filepath = Path(filepath)
    filename = os.path.basename(filepath)
    storage_name = get_session_storage_name(filepath, source_key)
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
        segment_cost_is_estimated = 1 if any(m.get('cost_is_estimated') for m in segment_msgs) else 0
        segment_cost_source = PIONEER_INPUT_ESTIMATE_SOURCE if segment_cost_is_estimated else None
        segment_cost_kind = "payg_estimated_input" if segment_cost_is_estimated else None
        ensure_model_reference(cur, segment_msgs[0]['model_raw'])
        cur.execute("""
            INSERT OR REPLACE INTO segments
            (session_id, model_raw, segment_index, first_msg_ts, last_msg_ts, msg_count,
             input_tokens, output_tokens, cache_read, cache_write, total_tokens,
             cost_input, cost_output, cost_cache_read, cost_cache_write, cost_total,
             cost_source, cost_kind, cost_is_estimated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (session_id, segment_msgs[0]['model_raw'], segment_index, first_ts, last_ts, len(segment_msgs),
              agg['input_tokens'], agg['output_tokens'], agg['cache_read'], agg['cache_write'], agg['total_tokens'],
              agg['cost_input'], agg['cost_output'], agg['cost_cache_read'], agg['cost_cache_write'], agg['cost_total'],
              segment_cost_source, segment_cost_kind, segment_cost_is_estimated))
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
                                (session_id, session_nickname, storage_name, created_at)
                            )

                    # User message - extract channel info + capture first user text
                    elif data.get("type") == "message" and data.get("message", {}).get("role") == "user":
                        msg = data.get("message", {})
                        user_text = extract_text_content(msg.get("content"))
                        # Capture first user text for classification
                        if user_text and first_user_text is None:
                            first_user_text = user_text
                        cid, cname, csurf = extract_channel_info(user_text)
                        if cid:
                            channel_id = cid
                        if cname:
                            channel_name = cname
                        if csurf != "gui":
                            surface = csurf
                        elif msg.get("sourceChannel") in ("discord", "telegram"):
                            surface = msg.get("sourceChannel")

                    # Assistant message - token/cost data
                    elif data.get("type") == "message" and data.get("message", {}).get("role") == "assistant":
                        msg = data.get("message", {})
                        msg_id = data.get("id")
                        parent_id = data.get("parentId")
                        timestamp = data.get("timestamp")

                        provider = msg.get("provider", "unknown")
                        model_raw = f"{provider}/{msg.get('model', 'unknown')}"

                        # FK safety: keep model_reference in sync with new/unknown models.
                        model_ref_id = get_model_ref_id(cur, model_raw)

                        # Skip openclaw/delivery-mirror - don't create segments for these
                        is_mirror = provider == "openclaw" and model_raw.endswith("/delivery-mirror")

                        usage = msg.get("usage", {})
                        usage_norm = {
                            "input_tokens": usage.get("input", 0),
                            "output_tokens": usage.get("output", 0),
                            "cache_read": usage.get("cacheRead", 0),
                            "cache_write": usage.get("cacheWrite", 0),
                            "total_tokens": usage.get("totalTokens", 0),
                        }

                        cost = usage.get("cost", {})
                        usage_norm, cost, cost_source, cost_kind, cost_is_estimated = apply_pioneer_input_estimate(
                            model_raw, timestamp, usage_norm, cost
                        )
                        input_tokens = usage_norm.get("input_tokens", 0)
                        output_tokens = usage_norm.get("output_tokens", 0)
                        cache_read = usage_norm.get("cache_read", 0)
                        cache_write = usage_norm.get("cache_write", 0)
                        total_tokens = usage_norm.get("total_tokens", 0)
                        cost_input = cost.get("input", 0) if cost else 0
                        cost_output = cost.get("output", 0) if cost else 0
                        cost_cache_read = cost.get("cacheRead", 0) if cost else 0
                        cost_cache_write = cost.get("cacheWrite", 0) if cost else 0
                        cost_total = cost.get("total", 0) if cost else 0

                        # Insert message (still record mirror messages, just don't segment on them)
                        cur.execute("""
                            INSERT OR REPLACE INTO messages
                            (id, session_id, parent_id, timestamp, model_ref_id,
                             input_tokens, output_tokens, cache_read, cache_write, total_tokens,
                             cost_input, cost_output, cost_cache_read, cost_cache_write, cost_total,
                             cost_source, cost_kind, cost_is_estimated, ingest_source,
                             duplicate_of, excluded_reason, is_excluded)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                                    NULL, NULL, 0)
                        """, (msg_id, session_id, parent_id, timestamp, model_ref_id,
                              input_tokens, output_tokens, cache_read, cache_write, total_tokens,
                              cost_input, cost_output, cost_cache_read, cost_cache_write, cost_total,
                              cost_source, cost_kind, cost_is_estimated,
                              f"openclaw_jsonl:{source_key}"))

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
                            'cost_source': cost_source,
                            'cost_kind': cost_kind,
                            'cost_is_estimated': cost_is_estimated,
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
            indexed_channel_id, indexed_channel_name, indexed_surface, indexed_source = extract_route_info_from_session_index(session_id)
            if indexed_channel_id and not channel_id:
                channel_id = indexed_channel_id
            if indexed_channel_name and not channel_name:
                channel_name = indexed_channel_name
            if indexed_surface != "gui" and surface == "gui":
                surface = indexed_surface

            # Look up channel name from ID if we have ID but no name
            if channel_id and not channel_name:
                channel_name = CHANNEL_ID_TO_NAME.get(channel_id, f"#{channel_id}")

            # Classify source and label
            source, label = infer_source_and_label(first_user_text, filename)

            if source_key == "fastbot":
                source = "agent"
                label = "fastbot"
                channel_name = "fastbot"
                surface = "agent"
            # If source is still "gui" but we detected a surface, upgrade it
            elif source == "gui" and surface in ("discord", "telegram", "dm"):
                source = surface
            elif source == "gui" and indexed_source:
                source = indexed_source

            cur.execute("""
                INSERT OR REPLACE INTO sessions
                (session_id, session_nickname, parent_nickname, filename, created_at,
                 channel_id, channel_name, channel_type, session_type, deleted_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (session_id, session_nickname, label, storage_name, created_at,
                  channel_id, channel_name, surface, source, deleted_at))

            return True, message_count, session_id

    except Exception as e:
        print(f"  Error reading {filename}: {e}")
        return False, 0, None

    return False, 0, None


def parse_codex_session_file(filepath, conn):
    """Parse a Codex CLI transcript and normalize token-count events into assistant usage rows."""
    filepath = Path(filepath)
    filename = get_session_storage_name(filepath, "codex_cli")
    cur = conn.cursor()

    session_id = extract_codex_session_id_from_file(filepath)
    session_meta = read_codex_session_meta(filepath)
    # App-server rollouts under agent codex-home are the authoritative usage
    # source: OpenClaw message logs record mostly zero usage for harness turns,
    # so do NOT skip them as "already covered by OpenClaw logs". Classic
    # ~/.codex/sessions files keep the old skip behaviour.
    if (
        session_id
        and not codex_app_server_root_for(filepath)
        and session_meta.get("originator") != "openclaw"
        and session_id in collect_openclaw_codex_thread_ids()
    ):
        return True, 0, session_id

    session_nickname = session_id[9:13] if session_id and len(session_id) >= 13 else session_id
    created_at = None
    message_count = 0
    insert_errors = 0
    current_model = None
    current_model_raw = None
    segment_index = 0
    segment_msgs = []
    seen_token_count_states = set()
    source_label = "codex_cli"
    originator = None

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
            'cost_input': sum(m.get('cost_input', 0) for m in segment_msgs),
            'cost_output': sum(m.get('cost_output', 0) for m in segment_msgs),
            'cost_cache_read': sum(m.get('cost_cache_read', 0) for m in segment_msgs),
            'cost_cache_write': sum(m.get('cost_cache_write', 0) for m in segment_msgs),
            'cost_total': sum(m.get('cost_total', 0) for m in segment_msgs),
        }
        ensure_model_reference(cur, segment_msgs[0]['model_raw'])
        cur.execute(
            """
            INSERT OR REPLACE INTO segments
            (session_id, model_raw, segment_index, first_msg_ts, last_msg_ts, msg_count,
             input_tokens, output_tokens, cache_read, cache_write, total_tokens,
             cost_input, cost_output, cost_cache_read, cost_cache_write, cost_total,
             cost_source, cost_kind, cost_is_estimated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                agg['cost_input'],
                agg['cost_output'],
                agg['cost_cache_read'],
                agg['cost_cache_write'],
                agg['cost_total'],
                'ratecard' if agg['cost_total'] else None,
                'payg_equivalent' if agg['cost_total'] else None,
                0 if agg['cost_total'] else None,
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
                    source_label = normalize_codex_source_label(payload.get("source") or source_label)
                    originator = payload.get("originator") or originator
                    ensure_session_stub()
                    continue

                if data.get("type") != "event_msg" or payload.get("type") != "token_count":
                    continue

                info = payload.get("info") if isinstance(payload.get("info"), dict) else {}
                last_usage = info.get("last_token_usage")
                if not isinstance(last_usage, dict):
                    continue
                dedupe_key = codex_token_count_dedupe_key(info)
                if dedupe_key in seen_token_count_states:
                    continue
                seen_token_count_states.add(dedupe_key)

                if not session_id:
                    session_id = extract_codex_session_id_from_path(filepath)
                    session_nickname = session_id[9:13] if session_id and len(session_id) >= 13 else session_id
                if not created_at:
                    created_at = data.get("timestamp")
                ensure_session_stub()

                is_openclaw_runtime = codex_is_openclaw_runtime(originator, codex_app_server_root_for(filepath))
                provider = "openai" if is_openclaw_runtime else "cli-codex"
                model_raw = f"{provider}/{current_model or 'unknown'}"
                timestamp = data.get("timestamp")
                msg_id = f"{session_id}:codex:{line_num}"
                normalized_usage = normalize_codex_usage(last_usage)
                insert_message_row(
                    cur,
                    msg_id,
                    session_id,
                    None,
                    timestamp,
                    model_raw,
                    normalized_usage,
                    ingest_source=(
                        "codex_app_server"
                        if is_openclaw_runtime
                        else "codex_cli"
                    ),
                )

                if current_model_raw is not None and model_raw != current_model_raw:
                    flush_segment()
                current_model_raw = model_raw
                msg_cost = maybe_calculate_cli_cost(model_raw, normalized_usage, None)
                segment_msgs.append({
                    'timestamp': timestamp,
                    'model_raw': model_raw,
                    **normalized_usage,
                    'cost_input': msg_cost.get('input', 0),
                    'cost_output': msg_cost.get('output', 0),
                    'cost_cache_read': msg_cost.get('cacheRead', 0),
                    'cost_cache_write': msg_cost.get('cacheWrite', 0),
                    'cost_total': msg_cost.get('total', 0),
                })
                message_count += 1

        flush_segment()

        if insert_errors:
            print(f"  WARNING: {insert_errors} line(s) failed during parsing of {filename}")
        if session_id:
            is_openclaw_runtime = codex_is_openclaw_runtime(originator, codex_app_server_root_for(filepath))
            session_type = "openclaw" if is_openclaw_runtime else "cli"
            channel_type = "openclaw" if is_openclaw_runtime else "cli"
            if is_openclaw_runtime and source_label == "vscode":
                source_label = None
            cur.execute(
                """
                INSERT OR REPLACE INTO sessions
                (session_id, session_nickname, parent_nickname, filename, created_at,
                 channel_id, channel_name, channel_type, session_type, deleted_at)
                VALUES (?, ?, ?, ?, ?, NULL, NULL, ?, ?, NULL)
                """,
                (session_id, session_nickname, source_label, filename, created_at, channel_type, session_type),
            )
            return True, message_count, session_id
    except Exception as e:
        print(f"  Error reading {filename}: {e}")
        return False, 0, None

    return False, 0, None


def migrate_full(conn):
    """Full migration - rebuild entire database."""
    print("\n=== Full Migration ===")

    # Initialize schema
    init_db(conn)

    # Populate reference tables
    populate_reference_tables(conn)
    migrate_compact_schema(conn)

    # Parse all OpenClaw session files (including deleted/reset) from all configured agents
    openclaw_files = []
    for source in OPENCLAW_SESSION_SOURCES:
        files = collect_openclaw_session_files(source)
        print(f"\nFound {len(files)} {source['label']} session files")
        openclaw_files.extend((source, filepath) for filepath in files)
    codex_files = collect_codex_session_files()
    print(f"Found {len(codex_files)} {CODEX_SESSION_SOURCE['label']} session files")
    kimi_files = collect_kimi_session_files()
    print(f"Found {len(kimi_files)} {KIMI_SESSION_SOURCE['label']} session files")
    all_files = (
        openclaw_files
        + [(CODEX_SESSION_SOURCE, filepath) for filepath in codex_files]
        + [(KIMI_SESSION_SOURCE, filepath) for filepath in kimi_files]
    )

    success_count = 0
    total_messages = 0

    cur = conn.cursor()
    for i, (source, filepath) in enumerate(all_files):
        if (i + 1) % 100 == 0:
            print(f"  Processing {i+1}/{len(all_files)}...")
            conn.commit()

        success, msg_count, session_id = parse_session_file(filepath, conn, source["key"])
        if success:
            success_count += 1
            total_messages += msg_count
            # Update ingest_state so incremental runs don't re-parse these files
            st = filepath.stat()
            storage_name = get_session_storage_name(filepath, source["key"])
            update_ingest_state(cur, storage_name, session_id, st.st_size, st.st_size, st.st_mtime)

    claude_files = collect_claude_session_files()
    print(f"Found {len(claude_files)} Claude session files")
    for i, filepath in enumerate(claude_files):
        if (i + 1) % 100 == 0:
            print(f"  Processing Claude {i+1}/{len(claude_files)}...")
            conn.commit()

        success, msg_count, session_id = parse_claude_session_file(filepath, conn)
        if success:
            st = filepath.stat()
            storage_name = get_session_storage_name(filepath, "claude")
            update_ingest_state(cur, storage_name, session_id, st.st_size, st.st_size, st.st_mtime)
            success_count += 1
            total_messages += msg_count

    conn.commit()
    print(f"\n✓ Migrated {success_count}/{len(all_files) + len(claude_files)} sessions")
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


def migrate_incremental(conn, include_openclaw=True, include_claude=True):
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

    total_processed_msgs = 0
    processed_files = 0
    touched_session_ids = set()

    if include_openclaw:
        openclaw_files = []
        for source in OPENCLAW_SESSION_SOURCES:
            openclaw_files.extend((source, filepath) for filepath in collect_openclaw_session_files(source))
        openclaw_files.extend((CODEX_SESSION_SOURCE, filepath) for filepath in collect_codex_session_files())
        openclaw_files.extend((KIMI_SESSION_SOURCE, filepath) for filepath in collect_kimi_session_files())

        to_process = []  # (source, filepath, start_offset)

        for source, filepath in openclaw_files:
            storage_name = get_session_storage_name(filepath, source["key"])
            st = filepath.stat()
            state = get_ingest_state(cur, storage_name)

            if not state:
                to_process.append((source, filepath, 0))
                continue

            last_offset = int(state.get('last_offset') or 0)
            last_mtime = state.get('last_mtime')

            if st.st_size < last_offset:
                to_process.append((source, filepath, 0))
                continue
            if st.st_size > last_offset:
                to_process.append((source, filepath, last_offset))
                continue
            if last_mtime is not None and st.st_mtime > float(last_mtime) and st.st_size == last_offset:
                to_process.append((source, filepath, 0))

        print(f"Found {len(to_process)} OpenClaw/Codex/Kimi session files needing ingest")

        for source, filepath, start_offset in to_process:
            filename = os.path.basename(filepath)
            storage_name = get_session_storage_name(filepath, source["key"])
            st = filepath.stat()
            state = get_ingest_state(cur, storage_name)
            known_session_id = state.get('session_id') if state else None

            if start_offset == 0 or not known_session_id:
                success, msg_count, session_id = parse_session_file(filepath, conn, source["key"])
                if not success or not session_id:
                    continue
                recompute_segments(conn, session_id)
                update_ingest_state(cur, storage_name, session_id, st.st_size, st.st_size, st.st_mtime)
                processed_files += 1
                total_processed_msgs += msg_count
                touched_session_ids.add(session_id)
                continue

            processed = ingest_messages_from_offset(filepath, conn, start_offset, known_session_id, source["key"])
            recompute_segments(conn, known_session_id)
            touched_session_ids.add(known_session_id)
            update_ingest_state(cur, storage_name, known_session_id, st.st_size, st.st_size, st.st_mtime)
            processed_files += 1
            total_processed_msgs += processed

            is_deleted, deleted_ts = detect_deleted(filename)
            if is_deleted:
                cur.execute("UPDATE sessions SET deleted_at = COALESCE(deleted_at, ?) WHERE session_id = ?", (deleted_ts, known_session_id))

    if include_claude:
        claude_files = collect_claude_session_files()
        claude_to_process = []
        for filepath in claude_files:
            storage_name = get_session_storage_name(filepath, "claude")
            st = filepath.stat()
            state = get_ingest_state(cur, storage_name)
            if not state:
                claude_to_process.append(filepath)
                continue
            last_offset = int(state.get("last_offset") or 0)
            last_mtime = state.get("last_mtime")
            if st.st_size != last_offset:
                claude_to_process.append(filepath)
                continue
            if last_mtime is not None and st.st_mtime > float(last_mtime):
                claude_to_process.append(filepath)

        print(f"Found {len(claude_to_process)} Claude session files needing ingest")

        for filepath in claude_to_process:
            st = filepath.stat()
            success, msg_count, session_id = parse_claude_session_file(filepath, conn)
            if not success or not session_id:
                continue
            recompute_segments(conn, session_id)
            touched_session_ids.add(session_id)
            storage_name = get_session_storage_name(filepath, "claude")
            update_ingest_state(cur, storage_name, session_id, st.st_size, st.st_size, st.st_mtime)
            processed_files += 1
            total_processed_msgs += msg_count

    if processed_files == 0:
        print("✓ Database is up to date")
        return touched_session_ids

    conn.commit()
    print(f"✓ Processed {processed_files} files")
    print(f"✓ Assistant messages ingested/upserted: {total_processed_msgs}")
    return touched_session_ids




def parse_args():
    ap = argparse.ArgumentParser(
        description="Ingest OpenClaw SQLite/JSONL and CLI usage into an AI Cost Tracker database."
    )
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--full", action="store_true", help="Rebuild the database from raw sources.")
    mode.add_argument("--incremental", action="store_true", help="Append new data (default for an existing DB).")
    ap.add_argument("--db", default=str(DB_PATH), help="SQLite database path.")
    ap.add_argument("--sessions-dir", default=str(SESSIONS_DIR), help="Primary OpenClaw JSONL sessions directory.")
    ap.add_argument("--openclaw-agents-dir", default=str(OPENCLAW_AGENTS_DIR), help="OpenClaw agents root containing per-agent SQLite stores and session directories.")
    ap.add_argument("--codex-sessions-dir", default=str(CODEX_SESSIONS_DIR), help="Classic Codex CLI sessions directory.")
    ap.add_argument("--claude-projects-dir", default=str(CLAUDE_PROJECTS_DIR), help="Claude Code projects directory.")
    ap.add_argument("--kimi-code-dir", default=str(KIMI_CODE_DIR), help="Kimi Code data directory.")
    ap.add_argument("--channel-mapping", default=None, help="Optional JSON channel-id mapping.")
    ap.add_argument("--backups-dir", default=str(BACKUPS_DIR), help="Rolling backup directory.")
    ap.add_argument("--openclaw-sqlite", dest="openclaw_sqlite", action="store_true", default=True, help="Ingest per-agent OpenClaw SQLite stores (default).")
    ap.add_argument("--no-openclaw-sqlite", dest="openclaw_sqlite", action="store_false", help="Disable OpenClaw SQLite ingestion.")
    ap.add_argument("--claude-only", action="store_true", help="Only ingest Claude Code sources.")
    ap.add_argument("--skip-claude", action="store_true", help="Do not ingest Claude Code sources.")
    return ap.parse_args()


def main():
    global DB_PATH, SESSIONS_DIR, SESSIONS_INDEX_PATH, OPENCLAW_AGENTS_DIR
    global OPENCLAW_SESSION_SOURCES, CODEX_SESSIONS_DIR, CODEX_SESSION_SOURCE
    global CODEX_HOME_GLOB, CLAUDE_PROJECTS_DIR, KIMI_CODE_DIR, KIMI_SESSION_SOURCE
    global CHANNEL_MAPPING_PATH, BACKUPS_DIR

    if sys.version_info < (3, 14):
        raise SystemExit("Python 3.14+ is required for compressed OpenClaw transcripts.")
    args = parse_args()
    DB_PATH = Path(args.db).expanduser().resolve()
    SESSIONS_DIR = Path(args.sessions_dir).expanduser().resolve()
    SESSIONS_INDEX_PATH = SESSIONS_DIR / "sessions.json"
    OPENCLAW_AGENTS_DIR = Path(args.openclaw_agents_dir).expanduser().resolve()
    CODEX_HOME_GLOB = OPENCLAW_AGENTS_DIR
    CODEX_SESSIONS_DIR = Path(args.codex_sessions_dir).expanduser().resolve()
    CODEX_SESSION_SOURCE = {"key": "codex_cli", "label": "Codex CLI", "dir": CODEX_SESSIONS_DIR}
    CLAUDE_PROJECTS_DIR = Path(args.claude_projects_dir).expanduser().resolve()
    KIMI_CODE_DIR = Path(args.kimi_code_dir).expanduser().resolve()
    KIMI_SESSION_SOURCE = {"key": "kimi_code", "label": "Kimi Code", "dir": KIMI_CODE_DIR}
    CHANNEL_MAPPING_PATH = Path(args.channel_mapping).expanduser().resolve() if args.channel_mapping else None
    BACKUPS_DIR = Path(args.backups_dir).expanduser().resolve()
    OPENCLAW_SESSION_SOURCES = discover_openclaw_session_sources()
    load_channel_mapping(CHANNEL_MAPPING_PATH)

    full_mode = args.full
    claude_only = args.claude_only
    skip_claude = args.skip_claude
    include_openclaw_sqlite = args.openclaw_sqlite
    include_openclaw = not claude_only
    include_claude = not skip_claude or claude_only

    print(f"AI Cost Tracker Migration")
    print(f"Database: {DB_PATH}")
    print("OpenClaw session sources:")
    for source in OPENCLAW_SESSION_SOURCES:
        print(f"  {source['key']}: {source['dir']}")
    print(f"Codex CLI sessions: {CODEX_SESSIONS_DIR}")
    print(f"Kimi Code sessions: {KIMI_CODE_DIR / 'sessions'}")

    available_sources = (
        any(source["dir"].exists() for source in OPENCLAW_SESSION_SOURCES)
        or CODEX_SESSIONS_DIR.exists()
        or CLAUDE_PROJECTS_DIR.exists()
        or (KIMI_CODE_DIR / "sessions").exists()
        or (include_openclaw_sqlite and bool(collect_openclaw_sqlite_sources()))
    )
    if not available_sources:
        print("ERROR: No supported usage sources were found.")
        print(f"  OpenClaw agents: {OPENCLAW_AGENTS_DIR}")
        print(f"  Primary sessions: {SESSIONS_DIR}")
        print(f"  Codex CLI: {CODEX_SESSIONS_DIR}")
        sys.exit(1)

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    BACKUPS_DIR.mkdir(parents=True, exist_ok=True)
    db_existed = DB_PATH.exists()

    # Rolling backup (best-effort)
    maybe_backup_db(DB_PATH)

    # Connect to database
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)
    ensure_pragmas(conn)

    try:
        if full_mode or (not args.incremental and not db_existed):
            migrate_full(conn)
            if include_openclaw_sqlite:
                ingest_openclaw_sqlite(conn)
            backfill_message_ingest_sources(conn)
            reconcile_usage_duplicates(conn)
            backfill_cli_payg_costs(conn)
        else:
            touched = migrate_incremental(conn, include_openclaw=include_openclaw, include_claude=include_claude)
            if include_openclaw_sqlite:
                touched.update(ingest_openclaw_sqlite(conn))
            backfill_message_ingest_sources(conn)
            reconciled_sessions, _ = reconcile_usage_duplicates(conn)
            touched.update(reconciled_sessions)
            backfill_cli_payg_costs(conn, session_ids=touched)
    finally:
        conn.close()

    print("\n✓ Migration complete")


if __name__ == "__main__":
    main()
