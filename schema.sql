-- AI Cost Tracker v4 schema

-- Model reference table (editable)
CREATE TABLE IF NOT EXISTS model_reference (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    model_raw TEXT NOT NULL UNIQUE,  -- e.g. "github-copilot/gpt-5-mini"
    endpoint TEXT NOT NULL,          -- e.g. "gh-copilot"
    author TEXT NOT NULL,            -- e.g. "OpenAI"
    model TEXT NOT NULL,             -- e.g. "gpt-5-mini"
    pru_multiplier REAL DEFAULT 1.0  -- Copilot PRU weighting: 0=free, 1=base, 3=opus, 0.33=cheap
);

-- Provider cost structure (multi-row: one row per billing period per provider)
CREATE TABLE IF NOT EXISTS provider_costs (
    provider TEXT NOT NULL,
    billing_start TEXT NOT NULL,       -- YYYY-MM-DD: when this rate takes effect
    plan_type TEXT NOT NULL,           -- 'flat' | 'flat_plus_usage' | 'payg' | 'free' | 'free_trial'
    monthly_cost REAL,
    extra_usage REAL,
    notes TEXT,
    PRIMARY KEY (provider, billing_start)
);

-- Monthly PRU "invoice" records (manual entry)
-- Intended for GitHub Copilot: record PRUs billed per model per billing cycle.
CREATE TABLE IF NOT EXISTS provider_pru_invoices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    provider TEXT NOT NULL,
    cycle_start TEXT NOT NULL,          -- YYYY-MM-DD (billing cycle start)
    cycle_end TEXT,                     -- YYYY-MM-DD (optional)
    model_raw TEXT NOT NULL,            -- e.g. github-copilot/claude-opus-4.6

    -- Legacy column name; originally used to store "included requests" counts.
    -- Keep for compatibility but prefer included_requests/billed_requests going forward.
    prus REAL NOT NULL,

    included_requests REAL,             -- Included requests this cycle (Copilot UI)
    billed_requests REAL NOT NULL DEFAULT 0,  -- Billed (overage) requests this cycle

    notes TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE(provider, cycle_start, model_raw)
);
CREATE INDEX IF NOT EXISTS idx_provider_pru_invoices_provider_cycle ON provider_pru_invoices(provider, cycle_start);

-- Sessions table
CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT PRIMARY KEY,
    session_nickname TEXT,        -- chars 9-13 of session_id (second group)
    parent_nickname TEXT,         -- Label: cron job label OR subagent task excerpt (nullable)
    filename TEXT,                -- original jsonl filename
    created_at TEXT,
    channel_id TEXT,              -- Discord channel ID
    channel_name TEXT,            -- Parsed channel name (e.g. "#betelgeuse")
    channel_type TEXT,            -- Surface hint: "discord", "telegram", "gui", "dm"
    session_type TEXT,            -- Source: "discord", "telegram", "cron", "subagent", "gui"
    deleted_at TEXT               -- ISO timestamp if deleted
);

-- Messages table (per-message granularity, compacted)
CREATE TABLE IF NOT EXISTS messages (
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
    cost_total REAL,
    cost_source TEXT,
    cost_kind TEXT,
    cost_is_estimated INTEGER DEFAULT 0,
    ingest_source TEXT,
    duplicate_of TEXT,
    excluded_reason TEXT,
    is_excluded INTEGER NOT NULL DEFAULT 0
);

-- Runtime usage can belong to an OpenClaw-owned session even when the complete
-- counters live in a Claude/Codex transcript. Keep the raw message untouched
-- and attribute it only in the derived/effective layer.
CREATE TABLE IF NOT EXISTS message_attributions (
    source_message_id TEXT PRIMARY KEY REFERENCES messages(id) ON DELETE CASCADE,
    canonical_session_id TEXT NOT NULL REFERENCES sessions(session_id),
    canonical_model_ref_id INTEGER NOT NULL REFERENCES model_reference(id),
    canonical_message_id TEXT REFERENCES messages(id),
    reason TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE INDEX IF NOT EXISTS idx_message_attributions_session
    ON message_attributions(canonical_session_id, canonical_model_ref_id);

-- Consumers aggregate this effective identity. The original runtime session,
-- model and ingest source remain available for audit via source_* columns.
DROP VIEW IF EXISTS effective_messages;
CREATE VIEW effective_messages AS
SELECT
    m.id,
    m.session_id,
    m.parent_id,
    m.timestamp,
    m.model_ref_id,
    m.input_tokens,
    m.output_tokens,
    m.cache_read,
    m.cache_write,
    m.total_tokens,
    m.cost_input,
    m.cost_output,
    m.cost_cache_read,
    m.cost_cache_write,
    m.cost_total,
    m.cost_source,
    m.cost_kind,
    m.cost_is_estimated,
    m.ingest_source,
    m.duplicate_of,
    m.excluded_reason,
    m.is_excluded,
    m.session_id AS source_session_id,
    m.model_ref_id AS source_model_ref_id,
    NULL AS canonical_message_id,
    NULL AS attribution_reason
FROM messages m
LEFT JOIN message_attributions a ON a.source_message_id = m.id
WHERE a.source_message_id IS NULL
UNION ALL
SELECT
    m.id,
    a.canonical_session_id AS session_id,
    m.parent_id,
    m.timestamp,
    a.canonical_model_ref_id AS model_ref_id,
    m.input_tokens,
    m.output_tokens,
    m.cache_read,
    m.cache_write,
    m.total_tokens,
    m.cost_input,
    m.cost_output,
    m.cost_cache_read,
    m.cost_cache_write,
    m.cost_total,
    m.cost_source,
    m.cost_kind,
    m.cost_is_estimated,
    m.ingest_source,
    m.duplicate_of,
    m.excluded_reason,
    m.is_excluded,
    m.session_id AS source_session_id,
    m.model_ref_id AS source_model_ref_id,
    a.canonical_message_id,
    a.reason AS attribution_reason
FROM message_attributions a
JOIN messages m ON m.id = a.source_message_id;

-- PAYG spend tracking (manual entry from console)
CREATE TABLE IF NOT EXISTS payg_spend (
    provider TEXT,
    date TEXT,                    -- "2026-02-04" or "2026-02" for monthly
    amount REAL,
    notes TEXT,
    PRIMARY KEY (provider, date)
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_messages_session ON messages(session_id);
CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages(timestamp);
CREATE INDEX IF NOT EXISTS idx_messages_model_ref ON messages(model_ref_id);
CREATE INDEX IF NOT EXISTS idx_messages_session_timestamp ON messages(session_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_messages_session_model_timestamp ON messages(session_id, model_ref_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_messages_source_timestamp ON messages(ingest_source, timestamp);
CREATE INDEX IF NOT EXISTS idx_messages_session_excluded_reason ON messages(session_id, excluded_reason);
CREATE INDEX IF NOT EXISTS idx_sessions_channel ON sessions(channel_name);
CREATE INDEX IF NOT EXISTS idx_sessions_created ON sessions(created_at);

-- Model segments (for sessions that switch models mid-session)
CREATE TABLE IF NOT EXISTS segments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL REFERENCES sessions(session_id),
    model_raw TEXT NOT NULL REFERENCES model_reference(model_raw),
    segment_index INTEGER NOT NULL,     -- 0, 1, 2... for each model in session
    first_msg_ts TEXT,                  -- First message timestamp in this segment
    last_msg_ts TEXT,                   -- Last message timestamp in this segment
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
    cost_source TEXT,
    cost_kind TEXT,
    cost_is_estimated INTEGER DEFAULT 0,
    UNIQUE(session_id, segment_index)
);

CREATE INDEX IF NOT EXISTS idx_segments_session ON segments(session_id);
CREATE INDEX IF NOT EXISTS idx_segments_model ON segments(model_raw);
CREATE INDEX IF NOT EXISTS idx_segments_session_model_idx ON segments(session_id, model_raw, segment_index);
CREATE INDEX IF NOT EXISTS idx_segments_time_bounds ON segments(first_msg_ts, last_msg_ts);

-- Durable OpenClaw channel session ids survive /new and /reset.  Preserve the
-- lifecycle events separately so derived segments can split one sticky owner
-- session into the reset generations the user actually sees.
CREATE TABLE IF NOT EXISTS session_boundaries (
    session_id TEXT NOT NULL REFERENCES sessions(session_id),
    source TEXT NOT NULL,
    event_seq INTEGER NOT NULL,
    timestamp TEXT NOT NULL,
    reason TEXT,
    PRIMARY KEY (session_id, source, event_seq)
);
CREATE INDEX IF NOT EXISTS idx_session_boundaries_session_time
    ON session_boundaries(session_id, timestamp);

-- Ingestion state (so the DB can be the archive even if old session logs disappear)
-- Tracks how much of each session file we've ingested, so incremental runs are append-only.
CREATE TABLE IF NOT EXISTS ingest_state (
    filename TEXT PRIMARY KEY,
    session_id TEXT,
    last_offset INTEGER NOT NULL DEFAULT 0,  -- byte offset in file
    last_size INTEGER,
    last_mtime REAL,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE INDEX IF NOT EXISTS idx_ingest_state_session ON ingest_state(session_id);
