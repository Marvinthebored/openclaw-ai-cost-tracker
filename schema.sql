-- AI Cost Tracker v3 Schema
-- Default database: ./cost_tracker.db

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
    cost_total REAL
);

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
    UNIQUE(session_id, segment_index)
);

CREATE INDEX IF NOT EXISTS idx_segments_session ON segments(session_id);
CREATE INDEX IF NOT EXISTS idx_segments_model ON segments(model_raw);

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
