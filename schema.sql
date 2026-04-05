-- AI Cost Tracker v3 Schema
-- Public package version

CREATE TABLE IF NOT EXISTS model_reference (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    model_raw TEXT NOT NULL UNIQUE,
    endpoint TEXT NOT NULL,
    author TEXT NOT NULL,
    model TEXT NOT NULL,
    pru_multiplier REAL DEFAULT 1.0
);

CREATE TABLE IF NOT EXISTS provider_costs (
    provider TEXT NOT NULL,
    billing_start TEXT NOT NULL,
    plan_type TEXT NOT NULL,
    monthly_cost REAL,
    extra_usage REAL,
    notes TEXT,
    PRIMARY KEY (provider, billing_start)
);

CREATE TABLE IF NOT EXISTS provider_pru_invoices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    provider TEXT NOT NULL,
    cycle_start TEXT NOT NULL,
    cycle_end TEXT,
    model_raw TEXT NOT NULL,
    prus REAL NOT NULL,
    included_requests REAL,
    billed_requests REAL NOT NULL DEFAULT 0,
    notes TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE(provider, cycle_start, model_raw)
);
CREATE INDEX IF NOT EXISTS idx_provider_pru_invoices_provider_cycle ON provider_pru_invoices(provider, cycle_start);

CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT PRIMARY KEY,
    session_nickname TEXT,
    parent_nickname TEXT,
    filename TEXT,
    created_at TEXT,
    channel_id TEXT,
    channel_name TEXT,
    channel_type TEXT,
    session_type TEXT,
    deleted_at TEXT
);

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

CREATE TABLE IF NOT EXISTS payg_spend (
    provider TEXT,
    date TEXT,
    amount REAL,
    notes TEXT,
    PRIMARY KEY (provider, date)
);

CREATE INDEX IF NOT EXISTS idx_messages_session ON messages(session_id);
CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages(timestamp);
CREATE INDEX IF NOT EXISTS idx_messages_model_ref ON messages(model_ref_id);
CREATE INDEX IF NOT EXISTS idx_sessions_channel ON sessions(channel_name);
CREATE INDEX IF NOT EXISTS idx_sessions_created ON sessions(created_at);

CREATE TABLE IF NOT EXISTS segments (
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
);

CREATE INDEX IF NOT EXISTS idx_segments_session ON segments(session_id);
CREATE INDEX IF NOT EXISTS idx_segments_model ON segments(model_raw);

CREATE TABLE IF NOT EXISTS ingest_state (
    filename TEXT PRIMARY KEY,
    session_id TEXT,
    last_offset INTEGER NOT NULL DEFAULT 0,
    last_size INTEGER,
    last_mtime REAL,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE INDEX IF NOT EXISTS idx_ingest_state_session ON ingest_state(session_id);
