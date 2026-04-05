# AI Cost Tracker

Standalone cost tracker for OpenClaw session logs.

## What it does

- ingests OpenClaw session `.jsonl` logs into SQLite
- tracks per-message and per-session token usage
- splits sessions into model segments when the model changes mid-session
- serves a browser dashboard for filtering, grouping, and estimated cost analysis
- shows source session-file metadata alongside tracked sessions
- includes browser download/upload actions for the editable reference TSV tables (`provider_costs`, `model_reference`, `provider_pru_invoices`)

## What is included

- `migrate_sessions.py` — builds / updates the SQLite database from OpenClaw logs
- `server.py` — small standalone HTTP server for the dashboard and JSON API
- `dashboard.html` — browser UI for filtering, grouping, and inspection
- `schema.sql` — SQLite schema
- `ref_export.py`, `ref_import.py`, `import_pru_invoice_csv.py` — helper scripts for reference data
- `ref/*.tsv` — editable TSV lookup/config tables
- `channel_mapping.example.json` — optional channel-id mapping example

## Prerequisites

- Python 3.11+ recommended
- OpenClaw session logs available locally
- modern browser

No third-party Python packages are required; everything here uses the standard library.

## Quick start

### 1) Choose a sessions directory

Default expected location:

```bash
~/.openclaw/agents/main/sessions
```

If your logs live elsewhere, pass `--sessions-dir` explicitly in the commands below.

### 2) Build the database

From this package directory:

```bash
python3 migrate_sessions.py --full
```

Or with an explicit sessions path:

```bash
python3 migrate_sessions.py --full \
  --sessions-dir /path/to/openclaw/agents/main/sessions
```

Optional: add a channel mapping file so Discord / Telegram channel IDs render as names:

```bash
python3 migrate_sessions.py --full \
  --sessions-dir /path/to/openclaw/agents/main/sessions \
  --channel-mapping ./channel_mapping.example.json
```

### 3) Start the dashboard server

```bash
python3 server.py
```

Open:

```text
http://127.0.0.1:8050/
```

If you used a non-default sessions directory and want the dashboard's **Refresh** button to keep working, start the server with the same location:

```bash
python3 server.py \
  --sessions-dir /path/to/openclaw/agents/main/sessions \
  --channel-mapping /path/to/channel_mapping.json
```

## Ingest flow

1. `migrate_sessions.py` scans OpenClaw `.jsonl` session files.
2. Session metadata goes into `sessions`.
3. Assistant messages and token/cost data go into `messages`.
4. Consecutive messages using the same model are rolled into `segments`.
5. `ingest_state` tracks offsets so later runs can be incremental.

### Full rebuild

```bash
python3 migrate_sessions.py --full
```

Use this when:
- starting from scratch
- changing schema logic
- wanting a clean rebuild from raw logs

### Incremental update

```bash
python3 migrate_sessions.py --incremental
```

Use this for normal day-to-day refreshes.

## Run flow

- `server.py` serves `dashboard.html`
- the browser calls `/api/cost-v3/sessions`
- the server queries SQLite and returns:
  - session segment rows
  - per-message detail rows
  - provider totals
  - reference-table data used for cost estimation
- clicking **Refresh** calls the migration script in incremental mode

## Reference data

The tracker uses three editable reference tables.

In the browser UI, these tables can be downloaded as TSV, edited externally, and uploaded back into the tracker.


- `provider_costs` — billing periods and plan types
- `model_reference` — display labels and PRU multipliers
- `provider_pru_invoices` — manual PRU invoice rows for providers like GitHub Copilot

### Export live DB tables to TSV

```bash
python3 ref_export.py
```

### Import edited TSVs back into the DB

```bash
python3 ref_import.py
```

### Import PRU invoice CSV

CSV format:

```text
model_raw,prus
```

Command:

```bash
python3 import_pru_invoice_csv.py \
  --provider github-copilot \
  --cycle-start 2026-02-01 \
  invoice.csv
```

## Notes

- The seeded `MODEL_REFERENCE_DATA` and `PROVIDER_COST_DATA` in `migrate_sessions.py` are defaults, not gospel.
- Unknown models are auto-added to `model_reference` during ingest.
- Channel mapping is optional; without it, the tracker still works.
- This project is built for OpenClaw session logs, not arbitrary chat exports.

## License

MIT. See `LICENSE`.
