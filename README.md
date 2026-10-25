# AI Cost Tracker

Standalone v4 cost tracker for OpenClaw's current per-agent SQLite stores, legacy
session JSONL, and supported CLI transcripts.

## Modern OpenClaw compatibility — 2026-10-25

This release includes the current standalone v4 importer, dashboard, and log viewer.
It supports these storage formats:

- Per-agent SQLite stores, including Zstandard-compressed events.
- Retained SQLite transcript archives for deleted sessions.
- Legacy OpenClaw JSONL sessions.
- Codex app-server rollouts, Claude Code transcripts, and Kimi Code transcripts.

The tracker counts runtime calls under their proven OpenClaw owner.
Whole-turn totals do not count again when runtime records prove the same usage.
Ambiguous or incomplete matches retain the wrapper total as a fallback.
Direct terminal sessions remain separate from OpenClaw-owned work.

The log viewer opens the newest associated runtime log by default.
Older runtime logs and the original OpenClaw transcript remain selectable.
Source databases and transcripts remain unchanged.

For compressed archives, use Python 3.14 or later.
The schema checks use synthetic fixtures, not a promise of compatibility with every future OpenClaw build.
No Gateway plugin, API key, or OpenClaw configuration change is required.
The tracker reads usage stores locally. The optional OpenClaw CLI supplies scheduled-job ownership metadata.

### Update an existing installation

Stop the tracker server before you update its code.
Back up your tracker database and edited files in `ref/`.
Then run:

```bash
git pull --ff-only
python3 migrate_sessions.py --incremental
python3 server.py
```

Use the same source-path flags as your original installation.
An incremental import updates the schema and recomputes duplicate exclusions.
It preserves your reference-table edits and raw usage records.
A full rebuild is not required.

### Custom runtime roots

Both the importer and server accept these flags:

```text
--openclaw-agents-dir /path/to/openclaw/agents
--sessions-dir /path/to/openclaw/agents/main/sessions
--codex-sessions-dir /path/to/codex/sessions
--claude-projects-dir /path/to/claude/projects
--kimi-code-dir /path/to/kimi-code
```

Pass the same flags to both commands.
The server passes these paths to the importer when you select **Refresh**.
The default server address is loopback-only. The dashboard has no authentication.
Do not expose it directly to an untrusted network.

### Verification

Run the synthetic regression suite:

```bash
python3 -m unittest discover -v
```

Tests cover SQLite ingestion, compressed events, archived transcripts, owner attribution,
whole-turn deduplication, repeated refreshes, API totals, and runtime-log selection.

## What it does

- incrementally ingests `~/.openclaw/agents/*/agent/openclaw-agent.sqlite`
- retains legacy OpenClaw JSONL plus Codex, Claude Code, and Kimi Code ingestion
- preserves raw usage rows and marks only proved cross-source duplicates as excluded
- tracks per-message and per-session token usage
- splits sessions into model segments when the model changes mid-session
- serves a browser dashboard for filtering, grouping, and estimated cost analysis
- opens OpenClaw session logs in a JSONL viewer when you click a session entry
- can inspect local JSONL/log files in the viewer via drag/drop, file picker, or folder picker
- shows source session-file metadata alongside tracked sessions
- displays four compact editable config panels together in the browser: provider
  costs, model translations, PRU invoices, and the PAYG-equivalent override rate card

## What is included

- `migrate_sessions.py` — builds / updates the SQLite database from OpenClaw and Codex CLI logs
- `server.py` — small standalone HTTP server for the dashboard and JSON API
- `dashboard.html` — browser UI for filtering, grouping, and inspection
- `jsonl_viewer.html` — JSONL/session-log viewer with server-log and local-file modes
- `schema.sql` — SQLite schema
- `ref_export.py`, `ref_import.py`, `import_pru_invoice_csv.py` — helper scripts for reference data
- `ref/*.tsv` — editable TSV lookup/config tables
- `channel_mapping.example.json` — optional channel-id mapping example

## Prerequisites

- Python 3.14+ (includes Zstandard support for compressed OpenClaw events and archives)
- OpenClaw data and/or supported CLI transcripts available locally
- modern browser

No third-party Python packages are required. The tracker uses the Python standard library.

## Quick start

Clone the repo and enter the package directory:

```bash
git clone https://github.com/Marvinthebored/openclaw-ai-cost-tracker.git
cd openclaw-ai-cost-tracker
```

### 1) Confirm the OpenClaw data root

Current OpenClaw stores one live usage database per agent under:

```bash
~/.openclaw/agents/*/agent/openclaw-agent.sqlite
```

Legacy JSONL sessions and Codex CLI transcripts are also auto-discovered at:

```bash
~/.openclaw/agents/*/sessions
~/.codex/sessions
```

If OpenClaw lives elsewhere, pass `--openclaw-agents-dir`; the other source
roots also have explicit CLI flags.

### 2) Build the database

From this package directory:

```bash
python3 migrate_sessions.py --full
```

Or with an explicit OpenClaw root:

```bash
python3 migrate_sessions.py --full \
  --openclaw-agents-dir /path/to/openclaw/agents
```

With OpenClaw, legacy JSONL, and Codex paths pinned explicitly:

```bash
python3 migrate_sessions.py --full \
  --openclaw-agents-dir /path/to/openclaw/agents \
  --sessions-dir /path/to/openclaw/agents/main/sessions \
  --codex-sessions-dir /path/to/.codex/sessions
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
  --openclaw-agents-dir /path/to/openclaw/agents \
  --sessions-dir /path/to/openclaw/agents/main/sessions \
  --codex-sessions-dir /path/to/.codex/sessions \
  --channel-mapping /path/to/channel_mapping.json
```

## Ingest flow

1. `migrate_sessions.py` scans per-agent OpenClaw SQLite stores and supported
   JSONL/CLI sources.
2. Session metadata goes into `sessions`.
3. Assistant messages and token/cost data go into `messages`.
4. Cross-source reconciliation keeps every raw row, marking only strict duplicate
   matches with `duplicate_of`, `excluded_reason`, and `is_excluded`.
5. Non-excluded messages are rolled into model `segments`.
6. `ingest_state` tracks SQLite rowids and file offsets for incremental refreshes.

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
- the browser calls `/api/cost-v4/sessions` for one summary per owning session and model
- expanded rows call `/api/cost-v4/messages` on demand for individual model calls
- the server queries SQLite and returns:
  - session segment rows
  - per-message detail rows when requested
  - provider totals
  - reference-table data used for cost estimation
- clicking a session entry opens either its SQLite transcript or source log in
  the JSONL viewer
- clicking **Refresh** calls the migration script in incremental mode

## Raw session log browsing

Session rows open `/jsonl-viewer`, which fetches a SQLite transcript from
`/api/cost-v4/transcript/<session_id>` or a bounded local source-log slice.

The same viewer can also browse local files without uploading them anywhere:

- drag a `.jsonl` / `.log` / `.txt` file onto the page
- use **open local file** for one or more files
- use **open local folder** to build a pick list from a folder of logs

If you run the tracker locally and prefer a different workflow, you can customize that endpoint to launch your preferred editor instead. The default package behavior stays browser-based and platform-agnostic.

## Reference data

The tracker exposes four compact editable panels together under **Config →
Reference tables**. Each panel saves independently.

- `provider_costs` — billing periods and plan types
- `model_reference` — raw-model translations, display labels, and PRU multipliers
- `provider_pru_invoices` — manual PRU invoice rows for providers like GitHub Copilot
- `payg_model_rates` — file-backed per-million-token override rates used to
  calculate PAYG-equivalent imported costs

The first three can also be exported/imported as TSV. `payg_model_rates.tsv`
is edited directly by its browser panel or as a normal TSV file. Saving it does
not rewrite historical rows until the next **Refresh**.

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
- Direct CLI sessions use `cli-codex/`, `cli-claude/`, or `cli-kimi/` model prefixes.
- Channel mapping is optional; without it, the tracker still works.
- OpenClaw SQLite ingestion is enabled by default; use
  `--no-openclaw-sqlite` only for a deliberate legacy-only run.
- SQLite databases, logs, backups, output, and local mapping files are ignored by Git.
- This project is built for OpenClaw and supported CLI transcripts, not arbitrary chat exports.

## License

MIT. See `LICENSE`.
