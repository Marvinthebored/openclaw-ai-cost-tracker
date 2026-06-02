# AI Cost Tracker Public Package - Resume

## What was packaged

- standalone `server.py` replacing the dependency on Marvin's `dashboard_server.py`
- sanitized v3.1 `dashboard.html` copied from the live tracker, using lazy message drilldown
- portable `migrate_sessions.py` with CLI-configurable paths
- portable reference-table utilities:
  - `ref_export.py`
  - `ref_import.py`
  - `import_pru_invoice_csv.py`
- safe sample/template files in `ref/`
- `channel_mapping.example.json`
- packaging docs: `README.md`, `.gitignore`

## Important caveats

- this package assumes OpenClaw session `.jsonl` files and Codex CLI session transcripts
- no sample session logs are included
- git remote: `https://github.com/Marvinthebored/openclaw-ai-cost-tracker.git`
- the dashboard UI is inline HTML/JS copied from the live tracker, so future UI changes will need manual sync

## Remaining pre-publish checks

1. optionally add screenshots
2. optionally add a tiny bundled sample log fixture for demo mode
