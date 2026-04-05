# AI Cost Tracker Public Package - Resume

## What was packaged

- standalone `server.py` replacing the dependency on Marvin's `dashboard_server.py`
- sanitized `dashboard.html` copied from the live tracker
- portable `migrate_sessions.py` with CLI-configurable paths
- portable reference-table utilities:
  - `ref_export.py`
  - `ref_import.py`
  - `import_pru_invoice_csv.py`
- safe sample/template files in `ref/`
- `channel_mapping.example.json`
- packaging docs: `README.md`, `.gitignore`

## Important caveats

- this package still assumes the input format is OpenClaw session `.jsonl`
- no sample session logs are included
- no license file yet
- no Git metadata has been initialized yet
- the dashboard UI is inline HTML/JS copied from the live tracker, so future UI changes will need manual sync

## Remaining pre-publish checks

1. run one clean full ingest in a fresh clone
2. verify the dashboard against a small non-private sample log set
3. add LICENSE
4. optionally add screenshots
5. decide final repository name and description
