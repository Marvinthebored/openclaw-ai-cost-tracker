# AI Cost Tracker Public Package - Release State

## Current package

- v4 dashboard and standalone Python server
- current OpenClaw per-agent SQLite ingestion, plus legacy JSONL sources
- auditable message-level duplicate reconciliation
- lazy per-segment message drilldown and SQLite transcript viewing
- four compact in-browser config panels:
  - provider costs
  - model translations
  - PRU invoices
  - PAYG-equivalent token-rate overrides
- browser and TSV workflows share the same local reference data

## Safety boundary

No usage database, transcript, backup, local channel mapping, screenshot, or
machine-specific path is included in the repository.
