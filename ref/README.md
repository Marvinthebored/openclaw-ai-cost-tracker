# Reference TSV Files

The `/ref/` directory contains simple TSV files that act as handy editing tools for the user-maintained lookup/config tables.

They are meant to be easy to open in a spreadsheet editor, review, edit, and re-import without touching code.

## Files

### `model_reference.tsv`

Used for aliases and metadata derived from the raw model names seen in the logs.

Contains:
- `endpoint` alias
- `model` alias / display name
- other model metadata fields used by the tracker
- `pru_multiplier` for every row

Notes:
- New raw model names found in logs are auto-populated with a best-guess row during ingest.
- The GitHub PRU multiplier is stored on every row, but is only actually relevant to GitHub/Copilot-style provider cost estimates.

### `provider_costs.tsv`

Used for provider subscription and pricing assumptions.

Contains:
- monthly subscription costs
- overage pricing
- billing / payment periods
- payment start dates used for pro-rata calculations

This is the table that tells the tracker how to spread provider costs across time.

### `provider_pru_invoices.tsv`

Used to store per-model GitHub PRU invoice/usage rows for more granular pro-rata cost work.

Why it exists:
- to preserve a place for real invoice-side PRU data when you want to reconcile or compare the tracker’s token-weighted estimates against billed per-model usage
- to support a future finer-grained GitHub/Copilot allocation path without redesigning the reference-data structure later

Contains:
- provider name
- billing cycle dates
- model name
- PRU totals
- optional request-count fields
- notes

## Important current status

`provider_pru_invoices.tsv` is **not currently used in the live cost-estimation path**.

Right now, the tracker’s active cost logic is driven by:
- `provider_costs`
- `model_reference.pru_multiplier`
- observed token usage from the logs

So this PRU invoice table is currently best understood as:
- useful reference data
- import/export-ready
- available for future finer-grained reconciliation
- a placeholder for a path that is **not currently implemented** in the active estimator

But it is **not** presently feeding the main estimate calculations.
