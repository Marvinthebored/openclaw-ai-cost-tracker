#!/usr/bin/env python3
"""Export reference tables from the tracker SQLite DB to TSV files."""

import argparse
import csv
import sqlite3
from datetime import datetime
from pathlib import Path

TABLES = {
    'provider_costs': {
        'query': 'SELECT provider, billing_start, plan_type, monthly_cost, extra_usage, notes FROM provider_costs ORDER BY provider, billing_start',
        'columns': ['provider', 'billing_start', 'plan_type', 'monthly_cost', 'extra_usage', 'notes'],
    },
    'model_reference': {
        'query': 'SELECT model_raw, endpoint, author, model, pru_multiplier FROM model_reference ORDER BY model_raw',
        'columns': ['model_raw', 'endpoint', 'author', 'model', 'pru_multiplier'],
    },
    'provider_pru_invoices': {
        'query': 'SELECT provider, cycle_start, cycle_end, model_raw, prus, included_requests, billed_requests, notes FROM provider_pru_invoices ORDER BY provider, cycle_start, model_raw',
        'columns': ['provider', 'cycle_start', 'cycle_end', 'model_raw', 'prus', 'included_requests', 'billed_requests', 'notes'],
    },
}


def parse_args():
    root = Path(__file__).resolve().parent
    ap = argparse.ArgumentParser()
    ap.add_argument('--db', default=str(root / 'cost_tracker.db'))
    ap.add_argument('--ref-dir', default=str(root / 'ref'))
    ap.add_argument('tables', nargs='*')
    return ap.parse_args()


def main():
    args = parse_args()
    db = Path(args.db).expanduser().resolve()
    ref_dir = Path(args.ref_dir).expanduser().resolve()
    ref_dir.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db))
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    tables = args.tables or list(TABLES.keys())
    try:
        for name in tables:
            if name not in TABLES:
                print(f'Unknown table: {name}')
                continue
            meta = TABLES[name]
            rows = conn.execute(meta['query']).fetchall()
            out = ref_dir / f'{name}.tsv'
            with open(out, 'w', newline='', encoding='utf-8') as f:
                f.write(f'# Exported: {now}  |  {len(rows)} rows  |  Edit then: python3 ref_import.py {name}\n')
                writer = csv.writer(f, delimiter='\t')
                writer.writerow(meta['columns'])
                for row in rows:
                    writer.writerow(['' if value is None else value for value in row])
            print(f'Exported {name} -> {out}')
    finally:
        conn.close()


if __name__ == '__main__':
    main()
