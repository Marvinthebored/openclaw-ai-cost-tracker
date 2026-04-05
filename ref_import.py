#!/usr/bin/env python3
"""Import reference TSVs into the tracker SQLite DB."""

import argparse
import csv
import io
import re
import sqlite3
from datetime import datetime
from pathlib import Path

TABLES = {
    'provider_costs': {
        'columns': ['provider', 'billing_start', 'plan_type', 'monthly_cost', 'extra_usage', 'notes'],
        'numeric': {'monthly_cost', 'extra_usage'},
        'delete_first': True,
        'insert': 'INSERT INTO provider_costs (provider, billing_start, plan_type, monthly_cost, extra_usage, notes) VALUES (?,?,?,?,?,?)',
    },
    'model_reference': {
        'columns': ['model_raw', 'endpoint', 'author', 'model', 'pru_multiplier'],
        'numeric': {'pru_multiplier'},
        'delete_first': False,
        'insert': """
            INSERT INTO model_reference (model_raw, endpoint, author, model, pru_multiplier)
            VALUES (?,?,?,?,?)
            ON CONFLICT(model_raw) DO UPDATE SET
                endpoint=excluded.endpoint,
                author=excluded.author,
                model=excluded.model,
                pru_multiplier=excluded.pru_multiplier
        """,
    },
    'provider_pru_invoices': {
        'columns': ['provider', 'cycle_start', 'cycle_end', 'model_raw', 'prus', 'included_requests', 'billed_requests', 'notes'],
        'numeric': {'prus', 'included_requests', 'billed_requests'},
        'delete_first': True,
        'insert': 'INSERT INTO provider_pru_invoices (provider, cycle_start, cycle_end, model_raw, prus, included_requests, billed_requests, notes) VALUES (?,?,?,?,?,?,?,?)',
    },
}


def parse_args():
    root = Path(__file__).resolve().parent
    ap = argparse.ArgumentParser()
    ap.add_argument('--db', default=str(root / 'cost_tracker.db'))
    ap.add_argument('--ref-dir', default=str(root / 'ref'))
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('tables', nargs='*')
    return ap.parse_args()


def parse_tsv(path: Path, meta: dict):
    rows = []
    export_ts = None
    lines = []
    with open(path, 'r', encoding='utf-8') as f:
        for raw_line in f:
            if raw_line.startswith('#'):
                if 'Exported:' in raw_line:
                    match = re.search(r'Exported:\s*(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', raw_line)
                    if match:
                        export_ts = datetime.strptime(match.group(1), '%Y-%m-%d %H:%M:%S')
                continue
            lines.append(raw_line)
    if export_ts:
        age_hrs = (datetime.now() - export_ts).total_seconds() / 3600
        print(f'{path.name}: exported {age_hrs:.1f}h ago')
    reader = csv.DictReader(io.StringIO(''.join(lines)), delimiter='\t')
    for line in reader:
        values = []
        for col in meta['columns']:
            value = (line.get(col, '') or '').strip()
            if value == '':
                values.append(None)
            elif col in meta['numeric']:
                values.append(float(value))
            else:
                values.append(value)
        rows.append(tuple(values))
    return rows


def main():
    args = parse_args()
    db = Path(args.db).expanduser().resolve()
    ref_dir = Path(args.ref_dir).expanduser().resolve()
    tables = args.tables or list(TABLES.keys())
    conn = sqlite3.connect(str(db))
    try:
        for name in tables:
            if name not in TABLES:
                print(f'Unknown table: {name}')
                continue
            tsv = ref_dir / f'{name}.tsv'
            if not tsv.exists():
                print(f'Missing TSV, skipping: {tsv}')
                continue
            meta = TABLES[name]
            rows = parse_tsv(tsv, meta)
            if args.dry_run:
                print(f'{name}: would import {len(rows)} rows')
                continue
            if meta['delete_first']:
                conn.execute(f'DELETE FROM {name}')
            for row in rows:
                conn.execute(meta['insert'], row)
            conn.commit()
            print(f'Imported {len(rows)} rows into {name}')
    finally:
        conn.close()
    if args.dry_run:
        print('Dry run complete; no changes written.')


if __name__ == '__main__':
    main()
