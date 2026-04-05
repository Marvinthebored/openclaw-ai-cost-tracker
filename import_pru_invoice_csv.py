#!/usr/bin/env python3
"""Import PRU invoice CSV data into the tracker SQLite DB."""

import argparse
import csv
import sqlite3
from pathlib import Path


def parse_args():
    root = Path(__file__).resolve().parent
    ap = argparse.ArgumentParser()
    ap.add_argument('--db', default=str(root / 'cost_tracker.db'))
    ap.add_argument('--provider', required=True)
    ap.add_argument('--cycle-start', required=True)
    ap.add_argument('--cycle-end')
    ap.add_argument('--notes')
    ap.add_argument('csv_path')
    return ap.parse_args()


def main():
    args = parse_args()
    db_path = Path(args.db).expanduser().resolve()
    rows = []
    with open(args.csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        if 'model_raw' not in reader.fieldnames or 'prus' not in reader.fieldnames:
            raise SystemExit('CSV must have columns: model_raw, prus')
        for row in reader:
            model_raw = (row.get('model_raw') or '').strip()
            if not model_raw:
                continue
            prus = float(row.get('prus') or 0)
            rows.append((args.provider, args.cycle_start, args.cycle_end, model_raw, prus, args.notes))
    conn = sqlite3.connect(str(db_path))
    try:
        conn.executemany(
            """
            INSERT OR REPLACE INTO provider_pru_invoices
              (provider, cycle_start, cycle_end, model_raw, prus, notes)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
        print(f'Imported {len(rows)} PRU invoice rows into {db_path}')
    finally:
        conn.close()


if __name__ == '__main__':
    main()
