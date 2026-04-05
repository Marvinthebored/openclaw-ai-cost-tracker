#!/usr/bin/env python3
"""Standalone web server for the AI Cost Tracker public package."""

import argparse
import json
import re
import socketserver
import sqlite3
import subprocess
import sys
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from socketserver import ThreadingMixIn
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parent
DEFAULT_DB = ROOT / 'cost_tracker.db'
DEFAULT_DASHBOARD = ROOT / 'dashboard.html'
DEFAULT_REF_DIR = ROOT / 'ref'
DEFAULT_MIGRATE_SCRIPT = ROOT / 'migrate_sessions.py'
DEFAULT_REF_EXPORT_SCRIPT = ROOT / 'ref_export.py'
DEFAULT_REF_IMPORT_SCRIPT = ROOT / 'ref_import.py'
DEFAULT_BACKUPS_DIR = ROOT / 'backups'
DEFAULT_SESSIONS_DIR = Path.home() / '.openclaw' / 'agents' / 'main' / 'sessions'
VALID_REF_TABLES = {'provider_costs', 'model_reference', 'provider_pru_invoices'}


def parse_args():
    ap = argparse.ArgumentParser(description='Run the AI Cost Tracker dashboard server.')
    ap.add_argument('--host', default='127.0.0.1')
    ap.add_argument('--port', type=int, default=8050)
    ap.add_argument('--db', default=str(DEFAULT_DB), help='SQLite database path')
    ap.add_argument('--dashboard', default=str(DEFAULT_DASHBOARD), help='Dashboard HTML path')
    ap.add_argument('--ref-dir', default=str(DEFAULT_REF_DIR), help='Reference TSV directory')
    ap.add_argument('--migrate-script', default=str(DEFAULT_MIGRATE_SCRIPT))
    ap.add_argument('--ref-export-script', default=str(DEFAULT_REF_EXPORT_SCRIPT))
    ap.add_argument('--ref-import-script', default=str(DEFAULT_REF_IMPORT_SCRIPT))
    ap.add_argument('--sessions-dir', default=str(DEFAULT_SESSIONS_DIR), help='OpenClaw session log directory used by the refresh endpoint')
    ap.add_argument('--channel-mapping', default=None, help='Optional JSON channel-mapping file passed through to migrate_sessions.py')
    ap.add_argument('--backups-dir', default=str(DEFAULT_BACKUPS_DIR), help='Backup directory passed through to migrate_sessions.py')
    return ap.parse_args()


class TrackerHTTPServer(ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, server_address, handler_cls, config):
        self.config = config
        super().__init__(server_address, handler_cls)


class TrackerHandler(BaseHTTPRequestHandler):
    server: TrackerHTTPServer

    def log_message(self, fmt, *args):
        return

    @property
    def cfg(self):
        return self.server.config

    def _respond(self, code, body, content_type='text/plain; charset=utf-8'):
        if isinstance(body, str):
            body = body.encode('utf-8')
        self.send_response(code)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _json(self, code, payload):
        self._respond(code, json.dumps(payload), 'application/json; charset=utf-8')

    def _db(self):
        db_path = self.cfg['db']
        if not db_path.exists():
            return None
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        return conn

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        if path in ('/', '/index.html', '/cost/v3'):
            self.serve_dashboard()
            return
        if path == '/api/cost-v3/sessions':
            self.handle_sessions()
            return
        if path == '/api/cost-v3/refresh':
            self.handle_refresh()
            return
        if path.startswith('/api/cost-v3/ref-disk/'):
            action = path.rsplit('/', 1)[-1]
            if action in {'export', 'import'}:
                self.handle_ref_disk(action)
                return
        if path.startswith('/api/cost-v3/ref/'):
            table = path.rsplit('/', 1)[-1]
            if table in VALID_REF_TABLES:
                self.handle_ref_get(table)
                return
        self._json(404, {'error': f'Not found: {path}'})

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path
        if path.startswith('/api/cost-v3/ref/'):
            table = path.rsplit('/', 1)[-1]
            if table in VALID_REF_TABLES:
                self.handle_ref_post(table)
                return
        self._json(404, {'error': f'Not found: {path}'})

    def serve_dashboard(self):
        dashboard = self.cfg['dashboard']
        if not dashboard.exists():
            self._respond(404, f'Dashboard not found: {dashboard}')
            return
        self._respond(200, dashboard.read_text(encoding='utf-8'), 'text/html; charset=utf-8')

    def handle_sessions(self):
        conn = self._db()
        if not conn:
            self._json(500, {'error': f'Database not found: {self.cfg["db"]}'})
            return
        try:
            sessions_query = """
                SELECT
                    s.session_id,
                    s.session_nickname,
                    s.parent_nickname,
                    s.created_at,
                    s.channel_name,
                    s.session_type,
                    s.deleted_at,
                    s.filename,
                    mr.endpoint,
                    mr.author,
                    mr.model,
                    mr.pru_multiplier,
                    seg.model_raw,
                    seg.segment_index,
                    seg.first_msg_ts,
                    seg.last_msg_ts,
                    seg.msg_count,
                    seg.input_tokens,
                    seg.output_tokens,
                    seg.cache_read,
                    seg.cache_write,
                    seg.input_tokens + seg.cache_read + seg.cache_write AS total_in,
                    seg.total_tokens,
                    seg.cost_input + seg.cost_output AS cost_io,
                    seg.cost_cache_read + seg.cost_cache_write AS cost_cache,
                    seg.cost_total AS cost_logged
                FROM sessions s
                INNER JOIN segments seg ON s.session_id = seg.session_id
                LEFT JOIN model_reference mr ON seg.model_raw = mr.model_raw
                ORDER BY seg.first_msg_ts DESC
            """
            sessions = []
            for row in conn.execute(sessions_query).fetchall():
                item = dict(row)
                item['cost_estimated'] = row['cost_logged'] or 0
                sessions.append(item)

            messages_query = """
                SELECT
                    m.id, m.session_id, m.parent_id, m.timestamp,
                    CASE
                        WHEN instr(mr.model_raw, '/') > 0 THEN substr(mr.model_raw, 1, instr(mr.model_raw, '/') - 1)
                        ELSE mr.model_raw
                    END AS provider,
                    mr.model_raw,
                    m.input_tokens, m.output_tokens, m.cache_read, m.cache_write, m.total_tokens,
                    m.cost_input, m.cost_output, m.cost_cache_read, m.cost_cache_write, m.cost_total
                FROM messages m
                INNER JOIN model_reference mr ON mr.id = m.model_ref_id
                ORDER BY m.timestamp ASC
            """
            messages = [dict(row) for row in conn.execute(messages_query).fetchall()]

            provider_totals_raw = {
                row['raw_provider']: row['total_tokens']
                for row in conn.execute(
                    """
                    SELECT
                        CASE
                            WHEN instr(seg.model_raw, '/') > 0 THEN substr(seg.model_raw, 1, instr(seg.model_raw, '/') - 1)
                            ELSE seg.model_raw
                        END AS raw_provider,
                        SUM(seg.total_tokens) AS total_tokens
                    FROM segments seg
                    GROUP BY raw_provider
                    """
                ).fetchall()
            }
            provider_prus_raw = {
                row['raw_provider']: row['total_prus']
                for row in conn.execute(
                    """
                    SELECT
                        CASE
                            WHEN instr(seg.model_raw, '/') > 0 THEN substr(seg.model_raw, 1, instr(seg.model_raw, '/') - 1)
                            ELSE seg.model_raw
                        END AS raw_provider,
                        SUM(seg.total_tokens * COALESCE(mr.pru_multiplier, 1.0)) AS total_prus
                    FROM segments seg
                    LEFT JOIN model_reference mr ON seg.model_raw = mr.model_raw
                    GROUP BY raw_provider
                    """
                ).fetchall()
            }
            provider_aliases = {
                'kimi-coding': 'kimi-code',
                'ollama-cloud': 'opencode',
            }
            provider_totals = {}
            for raw_provider, tokens in provider_totals_raw.items():
                provider = provider_aliases.get(raw_provider, raw_provider)
                provider_totals[provider] = provider_totals.get(provider, 0) + (tokens or 0)
            provider_total_prus = {}
            for raw_provider, prus in provider_prus_raw.items():
                provider = provider_aliases.get(raw_provider, raw_provider)
                provider_total_prus[provider] = provider_total_prus.get(provider, 0) + (prus or 0)

            tier_row = conn.execute(
                """
                SELECT
                    SUM(CASE WHEN seg.model_raw LIKE '%opus%' THEN seg.total_tokens ELSE 0 END) AS opus_tokens,
                    SUM(CASE WHEN seg.model_raw LIKE '%opus%' THEN 0 ELSE seg.total_tokens END) AS other_tokens,
                    SUM(CASE WHEN seg.model_raw LIKE '%opus%' THEN 0 ELSE seg.total_tokens * COALESCE(mr.pru_multiplier, 1.0) END) AS other_prus
                FROM segments seg
                LEFT JOIN model_reference mr ON seg.model_raw = mr.model_raw
                WHERE seg.model_raw LIKE 'github-copilot/%'
                """
            ).fetchone()

            provider_cost_rows = [dict(row) for row in conn.execute('SELECT * FROM provider_costs ORDER BY provider, billing_start').fetchall()]
            provider_costs = {}
            provider_cost_periods = {}
            for row in provider_cost_rows:
                provider_costs[row['provider']] = row
                provider_cost_periods.setdefault(row['provider'], []).append(row)

            self._json(200, {
                'sessions': sessions,
                'messages': messages,
                'provider_totals': provider_totals,
                'provider_costs': provider_costs,
                'provider_cost_periods': provider_cost_periods,
                'provider_total_prus': provider_total_prus,
                'copilot_opus_tokens': (tier_row['opus_tokens'] or 0) if tier_row else 0,
                'copilot_other_tokens': (tier_row['other_tokens'] or 0) if tier_row else 0,
                'copilot_other_prus': (tier_row['other_prus'] or 0) if tier_row else 0,
            })
        finally:
            conn.close()

    def handle_refresh(self):
        cmd = [
            sys.executable,
            str(self.cfg['migrate_script']),
            '--incremental',
            '--db', str(self.cfg['db']),
            '--sessions-dir', str(self.cfg['sessions_dir']),
            '--backups-dir', str(self.cfg['backups_dir']),
        ]
        if self.cfg['channel_mapping']:
            cmd += ['--channel-mapping', str(self.cfg['channel_mapping'])]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            output = (result.stdout + result.stderr).strip()
            ok = result.returncode == 0
            processed_files = 0
            processed_msgs = 0
            match = re.search(r'Processed (\d+) files', output)
            if match:
                processed_files = int(match.group(1))
            match = re.search(r'Assistant messages ingested/upserted: (\d+)', output)
            if match:
                processed_msgs = int(match.group(1))
            self._json(200 if ok else 500, {
                'success': ok,
                'new_sessions': processed_files,
                'new_messages': processed_msgs,
                'output': output,
            })
        except subprocess.TimeoutExpired:
            self._json(500, {'error': 'Migration timeout'})
        except Exception as exc:
            self._json(500, {'error': str(exc)})

    def handle_ref_get(self, table):
        conn = self._db()
        if not conn:
            self._json(500, {'error': f'Database not found: {self.cfg["db"]}'})
            return
        try:
            queries = {
                'provider_costs': 'SELECT provider, billing_start, plan_type, monthly_cost, extra_usage, notes FROM provider_costs ORDER BY provider, billing_start',
                'model_reference': 'SELECT id, model_raw, endpoint, author, model, pru_multiplier FROM model_reference ORDER BY model_raw',
                'provider_pru_invoices': 'SELECT id, provider, cycle_start, cycle_end, model_raw, prus, included_requests, billed_requests, notes FROM provider_pru_invoices ORDER BY provider, cycle_start, model_raw',
            }
            rows = [dict(r) for r in conn.execute(queries[table]).fetchall()]
            self._json(200, {'table': table, 'rows': rows})
        except Exception as exc:
            self._json(500, {'error': str(exc)})
        finally:
            conn.close()

    def handle_ref_post(self, table):
        conn = self._db()
        if not conn:
            self._json(500, {'error': f'Database not found: {self.cfg["db"]}'})
            return
        try:
            length = int(self.headers.get('Content-Length', '0') or '0')
            body = self.rfile.read(length) if length else b''
            data = json.loads(body.decode('utf-8') if body else '{}')
            rows = data.get('rows', [])
            if not rows:
                self._json(400, {'error': 'No rows provided'})
                return
            if table == 'provider_costs':
                conn.execute('DELETE FROM provider_costs')
                for row in rows:
                    conn.execute(
                        'INSERT INTO provider_costs (provider, billing_start, plan_type, monthly_cost, extra_usage, notes) VALUES (?,?,?,?,?,?)',
                        (row.get('provider'), row.get('billing_start'), row.get('plan_type', 'flat'), row.get('monthly_cost'), row.get('extra_usage'), row.get('notes')),
                    )
            elif table == 'model_reference':
                for row in rows:
                    conn.execute(
                        """
                        INSERT INTO model_reference (model_raw, endpoint, author, model, pru_multiplier)
                        VALUES (?,?,?,?,?)
                        ON CONFLICT(model_raw) DO UPDATE SET
                            endpoint=excluded.endpoint,
                            author=excluded.author,
                            model=excluded.model,
                            pru_multiplier=excluded.pru_multiplier
                        """,
                        (row.get('model_raw'), row.get('endpoint', ''), row.get('author', ''), row.get('model', ''), row.get('pru_multiplier', 1.0)),
                    )
            else:
                conn.execute('DELETE FROM provider_pru_invoices')
                for row in rows:
                    conn.execute(
                        'INSERT INTO provider_pru_invoices (provider, cycle_start, cycle_end, model_raw, prus, included_requests, billed_requests, notes) VALUES (?,?,?,?,?,?,?,?)',
                        (row.get('provider'), row.get('cycle_start'), row.get('cycle_end'), row.get('model_raw'), row.get('prus', 0), row.get('included_requests'), row.get('billed_requests', 0), row.get('notes')),
                    )
            conn.commit()
            self._json(200, {'ok': True, 'table': table, 'saved': len(rows)})
        except Exception as exc:
            conn.rollback()
            self._json(500, {'error': str(exc)})
        finally:
            conn.close()

    def handle_ref_disk(self, action):
        script = self.cfg['ref_export_script'] if action == 'export' else self.cfg['ref_import_script']
        cmd = [sys.executable, str(script), '--db', str(self.cfg['db']), '--ref-dir', str(self.cfg['ref_dir'])]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            output = (result.stdout + result.stderr).strip()
            ok = result.returncode == 0
            self._json(200 if ok else 500, {'ok': ok, 'action': action, 'output': output})
        except Exception as exc:
            self._json(500, {'ok': False, 'error': str(exc)})


def main():
    args = parse_args()
    config = {
        'db': Path(args.db).expanduser().resolve(),
        'dashboard': Path(args.dashboard).expanduser().resolve(),
        'ref_dir': Path(args.ref_dir).expanduser().resolve(),
        'migrate_script': Path(args.migrate_script).expanduser().resolve(),
        'ref_export_script': Path(args.ref_export_script).expanduser().resolve(),
        'ref_import_script': Path(args.ref_import_script).expanduser().resolve(),
        'sessions_dir': Path(args.sessions_dir).expanduser().resolve(),
        'channel_mapping': Path(args.channel_mapping).expanduser().resolve() if args.channel_mapping else None,
        'backups_dir': Path(args.backups_dir).expanduser().resolve(),
    }
    with TrackerHTTPServer((args.host, args.port), TrackerHandler, config) as httpd:
        print(f'AI Cost Tracker server running at http://{args.host}:{args.port}')
        print(f'Database: {config["db"]}')
        print(f'Dashboard: {config["dashboard"]}')
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print('\nServer stopped')


if __name__ == '__main__':
    main()
