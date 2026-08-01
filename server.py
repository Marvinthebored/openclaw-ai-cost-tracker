#!/usr/bin/env python3
"""Standalone web server for the AI Cost Tracker public package."""

import argparse
import json
import re
import sqlite3
import subprocess
import sys
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from socketserver import ThreadingMixIn
from urllib.parse import parse_qs, unquote, urlparse
import socketserver

ROOT = Path(__file__).resolve().parent
DEFAULT_DB = ROOT / 'cost_tracker.db'
DEFAULT_DASHBOARD = ROOT / 'dashboard.html'
DEFAULT_REF_DIR = ROOT / 'ref'
DEFAULT_MIGRATE_SCRIPT = ROOT / 'migrate_sessions.py'
DEFAULT_REF_EXPORT_SCRIPT = ROOT / 'ref_export.py'
DEFAULT_REF_IMPORT_SCRIPT = ROOT / 'ref_import.py'
DEFAULT_BACKUPS_DIR = ROOT / 'backups'
DEFAULT_SESSIONS_DIR = Path.home() / '.openclaw' / 'agents' / 'main' / 'sessions'
DEFAULT_OPENCLAW_AGENTS_DIR = Path.home() / '.openclaw' / 'agents'
DEFAULT_CODEX_SESSIONS_DIR = Path.home() / '.codex' / 'sessions'
VALID_REF_TABLES = {'provider_costs', 'model_reference', 'provider_pru_invoices', 'payg_model_rates'}
PAYG_RATE_COLS = ['model_raw', 'input_per_mtok', 'output_per_mtok', 'cache_read_per_mtok', 'cache_write_per_mtok', 'notes']
PAYG_RATE_NUMERIC = {'input_per_mtok', 'output_per_mtok', 'cache_read_per_mtok', 'cache_write_per_mtok'}


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
    ap.add_argument('--openclaw-agents-dir', default=str(DEFAULT_OPENCLAW_AGENTS_DIR), help='OpenClaw agents root containing the new per-agent SQLite stores')
    ap.add_argument('--codex-sessions-dir', default=str(DEFAULT_CODEX_SESSIONS_DIR), help='Codex CLI session log directory used by the refresh endpoint')
    ap.add_argument('--channel-mapping', default=None, help='Optional JSON channel-mapping file passed through to migrate_sessions.py')
    ap.add_argument('--backups-dir', default=str(DEFAULT_BACKUPS_DIR), help='Backup directory passed through to migrate_sessions.py')
    return ap.parse_args()


def resolve_session_log_path(filename: str, openclaw_dir: Path, codex_dir: Path):
    if not filename:
        return None
    if filename.startswith('codex_cli:'):
        rel = filename.split(':', 1)[1]
        if not rel:
            return None
        root = codex_dir.resolve()
        file_path = (root / rel).resolve()
        try:
            file_path.relative_to(root)
        except ValueError:
            return None
        return file_path if file_path.is_file() else None
    if Path(filename).name != filename:
        return None
    root = openclaw_dir.resolve()
    file_path = (root / filename).resolve()
    try:
        file_path.relative_to(root)
    except ValueError:
        return None
    return file_path if file_path.is_file() else None


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

    def _respond(self, code, body, content_type='text/plain; charset=utf-8', headers=None):
        if isinstance(body, str):
            body = body.encode('utf-8')
        self.send_response(code)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(body)))
        for key, value in (headers or {}).items():
            self.send_header(key, value)
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

        if path in ('/', '/index.html', '/cost', '/cost/v4'):
            self.serve_dashboard()
            return
        if path == '/jsonl-viewer':
            self.serve_jsonl_viewer()
            return
        if path == '/favicon.svg':
            favicon = ROOT / 'favicon.svg'
            self._respond(200, favicon.read_bytes(), 'image/svg+xml')
            return
        if path == '/api/jsonl-tail':
            self.handle_jsonl_tail(parsed)
            return
        if path.startswith('/session-log/'):
            self.handle_session_log(unquote(path[len('/session-log/'):]))
            return
        if path in ('/api/cost-v3/sessions', '/api/cost-v31/sessions', '/api/cost-v4/sessions'):
            self.handle_sessions()
            return
        if path in ('/api/cost-v31/messages', '/api/cost-v4/messages'):
            self.handle_messages(parsed)
            return
        if path in ('/api/cost-v3/refresh', '/api/cost-v4/refresh'):
            self.handle_refresh()
            return
        if path.startswith('/api/cost-v4/transcript/'):
            self.handle_openclaw_transcript(unquote(path[len('/api/cost-v4/transcript/'):]))
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

    def serve_jsonl_viewer(self):
        viewer = Path(__file__).resolve().parent / 'jsonl_viewer.html'
        if not viewer.exists():
            self._respond(404, f'JSONL viewer not found: {viewer}')
            return
        self._respond(200, viewer.read_text(encoding='utf-8'), 'text/html; charset=utf-8')

    def handle_session_log(self, session_id):
        if not session_id:
            self._respond(404, 'Session log not found\n')
            return

        conn = self._db()
        if not conn:
            self._respond(500, f'Database not found: {self.cfg["db"]}\n')
            return

        try:
            row = conn.execute(
                'SELECT filename FROM sessions WHERE session_id = ? LIMIT 1',
                (session_id,),
            ).fetchone()
        except Exception as exc:
            self._respond(500, f'Failed to look up session log: {exc}\n')
            return
        finally:
            conn.close()

        filename = row['filename'] if row else None
        if not filename:
            self._respond(404, 'Session log not found\n')
            return

        file_path = resolve_session_log_path(filename, self.cfg['sessions_dir'], self.cfg['codex_sessions_dir'])
        if not file_path:
            self._respond(404, 'Session log not found\n')
            return

        try:
            body = file_path.read_bytes()
        except Exception as exc:
            self._respond(500, f'Failed to read session log: {exc}\n')
            return

        if filename.startswith('codex_cli:'):
            safe_filename = Path(filename.split(':', 1)[1]).name.replace('"', '')
        else:
            safe_filename = filename.replace('"', '')
        self._respond(
            200,
            body,
            'text/plain; charset=utf-8',
            headers={'Content-Disposition': f'inline; filename="{safe_filename}"'},
        )

    def handle_openclaw_transcript(self, session_id):
        """Serve one OpenClaw SQLite transcript as JSONL for the viewer."""
        if not re.fullmatch(r'[0-9A-Za-z._:-]{1,160}', session_id or ''):
            self._json(400, {'error': 'Invalid session_id'})
            return

        lines = []
        agents_root = self.cfg['openclaw_agents_dir']
        for db_path in sorted(agents_root.glob('*/agent/openclaw-agent.sqlite')):
            try:
                conn = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True)
                conn.row_factory = sqlite3.Row
                try:
                    rows = conn.execute(
                        'SELECT event_json FROM transcript_events WHERE session_id = ? ORDER BY seq',
                        (session_id,),
                    ).fetchall()
                finally:
                    conn.close()
                if rows:
                    lines = [row['event_json'] for row in rows]
                    break
            except (OSError, sqlite3.Error):
                continue

        if lines:
            self._respond(200, '\n'.join(lines) + '\n', 'text/plain; charset=utf-8')
            return

        # Older JSONL-only OpenClaw installs still get a working log link.
        conn = self._db()
        try:
            row = conn.execute('SELECT filename FROM sessions WHERE session_id = ?', (session_id,)).fetchone() if conn else None
        finally:
            if conn:
                conn.close()
        filename = row['filename'] if row else None
        file_path = resolve_session_log_path(filename, self.cfg['sessions_dir'], self.cfg['codex_sessions_dir'])
        if file_path:
            self._respond(200, file_path.read_bytes(), 'text/plain; charset=utf-8')
            return
        self._json(404, {'error': f'No transcript found for session {session_id}'})

    def handle_jsonl_tail(self, parsed):
        """Serve a bounded slice of an explicitly allowed local text file."""
        params = parse_qs(parsed.query)
        raw_path = unquote((params.get('path') or [''])[0])
        if not raw_path:
            self._json(400, {'error': 'Missing path'})
            return
        candidate = Path(raw_path).expanduser().resolve()
        allowed_roots = [
            ROOT.resolve(),
            self.cfg['sessions_dir'].resolve(),
            self.cfg['codex_sessions_dir'].resolve(),
            self.cfg['openclaw_agents_dir'].resolve(),
            (Path.home() / '.claude').resolve(),
            (Path.home() / '.kimi-code').resolve(),
        ]
        if not any(candidate == root or candidate.is_relative_to(root) for root in allowed_roots):
            self._json(403, {'error': 'Path is outside the allowed local roots'})
            return
        if not candidate.is_file():
            self._json(404, {'error': f'File not found: {raw_path}'})
            return
        try:
            limit = max(1, min(int((params.get('lines') or ['2000'])[0]), 20000))
        except ValueError:
            limit = 2000
        try:
            start = int((params.get('start') or ['0'])[0])
        except ValueError:
            start = 0
        lines = candidate.read_text(encoding='utf-8', errors='replace').splitlines()
        total = len(lines)
        start_index = max(0, start - 1) if start > 0 else max(0, total - limit)
        selected = lines[start_index:start_index + limit]
        start_line = start_index + 1 if selected else 0
        end_line = start_index + len(selected)
        self._json(200, {
            'ok': True,
            'path': raw_path,
            'size': candidate.stat().st_size,
            'totalLines': total,
            'startLine': start_line,
            'endLine': end_line,
            'linesReturned': len(selected),
            'requestedLines': limit,
            'truncated': len(selected) < total,
            'hasPrev': start_index > 0,
            'hasNext': end_line < total,
            'text': '\n'.join(selected),
        })

    def _metadata_payload(self, conn):
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
            'kimi': 'kimi-code',
            'ollama-cloud': 'opencode',
            'deepseek-direct': 'deepseek',
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

        pioneer_ratecard = {}
        for row in self._read_payg_rates():
            model_raw = row.get('model_raw') or ''
            if model_raw.startswith('pioneer/'):
                pioneer_ratecard[model_raw] = {
                    'input': row.get('input_per_mtok') or 0,
                    'output': row.get('output_per_mtok') or 0,
                    'cache_read': row.get('cache_read_per_mtok') or 0,
                    'cache_write': row.get('cache_write_per_mtok') or 0,
                }

        return {
            'provider_totals': provider_totals,
            'provider_costs': provider_costs,
            'provider_cost_periods': provider_cost_periods,
            'provider_total_prus': provider_total_prus,
            'copilot_opus_tokens': tier_row['opus_tokens'] if tier_row else 0,
            'copilot_other_tokens': tier_row['other_tokens'] if tier_row else 0,
            'copilot_other_prus': tier_row['other_prus'] if tier_row else 0,
            'pioneer_ratecard': pioneer_ratecard,
            'pioneer_estimate_tokens': 1200,
        }

    def handle_sessions(self):
        params = parse_qs(urlparse(self.path).query)
        conn = self._db()
        if not conn:
            self._json(500, {'error': f'Database not found: {self.cfg["db"]}'})
            return
        try:
            start = (params.get('start') or [None])[0]
            end = (params.get('end') or [None])[0]
            usage_only = ((params.get('usage_only') or [''])[0].lower() in ('1', 'true', 'yes'))

            where = []
            args = []
            if start:
                where.append("COALESCE(seg.last_msg_ts, seg.first_msg_ts) >= ?")
                args.append(start)
            if end:
                where.append("COALESCE(seg.first_msg_ts, seg.last_msg_ts) <= ?")
                args.append(end)
            if usage_only:
                where.append("""(
                    COALESCE(seg.input_tokens, 0) > 0 OR
                    COALESCE(seg.output_tokens, 0) > 0 OR
                    COALESCE(seg.cache_read, 0) > 0 OR
                    COALESCE(seg.cache_write, 0) > 0 OR
                    COALESCE(seg.total_tokens, 0) > 0
                )""")
            where_sql = ("WHERE " + " AND ".join(where)) if where else ""
            sessions_query = f"""
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
                    seg.cost_input AS cost_input,
                    seg.cost_input + seg.cost_output AS cost_io,
                    seg.cost_cache_read + seg.cost_cache_write AS cost_cache,
                    seg.cost_total AS cost_logged
                FROM sessions s
                INNER JOIN segments seg ON s.session_id = seg.session_id
                LEFT JOIN model_reference mr ON seg.model_raw = mr.model_raw
                {where_sql}
                ORDER BY seg.first_msg_ts DESC
            """
            sessions = []
            for row in conn.execute(sessions_query, args).fetchall():
                item = dict(row)
                item['cost_estimated'] = row['cost_logged'] or 0
                sessions.append(item)

            payload = {'sessions': sessions}
            payload.update(self._metadata_payload(conn))
            self._json(200, payload)
        finally:
            conn.close()

    def handle_messages(self, parsed):
        params = parse_qs(parsed.query)
        session_id = (params.get('session_id') or [''])[0]
        model_raw = (params.get('model_raw') or [''])[0]
        segment_index_raw = (params.get('segment_index') or [''])[0]
        start = (params.get('start') or [None])[0]
        end = (params.get('end') or [None])[0]
        usage_only = ((params.get('usage_only') or [''])[0].lower() in ('1', 'true', 'yes'))

        if not session_id or not model_raw or segment_index_raw == '':
            self._json(400, {'error': 'Missing session_id, model_raw, or segment_index'})
            return

        try:
            segment_index = int(segment_index_raw)
        except ValueError:
            self._json(400, {'error': 'Invalid segment_index'})
            return

        conn = self._db()
        if not conn:
            self._json(500, {'error': f'Database not found: {self.cfg["db"]}'})
            return

        try:
            seg = conn.execute(
                """
                SELECT first_msg_ts, last_msg_ts
                FROM segments
                WHERE session_id = ? AND model_raw = ? AND segment_index = ?
                """,
                (session_id, model_raw, segment_index),
            ).fetchone()
            if not seg:
                self._json(404, {'error': 'Segment not found'})
                return

            where = [
                "m.session_id = ?",
                "mr.model_raw = ?",
                "m.timestamp >= ?",
                "m.timestamp <= ?",
            ]
            if any(row[1] == 'is_excluded' for row in conn.execute('PRAGMA table_info(messages)')):
                where.append('COALESCE(m.is_excluded, 0) = 0')
            args = [
                session_id,
                model_raw,
                seg['first_msg_ts'],
                seg['last_msg_ts'] or seg['first_msg_ts'],
            ]
            if start:
                where.append("m.timestamp >= ?")
                args.append(start)
            if end:
                where.append("m.timestamp <= ?")
                args.append(end)
            if usage_only:
                where.append("""(
                    COALESCE(m.input_tokens, 0) > 0 OR
                    COALESCE(m.output_tokens, 0) > 0 OR
                    COALESCE(m.cache_read, 0) > 0 OR
                    COALESCE(m.cache_write, 0) > 0 OR
                    COALESCE(m.total_tokens, 0) > 0
                )""")

            messages_query = f"""
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
                WHERE {" AND ".join(where)}
                ORDER BY m.timestamp ASC
            """
            messages = [dict(row) for row in conn.execute(messages_query, args).fetchall()]
            self._json(200, {'messages': messages})
        finally:
            conn.close()

    def handle_refresh(self):
        cmd = [
            sys.executable,
            str(self.cfg['migrate_script']),
            '--incremental',
            '--db', str(self.cfg['db']),
            '--sessions-dir', str(self.cfg['sessions_dir']),
            '--openclaw-agents-dir', str(self.cfg['openclaw_agents_dir']),
            '--openclaw-sqlite',
            '--codex-sessions-dir', str(self.cfg['codex_sessions_dir']),
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
            m = re.search(r'Processed (\d+) files', output)
            if m:
                processed_files = int(m.group(1))
            m = re.search(r'Assistant messages ingested/upserted: (\d+)', output)
            if m:
                processed_msgs = int(m.group(1))
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
        if table == 'payg_model_rates':
            self._json(200, {'table': table, 'rows': self._read_payg_rates()})
            return
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
            if table == 'payg_model_rates':
                self._write_payg_rates(rows)
                self._json(200, {
                    'ok': True,
                    'table': table,
                    'saved': len(rows),
                    'note': 'File-backed rate card saved; Refresh reprices derived costs.',
                })
                return
            if table == 'provider_costs':
                conn.execute('DELETE FROM provider_costs')
                for r in rows:
                    conn.execute(
                        'INSERT INTO provider_costs (provider, billing_start, plan_type, monthly_cost, extra_usage, notes) VALUES (?,?,?,?,?,?)',
                        (r.get('provider'), r.get('billing_start'), r.get('plan_type', 'flat'), r.get('monthly_cost'), r.get('extra_usage'), r.get('notes')),
                    )
            elif table == 'model_reference':
                import migrate_sessions as migrator
                for r in rows:
                    model_raw = r.get('model_raw') or ''
                    inferred_endpoint, inferred_author, _model, _pru = migrator.infer_model_reference(model_raw)
                    endpoint_in = r.get('endpoint') or inferred_endpoint or ''
                    conn.execute(
                        '''
                        INSERT INTO model_reference (model_raw, endpoint, author, model, pru_multiplier)
                        VALUES (?,?,?,?,?)
                        ON CONFLICT(model_raw) DO UPDATE SET
                            endpoint=excluded.endpoint,
                            author=excluded.author,
                            model=excluded.model,
                            pru_multiplier=excluded.pru_multiplier
                        ''',
                        (
                            model_raw,
                            migrator.MODEL_ENDPOINT_ALIASES.get(endpoint_in, endpoint_in),
                            r.get('author') or inferred_author or '',
                            migrator.canonical_model_display_name(model_raw),
                            1.0 if r.get('pru_multiplier') in (None, '') else r.get('pru_multiplier'),
                        ),
                    )
            else:
                conn.execute('DELETE FROM provider_pru_invoices')
                for r in rows:
                    conn.execute(
                        'INSERT INTO provider_pru_invoices (provider, cycle_start, cycle_end, model_raw, prus, included_requests, billed_requests, notes) VALUES (?,?,?,?,?,?,?,?)',
                        (r.get('provider'), r.get('cycle_start'), r.get('cycle_end'), r.get('model_raw'), r.get('prus', 0), r.get('included_requests'), r.get('billed_requests', 0), r.get('notes')),
                    )
            deleted_count = 0
            refused = []
            if table == 'model_reference':
                for model_raw in data.get('deleted') or []:
                    if not isinstance(model_raw, str) or not model_raw:
                        continue
                    ref_row = conn.execute('SELECT id FROM model_reference WHERE model_raw = ?', (model_raw,)).fetchone()
                    if not ref_row:
                        continue
                    used = conn.execute('SELECT COUNT(*) FROM messages WHERE model_ref_id = ?', (ref_row['id'],)).fetchone()[0]
                    if used:
                        refused.append({'model_raw': model_raw, 'messages': used})
                    else:
                        conn.execute('DELETE FROM model_reference WHERE id = ?', (ref_row['id'],))
                        deleted_count += 1
            conn.commit()
            self._json(200, {
                'ok': True,
                'table': table,
                'saved': len(rows),
                'deleted': deleted_count,
                'refused': refused,
            })
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

    def _payg_rate_path(self):
        return self.cfg['ref_dir'] / 'payg_model_rates.tsv'

    def _read_payg_rates(self):
        path = self._payg_rate_path()
        if not path.exists():
            return []
        rows = []
        for line in path.read_text(encoding='utf-8').splitlines():
            if not line.strip() or line.lstrip().startswith('#'):
                continue
            parts = line.split('\t')
            if parts[0].strip() == 'model_raw':
                continue
            row = {}
            for index, column in enumerate(PAYG_RATE_COLS):
                value = parts[index].strip() if index < len(parts) else ''
                if column in PAYG_RATE_NUMERIC:
                    try:
                        row[column] = float(value) if value else None
                    except ValueError:
                        row[column] = None
                else:
                    row[column] = value or None
            rows.append(row)
        return rows

    def _write_payg_rates(self, rows):
        path = self._payg_rate_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        comments = []
        if path.exists():
            for line in path.read_text(encoding='utf-8').splitlines():
                if line.lstrip().startswith('#'):
                    comments.append(line)
                else:
                    break
        if not comments:
            comments = [
                '# User-owned PAYG-equivalent rate card for imported usage.',
                '# USD per million tokens; edit in the browser or as TSV.',
            ]

        def fmt(value):
            if value in (None, ''):
                return ''
            try:
                number = float(value)
            except (TypeError, ValueError):
                return str(value)
            return str(int(number)) if number.is_integer() else str(number)

        lines = comments + ['\t'.join(PAYG_RATE_COLS)]
        for row in rows:
            lines.append('\t'.join(
                fmt(row.get(column)) if column in PAYG_RATE_NUMERIC else str(row.get(column) or '')
                for column in PAYG_RATE_COLS
            ))
        path.write_text('\n'.join(lines) + '\n', encoding='utf-8')


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
        'openclaw_agents_dir': Path(args.openclaw_agents_dir).expanduser().resolve(),
        'codex_sessions_dir': Path(args.codex_sessions_dir).expanduser().resolve(),
        'channel_mapping': Path(args.channel_mapping).expanduser().resolve() if args.channel_mapping else None,
        'backups_dir': Path(args.backups_dir).expanduser().resolve(),
    }
    with TrackerHTTPServer((args.host, args.port), TrackerHandler, config) as httpd:
        print(f'AI Cost Tracker server running at http://{args.host}:{args.port}')
        print(f'Database: {config["db"]}')
        print(f'Dashboard: {config["dashboard"]}')
        print(f'OpenClaw sessions: {config["sessions_dir"]}')
        print(f'OpenClaw agents: {config["openclaw_agents_dir"]}')
        print(f'Codex sessions: {config["codex_sessions_dir"]}')
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print('\nServer stopped')


if __name__ == '__main__':
    main()
