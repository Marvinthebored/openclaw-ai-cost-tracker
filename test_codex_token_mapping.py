import importlib.util
import json
import pathlib
import sqlite3
import tempfile
import unittest


MODULE_PATH = pathlib.Path(__file__).with_name("migrate_sessions.py")
SPEC = importlib.util.spec_from_file_location("migrate_sessions", MODULE_PATH)
migrate_sessions = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(migrate_sessions)


class CodexTokenMappingTest(unittest.TestCase):
    def parse_codex_lines(self, lines):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            session_id = "019e38b2-8fe6-73f3-bd01-5aa73e41a5dd"
            codex_file = root / f"rollout-2026-05-18T09-28-06-{session_id}.jsonl"
            codex_file.write_text("\n".join(json.dumps(line) for line in lines) + "\n")

            old_sources = migrate_sessions.OPENCLAW_SESSION_SOURCES
            old_cache = migrate_sessions._OPENCLAW_CODEX_THREAD_IDS_CACHE
            conn = None
            try:
                migrate_sessions.OPENCLAW_SESSION_SOURCES = []
                migrate_sessions._OPENCLAW_CODEX_THREAD_IDS_CACHE = set()
                conn = sqlite3.connect(":memory:")
                migrate_sessions.init_db(conn)
                success, count, parsed_session_id = migrate_sessions.parse_codex_session_file(codex_file, conn)
                rows = conn.execute(
                    """
                    SELECT input_tokens, output_tokens, cache_read, cache_write, total_tokens
                    FROM messages
                    ORDER BY timestamp, id
                    """
                ).fetchall()
                return success, count, parsed_session_id, rows
            finally:
                if conn is not None:
                    conn.close()
                migrate_sessions.OPENCLAW_SESSION_SOURCES = old_sources
                migrate_sessions._OPENCLAW_CODEX_THREAD_IDS_CACHE = old_cache

    def token_count_line(self, last_usage, total_usage=None, limit_id="codex", timestamp="2026-05-18T01:30:41.188Z"):
        info = {"last_token_usage": last_usage}
        if total_usage is not None:
            info["total_token_usage"] = total_usage
        return {
            "timestamp": timestamp,
            "type": "event_msg",
            "payload": {
                "type": "token_count",
                "info": info,
                "rate_limits": {"limit_id": limit_id},
            },
        }

    def test_cached_input_is_removed_from_stored_input(self):
        usage = {
            "input_tokens": 137732,
            "cached_input_tokens": 137088,
            "output_tokens": 193,
            "total_tokens": 137925,
            "reasoning_output_tokens": 17,
        }

        normalized = migrate_sessions.normalize_codex_usage(usage)

        self.assertEqual(normalized["input_tokens"], 644)
        self.assertEqual(normalized["cache_read"], 137088)
        self.assertEqual(normalized["output_tokens"], 193)
        self.assertEqual(normalized["total_tokens"], 137925)
        self.assertNotIn("reasoning_output_tokens", normalized)

    def test_duplicate_token_count_usage_with_different_rate_limit_ids_counts_once(self):
        usage = {
            "input_tokens": 13_653,
            "cached_input_tokens": 13_184,
            "output_tokens": 146,
            "reasoning_output_tokens": 16,
            "total_tokens": 13_799,
        }

        success, count, _, rows = self.parse_codex_lines([
            {
                "type": "session_meta",
                "timestamp": "2026-05-18T01:28:06.000Z",
                "payload": {"id": "019e38b2-8fe6-73f3-bd01-5aa73e41a5dd"},
            },
            self.token_count_line(usage, usage, "codex"),
            self.token_count_line(usage, usage, "codex_bengalfox"),
        ])

        self.assertTrue(success)
        self.assertEqual(count, 1)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0], (469, 146, 13_184, 0, 13_799))

    def test_distinct_token_count_running_totals_are_counted(self):
        first = {
            "input_tokens": 13_653,
            "cached_input_tokens": 13_184,
            "output_tokens": 146,
            "total_tokens": 13_799,
        }
        second_last = {
            "input_tokens": 16_400,
            "cached_input_tokens": 13_184,
            "output_tokens": 425,
            "total_tokens": 16_825,
        }
        second_total = {
            "input_tokens": 30_053,
            "cached_input_tokens": 26_368,
            "output_tokens": 571,
            "total_tokens": 30_624,
        }

        success, count, _, rows = self.parse_codex_lines([
            {
                "type": "session_meta",
                "timestamp": "2026-05-18T01:28:06.000Z",
                "payload": {"id": "019e38b2-8fe6-73f3-bd01-5aa73e41a5dd"},
            },
            self.token_count_line(first, first, "codex", "2026-05-18T01:30:41.188Z"),
            self.token_count_line(second_last, second_total, "codex_bengalfox", "2026-05-18T01:30:47.452Z"),
        ])

        self.assertTrue(success)
        self.assertEqual(count, 2)
        self.assertEqual([row[4] for row in rows], [13_799, 16_825])


    def test_cached_input_cannot_drive_input_negative(self):
        usage = {
            "input_tokens": 100,
            "cached_input_tokens": 125,
            "output_tokens": 9,
            "total_tokens": 109,
        }

        normalized = migrate_sessions.normalize_codex_usage(usage)

        self.assertEqual(normalized["input_tokens"], 0)
        self.assertEqual(normalized["cache_read"], 125)

    def test_cli_payg_equivalent_cost_uses_cache_read_rate(self):
        usage = {
            "input_tokens": 1_000_000,
            "output_tokens": 100_000,
            "cache_read": 2_000_000,
            "cache_write": 0,
            "total_tokens": 3_100_000,
        }

        cost = migrate_sessions.calculate_payg_equivalent_cost("codex/gpt-5.5", usage)

        self.assertAlmostEqual(cost["input"], 5.0)
        self.assertAlmostEqual(cost["cacheRead"], 1.0)
        self.assertAlmostEqual(cost["output"], 3.0)
        self.assertAlmostEqual(cost["total"], 9.0)

    def test_existing_nonzero_cost_wins_over_cli_calculation(self):
        usage = {"input_tokens": 1_000_000, "output_tokens": 1_000_000, "cache_read": 0, "cache_write": 0}
        existing = {"input": 0.01, "output": 0, "cacheRead": 0, "cacheWrite": 0, "total": 0.01}

        cost = migrate_sessions.maybe_calculate_cli_cost("codex/gpt-5.5", usage, existing)

        self.assertEqual(cost, existing)

    def test_claude_cli_uses_canonical_claude_rate(self):
        usage = {
            "input_tokens": 1_000_000,
            "output_tokens": 100_000,
            "cache_read": 2_000_000,
            "cache_write": 10_000,
        }

        cost = migrate_sessions.calculate_payg_equivalent_cost(
            "claude-cli/claude-sonnet-4-6", usage
        )

        self.assertAlmostEqual(cost["input"], 3.0)
        self.assertAlmostEqual(cost["output"], 1.5)
        self.assertAlmostEqual(cost["cacheRead"], 0.6)
        self.assertAlmostEqual(cost["cacheWrite"], 0.0375)
        self.assertAlmostEqual(cost["total"], 5.1375)

    def test_current_fable_model_key_has_a_rate(self):
        cost = migrate_sessions.calculate_payg_equivalent_cost(
            "claude/claude-fable-5-1",
            {"input_tokens": 1_000_000, "output_tokens": 0, "cache_read": 0, "cache_write": 0},
        )

        self.assertAlmostEqual(cost["total"], 10.0)

    def test_explicit_openclaw_codex_thread_ids_are_collected(self):
        thread_id = "019ddc6c-92cd-7f03-99ca-f8a1cfa1fdc0"
        turn_id = "019ddc6c-92f6-7290-8f79-b142d0b9fa69"

        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            (root / "normal.jsonl").write_text(
                json.dumps({
                    "type": "message",
                    "message": {
                        "role": "assistant",
                        "idempotencyKey": f"codex-app-server:{thread_id}:{turn_id}:assistant:1",
                    },
                }) + "\n"
            )
            (root / "normal.trajectory.jsonl").write_text(
                json.dumps({"data": {"threadId": thread_id, "turnId": turn_id}}) + "\n"
            )
            (root / "normal.jsonl.codex-app-server.json").write_text(
                json.dumps({"threadId": thread_id})
            )

            old_sources = migrate_sessions.OPENCLAW_SESSION_SOURCES
            old_cache = migrate_sessions._OPENCLAW_CODEX_THREAD_IDS_CACHE
            try:
                migrate_sessions.OPENCLAW_SESSION_SOURCES = [{"key": "main", "label": "test", "dir": root}]
                migrate_sessions._OPENCLAW_CODEX_THREAD_IDS_CACHE = None

                self.assertIn(thread_id, migrate_sessions.collect_openclaw_codex_thread_ids())
            finally:
                migrate_sessions.OPENCLAW_SESSION_SOURCES = old_sources
                migrate_sessions._OPENCLAW_CODEX_THREAD_IDS_CACHE = old_cache

    def test_openclaw_codex_file_is_retained_as_runtime_evidence(self):
        thread_id = "019ddc6c-92cd-7f03-99ca-f8a1cfa1fdc0"

        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            codex_file = root / f"rollout-2026-04-30T11-26-36-{thread_id}.jsonl"
            codex_file.write_text("\n".join([
                json.dumps({
                    "type": "session_meta",
                    "timestamp": "2026-04-30T03:26:36.000Z",
                    "payload": {"id": thread_id, "originator": "openclaw", "source": "vscode"},
                }),
                json.dumps({
                    "type": "event_msg",
                    "timestamp": "2026-04-30T03:26:37.000Z",
                    "payload": {
                        "type": "token_count",
                        "turn_id": "019ddc6c-92f6-7290-8f79-b142d0b9fa69",
                        "info": {"last_token_usage": {"input_tokens": 10, "output_tokens": 2, "total_tokens": 12}},
                    },
                }),
            ]) + "\n")

            old_sources = migrate_sessions.OPENCLAW_SESSION_SOURCES
            old_cache = migrate_sessions._OPENCLAW_CODEX_THREAD_IDS_CACHE
            try:
                migrate_sessions.OPENCLAW_SESSION_SOURCES = [{"key": "main", "label": "test", "dir": root}]
                migrate_sessions._OPENCLAW_CODEX_THREAD_IDS_CACHE = {thread_id}

                conn = sqlite3.connect(":memory:")
                migrate_sessions.init_db(conn)
                success, count, session_id = migrate_sessions.parse_codex_session_file(codex_file, conn)

                self.assertTrue(success)
                self.assertEqual(count, 1)
                self.assertEqual(session_id, thread_id)
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0], 1)
                model_raw, ingest_source = conn.execute(
                    """
                    SELECT mr.model_raw, m.ingest_source
                    FROM messages m
                    JOIN model_reference mr ON mr.id = m.model_ref_id
                    """
                ).fetchone()
                self.assertEqual((model_raw, ingest_source), ("openai/unknown", "codex_app_server"))
                session_type, channel_type, label = conn.execute(
                    "SELECT session_type, channel_type, parent_nickname FROM sessions"
                ).fetchone()
                self.assertEqual((session_type, channel_type, label), ("openclaw", "openclaw", None))
            finally:
                conn.close()
                migrate_sessions.OPENCLAW_SESSION_SOURCES = old_sources
                migrate_sessions._OPENCLAW_CODEX_THREAD_IDS_CACHE = old_cache

    def test_openclaw_v3_string_user_content_and_session_index_metadata(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            session_id = "e8bba757-1271-4ddf-9bdf-c1ff9b58f9c6"
            session_file = root / f"{session_id}.jsonl"
            sessions_index = root / "sessions.json"
            session_file.write_text("\n".join([
                json.dumps({
                    "type": "session",
                    "version": 3,
                    "id": session_id,
                    "timestamp": "2026-05-30T00:26:45.088Z",
                }),
                json.dumps({
                    "type": "message",
                    "id": "user-1",
                    "parentId": None,
                    "timestamp": "2026-05-30T00:26:45.086Z",
                    "message": {
                        "role": "user",
                        "sourceChannel": "discord",
                        "content": "Hey marv. the AI-cost-tracker broke.",
                    },
                }),
                json.dumps({
                    "type": "message",
                    "id": "assistant-1",
                    "parentId": "user-1",
                    "timestamp": "2026-05-30T00:27:00.000Z",
                    "message": {
                        "role": "assistant",
                        "provider": "openai",
                        "model": "gpt-5.5",
                        "content": [{"type": "text", "text": "Fixed."}],
                        "usage": {
                            "input": 10,
                            "output": 5,
                            "cacheRead": 20,
                            "cacheWrite": 0,
                            "totalTokens": 35,
                            "cost": {"input": 0.0, "output": 0.0, "cacheRead": 0.0, "cacheWrite": 0.0, "total": 0.0},
                        },
                    },
                }),
            ]) + "\n")
            sessions_index.write_text(json.dumps({
                "agent:main:discord:channel:1471126313480753162": {
                    "sessionId": session_id,
                    "channel": "discord",
                    "chatType": "channel",
                    "groupId": "1471126313480753162",
                    "displayName": "discord:1471126071805214730#operations",
                }
            }))

            old_sessions_index_path = migrate_sessions.SESSIONS_INDEX_PATH
            old_session_index_cache = migrate_sessions._OPENCLAW_SESSION_INDEX_CACHE
            conn = None
            try:
                migrate_sessions.SESSIONS_INDEX_PATH = sessions_index
                migrate_sessions._OPENCLAW_SESSION_INDEX_CACHE = None
                conn = sqlite3.connect(":memory:")
                migrate_sessions.init_db(conn)
                success, count, parsed_session_id = migrate_sessions.parse_session_file(session_file, conn, "main")

                self.assertTrue(success)
                self.assertEqual(count, 1)
                self.assertEqual(parsed_session_id, session_id)
                session_row = conn.execute(
                    "SELECT session_type, channel_type, channel_id, channel_name, parent_nickname FROM sessions WHERE session_id = ?",
                    (session_id,),
                ).fetchone()
                self.assertEqual(session_row, ("discord", "discord", "1471126313480753162", "#operations", None))
            finally:
                if conn is not None:
                    conn.close()
                migrate_sessions.SESSIONS_INDEX_PATH = old_sessions_index_path
                migrate_sessions._OPENCLAW_SESSION_INDEX_CACHE = old_session_index_cache

    def test_kimi_wire_usage_records_ingest_with_ratecard_costs(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            session_dir = root / "sessions" / "wd_test_123" / "session_7afda8c5-96c3-4b60-9039-629ec0a0bfdf"
            wire = session_dir / "agents" / "main" / "wire.jsonl"
            wire.parent.mkdir(parents=True)
            (session_dir / "state.json").write_text(json.dumps({
                "createdAt": "2026-07-27T10:10:52.520Z",
                "workDir": "/tmp/ai-cost-tracker-v3",
            }))
            wire.write_text("\n".join([
                json.dumps({
                    "type": "usage.record",
                    "model": "kimi-code/k3-256k",
                    "usage": {
                        "inputOther": 1000,
                        "output": 200,
                        "inputCacheRead": 3000,
                        "inputCacheCreation": 0,
                    },
                    "time": 1785146605231,
                }),
                json.dumps({
                    "type": "usage.record",
                    "model": "kimi-code/k3-256k",
                    "usage": {
                        "inputOther": 500,
                        "output": 100,
                        "inputCacheRead": 1000,
                        "inputCacheCreation": 0,
                    },
                    "time": 1785146617005,
                }),
            ]) + "\n")

            old_kimi_dir = migrate_sessions.KIMI_CODE_DIR
            conn = None
            try:
                migrate_sessions.KIMI_CODE_DIR = root
                conn = sqlite3.connect(":memory:")
                migrate_sessions.init_db(conn)
                success, count, session_id = migrate_sessions.parse_kimi_session_file(wire, conn)

                self.assertTrue(success)
                self.assertEqual(count, 2)
                self.assertEqual(session_id, "session_7afda8c5-96c3-4b60-9039-629ec0a0bfdf")
                rows = conn.execute(
                    """
                    SELECT input_tokens, output_tokens, cache_read, cache_write, total_tokens,
                           ROUND(cost_total, 6), cost_source, cost_kind
                    FROM messages
                    ORDER BY timestamp, id
                    """
                ).fetchall()
                self.assertEqual(rows, [
                    (1000, 200, 3000, 0, 4200, 0.0069, "ratecard", "payg_equivalent"),
                    (500, 100, 1000, 0, 1600, 0.0033, "ratecard", "payg_equivalent"),
                ])
                session_row = conn.execute(
                    "SELECT parent_nickname, channel_type, session_type FROM sessions WHERE session_id = ?",
                    (session_id,),
                ).fetchone()
                self.assertEqual(session_row, ("kimi:ai-cost-tracker-v3", "cli", "cli"))
            finally:
                if conn is not None:
                    conn.close()
                migrate_sessions.KIMI_CODE_DIR = old_kimi_dir

    def test_openclaw_sqlite_usage_ingest_is_incremental(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            agent_db = root / "main" / "agent" / "openclaw-agent.sqlite"
            agent_db.parent.mkdir(parents=True)
            source = sqlite3.connect(agent_db)
            source.executescript(
                """
                CREATE TABLE session_nodes (
                    session_key TEXT PRIMARY KEY,
                    current_session_id TEXT,
                    entry_json TEXT,
                    updated_at INTEGER,
                    status TEXT,
                    created_at INTEGER,
                    label TEXT,
                    display_name TEXT
                );
                CREATE TABLE transcript_events (
                    session_id TEXT NOT NULL,
                    seq INTEGER NOT NULL,
                    event_json TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    PRIMARY KEY (session_id, seq)
                );
                """
            )
            session_id = "7a4fd296-7fc1-4eb5-ad49-862f6fa2fd40"
            source.execute(
                """
                INSERT INTO session_nodes
                (session_key, current_session_id, entry_json, updated_at, created_at, display_name)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    "agent:main:discord:channel:1529687628868227082",
                    session_id,
                    json.dumps({"createdAt": 1785371200000, "displayName": "discord:test#dividends"}),
                    1785371200000,
                    1785371200000,
                    "discord:test#dividends",
                ),
            )
            source.execute(
                """
                INSERT INTO transcript_events (session_id, seq, event_json, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (
                    session_id,
                    1,
                    json.dumps({
                        "type": "message",
                        "id": "k3-event-1",
                        "timestamp": "2026-07-30T00:31:46.511Z",
                        "message": {
                            "role": "assistant",
                            "provider": "kimi",
                            "model": "k3",
                            "usage": {
                                "input": 771,
                                "output": 884,
                                "cacheRead": 231680,
                                "cacheWrite": 0,
                                "totalTokens": 233335,
                                "cost": {"total": 0.096661},
                            },
                        },
                    }),
                    1785371506511,
                ),
            )
            source.execute(
                """
                INSERT INTO transcript_events (session_id, seq, event_json, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (
                    session_id,
                    2,
                    json.dumps({
                        "type": "reset",
                        "id": "reset-1",
                        "timestamp": "2026-07-30T00:32:00.000Z",
                        "reason": "new",
                    }),
                    1785371520000,
                ),
            )
            source.execute(
                """
                INSERT INTO transcript_events (session_id, seq, event_json, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (
                    session_id,
                    3,
                    json.dumps({
                        "type": "message",
                        "id": "k3-event-2",
                        "timestamp": "2026-07-30T00:32:10.000Z",
                        "message": {
                            "role": "assistant",
                            "provider": "kimi",
                            "model": "k3",
                            "usage": {
                                "input": 100,
                                "output": 20,
                                "cacheRead": 300,
                                "cacheWrite": 0,
                                "totalTokens": 420,
                                "cost": {"total": 0.001},
                            },
                        },
                    }),
                    1785371530000,
                ),
            )
            source.commit()
            source.close()

            old_agents_dir = migrate_sessions.OPENCLAW_AGENTS_DIR
            tracker = None
            try:
                migrate_sessions.OPENCLAW_AGENTS_DIR = root
                tracker = sqlite3.connect(":memory:")
                migrate_sessions.init_db(tracker)

                first_touched = migrate_sessions.ingest_openclaw_sqlite(tracker)
                second_touched = migrate_sessions.ingest_openclaw_sqlite(tracker)

                self.assertEqual(first_touched, {session_id})
                self.assertEqual(second_touched, set())
                self.assertEqual(tracker.execute("SELECT COUNT(*) FROM messages").fetchone()[0], 2)
                rows = tracker.execute(
                    """
                    SELECT mr.model_raw, mr.endpoint, mr.author, m.total_tokens, ROUND(m.cost_total, 6)
                    FROM messages m
                    JOIN model_reference mr ON mr.id = m.model_ref_id
                    ORDER BY m.timestamp
                    """
                ).fetchall()
                self.assertEqual(rows, [
                    ("kimi/k3", "kimi-code", "Moonshot", 233335, 0.096661),
                    ("kimi/k3", "kimi-code", "Moonshot", 420, 0.001),
                ])
                segments = tracker.execute(
                    """
                    SELECT segment_index, model_raw, msg_count, total_tokens
                    FROM segments WHERE session_id = ? ORDER BY segment_index
                    """,
                    (session_id,),
                ).fetchall()
                self.assertEqual(segments, [
                    (0, "kimi/k3", 1, 233335),
                    (1, "kimi/k3", 1, 420),
                ])
                self.assertEqual(
                    tracker.execute(
                        "SELECT timestamp, reason FROM session_boundaries WHERE session_id = ?",
                        (session_id,),
                    ).fetchone(),
                    ("2026-07-30T00:32:00.000Z", "new"),
                )
                session_row = tracker.execute(
                    "SELECT channel_name, channel_type, session_type FROM sessions WHERE session_id = ?",
                    (session_id,),
                ).fetchone()
                self.assertEqual(session_row, ("#dividends", "discord", "discord"))
            finally:
                if tracker is not None:
                    tracker.close()
                migrate_sessions.OPENCLAW_AGENTS_DIR = old_agents_dir


if __name__ == "__main__":
    unittest.main()
