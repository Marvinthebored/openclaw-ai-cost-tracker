import sqlite3
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path

import migrate_sessions as migrator


class UsageReconciliationTests(unittest.TestCase):
    def setUp(self):
        # Synthetic tests never discover the developer's live OpenClaw stores.
        for name, value in [('collect_openclaw_sqlite_sources', []),
                            ('collect_scheduled_owner_keys', {})]:
            mock = patch.object(migrator, name, return_value=value)
            mock.start()
            self.addCleanup(mock.stop)
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "tracker.db"
        self.conn = sqlite3.connect(self.db_path)
        migrator.init_db(self.conn)
        migrator.populate_reference_tables(self.conn)

    def tearDown(self):
        self.conn.close()
        self.tmp.cleanup()

    def add_session(self, session_id, filename):
        self.conn.execute(
            """
            INSERT INTO sessions
            (session_id, session_nickname, filename, created_at, session_type)
            VALUES (?, ?, ?, '2026-07-01T00:00:00Z', 'gui')
            """,
            (session_id, session_id[:4], filename),
        )

    def add_message(
        self,
        *,
        message_id,
        session_id,
        timestamp,
        model_raw,
        source,
        input_tokens,
        output_tokens,
        cache_read,
        cache_write,
        total_tokens,
    ):
        migrator.insert_message_row(
            self.conn.cursor(),
            message_id,
            session_id,
            None,
            timestamp,
            model_raw,
            {
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "cache_read": cache_read,
                "cache_write": cache_write,
                "total_tokens": total_tokens,
            },
            ingest_source=source,
        )

    def test_reconciles_only_proven_codex_and_claude_duplicates(self):
        for session_id, filename in (
            ("oc", "openclaw_sqlite:main:test.db"),
            ("codex", "codex_cli:codex-home/main/rollout.jsonl"),
            ("claude", "claude:test.jsonl"),
        ):
            self.add_session(session_id, filename)

        self.add_message(
            message_id="oc-codex",
            session_id="oc",
            timestamp="2026-07-10T10:00:00.000Z",
            model_raw="openai/gpt-5.6-sol",
            source="openclaw_sqlite:main",
            input_tokens=100,
            output_tokens=20,
            cache_read=300,
            cache_write=0,
            total_tokens=420,
        )
        self.add_message(
            message_id="codex-duplicate",
            session_id="codex",
            timestamp="2026-07-10T09:59:59.500Z",
            model_raw="openai/gpt-5.6-sol",
            source="codex_app_server",
            input_tokens=100,
            output_tokens=20,
            cache_read=300,
            cache_write=0,
            total_tokens=420,
        )
        self.add_message(
            message_id="codex-token-mismatch",
            session_id="codex",
            timestamp="2026-07-10T09:59:59.600Z",
            model_raw="openai/gpt-5.6-sol",
            source="codex_app_server",
            input_tokens=101,
            output_tokens=20,
            cache_read=300,
            cache_write=0,
            total_tokens=421,
        )

        self.add_message(
            message_id="claude-cli-call",
            session_id="claude",
            timestamp="2026-07-11T10:00:00.000Z",
            model_raw="cli-claude/claude-opus-5",
            source="claude_cli",
            input_tokens=2,
            output_tokens=500,
            cache_read=100_000,
            cache_write=1_000,
            total_tokens=101_502,
        )
        self.add_message(
            message_id="claude-wrapper",
            session_id="oc",
            timestamp="2026-07-11T10:00:01.200Z",
            model_raw="anthropic/claude-opus-5",
            source="openclaw_sqlite:main",
            input_tokens=2,
            output_tokens=3,
            cache_read=100_000,
            cache_write=1_000,
            total_tokens=5,
        )
        self.add_message(
            message_id="claude-unmatched",
            session_id="oc",
            timestamp="2026-07-12T10:00:00.000Z",
            model_raw="anthropic/claude-opus-5",
            source="openclaw_sqlite:main",
            input_tokens=2,
            output_tokens=4,
            cache_read=0,
            cache_write=62_879,
            total_tokens=62_885,
        )
        self.conn.commit()

        touched, summary = migrator.reconcile_usage_duplicates(self.conn)

        self.assertEqual(summary["duplicate_codex_app_server_openclaw"]["calls"], 1)
        self.assertEqual(summary["duplicate_claude_wrapper_cli"]["calls"], 1)
        self.assertEqual(touched, {"codex", "claude", "oc"})
        rows = {
            row[0]: row[1:]
            for row in self.conn.execute(
                """
                SELECT id, is_excluded, duplicate_of, excluded_reason
                FROM messages
                ORDER BY id
                """
            )
        }
        self.assertEqual(
            rows["codex-duplicate"],
            (1, "oc-codex", "duplicate_codex_app_server_openclaw"),
        )
        self.assertEqual(
            rows["claude-wrapper"],
            (1, "claude-cli-call", "duplicate_claude_wrapper_cli"),
        )
        self.assertEqual(rows["codex-token-mismatch"], (0, None, None))
        self.assertEqual(rows["claude-unmatched"], (0, None, None))
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0], 6)
        claude_source = self.conn.execute(
            "SELECT session_type FROM sessions WHERE session_id = 'claude'"
        ).fetchone()[0]
        self.assertEqual(claude_source, "cli")

        attributions = self.conn.execute(
            """
            SELECT source_message_id, canonical_session_id, mr.model_raw, reason
            FROM message_attributions a
            JOIN model_reference mr ON mr.id = a.canonical_model_ref_id
            ORDER BY source_message_id
            """
        ).fetchall()
        self.assertEqual(attributions, [
            ("claude-cli-call", "oc", "anthropic/claude-opus-5", "openclaw_claude_runtime_usage"),
            ("codex-token-mismatch", "oc", "openai/gpt-5.6-sol", "openclaw_codex_runtime_usage"),
        ])
        effective = self.conn.execute(
            """
            SELECT seg.model_raw, seg.msg_count, seg.total_tokens
            FROM segments seg
            WHERE seg.session_id = 'oc'
            ORDER BY seg.segment_index
            """
        ).fetchall()
        self.assertEqual(effective, [
            ("openai/gpt-5.6-sol", 2, 841),
            ("anthropic/claude-opus-5", 2, 164_387),
        ])

    def test_routed_codex_thread_uses_per_call_rollout_rows(self):
        """A thread joined by idempotency key is accounted per rollout call.

        Both OpenClaw turn rows are exact sums of their calls (as the current
        app-server reports them); a turn row is superseded only while at least
        one attributed call falls in its window.
        """
        thread = "01a0aaaa-0000-7000-8000-000000000001"
        self.add_session("oc", "openclaw_sqlite:main:test.db")
        self.add_session(thread, "codex_cli:codex-home/main/rollout.jsonl")
        # Turn 1: two calls, turn row = sum. Turn 2: one call, an exact twin.
        calls = [
            ("call-1a", "2026-07-10T10:00:01.000Z", 900, 10, 0),
            ("call-1b", "2026-07-10T10:00:05.000Z", 20, 30, 896),
            ("call-2a", "2026-07-10T10:01:00.000Z", 40, 50, 1024),
        ]
        for message_id, timestamp, input_tokens, output_tokens, cache_read in calls:
            self.add_message(
                message_id=message_id,
                session_id=thread,
                timestamp=timestamp,
                model_raw="openai/gpt-6-astra",
                source="codex_app_server",
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                cache_read=cache_read,
                cache_write=0,
                total_tokens=input_tokens + output_tokens + cache_read,
            )
        turns = [
            ("oc-turn-1", "2026-07-10T10:00:05.100Z", 920, 40, 896),
            ("oc-turn-2", "2026-07-10T10:01:00.100Z", 40, 50, 1024),
            ("oc-turn-3", "2026-07-10T10:02:00.100Z", 7, 8, 9),  # rollout not ingested yet
        ]
        for message_id, timestamp, input_tokens, output_tokens, cache_read in turns:
            self.add_message(
                message_id=message_id,
                session_id="oc",
                timestamp=timestamp,
                model_raw="openai/gpt-6-astra",
                source="openclaw_sqlite:main",
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                cache_read=cache_read,
                cache_write=0,
                total_tokens=input_tokens + output_tokens + cache_read,
            )
        self.conn.commit()
        routes = {
            thread: {
                "session_id": "oc",
                "model_pairs": {("openai", "gpt-6-astra")},
                "message_ids": {"oc-turn-1", "oc-turn-2", "oc-turn-3"},
            }
        }
        original = migrator.collect_openclaw_codex_thread_routes
        migrator.collect_openclaw_codex_thread_routes = lambda include_unrouted=False: routes
        try:
            for attempt in range(2):  # idempotent
                touched, summary = migrator.reconcile_usage_duplicates(self.conn)
                if attempt == 0:
                    self.assertIn("oc", touched)
                else:
                    self.assertEqual(touched, set())
                self.assertEqual(
                    summary[migrator.SUPERSEDED_BY_CODEX_RUNTIME_REASON]["calls"], 2
                )
                # The exact twin (call-2a / oc-turn-2) must NOT be excluded in a routed thread.
                self.assertEqual(summary["duplicate_codex_app_server_openclaw"]["calls"], 0)
                rows = {
                    row[0]: row[1:]
                    for row in self.conn.execute(
                        "SELECT id, is_excluded, excluded_reason FROM messages"
                    )
                }
                self.assertEqual(rows["oc-turn-1"], (1, migrator.SUPERSEDED_BY_CODEX_RUNTIME_REASON))
                self.assertEqual(rows["oc-turn-2"], (1, migrator.SUPERSEDED_BY_CODEX_RUNTIME_REASON))
                self.assertEqual(rows["oc-turn-3"], (0, None))
                for message_id, *_ in calls:
                    self.assertEqual(rows[message_id], (0, None))
                attributed = self.conn.execute(
                    """
                    SELECT source_message_id FROM message_attributions
                    WHERE canonical_session_id = 'oc' AND reason = 'openclaw_codex_runtime_usage'
                    ORDER BY source_message_id
                    """
                ).fetchall()
                self.assertEqual([r[0] for r in attributed], ["call-1a", "call-1b", "call-2a"])
                effective = self.conn.execute(
                    """
                    SELECT model_raw, msg_count, input_tokens, output_tokens, cache_read, total_tokens
                    FROM segments WHERE session_id = 'oc'
                    """
                ).fetchall()
                # 3 calls + the uncovered turn-3 row; no double counting of turns 1 and 2.
                self.assertEqual(
                    effective,
                    [("openai/gpt-6-astra", 4, 967, 98, 1929, 2994)],
                )
        finally:
            migrator.collect_openclaw_codex_thread_routes = original


    def test_deleted_session_archive_still_routes_its_codex_thread(self):
        """Cron / cleanup=delete sessions leave only an archive; the route must survive."""
        import json
        from compression import zstd

        thread = "01a0bbbb-0000-7000-8000-000000000002"
        agent_db = Path(self.tmp.name) / "openclaw-agent.sqlite"
        src = sqlite3.connect(agent_db)
        src.execute("CREATE TABLE transcript_events (session_id TEXT, seq INTEGER, event_json TEXT)")
        src.execute(
            "CREATE TABLE session_transcript_archives "
            "(session_id TEXT, encoding TEXT, archive_blob BLOB)"
        )
        lines = [
            {"type": "session", "id": "cron-sess"},
            {"type": "message", "id": "turn-row", "message": {
                "role": "assistant", "provider": "openai", "model": "gpt-5.6-sol",
                "idempotencyKey": f"codex-app-server:{thread}:turn1:assistant:0"}},
        ]
        blob = zstd.compress("\n".join(json.dumps(l) for l in lines).encode())
        src.execute("INSERT INTO session_transcript_archives VALUES ('cron-sess', 'zstd', ?)", (blob,))
        src.commit()
        src.close()

        saved = (migrator.collect_openclaw_sqlite_sources, migrator.OPENCLAW_SESSION_SOURCES)
        migrator.collect_openclaw_sqlite_sources = lambda: [{"path": str(agent_db), "agent": "main"}]
        migrator.OPENCLAW_SESSION_SOURCES = []
        try:
            routes = migrator.collect_openclaw_codex_thread_routes()
        finally:
            migrator.collect_openclaw_sqlite_sources, migrator.OPENCLAW_SESSION_SOURCES = saved
        self.assertIn(thread, routes)
        self.assertEqual(routes[thread]["session_id"], "cron-sess")
        self.assertEqual(routes[thread]["message_ids"], {"turn-row"})

    def test_recomputed_segments_exclude_flagged_rows(self):
        self.add_session("s1", "openclaw_sqlite:main:test.db")
        self.add_message(
            message_id="kept",
            session_id="s1",
            timestamp="2026-07-15T00:00:00.000Z",
            model_raw="openai/gpt-5.5",
            source="openclaw_sqlite:main",
            input_tokens=10,
            output_tokens=5,
            cache_read=0,
            cache_write=0,
            total_tokens=15,
        )
        self.add_message(
            message_id="excluded",
            session_id="s1",
            timestamp="2026-07-15T00:00:01.000Z",
            model_raw="openai/gpt-5.5",
            source="openclaw_sqlite:main",
            input_tokens=100,
            output_tokens=50,
            cache_read=0,
            cache_write=0,
            total_tokens=150,
        )
        self.conn.execute(
            """
            UPDATE messages
            SET is_excluded = 1,
                duplicate_of = 'kept',
                excluded_reason = 'test'
            WHERE id = 'excluded'
            """
        )
        migrator.recompute_segments(self.conn, "s1")

        row = self.conn.execute(
            """
            SELECT msg_count, input_tokens, output_tokens, total_tokens
            FROM segments
            WHERE session_id = 's1'
            """
        ).fetchone()
        self.assertEqual(row, (1, 10, 5, 15))

    def test_duplicate_reconciliation_is_idempotent_and_one_to_one(self):
        self.add_session("oc", "openclaw_sqlite:main:test.db")
        self.add_session("codex", "codex_cli:codex-home/main/rollout.jsonl")
        for message_id, source, model_raw, timestamp in (
            ("oc-one", "openclaw_sqlite:main", "openai/gpt-5.6-sol", "2026-07-20T10:00:00.000Z"),
            ("codex-nearest", "codex_app_server", "openai/gpt-5.6-sol", "2026-07-20T10:00:00.200Z"),
            ("codex-second", "codex_app_server", "openai/gpt-5.6-sol", "2026-07-20T10:00:00.300Z"),
        ):
            self.add_message(
                message_id=message_id,
                session_id="oc" if source.startswith("openclaw") else "codex",
                timestamp=timestamp,
                model_raw=model_raw,
                source=source,
                input_tokens=100,
                output_tokens=20,
                cache_read=300,
                cache_write=0,
                total_tokens=420,
            )
        self.conn.commit()

        _, first = migrator.reconcile_usage_duplicates(self.conn)
        _, second = migrator.reconcile_usage_duplicates(self.conn)

        self.assertEqual(first, second)
        self.assertEqual(
            first["duplicate_codex_app_server_openclaw"]["calls"],
            1,
        )
        excluded = self.conn.execute(
            """
            SELECT id, duplicate_of
            FROM messages
            WHERE excluded_reason = 'duplicate_codex_app_server_openclaw'
            """
        ).fetchall()
        self.assertEqual(excluded, [("codex-nearest", "oc-one")])

    def test_new_model_display_best_guesses_are_provider_free(self):
        expected = {
            "anthropic/claude-opus-4-8": "opus-4.8",
            "openlimits/anthropic/claude-opus-4.8": "opus-4.8",
            "openrouter/deepseek/deepseek-v4-pro": "deepseek-v4-pro",
            "abacus/deepseek-ai/DeepSeek-V3.2": "deepseek-v3.2",
            "pioneer/zai-org/GLM-5.2": "glm-5.2",
            "fireworks-ai/accounts/fireworks/models/glm-5-fast": "glm-5-fast",
            "openlimits/minimax/minimax-m3": "minimax-m3",
            "kimi-coding/k2p5": "k2.5",
            "fireworks-ai/accounts/fireworks/routers/kimi-k2p6-turbo": "k2.6",
            "kimi-code/k3-256k": "k3",
            "kimi/kimi-for-coding-highspeed": "kimi-for-coding-highspeed",
        }
        for model_raw, display in expected.items():
            with self.subTest(model_raw=model_raw):
                self.assertEqual(
                    migrator.best_guess_model_display_name(model_raw),
                    display,
                )

    def test_existing_model_reference_aliases_survive_ingest(self):
        model_raw = "anthropic/claude-opus-4-8"
        self.conn.execute(
            """
            INSERT INTO model_reference
                (model_raw, endpoint, author, model, pru_multiplier)
            VALUES (?, ?, ?, ?, ?)
            """,
            (model_raw, "my-endpoint", "My Author", "Claude Big Boy", 7.5),
        )
        migrator.ensure_model_reference(self.conn.cursor(), model_raw)
        row = self.conn.execute(
            """
            SELECT endpoint, author, model, pru_multiplier
            FROM model_reference WHERE model_raw = ?
            """,
            (model_raw,),
        ).fetchone()
        self.assertEqual(tuple(row), ("my-endpoint", "My Author", "Claude Big Boy", 7.5))


if __name__ == "__main__":
    unittest.main()
