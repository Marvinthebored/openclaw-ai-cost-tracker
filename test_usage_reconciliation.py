import sqlite3
import tempfile
import unittest
from pathlib import Path

import migrate_sessions as migrator


class UsageReconciliationTests(unittest.TestCase):
    def setUp(self):
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
            model_raw="codex_cli/gpt-5.6-sol",
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
            model_raw="codex_cli/gpt-5.6-sol",
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
            model_raw="claude/claude-opus-5",
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
            model_raw="claude-cli/claude-opus-5",
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
            model_raw="claude-cli/claude-opus-5",
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
        self.assertEqual(touched, {"codex", "oc"})
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
        self.assertEqual(claude_source, "openclaw")

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
            ("codex-nearest", "codex_app_server", "codex_cli/gpt-5.6-sol", "2026-07-20T10:00:00.200Z"),
            ("codex-second", "codex_app_server", "codex_cli/gpt-5.6-sol", "2026-07-20T10:00:00.300Z"),
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

    def test_model_display_aliases_are_provider_free_and_stable(self):
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
                    migrator.canonical_model_display_name(model_raw),
                    display,
                )


if __name__ == "__main__":
    unittest.main()
