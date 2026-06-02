import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

import migrate_sessions


class CodexImportTests(unittest.TestCase):
    def test_codex_session_file_imports_into_existing_tables(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            sessions_root = root / "codex-sessions"
            log_dir = sessions_root / "2026" / "04" / "28"
            log_dir.mkdir(parents=True)
            log_path = log_dir / "rollout-2026-04-28T15-46-01-019dd30d-5ab3-71f2-9a23-2f0658896c77.jsonl"

            rows = [
                {
                    "type": "session_meta",
                    "timestamp": "2026-04-28T07:46:01.000Z",
                    "payload": {
                        "id": "019dd30d-5ab3-71f2-9a23-2f0658896c77",
                        "timestamp": "2026-04-28T07:46:01.000Z",
                        "source": "vscode",
                        "model_provider": "openai",
                        "model": "gpt-5.4",
                    },
                },
                {
                    "type": "turn_context",
                    "timestamp": "2026-04-28T07:46:02.000Z",
                    "payload": {
                        "model": "gpt-5.4",
                        "collaboration_mode": {"settings": {"model": "gpt-5.4"}},
                    },
                },
                {
                    "type": "event_msg",
                    "timestamp": "2026-04-28T07:46:12.144Z",
                    "payload": {
                        "type": "token_count",
                        "info": {
                            "last_token_usage": {
                                "input_tokens": 12567,
                                "cached_input_tokens": 7552,
                                "output_tokens": 303,
                                "total_tokens": 12870,
                            }
                        },
                    },
                },
                {
                    "type": "turn_context",
                    "timestamp": "2026-04-28T07:47:02.000Z",
                    "payload": {
                        "model": "gpt-5.4-mini",
                        "collaboration_mode": {"settings": {"model": "gpt-5.4-mini"}},
                    },
                },
                {
                    "type": "event_msg",
                    "timestamp": "2026-04-28T07:47:08.515Z",
                    "payload": {
                        "type": "token_count",
                        "info": {
                            "last_token_usage": {
                                "input_tokens": 45719,
                                "cached_input_tokens": 41344,
                                "output_tokens": 435,
                                "total_tokens": 46154,
                            }
                        },
                    },
                },
            ]
            log_path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")

            db_path = root / "cost_tracker.db"
            conn = sqlite3.connect(db_path)
            try:
                migrate_sessions.init_db(conn)
                migrate_sessions.populate_reference_tables(conn)
                migrate_sessions.migrate_compact_schema(conn)

                source = {"name": "codex_cli", "root": sessions_root}
                ok, msg_count, session_id = migrate_sessions.parse_session_file(log_path, conn, source)
                self.assertTrue(ok)
                self.assertEqual(msg_count, 2)
                self.assertEqual(session_id, "019dd30d-5ab3-71f2-9a23-2f0658896c77")

                session_row = conn.execute(
                    "SELECT filename, parent_nickname, channel_type, session_type FROM sessions WHERE session_id = ?",
                    (session_id,),
                ).fetchone()
                self.assertEqual(session_row[0], "codex_cli:2026/04/28/rollout-2026-04-28T15-46-01-019dd30d-5ab3-71f2-9a23-2f0658896c77.jsonl")
                self.assertEqual(session_row[1], "vscode")
                self.assertEqual(session_row[2], "gui")
                self.assertEqual(session_row[3], "codex_cli")

                message_rows = conn.execute(
                    """
                    SELECT mr.model_raw, m.input_tokens, m.cache_read, m.output_tokens, m.total_tokens
                    FROM messages m
                    INNER JOIN model_reference mr ON mr.id = m.model_ref_id
                    WHERE m.session_id = ?
                    ORDER BY m.timestamp ASC
                    """,
                    (session_id,),
                ).fetchall()
                self.assertEqual(
                    message_rows,
                    [
                        ("codex_cli/gpt-5.4", 12567, 7552, 303, 12870),
                        ("codex_cli/gpt-5.4-mini", 45719, 41344, 435, 46154),
                    ],
                )

                segment_rows = conn.execute(
                    "SELECT model_raw, segment_index, msg_count FROM segments WHERE session_id = ? ORDER BY segment_index ASC",
                    (session_id,),
                ).fetchall()
                self.assertEqual(
                    segment_rows,
                    [
                        ("codex_cli/gpt-5.4", 0, 1),
                        ("codex_cli/gpt-5.4-mini", 1, 1),
                    ],
                )
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
