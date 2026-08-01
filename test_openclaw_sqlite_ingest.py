import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

import migrate_sessions as migrator


class OpenClawSQLiteIngestTests(unittest.TestCase):
    def test_ingests_current_per_agent_schema_incrementally(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            agents_dir = root / "agents"
            source_path = agents_dir / "research" / "agent" / "openclaw-agent.sqlite"
            source_path.parent.mkdir(parents=True)
            source = sqlite3.connect(source_path)
            source.executescript(
                """
                CREATE TABLE session_nodes (
                    session_key TEXT,
                    current_session_id TEXT,
                    entry_json TEXT,
                    created_at INTEGER,
                    label TEXT,
                    display_name TEXT
                );
                CREATE TABLE transcript_events (
                    session_id TEXT,
                    seq INTEGER,
                    event_json TEXT,
                    created_at INTEGER
                );
                """
            )
            session_id = "11111111-2222-4333-8444-555555555555"
            source.execute(
                "INSERT INTO session_nodes VALUES (?, ?, ?, ?, ?, ?)",
                (
                    "agent:research:discord:channel:1234567890",
                    session_id,
                    json.dumps({
                        "createdAt": 1785580800000,
                        "displayName": "discord:example#research",
                        "delivery": {"origin": {"nativeChannelId": "1234567890"}},
                    }),
                    1785580800000,
                    "sqlite fixture",
                    "discord:example#research",
                ),
            )
            event = {
                "id": "assistant-event-1",
                "type": "message",
                "timestamp": "2026-08-01T00:00:01.000Z",
                "message": {
                    "role": "assistant",
                    "provider": "openai",
                    "model": "gpt-test",
                    "usage": {
                        "input": 120,
                        "output": 30,
                        "cacheRead": 450,
                        "cacheWrite": 0,
                        "totalTokens": 600,
                        "cost": {"input": 0.1, "output": 0.2, "total": 0.3},
                    },
                },
            }
            source.execute(
                "INSERT INTO transcript_events VALUES (?, ?, ?, ?)",
                (session_id, 1, json.dumps(event), 1785580801000),
            )
            source.commit()
            source.close()

            tracker = sqlite3.connect(root / "tracker.db")
            migrator.init_db(tracker)
            migrator.populate_reference_tables(tracker)
            previous_agents_dir = migrator.OPENCLAW_AGENTS_DIR
            migrator.OPENCLAW_AGENTS_DIR = agents_dir
            try:
                touched = migrator.ingest_openclaw_sqlite(tracker)
                self.assertEqual(touched, {session_id})
                self.assertEqual(migrator.ingest_openclaw_sqlite(tracker), set())
            finally:
                migrator.OPENCLAW_AGENTS_DIR = previous_agents_dir

            row = tracker.execute(
                """
                SELECT s.session_type, s.channel_name, mr.model_raw,
                       m.input_tokens, m.cache_read, m.total_tokens, m.ingest_source
                FROM messages m
                JOIN sessions s ON s.session_id = m.session_id
                JOIN model_reference mr ON mr.id = m.model_ref_id
                WHERE m.id = 'assistant-event-1'
                """
            ).fetchone()
            tracker.close()
            self.assertEqual(
                row,
                ("discord", "#research", "openai/gpt-test", 120, 450, 600, "openclaw_sqlite:research"),
            )


if __name__ == "__main__":
    unittest.main()
