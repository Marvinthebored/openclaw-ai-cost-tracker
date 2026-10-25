"""Claude transcripts must be classified by their recorded launcher.

`entrypoint` is the Claude analogue of Codex's `originator`: 'sdk-cli' means
the OpenClaw agent-SDK wrapper launched it, 'cli' means a hand-run terminal.
Before this, every Claude transcript was hardcoded to session_type='cli', so
OpenClaw-owned sessions showed up as direct CLI use.
"""

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


SESSION_ID = "89008805-c45b-4d61-aca7-3cd07e823f54"


def claude_lines(entrypoint, cwd="/home/example/.openclaw/workspace/assistant"):
    """Minimal transcript: a meta-ish user line plus one assistant line w/ usage."""
    base = {
        "sessionId": SESSION_ID,
        "cwd": cwd,
        "version": "2.1.272",
        "gitBranch": "main",
        "userType": "external",
        "isSidechain": False,
    }
    if entrypoint is not None:
        base["entrypoint"] = entrypoint
    return [
        {**base, "type": "user", "uuid": "u1", "parentUuid": None,
         "timestamp": "2026-09-20T01:00:00.000Z",
         "message": {"role": "user", "content": "hi"}},
        {**base, "type": "assistant", "uuid": "a1", "parentUuid": "u1",
         "timestamp": "2026-09-20T01:00:05.000Z",
         "message": {
             "role": "assistant",
             "model": "claude-opus-5",
             "content": [{"type": "text", "text": "hello"}],
             "usage": {
                 "input_tokens": 10,
                 "output_tokens": 20,
                 "cache_read_input_tokens": 30,
                 "cache_creation_input_tokens": 40,
             },
         }},
    ]


class ClaudeSessionProvenanceTest(unittest.TestCase):
    def parse(self, entrypoint, cwd="/home/example/.openclaw/workspace/assistant"):
        """Parse a synthetic transcript under a fake CLAUDE_PROJECTS_DIR."""
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            project = root / "-Users-admin--openclaw-workspace-marvin"
            project.mkdir(parents=True)
            path = project / f"{SESSION_ID}.jsonl"
            path.write_text(
                "\n".join(json.dumps(x) for x in claude_lines(entrypoint, cwd)) + "\n"
            )

            old_dir = migrate_sessions.CLAUDE_PROJECTS_DIR
            conn = None
            try:
                migrate_sessions.CLAUDE_PROJECTS_DIR = root
                conn = sqlite3.connect(":memory:")
                migrate_sessions.init_db(conn)
                ok, count, sid = migrate_sessions.parse_claude_session_file(path, conn)
                self.assertTrue(ok, "parser should accept the transcript")
                row = conn.execute(
                    "SELECT session_type, channel_type, filename FROM sessions WHERE session_id = ?",
                    (sid,),
                ).fetchone()
                # The refresh-time backfill must not stomp the classification.
                migrate_sessions.reclassify_runtime_session_identities(conn)
                after = conn.execute(
                    "SELECT session_type, channel_type FROM sessions WHERE session_id = ?",
                    (sid,),
                ).fetchone()
                return row, after
            finally:
                if conn is not None:
                    conn.close()
                migrate_sessions.CLAUDE_PROJECTS_DIR = old_dir

    def test_sdk_cli_is_openclaw_owned(self):
        (stype, ctype, _), after = self.parse("sdk-cli")
        self.assertEqual(stype, "openclaw")
        self.assertEqual(ctype, "openclaw")
        self.assertEqual(after, ("openclaw", "openclaw"), "backfill must not reset to cli")

    def test_sdk_ts_is_openclaw_owned(self):
        """Older TypeScript agent SDK: same OpenClaw harness, different label."""
        (stype, ctype, _), after = self.parse("sdk-ts")
        self.assertEqual(stype, "openclaw")
        self.assertEqual(after, ("openclaw", "openclaw"))

    def test_hand_run_cli_stays_cli(self):
        (stype, ctype, _), after = self.parse("cli", cwd="/home/example/project")
        self.assertEqual(stype, "cli")
        self.assertEqual(ctype, "cli")
        self.assertEqual(after, ("cli", "cli"))

    def test_missing_entrypoint_defaults_to_cli(self):
        """Pre-2.1 transcripts have no entrypoint; do not guess OpenClaw."""
        (stype, ctype, _), after = self.parse(None)
        self.assertEqual(stype, "cli")
        self.assertEqual(after, ("cli", "cli"))

    def test_cwd_is_not_the_discriminator(self):
        """A hand-run session inside the OpenClaw workspace is still CLI."""
        (stype, _, _), _ = self.parse("cli", cwd="/home/example/.openclaw/workspace/assistant")
        self.assertEqual(stype, "cli")

    def test_helper_mapping(self):
        for sdk in ("sdk-cli", "sdk-ts", "sdk-py"):
            self.assertEqual(migrate_sessions.claude_session_kind(sdk), "openclaw")
        for other in ("cli", None, "vscode", ""):
            self.assertEqual(migrate_sessions.claude_session_kind(other), "cli")


if __name__ == "__main__":
    unittest.main()
