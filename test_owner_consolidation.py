import json
import sqlite3
import unittest
from pathlib import Path
from unittest.mock import patch
import migrate_sessions as m
import test_usage_reconciliation as fixtures

class OwnerConsolidationTests(unittest.TestCase):
    setUp = fixtures.UsageReconciliationTests.setUp
    tearDown = fixtures.UsageReconciliationTests.tearDown
    add_session = fixtures.UsageReconciliationTests.add_session
    add_message = fixtures.UsageReconciliationTests.add_message
    def test_sdk_invocation_routes_all_calls_and_keeps_unmatched_cli(self):
        for sid,filename in [('owner','openclaw_sqlite:main:test.db'),('sdk','claude:sdk.jsonl'),('manual','claude:manual.jsonl')]:
            self.add_session(sid,filename)
        def add(mid,sid,ts,raw,source,output):
            self.add_message(message_id=mid,session_id=sid,timestamp=ts,model_raw=raw,
                source=source,input_tokens=2,output_tokens=output,cache_read=62508,cache_write=1091,
                total_tokens=63601+output)
        add('first','sdk','2026-10-01T10:00:00Z','anthropic-c/claude-opus-5-5','claude_cli',200)
        add('final','sdk','2026-10-01T10:00:10Z','anthropic-c/claude-opus-5-5','claude_cli',405)
        add('wrapper','owner','2026-10-01T10:00:10.300Z','anthropic/claude-opus-5-5','openclaw_sqlite:main',8)
        add('manual','manual','2026-10-01T10:00:10.200Z','cli-claude/claude-opus-5-5','claude_cli',8)
        # Recover a stale pre-fix attribution to an explicitly hand-run call.
        self.conn.execute("UPDATE messages SET is_excluded=1,duplicate_of='manual',excluded_reason='duplicate_claude_wrapper_cli' WHERE id='wrapper'")
        with patch.object(m,'read_claude_entrypoint',side_effect=lambda p:'sdk-cli' if p.name=='sdk.jsonl' else 'cli'),patch.object(m,'collect_openclaw_sqlite_sources',return_value=[]),patch.object(m,'collect_scheduled_owner_keys',return_value={}):
            m.reconcile_usage_duplicates(self.conn)
            self.assertEqual(self.conn.execute("select count(*),sum(output_tokens) from effective_messages where session_id='owner' and is_excluded=0").fetchone(),(2,605))
            self.assertEqual(self.conn.execute("select session_id from effective_messages where id='manual'").fetchone()[0],'manual')
            m.reconcile_usage_duplicates(self.conn)
            self.assertEqual(self.conn.execute("select count(*) from effective_messages where session_id='owner' and is_excluded=0").fetchone()[0],2)

    def test_explicit_scheduled_owner_restores_archived_source(self):
        source=Path(self.tmp.name)/'source.sqlite'
        db=sqlite3.connect(source)
        db.executescript('''create table session_nodes(session_key,current_session_id,entry_json,created_at,label,display_name);
            create table session_windows(session_id,session_key,previous_session_id);
            create table session_transcript_archives(session_id,session_key);''')
        db.execute("insert into session_nodes values('agent:main:dashboard:owner','owner','{}',0,'iOS',NULL)")
        db.execute("insert into session_transcript_archives values('run','agent:main:cron:job')")
        db.commit();db.close()
        for sid in ['owner','run']:self.add_session(sid,'openclaw_sqlite:main:test.db')
        self.add_message(message_id='call',session_id='run',timestamp='2026-10-01T10:00:00Z',
            model_raw='openai/gpt-6.1-sol',source='openclaw_sqlite:main',input_tokens=10,output_tokens=20,cache_read=30,cache_write=0,total_tokens=60)
        with patch.object(m,'collect_openclaw_sqlite_sources',return_value=[{'path':source}]):
            m.reconcile_session_owners(self.conn,{'job':'agent:main:dashboard:owner'})
            m.reconcile_session_owners(self.conn,{})
        self.assertEqual(self.conn.execute("select session_id,source_session_id from effective_messages where id='call'").fetchone(),('owner','run'))
        self.assertEqual(self.conn.execute("select session_type from sessions where session_id='run'").fetchone()[0],'cron')

    def test_standalone_codex_exec_is_cli_even_in_agent_home(self):
        sid = '019e38b2-8fe6-73f3-bd01-5aa73e41a5dd'
        rollout = Path(self.tmp.name) / f'rollout-{sid}.jsonl'
        lines = [
            dict(type='session_meta', timestamp='2026-10-01T10:00:00Z',
                 payload=dict(id=sid, originator='codex_exec')),
            dict(type='turn_context', payload=dict(model='gpt-6.1-sol')),
            dict(type='event_msg', timestamp='2026-10-01T10:00:01Z',
                 payload=dict(type='token_count', info=dict(last_token_usage=dict(
                     input_tokens=100,cached_input_tokens=80,output_tokens=20,total_tokens=120))))]
        rollout.write_text('\n'.join(json.dumps(row) for row in lines)+'\n')
        with patch.object(m,'codex_app_server_root_for',return_value={'dir':Path(self.tmp.name),'agent':'main'}),patch.object(m,'collect_openclaw_codex_thread_ids',return_value=set()):
            success, count, _ = m.parse_codex_session_file(rollout,self.conn)
        self.assertTrue(success)
        self.assertEqual(count,1)
        self.assertEqual(self.conn.execute('select session_type from sessions where session_id=?',(sid,)).fetchone()[0],'cli')
        self.assertEqual(self.conn.execute('select ingest_source from messages where session_id=?',(sid,)).fetchone()[0],'codex_cli')

if __name__=='__main__':unittest.main()
