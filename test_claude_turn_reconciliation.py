import unittest
from unittest.mock import patch
import migrate_sessions as m
from test_usage_reconciliation import UsageReconciliationTests as F

class ClaudeTurnTests(unittest.TestCase):
    setUp=F.setUp
    tearDown=F.tearDown
    add_session=F.add_session
    add_message=F.add_message

    def prepare(self, manual=False, ambiguous=False, overlap=False, mismatch=False):
        self.add_session('owner','openclaw_sqlite:main:test.db')
        self.add_session('sdk','claude:manual.jsonl' if manual else 'claude:sdk.jsonl')
        def add(mid,sid,ts,n):
            self.add_message(message_id=mid,session_id=sid,timestamp='2026-10-19T10:00:'+ts+'Z',model_raw='anthropic/claude-opus-5-5' if sid=='owner' else 'anthropic-c/claude-opus-5-5',source='openclaw_sqlite:main' if sid=='owner' else 'claude_cli',input_tokens=2*n,output_tokens=10*n,cache_read=100*n,cache_write=5*n,total_tokens=117*n)
        add('a','sdk','01.000',1);add('b','sdk','10.000',1);add('turn','owner','10.100',2)
        if ambiguous:
            self.add_session('sdk2','claude:sdk2.jsonl')
            add('a2','sdk2','01.000',1);add('b2','sdk2','10.000',1)
        if overlap:add('turn2','owner','10.200',2)
        if mismatch:self.conn.execute("update messages set cache_write=11 where id='turn'")

    def reconcile(self):
        with patch.object(m,'read_claude_entrypoint',side_effect=lambda p:'cli' if p.name=='manual.jsonl' else 'sdk-cli'),patch.object(m,'collect_openclaw_sqlite_sources',return_value=[]),patch.object(m,'collect_openclaw_codex_thread_routes',return_value={}),patch.object(m,'collect_scheduled_owner_keys',return_value={}):
            m.reconcile_usage_duplicates(self.conn)

    def test_exact_turn_and_repeat(self):
        self.prepare()
        for _ in range(2):
            self.reconcile()
            self.assertEqual(self.conn.execute("select count(*),sum(total_tokens) from effective_messages where session_id='owner' and is_excluded=0").fetchone(),(2,234))
            self.assertEqual(self.conn.execute("select excluded_reason from messages where id='turn'").fetchone()[0],m.CLAUDE_TURN_REASON)
            self.assertEqual(self.conn.execute("select count(*) from effective_messages where session_id='sdk' and is_excluded=0").fetchone()[0],0)

    def test_existing_runtime_attribution_does_not_double_count_turn(self):
        self.prepare()
        for mid,sid,stamp,raw,source in [
            ('prior','sdk','2026-10-19T09:59:00.000Z','anthropic-c/claude-opus-5-5','claude_cli'),
            ('prior-wrapper','owner','2026-10-19T09:59:00.100Z','anthropic/claude-opus-5-5','openclaw_sqlite:main')]:
            self.add_message(message_id=mid,session_id=sid,timestamp=stamp,model_raw=raw,source=source,
                input_tokens=2,output_tokens=10,cache_read=100,cache_write=5,total_tokens=117)
        self.reconcile()
        self.assertEqual(self.conn.execute("select count(*),sum(total_tokens) from effective_messages where session_id='owner' and is_excluded=0").fetchone(),(3,351))

    def test_no_unproven_matches(self):
        for flag in ['manual','ambiguous','overlap','mismatch']:
            with self.subTest(flag=flag):
                if flag!='manual':self.tearDown();self.setUp()
                self.prepare(**{flag:True});self.reconcile()
                self.assertEqual(self.conn.execute("select sum(is_excluded) from messages where id like 'turn%'").fetchone()[0],0)

if __name__=='__main__':unittest.main()
