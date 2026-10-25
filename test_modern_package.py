"""Portable package checks. Fixtures contain no local usage data."""
import os
import subprocess
import sys
import json
import sqlite3
import tempfile
import threading
import unittest
from compression import zstd
from pathlib import Path
from unittest.mock import patch
from urllib.request import urlopen

import migrate_sessions as m
import server


class ModernPackageTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.config = dict(db=self.root/'tracker.db', dashboard=server.DEFAULT_DASHBOARD, ref_dir=server.DEFAULT_REF_DIR,
                           sessions_dir=self.root/'sessions', openclaw_agents_dir=self.root/'agents',
                           codex_sessions_dir=self.root/'codex', claude_projects_dir=self.root/'claude',
                           kimi_code_dir=self.root/'kimi')

    def source(self):
        path = self.config['openclaw_agents_dir']/'research'/'agent'/'openclaw-agent.sqlite'
        path.parent.mkdir(parents=True)
        db = sqlite3.connect(path)
        db.executescript('''CREATE TABLE session_nodes (
            session_key, current_session_id, entry_json, created_at, label, display_name);
            CREATE TABLE transcript_events (session_id, seq, event_json, created_at, event_zstd);
            CREATE TABLE session_transcript_archives (session_id, encoding, archive_blob);''')
        return path, db

    def serve(self):
        httpd = server.TrackerHTTPServer(('127.0.0.1', 0), server.TrackerHandler, self.config)
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(httpd.server_close)
        self.addCleanup(thread.join, 2)
        self.addCleanup(httpd.shutdown)
        return 'http://127.0.0.1:' + str(httpd.server_address[1])

    def test_compressed_event_ingest_and_archive_viewer(self):
        path, db = self.source()
        sid = '11111111-2222-4333-8444-555555555555'
        db.execute('INSERT INTO session_nodes VALUES (?,?,?,?,?,?)',
                   ('agent:research:dashboard:test', sid, '{}', 0, 'fixture', 'fixture'))
        event = dict(id='compressed-call', type='message', timestamp='2026-10-25T01:00:00Z',
                     message=dict(role='assistant',provider='openai',model='fixture',
                                  usage=dict(input=10,output=20,cacheRead=30,totalTokens=60)))
        raw = json.dumps(event)
        db.execute('INSERT INTO transcript_events VALUES (?,?,?,?,?)',
                   (sid,1,None,0,zstd.compress(raw.encode())))
        db.execute('INSERT INTO session_transcript_archives VALUES (?,?,?)',
                   ('archived', 'zstd', zstd.compress((raw+'\n').encode())))
        db.commit(); db.close()
        tracker = sqlite3.connect(self.config['db'])
        m.init_db(tracker)
        with patch.object(m,'OPENCLAW_AGENTS_DIR',self.config['openclaw_agents_dir']):
            m.ingest_openclaw_sqlite(tracker)
            m.ingest_openclaw_sqlite(tracker)
        self.assertEqual(tracker.execute('SELECT count(*),sum(total_tokens) FROM messages').fetchone(),(1,60))
        tracker.close()
        base = self.serve()
        for session in [sid,'archived']:
            with urlopen(base+'/api/cost-v4/transcript/'+session) as response:
                self.assertEqual(json.loads(response.read())['id'],'compressed-call')
        # Source remains compressed and unchanged.
        source = sqlite3.connect(path)
        self.assertIsNone(source.execute('SELECT event_json FROM transcript_events').fetchone()[0])
        source.close()

    def test_grouped_api_and_newest_runtime_log(self):
        tracker = sqlite3.connect(self.config['db']);m.init_db(tracker)
        for sid in ['owner','old','new']:
            filename = 'openclaw_sqlite:research:source' if sid=='owner' else 'claude:'+sid+'.jsonl'
            tracker.execute("INSERT INTO sessions(session_id,filename,session_type) VALUES (?,?,'gui')",(sid,filename))
        self.config['claude_projects_dir'].mkdir()
        for sid in ['old','new']:
            (self.config['claude_projects_dir']/(sid+'.jsonl')).write_text('{}\n')
        for mid,sid,ts in [('a','old','2026-10-25T01:00:00Z'),('b','old','2026-10-25T01:01:00Z'),('c','new','2026-10-25T02:00:00Z')]:
            m.insert_message_row(tracker.cursor(),mid,sid,None,ts,'anthropic/fixture',
                                 dict(input_tokens=10,output_tokens=20,cache_read=30,total_tokens=60),ingest_source='claude_cli')
            m.add_message_attribution(tracker.cursor(),mid,'owner','anthropic/fixture','openclaw_claude_runtime_usage')
        m.recompute_segments(tracker,'owner');tracker.commit();tracker.close()
        base=self.serve()
        with urlopen(base+'/api/cost-v4/sessions') as response:
            rows=json.load(response)['sessions']
        self.assertEqual(len(rows),1)
        self.assertEqual((rows[0]['segment_index'],rows[0]['msg_count'],rows[0]['total_tokens']),(-1,3,180))
        query='?session_id=owner&model_raw=anthropic/fixture&segment_index=-1'
        with urlopen(base+'/api/cost-v4/log-sources'+query) as response:
            sources=json.load(response)['sources']
        self.assertEqual([r['session_id'] for r in sources],['new','old'])
        with urlopen(base+'/api/cost-v4/messages'+query+'&start=2026-10-25T02:00:00Z') as response:
            self.assertEqual([r['id'] for r in json.load(response)['messages']],['c'])
        self.assertEqual(m.resolve_tracker_runtime_path('claude:../secret',self.config),None)
        self.assertEqual(m.resolve_tracker_runtime_path('codex_cli:codex-home/research/test.jsonl',self.config),
                         str(self.config['openclaw_agents_dir']/'research/agent/codex-home/sessions/test.jsonl'))

    def test_clean_install_and_incremental_cli(self):
        _, db = self.source()
        sid = '11111111-2222-4333-8444-555555555555'
        db.execute('INSERT INTO session_nodes VALUES (?,?,?,?,?,?)',
                   ('agent:research:dashboard:test',sid,'{}',0,'fixture','fixture'))
        event = dict(id='fresh-call',type='message',timestamp='2026-10-25T01:00:00Z',
                     message=dict(role='assistant',provider='openai',model='fixture',
                                  usage=dict(input=10,output=20,totalTokens=30)))
        db.execute('INSERT INTO transcript_events VALUES (?,?,?,?,?)',
                   (sid,1,None,0,zstd.compress(json.dumps(event).encode())))
        db.commit();db.close()
        env = dict(os.environ, HOME=str(self.root), PATH=str(self.root/'no-executables'))
        flags = ['--db',str(self.config['db']),
                 '--openclaw-agents-dir',str(self.config['openclaw_agents_dir']),
                 '--sessions-dir',str(self.config['sessions_dir']),
                 '--codex-sessions-dir',str(self.config['codex_sessions_dir']),
                 '--claude-projects-dir',str(self.config['claude_projects_dir']),
                 '--kimi-code-dir',str(self.config['kimi_code_dir'])]
        script=Path(m.__file__).resolve()
        for mode in ['--full','--incremental']:
            result=subprocess.run([sys.executable,str(script),mode,*flags],env=env,
                                  cwd=self.root,capture_output=True,text=True,timeout=20)
            self.assertEqual(result.returncode,0,result.stdout+result.stderr)
            tracker=sqlite3.connect(self.config['db'])
            self.assertEqual(tracker.execute('SELECT count(*),sum(total_tokens) FROM effective_messages WHERE is_excluded=0').fetchone(),(1,30))
            tracker.close()


if __name__ == '__main__':
    unittest.main()
