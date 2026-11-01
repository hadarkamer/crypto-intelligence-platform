"""Offline checks of finite live-check scope, stopping, evidence and isolation."""
import base64
from contextlib import redirect_stdout
import gzip
import hashlib
import io
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import source_acceptance as check


class SourceAcceptanceTests(unittest.TestCase):
    def exercise(self,fail=None,cleanup_fail=False):
        calls=[];output=io.StringIO()
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);(root/'module.py').write_text('fixture')
            digest=hashlib.sha256(b'fixture').hexdigest()
            manifest=root/'manifest.json';manifest.write_text(json.dumps({v:{'module.py':digest} for v in ('base','candidate')}))
            resources=SimpleNamespace(cgroup_snapshot=lambda:{},proc_stats=lambda:{},
                clean_group=lambda p:None,clean_tracked_children=lambda tracked:None)
            if cleanup_fail:resources.clean_group=lambda p:(_ for _ in ()).throw(RuntimeError('cleanup'))
            diagnostic=SimpleNamespace(SERVICE='test-service',SESSION=('SOURCE_SESSION',),resources=resources,
                require_limits=lambda group:None,
                safe_env=lambda env,model:{'SOURCE_SESSION':'synthetic','COLLECTION_HEATMAP_MODEL':str(model)})
            def start(command,**kwargs):
                calls.append((command,kwargs))
                self.assertNotIn('DATABASE_URL',kwargs['env']);self.assertNotIn('OPENAI_API_KEY',kwargs['env'])
                out=Path(command[command.index('--output')+1])
                failed=len(calls)==fail
                (out/'report.json').write_text(json.dumps({'status':'failed' if failed else 'captured'}))
                (out/'hover.json').write_text('{"rows":[]}')
                (out/'session-secret.txt').write_text('must not export')
                return SimpleNamespace(pid=123,returncode=int(failed),poll=lambda:int(failed))
            argv=['check','--diagnostic','unused','--source-check','unused','--base-runtime',str(root),
                  '--candidate-runtime',str(root),'--manifest',str(manifest)]
            with patch.object(sys,'argv',argv),patch.object(check,'load',return_value=diagnostic),\
                 patch.object(check.subprocess,'Popen',side_effect=start),patch.dict(check.os.environ,
                 {'CG3_EARLY_SOURCE_ONE_OFF':'1','RENDER_GIT_COMMIT':check.BASE,'RENDER_SERVICE_ID':'test-service'}),\
                 redirect_stdout(output):
                code=check.main()
        records=[json.loads(line.removeprefix('CG3EARLY ')) for line in output.getvalue().splitlines()]
        raw=gzip.decompress(base64.b64decode(''.join(r['data'] for r in records if 'data' in r)))
        self.assertEqual(hashlib.sha256(raw).hexdigest(),records[-1]['sha256'])
        summary=json.loads(raw)
        self.assertTrue(all('session-secret.txt' not in x['artifacts'] for x in summary['runs']))
        return code,summary,calls

    def test_exactly_five_sequential_source_only_attempts(self):
        code,result,calls=self.exercise()
        self.assertEqual(code,0);self.assertTrue(result['completed']);self.assertEqual(len(calls),5)
        self.assertEqual([(x['variant'],x['model'],x['timeframe']) for x in result['runs']],list(check.PAIRS))
        self.assertEqual(result['ai_calls'],0);self.assertEqual(result['database_writes'],0)

    def test_first_failure_retains_evidence_and_stops_without_retry(self):
        code,result,calls=self.exercise(fail=2)
        self.assertEqual(code,1);self.assertEqual(len(calls),2)
        self.assertIn('hover.json',result['runs'][-1]['artifacts'])

    def test_cleanup_failure_retains_result_and_stops(self):
        code,result,calls=self.exercise(cleanup_fail=True)
        self.assertEqual(code,1);self.assertEqual(len(calls),1)
        self.assertEqual(result['runs'][0]['cleanup_errors'],['RuntimeError'])
        self.assertEqual(result['runs'][0]['report']['status'],'captured')

if __name__=='__main__':unittest.main()
