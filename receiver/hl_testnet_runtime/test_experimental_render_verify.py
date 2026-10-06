"""Pinned Render wrapper contracts; no Render, network or PostgreSQL calls."""
from contextlib import redirect_stdout
from copy import deepcopy
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from . import experimental_render_verify as render
from . import experimental_isolated_verify as isolated


class RenderVerifierTests(unittest.TestCase):
    def setUp(self):
        temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup)
        self.root=Path(temp.name)
        for name in ('receiver','producer'):
            folder=self.root/name;folder.mkdir()
            (folder/'candidate.py').write_text('value = 1\n')
            (folder/'requirements.txt').write_text('dependency==1\n')
        (self.root/'receiver'/'hl_testnet_runtime').mkdir()
        (self.root/'receiver'/'hl_testnet_runtime'/'sol_g65_conditional_stop.py').write_text('candidate = True\n')
        (self.root/'producer'/'sol_g65_experimental_store.py').write_text('producer = True\n')
        self.write_manifest()
        self.verify_patch=patch.object(render.isolated,'verify',side_effect=lambda *args,**kwargs:self.good_report())
        self.verify=self.verify_patch.start();self.addCleanup(self.verify_patch.stop)
        self.parity_patch=patch.object(render,'run_parity',return_value=self.parity_report())
        self.parity=self.parity_patch.start();self.addCleanup(self.parity_patch.stop)

    def write_manifest(self,**metadata):
        payload=dict(schema=render.BUNDLE_SCHEMA,candidate_files={k:isolated.candidate_files(self.root/k) for k in ('receiver','producer')},**metadata)
        raw=(json.dumps(payload,sort_keys=True,indent=2)+'\n').encode()
        (self.root/render.MANIFEST_NAME).write_bytes(raw)
        self.pin=hashlib.sha256(raw).hexdigest()
        self.manifest=render.validate_bundle(self.root,self.pin)

    def good_report(self):
        return dict(status='PASSED',postgresql='FRESH_LOOPBACK_POSTGRES18',producer_postgresql='FRESH_LOOPBACK_PRODUCER_DATABASE',
            postgresql_cleanup='STOPPED',snapshot_unchanged_after_tests=True,workspace_unchanged_since_snapshot=True,
            candidates={name:dict(files=self.manifest['candidate_files'][name],sha256=self.manifest['candidate_sha256'][name]) for name in ('receiver','producer')},
            stages=[dict(suite=name,tests_run=3,passed=3,skipped=0,failures=0,errors=0,unexpected_successes=0,
                network_attempts_blocked=0,exit_code=0,successful=True,postgresql_configured=True) for name in ('runtime','producer')])

    def parity_report(self):
        return dict(comparisons=4000,bars_per_comparison=4,network_attempts_blocked=0,
            producer_file_sha256=hashlib.sha256((self.root/'producer'/'sol_g65_experimental_store.py').read_bytes()).hexdigest(),
            candidate_sha256=hashlib.sha256((self.root/'receiver'/'hl_testnet_runtime'/'sol_g65_conditional_stop.py').read_bytes()).hexdigest(),
            producer_decision_functions_sha256='e'*64)

    def test_complete_bundle_zero_skip_and_verified_postgres_shutdown_pass(self):
        result=render.run_bundle(self.root,self.pin)
        self.assertEqual(result['status'],'PASSED')
        self.assertEqual(result['manifest_sha256'],self.pin)
        self.assertTrue(self.verify.call_args.kwargs['postgres'])
        self.assertEqual(result['stages']['runtime']['passed'],3)
        public=list((self.root/'public').iterdir())
        self.assertEqual([p.name for p in public],['index.html'])
        self.assertEqual(public[0].read_text(),render.PUBLIC['PASSED'])
        self.assertNotIn(self.pin,public[0].read_text())
        output=self.verify.call_args.args[2]
        self.assertFalse(output.parent.exists())

    def test_unpinned_modified_missing_and_extra_candidate_refuse_before_verify(self):
        for pin in (None,'','x'*64,'a'*64):
            with self.subTest(pin=pin):
                result=render.run_bundle(self.root,pin)
                self.assertEqual(result['status'],'FAILED')
        self.verify.assert_not_called()
        (self.root/'receiver'/'candidate.py').write_text('changed = 2\n')
        self.assertEqual(render.run_bundle(self.root,self.pin)['failure_code'],'BUNDLE_FILES_CHANGED')
        self.verify.assert_not_called()
        self.write_manifest();(self.root/'receiver'/'unexpected.py').write_text('extra = 3\n')
        self.assertEqual(render.run_bundle(self.root,self.pin)['failure_code'],'BUNDLE_FILES_CHANGED')
        self.verify.assert_not_called()

    def test_symlink_source_and_manifest_refused(self):
        source=self.root/'receiver'/'candidate.py';source.unlink();source.symlink_to(self.root/'producer'/'candidate.py')
        self.assertEqual(render.run_bundle(self.root,self.pin)['failure_code'],'BUNDLE_SYMLINK_REFUSED')
        source.unlink();source.write_text('value = 1\n');self.write_manifest()
        manifest=self.root/render.MANIFEST_NAME;target=self.root/'copied.json';target.write_bytes(manifest.read_bytes());manifest.unlink();manifest.symlink_to(target)
        self.assertEqual(render.run_bundle(self.root,self.pin)['failure_code'],'BUNDLE_MANIFEST_INVALID')
        self.verify.assert_not_called()

    def test_duplicate_manifest_json_key_refused(self):
        raw=b'{"schema":"experimental_render_bundle_v1","schema":"experimental_render_bundle_v1"}'
        (self.root/render.MANIFEST_NAME).write_bytes(raw)
        pin=hashlib.sha256(raw).hexdigest()
        self.assertEqual(render.run_bundle(self.root,pin)['failure_code'],'BUNDLE_MANIFEST_INVALID')
        self.verify.assert_not_called()

    def test_skips_failures_partial_and_missing_postgres_cleanup_never_pass(self):
        changes=[('status','PARTIAL_POSTGRESQL_NOT_VERIFIED'),('postgresql_cleanup',None),
            ('postgresql_cleanup','FAILED'),('producer_postgresql','NOT_CONFIGURED_BY_THIS_RUNNER'),
            ('snapshot_unchanged_after_tests',False),('workspace_unchanged_since_snapshot',False)]
        for field,value in changes:
            report=self.good_report();report[field]=value
            self.verify.side_effect=lambda *a,_report=report,**k:deepcopy(_report)
            with self.subTest(field=field,value=value):self.assertEqual(render.run_bundle(self.root,self.pin)['status'],'FAILED')
        for field in ('skipped','errors','failures','unexpected_successes','network_attempts_blocked'):
            report=self.good_report();report['stages'][0][field]=1
            self.verify.side_effect=lambda *a,_report=report,**k:deepcopy(_report)
            with self.subTest(field=field):self.assertEqual(render.run_bundle(self.root,self.pin)['failure_code'],'VERIFICATION_SKIPS_OR_FAILURES')
        self.parity.assert_not_called()

    def test_boolean_or_inconsistent_counts_cannot_report_pass(self):
        report=self.good_report();report['stages'][0]['tests_run']=True
        with self.assertRaisesRegex(render.RenderVerificationError,'COUNTS_INVALID'):
            render.checked_verification(report,self.manifest)
        report=self.good_report();report['stages'][0]['passed']=2
        with self.assertRaisesRegex(render.RenderVerificationError,'SKIPS_OR_FAILURES'):
            render.checked_verification(report,self.manifest)
        report=self.good_report();report['stages']=[report['stages'][0]]*2
        with self.assertRaisesRegex(render.RenderVerificationError,'SUITES_REQUIRED'):
            render.checked_verification(report,self.manifest)

    def test_post_test_mutation_is_detected_again_before_public_success(self):
        def mutate(*args,**kwargs):
            (self.root/'producer'/'candidate.py').write_text('modified = True\n')
            return self.parity_report()
        self.parity.side_effect=mutate
        result=render.run_bundle(self.root,self.pin)
        self.assertEqual(result['failure_code'],'BUNDLE_FILES_CHANGED')
        self.assertEqual((self.root/'public'/'index.html').read_text(),render.PUBLIC['FAILED'])

    def test_mismatching_snapshot_hash_is_failure(self):
        report=self.good_report();report['candidates']['receiver']['sha256']='b'*64
        with self.assertRaisesRegex(render.RenderVerificationError,'HASHES_CHANGED'):
            render.checked_verification(report,self.manifest)

    def test_existing_public_source_or_arbitrary_page_is_not_published_or_removed(self):
        public=self.root/'public';public.mkdir();source=public/'private.py';source.write_text('secret = True\n')
        result=render.run_bundle(self.root,self.pin)
        self.assertEqual(result['failure_code'],'PUBLIC_DIRECTORY_NOT_ISOLATED')
        self.verify.assert_not_called();self.assertTrue(source.is_file())
        source.unlink();page=public/'index.html';page.write_text('existing arbitrary page')
        self.assertEqual(render.run_bundle(self.root,self.pin)['failure_code'],'PUBLIC_DIRECTORY_NOT_ISOLATED')
        self.assertEqual(page.read_text(),'existing arbitrary page')

    def test_main_prints_only_bounded_fixed_summary_and_returns_nonzero_failure(self):
        output=io.StringIO()
        self.verify.side_effect=RuntimeError('secret-account-and-infrastructure-details')
        with patch.dict(os.environ,{render.PIN_ENV:self.pin,'PRIVATE_TOKEN':'must-not-leak'}),redirect_stdout(output):
            code=render.main(['--bundle-root',str(self.root)])
        self.assertEqual(code,1);text=output.getvalue();self.assertLess(len(text.encode()),render.MAX_SUMMARY_BYTES)
        for forbidden in ('secret-account','infrastructure','must-not-leak',str(self.root)):
            self.assertNotIn(forbidden,text)
        self.assertEqual(json.loads(text)['failure_code'],'RENDER_VERIFICATION_FAILED')

    def test_failed_suite_retains_safe_bounded_diagnostics_only(self):
        report=self.good_report();report['status']='FAILED';report['stages'][0].update(failures=1,passed=2,exit_code=1)
        report['stages'][0]['failure_details']=[dict(test_id='hl_testnet_runtime.test_case.Tests.test_pg',exception_type='AssertionError',message='SECRET')]*12
        report['stages'][1]['failure_details']=[dict(test_id='/private/file.py',exception_type='sensitive-exception-details')]
        self.verify.side_effect=lambda *a,**kw:report
        result=render.run_bundle(self.root,self.pin)
        self.assertEqual(result['status'],'FAILED')
        observed=result['verification'];self.assertEqual(observed['stages']['runtime']['failures'],1)
        self.assertEqual(observed['stages']['runtime']['exit_code'],1)
        self.assertEqual(len(observed['stages']['runtime']['failure_details']),10)
        self.assertEqual(observed['stages']['producer']['failure_details'],[])
        self.assertNotIn('SECRET',json.dumps(result));self.assertNotIn('/private',json.dumps(result))
        self.assertNotIn('sensitive-exception',json.dumps(result))
        self.assertEqual(observed['postgresql_cleanup'],'STOPPED')

    def test_optional_commit_metadata_not_executed_or_emitted(self):
        self.write_manifest(base_code_commits={'receiver':'b'*40,'producer':'c'*40},description='private metadata')
        result=render.run_bundle(self.root,self.pin)
        self.assertEqual(result['status'],'PASSED');self.assertNotIn('private metadata',json.dumps(result))

    def test_parity_runner_uses_fresh_environment_and_pinned_inputs(self):
        self.parity_patch.stop()
        data=dict(schema='g65_producer_differential_v1',status='PASSED',**self.parity_report(),
            live_exchange_requests_sent=0,decision_function_executed_unchanged=True,source_files_modified=False)
        def process(command,**kwargs):
            self.assertEqual(command[0],render.sys.executable)
            self.assertIn('--child',command);self.assertEqual(command[-1],'4000')
            self.assertNotIn('PRIVATE_TOKEN',kwargs['env']);self.assertNotIn('HL_JOURNAL_CI_URL',kwargs['env'])
            self.assertEqual(set(kwargs['env']),{'PATH','LANG','TZ','PYTHONUNBUFFERED','PYTHONDONTWRITEBYTECODE','PYTHONHASHSEED','PYTHONPATH'})
            Path(kwargs['log_path']).write_text(json.dumps(data))
            return {'exit_code':0}
        with patch.dict(os.environ,{'PRIVATE_TOKEN':'must-not-leak','HL_JOURNAL_CI_URL':'invalid'}),patch.object(render.core,'run_process',side_effect=process):
            result=render.run_parity(self.root/'receiver',self.root/'producer',self.root)
        self.assertEqual(result['comparisons'],4000)
        data['producer_file_sha256']='f'*64
        with patch.object(render.core,'run_process',side_effect=process),self.assertRaisesRegex(render.RenderVerificationError,'PARITY_REPORT_INVALID'):
            render.run_parity(self.root/'receiver',self.root/'producer',self.root)


if __name__=='__main__':unittest.main()
