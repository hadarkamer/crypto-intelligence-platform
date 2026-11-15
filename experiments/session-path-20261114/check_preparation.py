#!/usr/bin/env python3
"""Focused local preflight of the actual preparation block; no source/browser/DB."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tempfile
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--reference-package', type=Path, required=True)
    parser.add_argument('--reference-runtime', type=Path, required=True)
    args = parser.parse_args()
    package = args.reference_package.resolve(strict=True)
    runtime = args.reference_runtime.resolve(strict=True)
    source = (HERE/'run.sh').read_text()
    block = source.split('STAGE=runtime_generation\n', 1)[1].split('\nSTAGE=postgres_start', 1)[0]
    assert 'verify_runtime(prepare.RUNTIME)' not in block
    assert 'RENDER_GIT_COMMIT' not in block
    guard = block.split('for name, expected in observation_supervisor.RUNTIME_HASHES.items():', 1)[1]
    guard = 'for name, expected in observation_supervisor.RUNTIME_HASHES.items():' + guard.split("print('SESSION_PATH_BUILD", 1)[0]
    reference_hashes = {name: hashlib.sha256((package/name).read_bytes()).hexdigest()
        for name in ('approved_entry.py', 'observation_supervisor.py')}
    assert all(hashlib.sha256((HERE/'wrapper'/name).read_bytes()).hexdigest() == digest
        for name,digest in reference_hashes.items())
    report = {'preparation_without_worker_environment': False, 'missing_file_rejected': False,
        'changed_file_rejected': False, 'missing_worker_commit_rejected': False,
        'wrong_worker_commit_rejected': False, 'worker_guards_unchanged': False,
        'source_requests': 0, 'browser_launches': 0, 'database_connections': 0,
        'python': sys.version.split()[0]}
    with tempfile.TemporaryDirectory(prefix='cg-preparation-check-') as tmp:
        env = {'PATH': str(Path(sys.executable).parent)+':'+os.defpath,
            'HOME': tmp, 'LANG': 'C.UTF-8', 'PYTHONUTF8': '1', 'PYTHONDONTWRITEBYTECODE': '1'}
        assert 'RENDER_GIT_COMMIT' not in env
        command = 'set -euo pipefail\nROOT='+shlex.quote(str(ROOT))+'\n'+block
        result = subprocess.run(['bash','-c',command],env=env,capture_output=True,text=True,timeout=20)
        assert result.returncode == 0, result.stderr
        assert 'SESSION_PATH_BUILD actual_adapters=true' in result.stdout
        report['preparation_without_worker_environment'] = True
        guard_code = ('import hashlib,sys\nfrom pathlib import Path\nfrom types import SimpleNamespace\n'
            +'sys.path.insert(0,'+repr(str(HERE/'wrapper'))+')\nimport observation_supervisor\n'
            +'prepare=SimpleNamespace(RUNTIME=Path('+repr(str(ROOT/'standalone_collector/runtime'))+'))\n'+guard)
        target = ROOT/'standalone_collector/runtime/model1_execution.py'
        original = target.read_bytes()
        try:
            target.unlink()
            failed = subprocess.run([sys.executable,'-B','-c',guard_code],env=env,capture_output=True,text=True,timeout=5)
            assert failed.returncode != 0 and 'FileNotFoundError' in failed.stderr
            report['missing_file_rejected'] = True
            target.write_bytes(original+b'\n# deliberately corrupted disposable generated fixture\n')
            failed = subprocess.run([sys.executable,'-B','-c',guard_code],env=env,capture_output=True,text=True,timeout=5)
            assert failed.returncode != 0 and 'runtime_hash_mismatch' in failed.stderr
            report['changed_file_rejected'] = True
        finally:
            target.write_bytes(original)

        spec = importlib.util.spec_from_file_location('reuse_entry_negative_tests', package/'test_reuse_preflight.py')
        tests = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(tests)
        tests.BASELINE = runtime.parent
        original_copy = shutil.copyfile
        def copy_with_private_dependency(src, dst, *pos, **kwargs):
            value = original_copy(src, dst, *pos, **kwargs)
            if Path(src).resolve() == package/'session_capture.py':
                for name in ('dependencies','vendor'):
                    shutil.copytree(package/name, Path(dst).parent/name)
            return value
        test = tests.ReuseEntryTests()
        with patch.object(tests.shutil, 'copyfile', side_effect=copy_with_private_dependency):
            for commit, label in ((None,'missing_worker_commit_rejected'),('0'*40,'wrong_worker_commit_rejected')):
                receipt, facts = test.run_entry(commit=commit)
                assert receipt['status'] == 'blocked' and receipt['reason'] == 'worker_preflight_failed'
                assert receipt['crypto_dependency']['private_import_verified'] is True
                assert receipt['crypto_dependency']['native_aes_gcm_verified'] is True
                assert receipt['source_observer_launches'] == 0 and facts['keeper_launches'] == 0
                assert facts['network_calls'] == facts['real_database_connections'] == facts['browser_launches'] == 0
                report[label] = True
        assert all(hashlib.sha256((package/name).read_bytes()).hexdigest() == digest for name,digest in reference_hashes.items())
        report['worker_guards_unchanged'] = True
    report['temporary_fixture_removed'] = not Path(tmp).exists()
    print('SESSION_PREPARATION_CHECK '+json.dumps(report, sort_keys=True), flush=True)


if __name__ == '__main__':
    main()
