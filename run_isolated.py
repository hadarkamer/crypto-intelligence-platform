"""Run external DOGE tests against unchanged application checkouts, offline."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile

HERE = Path(__file__).resolve().parent
RUNTIME_MODULES = [
    'hl_testnet_runtime.' + name for name in (
        'test_doge_entry_refusals', 'test_doge_exit_attempt_caps',
        'test_doge_partial_dispatch', 'test_doge_partial_execution',
        'test_doge_partial_live_integration')
]
CHILD = r'''
import json, pathlib, sys, unittest
import sitecustomize
config = json.loads(sys.argv[1])
assert pathlib.Path(sitecustomize.__file__).resolve() == pathlib.Path(config['guard'])
assert sitecustomize.GUARD_ACTIVE is True
if config['suite'] == 'receiver':
    import hl_testnet_runtime
    hl_testnet_runtime.__path__.append(config['tests'])
else:
    sys.path.insert(0, config['tests'])
suite = unittest.defaultTestLoader.loadTestsFromNames(config['modules'])
result = unittest.TextTestRunner(verbosity=2).run(suite)
print('ISOLATED_RESULT ' + json.dumps(dict(
    suite=config['suite'], tests_run=result.testsRun,
    passed=result.testsRun-len(result.skipped)-len(result.failures)-len(result.errors),
    skipped=len(result.skipped), failures=len(result.failures), errors=len(result.errors),
    skip_reasons=sorted(set(reason for _, reason in result.skipped)))))
raise SystemExit(not result.wasSuccessful())
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--receiver', type=Path, required=True)
    parser.add_argument('--producer', type=Path, required=True)
    parser.add_argument('--python', type=Path, required=True)
    args = parser.parse_args()
    receiver, producer = args.receiver.resolve(), args.producer.resolve()
    # Keep a venv's executable path: resolving its symlink would bypass the venv.
    python = args.python.absolute()
    if not python.is_file():
        parser.error('--python must identify an existing Python executable')
    for root, expected in ((receiver, 'hl_testnet_runtime/experimental_execution_runtime.py'),
                           (producer, 'approved_alert_producer.py')):
        if not (root / expected).is_file():
            parser.error('Expected application checkout not found')
    with tempfile.TemporaryDirectory(prefix='doge-isolated-') as temporary:
        network_log = Path(temporary) / 'blocked-network.jsonl'
        environment = {
            'PATH': str(python.parent) + os.pathsep + os.defpath,
            'LANG': 'C.UTF-8', 'TZ': 'UTC', 'PYTHONUNBUFFERED': '1',
            'PYTHONDONTWRITEBYTECODE': '1', 'PYTHONHASHSEED': '0',
            'DOGE_ISOLATED_NETWORK_LOG': str(network_log),
        }
        results = []
        for name, root, other, test_dir, modules in (
            ('receiver', receiver, producer, HERE / 'receiver/hl_testnet_runtime', RUNTIME_MODULES),
            ('producer', producer, receiver, HERE / 'producer', ['approved_doge_partial_selftest']),
        ):
            environment['PYTHONPATH'] = os.pathsep.join(map(str, (
                HERE / 'guard', HERE / 'shared', root, other)))
            config = dict(suite=name, tests=str(test_dir), modules=modules,
                          guard=str(HERE / 'guard/sitecustomize.py'))
            result = subprocess.run([str(python), '-c', CHILD, json.dumps(config)],
                                    cwd=root, env=environment, timeout=300)
            results.append(result.returncode)
        attempts = network_log.read_text().splitlines() if network_log.exists() else []
        print('ISOLATED_NETWORK ' + json.dumps(dict(
            blocked_attempts=len(attempts), credentials_inherited=False,
            postgres_available=False)))
        return int(any(results) or bool(attempts))


if __name__ == '__main__':
    raise SystemExit(main())
