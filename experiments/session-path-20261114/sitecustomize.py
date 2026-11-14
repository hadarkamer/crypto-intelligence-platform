"""Test-only source boundaries in the actual collector child command."""
import os
import json
from pathlib import Path
import sys

if Path(sys.argv[0]).name == 'collection_model1_task.py':
    try:
        if os.environ.get('CG_SESSION_PATH_TEST') != 'dummy-only':
            raise RuntimeError('fixture_not_authorized')
        from check_path import install_child
        install_child()
    except BaseException as error:
        # Python otherwise continues after a sitecustomize error. Never run an
        # uninstrumented child when this explicitly isolated fixture fails.
        try:
            report = Path(os.environ['CG_SESSION_TEST_REPORT'])
            temporary = Path(os.environ['TMPDIR']).resolve()
            if report.parent.resolve() == temporary and report.name in ('browser-cycle-1.json', 'browser-cycle-2.json'):
                allowed = {'fixture_not_authorized','root_browser_forbidden','python_version_mismatch',
                    'playwright_version_mismatch','parent_secret_in_child','legacy_header_in_child',
                    'fixture_generation_invalid','fixture_report_scope_invalid','existing_html_fixture_missing'}
                code = str(error) if type(error).__name__ == 'CheckFailed' and str(error) in allowed else 'fixture_hook_setup_failed'
                kind = type(error).__name__
                payload = {'fixture_bootstrap_failed':True, 'reason_code':code,
                    'exception_kind':kind if kind in {'CheckFailed','ImportError','ModuleNotFoundError','OSError','ValueError','KeyError'} else 'other'}
                fd = os.open(report, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
                with os.fdopen(fd, 'w') as stream: json.dump(payload, stream)
        except BaseException:
            pass
        os._exit(97)
