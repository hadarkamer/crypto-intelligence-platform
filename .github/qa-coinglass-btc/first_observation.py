"""Run the pinned native pilot with passive diagnostics and retention for its first map."""
from pathlib import Path
import os
import sys

from make_retained_pilot import build

ROOT = Path(__file__).resolve().parent
SHA = 'ae032a47fcbda94ef21b53d4e56353f81504510f'


def prepare(destination):
    build(ROOT / 'dedicated_pilot.py', destination)
    lines = destination.read_text()
    start = '    from session_capture import CaptureSession\n'
    assert lines.count(start) == 1
    lines = lines.replace(start, start + '    from pilot_observation import install\n    install()\n')
    old = 'BROWSER_ENV = ('
    assert lines.count(old) == 1
    extra = ['auth_protected_proof_required','auth_attempt_not_reserved','auth_result_invalid',
             'state_store_failed','state_transaction_not_idle','state_expired','state_missing',
             'state_not_ready','state_invalid','state_too_large','state_lock_required',
             'session_configuration_invalid','session_cookie_attributes_unsupported','image_evidence_invalid',
             'screenshot_identity_mismatch','pilot_deadline','pilot_local_file_missing','pilot_local_permission_denied',
             'pilot_dependency_missing','pilot_browser_timeout','pilot_browser_closed','pilot_browser_operation',
             'pilot_database_privilege','pilot_database_operation','pilot_schema_missing','pilot_evidence_invalid']
    lines = lines.replace(old,'from bounded_pilot_diagnostics import classify\nPUBLIC_CODES.update('+repr(extra)+')\n'+old)
    for code in ('pilot_read_failed','pilot_operation_failed'):
        before = f"'reason':reason if reason in PUBLIC_CODES else '{code}'"
        assert lines.count(before) == 1
        lines = lines.replace(before, f"'reason':classify(error, reason, PUBLIC_CODES) or ('pilot_evidence_invalid' if isinstance(error, ValueError) and error.args==('pilot_evidence_invalid',) else '{code}')")
    compile(lines, str(destination), 'exec')
    destination.write_text(lines)


if __name__ == '__main__':
    from native_pilot_host import CLEAN, require_service, require_runtime
    os.umask(0o077)
    require_service(os.environ)
    if os.environ.get('RENDER_GIT_COMMIT') != SHA or sys.argv[1:] not in (['first'], ['prepare']):
        raise SystemExit('qa_pilot_scope_mismatch')
    require_runtime(ROOT)
    destination = ROOT / 'dedicated_pilot_recorded.py'
    prepare(destination)
    if sys.argv[1] == 'prepare':
        print('qa_first_ready')
        raise SystemExit(0)
    allowed = CLEAN | {'DATABASE_URL','COINGLASS_SESSION_KEY','COINGLASS_LOGIN_EMAIL',
                       'COINGLASS_LOGIN_PASSWORD','COINGLASS_SESSION_STORE_ENABLED','COINGLASS_MODERN_UI_ENABLED'}
    environment = {key:os.environ[key] for key in allowed if key in os.environ}
    os.execve(sys.executable,[sys.executable,'-B',str(destination),'first'],environment)
