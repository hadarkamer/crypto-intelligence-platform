"""Optional manual QA entry: one original pilot, fixed failure taxonomy, no second request."""
from __future__ import annotations
import os
from pathlib import Path
import sys

SHA = 'ae032a47fcbda94ef21b53d4e56353f81504510f'
PILOT_SHA256 = 'a8eeecb35ce957edd8cd9c40a61dc9df8dbf48e9f3c018091536315f169bb215'


def classify(exc, previous, approved):
    if previous in approved:
        return previous
    types = {
        ('builtins','TimeoutError'):'pilot_deadline',
        ('asyncio.exceptions','TimeoutError'):'pilot_deadline',
        ('builtins','FileNotFoundError'):'pilot_local_file_missing',
        ('builtins','PermissionError'):'pilot_local_permission_denied',
        ('builtins','ModuleNotFoundError'):'pilot_dependency_missing',
        ('builtins','ImportError'):'pilot_dependency_missing',
        ('playwright._impl._errors','TimeoutError'):'pilot_browser_timeout',
        ('playwright._impl._errors','TargetClosedError'):'pilot_browser_closed',
        ('playwright._impl._errors','Error'):'pilot_browser_operation',
        ('psycopg','OperationalError'):'pilot_database_operation',
        ('psycopg.errors','InsufficientPrivilege'):'pilot_database_privilege',
        ('psycopg.errors','UndefinedTable'):'pilot_schema_missing',
    }
    for cls in type(exc).__mro__:
        code=types.get((cls.__module__,cls.__name__))
        if code:
            return code
    return None


def run_once(argv):
    from hashlib import sha256
    root=Path.cwd()/'standalone_collector'
    sys.path.insert(0,str(root))
    from native_pilot_host import require_service,require_runtime
    require_service(os.environ)
    if os.environ.get('RENDER_GIT_COMMIT') != SHA:
        raise SystemExit('qa_commit_mismatch')
    require_runtime(root)
    original=(root/'dedicated_pilot.py').read_bytes()
    if sha256(original).hexdigest() != PILOT_SHA256:
        raise SystemExit('qa_pilot_digest_mismatch')
    allowed={'first','reuse','fresh','preflight'}
    # Internal children use inherited private IPC and one fixed command, never launch new parent work.
    internal=(len(argv)==3 and argv[0]=='_read' and argv[1] in allowed-{'preflight'}
              and os.environ.get('COINGLASS_SESSION_CONTROL_FD'))
    if len(argv)!=1 or argv[0] not in allowed:
        if not internal:raise SystemExit('qa_manual_stage_invalid')
    # Use the canonical CLI, type names observed in the limited except clauses only.
    # Nothing from vendor errors, HTTP or DOM is emitted. Gate all dynamic patches
    # against exact, known source tokens and SHA. Generated browser/auth code is unchanged.
    text=original.decode()
    def one(old,new):
        nonlocal text
        if text.count(old)!=1:raise SystemExit('qa_error_profile_mismatch')
        text=text.replace(old,new)
    labels=['auth_protected_proof_required','auth_attempt_not_reserved','auth_result_invalid',
            'session_conflict','state_store_failed','state_transaction_not_idle','state_expired',
            'state_missing','state_not_ready','state_invalid','state_too_large','state_lock_required',
            'session_configuration_invalid','session_cookie_attributes_unsupported','image_evidence_invalid',
            'screenshot_identity_mismatch','pilot_deadline','pilot_local_file_missing',
            'pilot_local_permission_denied','pilot_dependency_missing','pilot_browser_timeout',
            'pilot_browser_closed','pilot_browser_operation','pilot_database_privilege','pilot_database_operation',
            'pilot_schema_missing','pilot_child_signaled','pilot_child_no_receipt','pilot_child_invalid_receipt']
    one('BROWSER_ENV = (','PUBLIC_CODES.update('+repr(labels)+')\nBROWSER_ENV = (')
    one("'reason':reason if reason in PUBLIC_CODES else 'pilot_read_failed'",
        "'reason':safe_classify(error, reason, PUBLIC_CODES) or 'pilot_read_failed'")
    one("'reason':reason if reason in PUBLIC_CODES else 'pilot_operation_failed'",
        "'reason':safe_classify(error, reason, PUBLIC_CODES) or 'pilot_operation_failed'")
    one("result = {'ok':False,'reason':'pilot_read_failed'}",
        "result = {'ok':False,'reason':'pilot_child_signaled' if process.returncode < 0 else 'pilot_child_no_receipt'}")
    one("if output.exists() and output.stat().st_size <= 1024:",
        "if output.exists() and output.stat().st_size <= 1024:\n                result['reason']='pilot_child_invalid_receipt'")
    # The preflight must continue invoking the original no-source/inert _browser.
    # Route only the already-selected single child to this local adapter for allowlisted errors.
    one("env.pop('OPENAI_API_KEY',None)", "env.pop('OPENAI_API_KEY',None)\n            env.update({k:os.environ[k] for k in ('RENDER','RENDER_SERVICE_NAME','RENDER_GIT_COMMIT','RENDER_GIT_BRANCH','IS_PULL_REQUEST')})")
    one("sys.executable,str(Path(__file__).resolve()),'_read',mode,directory,",
        "sys.executable,qa_diag_path,'_read',mode,directory,")
    sys.argv=[str(root/'dedicated_pilot.py'),*argv]
    scope={'__name__':'__main__','__file__':str(root/'dedicated_pilot.py'),
           'safe_classify':classify,'qa_diag_path':str(Path(__file__).resolve())}
    exec(compile(text,'dedicated_pilot.py','exec'),scope)


if __name__=='__main__':
    run_once(sys.argv[1:])
