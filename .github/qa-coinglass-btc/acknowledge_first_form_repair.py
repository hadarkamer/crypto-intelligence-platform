"""Acknowledge only the finished, demonstrably unsubmitted first QA form incident."""
import base64
import json
import os
from pathlib import Path
import stat
import sys
from uuid import UUID

JOB = None
QA_SHA = 'ae032a47fcbda94ef21b53d4e56353f81504510f'

OBSERVATION = Path('/tmp/coinglass-btc-pilot-diagnostics/first-ui.json')


def require(ok, label):
    if not ok:
        raise SystemExit(label)


def check_observation(facts):
    require(facts.get('job_id') == str(JOB) and facts.get('attached') is True
            and facts.get('page_closed') is True, 'receipt_not_completed_for_first_job')
    require(facts.get('first_navigation') == {'path': 'official_login', 'http_status': 200}, 'wrong_first_navigation')
    require(facts.get('submit_reserved') is False and facts.get('form_reported_submitted') is False
            and 'before_submit_controls' in facts and facts['before_submit_controls'] is None,
            'submission_or_unknown')
    require(facts.get('form_result') == 'login_form_unavailable'
            and facts.get('stage_failure') == 'auth_form_unavailable', 'unexpected_form_outcome')
    seen = facts.get('after_form_controls') or {}
    require(seen.get('path') == 'official_login' and seen.get('email_visible') is True
            and seen.get('password_visible') is True, 'form_not_observed')
    for field in ('challenge_visible', 'code_visible','verification_prompt','security_check','request_blocked','interstitial_title'):
        require(seen.get(field) is False and all((facts.get(s) or {}).get(field) is not True
                for s in ('initial_controls','final_controls')), 'challenge_or_uncertain')


def read_receipt():
    directory = os.open(OBSERVATION.parent, os.O_NOFOLLOW | os.O_DIRECTORY | os.O_RDONLY)
    try:
        info = os.fstat(directory)
        require(info.st_uid == os.getuid() and not info.st_mode & 0o077, 'private_receipt_directory_required')
        fd = os.open(OBSERVATION.name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory)
        with os.fdopen(fd, 'rb') as infile:
            info = os.fstat(infile.fileno())
            require(stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid() and not info.st_mode & 0o077
                    and info.st_nlink == 1 and 0 < info.st_size <= 8192, 'private_receipt_required')
            check_observation(json.loads(infile.read(8193)))
    finally:
        os.close(directory)


def acknowledge(conn, store):
    from session_state import WORK_LOCK
    with conn.transaction():
        held = conn.execute('SELECT pg_try_advisory_lock(%s) AS ok', (WORK_LOCK,)).fetchone()
        require(held['ok'], 'worker_busy')
        row = conn.execute('''SELECT i.id,
                (i.phase='blocked' AND i.reason='auth_form_unavailable' AND i.attempts=1
                 AND i.owner_job IS NULL AND i.owner_revision IS NULL AND i.verified_at IS NULL
                 AND i.verified_job IS NULL AND i.superseded_by IS NULL AND i.retry_after IS NULL
                 AND s.status='unconfirmed' AND s.run_id IS NULL AND s.encrypted_payload IS NULL
                 AND s.map_retry_after IS NULL
                 AND NOT EXISTS (SELECT 1 FROM collector_private.coinglass_dedicated_auth i2
                                 WHERE i2.slot=i.slot AND i2.id<>i.id)
                 AND NOT EXISTS (SELECT 1 FROM collector_private.coinglass_dedicated_auth_jobs j2
                                 WHERE j2.incident_id=i.id AND j2.job_id<>j.job_id)) AS eligible
              FROM collector_private.coinglass_dedicated_auth_jobs j
              JOIN collector_private.coinglass_dedicated_auth i ON i.id=j.incident_id
              JOIN collector_private.coinglass_dedicated_sessions s ON s.slot=i.slot
              WHERE j.job_id=%s AND i.slot=%s''',(JOB,store.slot)).fetchone()
        require(row and row['eligible'] is True, 'db_state_changed_or_already_acknowledged')
        incident = str(row['id'])
    # Session-level WORK_LOCK remains held between committed checks and the store's own atomic transaction.
    successor = store.acknowledge_remediation(incident)
    with conn.transaction():
        confirmed = conn.execute('''SELECT 1 AS ok FROM collector_private.coinglass_dedicated_auth first
            JOIN collector_private.coinglass_dedicated_auth next ON next.id=first.superseded_by AND next.slot=first.slot
            JOIN collector_private.coinglass_dedicated_sessions s ON s.slot=first.slot
            WHERE first.id=%s AND first.phase='superseded' AND next.id=%s AND next.phase='retry'
              AND next.attempts=0 AND next.reason='auth_retry_pending' AND next.owner_job IS NULL
              AND s.status='unconfirmed' AND s.run_id IS NULL AND s.encrypted_payload IS NULL''',
            (incident,successor)).fetchone()
        require(confirmed is not None, 'acknowledgment_needs_readonly_reconciliation')


def main():
    global JOB
    if len(sys.argv) != 3:
        raise SystemExit('exact_job_and_qa_service_required')
    JOB = UUID(sys.argv[1])
    SERVICE_ID = sys.argv[2]
    from psycopg import connect
    from psycopg.rows import dict_row
    from native_pilot_host import require_service
    require_service(os.environ)
    require(os.environ.get('RENDER_GIT_COMMIT') == QA_SHA and os.environ.get('RENDER_SERVICE_ID') == SERVICE_ID,
            'qa_identity_mismatch')
    read_receipt()
    sys.path.insert(0, str(Path('standalone_collector/runtime').resolve(strict=True)))
    from dedicated_auth_state import DedicatedSessionStore
    with connect(os.environ['DATABASE_URL'],connect_timeout=5,row_factory=dict_row,
                 options='-c statement_timeout=5000 -c lock_timeout=1000') as conn:
        store = DedicatedSessionStore(conn,base64.b64decode(os.environ['COINGLASS_SESSION_KEY'],validate=True),
                                      os.environ['COINGLASS_LOGIN_EMAIL'])
        acknowledge(conn,store)
    print('{"first_incident_rearmed":true,"credential_submissions":0,"source_requests":0}')


if __name__ == '__main__':
    try:
        main()
    except Exception:
        raise SystemExit('qa_remediation_unavailable_check_metadata') from None
