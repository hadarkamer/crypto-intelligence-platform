"""Acknowledge only the finished, demonstrably unsubmitted direct-login 404 on the second QA job."""
import base64
import json
import os
from pathlib import Path
import stat
import sys
from uuid import UUID

JOB = PREVIOUS_JOB = None
QA_SHA = 'ae032a47fcbda94ef21b53d4e56353f81504510f'

OBSERVATION = Path('/tmp/coinglass-btc-pilot-diagnostics/first-ui.json')


def require(ok, label):
    if not ok:
        raise SystemExit(label)


def check_observation(facts):
    require(facts.get('job_id') == str(JOB) and facts.get('attached') is True
            and facts.get('page_closed') is True, 'receipt_not_completed_for_second_job')
    navigation = {'path': 'official_login', 'http_status': 404}
    require(facts.get('first_navigation') == navigation and facts.get('navigations') == [navigation]
            and facts.get('login_responses') == [], 'other_navigation_or_submission_response')
    require(facts.get('submit_reserved') is False and facts.get('form_reported_submitted') is False
            and 'before_submit_controls' in facts and facts['before_submit_controls'] is None,
            'submission_or_unknown')
    require(facts.get('form_result') == 'login_form_unavailable'
            and facts.get('stage_failure') == 'auth_form_unavailable', 'unexpected_form_outcome')
    negative = ('email_visible','password_visible','challenge_visible','code_visible','verification_prompt',
                'security_check','request_blocked','interstitial_title','invalid_credentials',
                'invalid_password','verify_email','restricted_account')
    for name in ('initial_controls','after_form_controls','final_controls'):
        seen = facts.get(name) or {}
        require(seen.get('path') == 'official_login' and all(seen.get(field) is False for field in negative),
                'control_present_or_unknown')


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
                 AND p.phase='superseded' AND p.reason='auth_form_unavailable' AND p.attempts=1
                 AND p.owner_job IS NULL AND p.owner_revision IS NULL AND p.verified_at IS NULL
                 AND p.verified_job IS NULL AND p.retry_after IS NULL
                 AND NOT EXISTS (SELECT 1 FROM collector_private.coinglass_dedicated_auth i2
                                 WHERE i2.slot=i.slot AND i2.id NOT IN (i.id, p.id))
                 AND NOT EXISTS (SELECT 1 FROM collector_private.coinglass_dedicated_auth_jobs j3
                                 WHERE j3.incident_id=p.id AND j3.job_id<>prev_job.job_id)
                 AND NOT EXISTS (SELECT 1 FROM collector_private.coinglass_dedicated_auth_jobs j2
                                 WHERE j2.incident_id=i.id AND j2.job_id<>j.job_id)) AS eligible
              FROM collector_private.coinglass_dedicated_auth_jobs j
              JOIN collector_private.coinglass_dedicated_auth i ON i.id=j.incident_id
              JOIN collector_private.coinglass_dedicated_sessions s ON s.slot=i.slot
              JOIN collector_private.coinglass_dedicated_auth p ON p.slot=i.slot AND p.superseded_by=i.id
              JOIN collector_private.coinglass_dedicated_auth_jobs prev_job ON prev_job.incident_id=p.id
              WHERE j.job_id=%s AND i.slot=%s AND prev_job.job_id=%s''',(JOB,store.slot,PREVIOUS_JOB)).fetchone()
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
    global JOB, PREVIOUS_JOB
    if len(sys.argv) != 4:
        raise SystemExit('exact_job_and_qa_service_required')
    JOB = UUID(sys.argv[1])
    PREVIOUS_JOB = UUID(sys.argv[2])
    require(JOB != PREVIOUS_JOB, 'distinct_first_and_second_jobs_required')
    SERVICE_ID = sys.argv[3]
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
    print('{"direct_login_404_incident_rearmed":true,"credential_submissions":0,"source_requests":0}')


if __name__ == '__main__':
    try:
        main()
    except Exception:
        raise SystemExit('qa_remediation_unavailable_check_metadata') from None
