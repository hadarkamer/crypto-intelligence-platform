"""Read only durable account-event metadata for one known QA pilot UUID. No browser or submit."""
import base64
import json
import os
from pathlib import Path
import sys
from uuid import UUID

HOST_SHA = 'ae032a47fcbda94ef21b53d4e56353f81504510f'
SERVICE = 'decision-hub-coinglass-pilot-20261125'
BRANCH = 'qa/coinglass-native-pilot-service-20261125'
AUTH_PHASES = {'preparing','submitted','retry','blocked','verified','superseded'}
CAUSES = {'auth_invalid_credentials','auth_challenge','auth_code_required','auth_rate_limited',
          'auth_unconfirmed','auth_gate_after_verification','auth_retry_exhausted','auth_form_unavailable',
          'auth_configuration_invalid','auth_retry_pending'}
STATES = {'ready','in_use','unconfirmed'}


def read_job(conn, slot, job):
    job = UUID(str(job))
    with conn.transaction():
        conn.execute('SET TRANSACTION READ ONLY')
        conn.execute("SET LOCAL statement_timeout='4s'")
        conn.execute("SET LOCAL lock_timeout='1s'")
        session = conn.execute("""
            SELECT status, (encrypted_payload IS NOT NULL) AS ciphertext_present,
                   (run_id = %s) IS TRUE AS owned_by_job,
                   (map_retry_after > clock_timestamp()) IS TRUE AS map_cooldown
            FROM collector_private.coinglass_dedicated_sessions WHERE slot = %s""", (job,slot)).fetchone()
        receipt = conn.execute("""
            SELECT i.phase, i.reason, i.attempts, (i.verified_job = %s) IS TRUE AS verified_this_job,
                   (i.owner_job = %s) IS TRUE AS owner_this_job,
                   (i.retry_after > clock_timestamp()) IS TRUE AS auth_cooldown
            FROM collector_private.coinglass_dedicated_auth_jobs j
            JOIN collector_private.coinglass_dedicated_auth i ON i.id = j.incident_id
            WHERE j.job_id = %s AND i.slot = %s""",(job,job,job,slot)).fetchone()
    out={'incident_linked_to_job':bool(receipt),'source_requests':0,'credential_submissions':0}
    if session:
        status,encrypted,owns,limited=session
        out.update(account_state=status if status in STATES else 'unknown',
                   ciphertext_present=bool(encrypted),session_owned_by_this_job=bool(owns),
                   map_cooldown_active=bool(limited))
    if receipt:
        phase,reason,attempts,verified,owns,limited=receipt
        out.update(event_phase=phase if phase in AUTH_PHASES else 'unknown',
                   event_reason=reason if reason in CAUSES else None,
                   preparation_attempts=attempts if type(attempts) is int and 0 <= attempts <= 2 else None,
                   checkpoint_for_this_job=bool(verified),incident_owned_by_this_job=bool(owns),
                   login_cooldown_active=bool(limited))
    return out


def main():
    if (len(sys.argv)!=2 or os.environ.get('RENDER_SERVICE_NAME')!=SERVICE or os.environ.get('RENDER')!='true'
            or os.environ.get('IS_PULL_REQUEST')!='false' or os.environ.get('RENDER_GIT_BRANCH')!=BRANCH
            or os.environ.get('RENDER_GIT_COMMIT')!=HOST_SHA):
        raise SystemExit('qa_identity_or_job_required')
    runtime=Path('standalone_collector/runtime').resolve();sys.path.insert(0,str(runtime))
    try:
        from dedicated_auth_state import account_slot
        import psycopg
        slot=account_slot(base64.b64decode(os.environ['COINGLASS_SESSION_KEY'],validate=True),
                          os.environ['COINGLASS_LOGIN_EMAIL'])
        with psycopg.connect(os.environ['DATABASE_URL'],connect_timeout=5) as conn:
            facts=read_job(conn,slot,sys.argv[1])
    except Exception:
        raise SystemExit('private_metadata_unavailable') from None
    print(json.dumps(facts,sort_keys=True))


if __name__=='__main__':
    main()
