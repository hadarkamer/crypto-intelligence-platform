"""Run the exact QA supervisor using a real local PostgreSQL test account store."""
import base64
import json
import os
from pathlib import Path
import sys
import time
import threading

HERE=Path(__file__).resolve().parent
PACK=HERE.parent/'unpacked'
sys.path[:0]=[str(PACK),str(PACK/'product/runtime')]
from pgserver import get_server
import psycopg
from psycopg.rows import dict_row
from dedicated_auth_state import account_slot
from session_state import WORK_LOCK
from account_large_object import save,load

key=bytes(range(32))
email='synthetic-readiness@example.test'
slot=account_slot(key,email)
previous='aa3ab31f-828f-4465-95da-60a462e802a5'
incident='11111111-2222-4333-8444-555555555555'
state={'cookies':[dict(name='qa_synthetic_account',value='not-a-real-account',
    domain='.coinglass.com',path='/',expires=-1,httpOnly=True,secure=True,sameSite='Lax')],'origins':[]}
source=get_server(Path('/tmp/readiness-source'),cleanup_mode='delete')
# Mirror qa_boot's stdout capture, including output from the real browser child.
root=Path('/tmp/coinglass-btc-once')
root.mkdir(mode=0o700,exist_ok=True)
original_stdout=os.dup(1)
read_fd,write_fd=os.pipe()
os.dup2(write_fd,1)
os.dup2(write_fd,2)
os.close(write_fd)
def capture_log():
    with os.fdopen(read_fd,'rb',buffering=0) as stream, (root/'metrics.log').open('ab',buffering=0) as log:
        while data:=stream.read(65536):
            log.write(data)
            os.write(original_stdout,data)
threading.Thread(target=capture_log,daemon=True).start()
os.environ.update(DATABASE_URL=source.get_uri(),COINGLASS_SESSION_KEY=base64.b64encode(key).decode(),
    COINGLASS_LOGIN_EMAIL=email,CG_QA_ROOT='/tmp/coinglass-btc-once',PORT='10000')
try:
    with psycopg.connect(source.get_uri(),autocommit=True,row_factory=dict_row) as db:
        db.execute((PACK/'product/session_state.sql').read_text())
        db.execute((PACK/'product/dedicated_auth_state.sql').read_text())
        db.execute('SELECT pg_advisory_lock(%s)',(WORK_LOCK,))
        db.execute("INSERT INTO collector_private.coinglass_dedicated_sessions(slot,revision,status) VALUES(%s,1,'unconfirmed')",(slot,))
        db.execute("INSERT INTO collector_private.coinglass_dedicated_auth(id,slot,phase,attempts,retry_until,verified_at,verified_job) VALUES(%s,%s,'verified',1,now()+interval '1 hour',now(),%s)",(incident,slot,previous))
        db.execute('INSERT INTO collector_private.coinglass_dedicated_auth_jobs VALUES(%s,%s)',(previous,incident))
        proof=save(db,key,slot,previous,state,account_verified=True)
        assert load(db,key,slot,previous,proof['oid'])==state
        # Prove owner-only ACL rejection on the real database, then restore the
        # fixture ACL. These operations never involve a genuine account store.
        db.execute(f"GRANT SELECT ON LARGE OBJECT {int(proof['oid'])} TO PUBLIC")
        try:
            load(db,key,slot,previous,proof['oid'])
            raise AssertionError('public account artifact accepted')
        except ValueError as error:
            assert str(error)=='account_artifact_acl_not_private'
        db.execute(f"REVOKE SELECT ON LARGE OBJECT {int(proof['oid'])} FROM PUBLIC")
        if os.getenv('CG_READINESS_CASE')=='bad-binding':
            os.environ['COINGLASS_LOGIN_EMAIL']='wrong-synthetic-owner@example.test'
    pin=json.loads((PACK/'run-pin.json').read_text())
    pin['experiment_started_epoch']=time.time()
    (PACK/'run-pin.json').write_text(json.dumps(pin))
    import qa_host
    os.umask(0o077)
    Path('/receipts/control.json').write_text(json.dumps({'host':'http://127.0.0.1:10000','key':qa_host.SECRET}))
    emit=qa_host.event
    def audited_event(kind,**data):
        if kind=='provisional_account_input':
            with qa_host.connect() as db:
                row=db.execute('SELECT revision,status,encrypted_payload FROM collector_private.coinglass_dedicated_sessions WHERE slot=%s',(slot,)).fetchone()
                from dedicated_auth_state import DedicatedSessionStore
                persisted=DedicatedSessionStore(db,key,email)._open(row['encrypted_payload'],row['revision'],allow_empty=True)
                assert persisted=={'cookies':[],'origins':[]}
                assert row['status']=='in_use' and row['revision']==1
                Path('/receipts/restore-guards.json').write_text(json.dumps(dict(
                    owner_only_artifact=True,public_acl_rejected=True,
                    canonical_cookie_count_before_proof=0,canonical_revision_before_proof=1,
                    fake_account_only=True)))
        emit(kind,**data)
    qa_host.event=audited_event
    qa_host.run()
finally:
    root=Path('/tmp/coinglass-btc-once')
    if (root/'supervisor-resources.json').exists():
        Path('/receipts/supervisor-resources.json').write_bytes((root/'supervisor-resources.json').read_bytes())
    source.cleanup()
    Path('/receipts/local-source-cleanup.json').write_text(json.dumps({'source_pgdata_removed':not Path('/tmp/readiness-source').exists(),'target_pgdata_removed':not (root/'pgdata').exists()}))
