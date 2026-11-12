"""Record-only recovery inbox/outcome delivery, with separate write authority.

This endpoint cannot construct an exchange client or invoke an execution cycle.
The existing runtime owns execution; Telegram only records a scoped decision.
"""
import hashlib
import hmac
import json
import re
import uuid

from . import position_recovery as recovery

PATH = '/internal/position-recovery/v1'
MAX_BYTES = 4096
MAX_DELIVERY_ATTEMPTS = 3


def signature(secret, stamp, body):
    return hmac.new(secret.encode(), PATH.encode()+b'\n'+stamp.encode()+b'\n'+body,
                    hashlib.sha256).hexdigest()


def settings(env):
    secret = env.get('POSITION_RECOVERY_API_SECRET', '')
    users = env.get('POSITION_RECOVERY_USER_IDS', '').split(',')
    users = {u.strip() for u in users if u.strip().isdecimal()}
    chat = env.get('POSITION_RECOVERY_CHAT_ID', '')
    if not re.fullmatch('[a-f0-9]{64}', secret) or not users or chat not in users:
        raise ValueError('RECOVERY_NOT_CONFIGURED')
    return secret, users, chat


def _find(state, identity):
    for row in state.get('position_recovery', {}).values():
        for notice in row['notices']:
            if notice['id'] == identity: return row, notice
    raise ValueError('RECOVERY_NOTICE_NOT_FOUND')


def operate(state, command, *, users, chat, now):
    """Atomic state operation; transport I/O is strictly outside this function."""
    op = command.get('op')
    if op == 'choose' and set(command) == {'op','event','round','choice','actor','chat'}:
        if command['chat'] != command['actor'] or str(command['actor']) not in users:
            raise ValueError('RECOVERY_PRIVATE_AUTHORIZED_USER_REQUIRED')
        if type(command['round']) is not int or not 1 <= command['round'] <= 1000000:
            raise ValueError('RECOVERY_ROUND_INVALID')
        return recovery.choose(state, token=command['event'], round_number=command['round'],
            choice=command['choice'], actor=command['actor'], allowed_users=users, now=now)
    if op == 'claim' and set(command) == {'op'}:
        for row in state.get('position_recovery', {}).values():
            for notice in row['notices']:
                if notice['delivery']=='SENDING' and now-notice['claimed_at_ms'] >= 60000:
                    notice['delivery']='UNKNOWN'
                if (notice['delivery'] != 'PENDING' or notice['retry_at_ms'] > now
                        or notice['attempts'] >= MAX_DELIVERY_ATTEMPTS): continue
                notice.update(delivery='SENDING', claim=uuid.uuid4().hex, claimed_at_ms=now,
                    claimed_revision=notice['revision'], attempts=notice['attempts']+1)
                return dict(status='CLAIMED', id=notice['id'], revision=notice['revision'],
                    claim=notice['claim'], chat=chat, event=row['event_id'], symbol=row['symbol'],
                    role=row['role'], current_phase=row['phase'], message_id=notice['message_id'],
                    result=notice['result'])
        return dict(status='EMPTY')
    if op == 'ack' and set(command) == {'op','id','claim','revision','status','message_id'}:
        row, notice = _find(state, command['id'])
        if (command['claim'] != notice.get('claim')
                or command['revision'] != notice.get('claimed_revision')):
            return dict(status='STALE')
        if command['status'] not in ('SENT','REJECTED','UNKNOWN'):
            raise ValueError('RECOVERY_DELIVERY_STATUS_INVALID')
        if notice['delivery'] not in ('SENDING','UNKNOWN'): return dict(status='ALREADY_RECORDED')
        if command['status'] == 'SENT':
            if type(command['message_id']) is not int or command['message_id'] <= 0:
                raise ValueError('RECOVERY_DELIVERY_RECEIPT_REQUIRED')
            notice['message_id'] = command['message_id']
            notice['delivery'] = 'SENT' if notice['revision'] == command['revision'] else 'PENDING'
        elif command['status'] == 'REJECTED':
            notice.update(delivery='PENDING', retry_at_ms=now+60000)
        else:
            # A lost Telegram response is not proof of non-delivery. Never
            # duplicate a possibly delivered message to make a flag green.
            notice['delivery'] = 'UNKNOWN'
        return dict(status='RECORDED')
    raise ValueError('RECOVERY_COMMAND_INVALID')


def application(environ, start_response, *, store, clock, env, wake):
    status, value = '404 Not Found', {'status':'NOT_FOUND'}
    try:
        secret, users, chat = settings(env)
        stamp = environ.get('HTTP_X_RECOVERY_TIMESTAMP', '')
        supplied = environ.get('HTTP_X_RECOVERY_SIGNATURE', '')
        size = environ.get('CONTENT_LENGTH', '')
        now = clock()
        if (environ.get('REQUEST_METHOD') != 'POST' or environ.get('QUERY_STRING')
                or environ.get('CONTENT_TYPE') != 'application/json'
                or not size.isdecimal() or not 0 < int(size) <= MAX_BYTES
                or not stamp.isdecimal() or abs(now//1000-int(stamp)) > 30):
            raise ValueError('RECOVERY_AUTHENTICATION_REQUIRED')
        raw = environ['wsgi.input'].read(int(size))
        if len(raw) != int(size) or not hmac.compare_digest(signature(secret, stamp, raw), supplied):
            raise ValueError('RECOVERY_AUTHENTICATION_REQUIRED')
        command = json.loads(raw)
        if not isinstance(command, dict): raise ValueError('RECOVERY_COMMAND_INVALID')
        if command.get('op') == 'choose' and (command.get('actor') != command.get('chat')
                or str(command.get('actor')) not in users):
            raise ValueError('RECOVERY_PRIVATE_AUTHORIZED_USER_REQUIRED')
        value = store.mutate(lambda state: operate(state, command, users=users, chat=chat, now=now))
        if command.get('op') == 'choose' and value['status'] == 'RECORDED': wake.set()
        status = '200 OK'
    except (ValueError, KeyError, TypeError):
        pass
    except Exception:
        status, value = '503 Service Unavailable', {'status':'RECOVERY_RECORDING_UNAVAILABLE'}
    body = json.dumps(value, separators=(',', ':'), allow_nan=False).encode()
    start_response(status, [('Content-Type','application/json'), ('Content-Length',str(len(body))),
                            ('Cache-Control','no-store')])
    return [body]
