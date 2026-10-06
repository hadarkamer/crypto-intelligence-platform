"""Default-off authenticated prospective inbox. This endpoint cannot trade.

Separate path, key and domain-bound HMAC prevent replay as legacy alert cards.
Schema creation remains an explicit future migration, never a request side effect.
"""
from datetime import datetime, timezone
import hashlib
import hmac
import json
import os
import re
import time

from .experimental_plan_store import PlanStore, PlanStoreError, moment
from .postgres_journal import PostgresJournal, JournalError

PATH = '/internal/testnet-experimental-plans/v1'
MODE = 'prospective_record_only_v1'
HEX = re.compile(r'[0-9a-f]{64}\Z')
MAX_BYTES = 65536


def enabled(env):
    # An old record-only intake setting does not opt in this new protocol.
    from .alert_cards_intake import enabled as legacy_environment_valid
    if (env.get('HL_TESTNET_EXPERIMENTAL_PLAN_INTAKE') != MODE
            or not HEX.fullmatch(env.get('HL_TESTNET_EXPERIMENTAL_PLAN_SECRET', ''))
            or not legacy_environment_valid(env)):
        return False
    try:
        moment(env['HL_TESTNET_EXPERIMENTAL_PLAN_NOT_BEFORE'])
        return True
    except (KeyError, ValueError, TypeError):
        return False


def signature(key, stamp, raw):
    if not isinstance(key, str) or not HEX.fullmatch(key):
        raise PlanStoreError('EXPERIMENTAL_AUTH_NOT_CONFIGURED')
    return hmac.new(bytes.fromhex(key), PATH.encode() + b'\n' + str(stamp).encode()
                    + b'\n' + raw, hashlib.sha256).hexdigest()


def authenticate(key, stamp, supplied, raw, now):
    if (not isinstance(stamp, str) or re.fullmatch(r'[0-9]{10}', stamp) is None
            or abs(now - int(stamp)) > 60 or not isinstance(supplied, str)
            or HEX.fullmatch(supplied) is None):
        return False
    try:
        return hmac.compare_digest(signature(key, stamp, raw), supplied)
    except (TypeError, ValueError):
        return False


def decoded(raw):
    def pairs(values):
        result = {}
        for key, value in values:
            if key in result:
                raise PlanStoreError('EXPERIMENTAL_DUPLICATE_JSON_KEY')
            result[key] = value
        return result
    def bad(value):
        raise PlanStoreError('EXPERIMENTAL_NONFINITE_JSON')
    if not isinstance(raw, bytes) or not 0 < len(raw) <= MAX_BYTES:
        raise PlanStoreError('EXPERIMENTAL_BODY_INVALID')
    try:
        return json.loads(raw, object_pairs_hook=pairs, parse_constant=bad)
    except (ValueError, UnicodeError, RecursionError):
        raise PlanStoreError('EXPERIMENTAL_BODY_INVALID') from None


def accept(raw, store, *, now, not_before):
    from experimental_execution_contract import validate
    message = validate(decoded(raw))
    return store.ingest(message, now=now, not_before=not_before)


def application(environ, start_response):
    code, result = '404 Not Found', dict(status='NOT_FOUND')
    try:
        if not enabled(os.environ):
            pass
        elif (environ.get('REQUEST_METHOD') != 'POST' or environ.get('QUERY_STRING')):
            code, result = '405 Method Not Allowed', dict(status='POST_ONLY')
        elif environ.get('CONTENT_TYPE') != 'application/json':
            code, result = '415 Unsupported Media Type', dict(status='JSON_REQUIRED')
        else:
            length = environ.get('CONTENT_LENGTH', '')
            if not length.isdecimal() or not 0 < int(length) <= MAX_BYTES:
                code, result = '413 Content Too Large', dict(status='BOUNDED_BODY_REQUIRED')
            else:
                raw = environ['wsgi.input'].read(int(length))
                if len(raw) != int(length):
                    raise PlanStoreError('EXPERIMENTAL_BODY_INVALID')
                at = time.time()
                if not authenticate(os.environ['HL_TESTNET_EXPERIMENTAL_PLAN_SECRET'],
                        environ.get('HTTP_X_PLAN_TIMESTAMP'), environ.get('HTTP_X_PLAN_SIGNATURE'), raw, at):
                    code, result = '403 Forbidden', dict(status='AUTHENTICATION_REQUIRED')
                else:
                    from experimental_execution_contract import validate
                    validate(decoded(raw))
                    # No metadata requests, wallet lookup, card registration or
                    # order code is imported or invoked by this request path.
                    result = accept(raw, PlanStore(PostgresJournal.from_env(os.environ)),
                        now=datetime.fromtimestamp(at, timezone.utc).isoformat(),
                        not_before=os.environ['HL_TESTNET_EXPERIMENTAL_PLAN_NOT_BEFORE'])
                    code = '200 OK'
    except JournalError as exc:
        invalid = str(exc) in {
            'EXPERIMENTAL_BODY_INVALID', 'EXPERIMENTAL_DUPLICATE_JSON_KEY',
            'EXPERIMENTAL_NONFINITE_JSON', 'EXPERIMENTAL_FUTURE_SOURCE',
            'EXPERIMENTAL_PLAN_NOT_RECEIVED_BEFORE_ARM', 'EXPERIMENTAL_PLAN_STALE',
            'EXPERIMENTAL_IMMUTABLE_PLAN_CHANGED', 'EXPERIMENTAL_SOURCE_SEQUENCE_CONFLICT'}
        code, result = (('400 Bad Request', dict(status='INVALID_EXPERIMENTAL_PLAN')) if invalid
                        else ('503 Service Unavailable', dict(status='EXPERIMENTAL_RECORDING_UNAVAILABLE')))
    except (ValueError, TypeError, KeyError):
        code, result = '400 Bad Request', dict(status='INVALID_EXPERIMENTAL_PLAN')
    except Exception:
        code, result = '503 Service Unavailable', dict(status='EXPERIMENTAL_RECORDING_UNAVAILABLE')
    body = json.dumps(result, separators=(',', ':')).encode()
    start_response(code, [('Content-Type', 'application/json'), ('Content-Length', str(len(body))),
                          ('Cache-Control', 'no-store'), ('X-Content-Type-Options', 'nosniff')])
    return [body]
