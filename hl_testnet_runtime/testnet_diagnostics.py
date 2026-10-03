"""Private bounded Testnet journal diagnostics; no venue or signing capability.

Only allowlisted configuration and durable SELECT observations are returned.
Stored snapshots retain their original time and are never current exchange
authority. HTTP cannot initialize storage, reserve requests or enable entries.
"""
from datetime import datetime, timezone
from decimal import Decimal
import hmac
import json
import os
import re
import time

from .postgres_journal import PostgresJournal, SCHEMA, VERSION as JOURNAL_VERSION

VERSION = 'testnet-readonly-diagnostics-v1'
SERVICE = 'srv-dakptbh594qs7395460g'
MAX_BYTES = 262144
MAX_RECENT = 20
MAX_EVENTS = 40
CARD_TABLE = 'hl_testnet_cards_v1.cards'
RECEIPTS = 'hl_testnet_cards_v1.delivery_receipts'
BUCKETS = 'hl_testnet_filled_dispatch_v1.buckets'
REQUESTS = 'hl_testnet_filled_dispatch_v1.requests'
EVENTS = 'hl_testnet_filled_dispatch_v1.events'
TABLES = (SCHEMA + '.metadata', SCHEMA + '.prepared', SCHEMA + '.attempts',
          CARD_TABLE, RECEIPTS, BUCKETS, REQUESTS, EVENTS,
          SCHEMA + '.request_budget_policy', SCHEMA + '.request_budget_tickets')
HEX = re.compile(r'[0-9a-f]{64}\Z')
TOKEN = re.compile(r'[A-Za-z0-9_-]{32,256}\Z')
PRIVATE_TOKEN_NAMES = ('HL_TESTNET_CARDS_INTAKE_SECRET',
                       'HL_TESTNET_REPORT_API_TOKEN', 'HL_TESTNET_APP_SIGNING_KEY')
MODES = ('read_only', 'single_testnet_attempt_v1', 'filled_card_controlled_v1',
         'long_stream_testnet_v1', 'cancel_monitor_testnet_v1',
         'cancel_rehearsal_testnet_v1')
ENUM_CONFIG = {
    'HL_TESTNET_RUNTIME_MODE': MODES,
    'HL_TESTNET_JOURNAL_BACKEND': ('staging_postgres_v1',),
    'HL_TESTNET_CARDS_PHASE1': ('record_only_v1',),
    'HL_TESTNET_CARDS_INTAKE': ('record_only_v1',),
    'HL_TESTNET_LONG_STREAM': ('approved_alerts_v1',),
    'HL_TESTNET_SHORT_STREAM': ('approved_alerts_v1',),
    'HL_TESTNET_LONG_ENTRY_ENABLED': ('true', 'false'),
    'HL_TESTNET_SHORT_ENTRY_ENABLED': ('true', 'false'),
    'HL_TESTNET_FILLED_DISPATCH': ('approved_long_stream_v1', 'approved_single_card_v1'),
    'HL_TESTNET_FILLED_AFTER_EXIT_POLICY': ('cancel_remainder_after_exit_v1',),
    'HL_TESTNET_TWO_ACCOUNT_EXECUTION': ('disabled',),
    'HL_TESTNET_EMERGENCY_CLOSE': ('approved_testnet_v1',),
    'HL_TESTNET_EMERGENCY_RELEASE': ('continuous_testnet_v1',),
    'HL_TESTNET_LONG_ACCOUNT_SOURCE': ('existing_single_account_v1',),
    'HL_TESTNET_CONNECTION_REVIEW': ('two_account_no_orders_v1',),
    'HL_TESTNET_CARD_SYNC': ('registered_readonly_v1',),
    'HL_TESTNET_FILLED_AUTOWAIT': ('approved_next_u21_once_v1',),
    'HL_TESTNET_APP_DELIVERY': ('ed25519_signed_v1',),
    'HL_TESTNET_SINGLE_TRIAL_CONTROLLER': ('approved_bounded_single_trial_v1',),
}


def _identifier(value, *, pattern=r'[A-Za-z0-9_.:-]{1,100}'):
    if (isinstance(value, str) and re.fullmatch(pattern, value)
            and not re.search(r'0x[0-9a-fA-F]{40,64}', value)):
        return value
    return None


def _number(value):
    from .card_lifecycle import number, text
    try:
        return text(number(value, signed=True))
    except ValueError:
        return None


def _time(value):
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, str) and len(value) <= 40:
        try:
            stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
            if stamp.tzinfo is not None:
                return stamp.astimezone(timezone.utc).isoformat()
        except ValueError:
            pass
    return None


def _moment(value):
    return value if type(value) is int and 0 < value < 10**15 else None


def _source(value):
    value = value if isinstance(value, dict) else {}
    return dict(event_id=_identifier(value.get('event_id')),
        symbol=_identifier(value.get('symbol'), pattern=r'[A-Z][A-Z0-9]{0,19}'),
        side=value.get('side') if value.get('side') in ('LONG', 'SHORT') else None,
        at=_time(value.get('at')),
        **{key: _number(value.get(key)) for key in ('entry', 'stop', 'take_profit')})


def _config(env):
    result = {key: (env.get(key) if env.get(key) in values else
                    'UNSET' if not env.get(key) else 'UNRECOGNIZED')
              for key, values in ENUM_CONFIG.items()}
    for key in ('HL_TESTNET_LONG_NOT_BEFORE', 'HL_TESTNET_SHORT_NOT_BEFORE',
                'HL_TESTNET_FILLED_NOT_BEFORE','HL_TESTNET_FILLED_SOURCE_EXPIRES_AT',
                'HL_TESTNET_SINGLE_TRIAL_NOT_BEFORE','HL_TESTNET_SINGLE_TRIAL_DEADLINE'):
        result[key] = _time(env.get(key))
    for key in ('HL_TESTNET_SHORT_TRIAL_CARD_ID', 'HL_TESTNET_FILLED_CARD_ID',
                'HL_TESTNET_PROTECTION_TIMING_CARD_ID'):
        value = env.get(key)
        result[key] = value if isinstance(value, str) and HEX.fullmatch(value) else None
    for key in ('HL_TESTNET_FILLED_APPROVAL_EXPIRES_MS','HL_TESTNET_PROTECTION_TIMING_STARTED_MS',
                'HL_TESTNET_PROTECTION_TIMING_EXPIRES_MS'):
        value = env.get(key)
        result[key] = _moment(int(value)) if isinstance(value,str) and value.isdigit() and len(value)<16 else None
    result['HL_TESTNET_SINGLE_TRIAL_RUN_ID'] = _identifier(env.get('HL_TESTNET_SINGLE_TRIAL_RUN_ID'),
                                                        pattern=r'hadar-timing-[0-9TZ]{16,20}')
    result['HL_TESTNET_SINGLE_TRIAL_COMMIT'] = _identifier(env.get('HL_TESTNET_SINGLE_TRIAL_COMMIT'),
                                                        pattern=r'[0-9a-f]{40}')
    result['RENDER_GIT_COMMIT'] = _identifier(env.get('RENDER_GIT_COMMIT'),
                                            pattern=r'[0-9a-f]{40,64}')
    result['RENDER_SERVICE_ID'] = SERVICE
    result['safety_pipeline_configured'] = bool(env.get('HL_TESTNET_SAFETY_PIPELINE'))
    return result


def _counts(conn, table, grouping=None):
    row = conn.execute(f'SELECT count(*) FROM {table}').fetchone()
    result = dict(total=int(row[0]))
    if grouping is not None:
        rows = conn.execute(f'SELECT {grouping},count(*) FROM {table} '
                            f'GROUP BY {grouping} ORDER BY count(*) DESC LIMIT 40').fetchall()
        result['by_status'] = [{ 'status': _identifier(name, pattern=r'[A-Z_]{1,100}'),
                               'count': int(count)} for name, count in rows]
    return result


def _timing(value):
    value = value if isinstance(value, dict) else {}
    numeric = ('first_fill_at_ms', 'first_fill_observed_at_ms', 'stop_observed_at_ms',
               'stop_verified_at_ms', 'stop_public_status_at_ms', 'fill_to_stop_public_ms',
               'fill_to_stop_observation_ms')
    result = {key: value[key] for key in numeric if type(value.get(key)) is int
              and 0 <= value[key] < 10**15}
    for key in ('quantity_at_stop_verification',):
        if key in value:
            result[key] = _number(value[key])
    if value.get('timing_semantics') == 'public_observation_before_commit':
        result['timing_semantics'] = value['timing_semantics']
    return result


def _bucket(value, checksum, revision, roles, now_ms):
    from . import card_lifecycle as life
    if not isinstance(value, dict) or life.digest(value) != checksum:
        raise ValueError('DIAGNOSTIC_BUCKET_INTEGRITY_FAILURE')
    role = roles.get(value.get('account'))
    result = dict(bucket=_identifier(value.get('bucket'), pattern=r'[0-9a-f]{64}'),
        revision=int(revision), account_role=role or 'unmapped_account',
        symbol=_identifier(value.get('symbol'), pattern=r'[A-Z][A-Z0-9]{0,19}'),
        pending_request_id=_identifier(value.get('pending'), pattern=r'[0-9a-f]{64}'),
        emergency_status=_identifier((value.get('emergency') or {}).get('phase'),
                                     pattern=r'[A-Z_]{1,80}'), cards=[])
    incident = value.get('emergency') or {}
    result['emergency'] = dict(card_id=_identifier(incident.get('card_id'),pattern=r'[0-9a-f]{64}'),
        phase=_identifier(incident.get('phase'),pattern=r'[A-Z_]{1,80}'),
        latched_at_ms=_moment(incident.get('latched_at_ms')),
        closed_at_ms=_moment(incident.get('closed_at_ms')),
        closure_origin=_identifier(incident.get('closure_origin'),pattern=r'[A-Z_]{1,80}'),
        provisional=incident.get('provisional') is True)
    ev = value.get('evidence')
    if not ev:
        result['evidence_status'] = 'NO_SAVED_SNAPSHOT'
        return result
    snap = ev['snapshot']
    result.update(evidence_at_ms=_moment(snap.get('at_ms')),
        evidence_age_ms=max(0, now_ms-snap['at_ms']),
        history_complete=snap.get('history_complete') is True,
        orders_complete=snap.get('orders_complete') is True,
        position_quantity=_number(snap.get('position_quantity')))
    bindings = value.get('bindings', [])
    if not bindings:
        result['evidence_status'] = 'NO_CARD_BINDINGS'
        return result
    if ev.get('bindings') != bindings:
        raise ValueError('DIAGNOSTIC_BINDINGS_CHANGED')
    emergency_ids = [r['observed_oid'] for r in (value.get('emergency') or {}).get('requests', [])
                     if r['proposal']['operation'] == 'EMERGENCY_CLOSE'
                     and r['phase'] == 'OBSERVED' and r.get('observed_oid')]
    # Deliberately evaluate the original observation time, then disclose age.
    # This is historical evidence, never fresh trading or account authority.
    report = life.review(bindings, snap, now_ms=snap['at_ms'],
                         emergency_stop_oids=emergency_ids)
    result['bucket_issues'] = report['bucket_issues']
    result['cards_truncated'] = len(report['cards']) > MAX_RECENT
    for row in report['cards'][:MAX_RECENT]:
        binding = next(b for b in bindings if b['card_id'] == row['card_id'])
        card = (value.get('originals', {}).get(row['card_id']) or {}).get('card') or {}
        from .trade_cards import validate_card, checksum as card_checksum
        card = validate_card(card)
        if (card['card_id'] != row['card_id'] or card_checksum(card) != binding['card_digest']
                or card['account_role'] != binding['role']):
            raise ValueError('DIAGNOSTIC_SOURCE_BINDING_CHANGED')
        source = _source((card.get('prepared') or {}).get('source'))
        links = {oid: leg for leg, ids in binding['orders'].items() for oid in ids}
        # Normalize duplicate replay facts before quantity/price summaries.
        fills = list({f['fill_id']: f for f in snap['fills'] if f['oid'] in links}.values())
        entries = [f for f in fills if links[f['oid']] == 'ENTRY']
        exits = [f for f in fills if links[f['oid']] != 'ENTRY']
        def average(rows):
            quantity = sum((Decimal(f['quantity']) for f in rows), Decimal(0))
            return life.text(sum((Decimal(f['quantity'])*Decimal(f['price']) for f in rows),
                                 Decimal(0))/quantity) if quantity else None
        remain = life.number(row['remaining_quantity'], signed=True)
        orders = [o for o in snap['open_orders'] if o['oid'] in links]
        result['cards'].append(dict(card_id=row['card_id'],source_event_id=source['event_id'],
            account_role=binding['role'],source=source,state=row['state'],
            planned_prices={key:_number(binding['prices'].get(key))
                            for key in ('entry','stop','take_profit')},
            planned_quantity=_number(binding['planned_quantity']),
            entry_quantity=row['entry_quantity'],exit_quantity=row['exit_quantity'],
            remaining_quantity=row['remaining_quantity'],actual_entry_price=average(entries),
            actual_exit_price=average(exits),first_entry_at_ms=min((f['at_ms'] for f in entries),default=None),
            final_entry_at_ms=max((f['at_ms'] for f in entries),default=None),
            last_exit_at_ms=max((f['at_ms'] for f in exits),default=None),
            entry_attempt_at_ms=_moment(value.get('entry_timing_armed',{}).get(row['card_id'])),
            protection_timing=_timing(value.get('protection_timing',{}).get(row['card_id'])),
            stop_quantity_observed=row['stop_quantity_observed'],
            take_profit_quantity_observed=row['take_profit_quantity_observed'],
            protection_verified_at_snapshot=(remain>0 and not report['bucket_issues']
                and not row['issues'] and life.number(row['stop_quantity_observed'])>=remain
                and life.number(row['take_profit_quantity_observed'])>=remain),
            closure_verified=row['closure_verified'],closure_origin=row.get('closure_origin'),
            issues=row['issues'],fills_truncated=len(fills)>MAX_RECENT,
            fills=[{key:f[key] for key in ('fill_id','oid','quantity','price','side','at_ms')}
                   for f in sorted(fills,key=lambda f:f['at_ms'])[-MAX_RECENT:]],
            open_orders_truncated=len(orders)>MAX_RECENT,
            open_orders=[dict(leg=links[o['oid']],**{key:o[key] for key in
                ('oid','quantity','price','trigger_price','side','reduce_only','state','order_type')})
                for o in orders[:MAX_RECENT]],
            terminal_orders=[dict(leg=links[o['oid']],**{key:o[key] for key in
                ('oid','state','filled_quantity','at_ms')}) for o in snap['terminal_orders']
                if o['oid'] in links][:MAX_RECENT]))
    return result


def load_diagnostics(env=None):
    env = dict(os.environ if env is None else env)
    journal = PostgresJournal.from_env(env)
    from .trade_cards import account_routes
    try:
        routes = account_routes(env)
        roles = {route['account']: role for role, route in routes.items() if route['account']}
        route_summary = {role: dict(status=route['status']) for role, route in routes.items()}
    except ValueError:
        roles, route_summary = {}, {'status':'ACCOUNT_CONFIGURATION_INVALID'}
    now_ms = time.time_ns()//1000000
    result = dict(version=VERSION,environment='testnet',generated_at_ms=now_ms,
        config=_config(env),account_roles=route_summary,
        order_requests_sent=0,signing_tested=False,exchange_calls=0,
        snapshot_semantics='persisted_history_only_not_current_exchange_authority')
    with journal._transaction() as conn:
        current = conn.execute('SELECT current_database()').fetchone()[0]
        result['database'] = dict(host=journal._parameters['host'],dbname=current,
                                  journal_version_expected=JOURNAL_VERSION)
        present = {table: conn.execute('SELECT to_regclass(%s)',(table,)).fetchone()[0] is not None
                   for table in TABLES}
        result['tables_present'] = present
        if present[SCHEMA+'.metadata']:
            row = conn.execute(f'SELECT version,created_at FROM {SCHEMA}.metadata WHERE singleton').fetchone()
            result['database'].update(journal_version=_identifier(row[0]),created_at=_time(row[1]))
        if present[SCHEMA+'.prepared']:
            result['legacy_prepared'] = _counts(conn,SCHEMA+'.prepared')
            rows = conn.execute(f'''SELECT plan_key,source_id,manifest->'source',
                    manifest->'execution',created_at FROM {SCHEMA}.prepared
                    ORDER BY created_at DESC,plan_key LIMIT 20''').fetchall()
            result['legacy_prepared']['recent'] = [dict(plan_key=_identifier(r[0],pattern=r'[0-9a-f]{64}'),
                source_event_id=_identifier(r[1]),source=_source(r[2]),rounded=_source(r[3]),
                created_at=_time(r[4])) for r in rows]
        if present[SCHEMA+'.attempts']:
            result['legacy_attempts'] = _counts(conn,SCHEMA+'.attempts',"result->>'status'")
        if present[CARD_TABLE]:
            result['cards'] = _counts(conn,CARD_TABLE,"manifest->>'state'")
            rows = conn.execute(f'''SELECT card_id,manifest->>'event_id',manifest->>'account_role',
                manifest->>'state',manifest->'prepared'->'source',manifest->'prepared'->'execution',
                manifest->'planning'->>'quantity',created_at FROM {CARD_TABLE}
                ORDER BY created_at DESC,card_id LIMIT 20''').fetchall()
            result['cards']['recent'] = [dict(card_id=_identifier(r[0],pattern=r'[0-9a-f]{64}'),
                source_event_id=_identifier(r[1]),account_role=r[2] if r[2] in ('long_account','short_account') else None,
                state=_identifier(r[3],pattern=r'[A-Z_]{1,80}'),source=_source(r[4]),rounded=_source(r[5]),
                planned_quantity=_number(r[6]),received_at=_time(r[7])) for r in rows]
        if present[RECEIPTS]:
            result['receipts'] = _counts(conn,RECEIPTS,'status')
            rows = conn.execute(f'''SELECT receipt_id,status,card_id,reason,received_at
                FROM {RECEIPTS} ORDER BY received_at DESC,receipt_id LIMIT 20''').fetchall()
            result['receipts']['recent'] = [dict(receipt_id=_identifier(r[0],pattern=r'[0-9a-f]{64}'),
                status=_identifier(r[1],pattern=r'[A-Z_]{1,80}'),card_id=_identifier(r[2],pattern=r'[0-9a-f]{64}'),
                reason=_identifier(r[3],pattern=r'[A-Z_]{1,80}'),received_at=_time(r[4])) for r in rows]
        if present[BUCKETS]:
            result['buckets'] = _counts(conn,BUCKETS)
            result['buckets']['emergency_latched_total'] = int(conn.execute(f'''SELECT count(*)
                FROM {BUCKETS} WHERE value->'emergency' IS NOT NULL''').fetchone()[0])
            rows = conn.execute(f'''SELECT value,digest,revision FROM {BUCKETS}
                ORDER BY (value->'evidence'->'snapshot'->>'at_ms')::bigint DESC NULLS LAST,bucket
                LIMIT 21''').fetchall()
            result['buckets'].update(truncated=len(rows)>MAX_RECENT,
                recent=[_bucket(*r,roles,now_ms) for r in rows[:MAX_RECENT]])
        if present[REQUESTS]:
            result['requests'] = _counts(conn,REQUESTS,'phase')
            rows = conn.execute(f'''SELECT request_id,bucket,phase,
                value->'proposal'->>'operation',value->'proposal'->>'leg',value->'proposal'->>'card_id',
                value->'proposal'->>'quantity',value->>'attempts',value->>'prepared_at_ms',
                value->>'attempt_at_ms',value->>'updated_at_ms' FROM {REQUESTS}
                WHERE phase NOT IN ('OBSERVED','ABORTED_UNSENT') ORDER BY value->>'updated_at_ms' DESC LIMIT 20''').fetchall()
            result['requests']['unresolved_recent'] = [dict(request_id=_identifier(r[0],pattern=r'[0-9a-f]{64}'),
                bucket=_identifier(r[1],pattern=r'[0-9a-f]{64}'),phase=_identifier(r[2],pattern=r'[A-Z_]{1,80}'),
                operation=_identifier(r[3],pattern=r'[A-Z_]{1,80}'),leg=_identifier(r[4],pattern=r'[A-Z_]{1,80}'),
                card_id=_identifier(r[5],pattern=r'[0-9a-f]{64}'),quantity=_number(r[6]),
                attempts=int(r[7]) if r[7] in ('0','1') else None,
                **{key:_moment(int(v)) if isinstance(v,str) and v.isdigit() and len(v)<16 else None
                   for key,v in zip(('prepared_at_ms','attempt_at_ms','updated_at_ms'),r[8:])}) for r in rows]
        if present[EVENTS]:
            result['events'] = _counts(conn,EVENTS,'event')
            row = conn.execute(f'SELECT min(at_ms),max(at_ms) FROM {EVENTS}').fetchone()
            result['events'].update(first_at_ms=_moment(row[0]),last_at_ms=_moment(row[1]))
            rows = conn.execute(f'''SELECT bucket,revision,event,request_id,at_ms FROM {EVENTS}
                ORDER BY at_ms DESC,bucket,revision DESC LIMIT 40''').fetchall()
            result['events']['recent'] = [dict(bucket=_identifier(r[0],pattern=r'[0-9a-f]{64}'),revision=int(r[1]),
                event=_identifier(r[2],pattern=r'[A-Z_]{1,100}'),request_id=_identifier(r[3],pattern=r'[0-9a-f]{64}'),
                at_ms=_moment(r[4])) for r in rows]
    # A separate read-only accounting observation never spends a permit.
    if present[SCHEMA+'.request_budget_policy'] and present[SCHEMA+'.request_budget_tickets']:
        try:
            from .request_budget import Budget
            budget = Budget(journal)
            result['request_budget'] = {priority:budget.capacity(requested_weight=1,priority=priority)
                                        for priority in ('background','protection')}
        except Exception:
            result['request_budget'] = {'status':'READ_UNAVAILABLE'}
    return result


def application(environ, start_response):
    token = os.environ.get('HL_TESTNET_DIAGNOSTICS_TOKEN','')
    supplied = environ.get('HTTP_X_TESTNET_DIAGNOSTICS_TOKEN','')
    authorized = (os.environ.get('RENDER_SERVICE_ID') == SERVICE
        and os.environ.get('HL_TESTNET_RUNTIME_MODE','read_only') in MODES
        and isinstance(token,str) and TOKEN.fullmatch(token)
        and isinstance(supplied,str) and TOKEN.fullmatch(supplied)
        and not any(token == os.environ.get(name) for name in PRIVATE_TOKEN_NAMES)
        and environ.get('REQUEST_METHOD') == 'GET' and not environ.get('QUERY_STRING')
        and environ.get('CONTENT_LENGTH','') in ('','0')
        and not environ.get('HTTP_TRANSFER_ENCODING')
        and hmac.compare_digest(token,supplied))
    if not authorized:
        status, body = '404 Not Found', b'Not found\n'
    else:
        try:
            body = json.dumps(load_diagnostics(),separators=(',',':'),allow_nan=False).encode()
            if len(body)>MAX_BYTES:
                raise ValueError('DIAGNOSTICS_TOO_LARGE')
            status = '200 OK'
        except Exception:
            status, body = '503 Service Unavailable', b'Diagnostics unavailable\n'
    start_response(status,[('Content-Type','application/json' if status=='200 OK' else 'text/plain'),
        ('Content-Length',str(len(body))),('Cache-Control','no-store'),
        ('X-Content-Type-Options','nosniff'),('Referrer-Policy','no-referrer'),
        ('Content-Security-Policy',"default-src 'none'; frame-ancestors 'none'")])
    return [body]
