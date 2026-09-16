"""Explicit ONE technical Testnet cancellation rehearsal, not a bot signal.

The operator authorizes a separate fixture, never resets the earlier trade's
account reservation, and never changes its orders. Only an unused BTC market
is allowed. Fixture prices come from a live Testnet mark; the half-threshold is
already satisfied, so this tests actual cancellation, NOT a later price crossing.
Default deployments and public HTTP cannot start this task. No mainnet option.
"""
from datetime import datetime, timezone
from decimal import Decimal
import json
import os
import time

MODE = 'cancel_rehearsal_testnet_v1'
INSPECT = 'inspect_cancel_rehearsal_testnet_v1'
SERVICE = 'srv-dakptbh594qs7395460g'
RUN_ID = 'cancel-rehearsal-20260916-v1'
TABLE = 'hl_testnet_execution_v1.cancel_rehearsal_v1'
THRESHOLD = '1.5'
SYMBOL = 'BTC'


class RehearsalError(ValueError):
    """Fixed local codes, never keys, URLs or raw remote errors."""


def authorize(env, now=None):
    from .checks import decode
    from .source_window import timestamp
    from .postgres_journal import account_address
    if (env.get('RENDER_SERVICE_ID') != SERVICE
            or env.get('HL_TESTNET_RUNTIME_MODE') not in (MODE, INSPECT)
            or env.get('HL_TESTNET_JOURNAL_BACKEND') != 'staging_postgres_v1'
            or env.get('HL_TESTNET_EXIT_TYPE') != 'tp_limit_sl_market'
            or env.get('HL_TESTNET_PRICE_ROUNDING') != 'nearest-half-up-perp-v1'):
        raise RehearsalError('REHEARSAL_NOT_AUTHORIZED')
    raw = env.get('HL_TESTNET_REHEARSAL_TICKET', '')
    if not isinstance(raw, str) or not 1 <= len(raw) <= 1024:
        raise RehearsalError('EXACT_REHEARSAL_TICKET_REQUIRED')
    ticket = decode(raw)
    if (not isinstance(ticket, dict)
            or set(ticket) != {'run_id','technical_fixture','issued_at','expires_at'}
            or ticket['run_id'] != RUN_ID or ticket['technical_fixture'] is not True):
        raise RehearsalError('EXACT_REHEARSAL_TICKET_REQUIRED')
    issued, end = timestamp(ticket['issued_at']), timestamp(ticket['expires_at'])
    now = datetime.now(timezone.utc) if now is None else now
    if not 0 < (end-issued).total_seconds() <= 180:
        raise RehearsalError('INVALID_REHEARSAL_WINDOW')
    if env['HL_TESTNET_RUNTIME_MODE'] == MODE and not issued <= now < end:
        raise RehearsalError('REHEARSAL_AUTHORIZATION_EXPIRED')
    account = account_address(env.get('HL_TESTNET_ACCOUNT_ADDRESS'))
    agent = account_address(env.get('HL_TESTNET_AGENT_ADDRESS'))
    if account == agent:
        raise RehearsalError('DEDICATED_AGENT_REQUIRED')
    return account, agent


def fixture_source(mark, now=None):
    from .checks import number
    mark = number(mark)
    if mark <= 1:
        raise RehearsalError('INVALID_BTC_MARK')
    entry = mark * Decimal('0.99')
    # New synthetic source is explicitly labeled; no real alert is rewritten.
    return {'kind':'SIGNAL','event_id':RUN_ID,'symbol':SYMBOL,'side':'LONG',
            'entry':format(entry,'f'), 'stop':format(entry*Decimal('0.985'),'f'),
            'take_profit':format(entry*Decimal('1.015'),'f'),
            'at':(datetime.now(timezone.utc) if now is None else now).isoformat()}


def foreign_snapshot(http, account):
    import hyperliquid_testnet_executor as sender
    rows = http.info('frontendOpenOrders',user=account)
    state = http.info('clearinghouseState',user=account)
    if not isinstance(rows,list) or len(rows)>1000:
        raise RehearsalError('ACCOUNT_SNAPSHOT_UNAVAILABLE')
    position = sender._position(state,SYMBOL)
    if any(not isinstance(row,dict) or not isinstance(row.get('coin'),str) for row in rows):
        raise RehearsalError('ACCOUNT_SNAPSHOT_UNAVAILABLE')
    fields = ('coin','oid','cloid','side','limitPx','sz','origSz','reduceOnly','isTrigger','triggerPx')
    others = [{f:row.get(f) for f in fields} for row in rows if row['coin'] != SYMBOL]
    others.sort(key=lambda r:str(r['oid']))
    positions = {p['position']['coin']:p['position']['szi'] for p in state['assetPositions']
                 if p['position']['coin'] != SYMBOL and sender._num(p['position']['szi'],zero=True,signed=True) != 0}
    return {'orders':others,'positions':positions}, position, [r for r in rows if r['coin']==SYMBOL]


class TrialStore:
    """One additional named experiment. Original attempts are never modified."""
    def __init__(self,journal): self.journal=journal
    def initialize(self):
        from .postgres_journal import LOCK
        with self.journal._transaction() as conn:
            self.journal._ready(conn)
            conn.execute('SELECT pg_advisory_xact_lock(%s)',(LOCK,))
            conn.execute(f'''CREATE TABLE IF NOT EXISTS {TABLE} (
                account text PRIMARY KEY, run_id text NOT NULL,
                plan_key text UNIQUE NOT NULL REFERENCES hl_testnet_execution_v1.prepared(plan_key),
                manifest jsonb NOT NULL, digest text NOT NULL, nonce bigint NOT NULL,
                result jsonb, created_at timestamptz NOT NULL DEFAULT clock_timestamp())''')
            conn.execute(f'REVOKE ALL ON {TABLE} FROM PUBLIC')
    def load(self,account):
        from .postgres_journal import digest
        with self.journal._transaction() as conn:
            row=conn.execute(f'SELECT run_id,plan_key,manifest,digest,nonce,result FROM {TABLE} WHERE account=%s',(account,)).fetchone()
        if row is None:return None
        if row[0]!=RUN_ID or digest(row[2])!=row[3] or row[2]['record']['account']!=account:
            raise RehearsalError('TRIAL_MANIFEST_MISMATCH')
        return {'key':row[1],'manifest':row[2],'nonce':row[4],'result':row[5]}
    def reserve(self,account,key,manifest):
        from .postgres_journal import canonical,digest,LOCK
        with self.journal._transaction() as conn:
            self.journal._ready(conn)
            conn.execute('SELECT pg_advisory_xact_lock(%s)',(LOCK,))
            if conn.execute(f'SELECT 1 FROM {TABLE} WHERE account=%s',(account,)).fetchone():
                raise RehearsalError('REHEARSAL_ALREADY_RESERVED_NO_NEW_ENTRY')
            nonce=conn.execute('SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint').fetchone()[0]
            conn.execute(f'INSERT INTO {TABLE}(account,run_id,plan_key,manifest,digest,nonce) VALUES(%s,%s,%s,%s::jsonb,%s,%s)',
                         (account,RUN_ID,key,canonical(manifest),digest(manifest),nonce))
        return nonce
    def save(self,account,result):
        from .postgres_journal import canonical
        with self.journal._transaction() as conn:
            if conn.execute(f'UPDATE {TABLE} SET result=%s::jsonb WHERE account=%s',(canonical(result),account)).rowcount!=1:
                raise RehearsalError('TRIAL_RESERVATION_MISSING')


def finish_fixture(key,record,rule,journal,exchange,*,allow_cancel):
    """Observe a real unfilled entry before calling the existing cancel engine.

    An emergency cleanup of this artificial fixture is distinguished from a
    half-threshold pass. It never becomes a time cancellation trading policy.
    """
    from . import half_threshold_cancel as policy, pending_cancel_executor as cancel
    sample=exchange.observe()
    decision=policy.evaluate(rule,**sample)
    status=decision['decision']
    result={'entry_observed_unfilled':status in ('WAITING_BELOW_CANCEL_THRESHOLD','CANCEL_CANDIDATE_NOT_SENT'),
            'actual_half_threshold_observed':decision['threshold_crossed'],
            'test_passed':False,'status':status,'cleanup_only':False}
    if not allow_cancel:
        return result
    if decision['any_fill_observed']:
        checked=exchange.verify_original()
        result.update(status='FIXTURE_FILLED_NOT_A_CANCEL_TEST',protection_verified=checked.get('protection_active') is True)
        return result
    if not result['entry_observed_unfilled']:
        return result
    ops=cancel.Operations(journal)
    if decision['cancel_candidate']:
        policy.remember_crossing(journal,key,rule,decision)
        checked=cancel.run_once(key,record,rule,ops,exchange,threshold_seen=True,journal=journal)
        result.update(status=checked['status'])
        result['test_passed']=checked['status']=='CANCELLATION_VERIFIED_FLAT'
    else:
        # Keep an artificial test order from becoming an unattended strategy.
        # This is cleanup, NOT evidence that the half-threshold rule triggered.
        action=cancel.cancel_action(record)
        fresh,nonce=ops.reserve(key,'cancel',action)
        if fresh:
            try:exchange.write(action,nonce,phase='cancel')
            except cancel.CancelError:pass
        checked=cancel.settle(key,record,rule,ops,exchange,nonce)
        ops.save(key,'cancel',checked)
        result.update(status=checked,cleanup_only=True)
    return result


def inspect_fixture(record,rule,http):
    """Public receipt evidence only; never sign, recreate or cancel."""
    from . import half_threshold_cancel as policy
    import hyperliquid_testnet_executor as sender
    replies=[http.info('orderStatus',user=record['account'],oid=o['c']) for o in record['action']['orders']]
    snapshot,position,open_orders=foreign_snapshot(http,record['account'])
    summary=[]
    for role,expected,reply in zip(('ENTRY','TAKE_PROFIT','STOP'),record['action']['orders'],replies):
        actual=reply.get('order',{}).get('order',{}) if isinstance(reply,dict) else {}
        state=reply.get('order',{}).get('status','unknown') if isinstance(reply,dict) else 'unknown'
        if state not in {'open','filled','canceled','siblingFilledCanceled','reduceOnlyCanceled','rejected','scheduledCancel','marginCanceled'}:state='unknown'
        matched=(actual.get('cloid')==expected['c'] and actual.get('coin')==SYMBOL
                 and actual.get('side')==('B' if expected['b'] else 'A')
                 and actual.get('reduceOnly') is expected['r'])
        summary.append({'role':role,'status':state,'identity_matches':matched})
    parent=replies[0].get('order',{}).get('order',{}) if isinstance(replies[0],dict) else {}
    zero_fill=False
    try:zero_fill=policy.decimal(parent['origSz'])==policy.decimal(parent['sz'])==policy.decimal(rule['size'])
    except Exception:pass
    entry_canceled=(summary[0]['status']=='canceled' and summary[0]['identity_matches'] and zero_fill and position==0)
    return {'entry_cancelled_and_zero_fill_verified':entry_canceled,
            'fixture_position_zero':position==0,'fixture_open_orders':len(open_orders),
            'receipts':summary},snapshot


def run(env):
    result={'mode':'testnet_cancel_rehearsal','run_id':RUN_ID,'technical_fixture':True,
            'formula_alert':False,'threshold_pct':THRESHOLD,'cancel_move_pct':'0.75',
            'symbol':SYMBOL,'initial_threshold_already_met':True,
            'entry_batches_sent':0,'cancel_requests_sent':0,'test_passed':False,
            'status':'DISABLED','continuous_trading':False}
    if env.get('HL_TESTNET_RUNTIME_MODE') not in (MODE,INSPECT):return result
    trial=None;account=None;exchange=None;entry_http=None;reserved=False
    try:
        account,agent=authorize(env)
        from . import checks,price_precision as precision,half_threshold_cancel as policy,pending_cancel_executor as cancel
        from .postgres_journal import PostgresJournal,digest
        import hyperliquid_testnet_executor as sender
        journal=PostgresJournal.from_env(env)
        trial=TrialStore(journal);trial.initialize()
        previous=trial.load(account)
        http=sender.TestnetHTTP()
        if previous is not None:
            # Every reboot is read-only for an already-reserved entry. Do not
            # create another order or silently reset the original account lock.
            evidence,after=inspect_fixture(previous['manifest']['record'],previous['manifest']['rule'],http)
            result.update(evidence,status='REHEARSAL_RECONCILED_READ_ONLY',replayed=True,
                          unrelated_orders_unchanged=after==previous['manifest']['before'])
            return result
        if env['HL_TESTNET_RUNTIME_MODE']==INSPECT:
            raise RehearsalError('NO_REHEARSAL_TO_INSPECT')
        before,position,orders=foreign_snapshot(http,account)
        if position!=0 or orders:raise RehearsalError('BTC_ALREADY_IN_USE_NO_TEST_ORDER')
        # Do not overlap any pre-existing entry pending elsewhere in the account.
        if any(o['reduceOnly'] is not True for o in before['orders']):
            raise RehearsalError('OTHER_PENDING_ENTRY_REQUIRES_REVIEW')
        reader=precision.MetadataReader(checks.InfoReader())
        active=reader.read('activeAssetData',user=account,coin=SYMBOL)
        if active.get('coin')!=SYMBOL or checks.address(active.get('user'))!=account:
            raise RehearsalError('MARK_ACCOUNT_MISMATCH')
        source=fixture_source(active.get('markPx'))
        prepared=precision.prepare_signal(source,reader.read('meta'))
        action=sender.build_action(prepared['execution'],reader.read('meta'),account,exit_type='tp_limit_sl_market')
        entry=action['orders'][0]
        if not Decimal(10)<=Decimal(entry['p'])*Decimal(entry['s'])<=Decimal(1600):
            raise RehearsalError('FIXTURE_NOTIONAL_BOUND')
        plan={k:prepared['execution'][k] for k in ('symbol','side','entry','stop','take_profit')}
        budget=checks.run_check({'HL_TESTNET_RUNTIME_MODE':'read_only','HL_TESTNET_ACCOUNT_ADDRESS':account,
            'HL_TESTNET_AGENT_ADDRESS':agent,'HL_TESTNET_CHECK_SYMBOL':SYMBOL,'HL_TESTNET_CHECK_PLAN':json.dumps(plan)},client=reader)
        diag=budget.get('budget_diagnostics') or {}
        if (budget.get('status')!='PRECHECK_PASSED_NOT_ORDER_AUTHORIZATION'
                or diag.get('plan_sha256')!=precision.digest(plan) or diag.get('current_settings_passed') is not True):
            raise RehearsalError('FIXTURE_BUDGET_NOT_VERIFIED')
        record={'account':account,'prepared':prepared,'action':action}
        spec={'event_id':RUN_ID,'rule_id':'TECHNICAL_CANCEL_TEST','threshold_pct':THRESHOLD,'source_digest':policy.digest(source)}
        rule=policy.rule_from_record(record,spec)
        # Actual Testnet prices only; never inject a fabricated price observation.
        active=reader.read('activeAssetData',user=account,coin=SYMBOL)
        if not Decimal(rule['cancel_price'])<=checks.number(active.get('markPx'))<Decimal(plan['take_profit']):
            raise RehearsalError('LIVE_MARK_NOT_IN_REHEARSAL_RANGE')
        book=http._post('/info',{'type':'l2Book','coin':SYMBOL})
        if (book.get('coin')!=SYMBOL or type(book.get('time')) is not int
                or not 0<=sender.now_ms()-book['time']<=3000
                or checks.number(book['levels'][1][0]['px'])<=Decimal(entry['p'])*Decimal('1.002')):
            raise RehearsalError('ENTRY_NOT_SAFELY_PASSIVE')
        wallet=sender._wallet()
        if sender._account(wallet.address)!=agent:raise RehearsalError('WRONG_REHEARSAL_AGENT')
        role=http.info('userRole',user=agent)
        if (role.get('role')!='agent' or sender._account(role.get('data',{}).get('user'))!=account
                or http.info('userRole',user=account)!={'role':'user'}):
            raise RehearsalError('WRONG_TESTNET_ACCOUNT_MAPPING')
        key,_=journal.save_prepared(account,prepared)
        policy.register_rule(journal,key,rule)
        cancel.Operations(journal).initialize()
        # Recheck BTC and all unrelated records immediately before reservation.
        latest,position,orders=foreign_snapshot(http,account)
        if latest!=before or position!=0 or orders:
            raise RehearsalError('ACCOUNT_CHANGED_BEFORE_FIXTURE')
        authorize(env)
        nonce=trial.reserve(account,key,{'record':record,'rule':rule,'before':before})
        reserved=True
        if sender.now_ms()-nonce>5000:raise RehearsalError('FIXTURE_RESERVATION_EXPIRED')
        authorize(env)
        entry_http=sender.TestnetHTTP(allow_orders=True)
        try:entry_http._post('/exchange',sender._signed_body(wallet,action,nonce))
        except Exception:pass  # Entry may exist. Read back, never re-send.
        result['entry_batches_sent']=entry_http.order_attempts
        exchange=cancel.Exchange(record,agent)
        # Allow delayed receipt indexing, bounded by the entry signature window.
        for attempt in range(4):
            sample=exchange.observe()
            decision=policy.evaluate(rule,**sample)
            if decision['decision']!='ORDER_OR_MARK_REQUIRES_REVIEW':break
            time.sleep(0.5 if attempt<2 else max(0,min(31,(nonce+31000-sender.now_ms())/1000)))
        result.update(finish_fixture(key,record,rule,journal,exchange,allow_cancel=True))
        evidence,after=inspect_fixture(record,rule,http)
        result.update(evidence,unrelated_orders_unchanged=after==before)
        result['test_passed']=(result['test_passed'] and evidence['entry_cancelled_and_zero_fill_verified']
                               and evidence['fixture_open_orders']==0 and after==before)
        trial.save(account,result)
    except RehearsalError as exc:
        result.update(status=str(exc),test_passed=False)
    except Exception:
        result.update(status='REHEARSAL_REQUIRES_READBACK' if reserved else 'REHEARSAL_PRECHECK_FAILED_NO_SEND',test_passed=False)
    result['entry_batches_sent']=getattr(entry_http,'order_attempts',0)
    result['cancel_requests_sent']=getattr(exchange,'cancel_requests_attempted',0)
    result['exchange_write_attempts']=result['entry_batches_sent']+getattr(exchange,'requests_attempted',0)
    result['observed_at_utc']=datetime.now(timezone.utc).isoformat()
    if reserved and trial is not None:
        try:trial.save(account,result)
        except Exception:result['storage_result_requires_review']=True
    return result


def startup():
    print(json.dumps({'testnet_cancel_rehearsal':run(os.environ)},sort_keys=True),flush=True)
