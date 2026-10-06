"""Integrated READ-ONLY safety chain, called by the existing observation worker.

Public evidence -> immutable local ownership -> lifecycle -> pending-request
journal -> exit recovery / parent handoff -> display-only copy. No dispatch,
no simulated attempts in user storage, no policy from the app, no new timer.
A successful assessment is NOT permission to trade. Partial-fill policy remains
unapproved; the hypothetical cancel-and-rebuild policy is never selected here.
"""
from copy import deepcopy
from . import card_lifecycle as life, card_exit_recovery as recovery
from . import parent_exit_handoff as handoff, card_display_projection as display
from .card_recovery_journal import RecoveryJournal, SCHEMA as RECOVERY_SCHEMA
from .postgres_journal import JournalError, validate_prepared, validate_action

MODE = 'integrated_readonly_v1'
SERVICE = 'srv-dakptbh594qs7395460g'
VERSION = 'integrated-testnet-safety-review-v1'


class SafetyError(JournalError, life.LifecycleError):
    """Fixed codes only, never raw replies or configuration values."""


def config(env):
    if (env.get('HL_TESTNET_SAFETY_PIPELINE') != MODE
            or env.get('RENDER_SERVICE_ID') != SERVICE
            or env.get('HL_TESTNET_RUNTIME_MODE') != 'read_only'
            or env.get('HL_TESTNET_CARD_SYNC') != 'registered_readonly_v1'
            or env.get('HL_TESTNET_JOURNAL_BACKEND') != 'staging_postgres_v1'
            or env.get('HL_TESTNET_TWO_ACCOUNT_EXECUTION') != 'disabled'):
        raise SafetyError('INTEGRATED_SAFETY_REQUIRES_NO_ORDER_MODE')
    return True


def market_sample(reader, account, symbol, clock):
    """Timestamp the start of the public read, not its completion, conservatively."""
    life.address(account); life.ident(symbol, r'[A-Z][A-Z0-9]{0,19}')
    start = clock()
    raw = reader.read('activeAssetData', user=account, coin=symbol)
    end = clock()
    if not isinstance(raw, dict) or raw.get('coin', symbol) != symbol:
        raise SafetyError('SAFETY_MARK_SYMBOL_MISMATCH')
    life.number(raw.get('markPx'), positive=True)
    return dict(symbol=symbol, mark_price=raw['markPx'], at_ms=start, completed_at_ms=end)


def links_from_records(bindings, records):
    """Match a verified historical binding by BOTH local identity digests.

    Never adopt an order because its coin, direction or size happens to match.
    Unknown/new execution binding formats fail closed until their own registrar
    is implemented. Record-only alert cards are not execution registrations.
    """
    life.validate_bindings(bindings)
    by_id = {b['card_id']: b for b in bindings}
    links = {}
    for account, key, prepared, action in records:
        account = life.address(account)
        cid = life.digest(['legacy_executed_testnet', account, key])
        if cid not in by_id:
            continue
        b = by_id[cid]
        prepared = validate_prepared(prepared)
        action = validate_action(action, prepared, account)
        identity = dict(plan_key=key, prepared=prepared, action=action)
        source = prepared['execution']
        if (b['card_digest'] != life.digest(identity) or life.address(b['account']) != account
                or b['symbol'] != source['symbol'] or b['side'] != source['side']
                or b['prices'] != {k:source[k] for k in ('entry','stop','take_profit')}
                or life.number(b['planned_quantity']) != life.number(action['orders'][0]['s'])
                or action.get('grouping') != 'normalTpsl'
                or any(len(b['orders'][leg]) != 1 for leg in life.LEGS)
                or cid in links):
            raise SafetyError('IMMUTABLE_EXECUTION_LINK_REQUIRES_REVIEW')
        links[cid] = dict(card_id=cid, card_digest=b['card_digest'], grouping='normalTpsl',
            parent_oid=b['orders']['ENTRY'][0],
            children={leg:b['orders'][leg][0] for leg in recovery.EXITS})
    return links


def assess(bindings, snapshot, sample, links, pending, *, revision, now_ms):
    """One combined assessment, pure and unconditionally non-executable."""
    life.validate_bindings(bindings)
    account, symbol = life.validate_snapshot(snapshot)
    if any(life.address(b['account']) != account or b['symbol'] != symbol for b in bindings):
        raise SafetyError('SAFETY_ONE_REGISTERED_BUCKET_REQUIRED')
    if not isinstance(links, dict) or not set(links) <= {b['card_id'] for b in bindings}:
        raise SafetyError('SAFETY_REGISTRY_MISMATCH')
    life.shape(sample, 'symbol mark_price at_ms completed_at_ms')
    life.number(sample['mark_price'], positive=True)
    for t in (sample['at_ms'], sample['completed_at_ms'], now_ms): life.moment(t)
    if (sample['symbol'] != symbol
            or not sample['at_ms'] <= sample['completed_at_ms'] <= now_ms
            or not 0 <= now_ms-sample['at_ms'] <= 15000
            or abs(sample['at_ms']-snapshot['at_ms']) > 15000):
        raise SafetyError('SAFETY_MARK_NOT_FRESH_AND_COMPARABLE')
    view = life.review(bindings, snapshot, now_ms=now_ms)
    context = dict(symbol=symbol, mark_price=sample['mark_price'], at_ms=sample['at_ms'], cards={
        b['card_id']:dict(grouping='parent_linked' if b['card_id'] in links else 'unknown',
            requests={leg:dict(state='NONE',code=None) for leg in recovery.EXITS}) for b in bindings})
    independent = recovery.plan(bindings, snapshot, context, now_ms=now_ms)
    native = [handoff.assess(bindings,snapshot,context,links[cid],now_ms=now_ms,policy=handoff.PRESERVE)
              for cid in sorted(links)]
    reasons = set(independent['reasons']) | set(view['bucket_issues'])
    for item in native: reasons.update(item['reasons'])
    if len(links) != len(bindings): reasons.add('REGISTERED_ORDER_GROUPING_REQUIRED')
    partial = [c['card_id'] for c in view['cards']
               if life.number(c['entry_quantity']) > 0 and any(
                   o['oid'] in next(b for b in bindings if b['card_id']==c['card_id'])['orders']['ENTRY']
                   for o in snapshot['open_orders'])]
    if partial: reasons.add('PARTIAL_FILL_POLICY_PENDING_OWNERS_DECISION')
    pending_phase = None
    if pending is not None:
        if (not isinstance(pending,dict) or pending.get('bucket') != independent['bucket']
                or pending.get('phase') not in ('PREPARED_NOT_SENT','OUTCOME_UNKNOWN','WAITING_FOR_EVIDENCE',
                    'REJECTED_DO_NOT_RETRY','CONFLICT_RECONCILIATION_REQUIRED')):
            raise SafetyError('INVALID_PENDING_RECOVERY_EVIDENCE')
        pending_phase = pending['phase']
        reasons.add('DURABLE_REQUEST_MUST_BE_RESOLVED_BEFORE_NEW_WORK')
    correction_count = int(independent['next_step'] is not None)
    for item in native:
        correction_count += int(item['next_step'] is not None)
        correction_count += int(bool(item['recovery_proposal'] and item['recovery_proposal']['next_step']))
    if correction_count: reasons.add('CORRECTION_PLANNED_BUT_NOT_AUTHORIZED')
    if view['needs_review']: reasons.add('LIFECYCLE_REQUIRES_REVIEW')
    projection = display.project(bindings,snapshot,revision=revision,now_ms=now_ms)
    return dict(version=VERSION, status='REVIEW_REQUIRED' if reasons else 'CHECKED_NO_ACTION_NEEDED',
        environment='testnet', assessed_cards=len(view['cards']), linked_cards=len(links),
        observed_at_ms=snapshot['at_ms'], assessed_at_ms=now_ms, checked_revision=revision,
        requires_review=bool(reasons), reasons=sorted(reasons), pending_phase=pending_phase,
        partial_policy='AWAITING_OWNERS_DECISION', partial_cards=len(partial),
        recovery_state=independent['state'], handoff_states=[x['state'] for x in native],
        correction_proposals=correction_count, dispatch_enabled=False, execution_authorized=False,
        order_requests_sent=0, simulated_attempts_started=0, app_delivery_enabled=False,
        display_copy=projection)


def from_database(conn, bindings, snapshot, sample, *, revision, now_ms, journal):
    """Read local ownership and pending state under the SAME report transaction.

    All network reads occur before this function. Never hold a DB lock during a
    venue call. No reservation/reply/attempt is created by this read-only chain.
    """
    journal._ready(conn)
    store = RecoveryJournal(journal)
    store.ready(conn)
    account, symbol = life.validate_snapshot(snapshot)
    bucket = store.bucket(account, symbol)
    store._lock(conn, bucket)
    rows = conn.execute('''SELECT p.account,p.plan_key,p.manifest,a.action
        FROM hl_testnet_execution_v1.prepared p
        JOIN hl_testnet_execution_v1.attempts a ON a.plan_key=p.plan_key AND a.account=p.account
        WHERE p.account=%s AND p.manifest->'execution'->>'symbol'=%s LIMIT 129''', (account,symbol)).fetchall()
    if len(rows) > 128:
        raise SafetyError('OWNERSHIP_READ_BUDGET_REQUIRES_PAGINATION')
    links = links_from_records(bindings, rows)
    row = conn.execute(f'SELECT active_id FROM {RECOVERY_SCHEMA}.buckets WHERE bucket=%s', (bucket,)).fetchone()
    pending = store._read(conn,row[0]) if row and row[0] is not None else None
    return assess(bindings,snapshot,sample,links,pending,revision=revision,now_ms=now_ms)


def summary(report):
    """No full card data, addresses, order IDs, plans or display receipts in logs."""
    fields = ('version','status','assessed_cards','linked_cards','checked_revision','requires_review',
              'reasons','pending_phase','partial_policy','partial_cards','recovery_state','handoff_states',
              'correction_proposals','dispatch_enabled','execution_authorized','order_requests_sent',
              'simulated_attempts_started','app_delivery_enabled')
    return {k:deepcopy(report[k]) for k in fields}
