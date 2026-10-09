"""Observed account activity, separate from bot intent and trade allocation.

External intervention hands an existing bot market to its human operator. It
never manufactures a request, allocates a manual fill, changes an exit or grants
permission to trade. Account snapshots and bounded deduplicated warnings survive
restart in the ordinary execution transaction.
"""
from copy import deepcopy
from decimal import Decimal

from . import card_lifecycle as life, card_sync_evidence as sync

VERSION = 'external-account-observations-v1'
HUMAN = 'HUMAN_MANAGED_EXTERNAL_INTERVENTION'
MANUAL_CLOSED = 'MANUALLY_CLOSED'
OCCUPIED = 'EXTERNAL_MARKET_OCCUPIED_NO_NEW_ENTRY'
HISTORY_INTERVAL_MS = 30000
MAX_EVENTS = 512
MAX_RECENT_FILLS = 4096
MAX_ORDERS = 10000


class ExternalActivityError(ValueError):
    pass


def lane(account, symbol):
    return life.digest([account, symbol])


def managed(state, account, symbol):
    return lane(account, symbol) in state.get('external_activity', {}).get('human_managed', {})


def entry_reason(state, account, message):
    """Flat release permits a new signal, not an older queued or replayed one."""
    retired = state.get('external_activity', {}).get('retired_markets', {}).get(
        lane(account, message['symbol']))
    if retired is None:
        return None
    import approved_alert_contract as contract
    # Approved alerts can refer to a much older source observation. Their
    # approval is the new entry decision; receipt and heartbeat times are not.
    stamp = message['approved_at'] if contract.is_approved(message) else message['created_at']
    if contract.moment_ms(stamp) <= retired['confirmed_at_ms']:
        return 'NEW_ALERT_REQUIRED_AFTER_MANUAL_RELEASE'
    return None


def _active(state, account):
    from .experimental_execution_runtime import FINAL
    return {t['symbol'] for t in state['trades'].values()
            if t['account'] == account and t['phase'] not in FINAL}


def ownership(state, legacy, account, samples=()):
    """Uncertain bot submissions remain bot uncertainty, never manual evidence."""
    ids = set()
    cloids = {}
    for trade in state['trades'].values():
        if trade['account'] == account:
            ids.update(trade['orders'])
    for bucket in legacy.get(account, []):
        for binding in bucket['bindings']:
            ids.update(oid for values in binding['orders'].values() for oid in values)
    for request in state['requests'].values():
        p = request['proposal']
        if p['account'] != account or request['phase'] == 'ABORTED_UNSENT':
            continue
        if request.get('observed_oid'):
            ids.add(request['observed_oid'])
        if p['action']['type'] in ('order', 'batchModify'):
            from .filled_quantity_dispatch import requested_order
            cloids[requested_order(p['action'])['c']] = request
    # Collection-local verified lookup responses resolve freshly filled orders
    # before the next journal observation has assigned their exchange OID.
    from . import filled_quantity_dispatch as boundary
    for raw in samples:
        if not isinstance(raw, dict) or raw.get('status') != 'order':
            continue
        order = raw.get('order', {}).get('order', {})
        request = cloids.get(order.get('cloid'))
        if request is not None:
            try:
                ids.add(boundary.identity(raw, request, max(raw['order']['statusTimestamp'], request['attempt_at_ms'])))
            except (ValueError, KeyError, TypeError, AttributeError):
                # No fabricated ownership on a changed/unproved lookup.
                pass
    return ids, cloids


def normalize_order(account, raw, now):
    oid = raw.get('oid')
    if type(oid) is not int or not 0 < oid < 2**64:
        raise ExternalActivityError('EXTERNAL_ORDER_ID_INVALID')
    life.ident(raw.get('coin'), r'[A-Za-z0-9:@._-]{1,64}')
    result = dict(account=account, symbol=raw['coin'], order_id=str(oid),
                  observed_at_ms=now, status='OPEN')
    # Persist exactly what was observed. Missing optional exchange fields are
    # unknown; an order observation is never a certificate of stop protection.
    for source, target in (('side', 'side'), ('sz', 'remaining_quantity'),
            ('origSz', 'original_quantity'), ('limitPx', 'limit_price'),
            ('triggerPx', 'trigger_price'), ('orderType', 'order_type'),
            ('reduceOnly', 'reduce_only'), ('isTrigger', 'is_trigger'),
            ('isPositionTpsl', 'position_tpsl'), ('timestamp', 'created_at_ms'),
            ('cloid', 'client_order_id')):
        if source in raw:
            value = raw[source]
            if source in ('sz', 'origSz', 'limitPx', 'triggerPx'):
                life.number(value)
            elif source in ('reduceOnly', 'isTrigger', 'isPositionTpsl'):
                if type(value) is not bool:
                    raise ExternalActivityError('EXTERNAL_ORDER_TERMS_INVALID')
            elif source == 'timestamp':
                if life.moment(value) > now:
                    raise ExternalActivityError('EXTERNAL_ORDER_FROM_FUTURE')
            elif not isinstance(value, str) or len(value) > 160:
                raise ExternalActivityError('EXTERNAL_ORDER_TERMS_INVALID')
            result[target] = value
    if 'side' in result and result['side'] not in ('A', 'B'):
        raise ExternalActivityError('EXTERNAL_ORDER_SIDE_INVALID')
    return result


def cached_history(cache, account, start, end):
    """Return a complete already-funded account interval when one is available."""
    pages = cache.samples[0]
    class CachedOnlyReader:
        def read(self, kind, owner, *, oid=None, start=None, end=None):
            key = (kind, owner, str(oid) if oid is not None else None, start, end)
            if key not in pages:
                raise sync.SyncError('CACHED_HISTORY_INCOMPLETE')
            return deepcopy(pages[key])
    samples = []
    for key in pages:
        kind, owner, _, beginning, ending = key
        if kind == 'userFillsByTime' and owner == account and beginning <= start and ending >= end:
            try:
                # Use the existing bounded split/validation logic, but never
                # fetch a missing child or mix independent verification passes.
                rows = sync.history(CachedOnlyReader(), account, beginning, ending)
            except (ValueError, KeyError, TypeError):
                continue
            samples.append([r for r in rows if start <= r['time'] <= end])
    return min(samples, key=len) if samples else None


def owned_request_evidence(state, account, samples, observed, *, now_ms):
    """Resolve exact bot submissions without assigning external fills to them."""
    from . import filled_quantity_dispatch as wire
    result={}
    for rid,request in state['requests'].items():
        p=request['proposal']
        if p['account']!=account or request['phase'] in ('OBSERVED','ABORTED_UNSENT'):
            continue
        cancel=p['action']['type']=='cancel'
        if p['action']['type'] not in ('order','batchModify','cancel'):
            continue
        original=request
        if cancel:
            target=str(p['action']['cancels'][0]['o'])
            original=next((r for r in state['requests'].values() if r['proposal']['account']==account
                and r['proposal']['card_id']==p['card_id'] and r['proposal']['action']['type'] in ('order','batchModify')
                and r.get('observed_oid')==target),None)
            if original is None:continue
        expected=wire.requested_order(original['proposal']['action'])
        raw=next((r for r in samples if isinstance(r,dict) and r.get('status')=='order'
                  and r.get('order',{}).get('order',{}).get('cloid')==expected['c']),None)
        if raw is None:continue
        try:
            oid=wire.identity(raw,original,now_ms)
            envelope=raw['order'];status=envelope['status']
            if status not in {'open','filled'} | sync.CANCELED | sync.REJECTED:continue
            trade=state['trades'][p['card_id']]
            fills={f['fill_id']:{k:f[k] for k in ('fill_id','quantity','price','at_ms')}
                   for f in list(trade['entry_fills'].values())+list(trade['exit_fills'].values())
                   if f['order_id']==oid}
            for f in observed['fills']:
                if f['oid']!=oid:continue
                value={k:f[k] for k in ('fill_id','quantity','price','at_ms')}
                if (f['account']!=account or f['symbol']!=p['symbol']
                        or f['side']!=('B' if expected['b'] else 'A')
                        or f['at_ms']<original['attempt_at_ms']
                        or status!='open' and f['at_ms']>envelope['statusTimestamp']
                        or (f['fill_id'] in fills and fills[f['fill_id']]!=value)):
                    raise ExternalActivityError('BOT_FILL_FACT_CHANGED')
                if original['proposal']['leg']=='ENTRY' and (Decimal(f['price'])>Decimal(expected['p'])
                        if expected['b'] else Decimal(f['price'])<Decimal(expected['p'])):
                    raise ExternalActivityError('BOT_ENTRY_FILL_PRICE_INVALID')
                fills[f['fill_id']]=value
            quantity=sum((Decimal(f['quantity']) for f in fills.values()),Decimal(0))
            if quantity>Decimal(expected['s']):continue
            opened=next((o for o in observed['orders'] if o['order_id']==oid),None)
            if status=='open':
                if cancel:continue
                if opened is None or Decimal(opened.get('remaining_quantity','-1'))+quantity!=Decimal(expected['s']):continue
                final='OPEN'
            else:
                if opened is not None or not observed['history_complete']:continue
                final='FILLED' if status=='filled' else 'CANCELED' if status in sync.CANCELED else 'REJECTED'
                if final=='FILLED' and quantity!=Decimal(expected['s']) or final=='REJECTED' and quantity:continue
            result[rid]=dict(oid=oid,cloid=expected['c'],wire_order=deepcopy(expected),status=final,
                            at_ms=envelope['statusTimestamp'],fills=sorted(fills.values(),key=lambda f:(f['at_ms'],f['fill_id'])))
            if cancel:result[rid]['cancel_observation']=True
        except (ValueError,KeyError,TypeError):
            continue
    return result


def observe(state, legacy, inventories, cache, *, now_ms, fill_reader=None):
    """Collect facts; no journal write, no order sends, no adoption of risk.

    Existing funded fill intervals are reused. The caller may supply a bounded
    background reader for an otherwise uncovered account history interval.
    """
    old = state.get('external_activity', {}).get('accounts', {})
    result = dict(version=VERSION, accounts={}, human_managed={}, owned_request_observations={})
    for account, inventory in inventories.items():
        all_samples = [v for k,v in cache.samples[0].items() if k[0]=='orderStatus' and k[1]==account]
        ids, cloids = ownership(state, legacy, account, all_samples)
        before = old.get(account, {})
        # A recent immutable bot fill does not become a manual trade merely
        # because its completed owner was moved out of the active journal.
        ids.update(f['oid'] for f in before.get('fills',[]) if f.get('origin')=='BOT')
        active = _active(state, account)
        legacy_active=set()
        for bucket in legacy.get(account,[]):
            if not bucket.get('bindings'):continue
            evidence=bucket.get('evidence')
            if evidence is None:
                legacy_active.add(bucket['symbol']);continue
            review=life.review(bucket['bindings'],evidence['snapshot'],now_ms=now_ms)
            if bucket.get('pending') or review['bucket_issues'] or any(c['state'] not in ('CLOSED','CANCELED_WITHOUT_FILL') or c['issues'] for c in review['cards']):
                legacy_active.add(bucket['symbol'])
        uncertain = any(r['proposal']['account'] == account and r['phase'] not in ('OBSERVED', 'ABORTED_UNSENT')
                        for r in state['requests'].values())
        orders = []
        for raw in inventory['orders']:
            row = normalize_order(account, raw, now_ms)
            known = row['order_id'] in ids or row.get('client_order_id') in cloids
            if known:
                ids.add(row['order_id'])
            row['origin'] = 'BOT' if known else 'UNATTRIBUTED' if uncertain else 'EXTERNAL'
            orders.append(row)
            if row['origin'] == 'EXTERNAL' and row['symbol'] in active:
                result['human_managed'][lane(account, row['symbol'])] = dict(
                    account=account, symbol=row['symbol'], at_ms=now_ms,
                    reason='EXTERNAL_OPEN_ORDER', order_id=row['order_id'])
            if known and row['symbol'] in active:
                expected = next((t['orders'][row['order_id']]['wire_order']
                    for t in state['trades'].values() if t['account']==account
                    and row['order_id'] in t['orders']), None)
                if expected is not None:
                    trigger = expected['t'].get('trigger')
                    activated=(trigger is not None and trigger.get('tpsl')=='tp'
                        and any(sample.get('order',{}).get('status')=='triggered'
                            and str(sample.get('order',{}).get('order',{}).get('oid'))==row['order_id']
                            for sample in all_samples if isinstance(sample,dict)))
                    changed = (('side' in raw and raw['side'] != ('B' if expected['b'] else 'A'))
                        or ('reduceOnly' in raw and raw['reduceOnly'] is not expected['r'])
                        or ('origSz' in raw and Decimal(raw['origSz']) != Decimal(expected['s']))
                        or ('limitPx' in raw and Decimal(raw['limitPx']) != Decimal(expected['p']))
                        or (trigger is not None and not activated and 'triggerPx' in raw
                            and Decimal(raw['triggerPx']) != Decimal(trigger['triggerPx'])))
                    planned = any(r['proposal']['account']==account and r['proposal']['symbol']==row['symbol']
                        and r['phase'] not in ('OBSERVED','ABORTED_UNSENT')
                        and r['proposal']['action']['type']=='batchModify' for r in state['requests'].values())
                    if changed and not planned:
                        result['human_managed'][lane(account,row['symbol'])] = dict(
                            account=account,symbol=row['symbol'],at_ms=now_ms,
                            reason='OBSERVED_BOT_ORDER_TERMS_CHANGED',order_id=row['order_id'])
        positions = {p['position']['coin']: p['position']['szi']
                     for p in inventory['positions']['assetPositions']}
        for quantity in positions.values():
            life.number(quantity, signed=True)
        observed = dict(account=account, observed_at_ms=now_ms, orders=orders, bot_symbols=sorted(active|legacy_active),
                        positions=positions, fills=[], history_complete=False,
                        history_cursor_ms=before.get('history_cursor_ms'),
                        history_started_at_ms=before.get('history_started_at_ms', max(1, now_ms-sync.OVERLAP_MS)))
        cursor = before.get('history_cursor_ms')
        start = (max(1,cursor-sync.OVERLAP_MS) if cursor is not None else
                 before.get('history_started_at_ms',max(1,now_ms-sync.OVERLAP_MS)))
        due = cursor is None or now_ms-cursor >= HISTORY_INTERVAL_MS
        raw_fills = cached_history(cache, account, start, now_ms)
        if raw_fills is None and due and fill_reader is not None:
            # Bounded recovery: never silently advance over an unobserved gap.
            if now_ms-start > sync.DAY_MS:
                observed['history_error'] = 'EXTERNAL_HISTORY_GAP_REQUIRES_REVIEW'
            else:
                try:
                    raw_fills = sync.history(fill_reader, account, start, now_ms)
                except (ValueError, OSError) as exc:
                    code = str(exc)
                    observed['history_error'] = code if code and len(code)<140 and all(c.isupper() or c.isdigit() or c=='_' for c in code) else 'EXTERNAL_HISTORY_UNAVAILABLE'
        if raw_fills is not None:
            try:
                # Buffered stream executions are individual immutable facts.
                # A completed REST interval covering one must reproduce it;
                # an empty/delayed response cannot silently advance past it.
                prior_facts = [{k:v for k,v in f.items() if k!='origin'}
                               for f in before.get('fills', []) if start <= f['at_ms'] <= now_ms]
                symbols = {r['coin'] for r in raw_fills} | {f['symbol'] for f in prior_facts}
                normalized = [f for symbol in symbols
                              for f in sync.merge_fills([f for f in prior_facts if f['symbol']==symbol],
                                                       raw_fills, account, symbol, start, now_ms)]
                if len(normalized) > MAX_RECENT_FILLS:
                    raise ExternalActivityError('EXTERNAL_HISTORY_CAPACITY_REVIEW_REQUIRED')
                for fill in normalized:
                    fill['origin'] = 'BOT' if fill['oid'] in ids else 'UNATTRIBUTED' if uncertain else 'EXTERNAL'
                    if fill['origin'] == 'EXTERNAL' and fill['symbol'] in active:
                        # A pre-existing historical fill must not take over a
                        # later bot trade merely because overlap repeats it.
                        active_cids={t['cid'] for t in state['trades'].values() if t['account']==account
                                     and t['symbol']==fill['symbol'] and t['symbol'] in active
                                     and t['phase'] not in ('CLOSED','CANCELED_WITHOUT_FILL',MANUAL_CLOSED)}
                        starts = [r['attempt_at_ms'] for r in state['requests'].values()
                                  if r['proposal']['card_id'] in active_cids and r['proposal']['operation']=='ENTRY']
                        if starts and fill['at_ms'] >= min(starts):
                            result['human_managed'][lane(account, fill['symbol'])] = dict(
                                account=account, symbol=fill['symbol'], at_ms=now_ms,
                                reason='EXTERNAL_FILL', fill_id=fill['fill_id'])
                observed.update(fills=normalized, history_complete=True, history_cursor_ms=now_ms)
            except (ValueError, KeyError, TypeError) as exc:
                if isinstance(exc, sync.SyncError) and str(exc) in (
                        'FILL_FACT_CHANGED', 'PREVIOUS_FILL_MISSING_IN_OVERLAP'):
                    raise ExternalActivityError('EXTERNAL_'+str(exc)) from None
                observed['history_error'] = 'EXTERNAL_FILL_FACTS_REQUIRE_REVIEW'
        result['accounts'][account] = observed
        prior_fills={f['fill_id']:f for f in before.get('fills',[])}
        for f in observed['fills']:
            old=prior_fills.get(f['fill_id'])
            if old is not None and {k:v for k,v in old.items() if k!='origin'}!={k:v for k,v in f.items() if k!='origin'}:
                raise ExternalActivityError('EXTERNAL_FILL_FACT_CHANGED')
        proofs=owned_request_evidence(state,account,all_samples,observed,now_ms=now_ms)
        result['owned_request_observations'].update(proofs)
        remaining_uncertain=any(r['proposal']['account']==account and r['phase'] not in ('OBSERVED','ABORTED_UNSENT')
                                and rid not in proofs for rid,r in state['requests'].items())
        if uncertain and not remaining_uncertain:
            # Known bot outcome resolved from exact independent exchange facts;
            # other OIDs now have a genuine external origin, not a guessed one.
            for order in observed['orders']:
                if order['origin']=='UNATTRIBUTED':
                    order['origin']='EXTERNAL'
                    if order['symbol'] in active:
                        result['human_managed'][lane(account,order['symbol'])]=dict(account=account,
                            symbol=order['symbol'],at_ms=now_ms,reason='EXTERNAL_OPEN_ORDER',order_id=order['order_id'])
            for fill in observed['fills']:
                if fill['origin']=='UNATTRIBUTED':
                    fill['origin']='EXTERNAL'
                    if fill['symbol'] in active:
                        starts=[r['attempt_at_ms'] for r in state['requests'].values()
                            if r['proposal']['account']==account and r['proposal']['symbol']==fill['symbol']
                            and r['proposal']['operation']=='ENTRY'
                            and state['trades'][r['proposal']['card_id']]['phase'] not in ('CLOSED','CANCELED_WITHOUT_FILL',MANUAL_CLOSED)]
                        if not starts or fill['at_ms']<min(starts):continue
                        result['human_managed'][lane(account,fill['symbol'])]=dict(account=account,
                            symbol=fill['symbol'],at_ms=now_ms,reason='EXTERNAL_FILL',fill_id=fill['fill_id'])
        # Explicit canceled status of a previously working order, without a
        # bot cancel/replace intent, is user-controlled work while exposure
        # remains. System-specific rejection/cancellation statuses retain their
        # ordinary recovery behavior. The origin cannot be proved from status.
        for raw in all_samples:
            envelope = raw.get('order', {}) if isinstance(raw, dict) else {}
            order = envelope.get('order', {})
            oid = str(order.get('oid', ''))
            symbol = order.get('coin')
            if envelope.get('status') != 'canceled' or symbol not in active or not Decimal(positions.get(symbol,'0')):
                continue
            owner = next((t for t in state['trades'].values() if t['account']==account
                          and oid in t['orders'] and t['orders'][oid]['status']=='OPEN'), None)
            if owner is None or owner['order_legs'].get(oid)=='ENTRY':
                continue
            planned = any(r['proposal']['account']==account and r['phase']!='ABORTED_UNSENT'
                and ((r['proposal']['action']['type']=='cancel'
                      and any(str(c['o'])==oid for c in r['proposal']['action']['cancels']))
                     or (r['proposal']['action']['type']=='batchModify'
                         and any(str(m['oid'])==oid for m in r['proposal']['action']['modifies'])))
                for r in state['requests'].values())
            if not planned:
                result['human_managed'][lane(account,symbol)] = dict(account=account,symbol=symbol,
                    at_ms=now_ms,reason='OBSERVED_ORDER_CANCELED_WITHOUT_BOT_CANCEL',order_id=oid)
    # Handoff is durable, including after the account position becomes zero.
    for key, fact in state.get('external_activity', {}).get('human_managed', {}).items():
        result['human_managed'].setdefault(key, deepcopy(fact))
    return result


def _merge_recent_fills(previous, incoming, cursor, *, preserve_origin=False):
    """Merge immutable facts without treating deltas as complete history."""
    values = {f['fill_id']: deepcopy(f) for f in previous}
    for fill in incoming:
        old = values.get(fill['fill_id'])
        if old is not None and {k:v for k,v in old.items() if k!='origin'} != {
                k:v for k,v in fill.items() if k!='origin'}:
            raise ExternalActivityError('EXTERNAL_FILL_FACT_CHANGED')
        saved = deepcopy(fill)
        if preserve_origin and old is not None and old['origin'] in ('BOT', 'EXTERNAL'):
            saved['origin'] = old['origin']
        values[fill['fill_id']] = saved
    retained = sorted((f for f in values.values() if f['at_ms'] >= (cursor or 0)-2*sync.OVERLAP_MS),
                      key=lambda f:(f['at_ms'],f['fill_id']))
    if len(retained) > MAX_RECENT_FILLS:
        raise ExternalActivityError('EXTERNAL_FILL_RETENTION_CAPACITY_REVIEW_REQUIRED')
    return retained


def record_fill_deltas(state, rows, *, now_ms):
    """Persist received executions without inventing an account inventory.

    The caller proves BOT ownership or leaves a fill UNATTRIBUTED. Existing
    complete REST clocks and observations remain untouched; an identical replay
    cannot downgrade an attribution already established by reconciliation.
    This pure reducer belongs inside the existing execution-state transaction.
    """
    life.moment(now_ms)
    if not isinstance(rows, (list, tuple)):
        raise ExternalActivityError('EXTERNAL_FILL_DELTA_BATCH_INVALID')
    fields = {'account', 'symbol', 'oid', 'fill_id', 'quantity', 'price',
              'fee', 'fee_token', 'side', 'at_ms', 'origin'}
    grouped = {}
    for fill in rows:
        if not isinstance(fill, dict) or set(fill) != fields:
            raise ExternalActivityError('EXTERNAL_FILL_DELTA_FACTS_INVALID')
        account = fill['account']
        if account not in state['routes'].values() or life.address(account) != account:
            raise ExternalActivityError('EXTERNAL_OBSERVATION_ACCOUNT_INVALID')
        life.ident(fill['symbol'], r'[A-Za-z0-9:@._-]{1,64}')
        life.ident(fill['oid'], r'[1-9][0-9]{0,19}')
        life.ident(fill['fill_id'], r'hl:(0|[1-9][0-9]*)')
        if (int(fill['oid']) >= 2**64 or fill['side'] not in ('A', 'B')
                or fill['origin'] not in ('BOT', 'UNATTRIBUTED')):
            raise ExternalActivityError('EXTERNAL_FILL_DELTA_FACTS_INVALID')
        life.number(fill['quantity'], positive=True)
        life.number(fill['price'], positive=True)
        life.number(fill['fee'], signed=True)
        life.ident(fill['fee_token'])
        if life.moment(fill['at_ms']) > now_ms:
            raise ExternalActivityError('EXTERNAL_FILL_FROM_FUTURE')
        grouped.setdefault(account, []).append(fill)
    if not grouped:
        return
    existing = state.get('external_activity', {})
    if existing and existing.get('version') != VERSION:
        raise ExternalActivityError('EXTERNAL_OBSERVATION_VERSION_INVALID')
    accounts = existing.get('accounts', {})
    updates = {}
    for account, fills in grouped.items():
        prior = accounts.get(account)
        if prior is None:
            saved = dict(account=account, fills=[], history_cursor_ms=None,
                         history_complete=False, history_started_at_ms=min(
                             max(1, now_ms-sync.OVERLAP_MS), min(f['at_ms'] for f in fills)))
        else:
            if prior.get('account') != account:
                raise ExternalActivityError('EXTERNAL_OBSERVATION_ACCOUNT_INVALID')
            saved = deepcopy(prior)
            if saved.get('history_cursor_ms') is None:
                # A later delivery can precede the first uncompleted interval.
                saved['history_started_at_ms'] = min(saved.get('history_started_at_ms', now_ms),
                                                       min(f['at_ms'] for f in fills))
        saved['fills'] = _merge_recent_fills(saved.get('fills', []), fills,
            saved.get('history_cursor_ms'), preserve_origin=True)
        updates[account] = saved
    # Validate every account before changing any part of the supplied state.
    value = state.setdefault('external_activity', dict(version=VERSION, accounts={}, human_managed={},
                                                     events=[], evicted_events=0))
    value['accounts'].update(updates)


def apply(state, observation):
    """Idempotent journal reducer; never rewrites attributable bot quantities."""
    if not observation:
        return
    if observation.get('version') != VERSION:
        raise ExternalActivityError('EXTERNAL_OBSERVATION_VERSION_INVALID')
    value = state.setdefault('external_activity', dict(version=VERSION, accounts={}, human_managed={},
                                                     events=[], evicted_events=0))
    event_ids = {e['event_id'] for e in value['events']}
    def event(kind, fact):
        row = dict(kind=kind, **deepcopy(fact))
        row['event_id'] = life.digest([kind, {k:v for k,v in fact.items() if k != 'observed_at_ms'}])
        if row['event_id'] not in event_ids:
            row['notification_status'] = 'AVAILABLE_IN_JOURNAL'
            value['events'].append(row); event_ids.add(row['event_id'])
    for account, current in observation['accounts'].items():
        if account not in state['routes'].values() or current['account'] != account:
            raise ExternalActivityError('EXTERNAL_OBSERVATION_ACCOUNT_INVALID')
        prior = value['accounts'].get(account, {})
        if current['observed_at_ms'] < prior.get('observed_at_ms', 0):
            raise ExternalActivityError('EXTERNAL_OBSERVATION_REGRESSION')
        previous_orders = {r['order_id']:r for r in prior.get('orders', [])}
        for row in current['orders']:
            if row['origin'] != 'EXTERNAL':
                continue
            old = previous_orders.get(row['order_id'])
            facts = lambda r:{k:v for k,v in r.items() if k != 'observed_at_ms'}
            if old is None or facts(old) != facts(row):
                event('EXTERNAL_OPEN_ORDER_OBSERVED' if old is None else 'EXTERNAL_ORDER_CHANGED', row)
        current_ids = {r['order_id'] for r in current['orders']}
        for oid, row in previous_orders.items():
            if row['origin'] == 'EXTERNAL' and oid not in current_ids:
                event('EXTERNAL_ORDER_NO_LONGER_OPEN', dict(account=account, symbol=row['symbol'],
                      order_id=oid, observed_at_ms=current['observed_at_ms'],
                      at_ms=current['observed_at_ms'], status='NO_LONGER_OPEN_OUTCOME_UNASSIGNED'))
        old_fills = {f['fill_id']:f for f in prior.get('fills', [])}
        cursor = current.get('history_cursor_ms') or prior.get('history_cursor_ms') or 0
        retained = _merge_recent_fills(prior.get('fills', []), current['fills'], cursor)
        for fill in current['fills']:
            previous = old_fills.get(fill['fill_id'])
            if fill['origin'] == 'EXTERNAL' and (previous is None or previous['origin'] != 'EXTERNAL'):
                event('EXTERNAL_FILL_OBSERVED', fill)
            old_fills[fill['fill_id']] = deepcopy(fill)
        saved = deepcopy(current); saved['fills'] = retained
        if not current['history_complete']:
            saved['history_cursor_ms'] = prior.get('history_cursor_ms')
        value['accounts'][account] = saved
        symbols = set(prior.get('positions', {})) | set(current['positions'])
        for symbol in symbols:
            before = prior.get('positions', {}).get(symbol, '0')
            after = current['positions'].get(symbol, '0')
            if Decimal(before) != Decimal(after) and (symbol not in _active(state, account) or managed(state, account, symbol)):
                event('ACCOUNT_POSITION_CHANGED', dict(account=account, symbol=symbol,
                      previous_quantity=before, actual_quantity=after, at_ms=current['observed_at_ms']))
    for key, fact in observation.get('human_managed', {}).items():
        if key != lane(fact['account'], fact['symbol']) or fact['account'] not in state['routes'].values():
            raise ExternalActivityError('EXTERNAL_HANDOFF_SCOPE_INVALID')
        if key not in value['human_managed']:
            cards=sorted(cid for cid,t in state['trades'].items() if t['account']==fact['account']
                         and t['symbol']==fact['symbol'] and t['phase'] not in ('CLOSED','CANCELED_WITHOUT_FILL',MANUAL_CLOSED))
            value['human_managed'][key] = dict(**deepcopy(fact), status=HUMAN, automatic_resume=False,card_ids=cards)
            for cid in cards:
                state['trades'][cid]['manual_management']=dict(status=HUMAN,started_at_ms=fact['at_ms'],
                    reason=fact['reason'],accounting_allocation='EXTERNAL_ACTIVITY_UNALLOCATED')
            event(HUMAN, dict(**deepcopy(fact), automatic_resume=False,
                             action='HUMAN_MANAGES_POSITION_AND_EXITS_BOT_SENDS_PAUSED_FOR_THIS_COIN'))
    # Handoff does not strand a bot request already sent. Resolve only exact
    # independently observed own order IDs/fills; external fills never enter it.
    for rid,row in observation.get('owned_request_observations',{}).items():
        request=state['requests'].get(rid)
        if request is None:raise ExternalActivityError('OBSERVED_BOT_REQUEST_MISSING')
        p=request['proposal']
        if not managed(state,p['account'],p['symbol']):continue
        if request['phase'] in ('OBSERVED','ABORTED_UNSENT'):continue
        trade=state['trades'][p['card_id']]
        recorded={k:deepcopy(v) for k,v in row.items() if k!='cancel_observation'}
        leg=trade['order_legs'].get(row['oid'],p['leg'])
        trade['orders'][row['oid']]=recorded;trade['order_legs'][row['oid']]=leg
        target=trade['entry_fills'] if leg=='ENTRY' else trade['exit_fills']
        for f in row['fills']:target[f['fill_id']]=dict(**deepcopy(f),order_id=row['oid'])
        request['phase']='OBSERVED';request['observed_oid']=row['oid']
        event('BOT_REQUEST_OBSERVED_DURING_HUMAN_MANAGEMENT',dict(account=p['account'],symbol=p['symbol'],
            request_id=rid,order_id=row['oid'],status=row['status'],at_ms=row['at_ms']))
    # Scope the handoff to this position. Two distinct ordinary observations
    # prove actual flatness and absence of ALL coin orders before a later
    # independent alert may use this market. No resumed management of open risk.
    for key,handoff in list(value['human_managed'].items()):
        account,symbol=handoff['account'],handoff['symbol']
        current=observation['accounts'].get(account)
        if current is None:continue
        at=current['observed_at_ms']
        for cid in handoff.get('card_ids',[]):
            if cid in state['trades']:
                state['trades'][cid]['manual_management'].update(
                    actual_position_quantity=current['positions'].get(symbol,'0'),observed_at_ms=at,
                    current_orders=[deepcopy(o) for o in current['orders'] if o['symbol']==symbol])
        pending=any(r['proposal']['account']==account and r['proposal']['symbol']==symbol
                    and r['phase'] not in ('OBSERVED','ABORTED_UNSENT') for r in state['requests'].values())
        flat=(current['history_complete'] and Decimal(current['positions'].get(symbol,'0'))==0
              and not any(o['symbol']==symbol for o in current['orders']) and not pending)
        if not flat:
            handoff.pop('first_flat_observation_ms',None)
            continue
        first=handoff.setdefault('first_flat_observation_ms',at)
        if at<=first:continue
        proof=dict(account=account,symbol=symbol,first_observation_ms=first,confirmed_at_ms=at,
                   actual_position_quantity='0',open_order_count=0,pending_bot_requests=0,
                   observation_digest=life.digest(current))
        for cid in handoff.get('card_ids',[]):
            trade=state['trades'].get(cid)
            if trade is None:continue
            prior_snapshot=state.get('collector_checkpoints',{}).get(key,{})
            facts={f['fill_id']:deepcopy(f) for f in prior_snapshot.get('fills',[])}
            for f in value['accounts'][account].get('fills',[]):
                if f['symbol']==symbol:
                    facts[f['fill_id']]={k:deepcopy(v) for k,v in f.items() if k!='origin'}
            trade['manual_closure_snapshot']=dict(environment='testnet',account=account,symbol=symbol,
                at_ms=at,history_complete=True,orders_complete=True,position_quantity='0',
                fills=sorted(facts.values(),key=lambda f:(f['at_ms'],f['fill_id'])),open_orders=[],
                terminal_orders=deepcopy(prior_snapshot.get('terminal_orders',[])))
            trade['phase']=MANUAL_CLOSED
            trade['manual_closure']=deepcopy(proof)
            trade['manual_management'].update(status=MANUAL_CLOSED,actual_position_quantity='0',
                                              observed_at_ms=at,current_orders=[])
            state['events'].append(dict(kind=MANUAL_CLOSED,at_ms=at,occurrence_id=cid,
                                        accounting_allocation='EXTERNAL_ACTIVITY_UNALLOCATED'))
        value.setdefault('retired_markets',{})[key]=proof
        value['human_managed'].pop(key)
        state.get('snapshots',{}).pop(key,None)
        state.get('collector_checkpoints',{}).pop(key,None)
        state.get('blocked_lanes',{}).pop(key,None)
        event('MANUAL_POSITION_FLAT_CONFIRMED',proof)
    if len(value['events']) > MAX_EVENTS:
        discarded = len(value['events'])-MAX_EVENTS
        value['events'] = value['events'][-MAX_EVENTS:]
        value['evicted_events'] += discarded
    value['observation_errors']=deepcopy(observation.get('errors',{}))


def closure_verified(trade):
    proof=trade.get('manual_closure',{})
    return (trade.get('phase')==MANUAL_CLOSED and proof.get('account')==trade['account']
        and proof.get('symbol')==trade['symbol']
        and type(proof.get('first_observation_ms')) is int
        and type(proof.get('confirmed_at_ms')) is int
        and 0<=proof['first_observation_ms']<proof['confirmed_at_ms']
        and proof.get('actual_position_quantity')=='0' and proof.get('open_order_count')==0
        and proof.get('pending_bot_requests')==0 and isinstance(proof.get('observation_digest'),str))


def review_inventory(account, buckets, orders, positions, *, role, observation=None, target_symbol=None):
    """Strict bot checks remain; externally observed markets are separate scope."""
    from .long_stream_runtime import _validate_account_inventory
    if observation is None:
        return _validate_account_inventory(account, buckets, orders, positions, role=role)
    current = observation.get('accounts', {}).get(account)
    if current is None:
        return _validate_account_inventory(account, buckets, orders, positions, role=role)
    human = {v['symbol'] for v in observation.get('human_managed', {}).values() if v['account']==account}
    known_symbols = set(current['bot_symbols'])
    external = {r['symbol'] for r in current['orders'] if r['origin']=='EXTERNAL'}
    external |= {s for s,q in current['positions'].items() if Decimal(q)!=0 and s not in known_symbols}
    excluded = human | (external-known_symbols)
    if target_symbol in excluded or target_symbol in external:
        raise ExternalActivityError(HUMAN if target_symbol in human else OCCUPIED)
    # The certificate is the exact current account inventory, not an arbitrary
    # list of order IDs to ignore. No pending bot attempt is bypassed.
    normalized = [normalize_order(account, r, current['observed_at_ms']) for r in orders]
    if (len({r['order_id'] for r in normalized}) != len(normalized)
            or len({r['position']['coin'] for r in positions['assetPositions']}) != len(positions['assetPositions'])):
        raise ExternalActivityError('EXTERNAL_INVENTORY_DUPLICATE_IDENTITY')
    actual = [{k:v for k,v in r.items() if k!='origin'} for r in current['orders']]
    if normalized != actual or current['positions'] != {r['position']['coin']:r['position']['szi'] for r in positions['assetPositions']}:
        raise ExternalActivityError('EXTERNAL_INVENTORY_CERTIFICATE_CHANGED')
    if any(s['pending'] is not None for s in buckets):
        raise ExternalActivityError('UNRESOLVED_ACCOUNT_REQUEST_NO_NEW_ENTRY')
    return _validate_account_inventory(account, [b for b in buckets if b['symbol'] not in excluded],
        [o for o in orders if o['coin'] not in excluded],
        dict(positions, assetPositions=[p for p in positions['assetPositions'] if p['position']['coin'] not in excluded]), role=role)
