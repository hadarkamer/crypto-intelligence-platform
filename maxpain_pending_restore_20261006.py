"""Disabled-by-default recovery of 13 specific cancelled notification plans.

Pure audit/transform plus bounded public-price reads and the existing atomic
notification-state store; no Telegram sends, exchange orders, or general admin API.
"""
from copy import deepcopy
from decimal import Decimal
import hashlib
import json
import math

from sol_proximity_experimental_signal import MAX_ACTIVE

MINUTE = 60_000
BATCH = 'USER_AUTHORIZED_20261006_RESTORE_13_SOURCE_REPLACED'
TERMINAL = 'CANCELLED_SOURCE_REPLACED'


def digest(obj):
    return hashlib.sha256(json.dumps(obj, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()).hexdigest()


def near(a, b):
    a, b = Decimal(str(a)), Decimal(str(b))
    return abs(a-b) <= Decimal('.002')*min(a, b)


def prices_audit(plan, bars, start_ms, end_ms):
    """Closed 1m candles; any touched entry means the original pending is no longer recoverable."""
    start = start_ms//MINUTE*MINUTE
    selected = [b for b in bars if start <= int(b[0]) < end_ms]
    expected = list(range(start, end_ms, MINUTE))
    if [int(b[0]) for b in selected] != expected:
        return {'ok':False, 'reason':'INCOMPLETE_MINUTE_EVIDENCE', 'expected':len(expected), 'observed':len(selected)}
    a, entry, target, stop = (plan[k] for k in ('direction','entry_price','target_price','stop_price'))
    result = {'ok':True, 'expected':len(expected), 'observed':len(selected), 'first_entry_ms':None, 'first_target_ms':None, 'first_stop_ms':None}
    for t, o, h, l, c in selected:
        if any(not isinstance(x, (int,float)) or not math.isfinite(x) or x <= 0 for x in (o,h,l,c)) or not l <= min(o,c) <= max(o,c) <= h:
            return {'ok':False, 'reason':'INVALID_OHLC'}
        if (l <= entry if a == 1 else h >= entry) and result['first_entry_ms'] is None:
            result['first_entry_ms'] = t
        if (h >= target if a == 1 else l <= target) and result['first_target_ms'] is None:
            result['first_target_ms'] = t
        if (l <= stop if a == 1 else h >= stop) and result['first_stop_ms'] is None:
            result['first_stop_ms'] = t
    result['last_close'] = selected[-1][4] if selected else None
    result['bars_sha256'] = digest(selected)
    return result


def audit_state(state, bars_by_source, now_ms, *, screening_only=False):
    """bars_by_source contains old original route and new Hyperliquid route; shared route is allowed."""
    migration = state['source_migration']
    coin = migration['coin']
    cutoff = now_ms//MINUTE*MINUTE
    active = deepcopy(state.get('active', []) + state.get('legacy_source_state', {}).get('active', []))
    prior = state.get('manual_pending_restorations', {}).get(BATCH, {})
    out = {'batch_id':BATCH, 'coin':coin, 'audited_ms':now_ms, 'cutoff_ms':cutoff,
           'state_sha256':digest(state), 'config_sha256':state['config_sha256'],
           'bar_cursor_ms':state['bar_cursor_ms'], 'screening_only':screening_only, 'rows':[]}
    candidates = [p for p in state['history'] if p.get('status') == TERMINAL and p.get('terminal_ms') == migration['migrated_ms']]
    for plan in sorted(candidates, key=lambda p:(p['arm_ms'], p['position_id'])):
        row = {'position_id':plan['position_id'], 'coin':coin, 'original':deepcopy(plan), 'eligible':False, 'reasons':[], 'sources':{}}
        reasons = row['reasons']
        pid = plan['position_id']
        if pid in prior:
            reasons.append('ALREADY_RESTORED')
        if plan['expires_ms'] <= (now_ms//MINUTE+1)*MINUTE:
            reasons.append('ORIGINAL_TTL_EXPIRED')
        if plan.get('fill_ms') is not None or plan.get('fill_price') is not None:
            reasons.append('NOT_AN_UNFILLED_PLAN')
        if plan.get('coin') != coin:
            reasons.append('WRONG_COIN')
        if any(p['position_id'] == pid for p in active):
            reasons.append('ALREADY_ACTIVE')
        if any(i.get('position_id') == pid for i in state.get('intents', [])):
            reasons.append('DELIVERY_INTENT_EXISTS')
        if any(p.get('position_id') == pid and p.get('status') != TERMINAL for p in state['history']):
            reasons.append('LATER_TERMINAL_RECORD_EXISTS')
        if state['config_sha256'] != migration['to_config_sha256']:
            reasons.append('CURRENT_CONFIG_MISMATCH')
        if not screening_only and state['bar_cursor_ms'] != cutoff-MINUTE:
            reasons.append('LIVE_CURSOR_NOT_CURRENT')
        for source in sorted(set((migration['legacy_price_source'], migration['new_price_source']))):
            audit = prices_audit(plan, bars_by_source.get(source, []), plan['arm_ms'], cutoff)
            row['sources'][source] = audit
            if not audit['ok']:
                reasons.append(source+':'+audit['reason'])
            else:
                for key, label in [('first_target_ms','TARGET_ALREADY_TOUCHED'),('first_entry_ms','ENTRY_ALREADY_REACHED'),('first_stop_ms','STOP_ALREADY_CROSSED')]:
                    if audit[key] is not None:
                        reasons.append(source+':'+label)
        neighbors = [dict(position_id=p['position_id'],target_price=p['target_price'],status=p['status'],timeframe=p.get('timeframe')) for p in active if p['position_id'] != pid and near(p['target_price'],plan['target_price'])]
        row['nearby_active'] = neighbors
        if len(active) >= MAX_ACTIVE:
            reasons.append('OPERATIONAL_CAPACITY_REACHED')
        if neighbors:
            reasons.append('NEARBY_ACTIVE_TARGET_REQUIRES_CURRENT_GROWTH_PROOF')
        row['eligible'] = not reasons
        if row['eligible']:
            # Protect later batch members from duplicating an accepted pending target.
            active.append(dict(plan,status='PENDING'))
        out['rows'].append(row)
    out['summary'] = {'cancelled_count':len(candidates), 'eligible':sum(r['eligible'] for r in out['rows']), 'ineligible':sum(not r['eligible'] for r in out['rows'])}
    out['audit_sha256'] = digest(out)
    return out


def restore_pure(state, audit, now_ms):
    """Apply only after root locks row and freshly verifies exact state+cursor. Returns copy."""
    if audit.get('screening_only'):
        raise ValueError('Screening-only audit cannot authorize mutation')
    if state.get('manual_pending_restorations', {}).get(BATCH):
        # A completed batch is idempotent, never reactivated twice.
        return deepcopy(state), []
    if digest(state) != audit['state_sha256']:
        raise ValueError('State changed since audit; re-audit required')
    if now_ms < audit['audited_ms'] or now_ms//MINUTE*MINUTE != audit['cutoff_ms']:
        raise ValueError('Audit minute changed; re-audit required')
    if digest({k:v for k,v in audit.items() if k != 'audit_sha256'}) != audit['audit_sha256']:
        raise ValueError('Audit hash mismatch')
    result = deepcopy(state)
    receipts = {}
    for row in audit['rows']:
        if not row['eligible']:
            continue
        p = deepcopy(row['original'])
        if len(result['active'])+len(result.get('legacy_source_state',{}).get('active',[])) >= MAX_ACTIVE:
            raise ValueError('Operational capacity reached')
        if p['expires_ms'] <= now_ms:
            raise ValueError('Original TTL expired')
        if any(x['position_id'] == p['position_id'] or near(x['target_price'],p['target_price']) for x in result['active']+result.get('legacy_source_state',{}).get('active',[])):
            raise ValueError('Active state conflict')
        p['status'] = 'PENDING'
        original_arm_ms = p['arm_ms']
        p['arm_ms'] = (now_ms//MINUTE+1)*MINUTE
        p.pop('terminal_ms', None)
        p['manual_source_restore'] = {'batch_id':BATCH, 'restored_ms':now_ms,
            'cancelled_ms':row['original']['terminal_ms'], 'audit_sha256':audit['audit_sha256'],
            'original_arm_ms':original_arm_ms, 'effective_resume_ms':p['arm_ms'],
            'original_price_source':state['source_migration']['legacy_price_source'],
            'monitor_price_source':state['source_migration']['new_price_source'],
            'policy':'PRESERVE_FROZEN_LEVELS_ORIGINAL_TTL_PROSPECTIVE_ONLY'}
        result['active'].append(p)
        receipts[p['position_id']] = deepcopy(p['manual_source_restore'])
    if receipts:
        result.setdefault('manual_pending_restorations', {})[BATCH] = receipts
        result['counts']['MANUAL_SOURCE_PENDING_RESTORED'] = result['counts'].get('MANUAL_SOURCE_PENDING_RESTORED',0)+len(receipts)
    return result, sorted(receipts)

# This module has no recurring collector and cannot place or cancel exchange orders.
# A narrowly named explicit enable nonce, exact captured plan fingerprints, and the
# original expiry bound this recovery to one already authorized incident.
import asyncio
import os

ENABLE_ENV = 'MAXPAIN_PENDING_RESTORE_20261006'
ENABLE_NONCE = 'RESTORE_ORIGINAL_13_UNEXPIRED_ONCE'
MAX_FRESH_OBSERVATION_MS = 15_000
_RECOVERY_LOCK = asyncio.Lock()
APPROVED = {'DOGE': {'bot_settings_key': 'sol-proximity-notification-store-v1:b1be4b88ff5e3a54a393abacadfc64ea05a999e98298da74bc11b457ab065813',
          'config_sha256': 'bb40274a9e4b123fc8d97371eb931a3c8544dc78793ef91d5411df8638cdd69f',
          'plan_hashes': {'9836768267bad009c716389a70b89c7997286666675324d14085e841488acb04': '815c0e85e26abbb106ee9fb415f27b1cf960fafd2beaf90b0b4121e11aef11a9'},
          'source_migration': {'cancelled_pending': 1,
                               'coin': 'DOGE',
                               'from_config_sha256': '2f1cc90bacf40e62e1564ba15fd26bbba1e06eab2e2128d291217aa8ca66efd2',
                               'legacy_price_source': 'BINANCE_SPOT_DOGEUSDT_TRADE_1M',
                               'migrated_ms': 1791274806834,
                               'new_price_source': 'HYPERLIQUID_DOGE_PERPETUAL_TRADE_1M',
                               'policy': 'CANCEL_PENDING_DRAIN_FILLED_ORIGINAL_SOURCE_REQUIRE_NEW_BASELINE',
                               'preserved_filled_or_unknown': 0,
                               'to_config_sha256': 'bb40274a9e4b123fc8d97371eb931a3c8544dc78793ef91d5411df8638cdd69f'}},
 'ETH': {'bot_settings_key': 'sol-proximity-notification-store-v1:08464a506669be1f69b379f478d9552e284e326810cdbfaddb6ad412ec66a74f',
         'config_sha256': 'c117967c25ff318c95a7143c0a460cc1ee11b91cf5f0382c88bd7d493a0fae4d',
         'plan_hashes': {'60edbb53ca83b1023c873d8c00490cb9b286de14d21c44e67976934fea2c1b30': 'e33baeeb84b75183f60732cd86757a600e94d5ea5da1f9188827ac8b599bfc9b',
                         '8b8e0ed60e2622e59a08f0807e3292e00064868c63a99cc75e20f0857732d200': '0f9cf41051d4eb5f236cefe208b26c4c4ad189f768eb867b5a83a3d3c7c03aac',
                         'a21f154b85a988950dade9e3bf1472a6e5697cd7b35fbed7d3eadc54ce4e8f49': '03dc927a4747dbb7610d1cd3a9a6df26335a765f4fb8172ff36cee3055f6bb58',
                         'ad62d7f80af026533c4f29ba90a9775f38d6dd0b043f171f822ccc1932683be7': '36893ea2f87122449005303b57b2f64539bef70039c38911af7b9c0e69e8c453',
                         'cd12ee60f91c8ae639732f5898a0451867f77a5602444340bb994f089e0e2fe7': '18ad49f30c0c51de5117a4889434d0990cf1263b4979f20ab4961ed6ab81306a',
                         'ff9009ba3aa7bc2225399a50cf175d8dc71391f25c38c20e0d5dcb7ddc5b433c': '55b21063dba101ae52c6ffebc3dd90ef8b83f84456d7410a097b821b9d24826a'},
         'source_migration': {'cancelled_pending': 6,
                              'coin': 'ETH',
                              'from_config_sha256': '0a534d611816646be5bad2cfb571a74a2bcd234e059b1ac070fd3414c628d854',
                              'legacy_price_source': 'BINANCE_SPOT_ETHUSDT_TRADE_1M',
                              'migrated_ms': 1791274806838,
                              'new_price_source': 'HYPERLIQUID_ETH_PERPETUAL_TRADE_1M',
                              'policy': 'CANCEL_PENDING_DRAIN_FILLED_ORIGINAL_SOURCE_REQUIRE_NEW_BASELINE',
                              'preserved_filled_or_unknown': 0,
                              'to_config_sha256': 'c117967c25ff318c95a7143c0a460cc1ee11b91cf5f0382c88bd7d493a0fae4d'}},
 'HYPE': {'bot_settings_key': 'sol-proximity-notification-store-v1:ed7436a757d41e64b2ae6726d97870c9b0c48ca87bc3eda3a0307807618785e0',
          'config_sha256': '8f0973d51c742e90cd6a6ce6c64c519131a8ab0f11e64014c0c236565091d8b9',
          'plan_hashes': {'1515be7860a146dfec902f4ffa4813f3686aa49bffe33ff43a597554097f2221': '474de667e813320bc59e2bdeba05edcafa7a3c616713f49ea8b930a27713a529',
                          'd7de741a5ec936172d5597616fb458a87e1a42442e31bbf171c8176a38c42f7b': 'a1d23d7471cf5b198cb418ed6791565990de384f667db8210485a68a6d9aadfc'},
          'source_migration': {'cancelled_pending': 2,
                               'coin': 'HYPE',
                               'from_config_sha256': 'ca897b4a125f38ac464327376b64dc354717ee781b8a3ee9402695e991df969d',
                               'legacy_price_source': 'HYPERLIQUID_HYPE_PERPETUAL_TRADE_1M',
                               'migrated_ms': 1791274806834,
                               'new_price_source': 'HYPERLIQUID_HYPE_PERPETUAL_TRADE_1M',
                               'policy': 'CANCEL_PENDING_DRAIN_FILLED_ORIGINAL_SOURCE_REQUIRE_NEW_BASELINE',
                               'preserved_filled_or_unknown': 1,
                               'to_config_sha256': '8f0973d51c742e90cd6a6ce6c64c519131a8ab0f11e64014c0c236565091d8b9'}},
 'SOL': {'bot_settings_key': 'sol-proximity-notification-store-v1:33a7fe46a85a4679b6d9fbe2541f97df51cdf99c87ef8c05941b8ab484ce3975',
         'config_sha256': 'b19c814cc7856c363487b0f4fec8b30cc3a7fe0f2774bc53a181332017c2070c',
         'plan_hashes': {'903b86d9e7784f2b1e8e162a24990334007326a5c127fa7c8c8afc0e08da3db5': 'aabbebb8e6e6573d082be4cbbc4ec5083d10ef826bce6a1461683a620ab523cb'},
         'source_migration': {'cancelled_pending': 1,
                              'coin': 'SOL',
                              'from_config_sha256': '6f293a821de4ce65629e654f12d2854e6c1bc73e3b825c7217a225a7f9c0f06a',
                              'legacy_price_source': 'BINANCE_SPOT_SOLUSDT_TRADE_1M',
                              'migrated_ms': 1791274806831,
                              'new_price_source': 'HYPERLIQUID_SOL_PERPETUAL_TRADE_1M',
                              'policy': 'CANCEL_PENDING_DRAIN_FILLED_ORIGINAL_SOURCE_REQUIRE_NEW_BASELINE',
                              'preserved_filled_or_unknown': 2,
                              'to_config_sha256': 'b19c814cc7856c363487b0f4fec8b30cc3a7fe0f2774bc53a181332017c2070c'}},
 'XRP': {'bot_settings_key': 'sol-proximity-notification-store-v1:fc67b9001e3635e67c39fcdc843318ce2aff7c97ec1888ff409d2d72cc15ebb3',
         'config_sha256': '65f276c71e3e2596c34faf390488a69f1362fc786189f87f7522a9c8b01be4a3',
         'plan_hashes': {'0ea7c69f7aeb0f9fea49f83b558a4e9ee2d42b4fa465c4a49e6b73063b3d75fe': '20b293f53f1f8b814bb12d201967f44bbcf5c5eb2f36f59efed8c4279fb4c096',
                         '60fcab9e7081ac0ba6d6f99ebf40101e40ef49b59d0776c60f3afa0cdfde7989': 'ad1bab3ce72a1896c06447b89405de9daea7fc5b211a2d62166d0b3aa58ffdf2',
                         '7845f07666db9151c7e034bd36267ce15e3fd1e6eaea415e0d957397183df57b': 'b45b77ed04cf7830291d8b2d5695f53e06725361360dc3792cb6098a14989ad1'},
         'source_migration': {'cancelled_pending': 3,
                              'coin': 'XRP',
                              'from_config_sha256': 'ddfff137a407b8e9fac63e7393e8aee7af1fefdb4f141464319b4d0e7d5a1443',
                              'legacy_price_source': 'BINANCE_SPOT_XRPUSDT_TRADE_1M',
                              'migrated_ms': 1791274806837,
                              'new_price_source': 'HYPERLIQUID_XRP_PERPETUAL_TRADE_1M',
                              'policy': 'CANCEL_PENDING_DRAIN_FILLED_ORIGINAL_SOURCE_REQUIRE_NEW_BASELINE',
                              'preserved_filled_or_unknown': 0,
                              'to_config_sha256': '65f276c71e3e2596c34faf390488a69f1362fc786189f87f7522a9c8b01be4a3'}}}


def enabled():
    return os.getenv(ENABLE_ENV, '') == ENABLE_NONCE


def exact_candidates(state):
    coin = state.get('source_migration', {}).get('coin')
    spec = APPROVED.get(coin)
    if not spec or state.get('config_sha256') != spec['config_sha256']:
        raise ValueError('Unapproved current formula configuration')
    if state.get('source_migration') != spec['source_migration']:
        raise ValueError('Unapproved source migration')
    found = {p['position_id']:p for p in state.get('history',[]) if p.get('position_id') in spec['plan_hashes']}
    for pid,p in found.items():
        if p.get('status') != TERMINAL or digest(p) != spec['plan_hashes'][pid]:
            raise ValueError('Original cancelled plan changed')
    return list(found.values())


def _live_veto(plan, bar):
    t,o,h,l,c=bar
    a,e,sl,target=(plan[k] for k in ('direction','entry_price','stop_price','target_price'))
    if any(not isinstance(x,(int,float)) or not math.isfinite(x) or x<=0 for x in (o,h,l,c)) or not l<=min(o,c)<=max(o,c)<=h:
        raise ValueError('Invalid current minute')
    return ((l<=e or h>=target or l<=sl) if a==1 else (h>=e or l<=target or h>=sl))


def apply_fresh(state, bars_by_source, live_by_source, observed_ms, now_ms):
    """Called under the existing store advisory/row lock; no I/O under that lock."""
    if not enabled():
        raise ValueError('One-off restore is disabled')
    if state.get('manual_pending_restore_incidents',{}).get(BATCH):
        return {'status':'ALREADY_PROCESSED','restored':0}
    approved=exact_candidates(state)
    # A historical item absent from retained history cannot be reconstructed live.
    if not approved:
        return {'status':'NO_RETAINED_ORIGINALS','restored':0}
    if now_ms<observed_ms or now_ms-observed_ms>MAX_FRESH_OBSERVATION_MS:
        raise ValueError('Price observation is stale')
    boundary=now_ms//MINUTE*MINUTE
    if observed_ms//MINUTE*MINUTE != boundary:
        raise ValueError('Minute rolled during restoration')
    if state['bar_cursor_ms'] != boundary-MINUTE:
        raise ValueError('Monitor must process latest closed minute first')
    audit=audit_state(state,bars_by_source,now_ms)
    expected_ids={p['position_id'] for p in approved}
    if {r['position_id'] for r in audit['rows']} != expected_ids:
        raise ValueError('Cancelled history differs from exact approved incident')
    routes=set((state['source_migration']['legacy_price_source'],state['source_migration']['new_price_source']))
    for route in routes:
        bar=live_by_source.get(route)
        if not bar or int(bar[0]) != boundary:
            raise ValueError('Current minute price evidence missing')
    for row in audit['rows']:
        for route in routes:
            if _live_veto(row['original'],live_by_source[route]):
                row['reasons'].append(route+':CURRENT_MINUTE_ENTRY_OR_TARGET_ALREADY_REACHED')
        row['eligible']=not row['reasons']
    audit['summary']['eligible']=sum(r['eligible'] for r in audit['rows'])
    audit['summary']['ineligible']=len(audit['rows'])-audit['summary']['eligible']
    audit.pop('audit_sha256',None);audit['audit_sha256']=digest(audit)
    updated,ids=restore_pure(state,audit,now_ms)
    receipt={'processed_ms':now_ms,'source_observed_ms':observed_ms,'restored_ids':ids,
             'decisions':[{'position_id':r['position_id'],'restored':r['eligible'],'reasons':r['reasons']} for r in audit['rows']],
             'audit_sha256':audit['audit_sha256'],'policy':'EXACT13_ORIGINAL_LEVELS_ORIGINAL_EXPIRY_NEXT_MINUTE_RESUME'}
    updated.setdefault('manual_pending_restore_incidents',{})[BATCH]=receipt
    state.clear();state.update(updated)
    return {'status':'APPLIED','restored':len(ids),'eligible_ids':ids,'receipt':receipt}


async def _fetch_closed(fetch,coin,start,end):
    rows=[]
    for chunk_start in range(start,end,1000*MINUTE):
        chunk_end=min(end,chunk_start+1000*MINUTE)
        rows.extend(await asyncio.to_thread(fetch,coin,chunk_start,chunk_end))
        # Yield between bounded public reads; ordinary monitor/delivery tasks retain priority.
        await asyncio.sleep(0)
    return rows


async def run_once(worker,scope):
    """At most one background attempt per worker process, serial across five coins."""
    import sol_proximity_experimental_store as store
    try:
        async with _RECOVERY_LOCK:
            if not enabled():
                return
            if store.key_for(scope) != APPROVED.get(worker.spec.coin,{}).get('bot_settings_key'):
                raise ValueError('Unapproved notification scope')
            state=await asyncio.to_thread(store.snapshot,scope)
            if state.get('manual_pending_restore_incidents',{}).get(BATCH):
                worker.runtime['pending_source_restore']={'status':'ALREADY_PROCESSED'}
                return
            plans=exact_candidates(state)
            now=worker.clock()
            unexpired=[p for p in plans if p['expires_ms']>(now//MINUTE+1)*MINUTE]
            if not unexpired:
                worker.runtime['pending_source_restore']={'status':'ALL_ORIGINAL_PLANS_EXPIRED','restored':0}
                return
            # 24h original pending window bounds the complete path request size.
            start=min(p['arm_ms'] for p in plans)
            if now-start>24*60*MINUTE:
                # Expired plans need no path fetch. Their audit remains ineligible.
                start=min(p['arm_ms'] for p in unexpired)
            cutoff=now//MINUTE*MINUTE
            migration=state['source_migration'];coin=migration['coin']
            routes={migration['new_price_source']:worker.cache.fetch}
            routes.setdefault(migration['legacy_price_source'],worker.legacy_cache.fetch)
            bars={}
            for route,fetch in routes.items():
                bars[route]=await _fetch_closed(fetch,coin,start,cutoff)
            # Normal worker keeps processing during collection; refresh cutoff if needed.
            current=worker.clock()//MINUTE*MINUTE
            if current-cutoff>2*MINUTE:
                raise ValueError('One-off collection exceeded bounded freshness interval')
            for route,fetch in routes.items():
                if current>cutoff:
                    bars[route].extend(await _fetch_closed(fetch,coin,cutoff,current))
            live={}
            observed=worker.clock()
            for route,fetch in routes.items():
                rows=await asyncio.to_thread(fetch,coin,current,current+MINUTE)
                if len(rows)!=1:
                    raise ValueError('Missing current candle')
                live[route]=rows[0]
            if worker.clock()//MINUTE*MINUTE != current:
                raise ValueError('Minute rolled during current quote capture')
            if not enabled():
                return
            # Existing transaction serializes against concurrent Watch intake and monitor.
            result=await worker.db(store.transact,scope,worker.clock(),
                action=lambda fresh:apply_fresh(fresh,bars,live,observed,worker.clock()))
            worker.runtime['pending_source_restore']=result
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        # Recovery failure never changes normal experiment readiness/retry scheduling.
        worker.runtime['pending_source_restore']={'status':'DEFERRED_NO_MUTATION','error_type':type(exc).__name__, 'detail':str(exc)}


def schedule(worker,scope):
    if enabled() and worker.source_restore_task is None:
        worker.source_restore_task=asyncio.create_task(run_once(worker,scope),name=worker.spec.coin.lower()+'-pending-restore-20261006')
