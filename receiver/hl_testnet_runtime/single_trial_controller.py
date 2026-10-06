"""Explicit deployment-invoked, bounded ONE Testnet trial; disabled by default.

The immutable run reservation precedes selection and survives process loss.
Selection consumes the run before run_one is invoked; uncertainty never grants
replay or a replacement card. No HTTP control or continuous-entry flag changes.
The spawned process gives the existing trial its own independent supervisor.
"""
from copy import deepcopy
from datetime import datetime, timezone
import json
import multiprocessing
import os
import re
import time
import urllib.error
import urllib.request
import uuid

from . import card_sync_evidence, checks, emergency_close, long_stream_runtime as stream
from . import protection_timing_trial as trial, two_account_execution as roles
from . import card_lifecycle as life
from .approved_alert_selection import eligible, next_cards
from .filled_dispatch_store import DispatchError, DispatchStore
from .postgres_journal import JournalError, PostgresJournal, canonical, digest
from .request_budget import BACKGROUND_LIMIT, TICKETS, Budget, BudgetError, request_weight
from .source_window import timestamp
from .trade_card_store import CardStore
from . import trade_cards

MODE = 'approved_bounded_single_trial_v1'
SCHEMA = 'hl_testnet_timing_runs_v1'
VERSION = 'bounded-single-timing-run-v1'
LOCK = 1729048277
MAX_WINDOW_SECONDS = 2400
HEALTH_URL = 'https://hl-testnet-check-yoyo.onrender.com/healthz'
_process = None
_stop = None


def config(env):
    """An absent opt-in performs no imports of wallets, DB access or requests."""
    mode = env.get('HL_TESTNET_SINGLE_TRIAL_CONTROLLER', '')
    if mode == '':
        return None
    if mode != MODE:
        raise DispatchError('SINGLE_TRIAL_CONTROLLER_MODE_INVALID')
    if (env.get('RENDER_SERVICE_ID') != roles.SERVICE
            or env.get('HL_TESTNET_RUNTIME_MODE') != stream.MODE
            or env.get('HL_TESTNET_FILLED_DISPATCH') != stream.RELEASE
            or env.get('HL_TESTNET_EMERGENCY_CLOSE') != emergency_close.APPROVAL
            or env.get('HL_TESTNET_LONG_ENTRY_ENABLED') != 'false'
            or env.get('HL_TESTNET_SHORT_ENTRY_ENABLED') != 'false'
            or env.get('HL_TESTNET_TWO_ACCOUNT_EXECUTION') != 'disabled'
            or env.get('HL_TESTNET_SAFETY_PIPELINE') or env.get('HL_TESTNET_CARD_SYNC')):
        raise DispatchError('SINGLE_TRIAL_REQUIRES_FIXED_TESTNET_AND_DISABLED_ENTRIES')
    commit = env.get('HL_TESTNET_SINGLE_TRIAL_COMMIT', '')
    run_id = env.get('HL_TESTNET_SINGLE_TRIAL_RUN_ID', '')
    if (not re.fullmatch(r'[0-9a-f]{40}', commit)
            or env.get('RENDER_GIT_COMMIT') != commit
            or not re.fullmatch(r'hadar-timing-[0-9TZ]{16,20}', run_id)):
        raise DispatchError('SINGLE_TRIAL_EXACT_COMMIT_AND_RUN_ID_REQUIRED')
    try:
        not_before = timestamp(env['HL_TESTNET_SINGLE_TRIAL_NOT_BEFORE'])
        deadline = timestamp(env['HL_TESTNET_SINGLE_TRIAL_DEADLINE'])
        if not 0 < (deadline - not_before).total_seconds() <= MAX_WINDOW_SECONDS:
            raise ValueError()
    except (KeyError, TypeError, ValueError):
        raise DispatchError('SINGLE_TRIAL_ABSOLUTE_BOUNDED_WINDOW_REQUIRED') from None
    return dict(run_id=run_id, commit=commit, not_before=not_before.isoformat(),
                deadline=deadline.isoformat(), environment='testnet')


class RunStore:
    """One positively committed ownership token; never adopt an existing run."""
    def __init__(self, journal):
        self.journal = journal

    def ready(self, conn):
        if conn.execute(f'SELECT version FROM {SCHEMA}.metadata WHERE singleton=true').fetchone() != (VERSION,):
            raise DispatchError('SINGLE_TRIAL_SCHEMA_REQUIRES_REVIEW')

    def initialize(self):
        with self.journal._transaction() as conn:
            self.journal._ready(conn)
            conn.execute('SELECT pg_advisory_xact_lock(%s)', (LOCK,))
            if conn.execute('SELECT to_regnamespace(%s)', (SCHEMA,)).fetchone()[0] is not None:
                self.ready(conn)
                return False
            conn.execute(f'CREATE SCHEMA {SCHEMA}')
            conn.execute(f'CREATE TABLE {SCHEMA}.metadata (singleton boolean PRIMARY KEY CHECK(singleton), version text NOT NULL)')
            conn.execute(f'INSERT INTO {SCHEMA}.metadata VALUES(true,%s)', (VERSION,))
            conn.execute(f'''CREATE TABLE {SCHEMA}.runs (
                run_id text PRIMARY KEY, owner text NOT NULL, stage text NOT NULL,
                deadline timestamptz NOT NULL, card_id text UNIQUE,
                value jsonb NOT NULL, digest text NOT NULL,
                environment text NOT NULL DEFAULT 'testnet' CHECK(environment='testnet'))''')
            conn.execute(f'REVOKE ALL ON SCHEMA {SCHEMA} FROM PUBLIC')
            for table in ('metadata', 'runs'):
                conn.execute(f'REVOKE ALL ON {SCHEMA}.{table} FROM PUBLIC')
        return True

    def claim(self, configuration, owner):
        record = dict(version=VERSION, configuration=deepcopy(configuration),
                      owner=owner, stage='WAITING_FOR_FRESH_CARD', card_id=None)
        with self.journal._transaction() as conn:
            self.ready(conn)
            conn.execute('SELECT pg_advisory_xact_lock(%s)', (LOCK,))
            # A selected or uncertain run blocks all subsequent run IDs until
            # explicitly reviewed. An expired unselected run cannot send.
            active = conn.execute(f'''SELECT 1 FROM {SCHEMA}.runs WHERE
                stage IN ('ONE_CANDIDATE_SELECTED','STOPPED_REQUIRES_REVIEW')
                OR (stage='WAITING_FOR_FRESH_CARD' AND deadline>clock_timestamp()) LIMIT 1''').fetchone()
            if active or conn.execute(f'SELECT 1 FROM {SCHEMA}.runs WHERE run_id=%s', (configuration['run_id'],)).fetchone():
                return False
            if not conn.execute('SELECT %s::timestamptz>clock_timestamp()', (configuration['deadline'],)).fetchone()[0]:
                return False
            conn.execute(f'''INSERT INTO {SCHEMA}.runs(run_id,owner,stage,deadline,value,digest)
                VALUES(%s,%s,%s,%s,%s::jsonb,%s)''', (configuration['run_id'], owner,
                record['stage'], configuration['deadline'], canonical(record), digest(record)))
        return True

    def update(self, configuration, owner, stage, *, card=None, result=None, failure_code=None):
        with self.journal._transaction() as conn:
            self.ready(conn)
            conn.execute('SELECT pg_advisory_xact_lock(%s)', (LOCK,))
            row = conn.execute(f'SELECT value,digest,stage,owner FROM {SCHEMA}.runs WHERE run_id=%s FOR UPDATE', (configuration['run_id'],)).fetchone()
            if (row is None or row[1] != digest(row[0]) or row[2] != row[0]['stage']
                    or row[3] != owner or row[0]['owner'] != owner
                    or row[0]['configuration'] != configuration):
                raise DispatchError('SINGLE_TRIAL_RESERVATION_CHANGED_NO_REPLAY')
            record = deepcopy(row[0])
            allowed = {'WAITING_FOR_FRESH_CARD': {'ONE_CANDIDATE_SELECTED', 'NO_ELIGIBLE_CARD_WITHIN_WINDOW', 'STOPPED_REQUIRES_REVIEW'},
                       'ONE_CANDIDATE_SELECTED': {'EXPERIMENT_COMPLETE', 'STOPPED_REQUIRES_REVIEW'}}
            if stage not in allowed.get(record['stage'], set()):
                raise DispatchError('SINGLE_TRIAL_STAGE_ALREADY_CONSUMED_NO_REPLAY')
            if stage == 'ONE_CANDIDATE_SELECTED':
                if card is None or not conn.execute('SELECT clock_timestamp()>=%s::timestamptz AND clock_timestamp()<%s::timestamptz',
                        (configuration['not_before'], configuration['deadline'])).fetchone()[0]:
                    raise DispatchError('SINGLE_TRIAL_SELECTION_WINDOW_EXPIRED')
                # Reopen and validate the genuine immutable delivery inside the
                # selection transaction; a readiness hint is never authority.
                stored = conn.execute('''SELECT manifest,digest FROM hl_testnet_cards_v1.cards c
                    WHERE card_id=%s AND EXISTS (SELECT 1 FROM hl_testnet_cards_v1.delivery_receipts r
                        WHERE r.card_id=c.card_id AND r.status='RECORDED')''', (card['card_id'],)).fetchone()
                now = conn.execute('SELECT clock_timestamp()').fetchone()[0]
                if (stored is None or trade_cards.checksum(stored[0]) != stored[1]
                        or stored[0] != card
                        or not eligible(card, not_before=timestamp(configuration['not_before']), now=now)
                        or (timestamp(card['source_expires_at']) - now).total_seconds() <= 90):
                    raise DispatchError('SINGLE_TRIAL_FRESH_RECORDED_CARD_REQUIRED')
                record.update(card_id=card['card_id'], account_role=card['account_role'],
                    source_event_id=card['prepared']['source']['event_id'],
                    source_at=card['prepared']['source']['at'], source_expires_at=card['source_expires_at'],
                    symbol=card['prepared']['execution']['symbol'])
            record['stage'] = stage
            if result is not None:
                record['result'] = deepcopy(result)
            if failure_code is not None:
                record['failure_code'] = failure_code
            conn.execute(f'UPDATE {SCHEMA}.runs SET stage=%s,card_id=%s,value=%s::jsonb,digest=%s WHERE run_id=%s',
                (stage, record['card_id'], canonical(record), digest(record), configuration['run_id']))
        return record


def deployed_health():
    """Read-only hint from the ordinary worker, not trial send authority."""
    try:
        with urllib.request.urlopen(HEALTH_URL, timeout=4) as response:
            raw = response.read(65537)
    except (urllib.error.URLError, TimeoutError, OSError):
        raise DispatchError('SINGLE_TRIAL_WORKER_HEALTH_UNAVAILABLE') from None
    if len(raw) > 65536:
        raise DispatchError('SINGLE_TRIAL_WORKER_HEALTH_INVALID')
    return json.loads(raw)['long_stream']


def ready_hint(card, states, health, now_ms, used):
    """Conservative local selection delay; run_one independently rechecks all."""
    safety = health.get('emergency_close', {})
    feed = health.get('fill_notifications', {}).get(card['account_role'], {})
    if (health.get('running') is not True or health.get('new_entries_enabled') is not False
            or health.get('short_entries_enabled') is not False
            or safety.get('running') is not True or safety.get('last_status') != 'PASS_COMPLETE'
            or type(safety.get('last_pass_at_ms')) is not int
            or not 0 <= now_ms - safety['last_pass_at_ms'] <= 15000
            or feed.get('entry_allowed') is not True):
        return False
    symbol = card['prepared']['execution']['symbol']
    cid = card['card_id']
    predecessors = []
    for state in states:
        if (state.get('pending') is not None or (state.get('emergency') is not None
                and state['emergency']['phase'] != 'CLOSED_VERIFIED')
                or cid in state['originals'] or cid in state.get('entry_timing_armed', {})):
            return False
        if state['symbol'] == symbol:
            if state.get('emergency') is not None or not stream.idle_flat(state):
                return False
            if state['bindings']:
                predecessors.append(state)
        if not stream._immutable_flat_checkpoint(state):
            ev = state.get('evidence')
            if (ev is None or ev['bindings'] != state['bindings']
                    or not ev['snapshot']['history_complete'] or not ev['snapshot']['orders_complete']
                    or not 0 <= now_ms - ev['snapshot']['at_ms'] <= 13000):
                return False
            if life.number(ev['snapshot']['position_quantity'], signed=True) != 0:
                from .filled_quantity_dispatch import _fully_protected_no_work
                if not _fully_protected_no_work(state, now_ms):
                    return False
    cost = 372
    if predecessors:
        cost = 328
        for state in predecessors:
            snap = state['evidence']['snapshot']
            planned = card_sync_evidence._planned_observation_reads(state['bindings'], snap,
                snap['at_ms'], now_ms, reuse_verified_terminals=True)
            cost += sum(20 if r['type'] == 'userFillsByTime' else request_weight('/info', r) for r in planned)
    return type(used) is int and used >= 0 and used + cost <= BACKGROUND_LIMIT - 2


def candidate(journal, environment, configuration, now):
    ids = next_cards(journal, not_before=configuration['not_before'], now=now)
    if not ids:
        return None
    health = deployed_health()
    with journal._transaction() as conn:
        conn.execute('SET TRANSACTION READ ONLY')
        from .filled_dispatch_store import SCHEMA as dispatch_schema
        incident = conn.execute(f'''SELECT 1 FROM {dispatch_schema}.buckets
            WHERE jsonb_typeof(value->'emergency')='object'
            AND value->'emergency'->>'phase' IS DISTINCT FROM 'CLOSED_VERIFIED' LIMIT 1''').fetchone()
        used = int(conn.execute(f'SELECT COALESCE(sum(weight),0) FROM {TICKETS} WHERE admitted_at>clock_timestamp()-interval \'69 seconds\'').fetchone()[0])
    if incident:
        return None
    store, cards = DispatchStore(journal), CardStore(journal)
    # Public context is only a selection hint. Share one read for cards in the
    # same account/market during this pass, never across passes or send gates.
    prices = {}
    reader = None
    for cid in ids:
        card = cards.load(cid)
        if card['prepared']['execution'] is None or (timestamp(card['source_expires_at']) - now).total_seconds() <= 90:
            continue
        route = roles.route_for(environment, card['account_role'])
        price_key = (route['account'], card['prepared']['execution']['symbol'])
        hint_cost = (0 if price_key in prices else request_weight('/info',
            dict(type='activeAssetData', user=price_key[0], coin=price_key[1])))
        if not ready_hint(card, store.for_account(route['account']), health,
                int(now.timestamp() * 1000), used + hint_cost):
            continue
        if price_key not in prices:
            if reader is None:
                reader = checks.InfoReader(budget=Budget(journal), priority='background')
            began = time.monotonic()
            try:
                raw = reader.read('activeAssetData', user=price_key[0], coin=price_key[1])
                checks.capacity(raw, price_key[0], price_key[1])
                price = checks.number(raw['markPx'])
            except BudgetError as exc:
                if str(exc) in trial.TRANSIENT_BUDGET_CODES:
                    return None
                raise
            except checks.Blocked as exc:
                code = ('SINGLE_TRIAL_PUBLIC_PRICE_UNAVAILABLE' if str(exc) in
                    ('READ_UNAVAILABLE', 'RESPONSE_BOUND_EXCEEDED') else
                    'SINGLE_TRIAL_PUBLIC_PRICE_INVALID')
                raise DispatchError(code) from None
            if time.monotonic() - began > 5:
                return None
            used += hint_cost
            prices[price_key] = (price, began)
        price, sampled_at = prices[price_key]
        if time.monotonic() - sampled_at > 5:
            # One pass cannot renew a consumed context read or use an old hint.
            continue
        execution = card['prepared']['execution']
        stop, take = (checks.number(execution[key]) for key in ('stop', 'take_profit'))
        if min(stop, take) < price < max(stop, take):
            return card
    return None


def publish(record):
    print(json.dumps({'testnet_single_timing_trial': record}, sort_keys=True), flush=True)


def verify_handoff(environment, journal, card, result):
    """A later committed checkpoint proves independent ongoing management."""
    if not result.get('ongoing_management_required'):
        return result
    result = deepcopy(result)
    if result.get('trial_supervisor_stopped') is not True:
        result['status'] = 'RECONCILIATION_REQUIRED'
        return result
    after = time.time_ns() // 1000000
    until = time.monotonic() + 45
    route = roles.route_for(environment, card['account_role'])
    store = DispatchStore(journal)
    while time.monotonic() < until:
        try:
            now = time.time_ns() // 1000000
            states = [s for s in store.for_account(route['account'])
                      if any(b['card_id'] == card['card_id'] for b in s['bindings'])]
            if len(states) == 1:
                current = states[0]
                observed = trial.outcome(current, card['card_id'], now)
                if (observed['evidence_at_ms'] is not None and observed['evidence_at_ms'] > after
                        and (observed['protection_verified'] or observed['terminal_verified'])):
                    health = deployed_health()
                    safety = health.get('emergency_close', {})
                    at = safety.get('last_pass_at_ms')
                    if (health.get('running') is True and health.get('new_entries_enabled') is False
                            and health.get('short_entries_enabled') is False
                            and safety.get('running') is True and safety.get('last_status') == 'PASS_COMPLETE'
                            and type(at) is int and 0 <= time.time_ns() // 1000000 - at <= 15000):
                        result.update(deployed_worker_handoff_verified=True,
                            handoff_observation=observed,
                            handoff_timing=trial.durable_timing(current, card['card_id'], time.time_ns() // 1000000))
                        return result
        except Exception:
            # Read-only inspection may wait within this original 45s bound.
            # It cannot trigger another order, card selection or trial grant.
            pass
        time.sleep(2)
    result.update(status='RECONCILIATION_REQUIRED', deployed_worker_handoff_verified=False)
    return result


def execute(environment, stopped, *, clock=None, find=None, runner=None, store=None):
    """Exactly one run_one invocation after positively committed selection."""
    configuration = config(environment)
    if configuration is None:
        return
    clock = clock or (lambda: datetime.now(timezone.utc))
    find = find or candidate
    runner = runner or trial.run_one
    deadline, beginning = timestamp(configuration['deadline']), timestamp(configuration['not_before'])
    if clock() >= deadline or stopped.is_set():
        return
    store = store or RunStore(PostgresJournal.from_env(environment))
    owner = uuid.uuid4().hex
    store.initialize()
    if not store.claim(configuration, owner):
        publish(dict(run_id=configuration['run_id'], stage='RUN_ALREADY_RESERVED_NO_REPLAY'))
        return
    wall_deadline = time.monotonic() + max(0, (deadline - clock()).total_seconds())
    publish(dict(run_id=configuration['run_id'], stage='WAITING_FOR_FRESH_CARD', deadline=configuration['deadline']))
    selected = False
    last_wait = None
    try:
        while not stopped.is_set() and clock() < deadline and time.monotonic() < wall_deadline:
            if config(environment) != configuration:
                raise DispatchError('SINGLE_TRIAL_CONFIG_CHANGED_NO_REPLAY')
            now = clock()
            try:
                card = find(store.journal, environment, configuration, now) if now >= beginning else None
            except JournalError as exc:
                # Read-only readiness can wait; a reservation/selection commit
                # uncertainty is deliberately outside this retry boundary.
                if str(exc) not in ('SINGLE_TRIAL_WORKER_HEALTH_UNAVAILABLE',
                        'SINGLE_TRIAL_PUBLIC_PRICE_UNAVAILABLE', 'PERSISTENCE_UNAVAILABLE_NO_SEND'):
                    raise
                if last_wait != str(exc):
                    publish(dict(run_id=configuration['run_id'], stage='WAITING_FOR_FRESH_CARD', failure_code=str(exc), deadline=configuration['deadline']))
                    last_wait = str(exc)
                stopped.wait(5)
                continue
            if card is not None:
                record = store.update(configuration, owner, 'ONE_CANDIDATE_SELECTED', card=card)
                selected = True
                publish(record)
                if stopped.is_set() or clock() >= deadline:
                    raise DispatchError('SINGLE_TRIAL_SELECTION_STOPPED_NO_ENTRY')
                # The selected run is consumed even if process loss follows
                # this commit. No recovery path invokes the runner again.
                result = runner(dict(environment), card['card_id'])
                result = verify_handoff(environment, store.journal, card, result)
                stage = ('STOPPED_REQUIRES_REVIEW' if result.get('status') == 'RECONCILIATION_REQUIRED'
                         else 'EXPERIMENT_COMPLETE')
                publish(store.update(configuration, owner, stage, result=result))
                return
            stopped.wait(5)
        publish(store.update(configuration, owner, 'NO_ELIGIBLE_CARD_WITHIN_WINDOW'))
    except Exception as exc:
        code = str(exc)
        code = code if isinstance(exc, (DispatchError, life.LifecycleError)) and re.fullmatch(r'[A-Z][A-Z0-9_]{2,99}', code) else type(exc).__name__
        try:
            publish(store.update(configuration, owner, 'STOPPED_REQUIRES_REVIEW', failure_code=code))
        except Exception:
            # Lost update ACK cannot release the positively committed claim or
            # selected reservation. A new process must inspect, never replay.
            publish(dict(run_id=configuration['run_id'], stage='STOPPED_REQUIRES_REVIEW', selected=selected, failure_code=code))


def _worker(environment, stopped):
    try:
        execute(environment, stopped)
    except Exception as exc:
        publish(dict(stage='STOPPED_REQUIRES_REVIEW', failure_code=type(exc).__name__))


def start():
    """Called only after ordinary workers start; explicit opt-in or no action."""
    global _process, _stop
    environment = dict(os.environ)
    try:
        configuration = config(environment)
    except DispatchError as exc:
        # A stale one-off opt-in must never stop the ordinary guard worker.
        # execute() retains the strict, raising authority check independently.
        publish(dict(stage='CONTROLLER_DISABLED_REQUIRES_REVIEW', failure_code=str(exc)))
        return False
    if configuration is None or datetime.now(timezone.utc) >= timestamp(configuration['deadline']):
        return False
    if _process is not None and _process.is_alive():
        return False
    context = multiprocessing.get_context('spawn')
    _stop = context.Event()
    _process = context.Process(target=_worker, args=(environment, _stop), name='testnet-single-timing-trial', daemon=False)
    _process.start()
    return True


def stop():
    """Stops selection only; never terminate a selected trial's protection."""
    if _stop is not None:
        _stop.set()
