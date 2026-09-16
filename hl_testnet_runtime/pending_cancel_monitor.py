"""Cancellation-only Testnet monitor. Never creates entries or cancels all orders.

Five-second delay after each completed pass; no overlapping timer jobs. Only
registered immutable formula rules can reach the existing cancellation engine.
Exchange transport and durable cancel/repair reservations remain in that engine.
A Free Render instance can sleep: this module does not claim 24/7 availability,
keep itself awake, reconstruct missed prices, or introduce time cancellation.
"""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import re
import threading
import time

MODE = 'cancel_monitor_testnet_v1'
SERVICE = 'srv-dakptbh594qs7395460g'
POLICY = 'favorable-half-formula-threshold-v1'
STATE = 'hl_testnet_execution_v1.limit_cancel_monitor_state'
MAX_RULES = 32
INTERVAL_SECONDS = 5
TERMINAL = frozenset({'ENTRY_ALREADY_FILLED_NO_CANCEL', 'FILL_OBSERVED_DO_NOT_CANCEL',
    'CANCELLATION_VERIFIED_FLAT', 'RACED_FILL_PROTECTION_RESTORED',
    'FILLED_BEFORE_CANCEL_PROTECTION_VERIFIED'})
OK = TERMINAL | {'WAITING_BELOW_CANCEL_THRESHOLD'}


class MonitorError(ValueError):
    """Fixed local codes; never interpolate configuration or remote replies."""


@dataclass(frozen=True)
class Config:
    account: str
    agent: str

    @classmethod
    def from_env(cls, env):
        if (env.get('RENDER_SERVICE_ID') != SERVICE
                or env.get('HL_TESTNET_RUNTIME_MODE') != MODE
                or env.get('HL_TESTNET_JOURNAL_BACKEND') != 'staging_postgres_v1'
                or env.get('HL_TESTNET_PRICE_ROUNDING') != 'nearest-half-up-perp-v1'
                or env.get('HL_TESTNET_EXIT_TYPE') != 'tp_limit_sl_market'
                or env.get('HL_TESTNET_CANCEL_MONITOR_POLICY') != POLICY):
            raise MonitorError('CANCEL_MONITOR_NOT_AUTHORIZED')
        values = [env.get(k, '') for k in ('HL_TESTNET_ACCOUNT_ADDRESS','HL_TESTNET_AGENT_ADDRESS')]
        if any(not isinstance(v, str) or not re.fullmatch(r'0x[0-9a-fA-F]{40}', v)
               or int(v[2:], 16) == 0 for v in values):
            raise MonitorError('CANCEL_MONITOR_ADDRESSES_REQUIRED')
        account, agent = [v.lower() for v in values]
        if account == agent:
            raise MonitorError('DEDICATED_AGENT_REQUIRED')
        return cls(account, agent)


class Store:
    """Only the selected staging journal and additive monitoring state."""
    def __init__(self, journal, account):
        self.journal, self.account = journal, account

    def initialize(self):
        from .postgres_journal import LOCK
        with self.journal._transaction() as conn:
            self.journal._ready(conn)
            conn.execute('SELECT pg_advisory_xact_lock(%s)', (LOCK,))
            for table in ('limit_cancel_rules','limit_cancel_operations'):
                if conn.execute('SELECT to_regclass(%s)', ('hl_testnet_execution_v1.' + table,)).fetchone()[0] is None:
                    raise MonitorError('REGISTERED_CANCEL_STORAGE_REQUIRED')
            conn.execute(f'''CREATE TABLE IF NOT EXISTS {STATE} (
                plan_key text PRIMARY KEY REFERENCES hl_testnet_execution_v1.prepared(plan_key),
                status text NOT NULL, terminal boolean NOT NULL,
                observed_at timestamptz NOT NULL DEFAULT clock_timestamp())''')
            conn.execute(f'REVOKE ALL ON {STATE} FROM PUBLIC')

    def active_keys(self):
        with self.journal._transaction() as conn:
            rows = conn.execute(f'''SELECT r.plan_key FROM hl_testnet_execution_v1.limit_cancel_rules r
                JOIN hl_testnet_execution_v1.prepared p ON p.plan_key=r.plan_key
                JOIN hl_testnet_execution_v1.attempts a ON a.plan_key=r.plan_key AND a.account=p.account
                LEFT JOIN {STATE} m ON m.plan_key=r.plan_key
                WHERE p.account=%s AND NOT coalesce(m.terminal,false)
                ORDER BY r.created_at,r.plan_key LIMIT %s''', (self.account,MAX_RULES+1)).fetchall()
        if len(rows) > MAX_RULES:
            raise MonitorError('MONITOR_RULE_LIMIT_REQUIRES_REVIEW')
        return [r[0] for r in rows]

    def save(self, key, status):
        if not isinstance(status,str) or not re.fullmatch(r'[A-Z_]{1,80}',status):
            raise MonitorError('INVALID_MONITOR_RESULT')
        # Scope writes to this account. A terminal result cannot be overwritten
        # by an older in-flight pass from a different process during deployment.
        with self.journal._transaction() as conn:
            count = conn.execute(f'''INSERT INTO {STATE} AS saved(plan_key,status,terminal)
                SELECT plan_key,%s,%s FROM hl_testnet_execution_v1.prepared
                WHERE plan_key=%s AND account=%s
                ON CONFLICT(plan_key) DO UPDATE SET
                  status=CASE WHEN saved.terminal THEN saved.status ELSE EXCLUDED.status END,
                  terminal=saved.terminal OR EXCLUDED.terminal, observed_at=clock_timestamp()
                RETURNING plan_key''', (status,status in TERMINAL,key,self.account)).fetchall()
        if len(count) != 1:
            raise MonitorError('MONITOR_RECORD_NOT_BOUND_TO_ACCOUNT')


def inventory(account):
    """Public reads, no key access. Unregistered orders are counted, never managed."""
    import hyperliquid_testnet_executor as sender
    rows = sender.TestnetHTTP().info('frontendOpenOrders', user=account)
    if not isinstance(rows,list) or len(rows) > 10000:
        raise MonitorError('ACCOUNT_INVENTORY_UNAVAILABLE')
    entries = exits = 0
    for row in rows:
        if not isinstance(row,dict) or type(row.get('reduceOnly')) is not bool:
            raise MonitorError('ACCOUNT_INVENTORY_UNAVAILABLE')
        if row['reduceOnly']:
            exits += 1
        else:
            entries += 1
    return {'open_entry_orders':entries, 'open_exit_orders':exits}


class Monitor:
    def __init__(self, config, store, dispatcher, read_inventory=inventory):
        self.config, self.store = config, store
        self.dispatcher, self.read_inventory = dispatcher, read_inventory
        self.cycles = self.writes = self.cancels = self.failures = 0
        self.started_at = datetime.now(timezone.utc).isoformat()
        self.last_finished = None

    def tick(self, stop):
        if stop.is_set():
            return {'status':'MONITOR_STOPPED'}
        report = {'mode':MODE, 'entry_sending_enabled':False,
                  'cancellation_sending_enabled':True, 'time_cancel_enabled':False,
                  'sampling_delay_seconds':INTERVAL_SECONDS, 'decisions':{}}
        report.update(self.read_inventory(self.config.account))
        keys = self.store.active_keys()  # Re-discover newly registered rules.
        report['active_registered_rules'] = len(keys)
        for key in keys:
            if stop.is_set():
                break
            result = self.dispatcher(key,journal=self.store.journal,
                                     agent=self.config.agent,enable_testnet=True)
            status = result.get('status')
            if not isinstance(status,str) or not re.fullmatch(r'[A-Z_]{1,80}',status):
                raise MonitorError('INVALID_MONITOR_RESULT')
            cancel_count = result.get('cancel_requests_sent',0)
            write_count = result.get('exchange_write_attempts',0)
            if type(cancel_count) is not int or type(write_count) is not int or not 0 <= cancel_count <= write_count <= 2:
                raise MonitorError('INVALID_MONITOR_COUNTERS')
            self.cancels += cancel_count
            self.writes += write_count
            self.store.save(key,status)
            report['decisions'][status] = report['decisions'].get(status,0)+1
        self.cycles += 1
        self.last_finished = time.monotonic()
        report.update(status='MONITOR_PASS_COMPLETED', cycles=self.cycles,
            cancel_requests_sent_total=self.cancels, exchange_write_attempts_total=self.writes,
            observed_at_utc=datetime.now(timezone.utc).isoformat())
        return report

    def run(self, stop, emit):
        previous = None
        last_log = 0.0
        while not stop.is_set():
            try:
                report = self.tick(stop)
                delay = INTERVAL_SECONDS
                self.failures = 0
            except Exception:
                self.failures += 1
                delay = min(60, INTERVAL_SECONDS * (2 ** min(self.failures,4)))
                report = {'mode':MODE,'status':'MONITOR_REQUIRES_REVIEW',
                          'consecutive_failures':self.failures,
                          'retry_read_delay_seconds':delay,'entry_sending_enabled':False,
                          'cancel_requests_sent_total':self.cancels,
                          'exchange_write_attempts_total':self.writes}
            meaningful = {k:v for k,v in report.items() if k not in ('observed_at_utc','cycles')}
            now = time.monotonic()
            if meaningful != previous or now-last_log >= 60:
                emit(report)
                previous, last_log = meaningful, now
            # No timer piles up while a request is slow. Cancellation retries are
            # decided by the durable engine; this loop cannot replay a POST.
            stop.wait(delay)


_lock = threading.Lock()
_thread = None
_stop = threading.Event()
_health = {'status':'NOT_STARTED','entry_sending_enabled':False}
_last_health = None


def _emit(report):
    global _health, _last_health
    with _lock:
        _health = dict(report)
        _last_health = time.monotonic()
    print(json.dumps({'testnet_cancel_monitor':report},sort_keys=True),flush=True)


def health():
    with _lock:
        alive = _thread is not None and _thread.is_alive()
        recent = _last_health is not None and time.monotonic()-_last_health < 120
        return {'monitor_running':alive, 'recent_report':recent,
                'entry_sending_enabled':False,
                'cancellation_sending_enabled':alive,
                'status':_health.get('status','NOT_STARTED')}


def _worker(env):
    try:
        config = Config.from_env(env)
        from .postgres_journal import PostgresJournal
        from .pending_cancel_executor import cancel_registered_once
        store = Store(PostgresJournal.from_env(env),config.account)
        store.initialize()  # DDL is never executed in the repeated tick.
        Monitor(config,store,cancel_registered_once).run(_stop,_emit)
    except Exception:
        _emit({'mode':MODE,'status':'MONITOR_INITIALIZATION_FAILED',
               'entry_sending_enabled':False,'cancellation_sending_enabled':False})


def start():
    global _thread
    import os
    Config.from_env(os.environ)  # Reject before thread creation or key/DB access.
    with _lock:
        if _thread is not None and _thread.is_alive():
            return False
        _stop.clear()
        _thread = threading.Thread(target=_worker,args=(os.environ,),
                                   daemon=True,name='registered-testnet-cancel-monitor')
        _thread.start()
    return True


def stop():
    _stop.set()
