"""Bounded, lossless execution history maintenance; never deletes cold records.

Explicit initialization installs archive tables. Maintenance shares the exact
state lock/transaction, archives only proven final obligations, and leaves an
indexed durable occurrence tombstone outside the hot JSON document. No exchange
work, schema creation, or restore-to-active operation exists on the hot path.
"""
from contextlib import contextmanager, closing
from copy import deepcopy
from decimal import Decimal
import json
import sqlite3

import approved_alert_contract as contract
from . import card_lifecycle as life

VERSION = 'execution-history-v1'
INTERVAL_MS = 60000
MAX_BATCH = 32
MAX_SCAN = 128
MAX_BATCH_BYTES = 2*1024*1024
MAX_RECORD_BYTES = 16*1024*1024
FINAL = frozenset(('CLOSED', 'CANCELED_WITHOUT_FILL', 'MANUALLY_CLOSED'))
TERMINAL = frozenset(('FILLED', 'CANCELED', 'REJECTED'))
EXTRAS = ('formula_states', 'source_condition_errors', 'entry_blocked', 'entry_decisions')


def maintenance_readiness(store, failure):
    """Allow deferred *storage availability* maintenance after a durable reload.

    Domain/integrity/capacity failures and unrecognized programming errors never
    become permission to enter. Even allowed transient failures need a fresh,
    checksummed state and 1 MiB of headroom (1/16 of the existing hard limit).
    Subsequent order preparation must still commit before any exchange send.
    This helper creates no tables, writes no journal, and performs no network I/O.
    """
    from .experimental_execution_state import StateError, encode, MAX_STATE_BYTES
    from .postgres_journal import JournalError
    transient = (isinstance(failure, OSError)
        or isinstance(failure, sqlite3.OperationalError)
        or type(failure) is JournalError and str(failure) == 'PERSISTENCE_UNAVAILABLE_NO_SEND')
    if isinstance(failure, StateError) or not transient:
        return dict(entries_allowed=False, status='HISTORY_MAINTENANCE_REVIEW_REQUIRED')
    # load verifies the existing checksum/domain; an unavailable/corrupt live
    # journal escapes and cannot be hidden by the maintenance fallback.
    state = store.load()
    size = len(encode(state).encode())
    reserve = MAX_STATE_BYTES // 16
    if size > MAX_STATE_BYTES-reserve:
        return dict(entries_allowed=False, status='HISTORY_ACTIVE_CAPACITY_REVIEW_REQUIRED',
                    state_bytes=size, reserved_bytes=reserve)
    return dict(entries_allowed=True, status='HISTORY_MAINTENANCE_DEFERRED',
                state_bytes=size, reserved_bytes=reserve)


def _error(code):
    from .experimental_execution_state import StateError
    return StateError(code)


def _checked(raw, checksum):
    value = json.loads(raw) if isinstance(raw, str) else raw
    if not isinstance(value, dict) or life.digest(value) != checksum:
        raise _error('ARCHIVED_HISTORY_INTEGRITY_FAILURE')
    return deepcopy(value)


def _ids(values):
    result = list(dict.fromkeys(values))
    if len(result) > 256:
        raise _error('BOUNDED_HISTORY_LOOKUP_REQUIRED')
    for cid in result:
        life.ident(cid, r'[0-9a-f]{64}')
    return result


def _lane(account, symbol):
    return life.digest([account, symbol])


def _eligible(state, cid, now):
    source = state['sources'][cid]
    # Unknown extension obligations are retained conservatively. An archive
    # may never infer that an outstanding notification or action was delivered.
    if any(state.get(k) for k in ('pending_actions', 'pending_notifications', 'notification_outbox', 'outbox')):
        return False
    trade = state['trades'].get(cid)
    requests = [r for r in state['requests'].values() if r['proposal']['card_id'] == cid]
    if trade is None:
        if requests:
            return False
        msg = source['source']
        if source['entry_permission'] == 'RETIRED':
            return True
        deadline = msg.get('expires_at') if contract.is_approved(msg) else msg.get('valid_until')
        return deadline is not None and now >= contract.moment_ms(deadline)
    if trade['phase'] not in FINAL or trade.get('pending_notifications') or trade.get('pending_actions'):
        return False
    if trade['phase']=='MANUALLY_CLOSED':
        from .experimental_external_activity import closure_verified
        return closure_verified(trade) and all(r['phase'] in ('OBSERVED','ABORTED_UNSENT') for r in requests)
    lane = _lane(trade['account'], trade['symbol'])
    if state.get('blocked_lanes', {}).get(lane):
        return False
    entry = sum((Decimal(f['quantity']) for f in trade['entry_fills'].values()), Decimal(0))
    exits = sum((Decimal(f['quantity']) for f in trade['exit_fills'].values()), Decimal(0))
    if entry != exits or any(o['status'] not in TERMINAL for o in trade['orders'].values()):
        return False
    if trade['phase'] == 'CLOSED' and (entry <= 0 or not any(
            e.get('occurrence_id') == cid and e.get('kind') == 'CLOSED' for e in state['events'])):
        return False
    for request in requests:
        if request['phase'] == 'ABORTED_UNSENT':
            continue
        if request['phase'] != 'OBSERVED':
            return False
        oid = request.get('observed_oid')
        if oid is None:
            if request.get('terminal_state') not in ('REJECTED_NO_ORDER', 'REJECTED_NO_EFFECT'):
                return False
        elif oid not in trade['orders'] or trade['orders'][oid]['status'] not in TERMINAL:
            return False
    return True


def _bundle(state, cid, now):
    trade = state['trades'].get(cid)
    requests = {rid: deepcopy(r) for rid, r in state['requests'].items() if r['proposal']['card_id'] == cid}
    result = dict(version=VERSION, domain=state['domain'], occurrence_id=cid,
        archived_at_ms=now, state_revision=state['revision'], source=deepcopy(state['sources'][cid]),
        trade=deepcopy(trade), requests=requests,
        events=[deepcopy(e) for e in state['events'] if e.get('occurrence_id') == cid or e.get('request_id') in requests],
        auxiliary={k:deepcopy(state[k][cid]) for k in EXTRAS if cid in state.get(k,{})},
        snapshots={})
    if trade or state['sources'].get(cid):
        msg = state['sources'][cid]['source']
        account = trade['account'] if trade else state['routes']['long_account' if msg['side']=='LONG' else 'short_account']
        lane = _lane(account, msg['symbol'])
        # Capture the complete checkpoints before pruning. These are audit
        # evidence, never rehydrated into active ownership after archiving.
        for key in ('snapshots', 'collector_checkpoints'):
            if lane in state.get(key, {}):
                result['snapshots'][key] = deepcopy(state[key][lane])
    if trade and trade['phase']=='MANUALLY_CLOSED':
        result['snapshots']['collector_checkpoints']=deepcopy(trade['manual_closure_snapshot'])
        result['snapshots'].pop('snapshots',None)
    return result


def _prune(state, bundle):
    cid = bundle['occurrence_id']; trade = bundle['trade']
    state['sources'].pop(cid)
    state['trades'].pop(cid, None)
    for rid in bundle['requests']:
        state['requests'].pop(rid)
    state['events'] = [e for e in state['events'] if e.get('occurrence_id') != cid and e.get('request_id') not in bundle['requests']]
    for key in EXTRAS:
        state.get(key, {}).pop(cid, None)
    if trade:
        lane = _lane(trade['account'], trade['symbol']); ids = set(trade['orders'])
        for key in ('snapshots', 'collector_checkpoints'):
            snap = state.get(key, {}).get(lane)
            if snap:
                for field in ('orders', 'fills', 'open_orders', 'terminal_orders'):
                    if field in snap:
                        snap[field] = [r for r in snap[field] if str(r.get('oid')) not in ids]
    msg = bundle['source']['source']
    account = state['routes']['long_account' if msg['side']=='LONG' else 'short_account']
    lane = _lane(account,msg['symbol'])
    hot_lane = any(s['source']['symbol']==msg['symbol'] and s['source']['side']==msg['side'] for s in state['sources'].values())
    hot_lane = hot_lane or any(t['account']==account and t['symbol']==msg['symbol'] for t in state['trades'].values())
    if not hot_lane:
        for key in ('snapshots','collector_checkpoints'):
            snap = state.get(key,{}).get(lane)
            if snap and Decimal(snap['position_quantity']) == 0 and not any(snap.get(k) for k in ('orders','fills','open_orders','terminal_orders')):
                state[key].pop(lane)
    # Preserve len-derived request identities, including exact final-send
    # replay that temporarily removes the currently checked request.
    state['archived_request_count'] = state.get('archived_request_count', 0) + len(bundle['requests'])


class _SQL:
    def __init__(self, conn, schema=None):
        self.conn, self.schema = conn, schema
        self.p = '%s' if schema else '?'
    def table(self, name):
        return (self.schema + '.' if self.schema else 'experimental_') + name
    def execute(self, sql, args=()):
        return self.conn.execute(sql.replace('?', self.p), args)
    def initialize(self):
        for name, definition in (
            ('history', 'occurrence_id TEXT PRIMARY KEY, value TEXT NOT NULL, checksum TEXT NOT NULL, source TEXT NOT NULL, source_checksum TEXT NOT NULL'),
            ('history_updates', 'occurrence_id TEXT NOT NULL, revision BIGINT NOT NULL, value TEXT NOT NULL, checksum TEXT NOT NULL, PRIMARY KEY(occurrence_id, revision)'),
            ('history_orders', 'account TEXT NOT NULL, symbol TEXT NOT NULL, oid TEXT NOT NULL, occurrence_id TEXT NOT NULL, value TEXT NOT NULL, checksum TEXT NOT NULL, PRIMARY KEY(account, symbol, oid)')):
            self.execute(f'CREATE TABLE IF NOT EXISTS {self.table(name)} ({definition})')
            if self.schema:
                self.execute(f'REVOKE ALL ON {self.table(name)} FROM PUBLIC')
    def get(self, cid):
        row = self.execute(f'SELECT value,checksum,source,source_checksum FROM {self.table("history")} WHERE occurrence_id=?', (cid,)).fetchone()
        if row is None:
            return None
        result = _checked(*row[:2]); result['current_source'] = _checked(*row[2:])
        return result
    def insert(self, bundle):
        source = deepcopy(bundle['source'])
        source.update(entry_permission='RETIRED', terminal_reason=source.get('terminal_reason') or 'ARCHIVED_TERMINAL_OCCURRENCE')
        self.execute(f'INSERT INTO {self.table("history")} VALUES(?,?,?,?,?)',
            (bundle['occurrence_id'], life.encoded(bundle), life.digest(bundle), life.encoded(source), life.digest(source)))
        trade = bundle['trade']
        if trade:
            for oid, row in trade['orders'].items():
                checkpoint = bundle['snapshots'].get('collector_checkpoints', {})
                evidence = dict(order=row,
                    collector_terminal=next((r for r in checkpoint.get('terminal_orders', []) if r['oid'] == oid), None),
                    collector_fills=[r for r in checkpoint.get('fills', []) if r['oid'] == oid])
                self.execute(f'INSERT INTO {self.table("history_orders")} VALUES(?,?,?,?,?,?)',
                    (trade['account'], trade['symbol'], oid, bundle['occurrence_id'], life.encoded(evidence), life.digest(evidence)))
    def update_source(self, cid, source, events):
        value = dict(source=source, events=events)
        self.execute(f'INSERT INTO {self.table("history_updates")} VALUES(?,?,?,?)',
            (cid, source['revision'], life.encoded(value), life.digest(value)))
        self.execute(f'UPDATE {self.table("history")} SET source=?,source_checksum=? WHERE occurrence_id=?',
            (life.encoded(source), life.digest(source), cid))


class HistoryMixin:
    """Store-specific fields below select the existing transaction and validator."""
    @contextmanager
    def _history_snapshot(self):
        """One checked read snapshot, without the trading writer lock.

        PostgreSQL needs REPEATABLE READ because the card spans state, archive
        and source updates. Use a separate connection: the journal's normal
        transaction has already queried the database before yielding, and a
        leased writer connection must not be borrowed for presentation reads.
        SQLite keeps its configured journal mode; query_only is connection-local.
        """
        from .experimental_execution_state import checked as software_checked
        if hasattr(self, 'path'):
            with closing(self._connect()) as conn:
                try:
                    conn.execute('PRAGMA query_only=ON')
                    conn.execute('BEGIN')
                    row = conn.execute('SELECT value,checksum FROM experimental_state WHERE singleton=1').fetchone()
                    if row is None: raise _error('EXPLICIT_ISOLATED_INITIALIZATION_REQUIRED')
                    yield _SQL(conn), software_checked(*row)
                finally:
                    conn.rollback()
            return
        if self.domain == 'testnet':
            from .experimental_live_state import checked, PG_SCHEMA
        else:
            from .experimental_execution_state import PG_SCHEMA
            checked = software_checked
        from .postgres_journal import JournalError
        # Keep the journal's identity and redacted failure policy, but configure
        # the transaction before its first SELECT establishes a snapshot.
        try:
            with self.journal._connect() as conn:
                conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
                actual = conn.execute('SELECT current_database()').fetchone()[0]
                if actual != self.journal._parameters['dbname']:
                    raise JournalError('WRONG_DATABASE')
                if self.domain == 'testnet': self._ready(conn)
                row = conn.execute(f'SELECT value,checksum FROM {PG_SCHEMA}.state WHERE singleton').fetchone()
                if row is None: raise _error('EXPLICIT_ISOLATED_INITIALIZATION_REQUIRED')
                yield _SQL(conn, PG_SCHEMA), checked(*row)
        except JournalError:
            raise
        except Exception:
            raise JournalError('PERSISTENCE_UNAVAILABLE_NO_SEND') from None

    @contextmanager
    def _history_transaction(self):
        from .experimental_execution_state import checked as software_checked, encode
        if hasattr(self, 'path'):
            with closing(self._connect()) as conn:
                try:
                    conn.execute('BEGIN IMMEDIATE')
                    row = conn.execute('SELECT value,checksum FROM experimental_state WHERE singleton=1').fetchone()
                    if row is None: raise _error('EXPLICIT_ISOLATED_INITIALIZATION_REQUIRED')
                    state = software_checked(*row)
                    immutable = {k:deepcopy(state[k]) for k in ('version','domain','routes','not_before_ms','revision')}
                    def save():
                        if any(state.get(k)!=v for k,v in immutable.items()):
                            raise _error('ISOLATED_STATE_IDENTITY_CHANGED')
                        state['revision'] += 1
                        conn.execute('UPDATE experimental_state SET value=?,checksum=? WHERE singleton=1', (encode(state), life.digest(state)))
                    yield _SQL(conn), state, save
                    conn.commit()
                except BaseException:
                    conn.rollback(); raise
            return
        if self.domain == 'testnet':
            from .experimental_live_state import checked, PG_SCHEMA, LOCK
        else:
            from .experimental_execution_state import PG_SCHEMA
            checked, LOCK = software_checked, 1729048361
        with self.journal._transaction() as conn:
            conn.execute('SELECT pg_advisory_xact_lock(%s)', (LOCK,))
            if self.domain == 'testnet': self._ready(conn)
            row = conn.execute(f'SELECT value,checksum FROM {PG_SCHEMA}.state WHERE singleton FOR UPDATE').fetchone()
            if row is None: raise _error('EXPLICIT_ISOLATED_INITIALIZATION_REQUIRED')
            state = checked(*row)
            immutable = {k:deepcopy(state[k]) for k in ('version','domain','routes','agents','not_before_ms','revision') if k in state}
            def save():
                if any(state.get(k)!=v for k,v in immutable.items()):
                    raise _error('TESTNET_STATE_IDENTITY_CHANGED')
                state['revision'] += 1
                checked(state, life.digest(state))
                conn.execute(f'UPDATE {PG_SCHEMA}.state SET value=%s::jsonb,checksum=%s WHERE singleton', (encode(state), life.digest(state)))
            yield _SQL(conn, PG_SCHEMA), state, save

    def initialize_history(self):
        """Explicit additive migration. Never called from a recurring cycle."""
        with self._history_transaction() as (db, state, save):
            if state.get('history'):
                if state['history'].get('version') != VERSION:
                    raise _error('HISTORY_SCHEMA_REVIEW_REQUIRED')
                return False
            db.initialize()
            state['history'] = dict(version=VERSION, archived_count=0, archived_trades=0,
                archived_closed=0, last_maintenance_ms=None, cursor='')
            state.setdefault('archived_request_count', 0)
            save()
        return True

    def mutate_sources(self, transition, occurrence_ids):
        """Atomic receive against hot records or indexed cold tombstones."""
        ids = _ids(occurrence_ids)
        if not self.load().get('history'):
            def unmigrated(value):
                # Initialization and archival may win after the optimistic
                # read. Never admit an occurrence without checking cold state.
                if value.get('history'):
                    raise _error('HISTORY_INITIALIZATION_CHANGED_RETRY')
                return transition(value)
            return self.mutate(unmigrated)
        with self._history_transaction() as (db, state, save):
            archived = {}
            if state.get('history'):
                for cid in ids:
                    if cid not in state['sources']:
                        record = db.get(cid)
                        if record:
                            archived[cid] = record['current_source']
                            state['sources'][cid] = deepcopy(record['current_source'])
            result = transition(state)
            for cid, previous in archived.items():
                current = state['sources'].pop(cid)
                if cid in state['trades'] or any(r['proposal']['card_id'] == cid for r in state['requests'].values()):
                    raise _error('ARCHIVED_OCCURRENCE_CANNOT_REENTER')
                if current['entry_permission'] != 'RETIRED':
                    raise _error('ARCHIVED_OCCURRENCE_CANNOT_REENTER')
                events = [e for e in state['events'] if e.get('occurrence_id') == cid]
                if current != previous:
                    db.update_source(cid, current, events)
                elif events:
                    raise _error('ARCHIVED_DUPLICATE_CANNOT_CREATE_EVENT')
                state['events'] = [e for e in state['events'] if e.get('occurrence_id') != cid]
                for key in EXTRAS: state.get(key, {}).pop(cid, None)
            save()
        return deepcopy(result)

    def compact_history(self, *, now_ms, batch_size=MAX_BATCH, scan_limit=MAX_SCAN, force=False):
        life.moment(now_ms)
        if type(batch_size) is not int or not 1 <= batch_size <= MAX_BATCH or type(scan_limit) is not int or not 1 <= scan_limit <= MAX_SCAN:
            raise _error('BOUNDED_HISTORY_BATCH_REQUIRED')
        history = self.load().get('history')
        if not history:
            return dict(status='EXPLICIT_HISTORY_INITIALIZATION_REQUIRED', archived=0)
        prior = history['last_maintenance_ms']
        if prior is not None and now_ms < prior:
            raise _error('HISTORY_CLOCK_REGRESSION')
        if not force and prior is not None and now_ms-prior < INTERVAL_MS:
            return dict(status='HISTORY_INTERVAL_NOT_DUE', archived=0)
        with self._history_transaction() as (db, state, save):
            history = state.get('history')
            if not history:
                return dict(status='EXPLICIT_HISTORY_INITIALIZATION_REQUIRED', archived=0)
            prior = history['last_maintenance_ms']
            if prior is not None and now_ms < prior:
                raise _error('HISTORY_CLOCK_REGRESSION')
            if not force and prior is not None and now_ms-prior < INTERVAL_MS:
                return dict(status='HISTORY_INTERVAL_NOT_DUE', archived=0)
            ids = sorted(state['sources'])
            candidates = [cid for cid in ids if cid > history['cursor']][:scan_limit]
            if not candidates: candidates = ids[:scan_limit]
            archived = total_bytes = oversize = scanned = 0
            for cid in candidates:
                scanned += 1
                history['cursor'] = cid
                if not _eligible(state, cid, now_ms): continue
                record = _bundle(state, cid, now_ms)
                size = len(life.encoded(record).encode())
                if size > MAX_RECORD_BYTES:
                    oversize += 1
                    continue
                # A single already-bounded hot record can exceed the normal
                # batch budget. Archive it alone rather than starve forever.
                if archived and total_bytes+size > MAX_BATCH_BYTES: break
                db.insert(record); _prune(state, record)
                history['archived_count'] += 1
                history['archived_trades'] += int(record['trade'] is not None)
                history['archived_closed'] += int(record['trade'] is not None and record['trade']['phase'] in ('CLOSED','MANUALLY_CLOSED'))
                archived += 1; total_bytes += size
                if archived >= batch_size or total_bytes >= MAX_BATCH_BYTES: break
            history['last_maintenance_ms'] = now_ms
            save()
        return dict(status='HISTORY_RECORD_CAPACITY_REVIEW_REQUIRED' if oversize else 'HISTORY_MAINTENANCE_COMPLETE',
            archived=archived, scanned=scanned, bytes=total_bytes, oversized_records=oversize)

    def archive_record(self, occurrence_id):
        cid = _ids([occurrence_id])[0]
        if not self.load().get('history'): return None
        with self._history_transaction() as (db, state, _save):
            return db.get(cid) if state.get('history') else None

    def trade_card(self, occurrence_id, *, after_update_revision=0, update_limit=100):
        """Read one durable card atomically across archival; never revive it.

        The card is the exact pre-archive projection. Later source cancellations
        remain separately visible in current_source and paged source_updates.
        A source-only occurrence returns None: it has not become a trade.
        """
        from .experimental_execution_cards import _card, card_from_record
        cid = _ids([occurrence_id])[0]
        if (type(after_update_revision) is not int or after_update_revision < 0
                or type(update_limit) is not int or not 1 <= update_limit <= 100):
            raise _error('BOUNDED_HISTORY_PAGE_REQUIRED')
        with self._history_snapshot() as (db, state):
            if cid in state['trades']:
                return dict(card=_card(state, cid), location='active',
                    snapshot_revision=state['revision'],
                    current_source=deepcopy(state['sources'].get(cid)),
                    source_updates=[], next_update_revision=None, archived_at_ms=None)
            if not state.get('history'):
                return None
            record = db.get(cid)
            if record is None or record['trade'] is None:
                return None
            rows = db.execute(f'SELECT revision,value,checksum FROM {db.table("history_updates")} WHERE occurrence_id=? AND revision>? ORDER BY revision LIMIT ?',
                (cid, after_update_revision, update_limit+1)).fetchall()
            return dict(card=card_from_record(record, domain=self.domain), location='archived',
                snapshot_revision=state['revision'], current_source=record['current_source'],
                source_updates=[_checked(raw, checksum) for _revision,raw,checksum in rows[:update_limit]],
                next_update_revision=rows[update_limit-1][0] if len(rows)>update_limit else None,
                archived_at_ms=record['archived_at_ms'])

    def history_page(self, *, after='', limit=100):
        if after: _ids([after])
        if type(limit) is not int or not 1 <= limit <= 100:
            raise _error('BOUNDED_HISTORY_PAGE_REQUIRED')
        if not self.load().get('history'): return dict(records=[], next_cursor=None, archived_count=0)
        with self._history_transaction() as (db, state, _save):
            if not state.get('history'): return dict(records=[], next_cursor=None, archived_count=0)
            rows = db.execute(f'SELECT occurrence_id,value,checksum FROM {db.table("history")} WHERE occurrence_id>? ORDER BY occurrence_id LIMIT ?', (after,limit+1)).fetchall()
            records = [_checked(raw,checksum) for _cid,raw,checksum in rows[:limit]]
            return dict(records=records, next_cursor=rows[limit-1][0] if len(rows)>limit else None,
                archived_count=state['history']['archived_count'])

    def history_updates_page(self, occurrence_id, *, after_revision=0, limit=100):
        cid = _ids([occurrence_id])[0]
        if type(after_revision) is not int or after_revision < 0 or type(limit) is not int or not 1 <= limit <= 100:
            raise _error('BOUNDED_HISTORY_PAGE_REQUIRED')
        if not self.load().get('history'): return dict(records=[],next_revision=None)
        with self._history_transaction() as (db,state,_save):
            rows = db.execute(f'SELECT revision,value,checksum FROM {db.table("history_updates")} WHERE occurrence_id=? AND revision>? ORDER BY revision LIMIT ?', (cid,after_revision,limit+1)).fetchall()
            return dict(records=[_checked(raw,checksum) for _revision,raw,checksum in rows[:limit]],
                next_revision=rows[limit-1][0] if len(rows)>limit else None)

    def archived_order_evidence(self, account, symbol, order_ids):
        ids = list(dict.fromkeys(str(oid) for oid in order_ids))
        if len(ids)>10000: raise _error('BOUNDED_HISTORY_ORDER_LOOKUP_REQUIRED')
        life.address(account); life.ident(symbol, r'[A-Z][A-Z0-9]{0,19}')
        for oid in ids: life.ident(oid, r'[1-9][0-9]{0,19}')
        if not self.load().get('history') or not ids: return {}
        with self._history_transaction() as (db, state, _save):
            if not state.get('history') or not ids: return {}
            result = {}
            for offset in range(0,len(ids),256):
                group = ids[offset:offset+256]
                query = f'SELECT oid,value,checksum FROM {db.table("history_orders")} WHERE account=? AND symbol=? AND oid IN ({",".join("?" for _ in group)})'
                for oid,raw,checksum in db.execute(query,(account,symbol,*group)).fetchall():
                    result[oid] = _checked(raw,checksum)
            return result

    def archived_orders(self, account, symbol, order_ids):
        return {oid:record['order'] for oid,record in self.archived_order_evidence(account,symbol,order_ids).items()}

    def strip_archived_collector_snapshot(self, snapshot):
        value = deepcopy(snapshot)
        ids = [r['oid'] for field in ('fills','open_orders','terminal_orders') for r in value[field]]
        known = self.archived_order_evidence(value['account'],value['symbol'],ids)
        for row in value['open_orders']:
            if row['oid'] in known: raise _error('ARCHIVED_TERMINAL_ORDER_BECAME_ACTIVE')
        for row in value['terminal_orders']:
            if row['oid'] in known and row != known[row['oid']]['collector_terminal']:
                raise _error('ARCHIVED_TERMINAL_ORDER_CHANGED')
        for row in value['fills']:
            if row['oid'] in known:
                old = {r['fill_id']:r for r in known[row['oid']]['collector_fills']}
                if old.get(row['fill_id']) != row:
                    raise _error('ARCHIVED_TERMINAL_FILL_CHANGED')
        for field in ('fills','terminal_orders'):
            value[field] = [r for r in value[field] if r['oid'] not in known]
        return value

    def strip_archived_orders(self, context):
        """Verify immutable old rows before removing them from active snapshots."""
        value = deepcopy(context)
        for snapshot in value.get('snapshots', []):
            known = self.archived_orders(snapshot['account'], snapshot['symbol'], (r['oid'] for r in snapshot['orders']))
            for row in snapshot['orders']:
                if row['oid'] in known and row != known[row['oid']]:
                    raise _error('ARCHIVED_TERMINAL_ORDER_CHANGED')
            snapshot['orders'] = [r for r in snapshot['orders'] if r['oid'] not in known]
        return value
