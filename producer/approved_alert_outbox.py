"""Explicit-migration durable approved-alert transport outbox.

The source owns approval and cancellation facts. Receiver acknowledgement only
advances transport state, never creates a source or exchange fill. Completed
cancellations compact into permanent identity tombstones, so notification
history pruning and process restarts cannot recreate an old occurrence.
"""
import json

import approved_alert_contract as contract
from approved_alert_producer import approved_messages

TABLE = 'approved_alert_execution_outbox'
MAX_PENDING = 8192


def synchronize(conn, source_key, state, now_ms):
    """Called inside the existing source transaction; no DDL/commit/network."""
    rows = conn.execute(
        'SELECT occurrence_id,source_position_id,payload_json,source_sequence,canceled '
        'FROM approved_alert_execution_outbox WHERE source_key=%s AND NOT canceled',
        (source_key,)).fetchall()
    positions = {p['position_id']: p for p in state.get('active', []) + state.get('history', [])}
    records = {v['occurrence_id']: v for v in approved_messages(state, now_ms=now_ms, fence_ms=1)}
    ids = {v['occurrence_id']: i['position_id'] for i in state.get('intents', [])
           for v in approved_messages(dict(state, intents=[i]), now_ms=now_ms, fence_ms=1)}
    # The durable row survives removal of source history/intents. Missing or
    # ended source position withdraws the remainder, never closes a venue fill.
    for row in rows:
        value = json.loads(row['payload_json'])
        observed = positions.get(row['source_position_id'])
        if observed is not None and observed.get('status') == 'OPEN':
            continue
        as_of = max(contract.moment_ms(value['approved_at']),
                    ((observed or {}).get('terminal_ms') or (observed or {}).get('unknown_ms')
                     or state['bar_cursor_ms']) + 60_000)
        if as_of > now_ms:
            raise contract.ContractError('FUTURE_SOURCE_STATE')
        value.update(kind='CANCEL', source_state='CANCELED', source_as_of=contract.iso_ms(as_of),
                     source_sequence=as_of*10+contract.RANK['CANCEL'],
                     cancel_reason='SOURCE_OBSERVATION_ENDED')
        records[value['occurrence_id']] = contract.validate(value)
        ids[value['occurrence_id']] = row['source_position_id']
    if not records:
        return False
    # Serialize admission accounting across source scopes. Source locks are
    # taken first and every transaction owns just one source scope, so this
    # global capacity lock cannot introduce an inverse lock acquisition.
    conn.execute("SELECT pg_advisory_xact_lock(hashtext('approved-alert-outbox-capacity-v2'))")
    count = conn.execute('SELECT count(*) AS count FROM approved_alert_execution_outbox '
                         'WHERE payload_json IS NOT NULL').fetchone()['count']
    existing = {row['occurrence_id'] for row in conn.execute(
        'SELECT occurrence_id FROM approved_alert_execution_outbox '
        'WHERE source_key=%s AND occurrence_id=ANY(%s)', (source_key, list(records))).fetchall()}
    changed = False
    for identity, value in records.items():
        if identity not in existing and count >= MAX_PENDING:
            # Capacity never blocks a cancellation/replay of a retained row.
            # New, unsubmitted approvals fail closed; no source decision rolls
            # back and this record can be retried within its original deadline.
            print('[approved-execution] outbox admission capacity reached', flush=True)
            continue
        raw = json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)
        row = conn.execute(
            'INSERT INTO approved_alert_execution_outbox '
            '(source_key,occurrence_id,source_position_id,payload_json,source_sequence,canceled) '
            'VALUES(%s,%s,%s,%s,%s,%s) '
            'ON CONFLICT(source_key,occurrence_id) DO UPDATE SET '
            'payload_json=EXCLUDED.payload_json,source_sequence=EXCLUDED.source_sequence,'
            'canceled=EXCLUDED.canceled '
            'WHERE NOT approved_alert_execution_outbox.canceled '
            'AND EXCLUDED.source_sequence>approved_alert_execution_outbox.source_sequence '
            'RETURNING occurrence_id',
            (source_key, identity, ids[identity], raw, value['source_sequence'], value['kind'] == 'CANCEL')).fetchone()
        changed = changed or row is not None
        if row is not None and identity not in existing:
            count += 1
    return changed


def read(conn, source_keys):
    result = []
    for key in source_keys:
        rows = conn.execute(
            'SELECT payload_json FROM approved_alert_execution_outbox '
            'WHERE source_key=%s AND acknowledged_sequence<source_sequence '
            'ORDER BY canceled DESC,source_sequence LIMIT %s', (key, MAX_PENDING)).fetchall()
        for row in rows:
            raw = row['payload_json']
            if not isinstance(raw, str) or len(raw.encode()) > contract.MAX_BYTES:
                raise contract.ContractError('APPROVED_OUTBOX_PAYLOAD')
            result.append(contract.validate(json.loads(raw)))
    return result


def acknowledge(conn, source_keys, value):
    """Exact revision acknowledgement cannot erase a newer cancellation."""
    conn.execute(
        'UPDATE approved_alert_execution_outbox SET acknowledged_sequence=%s,'
        'payload_json=CASE WHEN canceled THEN NULL ELSE payload_json END '
        'WHERE source_key=ANY(%s) AND occurrence_id=%s AND source_sequence=%s '
        'AND acknowledged_sequence<source_sequence',
        (value['source_sequence'], list(source_keys), value['occurrence_id'], value['source_sequence']))
