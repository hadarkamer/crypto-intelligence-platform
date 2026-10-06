"""Transaction-scoped, finite keyset laps over immutable source event IDs."""
from __future__ import annotations


def claim_event_page(conn, queue_key, *, limit, predicate, params=()):
    """Advance only with the caller's successful transaction.

    A lap fixes its high-water ID. New inserts cannot indefinitely extend it;
    the next lap recovers old rows whose delivery state or outcomes changed.
    ``predicate`` is static, internal SQL supplied by the owning worker.
    """
    conn.execute('''INSERT INTO research_event_scan_cursors(queue_key)
        VALUES(%s) ON CONFLICT DO NOTHING''', (queue_key,))
    state = conn.execute('''SELECT last_event_id,high_water_event_id
        FROM research_event_scan_cursors WHERE queue_key=%s FOR UPDATE''',
        (queue_key,)).fetchone()
    cursor, high = int(state['last_event_id']), int(state['high_water_event_id'])
    if cursor >= high:
        cursor = 0
        latest = conn.execute('''SELECT event_id FROM research_events
            ORDER BY event_id DESC LIMIT 1''').fetchone()
        high = int(latest['event_id']) if latest else 0
    cap = max(1, min(int(limit), 1024))
    rows = conn.execute(f'''SELECT e.event_id FROM research_events e
        WHERE e.event_id>%s AND e.event_id<=%s AND ({predicate})
        ORDER BY e.event_id LIMIT %s''', (cursor, high, *params, cap)).fetchall()
    ids = [int(row['event_id']) for row in rows]
    next_id = ids[-1] if len(ids) == cap else high
    conn.execute('''UPDATE research_event_scan_cursors
        SET last_event_id=%s,high_water_event_id=%s,updated_at_utc=NOW()
        WHERE queue_key=%s''', (next_id, high, queue_key))
    return ids


def retain_unprocessed_tail(conn, queue_key, last_selected_id):
    """Leave an unprocessed page suffix inside its original finite lap."""
    row = conn.execute('''UPDATE research_event_scan_cursors
        SET last_event_id=%s,updated_at_utc=NOW()
        WHERE queue_key=%s AND last_event_id>=%s
        RETURNING last_event_id''',
        (int(last_selected_id), queue_key, int(last_selected_id))).fetchone()
    if row is None:
        raise RuntimeError('Cannot retain an unclaimed source page suffix')
