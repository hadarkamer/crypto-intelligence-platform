"""Private incident tracking and read-only status for classic collector jobs."""
from __future__ import annotations

import asyncio
import hmac
import os
from uuid import UUID, uuid4


class LoginIncidentError(Exception):
    """Non-sensitive error code for worker-side handling."""


def waiting_incident(conn, symbol, model, timeframe):
    with conn.transaction():
        row = conn.execute("""
        SELECT i.id FROM collector_private.coinglass_login_incidents i
        JOIN collector_private.coinglass_login_jobs j ON j.incident_id=i.id
        WHERE i.resolved_at IS NULL AND j.symbol=%s AND j.model=%s AND j.timeframe=%s
        LIMIT 1
        """, (symbol, model, timeframe)).fetchone()
    return str(row['id']) if row else None


def record_login_required(conn, symbol, model, timeframe, job_id):
    """Worker-only. Called while it still owns WORK_LOCK, for an explicit page gate."""
    with conn.transaction():
        prior = conn.execute("""
            SELECT j.incident_id,j.symbol,j.model,j.timeframe,i.resolved_at
            FROM collector_private.coinglass_login_jobs j JOIN collector_private.coinglass_login_incidents i
                ON i.id=j.incident_id WHERE j.job_id=%s
            """, (job_id,)).fetchone()
        if prior:
            if (prior['symbol'], prior['model'], prior['timeframe']) != (symbol, model, timeframe):
                raise LoginIncidentError('login_job_identity_mismatch')
            if prior['resolved_at'] is None:
                return str(prior['incident_id'])
        incident = conn.execute("""
            INSERT INTO collector_private.coinglass_login_incidents (id) VALUES (%s)
            ON CONFLICT ((true)) WHERE resolved_at IS NULL DO UPDATE SET
                opened_at=collector_private.coinglass_login_incidents.opened_at
            RETURNING id
            """, (str(uuid4()),)).fetchone()
        incident_id = incident['id']
        conn.execute("""
            INSERT INTO collector_private.coinglass_login_jobs
                (job_id,incident_id,symbol,model,timeframe) VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (job_id) DO UPDATE SET incident_id=EXCLUDED.incident_id
            """, (job_id, incident_id, symbol, model, timeframe))
        return str(incident_id)


def incident_status(conn, incident_id=None, job_id=None):
    projection = """
        SELECT id, human_wait_ms, CASE WHEN resolved_at IS NOT NULL THEN 'resolved'
            WHEN window_expires_at > clock_timestamp() THEN 'in_progress'
            ELSE 'waiting' END AS status
        FROM collector_private.coinglass_login_incidents
        """
    if job_id is not None:
        row = conn.execute(projection + ' WHERE id=(SELECT incident_id FROM '
                           'collector_private.coinglass_login_jobs WHERE job_id=%s)', (job_id,)).fetchone()
    elif incident_id is None:
        row = conn.execute(projection + ' WHERE resolved_at IS NULL').fetchone()
    else:
        row = conn.execute(projection + ' WHERE id=%s', (incident_id,)).fetchone()
    data = {'status': row['status'] if row else 'none',
            'incident_id': str(row['id']) if row else None}
    if row and row['status']=='resolved' and row['human_wait_ms'] is not None:
        data['human_wait_ms'] = row['human_wait_ms']
    return data


def register_legacy_status(application, store, active):
    from aiohttp import web

    headers = {'Cache-Control': 'no-store', 'Referrer-Policy': 'no-referrer',
               'X-Content-Type-Options': 'nosniff'}
    bridge_secret = os.getenv('COINGLASS_COLLECTOR_TOKEN', '')
    managed = os.getenv('COINGLASS_SESSION_STORE_ENABLED', '').lower() == 'true'

    def authorize(request):
        presented = request.headers.get('Authorization', '')
        if (len(bridge_secret.encode()) < 32 or len(presented) > 1024 or
                not hmac.compare_digest(presented.encode(), ('Bearer ' + bridge_secret).encode())):
            raise web.HTTPUnauthorized(headers=headers)

    def read_status(incident_id, job_id):
        with store.connect() as conn:
            return incident_status(conn, incident_id, job_id)

    def respond(data, status=200):
        return web.json_response(data, headers=headers, status=status)

    async def status(request):
        authorize(request)
        if not (managed and active and store):
            return respond({'status': 'unavailable', 'incident_id': None, 'can_launch': False})
        incident_id = request.query.get('incident_id')
        job_id = request.query.get('job_id')
        if incident_id is not None and job_id is not None:
            return respond({'error': 'invalid_status_selector'}, 400)
        selector = incident_id if incident_id is not None else job_id
        if selector is not None:
            try:
                UUID(selector)
            except ValueError:
                return respond({'error': 'invalid_status_selector'}, 400)
        try:
            data = await asyncio.to_thread(read_status, incident_id, job_id)
        except Exception:
            return respond({'status': 'unavailable', 'incident_id': None, 'can_launch': False}, 503)
        return respond({**data, 'can_launch': False})

    application.router.add_get('/api/collection/coinglass/session-status', status)
