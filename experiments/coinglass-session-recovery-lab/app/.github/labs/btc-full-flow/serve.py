"""Expose the real collector app on loopback; lab admin controls time only."""
import json
import os
from pathlib import Path
import sys

ROOT = Path(os.environ['QUEUE_LAB_COLLECTOR'])
sys.path.insert(0, str(ROOT / 'standalone_collector'))
from aiohttp import web
import psycopg
from app import create_app

application = create_app()

async def control(request):
    state = await request.json()
    assert set(state) == {'now', 'failedMap'}
    from datetime import datetime
    now = datetime.fromisoformat(state['now'].replace('Z', '+00:00')).isoformat()
    with psycopg.connect(os.environ['DATABASE_URL'], autocommit=True) as conn:
        # This database is newly created and local; no production DB access.
        from psycopg import sql
        conn.execute(sql.SQL('alter database queue_lab set lab.now = {}').format(sql.Literal(now)))
    Path(os.environ['QUEUE_LAB_CONTROL']).write_text(json.dumps(state))
    return web.json_response({'ok': True})

application.router.add_post('/lab/control', control)
web.run_app(application, host='127.0.0.1', port=21879, access_log=None)

