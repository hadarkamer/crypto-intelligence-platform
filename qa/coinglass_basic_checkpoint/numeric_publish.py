"""Deterministically format verified BASIC source that survived a fresh database connection."""
from base64 import b64decode
from hashlib import sha256
import json
import os
from pathlib import Path
import sys
import time

from retained_evidence import unpack
from qa_job_server import write_once
from modern_map_task import numeric_result


def read_source(job, digest, oid):
    from psycopg import connect
    from psycopg.rows import dict_row
    from market_vault import retrieve
    dsn = os.environ.pop('DATABASE_URL')
    key = b64decode(os.environ.pop('COINGLASS_SESSION_KEY'), validate=True)
    with connect(dsn, autocommit=True, connect_timeout=5, row_factory=dict_row) as db:
        db.execute('SET default_transaction_read_only = on')
        return retrieve(db, key, job, digest, oid)


def publish(job, digest, oid):
    began = time.monotonic()
    data = read_source(job, digest, oid)
    snapshot, png = unpack(data, job)
    chart = sha256(png).hexdigest()
    if sha256(data).hexdigest() != digest or snapshot['source_sha256'] != chart:
        raise ValueError('source_bytes_mismatch')
    normal = numeric_result(snapshot, job, png)
    if (normal.get('result_type') != 'verified_basic' or normal.get('basic_snapshot') != snapshot
            or normal.get('run_id') != job or normal.get('evidence', {}).get('sha256') != chart):
        raise ValueError('invalid_numeric_result')
    write_once('numeric-publication.json', dict(job_id=job, source_sha256=digest, chart_sha256=chart,
                 result=normal, private_source_reopened=True, model_calls=0,
                 wall_seconds=round(time.monotonic()-began, 4)))


if __name__=='__main__':
    publish(sys.argv[1], sys.argv[2], int(sys.argv[3]))
