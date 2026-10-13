#!/usr/bin/env python3
"""Correctness-only verification on one owned disposable PostgreSQL connection."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace
from uuid import uuid4

sys.dont_write_bytecode = True
REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from ordered_postgres_memory_replay import (
    canonical, connect, connection_receipt, run_operation, sha_file, source_hashes,
)
from ordered_cursor_lifetime_probe import driver_binding, source_semantics_audit
import ordered_postgres_scope_fixture as scope_fixture

VERSION = 'ordered-binding-correctness-v1'


def publish(path, value):
    """Fsync and publish create-only; incomplete files are never PASS receipts."""
    pending = path.with_suffix(path.suffix + '.pending')
    with pending.open('x', encoding='utf-8') as stream:
        stream.write(canonical(value) + '\n')
        stream.flush()
        os.fsync(stream.fileno())
    os.link(pending, path)
    pending.unlink()
    descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def verify(args):
    from psycopg import sql
    receipt_path = Path(args.connection_receipt).resolve()
    host = connection_receipt(receipt_path)
    host_sha = sha_file(receipt_path)
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    sources = source_hashes()
    driver = driver_binding()
    tool_sha = sha_file(__file__)
    schema = 'ordered_memory_replay_' + uuid4().hex[:24]
    conn = None
    try:
        # The only connection creation in this tool: connect verifies the sole
        # owned marker before any schema or fixture write.
        conn = connect(host)
        conn.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
        conn.execute(sql.SQL('SET search_path TO {}').format(sql.Identifier(schema)))
        fixture = scope_fixture.prepare(conn, SimpleNamespace(rows=32))
        conn.commit()
        counts = {}
        full_result = run_operation(conn, 'scope', fixture, [], full_parity=True,
                                    cursor_policy='source', cursor_counts=counts)
        conn.rollback()
        checkpoint = scope_fixture.verify(conn, fixture)
        conn.rollback()
        if (full_result.get('full_parity_verified') is not True
                or full_result.get('reference_parity') is not True
                or counts.get('source_scalar_bindings') != 1
                or counts.get('scalar_conversions') != 0
                or checkpoint.get('fixed_source_cardinality') is not True):
            raise AssertionError('Full source binding correctness contract failed')
        # The audit reuses this same connection, performs five positive cases,
        # and closes it through its existing context manager. No reconnect.
        positive_audit = source_semantics_audit(lambda:conn, 60)
        if positive_audit.get('result') != 'PASS' or len(positive_audit.get('cases', [])) != 5:
            raise AssertionError('Positive source semantics audit did not complete')
    finally:
        if conn is not None:
            conn.close()
    if conn is None or not conn.closed:
        raise AssertionError('Owned connection did not close')
    if (source_hashes() != sources or driver_binding() != driver
            or sha_file(receipt_path) != host_sha or sha_file(__file__) != tool_sha):
        raise AssertionError('Correctness inputs changed during verification')
    manifest = {'version':VERSION, 'kind':'binding_correctness', 'sources':sources,
                'driver_binding':driver, 'run_token':host['run_token'],
                'fixture':fixture, 'expected':full_result,
                'connection_receipt_sha256':host_sha, 'tool_sha256':tool_sha}
    manifest_path = output/'fixture_manifest.json'
    publish(manifest_path, manifest)
    result = {'version':VERSION, 'kind':'binding_correctness', 'result':'PASS',
        'actual_loader':True, 'proxy_rewrites':0, 'measured_passes':0,
        'source_hashes':sources, 'driver_binding':driver, 'tool_sha256':tool_sha,
        'run_token':host['run_token'], 'connection_receipt_sha256':host_sha,
        'fixture_manifest_sha256':sha_file(manifest_path),
        'full_scope_result':full_result, 'source_semantics_audit':positive_audit,
        'cursor_counts':counts, 'checkpoint_verified':True, 'checkpoint':checkpoint,
        'connection_closed':True, 'connection_count':1,
        'limitations':['Correctness only; no memory measurement or production efficacy claim.',
                       'Five positive input cases only; overflow evidence belongs to separate owned-host receipts.']}
    publish(output/'correctness_receipt.json', result)
    print(json.dumps({'result':'PASS','receipt':str(output/'correctness_receipt.json')}, sort_keys=True))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--connection-receipt', required=True)
    parser.add_argument('--output', required=True)
    verify(parser.parse_args())


if __name__ == '__main__':
    main()
