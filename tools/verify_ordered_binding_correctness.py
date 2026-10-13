#!/usr/bin/env python3
"""Actual-source correctness and recovery on one owned disposable PostgreSQL host."""
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
import ordered_binding_range_check as range_check
import ordered_postgres_scope_fixture as scope_fixture

VERSION = 'ordered-binding-correctness-v1'
MODULE = 'research_common_window_metrics_store.py'
SOURCE_SHA256 = {
    'baseline':'e1e74fc7cac10b5b454614c242621857ab1ae5d23cdbffe6e20dc961051f5341',
    'fixed':'ac2e05e3157928f9daaa8735c0aee533dc3ce2886251a1242e9344a93ba752fd',
}


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
    import research_common_window_metrics_store as common
    loader_path = Path(common.__file__).resolve()
    loader_sha = sha_file(loader_path)
    source_arm = next((arm for arm, sha in SOURCE_SHA256.items() if sha == loader_sha), None)
    if (loader_path != REPO/MODULE or source_arm is None
            or sources[MODULE] != loader_sha):
        raise AssertionError('Correctness requires an exact pinned application source')
    driver = driver_binding()
    tool_sha = sha_file(__file__)
    schema = 'ordered_memory_replay_' + uuid4().hex[:24]
    conn = None
    try:
        # connect verifies the sole owned marker before any fixture write.
        conn = connect(host)
        conn.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
        conn.execute(sql.SQL('SET search_path TO {}').format(sql.Identifier(schema)))
        fixture = scope_fixture.prepare(conn, SimpleNamespace(rows=32))
        conn.commit()
        counts = {}
        full_result = run_operation(conn, 'scope', fixture, [], full_parity=True,
                                    cursor_policy='observed', cursor_counts=counts)
        conn.rollback()
        checkpoint = scope_fixture.verify(conn, fixture)
        conn.rollback()
        if (full_result.get('full_parity_verified') is not True
                or full_result.get('reference_parity') is not True
                or counts != {'queries':4, 'fetchone':2, 'fetchall':2, 'array_targets':1,
                    'scalar_conversions':0, 'explicit_closes':0,
                    'observed_list_bindings':int(source_arm == 'baseline'),
                    'observed_scalar_bindings':int(source_arm == 'fixed')}
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
    # Each factory call validates the same owned marker, selects the already
    # seeded schema and commits SET search_path before returning IDLE. The range
    # helper performs only read-only queries and owns each connection's close.
    range_audit = range_check.audit(lambda:connect(host, schema), 60)
    range_audit.update(loader_sha256=loader_sha,
                       checker_sha256=sources['tools/ordered_binding_range_check.py'])
    reference_hashes = {case['sha256'] for case in positive_audit['cases']
                       if case['case'] in ('duplicate_and_missing', 'existing_int_normalization')}
    if reference_hashes != {range_audit['reference_sha256']}:
        raise AssertionError('Positive and recovery loader results differ')
    if (source_hashes() != sources or driver_binding() != driver
            or sha_file(receipt_path) != host_sha or sha_file(__file__) != tool_sha):
        raise AssertionError('Correctness inputs changed during verification')
    manifest = {'version':VERSION, 'kind':'binding_correctness', 'sources':sources,
                'source_arm':source_arm, 'loader_sha256':loader_sha,
                'driver_binding':driver, 'run_token':host['run_token'],
                'fixture':fixture, 'expected':full_result,
                'connection_receipt_sha256':host_sha, 'tool_sha256':tool_sha}
    manifest_path = output/'fixture_manifest.json'
    publish(manifest_path, manifest)
    result = {'version':VERSION, 'kind':'binding_correctness', 'result':'PASS',
        'actual_loader':True, 'proxy_rewrites':0, 'measured_passes':0,
        'source_arm':source_arm, 'loader_sha256':loader_sha, 'cursor_policy':'observed',
        'source_hashes':sources, 'driver_binding':driver, 'tool_sha256':tool_sha,
        'run_token':host['run_token'], 'connection_receipt_sha256':host_sha,
        'fixture_manifest_sha256':sha_file(manifest_path),
        'full_scope_result':full_result, 'source_semantics_audit':positive_audit,
        'range_audit':range_audit,
        'cursor_counts':counts, 'checkpoint_verified':True, 'checkpoint':checkpoint,
        'connection_closed':True, 'connection_count':3,
        'limitations':['Correctness only; no memory measurement or production efficacy claim.',
                       '32 synthetic scope rows, five positive input cases and two read-only overflow/recovery cases.',
                       'Three sequential owned-host connections; no production source or external service.']}
    publish(output/'correctness_receipt.json', result)
    print(json.dumps({'result':'PASS','receipt':str(output/'correctness_receipt.json')}, sort_keys=True))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--connection-receipt', required=True)
    parser.add_argument('--output', required=True)
    verify(parser.parse_args())


if __name__ == '__main__':
    main()
