"""Isolated differential check against the frozen producer's actual g65 code.

Usage: python -m hl_testnet_runtime.experimental_g65_source_parity \
    --producer-root /path/to/producer

The parent starts a clean-environment child. The child installs the existing
Python I/O guard before loading the candidate and executes only the producer's
named pure decision/helper AST definitions. PostgreSQL and notification storage
are explicit in-memory doubles, never imported from the producer. The output is
comparison evidence, separate from unittest counts. No live access or mutation.
"""
from __future__ import annotations

import argparse
import ast
import contextlib
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import os
from pathlib import Path
import random
import subprocess
import sys
from uuid import uuid4

FUNCTIONS = frozenset(('utc', 'iso', '_decimal', '_count', '_bar', '_close',
                       '_ambiguous', 'advance_position'))
ENVIRONMENT = frozenset(('PATH','LANG','TZ','PYTHONUNBUFFERED','PYTHONDONTWRITEBYTECODE',
                         'PYTHONHASHSEED','PYTHONPATH','LC_CTYPE'))


def compare(producer_root, *, seed=65, cases=4000):
    """Only call in the guarded clean-environment child, not a live bot process."""
    if set(os.environ) - ENVIRONMENT:
        raise ValueError('PARITY_REQUIRES_CLEAN_CHILD_ENVIRONMENT')
    from .render_preflight.guard import install
    blocked = install()
    from . import sol_g65_conditional_stop as candidate
    from experimental_execution_fixtures import sol_g65_message
    path = Path(producer_root) / 'sol_g65_experimental_store.py'
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 1024*1024:
        raise ValueError('PARITY_SOURCE_FILE_REQUIRED')
    raw = path.read_bytes()
    tree = ast.parse(raw)
    nodes = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in FUNCTIONS]
    if len(nodes) != len(FUNCTIONS) or {node.name for node in nodes} != FUNCTIONS:
        raise ValueError('PARITY_SOURCE_DECISION_FUNCTIONS_REQUIRED')
    selected = ast.Module(body=nodes, type_ignores=[])
    function_hash = hashlib.sha256(ast.dump(selected, include_attributes=False).encode()).hexdigest()
    namespace = dict(deepcopy=deepcopy, datetime=datetime, timedelta=timedelta, timezone=timezone,
        Decimal=Decimal, InvalidOperation=InvalidOperation, uuid4=uuid4,
        MINUTE=timedelta(minutes=1), SIGNAL_TTL=timedelta(seconds=90),
        _connect=lambda _: contextlib.nullcontext(None), key_for=lambda _: 'offline-parity',
        _save=lambda *args: None, _maintain=lambda *args: None)
    exec(compile(selected, 'frozen_producer_g65_decision_ast', 'exec'), namespace)
    rng = random.Random(seed)
    iso = lambda ms: datetime.fromtimestamp(ms/1000, timezone.utc).isoformat()
    for case in range(cases):
        message = sol_g65_message()
        value = candidate.initialize_from_contract(message['occurrence_id'], message)
        ref, levels = value['reference_at_ms'], value['levels']
        active = dict(position_id='offline-parity', phase='PENDING', entry_at=None,
            entry_price=float(levels['entry']), stop_price=float(levels['initial_stop']),
            take_price=float(levels['take_profit']), cancel_price=float(levels['pending_cancel']),
            original_take_distance=float(levels['original_take_distance']),
            lock_stop_price=float(levels['locked_stop']), lock_triggered_at=None,
            lock_effective_at=None, bar_cursor=iso(ref-60000))
        stored = dict(active=active, history=[], intents=[], counts={})
        namespace['_locked'] = lambda *args: stored
        rows = []
        for i in range(4):
            low, high = sorted(rng.sample(range(9750, 10200), 2))
            opened, closed = rng.randint(low, high), rng.randint(low, high)
            rows.append(dict(open_at_ms=ref+i*60000, open=str(opened/100),
                             high=str(high/100), low=str(low/100), close=str(closed/100)))
        producer_rows = [dict(open_at=iso(row['open_at_ms']),
                             **{k:v for k,v in row.items() if k != 'open_at_ms'}) for row in rows]
        namespace['advance_position']('offline-parity', producer_rows, iso(ref+240000))
        observed = candidate.advance(value, rows, now_ms=ref+240000)
        source = stored['active'] or stored['history'][-1]
        expected_outcome = ('AMBIGUOUS' if source.get('monitor_status') == 'AMBIGUOUS' else
            'CANCEL' if source.get('outcome') == 'CANCELLED_BEFORE_FILL' else source.get('outcome'))
        actual_outcome = observed['outcome']['kind'] if observed['outcome'] else None
        actual_lock = iso(observed['lock_effective_at_ms']) if observed['lock_effective_at_ms'] is not None else None
        if (actual_outcome != expected_outcome or observed['phase'] != source['phase']
                or actual_lock != source['lock_effective_at']
                or actual_outcome in ('SL','TP') and float(observed['outcome']['price']) != source['exit_price']):
            raise ValueError('G65_DIFFERENTIAL_MISMATCH_CASE_' + str(case))
    if blocked:
        raise ValueError('PARITY_NETWORK_ATTEMPT_BLOCKED')
    return dict(schema='g65_producer_differential_v1', status='PASSED', seed=seed,
        comparisons=cases, bars_per_comparison=4,
        compared=['outcome','phase','lock_effective_time','exit_price'],
        producer_file_sha256=hashlib.sha256(raw).hexdigest(),
        producer_decision_functions_sha256=function_hash,
        candidate_sha256=hashlib.sha256(Path(candidate.__file__).read_bytes()).hexdigest(),
        decision_function_executed_unchanged=True,
        database='IN_MEMORY_DOUBLE', notification_storage='IN_MEMORY_DOUBLE',
        network_guard='PYTHON_IO_BOUNDARY', network_attempts_blocked=len(blocked),
        live_exchange_requests_sent=0, source_files_modified=False)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--producer-root', required=True, type=Path)
    parser.add_argument('--seed', type=int, default=65)
    parser.add_argument('--cases', type=int, default=4000)
    parser.add_argument('--child', action='store_true', help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    if not 1 <= args.cases <= 100000:
        parser.error('--cases must be between 1 and 100000')
    if args.child:
        try:
            result = compare(args.producer_root, seed=args.seed, cases=args.cases)
        except Exception as exc:
            result = dict(schema='g65_producer_differential_v1',status='FAILED',
                failure_code=str(exc) if isinstance(exc,ValueError) else 'PARITY_CHECK_FAILED')
        print(json.dumps(result, sort_keys=True, indent=2))
        return 0 if result['status']=='PASSED' else 1
    root = Path(__file__).resolve().parents[1]
    environment = dict(PATH=os.defpath,LANG='C.UTF-8',TZ='UTC',PYTHONUNBUFFERED='1',
        PYTHONDONTWRITEBYTECODE='1',PYTHONHASHSEED='0',PYTHONPATH=str(root))
    result = subprocess.run([sys.executable,'-m','hl_testnet_runtime.experimental_g65_source_parity',
        '--child','--producer-root',str(args.producer_root.resolve()),'--seed',str(args.seed),
        '--cases',str(args.cases)],cwd=root,env=environment,capture_output=True,text=True,timeout=180)
    if result.stdout:
        print(result.stdout,end='')
    else:
        print(json.dumps(dict(status='FAILED',failure_code='PARITY_CHILD_FAILED')))
    return result.returncode


if __name__=='__main__':
    raise SystemExit(main())
