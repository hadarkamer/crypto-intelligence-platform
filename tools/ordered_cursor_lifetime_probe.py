"""Disposable scope-only cursor experiments; never imported by the service."""
from __future__ import annotations

import ast
import gc
import hashlib
import inspect
import json
from pathlib import Path
import sys
import textwrap
import weakref

POLICIES = ('baseline', 'close', 'scalar-array', 'source', 'observed')
VERSION = 'ordered-cursor-lifetime-probe-v1'


def query_contract():
    """Freeze the exact four source SQL literals before measured work begins."""
    import research_formula_ordered_store as store
    import research_common_window_metrics_store as common
    queries = []
    for function in (store.load_scope_rows, common.available, common.load_by_event_ids):
        tree = ast.parse(textwrap.dedent(inspect.getsource(function)))
        found = [node for node in ast.walk(tree) if isinstance(node, ast.Call)
                 and isinstance(node.func, ast.Attribute) and node.func.attr == 'execute']
        for node in sorted(found, key=lambda value:value.lineno):
            if not node.args or not isinstance(node.args[0], ast.Constant) or type(node.args[0].value) is not str:
                raise AssertionError('Unexpected scope SQL construction')
            queries.append(node.args[0].value)
    if len(queries) != 4 or 'event_id=ANY(%s::bigint[])' not in queries[3]:
        raise AssertionError('Scope SQL contract changed')
    return tuple(queries)


def scalar_array(ids):
    if type(ids) is not list or not 1 <= len(ids) <= 10000:
        raise ValueError('Expected a bounded nonempty integer ID list')
    if any(type(value) is not int or not -(2**63) <= value < 2**63 for value in ids):
        raise ValueError('Expected exact signed int64 IDs, excluding bool')
    return '{' + ','.join(str(value) for value in ids) + '}'


def validate_source_array(value):
    """Inspect a fixture binding without replacing or re-encoding it."""
    if (type(value) is not str or not 3 <= len(value) <= 210001
            or value[0] != '{' or value[-1] != '}'):
        raise ValueError('Expected a bounded canonical array string from application source')
    tokens = value[1:-1].split(',')
    if not 1 <= len(tokens) <= 10000:
        raise ValueError('Source array exceeds fixture bound')
    previous = None
    for token in tokens:
        number = int(token)
        if (str(number) != token or not -(2**63) <= number < 2**63
                or (previous is not None and number <= previous)):
            raise ValueError('Source array must contain sorted unique canonical signed-int64 IDs')
        previous = number


class CursorPolicyConnection:
    """Four exact queries, counters only; no cursor, SQL-parameter or row history."""
    def __init__(self, connection, policy, contract, *, validate_binding=False):
        if policy not in POLICIES:
            raise ValueError('Unknown cursor policy')
        self.connection, self.policy, self.contract = connection, policy, contract
        self.validate_binding = validate_binding
        self.counts = {'queries':0, 'fetchone':0, 'fetchall':0, 'array_targets':0,
                       'scalar_conversions':0, 'explicit_closes':0}
        if policy == 'source':
            self.counts['source_scalar_bindings'] = 0
        if policy == 'observed':
            self.counts.update(observed_list_bindings=0, observed_scalar_bindings=0)
        self.pending = False

    def execute(self, query, params=None, **kwargs):
        index = self.counts['queries']
        if self.pending or index >= 4 or type(query) is not str or query != self.contract[index]:
            raise AssertionError('Unexpected scope query or unfinished cursor')
        if index == 3:
            self.counts['array_targets'] += 1
            if type(params) is not tuple or len(params) != 3:
                raise AssertionError('Unexpected common-window parameter shape')
            if self.policy == 'observed':
                # Identical, constant-cost observer in both actual-source arms.
                # Full representation checks belong only to prep/post audits.
                if type(params[0]) is list:
                    self.counts['observed_list_bindings'] += 1
                    if self.validate_binding:
                        validate_source_array(scalar_array(params[0]))
                elif type(params[0]) is str:
                    self.counts['observed_scalar_bindings'] += 1
                    if self.validate_binding:
                        validate_source_array(params[0])
                else:
                    raise AssertionError('Expected an actual list or scalar source binding')
            elif self.policy == 'source':
                validate_source_array(params[0])
                self.counts['source_scalar_bindings'] += 1
            elif type(params[0]) is not list:
                raise AssertionError('Expected baseline list binding')
            elif self.policy == 'scalar-array':
                params = (scalar_array(params[0]), params[1], params[2])
                self.counts['scalar_conversions'] += 1
        self.counts['queries'] += 1
        cursor = self.connection.execute(query, params, **kwargs)
        self.pending = True
        return CursorPolicyCursor(cursor, self, 'fetchone' if index in (0,2) else 'fetchall')

    def finish(self):
        expected = {'queries':4, 'fetchone':2, 'fetchall':2, 'array_targets':1,
                    'scalar_conversions':int(self.policy == 'scalar-array'),
                    'explicit_closes':4 if self.policy == 'close' else 0}
        if self.policy == 'source':
            expected['source_scalar_bindings'] = 1
        if self.policy == 'observed':
            observed = {key:self.counts[key] for key in
                        ('observed_list_bindings', 'observed_scalar_bindings')}
            if sum(observed.values()) != 1:
                raise AssertionError('Expected exactly one observed source binding')
            expected.update(observed)
        if self.pending or self.counts != expected:
            raise AssertionError('Scope cursor workload differs from fixed contract')
        return dict(self.counts)


class CursorPolicyCursor:
    def __init__(self, cursor, owner, expected):
        self.cursor, self.owner, self.expected = cursor, owner, expected
        self.fetched = False

    def _fetch(self, method):
        if self.fetched or method != self.expected:
            raise AssertionError('Unexpected cursor fetch operation')
        self.fetched = True
        self.owner.counts[method] += 1
        try:
            # Return the exact decoded object; no copy or retained snapshot.
            return getattr(self.cursor, method)()
        finally:
            self.owner.pending = False
            if self.owner.policy == 'close':
                original_failure = sys.exc_info()[0] is not None
                try:
                    self.cursor.close()
                except BaseException:
                    if not original_failure:
                        raise
                else:
                    self.owner.counts['explicit_closes'] += 1

    def fetchone(self):
        return self._fetch('fetchone')

    def fetchall(self):
        return self._fetch('fetchall')


def driver_binding():
    import psycopg
    import psycopg_binary
    from psycopg import pq
    from psycopg.adapt import Transformer
    paths = [(Path(psycopg.__file__).parent/name) for name in
             ('connection.py','cursor.py','_cursor_base.py','adapt.py','_py_transformer.py','types/array.py','rows.py')]
    paths.extend(Path(psycopg_binary.__file__).parent.glob('*.so'))
    hashes = {}
    for path in sorted(paths):
        digest = hashlib.sha256()
        with path.open('rb') as stream:
            for block in iter(lambda:stream.read(1024*1024), b''): digest.update(block)
        key = str(path.relative_to(Path(psycopg.__file__).parent.parent))
        hashes[key] = digest.hexdigest()
    return {'psycopg':psycopg.__version__, 'pq_implementation':pq.__impl__,
            'libpq':pq.version(), 'transformer_type':Transformer.__module__+'.'+Transformer.__name__,
            'source_sha256':hashes}


def _weak(value):
    try:
        return weakref.ref(value), 'supported'
    except TypeError:
        return None, 'unsupported'


def _existing_carrier(transformer):
    """Only known Transformer→dict→RowDumper edges; hard caps, no heap scan."""
    from psycopg.types.array import BaseListDumper
    roots = gc.get_referents(transformer)
    if len(roots) > 32:
        return None, 'incomplete_root_cap'
    inspected = 0
    for cache in roots:
        if type(cache) is not dict:
            continue
        if len(cache) > 32:
            return None, 'incomplete_cache_cap'
        for wrapper in cache.values():
            inspected += 1
            if inspected > 64:
                return None, 'incomplete_node_cap'
            if type(wrapper).__module__ != 'psycopg_binary._psycopg' or type(wrapper).__name__ != 'RowDumper':
                continue
            leaves = gc.get_referents(wrapper)
            if len(leaves) > 16:
                return None, 'incomplete_leaf_cap'
            for leaf in leaves:
                if isinstance(leaf, BaseListDumper) and getattr(leaf, '_tx', None) is transformer:
                    return weakref.ref(leaf), 'existing_list_dumper_backedge'
    return None, 'not_observed'


def driver_cycle_probe(connection_factory):
    """Small actual-SQL ownership probe in preparation, outside measured child."""
    def execute_and_release():
        conn = connection_factory()
        try:
            # Scalar output avoids introducing an independent array-loader cycle.
            cursor = conn.execute('SELECT 1::bigint AS probe_value WHERE 1=ANY(%s::bigint[])', ([1,2,3],))
            if cursor.fetchone() != {'probe_value':1}:
                raise AssertionError('Driver ownership probe returned wrong value')
            carrier, discovery = _existing_carrier(cursor._tx)
            cursor_ref, cursor_status = _weak(cursor)
            transformer_ref, transformer_status = _weak(cursor._tx)
            result_ref, result_status = _weak(cursor.pgresult)
            return (carrier, cursor_ref, transformer_ref, result_ref,
                    {'discovery':discovery, 'cursor_weakref':cursor_status,
                     'transformer_weakref':transformer_status,'pgresult_weakref':result_status})
        finally:
            conn.rollback()
            conn.close()
    carrier_ref, cursor_ref, tx_ref, result_ref, output = execute_and_release()
    # The inner frame and traversal lists have gone. Only weakrefs survive.
    carrier = carrier_ref() if carrier_ref is not None else None
    result = carrier._tx.pgresult if carrier is not None else None
    output.update(version=VERSION, cursor_alive=(cursor_ref() is not None if cursor_ref else None),
        list_dumper_alive=(carrier is not None if carrier_ref is not None else None),
        carrier_pgresult_present=(result is not None if carrier_ref is not None else None),
        carrier_pgresult_rows=(result.ntuples if result is not None else None),
        carrier_pgresult_columns=(result.nfields if result is not None else None),
        transformer_alive=(tx_ref() is not None if tx_ref else None),
        pgresult_alive=(result_ref() is not None if result_ref else None),
        normal_gc=gc.isenabled(), no_forced_collection=True,
        limitation='Small scalar-result lifetime only; not a byte attribution or production leak proof. Natural collection may occur before observation.')
    return output


def source_semantics_audit(connection_factory, window_minutes=60):
    """Actual-loader edge cases after measurement; no experiment proxy/rewrites."""
    from psycopg.pq import TransactionStatus
    import research_common_window_metrics_store as common
    cases = []
    def record(name, result):
        encoded = json.dumps(result, sort_keys=True, separators=(',', ':'), default=str,
                             ensure_ascii=False, allow_nan=False).encode()
        cases.append({'case':name,'rows':len(result),'sha256':hashlib.sha256(encoded).hexdigest()})
    with connection_factory() as conn:
        conn.rollback()
        if conn.pgconn.transaction_status != TransactionStatus.IDLE:
            raise AssertionError('Source audit requires an idle real connection')
        empty = common.load_by_event_ids(conn, [], window_minutes)
        if empty != {} or conn.pgconn.transaction_status != TransactionStatus.IDLE:
            raise AssertionError('Empty input changed result or executed a database command')
        record('empty_no_query', empty)
        reference = common.load_by_event_ids(conn, [1], window_minutes)
        if set(reference) != {1}:
            raise AssertionError('Source audit fixture event 1 is absent')
        for name, ids, expected in [
            ('duplicate_and_missing', [1,1,2**63-1], reference),
            ('existing_int_normalization', [True,'1',1.9,False,-1.9,'0001'], reference),
            ('signed_int64_boundaries', [-(2**63),2**63-1], {}),
            ('string_int64_boundaries', [str(-(2**63)),str(2**63-1)], {}),
        ]:
            result = common.load_by_event_ids(conn, ids, window_minutes)
            if result != expected:
                raise AssertionError('Actual loader changed input normalization or membership')
            record(name, result)
    return {'version':VERSION,'result':'PASS','actual_loader':True,'proxy_used':False,
            'cases':cases,'input_int_conversion_preserved':True,'source_writes':0,
            'error_probe_mode':'separate_owned_hosts_required',
            'deferred_cases':['overflow_above_int64','overflow_below_int64']}
