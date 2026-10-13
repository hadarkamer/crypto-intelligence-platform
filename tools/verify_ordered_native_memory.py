#!/usr/bin/env python3
"""Validate closed native ABBA evidence and summarize client measurements offline.

No database connection, native runner, source acquisition or measurement is
started here. Integrity PASS is independent of whether the fixed arm uses less
memory. All four raw sealed journals remain the authority for the aggregates.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
from pathlib import Path
import re
import statistics
import sys
import zipfile

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
VERSION = 'ordered-native-memory-comparison-v1'
MODULE = 'research_common_window_metrics_store.py'
SOURCE_PINS = {
    'baseline': 'e1e74fc7cac10b5b454614c242621857ab1ae5d23cdbffe6e20dc961051f5341',
    'fixed': 'ac2e05e3157928f9daaa8735c0aee533dc3ce2886251a1242e9344a93ba752fd',
}
ARMS = (('A1', 'baseline'), ('B1', 'fixed'), ('B2', 'fixed'), ('A2', 'baseline'))
FILES = ('fixture_manifest.json', 'samples.jsonl', 'receipt.json', 'bundle_manifest.json')
TABLES = ('research_events', 'research_ordered_formula_matches', 'research_event_btc_movements',
          'research_btc_parent_movements', 'research_ordered_first_touch_outcomes',
          'research_common_window_metrics', 'research_btc_price_bars')
PHASES = ('connect_and_marker', 'scope_rows', 'common_windows', 'input_hash', 'summary',
          'common_metrics', 'rollback', 'connection_close', 'pass_return')
NATIVE_FIELDS = ('arena', 'ordblks', 'smblks', 'hblks', 'hblkhd', 'usmblks',
                 'fsmblks', 'uordblks', 'fordblks', 'keepcost')
CASES = ('empty_no_query', 'duplicate_and_missing', 'existing_int_normalization',
         'signed_int64_boundaries', 'string_int64_boundaries')
HASH_FIELDS = ('decoded_rows_sha256', 'decoded_common_windows_sha256',
               'summary_sha256', 'common_metrics_sha256')
PROTOCOL = {'mode': 'scope', 'rows': 2000, 'passes': 12,
            'scope_parity': 'postonly', 'cursor_policy': 'observed'}
RUNTIME_FIELDS = ('python_version', 'python_implementation', 'libc_name', 'libc_version',
                  'psycopg_version', 'postgres_server_version_num', 'libpq_version',
                  'pq_implementation', 'machine')


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False,
                      default=str, allow_nan=False).encode()


def unique_object(pairs):
    result = dict(pairs)
    require(len(result) == len(pairs), 'Duplicate JSON keys')
    return result


def decode(raw):
    return json.loads(raw, object_pairs_hook=unique_object,
                      parse_constant=lambda _: (_ for _ in ()).throw(ValueError('Nonfinite JSON')))


def read(path, maximum=4 * 1024 * 1024):
    path = Path(path)
    require(not any(item.is_symlink() for item in (path, *path.parents)), 'Evidence symlink forbidden')
    require(path.is_file() and path.stat().st_size <= maximum, 'Missing or oversized evidence: ' + path.name)
    return path.read_bytes()


def integer(value, *, minimum=0):
    return type(value) is int and value >= minimum


def hash_value(value):
    return type(value) is str and re.fullmatch('[0-9a-f]{64}', value) is not None


def closed_bundle(folder):
    memory = folder/'memory'
    sealed_raw, blob = read(memory/'sealed.json'), read(memory/'evidence.zip')
    sealed = decode(sealed_raw)
    require(sealed.get('result') == 'PASS' and sealed.get('readback_verified') is True
            and sealed.get('file') == 'evidence.zip' and sealed.get('bytes') == len(blob)
            and sealed.get('sha256') == sha(blob), 'Unverified memory seal')
    with zipfile.ZipFile(io.BytesIO(blob)) as archive:
        require(archive.namelist() == list(FILES), 'Unexpected sealed archive members/order')
        require(all(item.file_size <= 4 * 1024 * 1024 for item in archive.infolist())
                and sum(item.file_size for item in archive.infolist()) <= 8 * 1024 * 1024,
                'Uncompressed archive exceeds bound')
        raw = {name: archive.read(name) for name in FILES}
    inventory = decode(raw['bundle_manifest.json'])
    require(set(inventory) == set(FILES[:-1]), 'Bundle inventory mismatch')
    for name in FILES:
        if (memory/name).exists() or (memory/name).is_symlink():
            require(read(memory/name) == raw[name], 'Loose/sealed evidence differs: ' + name)
        if name in inventory:
            require(inventory[name] == {'sha256': sha(raw[name]), 'bytes': len(raw[name])},
                    'Bundle member hash mismatch: ' + name)
    receipt, manifest = decode(raw['receipt.json']), decode(raw['fixture_manifest.json'])
    require(sealed.get('receipt_sha256') == sha(raw['receipt.json'])
            and receipt.get('fixture_manifest_sha256') == sha(raw['fixture_manifest.json']),
            'Receipt/fixture seal mismatch')
    return raw, receipt, manifest, {'sealed_sha256': sha(sealed_raw), 'archive_sha256': sha(blob),
        'receipt_sha256': sha(raw['receipt.json']), 'archive_bytes': len(blob)}


def positive_audit(audit):
    require(audit.get('result') == 'PASS' and audit.get('actual_loader') is True
            and audit.get('proxy_used') is False and audit.get('source_writes') == 0
            and audit.get('input_int_conversion_preserved') is True
            and [item.get('case') for item in audit.get('cases', [])] == list(CASES)
            and audit.get('deferred_cases') == ['overflow_above_int64', 'overflow_below_int64'],
            'Positive input audit incomplete')
    require(all(hash_value(item.get('sha256')) and integer(item.get('rows'))
                for item in audit['cases']), 'Positive input fingerprints invalid')


def slope(values):
    center = (len(values)-1)/2
    return sum((index-center)*(value-statistics.mean(values)) for index,value in enumerate(values)) / sum(
        (index-center)**2 for index in range(len(values)))


def validate_arm(folder, label, variant, expected_sources):
    folder = Path(folder)
    evidence = {name: read(folder/name) for name in
        ('preflight.json', 'native_lifecycle.json', 'runner_cleanup.json', 'host/connection.json')}
    pre, life, cleanup, host = [decode(evidence[name]) for name in evidence]
    expected = dict(expected_sources, **{MODULE: SOURCE_PINS[variant]})
    bindings = pre['source_bindings']
    contract_raw = read(ROOT/'tools/ordered_native_runtime_contract_20261012.json')
    contract = decode(contract_raw)
    require(pre.get('result') == 'READY' and pre.get('blockers') == []
            and pre.get('native_lifecycle_tested') is False and pre.get('contract') == contract,
            label + ': preflight failed')
    require(bindings['actual_sources'] == expected and life.get('source_bindings') == bindings
            and life.get('sources_unchanged') is True, label + ': source inventory mismatch')
    for key,name in (('runner', 'tools/run_ordered_native_postgres.py'),
                     ('contract', 'tools/ordered_native_runtime_contract_20261012.json'),
                     ('verifier', 'tools/verify_ordered_binding_correctness.py')):
        require(bindings.get(key) == sha(read(ROOT/name)), label + ': executable source mismatch: ' + key)
    require(pre.get('runner_sha256') == bindings['runner']
            and pre.get('contract_sha256') == bindings['contract'], label + ': preflight source binding mismatch')
    observed = pre['observed']
    for key in contract['required_keys']:
        require(type(observed.get(key)) is type(contract['expected'][key])
                and observed[key] == contract['expected'][key], label + ': runtime mismatch: ' + key)
    for name in ('postgres', 'initdb'):
        require(hash_value(bindings.get(name)) and pre['binaries'][name]['sha256'] == bindings[name]
                and pre['binaries'][name]['server_version_num'] == 180006, label + ': binary binding mismatch')
    require(hash_value(bindings.get('python')), label + ': Python binary hash missing')
    require(life.get('kind') == 'native_scope_memory' and life.get('memory_arm') == variant
            and life.get('result') == 'PASS' and life.get('native_lifecycle_attempted') is True
            and life.get('native_lifecycle_tested') is True and life.get('measured_passes') == 12,
            label + ': native memory lifecycle incomplete')
    require(life.get('preflight_sha256') == sha(evidence['preflight.json'])
            and host.get('runtime_preflight_sha256') == sha(evidence['preflight.json']),
            label + ': preflight byte chain mismatch')
    require(host.get('version') == 'isolated-ordered-pg-host-v1'
            and host.get('host') == '127.0.0.1' and host.get('engine') == 'native PostgreSQL'
            and host.get('dbname') == host.get('user') == 'postgres'
            and host.get('marker_table') == 'public.local_memory_replay_identity'
            and host.get('purpose') == 'isolated-ordered-memory-replay'
            and host.get('run_token') == life.get('run_token')
            and re.fullmatch('[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}', host['run_token'])
            and integer(host.get('server_pid'), minimum=1)
            and integer(host.get('port'), minimum=1024) and host['port'] <= 65535
            and host.get('server_version_num') == observed['postgres_server_version_num'] == 180006,
            label + ': host identity invalid')
    identity = life['postmaster_identity']
    require(identity['pid'] == host['server_pid'] and identity['port'] == host['port']
            and integer(identity['start_ticks'], minimum=1) and integer(identity['start_epoch'], minimum=1),
            label + ': postmaster identity mismatch')
    require(cleanup.get('exit_code') == 0 and cleanup.get('cleanup') == []
            and len(life['cleanup']) == 4, label + ': runner cleanup incomplete')
    require(len({item['pid'] for item in life['cleanup']}) == 4
            and all(item.get('group_gone') is True and item.get('forced') is False
                    and item.get('leader_exit_code') == 0 for item in life['cleanup']),
            label + ': owned child cleanup failed')
    server_clean = [item for item in life['cleanup'] if item['pid'] == host['server_pid']]
    require(len(server_clean) == 1 and server_clean[0]['signals'] == [2]
            and all(item['signals'] == [] for item in life['cleanup'] if item['pid'] != host['server_pid']),
            label + ': shutdown was not normal')
    descendants = life['descendant_cleanup']
    require(descendants.get('forced') is False and descendants.get('tracked_live_remaining') == 0
            and all(item.get('state') == 'zombie_not_executing' for item in descendants.get('observations', [])),
            label + ': recorded descendants remain live')
    raw, receipt, manifest, seal = closed_bundle(folder)
    require(life.get('memory_receipt_sha256') == seal['receipt_sha256']
            and life.get('memory_sealed_sha256') == seal['sealed_sha256'], label + ': memory lifecycle seal mismatch')
    require(all(receipt.get(key) == manifest.get(key) == value for key,value in PROTOCOL.items()),
            label + ': protocol changed')
    require(receipt.get('result') == 'PASS' and receipt.get('checkpoint_unchanged') is True
            and receipt.get('postmeasurement_content_parity') is True
            and receipt.get('engine') == manifest.get('engine') == 'native PostgreSQL'
            and manifest['connection_receipt_sha256'] == sha(evidence['host/connection.json'])
            and manifest['sources'] == receipt['source_hashes'] == expected
            and manifest['driver_binding'] == receipt['driver_binding'] == observed['driver_binding']
            and manifest['driver_cycle_probe'] == receipt['driver_cycle_probe']
            and manifest['server'] == receipt['server'] == host['server_version'], label + ': replay bindings invalid')
    processes = life['memory_processes']
    require(len(processes) == 2 and [item['stage'] for item in processes] == ['prepare','measure']
            and all(integer(item['pid'], minimum=1) for item in processes)
            and processes[0]['pid'] != processes[1]['pid']
            and receipt['preparation_pid'] == manifest['preparation_pid'] == processes[0]['pid']
            and receipt['measurement_pid'] == processes[1]['pid']
            and {item['pid'] for item in processes} <= {item['pid'] for item in life['cleanup']},
            label + ': prepare/measurement process separation invalid')
    loaded_module = receipt['loaded_metrics_module']
    require(loaded_module == manifest['loaded_metrics_module']
            and set(loaded_module) == {'path','sha256'} and loaded_module['sha256'] == SOURCE_PINS[variant]
            and Path(loaded_module['path']).is_absolute() and Path(loaded_module['path']).name == MODULE
            and '..' not in Path(loaded_module['path']).parts, label + ': actual loaded module binding invalid')
    require(receipt.get('psycopg') == observed['psycopg_version']
            and receipt.get('libpq') == observed['libpq_version']
            and receipt.get('libc') == [observed['libc_name'], observed['libc_version']]
            and receipt.get('python', '').split()[0] == observed['python_version'], label + ': client runtime mismatch')
    full = manifest['expected']
    require(full == receipt['postmeasurement_full_scope_audit']
            and all(full.get(key) is True for key in ('full_parity_verified', 'reference_parity', 'synthetic_only', 'common_coverage_complete'))
            and full.get('truncated') is False and full.get('excluded_waves') == 0
            and all(full.get(key) == 2000 for key in ('loaded_rows', 'common_window_rows', 'independent_waves', 'common_valid_representatives'))
            and all(hash_value(full.get(key)) for key in (*HASH_FIELDS, 'evaluation_input_sha256')),
            label + ': full result parity incomplete')
    measured = {key:value for key,value in full.items() if key not in HASH_FIELDS}
    measured['full_parity_verified'] = False
    require(manifest['expected_measured'] == measured, label + ': measured-result projection changed')
    fixture, checkpoint = manifest['fixture'], manifest['baseline']
    require(fixture['rows'] == 2000 and fixture['source_cardinality'] == {key:2000 for key in TABLES}
            and checkpoint == {'identity': fixture['identity'], 'source_cardinality': fixture['source_cardinality'],
                               'fixed_source_cardinality': True}
            and fixture['identity'] == {'schema': manifest['schema'], 'database': 'postgres'}
            and re.fullmatch('ordered_memory_replay_[0-9a-f]{24}', manifest['schema']), label + ': fixture checkpoint invalid')
    positive_audit(receipt['postmeasurement_source_semantics_audit'])
    sample_info = receipt['samples']
    samples = [decode(line) for line in raw['samples.jsonl'].splitlines()]
    require(sample_info == {'file':'samples.jsonl', 'bytes':len(raw['samples.jsonl']),
        'sha256':sha(raw['samples.jsonl']), 'records':109, 'pass_ids':list(range(1,13))}
        and len(samples) == 109, label + ': journal inventory mismatch')
    expected_order = [(1, 'observer_baseline', 'sample')] + [
        (index, phase, 'pass_end' if phase == 'pass_return' else 'sample')
        for index in range(1,13) for phase in PHASES]
    require([(row['pass_index'],row['phase'],row['event']) for row in samples] == expected_order,
            label + ': journal phases omitted or reordered')
    require(len({row['pid'] for row in samples}) == len({row['thread_id'] for row in samples}) == 1
            and all(integer(row['pid'], minimum=1) and integer(row['thread_id'], minimum=1)
                    and row.get('scope_index') == 0 and row.get('gc_enabled') is True
                    and row['pid'] == receipt['measurement_pid'] and row.get('trace') is None for row in samples)
            and receipt.get('gc_enabled') is True and receipt.get('forced_gc_calls') == 0
            and receipt.get('allocator_trim_calls') == 0, label + ': process/thread/GC protocol changed')
    require(len(receipt['gc_thresholds']) == 3 and all(integer(value) for value in receipt['gc_thresholds']),
            label + ': GC thresholds invalid')
    for row in samples:
        require(all(integer(row.get(key)) for key in ('rss_bytes','ru_maxrss_bytes','python_allocated_blocks'))
                and row['rss_bytes'] > 0 and row['ru_maxrss_bytes'] > 0, label + ': missing memory counters')
        require(len(row['gc_count']) == len(row['gc_stats']) == 3
                and all(type(value) is int for value in row['gc_count'])
                and all(set(item) == {'collections','collected','uncollectable'}
                        and all(integer(value) for value in item.values()) for item in row['gc_stats']), label + ': GC counters invalid')
        native = row['native']
        require(native.get('status') == 'ok' and native.get('glibc_version') == [2,36]
                and set(native.get('stats') or {}) == set(NATIVE_FIELDS)
                and all(integer(value) for value in native['stats'].values()), label + ': native allocation counters unavailable')
    high = [row['ru_maxrss_bytes'] for row in samples]
    require(high == sorted(high), label + ': lifetime high-water counter regressed')
    ends = [row for row in samples if row['event'] == 'pass_end']
    counters = {'queries':4, 'fetchone':2, 'fetchall':2, 'array_targets':1, 'scalar_conversions':0,
                'explicit_closes':0, 'observed_list_bindings':int(variant == 'baseline'),
                'observed_scalar_bindings':int(variant == 'fixed')}
    result_sha = sha(canonical(measured))
    for row in ends:
        require(row['result'] == measured and row['result_sha256'] == result_sha
                and row['mode'] == 'scope' and row['scope_parity'] == 'postonly'
                and row['cursor_policy'] == 'observed' and row['cursor_counts'] == counters
                and row['commits_in_workload'] == 0 and row['rollbacks'] == 1,
                label + ': measured result/workload differs')
        require(all(integer(row.get(key)) for key in ('elapsed_ns','cpu_ns','observer_ns'))
                and row['elapsed_ns'] > 0 and row['observer_ns'] <= row['elapsed_ns'], label + ': timing counters invalid')
    require(receipt['results'] == [{'pass_index':index, 'result_sha256':result_sha,
            'thread_id':samples[0]['thread_id']} for index in range(1,13)], label + ': completed-pass receipt mismatch')
    rss = [row['rss_bytes'] for row in ends]
    warm = rss[3:]
    aggregate = {'label':label, 'source':variant, 'endpoint_rss_bytes':rss,
        'warm_median_rss_bytes':statistics.median(warm), 'warm_min_rss_bytes':min(warm),
        'warm_max_rss_bytes':max(warm), 'warm_slope_bytes_per_pass':slope(warm),
        'sampled_max_rss_bytes':max(row['rss_bytes'] for row in samples),
        'lifetime_maxrss_bytes':high[-1],
        'total_elapsed_ns':sum(row['elapsed_ns'] for row in ends),
        'total_cpu_ns':sum(row['cpu_ns'] for row in ends),
        'total_observer_ns':sum(row['observer_ns'] for row in ends)}
    proof = {'label':label, **seal, 'native_receipt_sha256':{name:sha(value) for name,value in evidence.items()},
        'measurement_pid':samples[0]['pid'], 'measurement_thread_id':samples[0]['thread_id'],
        'preparation_pid':processes[0]['pid'], 'loaded_metrics_module':loaded_module,
        'run_token':host['run_token'], 'schema':manifest['schema'], 'postmaster_identity':identity,
        'source_bindings':bindings, 'runtime':{key:observed[key] for key in RUNTIME_FIELDS},
        'driver_binding':observed['driver_binding'], 'gc_thresholds':receipt['gc_thresholds'],
        'full_result':full, 'positive_audit':receipt['postmeasurement_source_semantics_audit'],
        'fixture_semantics':{key:value for key,value in fixture.items() if key != 'identity'},
        'measured_result_sha256':result_sha, 'normal_cleanup':True,
        'initial_rss_bytes':samples[0]['rss_bytes'], 'initial_lifetime_maxrss_bytes':high[0],
        'gc_collections_delta':[samples[-1]['gc_stats'][i]['collections']-samples[0]['gc_stats'][i]['collections'] for i in range(3)]}
    return aggregate, proof


def validate_comparison(arms, *, expected_sources=None):
    """Return bounded aggregates plus private proof bindings for ordered A/B/B/A roots."""
    require(len(arms) == 4 and len({str(Path(item).resolve()) for item in arms}) == 4, 'Four distinct ordered arms required')
    if expected_sources is None:
        sys.path.insert(0, str(ROOT/'tools'))
        from ordered_postgres_memory_replay import source_hashes
        expected_sources = source_hashes()
    require(type(expected_sources) is dict and expected_sources.get(MODULE) == SOURCE_PINS['fixed']
            and all(hash_value(value) for value in expected_sources.values()), 'Pinned fixed source inventory required')
    aggregates, proofs = [], []
    for folder,(label,variant) in zip(arms, ARMS):
        aggregate, proof = validate_arm(Path(folder), label, variant, expected_sources)
        aggregates.append(aggregate)
        proofs.append(proof)
    for field in ('runtime','driver_binding','gc_thresholds','full_result','positive_audit',
                  'fixture_semantics','measured_result_sha256'):
        require(all(proof[field] == proofs[0][field] for proof in proofs), 'Cross-arm parity/control mismatch: ' + field)
    for key in proofs[0]['source_bindings']:
        if key != 'actual_sources':
            require(all(proof['source_bindings'][key] == proofs[0]['source_bindings'][key] for proof in proofs),
                    'Cross-arm executable binding mismatch: ' + key)
    for field in ('run_token','schema','measurement_pid'):
        require(len({proof[field] for proof in proofs}) == 4, 'Fresh-arm identity repeated: ' + field)
    require(len({proof[key] for proof in proofs for key in ('preparation_pid','measurement_pid')}) == 8,
            'Fresh preparation/measurement process reused')
    require(proofs[0]['loaded_metrics_module'] == proofs[3]['loaded_metrics_module']
            and proofs[1]['loaded_metrics_module'] == proofs[2]['loaded_metrics_module']
            and proofs[0]['loaded_metrics_module']['path'] != proofs[1]['loaded_metrics_module']['path'],
            'Expected separate pinned baseline/fixed physical source roots')
    require(len({(proof['postmaster_identity']['pid'],proof['postmaster_identity']['start_ticks']) for proof in proofs}) == 4,
            'Postmaster identity reused')
    starts = [proof['postmaster_identity']['start_epoch'] for proof in proofs]
    require(starts == sorted(starts), 'Arms not in recorded chronological ABBA order')
    paired = []
    for a,b in ((0,1),(3,2)):
        paired.append({'baseline':aggregates[a]['label'], 'fixed':aggregates[b]['label'],
            'fixed_minus_baseline_warm_median_rss_bytes':aggregates[b]['warm_median_rss_bytes']-aggregates[a]['warm_median_rss_bytes'],
            'fixed_minus_baseline_sampled_max_rss_bytes':aggregates[b]['sampled_max_rss_bytes']-aggregates[a]['sampled_max_rss_bytes'],
            'fixed_minus_baseline_total_elapsed_ns':aggregates[b]['total_elapsed_ns']-aggregates[a]['total_elapsed_ns']})
    summary = {'order':[item[0] for item in ARMS], 'rows':2000,
               'passes_per_arm':12, 'warm_passes':[4,12], 'arms':aggregates}
    require(len(canonical(summary)) <= 16384, 'Aggregate exceeds publication bound')
    return {'version':VERSION, 'result':'PASS', 'summary':summary, 'arms':proofs, 'paired_deltas':paired,
        'expected_fixed_sources':expected_sources, 'semantic_result_sha256':sha(canonical(proofs[0]['full_result'])),
        'validator_sha256':sha(read(Path(__file__))),
        'limitations':['Synthetic one-scope client benchmark; no production OOM-resolution claim.',
            'RSS is sampled at phase/end points; sampled maximum is not a continuous peak.',
            'ru_maxrss is client lifetime high-water through the last measured sample, including startup/inheritance.',
            'Allocator counters are not additive RSS partitions or reclaimable-byte estimates.',
            'Passes within a process are correlated; two fresh repeats are engineering replication, not inferential proof.',
            'Normal GC; full diagnostic parity is outside measured passes, production input hashing remains inside.',
            'Scope checkpoint checks identity/cardinality; full projected content parity is verified separately.',
            'Integrity PASS does not require or establish a reduction in memory.']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('arms', type=Path, nargs=4, metavar='ARM')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = validate_comparison(args.arms)
    with args.output.open('x', encoding='utf-8') as stream:
        json.dump(result, stream, sort_keys=True, indent=2, allow_nan=False)
        stream.write('\n')
    print(json.dumps({'result':'PASS', 'output':str(args.output), 'arms':4, 'measured_passes':48}))


if __name__ == '__main__':
    main()
