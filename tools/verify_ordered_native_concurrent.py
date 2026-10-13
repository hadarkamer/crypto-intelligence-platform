#!/usr/bin/env python3
"""Offline validation of bounded simultaneous research-path measurements.

Integrity acceptance is independent of any memory or timing improvement. The
client verifier does not connect to a database or execute a measurement.
"""
from __future__ import annotations

import argparse
import io
import json
from pathlib import Path
import re
import statistics
import sys
import zipfile

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'tools'))
from verify_ordered_native_memory import (ARMS, CASES, FILES, HASH_FIELDS, MODULE,
    NATIVE_FIELDS, RUNTIME_FIELDS, SOURCE_PINS, TABLES, canonical, decode,
    hash_value, integer, positive_audit, read, require, sha, slope)

VERSION = 'ordered-native-concurrent-comparison-v1'


def closed_bundle(folder):
    folder = Path(folder)
    sealed_raw, blob = read(folder/'sealed.json'), read(folder/'evidence.zip')
    sealed = decode(sealed_raw)
    require(sealed.get('result') == 'PASS' and sealed.get('readback_verified') is True
            and sealed.get('file') == 'evidence.zip' and sealed.get('bytes') == len(blob)
            and sealed.get('sha256') == sha(blob), 'Unverified concurrent seal')
    with zipfile.ZipFile(io.BytesIO(blob)) as archive:
        require(archive.namelist() == list(FILES), 'Unexpected sealed archive members/order')
        require(all(item.file_size <= 4*1024*1024 for item in archive.infolist())
                and sum(item.file_size for item in archive.infolist()) <= 8*1024*1024,
                'Uncompressed archive exceeds bound')
        raw = {name:archive.read(name) for name in FILES}
    inventory = decode(raw['bundle_manifest.json'])
    require(set(inventory) == set(FILES[:-1]), 'Bundle inventory mismatch')
    for name in FILES:
        if (folder/name).exists() or (folder/name).is_symlink():
            require(read(folder/name) == raw[name], 'Loose/sealed evidence differs: '+name)
        if name in inventory:
            require(inventory[name] == {'sha256':sha(raw[name]), 'bytes':len(raw[name])},
                    'Bundle member hash mismatch: '+name)
    receipt, manifest = decode(raw['receipt.json']), decode(raw['fixture_manifest.json'])
    require(sealed.get('receipt_sha256') == sha(raw['receipt.json'])
            and receipt.get('fixture_manifest_sha256') == sha(raw['fixture_manifest.json']),
            'Receipt/fixture seal mismatch')
    return raw, receipt, manifest, {'sealed_sha256':sha(sealed_raw), 'archive_sha256':sha(blob),
        'receipt_sha256':sha(raw['receipt.json']), 'archive_bytes':len(blob)}


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
    require(life.get('kind') == 'native_concurrent_memory' and life.get('concurrent_arm') == variant
            and life.get('result') == 'PASS' and life.get('native_lifecycle_attempted') is True
            and life.get('native_lifecycle_tested') is True and life.get('measured_rounds') == 12,
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
    aggregate, proof = _client(folder/'concurrent', variant, folder/'host/connection.json',
                               bindings, observed['driver_binding'], life['concurrent_processes'])
    require(life.get('concurrent_receipt_sha256') == proof['receipt_sha256']
            and life.get('concurrent_sealed_sha256') == proof['sealed_sha256'],
            label + ': concurrent lifecycle seal mismatch')
    require({proof['preparation_pid'],proof['measurement_pid']} <= {item['pid'] for item in life['cleanup']},
            label + ': measured processes not owned by cleanup')
    require(proof['runtime'] == {key:observed[key] for key in RUNTIME_FIELDS},
            label + ': measured runtime differs from preflight')
    aggregate.update(label=label, source=variant)
    proof.update(label=label, native_receipt_sha256={name:sha(value) for name,value in evidence.items()},
                 source_bindings=bindings, run_token=host['run_token'], postmaster_identity=identity,
                 normal_cleanup=True)
    return aggregate, proof


PROTOCOL = {'rows_per_lane':2000, 'report_parents':8, 'events_per_parent':250,
            'rounds':12, 'lanes':2, 'gate_timeout_seconds':30, 'round_timeout_seconds':60,
            'abort_join_seconds':5, 'samples':25, 'scope_parity':'postonly', 'cursor_policy':'observed',
            'transaction_read_only':True, 'transaction_isolation':'repeatable read'}
REPLAY_VERSION = 'ordered-postgres-concurrent-replay-v1'
KIND = 'ordered_scope_and_btc_report'


def memory_sample(row, pid, tid):
    require(row.get('pid') == pid and row.get('thread_id') == tid
            and row.get('scope_index') == 0 and row.get('gc_enabled') is True
            and row.get('trace') is None, 'Coordinator/process/GC sample mismatch')
    require(all(integer(row.get(key)) for key in ('rss_bytes','ru_maxrss_bytes','python_allocated_blocks'))
            and row['rss_bytes'] > 0 and row['ru_maxrss_bytes'] > 0, 'Missing memory counters')
    require(len(row['gc_count']) == len(row['gc_stats']) == 3
            and all(type(value) is int for value in row['gc_count'])
            and all(set(item) == {'collections','collected','uncollectable'}
                    and all(integer(value) for value in item.values()) for item in row['gc_stats']),
            'GC counters invalid')
    native = row['native']
    require(native.get('status') == 'ok' and native.get('glibc_version') == [2,36]
            and set(native.get('stats') or {}) == set(NATIVE_FIELDS)
            and all(integer(value) for value in native['stats'].values()),
            'Native allocation measurements unavailable')


def _client(folder, variant, host_receipt, bindings, driver, processes):
    require(variant in SOURCE_PINS, 'Unknown concurrent arm')
    host_raw = read(host_receipt)
    host = decode(host_raw)
    raw, receipt, manifest, seal = closed_bundle(Path(folder))
    expected_sources = bindings['actual_sources']
    require(expected_sources.get(MODULE) == SOURCE_PINS[variant]
            and all(hash_value(value) for value in expected_sources.values()), 'Pinned arm source missing')
    require(receipt.get('version') == manifest.get('version') == REPLAY_VERSION
            and receipt.get('kind') == manifest.get('kind') == KIND
            and canonical(receipt.get('protocol')) == canonical(manifest.get('protocol')) == canonical(PROTOCOL),
            'Concurrent protocol changed')
    require(receipt.get('result') == 'PASS' and receipt.get('checkpoint_unchanged') is True
            and receipt.get('postmeasurement_content_parity') is True
            and receipt.get('engine') == manifest.get('engine') == 'native PostgreSQL'
            and manifest.get('connection_receipt_sha256') == receipt.get('connection_receipt_sha256') == sha(host_raw)
            and manifest.get('sources') == receipt.get('source_hashes') == expected_sources
            and manifest.get('driver_binding') == receipt.get('driver_binding') == driver
            and manifest.get('driver_cycle_probe') == receipt.get('driver_cycle_probe')
            and manifest.get('server') == receipt.get('server') == host['server_version'],
            'Concurrent client bindings invalid')
    require(len(processes) == 2 and [item['stage'] for item in processes] == ['prepare','measure']
            and all(integer(item['pid'], minimum=1) for item in processes)
            and processes[0]['pid'] != processes[1]['pid']
            and receipt.get('preparation_pid') == manifest.get('preparation_pid') == processes[0]['pid']
            and receipt.get('measurement_pid') == processes[1]['pid'], 'Fresh measurement process missing')
    pid, tid = receipt['measurement_pid'], receipt['coordinator_thread_id']
    require(integer(tid, minimum=1), 'Coordinator thread missing')
    require(receipt.get('gc_enabled') is True and receipt.get('forced_gc_calls') == 0
            and receipt.get('allocator_trim_calls') == 0 and len(receipt['gc_thresholds']) == 3
            and all(integer(value) for value in receipt['gc_thresholds']), 'Normal GC controls changed')
    preflight = decode(read(Path(host_receipt).parents[1]/'preflight.json'))
    observed = preflight['observed']
    contract = decode(read(ROOT/'tools/ordered_native_runtime_contract_20261012.json'))['expected']
    require(all(type(observed.get(key)) is type(value) and observed[key] == value
                for key,value in contract.items()), 'Client preflight runtime mismatch')
    require(receipt.get('psycopg') == contract['psycopg_version']
            and receipt.get('libpq') == observed['libpq_version'] == driver['libpq']
            and receipt.get('libc') == [contract['libc_name'],contract['libc_version']]
            and receipt.get('python','').split()[0] == contract['python_version']
            and host['server_version_num'] == contract['postgres_server_version_num'],
            'Concurrent runtime mismatch')
    ordered, report = manifest['ordered'], manifest['report']
    post = receipt['postmeasurement']
    full = ordered['expected']
    require(full == post['ordered_full']
            and all(full.get(key) is True for key in ('full_parity_verified','reference_parity','synthetic_only','common_coverage_complete'))
            and full.get('truncated') is False and full.get('excluded_waves') == 0
            and full.get('successes') == 1500 and full.get('failures') == 500
            and all(full.get(key) == 2000 for key in ('loaded_rows','common_window_rows','independent_waves','common_valid_representatives'))
            and all(hash_value(full.get(key)) for key in (*HASH_FIELDS,'evaluation_input_sha256')),
            'Ordered full parity incomplete')
    measured = {key:value for key,value in full.items() if key not in HASH_FIELDS}
    measured['full_parity_verified'] = False
    require(ordered['expected_measured'] == measured, 'Ordered measured projection changed')
    fixture = ordered['fixture']
    require(fixture['rows'] == 2000 and fixture['source_cardinality'] == {key:2000 for key in TABLES}
            and fixture['identity'] == {'schema':ordered['schema'],'database':'postgres'}
            and re.fullmatch('ordered_memory_replay_[0-9a-f]{24}',ordered['schema'])
            and ordered['checkpoint'] == post['ordered_checkpoint'] == {
                'identity':fixture['identity'],'source_cardinality':fixture['source_cardinality'],
                'fixed_source_cardinality':True}, 'Ordered checkpoint changed')
    positive_audit(post['ordered_source_semantics'])
    require(report['checkpoint'] == post['report_checkpoint']
            and report['full_oracle'] == post['report_full'], 'BTC full oracle/checkpoint changed')
    loaded = receipt['loaded_modules']
    require(loaded == manifest['loaded_modules'], 'Loaded modules changed since preparation')
    _report_and_modules(report, loaded, expected_sources, ordered['schema'])
    samples = [decode(line) for line in raw['samples.jsonl'].splitlines()]
    require(receipt['samples'] == {'file':'samples.jsonl','bytes':len(raw['samples.jsonl']),
        'sha256':sha(raw['samples.jsonl']),'records':25,'round_ids':list(range(1,13))}
        and len(samples) == 25, 'Concurrent journal inventory mismatch')
    expected_order = [(0,'observer_baseline','sample')] + [
        (index,phase,'round_end' if phase == 'round_end' else 'sample')
        for index in range(1,13) for phase in ('both_loaded','round_end')]
    require([(row['round_index'],row['phase'],row['event']) for row in samples] == expected_order,
            'Concurrent phase journal incomplete/reordered')
    for row in samples:
        memory_sample(row,pid,tid)
        require(row['pass_index'] == row['round_index'], 'Round/pass journal mismatch')
    highs = [row['ru_maxrss_bytes'] for row in samples]
    require(highs == sorted(highs), 'Lifetime high-water regressed')
    rounds = receipt['results']
    require(len(rounds) == 12 and [item['round_index'] for item in rounds] == list(range(1,13)),
            'Completed rounds missing')
    expected_cursor = {'queries':4,'fetchone':2,'fetchall':2,'array_targets':1,
        'scalar_conversions':0,'explicit_closes':0,'observed_list_bindings':int(variant=='baseline'),
        'observed_scalar_bindings':int(variant=='fixed')}
    require(ordered['cursor_counts'] == expected_cursor, 'Ordered cursor contract changed')
    role_threads = {'ordered':set(),'report':set()}
    for index,record in enumerate(rounds,1):
        loaded_sample, end = samples[2*index-1:2*index+1]
        _round(record,loaded_sample,end,pid,tid,measured,report['expected'],expected_cursor,role_threads)
    require(all(len(values) == 1 for values in role_threads.values())
            and len(set.union(*role_threads.values())) == 2, 'Persistent lane threads changed')
    expected_threads = {role:next(iter(values)) for role,values in role_threads.items()}
    require(receipt.get('lane_thread_ids') == samples[0].get('lane_thread_ids') == expected_threads,
            'Baseline/final lane thread mapping changed')
    require(integer(receipt.get('probe_observer_ns'))
            and receipt['probe_observer_ns'] >= sum(record['observer_ns'] for record in rounds),
            'Observer totals invalid')
    both = [samples[2*index-1]['rss_bytes'] for index in range(1,13)]
    ends = [samples[2*index] for index in range(1,13)]
    end_rss = [row['rss_bytes'] for row in ends]
    warm_both,warm_ends = both[3:],end_rss[3:]
    aggregate = {'both_loaded_rss_bytes':both,'round_end_rss_bytes':end_rss,
        'warm_both_loaded_median_rss_bytes':statistics.median(warm_both),
        'warm_both_loaded_min_rss_bytes':min(warm_both),'warm_both_loaded_max_rss_bytes':max(warm_both),
        'warm_round_end_median_rss_bytes':statistics.median(warm_ends),
        'warm_round_end_min_rss_bytes':min(warm_ends),'warm_round_end_max_rss_bytes':max(warm_ends),
        'warm_round_end_slope_bytes_per_round':slope(warm_ends),
        'sampled_max_rss_bytes':max(row['rss_bytes'] for row in samples),
        'lifetime_maxrss_bytes':highs[-1],
        'total_elapsed_ns':sum(record['elapsed_ns'] for record in rounds),
        'total_cpu_ns':sum(record['cpu_ns'] for record in rounds),
        'total_observer_ns':sum(record['observer_ns'] for record in rounds)}
    proof = {**seal,'measurement_pid':pid,'preparation_pid':processes[0]['pid'],
        'coordinator_thread_id':tid,'lane_thread_ids':{key:next(iter(value)) for key,value in role_threads.items()},
        'loaded_modules':loaded,'schemas':[ordered['schema'],report['fixture']['schema']],
        'runtime':{key:observed[key] for key in RUNTIME_FIELDS},'driver_binding':driver,
        'gc_thresholds':receipt['gc_thresholds'],'ordered_full':full,'report_full':report['full_oracle'],
        'positive_audit':post['ordered_source_semantics'],
        'fixture_semantics':{'ordered':{key:value for key,value in fixture.items() if key!='identity'},
                             'report':_report_semantics(report['fixture'])},
        'initial_rss_bytes':samples[0]['rss_bytes'],'initial_lifetime_maxrss_bytes':highs[0]}
    return aggregate,proof


def verify_client(folder, arm, host_receipt, bindings, driver, processes):
    _, proof = _client(folder,arm,host_receipt,bindings,driver,processes)
    return {'concurrent_receipt_sha256':proof['receipt_sha256'],
            'concurrent_sealed_sha256':proof['sealed_sha256'],'measured_rounds':12}


def _report_semantics(fixture):
    value = {key:item for key,item in fixture.items() if key not in ('schema','identity','checkpoint')}
    value['checkpoint'] = {key:item for key,item in fixture['checkpoint'].items() if key != 'identity'}
    return value


def _report_and_modules(report, loaded, sources, ordered_schema):
    fixture = report['fixture']
    counts = {'research_events':2000,'research_event_btc_movements':2000,
              'research_btc_parent_movements':8,'research_btc_price_bars':400,'research_alert_outcomes':0}
    require(fixture.get('version') == 'ordered-concurrent-report-fixture-v1'
            and re.fullmatch('ordered_report_replay_[0-9a-f]{24}',fixture['schema'])
            and fixture['schema'] != ordered_schema
            and fixture['identity'] == {'schema':fixture['schema'],'database':'postgres'}
            and fixture.get('parents') == 8 and fixture.get('events_per_parent') == 250
            and fixture.get('events') == 2000 and fixture.get('snapshot_canonical_bytes_each') == 2048
            and fixture.get('snapshot_canonical_bytes_total') == 4096000
            and fixture.get('source_cardinality') == counts,
            'BTC fixed source population/width/schema invalid')
    expected = report['expected']
    require(expected == fixture['expected'] and set(expected) == {
            'source_sha256','waves','events','snapshot_compacted'}
            and hash_value(expected['source_sha256']) and expected['waves'] == 8
            and expected['events'] == 2000 and expected['snapshot_compacted'] is True,
            'BTC measured expected result invalid')
    oracle = report['full_oracle']
    descriptor = fixture['expected_source']
    require(oracle == fixture['full_oracle'] and oracle == {'result':'PASS',
            'full_canonical_source_equal':True,'expected_source':descriptor,'loaded_result':expected}
            and set(descriptor) == {'source_sha256','canonical_bytes','waves','events','reference'}
            and descriptor['source_sha256'] == expected['source_sha256']
            and integer(descriptor['canonical_bytes'],minimum=1) and descriptor['canonical_bytes'] <= 16*1024*1024
            and descriptor['waves'] == 8 and descriptor['events'] == 2000
            and descriptor['reference'] == 'deterministic_seed_records_and_pure_compactor',
            'BTC independent full-source oracle invalid')
    checkpoint = report['checkpoint']
    require(checkpoint == fixture['checkpoint'] and checkpoint['identity'] == fixture['identity']
            and checkpoint.get('fixed_source_cardinality') is True and set(checkpoint['tables']) == set(counts)
            and all(value == {'rows':counts[name],'sha256':value.get('sha256')}
                    and hash_value(value.get('sha256')) for name,value in checkpoint['tables'].items()),
            'BTC exact table checkpoint invalid')
    require(set(fixture['migrations']) == {'001_research_archive_v1.sql','021_btc_parent_movements_v1.sql'}
            and all(value == sources['migrations/'+name] for name,value in fixture['migrations'].items()),
            'BTC migration/source binding invalid')
    require(set(loaded) == {'research_common_window_metrics_store','research_btc_wave_report_worker',
                            'research_btc_wave_endpoint_report'}, 'Unexpected loaded application modules')
    roots = set()
    for name,value in loaded.items():
        require(set(value) == {'path','sha256'} and value['sha256'] == sources[name+'.py']
                and Path(value['path']).is_absolute() and Path(value['path']).name == name+'.py'
                and '..' not in Path(value['path']).parts, 'Actual loaded application module mismatch')
        roots.add(str(Path(value['path']).parent))
    require(len(roots) == 1, 'Concurrent lanes imported different staged source roots')


def _round(record, loaded, end, pid, coordinator_tid, ordered_expected, report_expected, cursor_counts, role_threads):
    require(set(record['lanes']) == {'ordered','report'} and set(record['gate']['ready']) == {'ordered','report'},
            'Round lane inventory invalid')
    gate = record['gate']
    require(integer(record.get('started_ns'),minimum=1) and integer(record.get('completed_ns'),minimum=1)
            and 0 < record['completed_ns']-record['started_ns'] <= 60*10**9, 'Round lifecycle clocks invalid')
    require(all(integer(gate.get(key),minimum=1) for key in ('both_loaded_ns','release_ns'))
            and gate['both_loaded_ns'] <= gate['release_ns'], 'Gate sample/release ordering invalid')
    require(loaded['gate']['ready'] == gate['ready']
            and loaded['gate']['both_loaded_ns'] == gate['both_loaded_ns']
            and loaded['gate'].get('release_ns') is None and loaded['gate'].get('released_ns') == {}
            and set(gate['released_ns']) == {'ordered','report'}, 'Hydrated gate sample differs')
    require(end['round_index'] == record['round_index'], 'Round endpoint identity differs')
    for key,value in record.items():
        require(end.get(key) == value, 'Endpoint/round receipt differs: '+key)
    threads,backends = set(),set()
    for role,expected in (('ordered',ordered_expected),('report',report_expected)):
        lane,ready = record['lanes'][role],gate['ready'][role]
        require(lane.get('round_index') == record['round_index'], 'Lane round identity differs')
        require(lane.get('pid') == ready.get('pid') == pid
                and integer(lane.get('thread_id'),minimum=1)
                and lane['thread_id'] == ready.get('thread_id') != coordinator_tid
                and integer(lane.get('backend_pid'),minimum=1)
                and lane['backend_pid'] == ready.get('backend_pid'), 'Lane identity/gate binding invalid')
        require(integer(ready.get('ready_ns'),minimum=1)
                and record['started_ns'] <= ready['ready_ns'] <= gate['both_loaded_ns'],
                'Lane was not ready before shared RSS sample')
        require(lane.get('result') == expected and lane.get('result_sha256') == sha(canonical(expected)),
                'Lane result differs from prepared oracle')
        if role == 'ordered':
            require(lane.get('cursor_counts') == cursor_counts, 'Measured ordered binding counts differ')
        else:
            require(lane.get('cursor_counts') is None, 'Unexpected report cursor accounting')
        require(integer(gate['released_ns'][role],minimum=1)
                and gate['release_ns'] <= gate['released_ns'][role] <= record['completed_ns'],
                'Lane did not observe release before round completion')
        require(lane.get('rollbacks') == 1 and lane.get('commits_in_workload') == 0
                and lane.get('connection_closed') is True
                and lane.get('transaction') == {'read_only':'on','isolation':'repeatable read'},
                'Lane connection cleanup/read-only transaction not proven')
        require(gate['release_ns']-ready['ready_ns'] <= 30*10**9, 'Hydrated gate exceeded deadline')
        threads.add(lane['thread_id']); backends.add(lane['backend_pid'])
        role_threads[role].add(lane['thread_id'])
    require(len(threads) == len(backends) == 2, 'Lanes did not use distinct threads/connections')
    require(all(integer(record.get(key)) for key in ('elapsed_ns','cpu_ns','observer_ns'))
            and 0 < record['elapsed_ns'] <= 60*10**9
            and record['observer_ns'] <= record['elapsed_ns'], 'Round timing/budget invalid')


def validate_comparison(arms, *, expected_sources=None):
    require(len(arms) == 4 and len({str(Path(item).resolve()) for item in arms}) == 4,
            'Four distinct ordered arms required')
    if expected_sources is None:
        from ordered_postgres_concurrent_replay import source_hashes
        expected_sources = source_hashes()
    require(type(expected_sources) is dict and expected_sources.get(MODULE) == SOURCE_PINS['fixed']
            and all(hash_value(value) for value in expected_sources.values()), 'Pinned fixed source inventory required')
    aggregates,proofs = [],[]
    for folder,(label,variant) in zip(arms,ARMS):
        aggregate,proof = validate_arm(folder,label,variant,expected_sources)
        aggregates.append(aggregate); proofs.append(proof)
    for field in ('runtime','driver_binding','gc_thresholds','ordered_full','report_full','positive_audit','fixture_semantics'):
        require(all(proof[field] == proofs[0][field] for proof in proofs), 'Cross-arm parity/control mismatch: '+field)
    for key in proofs[0]['source_bindings']:
        if key != 'actual_sources':
            require(all(proof['source_bindings'][key] == proofs[0]['source_bindings'][key] for proof in proofs),
                    'Cross-arm executable binding mismatch: '+key)
    for field in ('run_token','measurement_pid'):
        require(len({proof[field] for proof in proofs}) == 4, 'Fresh-arm identity reused: '+field)
    require(len({schema for proof in proofs for schema in proof['schemas']}) == 8,
            'Separate fixture schemas reused')
    require(len({proof[key] for proof in proofs for key in ('preparation_pid','measurement_pid')}) == 8,
            'Fresh preparation/measurement process reused')
    require(proofs[0]['loaded_modules'] == proofs[3]['loaded_modules']
            and proofs[1]['loaded_modules'] == proofs[2]['loaded_modules']
            and proofs[0]['loaded_modules'] != proofs[1]['loaded_modules'],
            'Expected separate baseline/fixed physical source roots')
    require(len({(proof['postmaster_identity']['pid'],proof['postmaster_identity']['start_ticks']) for proof in proofs}) == 4,
            'Postmaster identity reused')
    starts = [proof['postmaster_identity']['start_epoch'] for proof in proofs]
    require(starts == sorted(starts), 'ABBA chronological order invalid')
    summary = {'order':[item[0] for item in ARMS],'rows_per_lane':2000,'rounds_per_arm':12,
               'lanes':2,'warm_rounds':[4,12],'arms':aggregates}
    require(len(canonical(summary)) <= 16384, 'Summary exceeds publication bound')
    return {'version':VERSION,'result':'PASS','summary':summary,'arms':proofs,
        'expected_fixed_sources':expected_sources,'validator_sha256':sha(read(Path(__file__))),
        'semantic_result_sha256':sha(canonical({'ordered':proofs[0]['ordered_full'],'report':proofs[0]['report_full']})),
        'limitations':[
            'Synthetic ordered-scope and BTC-source read/calculation paths; not a full-worker or production replay.',
            'Both decoded datasets coexist at a forced gate; this proves live-data overlap, not parallel Python execution.',
            'RSS is sampled by one coordinator; sampled maximum is not a continuous peak.',
            'ru_maxrss is client lifetime high-water through last measured endpoint, before postmeasurement audits.',
            'Allocator counters are not additive RSS partitions or reclaimable-byte estimates.',
            'Normal GC remains enabled; source preparation and full parity checks are outside measurement.',
            'Within-process rounds are correlated; two fresh repeats are engineering replication, not statistical proof.',
            'Integrity PASS does not require reduced memory and does not establish production OOM resolution.']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('arms',type=Path,nargs=4,metavar='ARM')
    parser.add_argument('--output',type=Path,required=True)
    args = parser.parse_args()
    result = validate_comparison(args.arms)
    with args.output.open('x',encoding='utf-8') as stream:
        json.dump(result,stream,sort_keys=True,indent=2,allow_nan=False)
        stream.write('\n')
    print(json.dumps({'result':'PASS','output':str(args.output),'arms':4,'rounds':48,'operations':96}))


if __name__ == '__main__':
    main()
