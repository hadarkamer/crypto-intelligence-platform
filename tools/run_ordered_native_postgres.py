#!/usr/bin/env python3
"""Preflight a supplied native runtime, then own one isolated native cluster.

Default: correctness only. Explicit memory arms run fixed synthetic scope work.
No installation, external DSN, existing PGDATA or production connection.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import sys
import time
import zipfile
from uuid import uuid4

VERSION = 'ordered-native-postgres-runner-v1'
FIELDS = {'python_version':str, 'python_implementation':str, 'libc_name':str,
          'libc_version':str, 'psycopg_version':str, 'postgres_server_version_num':int,
          'libpq_version':int, 'pq_implementation':str, 'machine':str}
ESSENTIAL = set(FIELDS)-{'libpq_version','pq_implementation','machine'}
OWNED_GROUPS = set()
OWNED_PROCESSES = {}


class RunnerInterrupted(BaseException):
    """Cancellation must pass through broad optional preflight error handling."""


RUNTIME_PROBE = r'''
import ctypes,hashlib,json,platform,sys
from pathlib import Path
import psycopg,psycopg_binary
from psycopg.adapt import Transformer
libc=ctypes.CDLL('libc.so.6')
version=libc.gnu_get_libc_version
version.argtypes=[]
version.restype=ctypes.c_char_p
paths=[Path(psycopg.__file__).parent/name for name in
 ('connection.py','cursor.py','_cursor_base.py','adapt.py','_py_transformer.py','types/array.py','rows.py')]
paths.extend(Path(psycopg_binary.__file__).parent.glob('*.so'))
hashes={str(p.relative_to(Path(psycopg.__file__).parent.parent)):hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
driver=dict(psycopg=psycopg.__version__,pq_implementation=psycopg.pq.__impl__,libpq=psycopg.pq.version(),
 transformer_type=Transformer.__module__+'.'+Transformer.__name__,source_sha256=hashes)
print(json.dumps(dict(python_version=platform.python_version(),driver_binding=driver,
 python_implementation=sys.implementation.name,libc_name='glibc',
 libc_version=version().decode('ascii'),psycopg_version=psycopg.__version__,
 libpq_version=psycopg.pq.version(),pq_implementation=psycopg.pq.__impl__,
 machine=platform.machine())))
'''
BOOTSTRAP = r'''
import json,os,sys,psycopg
directory,sock,port,pid,expected,token=sys.argv[1:]
with psycopg.connect(host=sock,port=int(port),dbname='postgres',user='postgres',
 connect_timeout=5,autocommit=True,options='-c statement_timeout=10000 -c timezone=UTC') as conn:
 row=conn.execute("SELECT current_setting('data_directory'),current_setting('server_version_num')::int,version(),current_database()").fetchone()
 if os.path.realpath(row[0])!=directory or row[1]!=int(expected) or row[3]!='postgres':
  raise RuntimeError('Owned server SQL identity mismatch')
 conn.execute('CREATE TABLE public.local_memory_replay_identity (run_token text PRIMARY KEY,purpose text NOT NULL)')
 conn.execute('INSERT INTO public.local_memory_replay_identity VALUES(%s,%s)',(token,'isolated-ordered-memory-replay'))
 marker=conn.execute('SELECT run_token,purpose FROM public.local_memory_replay_identity').fetchall()
 if marker!=[(token,'isolated-ordered-memory-replay')]:raise RuntimeError('Owned marker mismatch')
 print(json.dumps(dict(version='isolated-ordered-pg-host-v1',host='127.0.0.1',port=int(port),
  dbname='postgres',user='postgres',run_token=token,marker_table='public.local_memory_replay_identity',
  purpose='isolated-ordered-memory-replay',server_pid=int(pid),server_version=row[2],
  server_version_num=row[1],engine='native PostgreSQL',data_dir=directory)))
'''


def sha_file(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda:stream.read(1024*1024), b''): digest.update(block)
    return digest.hexdigest()


def publish(path, value):
    pending = path.with_suffix(path.suffix+'.pending')
    with pending.open('x', encoding='utf-8') as stream:
        json.dump(value, stream, sort_keys=True, indent=2)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())
    os.link(pending, path)
    pending.unlink()
    descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try: os.fsync(descriptor)
    finally: os.close(descriptor)


def validate_contract(value):
    if type(value) is not dict or value.get('version') != 'ordered-native-runtime-contract-v1':
        raise ValueError('Invalid runtime contract version')
    expected, required = value.get('expected'), value.get('required_keys')
    if type(expected) is not dict or type(required) is not list or not all(type(k) is str for k in required):
        raise ValueError('Invalid runtime contract shape')
    if len(set(required)) != len(required) or not ESSENTIAL <= set(required) <= set(FIELDS):
        raise ValueError('Runtime contract omits required dimensions')
    if any(key not in FIELDS or type(item) is not FIELDS[key] for key,item in expected.items()):
        raise ValueError('Invalid expected runtime field')
    unknown = value.get('unknown_fields')
    if type(unknown) is not list or not all(type(item) is str for item in unknown):
        raise ValueError('Unknown runtime dimensions must be explicit')
    return value


def compare_runtime(contract, observed):
    checks, blockers = {}, []
    for key in contract['required_keys']:
        expected, actual = contract['expected'].get(key), observed.get(key)
        passed = expected is not None and type(actual) is FIELDS[key] and actual == expected
        checks[key] = {'expected':expected,'observed':actual,'matched':passed}
        if not passed: blockers.append('runtime_mismatch_or_unknown:'+key)
    return checks, blockers


def clean_environment():
    return {key:value for key,value in os.environ.items()
            if not key.startswith(('PG','PYTHON'))}


def spawn_owned(command, **kwargs):
    pending = []
    def defer(signum, _frame):
        if not pending: pending.append(signum)
    previous = {sig:signal.signal(sig,defer) for sig in (signal.SIGINT,signal.SIGTERM)}
    try:
        proc = subprocess.Popen(command, start_new_session=True, env=clean_environment(), **kwargs)
        OWNED_GROUPS.add(proc.pid)
        OWNED_PROCESSES[proc.pid] = proc
    finally:
        for sig,handler in previous.items(): signal.signal(sig,handler)
    if pending: raise RunnerInterrupted('Runner interrupted during owned child launch')
    return proc


def group_exists(pgid):
    try: os.killpg(pgid, 0)
    except ProcessLookupError: return False
    return True


def stop_owned(proc, timeout=20, first_signal=signal.SIGTERM):
    if proc.pid not in OWNED_GROUPS:
        raise ValueError('Refusing to signal an unowned process group')
    actions = []
    stages = [(first_signal,timeout)]
    if first_signal == signal.SIGINT: stages.append((signal.SIGQUIT,3))
    stages.append((signal.SIGKILL,3))
    for signum, wait_seconds in stages:
        proc.poll()  # Reap an exited leader before checking descendants.
        if not group_exists(proc.pid): break
        try: os.killpg(proc.pid, signum)
        except ProcessLookupError: break
        actions.append(int(signum))
        deadline = time.monotonic()+wait_seconds
        while time.monotonic() < deadline:
            proc.poll()
            if not group_exists(proc.pid): break
            time.sleep(0.05)
    proc.poll()
    gone = not group_exists(proc.pid)
    if gone:
        OWNED_GROUPS.discard(proc.pid)
        OWNED_PROCESSES.pop(proc.pid, None)
    return {'pid':proc.pid,'signals':actions,'group_gone':gone,'leader_exit_code':proc.returncode,
            'forced':any(item in (int(signal.SIGKILL),int(signal.SIGQUIT)) for item in actions)}


def capture(command, timeout=10):
    proc = spawn_owned(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        stdout, stderr = proc.communicate(timeout=timeout)
        if proc.returncode or len(stdout)>65536:
            raise RuntimeError('Bounded subprocess failed or exceeded output limit')
        return stdout.decode('utf-8')
    finally:
        cleanup = stop_owned(proc, timeout=2)
        if not cleanup['group_gone'] or cleanup['forced']:
            raise RuntimeError('Bounded subprocess cleanup failed')


def preflight(args):
    blockers, observed, binaries = [], {}, {}
    result = {'version':VERSION,'kind':'preflight','native_lifecycle_tested':False,
              'blockers':blockers,'observed':observed,'binaries':binaries}
    try:
        contract_path = Path(args.contract)
        if contract_path.stat().st_size > 65536: raise ValueError('Contract too large')
        contract = validate_contract(json.loads(contract_path.read_text()))
        result.update(contract=contract, contract_sha256=sha_file(contract_path))
    except (OSError, ValueError, TypeError):
        blockers.append('invalid_or_missing_contract')
        result['result'] = 'BLOCKED'
        return result
    if any(key.startswith('PG') for key in os.environ):
        blockers.append('ambient_pg_environment_forbidden')
        result['result'] = 'BLOCKED'
        return result  # No child, including a version probe, may run.
    if sys.platform != 'linux': blockers.append('linux_required')
    if os.geteuid() == 0: blockers.append('nonroot_user_required')
    try:
        if int(Path('/proc/self/stat').read_text().split(' ',1)[0]) != os.getpid():
            blockers.append('unsupported_proc_pid_namespace')
    except (OSError, ValueError): blockers.append('unsupported_proc_pid_namespace')
    if len(os.fsencode(str(Path(args.output)/'host'/'socket'))) > 85:
        blockers.append('unix_socket_directory_too_long')
    python = Path(args.python)
    if not python.is_absolute() or not python.is_file() or not os.access(python,os.X_OK):
        blockers.append('absolute_python_executable_required')
    else:
        try: observed.update(json.loads(capture([str(python),'-I','-c',RUNTIME_PROBE])))
        except Exception: blockers.append('runtime_probe_failed')
    pg_bin = Path(args.pg_bin)
    for name in ('postgres','initdb'):
        path = pg_bin/name
        if not pg_bin.is_absolute() or not path.is_file() or not os.access(path,os.X_OK):
            blockers.append('native_binary_missing:'+name)
            continue
        try:
            version = capture([str(path),'--version']).strip()
            match = re.fullmatch(r'(?:postgres|initdb) \(PostgreSQL\) (\d+)\.(\d+)(?:\s.*)?',version)
            if not match or int(match[1]) < 10: raise ValueError('Unsupported native version')
            number = int(match[1])*10000+int(match[2])
            binaries[name] = {'path':str(path.resolve()),'version':version,'server_version_num':number,
                              'sha256':sha_file(path)}
            if name == 'postgres': observed['postgres_server_version_num'] = number
        except Exception: blockers.append('native_version_probe_failed:'+name)
    if len(binaries)==2 and binaries['postgres']['server_version_num'] != binaries['initdb']['server_version_num']:
        blockers.append('native_binary_version_mismatch')
    result['checks'], mismatches = compare_runtime(contract, observed)
    blockers.extend(mismatches)
    if not blockers:
        try: result['source_bindings'] = source_bindings(args)
        except Exception: blockers.append('source_binding_failed')
    result.update(result='BLOCKED' if blockers else 'READY',
                  qualification='Declared contract only; unknown live dimensions remain unestablished.')
    return result


def process_identity(pid):
    root = Path('/proc')/str(pid)
    fields = (root/'stat').read_text().rsplit(')',1)[1].split()
    return {'pid':pid,'state':fields[0],'start_ticks':int(fields[19]),
            'executable':os.path.realpath(root/'exe'),
            'cmdline':(root/'cmdline').read_bytes()[:16384].split(b'\0')}


def verify_postmaster(path, pid, data, port, *, start_epoch=None, start_ticks=None, executable=None):
    lines = Path(path).read_text().splitlines()
    if (len(lines)<4 or int(lines[0]) != pid or Path(lines[1]).resolve()!=Path(data).resolve()
            or int(lines[3]) != port):
        raise RuntimeError('Postmaster ownership mismatch')
    if start_epoch is not None and not start_epoch-1 <= int(lines[2]) <= start_epoch+10:
        raise RuntimeError('Postmaster start epoch mismatch')
    if start_ticks is not None or executable is not None:
        identity = process_identity(pid)
        if (identity['start_ticks'] != start_ticks or identity['state']=='Z'
                or identity['executable'] != str(Path(executable).resolve())
                or os.fsencode(str(Path(data).resolve())) not in identity['cmdline']):
            raise RuntimeError('Live postmaster process identity mismatch')
    return True


def source_bindings(args):
    sys.path.insert(0, str(Path(__file__).parent))
    if getattr(args, 'concurrent_arm', None):
        from ordered_postgres_concurrent_replay import source_hashes
    else:
        from ordered_postgres_memory_replay import source_hashes
    return {'runner':sha_file(__file__),'contract':sha_file(args.contract),
            'python':sha_file(args.python),'postgres':sha_file(Path(args.pg_bin)/'postgres'),
            'initdb':sha_file(Path(args.pg_bin)/'initdb'),
            'verifier':sha_file(Path(__file__).with_name('verify_ordered_binding_correctness.py')),
            'actual_sources':source_hashes()}


def verify_correctness(output, token, host_receipt, bindings, driver):
    folder = output/'correctness'
    path = folder/'correctness_receipt.json'
    value = json.loads(path.read_text())
    manifest = json.loads((folder/'fixture_manifest.json').read_text())
    if (value.get('kind')!='binding_correctness' or value.get('result')!='PASS'
            or value.get('run_token')!=token or value.get('measured_passes')!=0
            or value.get('actual_loader') is not True or value.get('proxy_rewrites')!=0
            or value.get('connection_closed') is not True or value.get('checkpoint_verified') is not True
            or value.get('source_hashes')!=bindings['actual_sources']
            or value.get('driver_binding')!=driver or value.get('tool_sha256')!=bindings['verifier']
            or value.get('connection_receipt_sha256')!=sha_file(host_receipt)
            or value.get('fixture_manifest_sha256')!=sha_file(folder/'fixture_manifest.json')
            or manifest.get('sources')!=bindings['actual_sources'] or manifest.get('driver_binding')!=driver
            or manifest.get('run_token')!=token or manifest.get('tool_sha256')!=bindings['verifier']
            or manifest.get('connection_receipt_sha256')!=sha_file(host_receipt)
            or manifest.get('expected')!=value.get('full_scope_result')):
        raise RuntimeError('Correctness receipt source/driver/host binding mismatch')
    result = value['full_scope_result']
    if (result.get('loaded_rows')!=32 or result.get('full_parity_verified') is not True
            or result.get('reference_parity') is not True
            or value.get('source_semantics_audit',{}).get('result')!='PASS'):
        raise RuntimeError('Correctness evidence incomplete')
    return sha_file(path)


def verify_memory(output, arm, host_receipt, bindings, driver, processes):
    """Require complete sealed native evidence before accepting this one arm."""
    folder = output/'memory'
    sealed = json.loads((folder/'sealed.json').read_text())
    archive_path = folder/'evidence.zip'
    if (sealed.get('result') != 'PASS' or sealed.get('readback_verified') is not True
            or sealed.get('file') != 'evidence.zip' or archive_path.stat().st_size > 2097152
            or sealed.get('bytes') != archive_path.stat().st_size
            or sealed.get('sha256') != sha_file(archive_path)):
        raise RuntimeError('Memory archive is incomplete or changed')
    names = ['fixture_manifest.json', 'samples.jsonl', 'receipt.json', 'bundle_manifest.json']
    with zipfile.ZipFile(archive_path) as archive:
        if (archive.namelist() != names or sum(item.file_size for item in archive.infolist()) > 4194304
                or archive.testzip() is not None):
            raise RuntimeError('Memory archive inventory is invalid')
        raw = {name:archive.read(name) for name in names}
    if any(raw[name] != (folder/name).read_bytes() for name in names):
        raise RuntimeError('Memory archive differs from completed files')
    inventory = json.loads(raw['bundle_manifest.json'])
    if set(inventory) != set(names[:-1]): raise RuntimeError('Memory manifest inventory differs')
    for name in names[:-1]:
        if inventory[name] != {'sha256':hashlib.sha256(raw[name]).hexdigest(), 'bytes':len(raw[name])}:
            raise RuntimeError('Memory artifact checksum mismatch')
    manifest, receipt = json.loads(raw['fixture_manifest.json']), json.loads(raw['receipt.json'])
    samples = [json.loads(line) for line in raw['samples.jsonl'].splitlines()]
    endpoints = [row for row in samples if row.get('event') == 'pass_end']
    expected_ids = list(range(1,13))
    expected_counts = {'queries':4, 'fetchone':2, 'fetchall':2, 'array_targets':1,
        'scalar_conversions':0, 'explicit_closes':0,
        'observed_list_bindings':int(arm == 'baseline'), 'observed_scalar_bindings':int(arm == 'fixed')}
    expected_module = Path(__file__).resolve().parents[1]/'research_common_window_metrics_store.py'
    module = {'path':str(expected_module), 'sha256':bindings['actual_sources']['research_common_window_metrics_store.py']}
    if (receipt.get('result') != 'PASS' or receipt.get('mode') != 'scope'
            or receipt.get('rows') != 2000 or receipt.get('passes') != 12
            or receipt.get('scope_parity') != 'postonly' or receipt.get('cursor_policy') != 'observed'
            or receipt.get('engine') != 'native PostgreSQL' or manifest.get('engine') != 'native PostgreSQL'
            or manifest.get('rows') != 2000 or manifest.get('passes') != 12
            or manifest.get('mode') != 'scope' or manifest.get('scope_parity') != 'postonly'
            or manifest.get('cursor_policy') != 'observed'
            or receipt.get('source_hashes') != bindings['actual_sources']
            or manifest.get('sources') != bindings['actual_sources']
            or receipt.get('driver_binding') != driver or manifest.get('driver_binding') != driver
            or receipt.get('loaded_metrics_module') != module or manifest.get('loaded_metrics_module') != module
            or manifest.get('connection_receipt_sha256') != sha_file(host_receipt)
            or receipt.get('fixture_manifest_sha256') != hashlib.sha256(raw['fixture_manifest.json']).hexdigest()
            or sealed.get('receipt_sha256') != hashlib.sha256(raw['receipt.json']).hexdigest()
            or receipt.get('checkpoint_unchanged') is not True
            or receipt.get('postmeasurement_content_parity') is not True
            or receipt.get('postmeasurement_full_scope_audit') != manifest.get('expected')
            or receipt.get('gc_enabled') is not True or receipt.get('forced_gc_calls') != 0
            or receipt.get('allocator_trim_calls') != 0
            or receipt.get('preparation_pid') != processes[0]['pid']
            or manifest.get('preparation_pid') != processes[0]['pid']
            or receipt.get('measurement_pid') != processes[1]['pid']
            or processes[0]['pid'] == processes[1]['pid']):
        raise RuntimeError('Native memory receipt/source/process binding mismatch')
    sample_meta = receipt.get('samples', {})
    if (sample_meta != {'file':'samples.jsonl', 'bytes':len(raw['samples.jsonl']),
            'sha256':hashlib.sha256(raw['samples.jsonl']).hexdigest(), 'records':len(samples),
            'pass_ids':expected_ids} or [row.get('pass_index') for row in endpoints] != expected_ids
            or not samples or len({row.get('thread_id') for row in samples}) != 1
            or any(row.get('pid') != processes[1]['pid'] or row.get('gc_enabled') is not True for row in samples)):
        raise RuntimeError('Native memory sample journal is incomplete')
    expected_result = manifest['expected_measured']
    result_sha = hashlib.sha256(json.dumps(expected_result, sort_keys=True, separators=(',', ':'),
                                         ensure_ascii=False, default=str, allow_nan=False).encode()).hexdigest()
    if any(row.get('result') != expected_result or row.get('result_sha256') != result_sha
           or row.get('cursor_counts') != expected_counts or row.get('cursor_policy') != 'observed'
           or row.get('commits_in_workload') != 0 or row.get('rollbacks') != 1 for row in endpoints):
        raise RuntimeError('Native memory pass contract changed')
    expected_cases = {'empty_no_query','duplicate_and_missing','existing_int_normalization',
                      'signed_int64_boundaries','string_int64_boundaries'}
    audit = receipt.get('postmeasurement_source_semantics_audit', {})
    if (audit.get('result') != 'PASS' or audit.get('actual_loader') is not True
            or audit.get('proxy_used') is not False or len(audit.get('cases', [])) != 5
            or {case['case'] for case in audit['cases']} != expected_cases
            or manifest['expected'].get('loaded_rows') != 2000
            or manifest['expected'].get('reference_parity') is not True
            or manifest['expected'].get('full_parity_verified') is not True):
        raise RuntimeError('Native memory full correctness audit is incomplete')
    return {'memory_receipt_sha256':sha_file(folder/'receipt.json'),
            'memory_sealed_sha256':sha_file(folder/'sealed.json'), 'measured_passes':12}


def remember_descendants(pid, tracked):
    """Only descendants of the owned postmaster; bounded by this cluster's cap."""
    pending, visited = [pid], set()
    while pending:
        parent = pending.pop()
        if parent in visited: continue
        visited.add(parent)
        try: children = (Path('/proc')/str(parent)/'task'/str(parent)/'children').read_text().split()
        except FileNotFoundError: continue
        if len(children)>64 or len(tracked)+len(children)>256:
            raise RuntimeError('Owned descendant tracking cap exceeded')
        for child in map(int,children):
            try: tracked[child] = process_identity(child)['start_ticks']
            except FileNotFoundError: continue
            pending.append(child)


def clean_tracked_descendants(tracked):
    # Native backends may have separate process groups. Signal only exact
    # descendants captured from this postmaster, with PID-reuse protection.
    observed, forced, pending = [], False, {}
    for pid,ticks in tracked.items():
        try: current = process_identity(pid)
        except FileNotFoundError: continue
        if current['start_ticks']!=ticks: continue
        if current['state']=='Z':
            observed.append({'pid':pid,'state':'zombie_not_executing'})
            continue
        forced = True
        try: os.kill(pid,signal.SIGKILL)
        except ProcessLookupError: continue
        pending[pid] = ticks
    deadline = time.monotonic()+3
    while pending and time.monotonic()<deadline:
        for pid,ticks in list(pending.items()):
            try: current = process_identity(pid)
            except FileNotFoundError:
                pending.pop(pid)
                continue
            if current['start_ticks']!=ticks: pending.pop(pid)
            elif current['state']=='Z':
                observed.append({'pid':pid,'state':'zombie_not_executing'})
                pending.pop(pid)
        if pending: time.sleep(0.05)
    observed.extend({'pid':pid,'state':'still_executing'} for pid in pending)
    return {'tracked_count':len(tracked),'observations':observed,'forced':forced,
            'tracked_live_remaining':sum(item['state']=='still_executing' for item in observed),
            'scope':'Previously observed owned descendants; not a global process scan.'}


def new_output(path):
    path = Path(os.path.abspath(path))
    if any(item.is_symlink() for item in (path,*path.parents)):
        raise ValueError('Output path must not contain symlinks')
    path.mkdir(mode=0o700, parents=False, exist_ok=False)
    os.chmod(path,0o700)
    return path


def run_native(args, output, readiness):
    host = output/'host'
    host.mkdir(mode=0o700)
    data, sock = host/'data', host/'socket'
    sock.mkdir(mode=0o700)
    token = str(uuid4())
    server = client = initial = None
    tracked = {}
    concurrent = getattr(args, 'concurrent_arm', None)
    arm = concurrent or getattr(args, 'memory_arm', None)
    prefix = 'concurrent' if concurrent else 'memory'
    lifecycle = {'version':VERSION,'kind':('native_concurrent_memory' if concurrent else 'native_scope_memory') if arm else 'native_correctness','result':'FAIL',
                 'run_token':token,'native_lifecycle_attempted':False,
                 'native_lifecycle_tested':False,'cleanup':[],
                 'preflight_sha256':sha_file(output/'preflight.json'),'measured_passes':0}
    if arm:
        lifecycle.update({prefix+'_arm':arm, prefix+'_processes':[]})
    try:
        bindings = readiness['source_bindings']
        if source_bindings(args) != bindings: raise RuntimeError('Preflight sources changed')
        lifecycle['source_bindings'] = bindings
        with (host/'initdb.log').open('xb') as log:
            initial = spawn_owned([str(Path(args.pg_bin)/'initdb'),'-D',str(data),'-U','postgres',
                '--auth-local=trust','--auth-host=trust','--encoding=UTF8','--locale=C','--no-instructions'],
                stdout=log,stderr=subprocess.STDOUT)
            if initial.wait(timeout=60): raise RuntimeError('initdb failed')
            item = stop_owned(initial)
            lifecycle['cleanup'].append(item)
            if not item['group_gone'] or item['signals']: raise RuntimeError('initdb descendants survived')
        with socket.socket() as reservation:
            reservation.bind(('127.0.0.1',0))
            port = reservation.getsockname()[1]
        with (host/'postgres.log').open('xb') as log:
            started_epoch = int(time.time())
            server = spawn_owned([str(Path(args.pg_bin)/'postgres'),'-D',str(data),'-h','127.0.0.1',
                '-p',str(port),'-k',str(sock),'-c','unix_socket_permissions=0700',
                '-c','max_connections=8','-c','shared_buffers=32MB'], stdout=log,stderr=subprocess.STDOUT)
            lifecycle['native_lifecycle_attempted'] = True
            started_ticks = process_identity(server.pid)['start_ticks']
            deadline = time.monotonic()+20
            while not (data/'postmaster.pid').exists() or not (sock/f'.s.PGSQL.{port}').exists():
                if server.poll() is not None: raise RuntimeError('Postmaster exited during startup')
                if time.monotonic()>=deadline: raise TimeoutError('Postmaster readiness timeout')
                remember_descendants(server.pid,tracked)
                time.sleep(0.05)
            identity = dict(start_epoch=started_epoch,start_ticks=started_ticks,
                            executable=Path(args.pg_bin)/'postgres')
            verify_postmaster(data/'postmaster.pid',server.pid,data,port,**identity)
            lifecycle['postmaster_identity'] = dict(pid=server.pid,start_epoch=started_epoch,
                                                   start_ticks=started_ticks,port=port)
            if source_bindings(args) != bindings: raise RuntimeError('Sources changed before marker write')
            remember_descendants(server.pid,tracked)
            expected = readiness['contract']['expected']['postgres_server_version_num']
            raw = capture([args.python,'-I','-c',BOOTSTRAP,str(data.resolve()),str(sock),str(port),
                           str(server.pid),str(expected),token],timeout=20)
            connection = json.loads(raw)
            if (connection.get('run_token')!=token or connection.get('server_pid')!=server.pid
                    or connection.get('port')!=port or connection.get('server_version_num')!=expected):
                raise RuntimeError('Bootstrap receipt mismatch')
            connection['runtime_preflight_sha256'] = lifecycle['preflight_sha256']
            publish(host/'connection.json',connection)
            if server.poll() is not None: raise RuntimeError('Postmaster exited before verifier')
            verify_postmaster(data/'postmaster.pid',server.pid,data,port,**identity)
            if source_bindings(args) != bindings: raise RuntimeError('Sources changed before fixture write')
            launcher = ('import runpy,sys; from pathlib import Path; '
                        'sys.dont_write_bytecode=True; p=sys.argv.pop(1); '
                        'sys.path.insert(0,str(Path(p).parent)); sys.argv[0]=p; '
                        'runpy.run_path(p,run_name="__main__")')
            if arm:
                (output/prefix).mkdir(mode=0o700)
                stages = [('prepare',240), ('measure',900)]
                tool = Path(__file__).with_name('ordered_postgres_concurrent_replay.py' if concurrent else 'ordered_postgres_memory_replay.py')
            else:
                stages = [('correctness',180)]
                tool = Path(__file__).with_name('verify_ordered_binding_correctness.py')
            for stage,seconds in stages:
                verify_postmaster(data/'postmaster.pid',server.pid,data,port,**identity)
                if source_bindings(args) != bindings: raise RuntimeError('Sources changed before client stage')
                command = [args.python,'-I','-c',launcher,str(tool),
                    '--connection-receipt',str(host/'connection.json'),
                    '--output',str(output/(prefix if arm else 'correctness'))]
                if concurrent:
                    command.extend(['--stage',stage])
                elif arm:
                    command.extend(['--mode','scope','--rows','2000','--passes','12',
                        '--scope-parity','postonly','--cursor-policy','observed','--stage',stage])
                logname = prefix+'_'+stage if arm else 'correctness'
                with (output/(logname+'.stdout.log')).open('xb') as out, (output/(logname+'.stderr.log')).open('xb') as err:
                    client = spawn_owned(command,stdout=out,stderr=err)
                    if arm: lifecycle[prefix+'_processes'].append({'stage':stage,'pid':client.pid})
                    deadline = time.monotonic()+seconds
                    while client.poll() is None:
                        if server.poll() is not None: raise RuntimeError('Postmaster exited during client stage')
                        remember_descendants(server.pid,tracked)
                        if time.monotonic()>=deadline: raise TimeoutError('Native client stage timeout')
                        time.sleep(0.05)
                    if client.returncode: raise RuntimeError('Native client stage failed: '+stage)
                item = stop_owned(client)
                lifecycle['cleanup'].append(item)
                if not item['group_gone'] or item['signals']:
                    raise RuntimeError('Client stage descendants survived')
            if arm:
                if concurrent:
                    from verify_ordered_native_concurrent import verify_client
                    proof = verify_client(output/prefix,arm,host/'connection.json',bindings,
                        readiness['observed']['driver_binding'],lifecycle[prefix+'_processes'])
                else:
                    proof = verify_memory(output,arm,host/'connection.json',bindings,
                        readiness['observed']['driver_binding'],lifecycle['memory_processes'])
                lifecycle.update(proof)
            else:
                receipt_sha = verify_correctness(output,token,host/'connection.json',bindings,
                                                 readiness['observed']['driver_binding'])
                lifecycle['correctness_receipt_sha256'] = receipt_sha
            if server.poll() is not None: raise RuntimeError('Postmaster exited before controlled shutdown')
            lifecycle['result'] = 'PASS'
    except (Exception,RunnerInterrupted) as error:
        lifecycle.update(result='FAIL',error_type=type(error).__name__,error=str(error)[:300])
    finally:
        # Repeated signals cannot interrupt bounded cleanup or final receipt.
        for sig in (signal.SIGINT,signal.SIGTERM): signal.signal(sig,signal.SIG_IGN)
        if server is not None and server.poll() is None:
            try: remember_descendants(server.pid,tracked)
            except Exception as error:
                lifecycle.update(result='FAIL',descendant_tracking_error=type(error).__name__)
        for proc,first in ((client,signal.SIGTERM),(server,signal.SIGINT),(initial,signal.SIGTERM)):
            if proc is None or proc.pid not in OWNED_GROUPS: continue
            try:
                item = stop_owned(proc,first_signal=first)
                lifecycle['cleanup'].append(item)
                if (not item['group_gone'] or item['forced']
                        or (proc is client and item['signals'])
                        or (proc is server and item['leader_exit_code']!=0)):
                    lifecycle['result']='FAIL'
            except Exception as error:
                lifecycle['result']='FAIL'
                lifecycle['cleanup'].append({'pid':proc.pid,'error_type':type(error).__name__})
        try:
            descendants = clean_tracked_descendants(tracked)
            lifecycle['descendant_cleanup'] = descendants
            if descendants['forced'] or descendants['tracked_live_remaining']: lifecycle['result']='FAIL'
        except Exception as error:
            lifecycle.update(result='FAIL',descendant_cleanup_error=type(error).__name__)
        try:
            lifecycle['sources_unchanged'] = source_bindings(args)==readiness['source_bindings']
            if not lifecycle['sources_unchanged']: lifecycle['result']='FAIL'
        except Exception:
            lifecycle.update(result='FAIL',sources_unchanged=False)
        lifecycle['native_lifecycle_tested'] = lifecycle['result']=='PASS'
        publish(output/'native_lifecycle.json',lifecycle)
    return 0 if lifecycle['result']=='PASS' else 1


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--python',required=True)
    parser.add_argument('--pg-bin',required=True)
    parser.add_argument('--contract',required=True)
    parser.add_argument('--output',required=True)
    parser.add_argument('--preflight-only',action='store_true')
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument('--memory-arm',choices=['baseline','fixed'],
                        help='Explicit isolated 2000-row/12-pass actual-source memory arm')
    modes.add_argument('--concurrent-arm',choices=['baseline','fixed'],
                        help='Explicit isolated ordered plus BTC report overlap, 12 rounds')
    args = parser.parse_args(argv)
    output = new_output(args.output)
    args.output = str(output)
    def interrupted(signum, _frame): raise RunnerInterrupted('Runner interrupted')
    previous = {sig:signal.signal(sig,interrupted) for sig in (signal.SIGINT,signal.SIGTERM)}
    code, cleanup = 1, []
    try:
        readiness = preflight(args)
        readiness['runner_sha256'] = sha_file(__file__)
        publish(output/'preflight.json',readiness)
        if readiness['result']!='READY': code = 2
        elif args.preflight_only: code = 0
        else: code = run_native(args,output,readiness)
    except (Exception,RunnerInterrupted) as error:
        if not (output/'preflight.json').exists():
            publish(output/'preflight.json',{'version':VERSION,'kind':'preflight','result':'BLOCKED',
                'native_lifecycle_tested':False,'blockers':['preflight_interrupted_or_failed'],
                'error_type':type(error).__name__})
        code = 2
    finally:
        for sig in previous: signal.signal(sig,signal.SIG_IGN)
        # Includes failed/aborted version and bootstrap captures, not only the
        # three long-lived lifecycle variables. No external groups are scanned.
        for proc in list(OWNED_PROCESSES.values()):
            try:
                item = stop_owned(proc,timeout=2)
                cleanup.append(item)
                if not item['group_gone'] or item['forced']: code = 1
            except Exception as error:
                code = 1
                cleanup.append({'pid':proc.pid,'error_type':type(error).__name__})
        publish(output/'runner_cleanup.json',{'version':VERSION,'cleanup':cleanup,'exit_code':code,
                'scope':'Owned process groups and recorded postmaster descendants only.'})
        for sig,handler in previous.items(): signal.signal(sig,handler)
    return code


if __name__=='__main__':
    raise SystemExit(main())
