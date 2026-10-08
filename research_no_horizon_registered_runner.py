"""Explicit standalone orchestration for the eight original October registrations.

The reviewed checkpoint adapter is supplied from the retained evidence bundle;
its exact bytes and the original registration are verified before use. Native
PostgreSQL acquisition stores original SQL/raw JSON proofs in the destination.
It never manufactures a Render MCP envelope or re-registers an experiment.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import signal
import sys
import tarfile
import threading
from types import SimpleNamespace
import uuid
import zipfile

VERSION = 'original-eight-native-runner-v1'
MODE = 'DIRECT_POSTGRES_NATIVE'
ADAPTER_HASHES = {
    'queue_driver.py': 'e1cc8423aa9f64386907462deaf01d5d2b04ff085a7679c8eb2b96cb46547d7f',
    'connector_bridge.py': '35e02ce20fca2ccf1a63ce02f91215903d1fc46ffa44ad07fbcb677326c4db8e',
    'runtime/server.mjs': '3c89f842423166d343fa046db06d1c4b60f33ece01ffb0e88c3f9e1a5dccbb19',
    'runtime/package.json': '626f2642fd15d866dd4735939614893d672dea3211fdb1074485c74349fa624e',
    'runtime/package-lock.json': '234c6be3aca56fe102cc305197fc24f7706e8be4f23b87ff6647692df4b23e35',
}
MAX_ARCHIVE_BYTES = 512 * 1024 * 1024


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def encoded(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2,
                       allow_nan=False, default=str) + '\n').encode()


def atomic_json(path, value):
    path = Path(path)
    if path.is_symlink():
        raise ValueError('SYMLINK_STATE_REJECTED')
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    try:
        with temporary.open('xb') as handle:
            handle.write(encoded(value))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        temporary.unlink(missing_ok=True)


@contextmanager
def work_lock(work):
    work.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(work / 'runner.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError('REGISTERED_RUNNER_ALREADY_ACTIVE') from None
        yield
    finally:
        os.close(descriptor)


def load_adapter(directory):
    directory = directory.resolve()
    for name, expected in ADAPTER_HASHES.items():
        path = directory / name
        if path.is_symlink() or digest(path.read_bytes()) != expected:
            raise ValueError('FROZEN_ADAPTER_CHANGED')
    if not (directory / 'runtime/node_modules/@electric-sql/pglite').is_dir():
        raise ValueError('PINNED_NODE_DEPENDENCIES_MISSING')
    spec = importlib.util.spec_from_file_location('registered_checkpoint_adapter', directory / 'queue_driver.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def mode_identity(adapter, args, registrations):
    return {'version': VERSION, 'transport': MODE, 'source_id': args.source_id,
            'original_summary_sha256': digest((args.checkpoint / 'actual_registration_summary.json').read_bytes()),
            'original_registrations_sha256': digest(encoded(registrations)),
            'frozen_source_commit': adapter.COMMIT,
            'telegram_authorized': False, 'trading_authorized': False}


def native_mode(store, work, identity, *, adopt=False):
    if (work / 'transport_evidence').exists() or (work / 'transport_evidence').is_symlink():
        raise ValueError('EXTERNAL_TRANSPORT_EVIDENCE_REQUIRES_ITS_ORIGINAL_ADAPTER')
    path = work / 'native_transport_identity.json'
    if path.is_symlink():
        raise ValueError('SYMLINK_NATIVE_MODE_REJECTED')
    if path.exists():
        if json.loads(path.read_bytes()) != identity:
            raise ValueError('NATIVE_TRANSPORT_IDENTITY_CHANGED')
        source_path = work / 'native_source_identity.json'
        if source_path.is_symlink():
            raise ValueError('SYMLINK_SOURCE_IDENTITY_REJECTED')
        if source_path.exists():
            source_identity = json.loads(source_path.read_bytes())
            if (source_identity.get('transport') != MODE or source_identity.get('source_id') != identity['source_id'] or
                    not re.fullmatch(r'[a-f0-9]{64}', source_identity.get('endpoint_sha256', '')) or
                    source_identity.get('sslmode') not in ('disable', 'verify-full')):
                raise ValueError('NATIVE_SOURCE_IDENTITY_INVALID')
        elif any(has_fetched_evidence(store.report(request_id)) for request_id in store_ids(store)):
            raise ValueError('NATIVE_PROOFS_REQUIRE_RETAINED_SOURCE_IDENTITY')
        return True
    if not adopt:
        raise ValueError('NATIVE_TRANSPORT_NOT_INITIALIZED')
    # An externally acquired checkpoint cannot be silently relabelled native.
    for request_id in store_ids(store):
        report = store.report(request_id)
        if report['status'] != 'WAITING' or report['proof_count'] != 0 or report['executor_plan_id'] is not None:
            raise ValueError('ONLY_UNACQUIRED_ORIGINAL_CHECKPOINT_CAN_ADOPT_NATIVE_MODE')
    atomic_json(path, identity)
    return True


def has_fetched_evidence(report):
    terminal = report.get('terminal_receipt') or {}
    return bool(report['proof_count'] or terminal.get('raw_response_retained') or
                terminal.get('rejected_proof_summary') or
                (terminal.get('diagnostic') or {}).get('raw_response_sha256'))


def store_ids(store):
    with store.connection.transaction():
        return [row['request_id'] for row in store.connection.execute(
            'SELECT request_id FROM research_no_horizon_acquisition_requests ORDER BY request_id').fetchall()]


def source_factory(args):
    """No environment credential read or source connection until a due claim."""
    def connect():
        import psycopg
        from psycopg.conninfo import conninfo_to_dict
        from psycopg.rows import dict_row
        raw = os.getenv(args.source_url_env, '')
        if not raw:
            raise RuntimeError('EXPLICIT_READ_SOURCE_NOT_CONFIGURED')
        try:
            config = conninfo_to_dict(raw)
            host = config.get('host', '')
            if (not host or ',' in host or host.startswith('/') or
                    not config.get('user') or not config.get('dbname') or
                    any(key in config for key in ('service', 'hostaddr', 'options'))):
                raise ValueError('explicit single endpoint required')
            local = host in ('localhost', '127.0.0.1', '::1')
            if local and not args.allow_local_source:
                raise ValueError('local source requires explicit test switch')
            sslmode = 'disable' if local else 'verify-full'
            endpoint = {key: config.get(key, '5432' if key == 'port' else '')
                        for key in ('host', 'port', 'dbname', 'user')}
            identity = {'transport': MODE, 'source_id': args.source_id,
                        'endpoint_sha256': digest(encoded(endpoint)), 'sslmode': sslmode}
            target = args.work_dir / 'native_source_identity.json'
            if target.is_symlink():
                raise ValueError('symlink source identity')
            if target.exists() and json.loads(target.read_bytes()) != identity:
                raise ValueError('source endpoint changed')
            connection = psycopg.connect(raw, row_factory=dict_row, connect_timeout=5,
                sslmode=sslmode, **({} if local else {'sslrootcert': 'system'}),
                options='-c default_transaction_read_only=on -c timezone=UTC '
                        '-c statement_timeout=15000 -c lock_timeout=1000 '
                        '-c idle_in_transaction_session_timeout=20000 '
                        '-c application_name=no_horizon_registered_source')
            try:
                if not target.exists():
                    atomic_json(target, identity)
            except Exception:
                connection.close()
                raise
            return connection
        except Exception:
            # Driver/DSN errors can contain credentials. Never print them.
            raise RuntimeError('READ_SOURCE_CONNECTION_FAILED') from None
    return connect


def export_state(args):
    """Called only under the process lock after the registry closed cleanly."""
    if args.archive is None or args.archive.exists() or args.archive.is_symlink():
        raise ValueError('NEW_ARCHIVE_PATH_REQUIRED')
    args.archive.parent.mkdir(parents=True, exist_ok=True)
    names = ['working_registry_snapshot.tar.gz', 'working_registry_identity.json', 'native_transport_identity.json']
    names += [name for name in ('native_source_identity.json',
                               'scheduler_cursor.json') if (args.work_dir / name).exists()]
    names += sorted(str(path.relative_to(args.work_dir)) for path in (args.work_dir / 'receipts').glob('*.json'))
    if len(names) > 9999 or sum((args.work_dir / name).stat().st_size for name in names) > MAX_ARCHIVE_BYTES:
        raise ValueError('CHECKPOINT_EXPORT_BUDGET_EXCEEDED')
    payloads = {}
    for name in names:
        path = args.work_dir / name
        if path.is_symlink():
            raise ValueError('SYMLINK_CHECKPOINT_REJECTED')
        payloads[name] = path.read_bytes()
    if sum(map(len, payloads.values())) > MAX_ARCHIVE_BYTES:
        raise ValueError('CHECKPOINT_EXPORT_BUDGET_EXCEEDED')
    manifest = {'version': VERSION, 'transport': MODE,
                'files': {name: {'sha256': digest(raw), 'bytes': len(raw)} for name, raw in payloads.items()}}
    with zipfile.ZipFile(args.archive, 'x', compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr('manifest.json', encoded(manifest))
        for name, raw in payloads.items():
            archive.writestr(name, raw)
    return {'archive_sha256': digest(args.archive.read_bytes()), 'file_count': len(payloads)}


def restore_state(args):
    """Restore one complete native checkpoint into an otherwise empty work dir."""
    if args.archive is None or args.archive.is_symlink() or args.archive.stat().st_size > MAX_ARCHIVE_BYTES:
        raise ValueError('BOUNDED_CHECKPOINT_ARCHIVE_REQUIRED')
    if set(path.name for path in args.work_dir.iterdir()) != {'runner.lock'}:
        raise ValueError('RESTORE_REQUIRES_NEW_WORK_DIRECTORY')
    allowed = {'working_registry_snapshot.tar.gz', 'working_registry_identity.json',
               'native_transport_identity.json', 'native_source_identity.json', 'scheduler_cursor.json'}
    with zipfile.ZipFile(args.archive) as archive:
        entries = archive.infolist()
        if (len(entries) > 10000 or len({entry.filename for entry in entries}) != len(entries)
                or sum(entry.file_size for entry in entries) > MAX_ARCHIVE_BYTES):
            raise ValueError('CHECKPOINT_ARCHIVE_BUDGET_OR_DUPLICATE')
        for entry in entries:
            name = entry.filename
            if (name not in allowed | {'manifest.json'} and
                    not re.fullmatch(r'receipts/[0-9]{8}T[0-9]{6}-[a-f0-9]{32}\.json', name)):
                raise ValueError('CHECKPOINT_ARCHIVE_UNEXPECTED_PATH')
            if (entry.external_attr >> 16) & 0o170000 == 0o120000:
                raise ValueError('CHECKPOINT_ARCHIVE_SYMLINK')
        manifest = json.loads(archive.read('manifest.json'))
        if (manifest.get('version') != VERSION or manifest.get('transport') != MODE or
                set(manifest.get('files', {})) != {entry.filename for entry in entries} - {'manifest.json'} or
                not {'working_registry_snapshot.tar.gz', 'working_registry_identity.json',
                     'native_transport_identity.json'} <= set(manifest['files'])):
            raise ValueError('CHECKPOINT_MANIFEST_MISMATCH')
        payloads = {name: archive.read(name) for name in manifest['files']}
    for name, raw in payloads.items():
        if manifest['files'][name] != {'sha256': digest(raw), 'bytes': len(raw)}:
            raise ValueError('CHECKPOINT_PAYLOAD_CHANGED')
    for name, raw in payloads.items():
        path = args.work_dir / name
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('xb') as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
    data = args.work_dir / 'registry_data'
    data.mkdir()
    with tarfile.open(args.work_dir / 'working_registry_snapshot.tar.gz') as archive:
        members = archive.getmembers()
        if sum(member.size for member in members) > 2 * MAX_ARCHIVE_BYTES:
            raise ValueError('REGISTRY_SNAPSHOT_BUDGET_EXCEEDED')
        for member in members:
            # PGlite dumpDataDir uses leading '/' even for its portable root.
            # Python's data filter strips it; validate that same relative name.
            relative = member.name.lstrip('/')
            if (member.issym() or member.islnk() or '..' in Path(relative).parts or
                    not (data / relative).resolve().is_relative_to(data)):
                raise ValueError('REGISTRY_SNAPSHOT_UNSAFE_PATH')
        archive.extractall(data, filter='data')


def run_cycle(args, adapter, stop):
    import research_no_horizon_acquisition_store as acquisition
    import research_no_horizon_scheduler as scheduler
    summary, plans, registrations = adapter.checkpoint_context(args.checkpoint)
    identity = mode_identity(adapter, args, registrations)
    for name in ('registry_data', 'receipts', 'working_registry_snapshot.tar.gz', 'working_registry_identity.json'):
        if (args.work_dir / name).is_symlink():
            raise ValueError('SYMLINK_WORK_STATE_REJECTED')
    cursor_path = args.work_dir / 'scheduler_cursor.json'
    if cursor_path.is_symlink():
        raise ValueError('SYMLINK_CURSOR_REJECTED')
    cursor = json.loads(cursor_path.read_bytes())['next_cursor'] if cursor_path.exists() else 0
    if type(cursor) is not int or not 0 <= cursor < 8:
        raise ValueError('INVALID_RESTORED_CURSOR')
    engine_args = SimpleNamespace(**vars(args))
    engine_args.node = str(args.repo / 'tools/no_horizon_guarded_node.py')
    os.environ['NO_HORIZON_RUNNER_NODE'] = str(Path(args.node).resolve())
    os.environ['NO_HORIZON_RUNNER_PID'] = str(os.getpid())
    with adapter.destination(engine_args, summary) as connection:
        store = acquisition.AcquisitionStore(connection)
        adapter.verify_registered(store, registrations)
        if args.command in ('export', 'restore') or (args.work_dir / 'native_transport_identity.json').exists():
            native_mode(store, args.work_dir, identity)
        if args.command in ('status', 'export', 'restore'):
            result = {'requests': [store.report(key) for key in registrations],
                      'source_queries_performed': 0, 'automatic_worker_active': False}
        else:
            native_mode(store, args.work_dir, identity, adopt=True)
            tick = scheduler.run_tick(connection, plans, registrations,
                source_connection_factory=source_factory(args), worker_id='original-eight-native-runner',
                provenance_verifier=lambda backend, items: native_mode(backend, args.work_dir, identity),
                cursor=cursor, request_budget=args.request_budget,
                acquisition_leaf_budget=args.leaf_budget, execution_passes=args.passes,
                candle_budget=args.candle_budget, cancelled=stop.is_set)
            result = {'native_tick': tick, 'next_cursor': tick['next_cursor']}
    # Destination context verifies clean shutdown before publishing progress.
    if 'next_cursor' in result:
        atomic_json(cursor_path, {'next_cursor': result['next_cursor']})
    result.update(version=VERSION, transport=MODE, completed_at_utc=datetime.now(timezone.utc).isoformat(),
                  clean_registry_shutdown=True, telegram_authorized=False, trading_authorized=False,
                  source_configured=bool(os.getenv(args.source_url_env)),
                  host_durability_verified=False, deployment_verified=False,
                  working_snapshot_sha256=digest((args.work_dir / 'working_registry_snapshot.tar.gz').read_bytes()))
    receipt = args.work_dir / 'receipts' / (datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S') + '-' + uuid.uuid4().hex + '.json')
    atomic_json(receipt, result)
    output = {'receipt': str(receipt), 'source_configured': result['source_configured'],
              'clean_registry_shutdown': True, 'telegram_authorized': False, 'trading_authorized': False}
    tick = result.get('native_tick')
    if tick is not None:
        groups = tick['groups']
        terminal = len(groups) == 4 and all(group['status'] in ('SELECTED', 'BLOCKED_ACQUISITION') for group in groups)
        output.update(error_count=len(tick['errors']), selection_complete=tick['selection_complete'],
                      blocked_groups=sum(group['status'] == 'BLOCKED_ACQUISITION' for group in groups),
                      terminal=terminal)
    if args.command == 'export':
        output.update(export_state(args))
    return output


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('status', 'tick', 'run', 'export', 'restore'))
    parser.add_argument('--checkpoint', required=True, type=Path)
    parser.add_argument('--adapter-dir', required=True, type=Path)
    parser.add_argument('--work-dir', required=True, type=Path)
    parser.add_argument('--source-id', required=True)
    parser.add_argument('--source-url-env', default='RESEARCH_NO_HORIZON_READ_DATABASE_URL')
    parser.add_argument('--enable-acquisition', action='store_true')
    parser.add_argument('--allow-local-source', action='store_true')
    parser.add_argument('--node', default=os.getenv('CODEX_PRIMARY_RUNTIME_NODE') or shutil.which('node'))
    parser.add_argument('--port', default=55440, type=int)
    parser.add_argument('--interval-seconds', default=300, type=int)
    parser.add_argument('--request-budget', default=8, type=int)
    parser.add_argument('--leaf-budget', default=1, type=int)
    parser.add_argument('--passes', default=1, type=int)
    parser.add_argument('--candle-budget', default=1024, type=int)
    parser.add_argument('--archive', type=Path)
    args = parser.parse_args(argv)
    args.repo = Path(__file__).resolve().parent
    for key in ('checkpoint', 'adapter_dir', 'work_dir', 'archive'):
        value = getattr(args, key)
        if value is not None:
            if value.is_symlink():
                parser.error('symlink path rejected')
            setattr(args, key, value.resolve())
    if (not args.node or not 1 <= args.port <= 65535 or not 5 <= args.interval_seconds <= 86400 or
            not 1 <= args.request_budget <= 8 or not 1 <= args.leaf_budget <= 4 or
            not 1 <= args.passes <= 8 or not 1 <= args.candle_budget <= 65536 or
            not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', args.source_url_env) or
            not re.fullmatch(r'[A-Za-z0-9_.:-]{1,120}', args.source_id)):
        parser.error('invalid bounded runner configuration')
    if args.command in ('tick', 'run') and not args.enable_acquisition:
        parser.error('tick/run require --enable-acquisition')
    for protected in (args.checkpoint, args.repo, args.adapter_dir):
        if args.work_dir == protected or protected in args.work_dir.parents or args.work_dir in protected.parents:
            parser.error('mutable state must be separate from checkpoint and code')
        if args.archive is not None and (args.archive == protected or protected in args.archive.parents):
            parser.error('archive must be outside protected input')
    if args.archive is not None and (args.archive == args.work_dir or args.work_dir in args.archive.parents):
        parser.error('archive must be outside working state')
    adapter = load_adapter(args.adapter_dir)
    stop = threading.Event()
    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, lambda *_: stop.set())
    with work_lock(args.work_dir):
        if args.command == 'restore':
            restore_state(args)
        while not stop.is_set():
            output = run_cycle(args, adapter, stop)
            print(json.dumps(output, sort_keys=True), flush=True)
            if args.command != 'run' or output.get('terminal') or stop.wait(args.interval_seconds):
                break


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print('REGISTERED_RUNNER_FAILED: ' + type(error).__name__, file=sys.stderr)
        raise SystemExit(1)
