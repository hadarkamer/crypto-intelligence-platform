#!/usr/bin/env bash
# Test-only runner entry. Called inside the existing loopback/PID namespace.
set -Eeuo pipefail
umask 077
ROOT="$(cd -- "$(dirname -- "$0")/../.." && pwd)"
WORK="${CG_TEST_WORK:?owned work directory required}"
STAGE=preflight
PG_STARTED=false
PG_BIN="$(pg_config --bindir)"
PGDATA="$WORK/pgdata"
SOCKET="$WORK/socket"
cleanup() {
  code=$?
  trap - EXIT INT TERM
  set +e
  clean=true
  if [ "$PG_STARTED" = true ] || [ -f "$PGDATA/postmaster.pid" ]; then
    "$PG_BIN/pg_ctl" -D "$PGDATA" -m immediate -w -t 10 stop >/dev/null 2>&1 || clean=false
  fi
  if "$PG_BIN/pg_ctl" -D "$PGDATA" status >/dev/null 2>&1; then clean=false; fi
  if [ -f "$PGDATA/postmaster.pid" ]; then clean=false; fi
  # Only owned dummy cluster, fixtures and generated runtime are removed here.
  rm -rf -- "$PGDATA" "$SOCKET" "$WORK/private" "$ROOT/standalone_collector/runtime"
  for path in "$PGDATA" "$SOCKET" "$WORK/private" "$ROOT/standalone_collector/runtime"; do
    if [ -e "$path" ]; then clean=false; fi
  done
  printf 'SESSION_PATH_CLEANUP {"postgres_stopped":%s,"owned_data_removed":%s,"last_stage":"%s"}\n' "$clean" "$clean" "$STAGE"
  if [ "$clean" != true ]; then code=1; fi
  exit "$code"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

test "$(id -u)" -ne 0
test -d "$WORK"
test ! -L "$WORK"
test -x "$PG_BIN/initdb"
test -x "$PG_BIN/pg_ctl"
test -x "$PG_BIN/createdb"
test ! -e "$PGDATA"
test ! -e "$SOCKET"
python - <<'PY'
import errno,json,os,socket,sys
from pathlib import Path
from importlib.metadata import version
assert sys.version_info[:3] == (3,11,11), 'python_version'
assert os.getuid() != 0
cap=next(x.split(':',1)[1].strip() for x in Path('/proc/self/status').read_text().splitlines() if x.startswith('CapEff:'))
assert int(cap,16)==0, 'effective_capabilities'
assert {name for _,name in socket.if_nameindex()} == {'lo'}, 'network_interfaces'
for name in ('COINGLASS_COOKIE_HEADER','COINGLASS_STORAGE_STATE_JSON','DATABASE_URL','OPENAI_API_KEY','COINGLASS_SESSION_KEY'):
 assert not os.environ.get(name), 'unexpected_credential_input'
expected={'playwright':'1.55.0','requests':'2.32.5','Pillow':'12.3.0','pycryptodome':'3.21.0','psycopg':'3.2.3','aiohttp':'3.11.11'}
for package,wanted in expected.items(): assert version(package)==wanted, 'dependency_version'
probe=socket.socket();probe.settimeout(1)
try:
 try:probe.connect(('192.0.2.1',443))
 except OSError as exc:assert exc.errno==errno.ENETUNREACH, 'isolation_not_proven'
 else:raise AssertionError('external_access_possible')
finally:probe.close()
print('SESSION_PATH_PRELAUNCH '+json.dumps({'python':sys.version.split()[0], 'packages':expected,
 'unprivileged':True,'capabilities_empty':True,'loopback_only':True,'external_enetunreach':True,
 'real_credentials':False,'internal_sandbox_enabled':False,'dummy_only_exception':True}),flush=True)
PY
test -z "$(ip route show)"
test -z "$(ip -6 route show default)"

STAGE=runtime_generation
python - "$ROOT" <<'PY'
import contextlib,io,subprocess,sys
from pathlib import Path
from unittest.mock import patch
root=Path(sys.argv[1]);sys.path.insert(0,str(root/'standalone_collector'))
import prepare
def skip_previous_tests(command,**kwargs):
 assert command[:3]==[sys.executable,'-m','unittest'], 'unexpected_prepare_subprocess'
 return subprocess.CompletedProcess(command,0)
# Installed versions were checked above. Reuse the actual existing build
# adapters; do not reinstall Chromium or rerun the already-passed suites.
with patch.object(prepare,'prepare_browser'), patch.object(prepare.subprocess,'run',side_effect=skip_previous_tests), contextlib.redirect_stdout(io.StringIO()):
 prepare.prepare()
print('SESSION_PATH_BUILD actual_adapters=true repeated_suites=false source_requests=0',flush=True)
PY

STAGE=postgres_start
mkdir -m 700 "$SOCKET"
"$PG_BIN/initdb" -D "$PGDATA" --no-locale --encoding=UTF8 --auth-local=trust --auth-host=reject >"$WORK/pg-init.log" 2>&1
"$PG_BIN/pg_ctl" -D "$PGDATA" -l "$WORK/pg-server.log" -o "-c listen_addresses='' -c unix_socket_directories='$SOCKET' -c unix_socket_permissions=0700 -c log_statement=none -c log_min_error_statement=panic" -w -t 10 start >/dev/null
PG_STARTED=true
"$PG_BIN/createdb" -h "$SOCKET" cg_session_test
export CG_TEST_DATABASE_URL="host=$SOCKET dbname=cg_session_test user=$(id -un)"
export CG_TEST_PGDATA="$PGDATA"
export PYTHONDONTWRITEBYTECODE=1

STAGE=independent_postgres_connections
python -B "$ROOT/experiments/session-path-20261114/check_postgres.py"
STAGE=full_service_browser_path
python -B "$ROOT/experiments/session-path-20261114/check_path.py" --output "$WORK/path-result.json"
STAGE=completed
