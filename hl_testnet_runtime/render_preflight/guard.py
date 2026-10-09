"""Test-only Python socket, descendant-process and native psycopg boundaries.

This is deliberately not described as an OS network sandbox. No credentials are
present; existing test doubles and exact local CI database validation also apply.
"""
import ipaddress
import os
from pathlib import Path
import re
import subprocess
import sys
from urllib.parse import urlsplit

_attempts = None
PRODUCER_ADMIN_DB = "test_experimental_ci"
PRODUCER_DATABASE = re.compile(r"test_(?:r2732|row71205|sol_g65|approved_alert)_[0-9a-f]{32}")
_CONNECTION_KEYS = frozenset({"host", "port", "dbname", "user", "password", "sslmode",
                            "connect_timeout", "options", "autocommit", "row_factory"})


def database_target(value, name):
    """Validate a generated local target without echoing credentials in failures."""
    try:
        u = urlsplit(value)
        if (u.scheme != "postgresql" or u.hostname != "127.0.0.1" or u.path != "/" + name
                or u.username != "preflight" or not u.password or u.query or u.fragment
                or u.port is None or not 1024 <= u.port <= 65535):
            raise ValueError()
        return dict(host=u.hostname, port=u.port, dbname=name, user=u.username, password=u.password)
    except (ValueError, TypeError):
        raise RuntimeError("NONLOCAL_DATABASE_REFUSED") from None


def producer_target(value, runtime_database):
    producer = database_target(value, PRODUCER_ADMIN_DB)
    if runtime_database is None or any(producer[key] != runtime_database[key]
                                     for key in ("host", "port", "user", "password")):
        raise RuntimeError("PRODUCER_DATABASE_IDENTITY_MISMATCH")
    return producer


def connection_allowed(args, kwargs, runtime_database, producer_database):
    """Native libpq may connect only to the freshly generated local fixture DBs.

    Reject hostaddr, service, passfile, multi-host DSNs and arbitrary options,
    including connection overrides hidden inside an otherwise matching DSN.
    Parsing does not contact a database. The producer fixtures use a positional
    libpq DSN when creating their UUID-named databases.
    """
    if not set(kwargs) <= _CONNECTION_KEYS or len(args) > 1:
        return False
    options = kwargs.get("options")
    if options is not None and (not isinstance(options, str) or not re.fullmatch(
            r"-c (?:(?:statement_timeout|lock_timeout|idle_in_transaction_session_timeout)=[0-9]+|"
            r"(?:synchronous_commit|default_transaction_read_only)=on)(?: -c (?:(?:statement_timeout|lock_timeout|"
            r"idle_in_transaction_session_timeout)=[0-9]+|(?:synchronous_commit|default_transaction_read_only)=on))*", options)):
        return False
    if "sslmode" in kwargs and kwargs["sslmode"] != "disable":
        return False
    if "connect_timeout" in kwargs and (type(kwargs["connect_timeout"]) is not int
                                       or not 1 <= kwargs["connect_timeout"] <= 10):
        return False
    if args:
        if producer_database is None or not isinstance(args[0], str):
            return False
        from psycopg.conninfo import conninfo_to_dict
        try:
            parsed = conninfo_to_dict(args[0])
        except Exception:
            return False
        if not set(parsed) <= {"host", "port", "dbname", "user", "password"}:
            return False
        # No connection identity override via kwargs, even if numerically equal.
        if set(kwargs) & {"host", "port", "dbname", "user", "password"}:
            return False
        target = producer_database
    else:
        parsed = kwargs
        target = runtime_database
    if target is None:
        return False
    if any(str(parsed.get(key)) != str(target[key]) for key in ("host", "port", "user", "password")):
        return False
    name = parsed.get("dbname")
    if args:
        return name == PRODUCER_ADMIN_DB or (isinstance(name, str) and PRODUCER_DATABASE.fullmatch(name) is not None)
    return name == "hl_journal_ci"


def install():
    global _attempts
    if _attempts is not None:
        return _attempts
    root = str(Path(__file__).resolve().parents[2])
    ci_url = os.environ.get("HL_JOURNAL_CI_URL")
    test_url = os.environ.get("TEST_DATABASE_URL")
    database = database_target(ci_url, "hl_journal_ci") if ci_url else None
    producer = producer_target(test_url, database) if test_url else None
    attempts = []

    def loopback(host):
        if isinstance(host, bytes):
            host = host.decode("ascii", "strict")
        if host == "localhost":
            return True
        try:
            return ipaddress.ip_address(host).is_loopback
        except (ValueError, TypeError):
            return False

    def audit(event, args):
        if event == "socket.getaddrinfo":
            host = args[0]
        elif event in ("socket.connect", "socket.sendto"):
            address = args[-1]
            host = address[0] if isinstance(address, tuple) and address else None
        else:
            return
        if not loopback(host):
            attempts.append("blocked")
            raise RuntimeError("PREFLIGHT_EXTERNAL_NETWORK_DISABLED")
    sys.addaudithook(audit)

    # Native libpq does not emit Python socket audit events. Restrict the exact
    # generated local database before delegating to psycopg's native connection.
    import psycopg
    original_connect = psycopg.connect
    def local_connect(*args, **kwargs):
        if not connection_allowed(args, kwargs, database, producer):
            attempts.append("database_blocked")
            raise RuntimeError("PREFLIGHT_NONLOCAL_DATABASE_DISABLED")
        return original_connect(*args, **kwargs)
    psycopg.connect = local_connect

    # Existing durable fixtures use Python -c children, sometimes replacing env.
    # Prepend the same guard before their script and remove inherited settings.
    original_popen = subprocess.Popen
    class GuardedPopen(original_popen):
        def __init__(self, args, *positional, **kwargs):
            if kwargs.get("executable") is not None or (len(positional) >= 2 and positional[1] is not None):
                raise PermissionError("PREFLIGHT_SUBPROCESS_REFUSED")
            if (not kwargs.get("shell") and isinstance(args, (list, tuple))
                    and list(args) == ["uname", "-p"]):
                # Python platform.platform() uses exactly this local metadata probe.
                kwargs["env"] = {"PATH": os.defpath, "LANG": "C.UTF-8"}
                super().__init__(["/usr/bin/uname", "-p"], *positional, **kwargs)
                return
            if (not kwargs.get("shell") and isinstance(args, (list, tuple))
                    and list(args) == ["/sbin/ldconfig", "-p"]):
                # ctypes.find_library reads the existing cache for GMP/SDK imports.
                # No cache updates, alternate caches, compilation or other flags.
                kwargs["env"] = {"PATH": os.defpath, "LANG": "C"}
                super().__init__(["/sbin/ldconfig", "-p"], *positional, **kwargs)
                return
            if (kwargs.get("shell") or not isinstance(args, (list, tuple)) or len(args) < 3
                    or str(args[0]) != sys.executable or args[1] != "-c" or not isinstance(args[2], str)):
                # Match the OS process-denial contract. ctypes catches OSError
                # when an optional native-library probe cannot execute; denial
                # must not prevent a dependency's normal non-native fallback.
                raise PermissionError("PREFLIGHT_SUBPROCESS_REFUSED")
            supplied = dict(os.environ if kwargs.get("env") is None else kwargs["env"])
            selected_url = supplied.get("HL_JOURNAL_CI_URL")
            if selected_url is not None and selected_url != ci_url:
                raise RuntimeError("PREFLIGHT_CHILD_DATABASE_CHANGED")
            selected_producer_url = supplied.get("TEST_DATABASE_URL")
            if selected_producer_url is not None and (selected_producer_url != test_url or selected_url != ci_url):
                raise RuntimeError("PREFLIGHT_CHILD_DATABASE_CHANGED")
            safe_names = {"PATH", "LANG", "TZ", "PYTHONUNBUFFERED", "PYTHONDONTWRITEBYTECODE",
                          "PYTHONHASHSEED", "LC_CTYPE"}
            child_env = {key: value for key, value in supplied.items() if key in safe_names}
            child_env["PYTHONPATH"] = root
            if selected_url:
                child_env["HL_JOURNAL_CI_URL"] = selected_url
            if selected_producer_url:
                child_env["TEST_DATABASE_URL"] = selected_producer_url
            kwargs["env"] = child_env
            bootstrap = ("import sys; sys.path.insert(0," + repr(root) + "); "
                "from hl_testnet_runtime.render_preflight.guard import install; install(); "
                "exec(compile(" + repr(args[2]) + ",'<offline-child>','exec'),"
                "{'__name__':'__main__','__file__':'<offline-child>'})")
            super().__init__([args[0], "-c", bootstrap, *args[3:]], *positional, **kwargs)
    subprocess.Popen = GuardedPopen
    _attempts = attempts
    return attempts
