"""Test-only Python socket, descendant-process and native psycopg boundaries.

This is deliberately not described as an OS network sandbox. No credentials are
present; existing test doubles and exact local CI database validation also apply.
"""
import ipaddress
import os
from pathlib import Path
import subprocess
import sys
from urllib.parse import urlsplit

_attempts = None


def install():
    global _attempts
    if _attempts is not None:
        return _attempts
    attempts = []
    _attempts = attempts
    root = str(Path(__file__).resolve().parents[2])
    ci_url = os.environ.get("HL_JOURNAL_CI_URL")
    database = None
    if ci_url:
        u = urlsplit(ci_url)
        if (u.scheme != "postgresql" or u.hostname != "127.0.0.1" or u.path != "/hl_journal_ci"
                or u.username != "preflight" or not u.password or u.query or u.fragment
                or u.port is None or not 1024 <= u.port <= 65535):
            raise RuntimeError("NONLOCAL_DATABASE_REFUSED")
        database = dict(host=u.hostname, port=u.port, dbname="hl_journal_ci", user=u.username, password=u.password)

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
        if args or database is None or any(kwargs.get(key) != value for key, value in database.items()):
            attempts.append("database_blocked")
            raise RuntimeError("PREFLIGHT_NONLOCAL_DATABASE_DISABLED")
        return original_connect(**kwargs)
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
            safe_names = {"PATH", "LANG", "TZ", "PYTHONUNBUFFERED", "PYTHONDONTWRITEBYTECODE",
                          "PYTHONHASHSEED", "LC_CTYPE"}
            child_env = {key: value for key, value in supplied.items() if key in safe_names}
            child_env["PYTHONPATH"] = root
            if selected_url:
                child_env["HL_JOURNAL_CI_URL"] = selected_url
            kwargs["env"] = child_env
            bootstrap = ("import sys; sys.path.insert(0," + repr(root) + "); "
                "from hl_testnet_runtime.render_preflight.guard import install; install(); "
                "exec(compile(" + repr(args[2]) + ",'<offline-child>','exec'),"
                "{'__name__':'__main__','__file__':'<offline-child>'})")
            super().__init__([args[0], "-c", bootstrap, *args[3:]], *positional, **kwargs)
    subprocess.Popen = GuardedPopen
    return attempts
