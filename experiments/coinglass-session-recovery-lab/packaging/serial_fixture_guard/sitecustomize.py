"""Fail-closed software egress guard for the fixed serial fixtures, not an OS firewall."""
import atexit
import ipaddress
import json
import os
from pathlib import Path
import sys
from urllib.parse import urlsplit

COUNTS = {"canary_blocked": 0, "denied_network": 0, "denied_browser_http": 0,
          "denied_browser_websocket": 0, "source_contexts_offline": 0}

def loopback(host):
    if isinstance(host, bytes):
        host = host.decode("ascii", errors="strict")
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except (ValueError, TypeError):
        return False

def require_local(host, *, canary=False):
    if not loopback(host):
        COUNTS["canary_blocked" if canary else "denied_network"] += 1
        raise PermissionError("Fixture forbids non-loopback network destination")

def audit(event, args):
    if event in ("socket.connect", "socket.bind"):
        address = args[1]
        if isinstance(address, tuple):
            require_local(address[0])
    elif event in ("socket.getaddrinfo", "socket.gethostbyname"):
        require_local(args[0])

def install():
    try:
        require_local("forbidden-fixture.invalid", canary=True)
    except PermissionError:
        pass
    else:
        raise RuntimeError("Fixture egress canary did not reject")
    sys.addaudithook(audit)
    output = Path(os.environ["QUEUE_LAB_GUARD_AUDIT_DIR"])
    if not output.is_dir() or output.is_symlink():
        raise RuntimeError("Missing owned guard evidence directory")
    def receipt():
        (output / f"python-{os.getpid()}.json").write_text(json.dumps(COUNTS, indent=2) + "\n")
    atexit.register(receipt)
    entry = Path(sys.argv[0]).name
    if entry not in {"collection_model1_task.py", "browser.py"}:
        return
    from playwright.sync_api import Browser
    original = Browser.new_context
    def context(browser, *args, **kwargs):
        kwargs["service_workers"] = "block"
        if entry == "collection_model1_task.py":
            kwargs["offline"] = True
            COUNTS["source_contexts_offline"] += 1
        value = original(browser, *args, **kwargs)
        def route(request):
            url = urlsplit(request.request.url)
            if url.scheme in {"http", "https"} and loopback(url.hostname):
                request.continue_()
            else:
                COUNTS["denied_browser_http"] += 1
                request.abort()
        # The original source fixture later adds the last-matching route that
        # fulfills synthetic HTML; any unhandled source request fails offline.
        value.route("**/*", route)
        def websocket(route):
            if loopback(urlsplit(route.url).hostname) and entry == "browser.py":
                route.connect_to_server()
            else:
                COUNTS["denied_browser_websocket"] += 1
                route.close()
        value.route_web_socket("**/*", websocket)
        return value
    Browser.new_context = context
    if entry == "collection_model1_task.py":
        fixture = Path(os.environ["QUEUE_LAB_ORIGINAL_FIXTURE"]).resolve(strict=True)
        expected = Path(os.environ["QUEUE_LAB_COLLECTOR"]).parent / "app/.github/labs/btc-full-flow/sitecustomize.py"
        if fixture != expected.resolve(strict=True):
            raise RuntimeError("Unexpected original fixture path")
        # exec preserves argv[0]; runpy would skip the original scanner condition.
        exec(compile(fixture.read_bytes(), str(fixture), "exec"),
             {"__name__": "serial_original_fixture", "__file__": str(fixture)})

try:
    install()
except BaseException:
    # Python otherwise prints sitecustomize errors and continues into main.
    os.write(2, b"Serial fixture guard initialization failed; execution refused.\n")
    os._exit(78)
