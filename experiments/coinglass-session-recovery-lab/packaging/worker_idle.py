"""Inert temporary worker: no imports of app/collector, network, DB or browser."""
import json
import os
import signal
import threading


def main():
    # No credentials are expected on this fixture worker. Report no values.
    forbidden = (
        "COINGLASS_COOKIE_HEADER", "COINGLASS_STORAGE_STATE_JSON",
        "COINGLASS_COLLECTOR_TOKEN", "DATABASE_URL", "OPENAI_API_KEY",
        "SUPABASE_SERVICE_ROLE_KEY", "SUPABASE_ANON_KEY",
    )
    if os.getuid() == 0 or any(os.environ.get(key) for key in forbidden):
        print(json.dumps({"mode": "fixture_idle", "status": "unsafe_configuration"}), flush=True)
        return 2
    done = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: done.set())
    signal.signal(signal.SIGINT, lambda *_: done.set())
    print(json.dumps({"mode": "fixture_idle", "status": "ready", "tests_started": 0}), flush=True)
    done.wait()
    print(json.dumps({"mode": "fixture_idle", "status": "stopped"}), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
