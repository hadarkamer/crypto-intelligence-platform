"""Inert status HTTP service and bounded offline test CLI. No bot entrypoint."""
import argparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path

from .core import ROOT, Runner, create_manifest


def handler_for(runner):
    class Handler(BaseHTTPRequestHandler):
        server_version = "OfflinePreflight"
        sys_version = ""

        def do_GET(self):
            if self.path == "/healthz":
                status, payload = 200, {"alive": True, "test_status": runner.snapshot()["status"]}
            elif self.path in ("/", "/summary"):
                status, payload = 200, runner.snapshot()
            else:
                status, payload = 404, {"error": "NOT_FOUND"}
            data = json.dumps(payload, sort_keys=True, allow_nan=False).encode()
            if len(data) > 65536:
                status, data = 500, b'{"error":"SUMMARY_LIMIT"}'
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            self.wfile.write(data)

        def do_POST(self):
            self.send_error(405, "NO_CONTROLS")

        def log_message(self, *_args):
            pass
    return Handler


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("manifest", "check", "serve"))
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--manifest", type=Path, default=ROOT / "hl_testnet_runtime/render_preflight/manifest.json")
    parser.add_argument("--expected-hash", default=os.environ.get("PREFLIGHT_EXPECTED_MANIFEST_SHA256", ""))
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--no-postgres", action="store_true")
    args = parser.parse_args()
    if args.command == "manifest":
        print(create_manifest(args.root, args.manifest))
        return
    runner = Runner(args.root, args.manifest, args.expected_hash, output_dir=args.output_dir,
                    allow_postgres=not args.no_postgres)
    if args.command == "check":
        runner.start_once()
        runner.join()
        print(json.dumps(runner.snapshot(), indent=2))
        status = runner.snapshot()["status"]
        raise SystemExit(0 if status == "PASSED" else (2 if status == "PARTIAL" else 1))
    port = int(os.environ.get("PORT", "10000"))
    if not 1024 <= port <= 65535:
        raise ValueError("INVALID_PORT")
    server = ThreadingHTTPServer(("0.0.0.0", port), handler_for(runner))
    server.daemon_threads = True
    runner.start_once()
    server.serve_forever(poll_interval=0.5)


if __name__ == "__main__":
    main()
