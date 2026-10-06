"""Default-off standalone source-to-Testnet-plan gateway worker.

Launch only after approving the concrete source and receiver release:
``python -m experimental_execution_forwarder``. Configuration explicitly names
the HTTPS recipient, shared record-authentication key, prospective release
fence and existing subscription hashes. No trading keys are read here.

The source producer must independently enable prospective MaxPain evidence
capture with EXPERIMENTAL_EXECUTION_BRIDGE_MODE. This worker cannot reconstruct
missing evidence or turn historical notifications into new execution plans.
All database reads and HTTPS delivery use the reviewed bridge/transport paths.
No source state, alert, strategy or database schema is modified by this worker.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
import json
import os
import signal
import threading

import experimental_execution_bridge as bridge
import experimental_execution_contract as contract
import experimental_execution_transport as transport

DELAY_SECONDS = 30
# One subscription normally contains every formula. Restrict the configured
# recipient count to the existing alert forwarder's operational capacity.
MAX_CONFIGURED_SCOPES = 4


@dataclass(frozen=True)
class Configuration:
    transport: transport.Config
    scopes: tuple[str, ...]


def configuration(env):
    """No opt-in means no credential lookup, database import or HTTP activity."""
    if env.get('EXPERIMENTAL_EXECUTION_FORWARD_MODE') != transport.MODE:
        return None
    try:
        if env.get('EXPERIMENTAL_EXECUTION_BRIDGE_MODE') != bridge.MODE:
            raise ValueError()
        scopes = json.loads(env.get('EXPERIMENTAL_EXECUTION_FORWARD_SCOPES', ''))
        hosts = json.loads(env.get('EXPERIMENTAL_EXECUTION_FORWARD_ALLOWED_HOSTS', ''))
        if (not isinstance(scopes, list) or not 0 < len(scopes) <= MAX_CONFIGURED_SCOPES
                or any(not isinstance(v, str) or not contract.HEX.fullmatch(v) for v in scopes)
                or len(set(scopes)) != len(scopes) or not isinstance(hosts, list)
                or any(not isinstance(v, str) for v in hosts)):
            raise ValueError()
        fence = datetime.fromisoformat(env.get('EXPERIMENTAL_EXECUTION_FORWARD_NOT_BEFORE', ''))
        cfg = transport.Config(
            endpoint=env.get('EXPERIMENTAL_EXECUTION_FORWARD_ENDPOINT', ''),
            allowed_hosts=tuple(hosts),
            secret=env.get('EXPERIMENTAL_EXECUTION_FORWARD_SECRET', ''),
            fence=fence, mode=transport.MODE).validate()
        return Configuration(cfg, tuple(sorted(scopes)))
    except (ValueError, TypeError, AttributeError, OverflowError):
        # Never include URLs, credentials, environment values or parsed input
        # in operator logs, including exception messages from third parties.
        raise transport.TransportError('FORWARD_CONFIGURATION_REQUIRED') from None


class Forwarder:
    """Single non-overlapping periodic sender with a bounded retained queue."""
    def __init__(self, config):
        if config is not None and type(config) is not Configuration:
            raise transport.TransportError('FORWARD_CONFIGURATION_REQUIRED')
        self.config = config
        self.sender = transport.Sender()
        self.lock = threading.Lock()
        self.cycles = 0

    def run_once(self):
        if not self.lock.acquire(blocking=False):
            return dict(status='FORWARD_CYCLE_ALREADY_RUNNING', attempted=0,
                        source_records_changed=0, exchange_requests_sent=0)
        try:
            # A later tick cannot overlap a slow source/HTTPS read. The sender
            # bounds each cycle; shutdown waits for the in-flight cycle only.
            report = self.sender.tick(self.config.scopes if self.config else (),
                                      self.config.transport if self.config else None)
            self.cycles += 1
            return dict(report, cycles=self.cycles)
        finally:
            self.lock.release()

    def run(self, stop, *, emit):
        """Fixed cadence after completion: no catch-up burst or parallel tick."""
        while not stop.is_set():
            report = self.run_once()
            emit(report)
            if report['status'] == 'DISABLED':
                return
            if stop.wait(DELAY_SECONDS):
                return


def _emit(report):
    print(json.dumps({'experimental_execution_forwarder': report}, sort_keys=True), flush=True)


def main(argv=None, *, env=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--once', action='store_true', help='Perform one bounded forwarding cycle')
    parser.add_argument('--check-config', action='store_true',
                        help='Validate explicit configuration without database or network access')
    args = parser.parse_args(argv)
    try:
        config = configuration(os.environ if env is None else env)
        if args.check_config:
            _emit(dict(status='CONFIGURED' if config else 'DISABLED',
                       scopes=len(config.scopes) if config else 0,
                       source_records_changed=0, exchange_requests_sent=0))
            return 0
        worker = Forwarder(config)
        if args.once or config is None:
            report = worker.run_once()
            _emit(report)
            return 0 if report['status'] in ('DISABLED', 'COMPLETED') else 2
        stop = threading.Event()
        previous = {}
        try:
            for kind in (signal.SIGTERM, signal.SIGINT):
                previous[kind] = signal.signal(kind, lambda *_: stop.set())
            worker.run(stop, emit=_emit)
        finally:
            for kind, handler in previous.items():
                signal.signal(kind, handler)
        return 0
    except Exception:
        _emit(dict(status='FORWARD_INITIALIZATION_OR_CYCLE_UNAVAILABLE',
                   source_records_changed=0, exchange_requests_sent=0))
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
