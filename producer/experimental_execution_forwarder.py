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
    mode = env.get('EXPERIMENTAL_EXECUTION_FORWARD_MODE')
    if mode not in (transport.MODE, transport.APPROVED_MODE):
        return None
    try:
        required_bridge = bridge.APPROVED_MODE if mode == transport.APPROVED_MODE else bridge.MODE
        if env.get('EXPERIMENTAL_EXECUTION_BRIDGE_MODE') != required_bridge:
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
            fence=fence, mode=mode).validate()
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
        self.wake = threading.Event()

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
            self.wake.clear()
            report = self.run_once()
            emit(report)
            if report['status'] == 'DISABLED':
                return
            if self.config and self.config.transport.mode == transport.APPROVED_MODE:
                # Clear before reading, so a commit during a slow sender cycle
                # remains set and prompts another bounded, non-overlapping read.
                self.wake.wait(DELAY_SECONDS)
            elif stop.wait(DELAY_SECONDS):
                return

    def source_committed(self, source_key):
        if (self.config and self.config.transport.mode == transport.APPROVED_MODE
                and source_key in bridge.approved_source_keys(self.config.scopes)):
            self.wake.set()


_BACKGROUND_LOCK = threading.Lock()
_BACKGROUND = None


def maybe_start(*, env=None):
    """Opt-in source-process lifecycle; no separate sender poll before wake.

    This is called by main startup. The transaction hook only sets an Event;
    it never reads a DB, posts HTTP, or starts a thread while holding a DB lock.
    Standalone use still has the documented 30-second recovery cadence.
    """
    global _BACKGROUND
    values = os.environ if env is None else env
    if values.get('EXPERIMENTAL_EXECUTION_FORWARD_MODE') != transport.APPROVED_MODE:
        return None
    try:
        cfg = configuration(values)
        with _BACKGROUND_LOCK:
            if _BACKGROUND is not None and _BACKGROUND[2].is_alive():
                return _BACKGROUND[0]
            worker, stop = Forwarder(cfg), threading.Event()
            def run():
                try:
                    worker.run(stop, emit=_emit)
                except Exception:
                    _emit(dict(status='FORWARD_BACKGROUND_UNAVAILABLE', source_records_changed=0,
                               exchange_requests_sent=0))
            thread = threading.Thread(target=run, name='approved-alert-forwarder', daemon=True)
            _BACKGROUND = worker, stop, thread
            thread.start()
            return worker
    except Exception:
        _emit(dict(status='FORWARD_CONFIGURATION_UNAVAILABLE', source_records_changed=0,
                   exchange_requests_sent=0))
        return None


def notify_source_commit(source_key):
    # Caller is already outside the source transaction. A nonblocking local
    # notification cannot delay Telegram or roll back the committed decision.
    current = _BACKGROUND
    if current is not None:
        current[0].source_committed(source_key)


def stop_background(timeout=15):
    global _BACKGROUND
    with _BACKGROUND_LOCK:
        current = _BACKGROUND
        if current is None:
            return True
        current[1].set()
        current[0].wake.set()
    current[2].join(timeout)
    with _BACKGROUND_LOCK:
        stopped = not current[2].is_alive()
        if stopped and _BACKGROUND is current:
            _BACKGROUND = None
        return stopped


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
                previous[kind] = signal.signal(kind, lambda *_: (stop.set(), worker.wake.set()))
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
