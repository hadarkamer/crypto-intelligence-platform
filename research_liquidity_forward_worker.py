"""Separate prospective DOGE reader and paper book under one durable study ID.

Source reads are read-only. The existing checkpoint store writes only the new
study's registration, journal and projection. No orders or alert delivery.
"""
from __future__ import annotations

import copy
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import platform
import re
import sys
import time
import uuid

from research_forward_paper_worker import ForwardPaperWorker as BaseWorker, fetch_quotes
import research_liquidity_forward_engine as core
import research_liquidity_forward_protocol as reference
import research_liquidity_forward_source as source

STUDY_ID = core.STUDY_ID
PROTOCOL_SHA256 = core.PROTOCOL_SHA256
PACKAGE = Path(__file__).resolve().parent / 'liquidity_forward_runtime'
TRANSPORT_VERSION = 'liquidity-forward-readonly-binance-v1'


def _utc():
    return datetime.now(timezone.utc).isoformat()


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)


def _sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def build_config(repo_dir=None):
    root = Path(repo_dir) if repo_dir else Path(__file__).resolve().parent
    protocol = json.loads((root / 'research_protocols/doge_liquidity_source_extension_v1.json').read_text(encoding='utf-8'))
    if hashlib.sha256(_json(protocol).encode()).hexdigest() != PROTOCOL_SHA256:
        raise ValueError('Frozen liquidity protocol changed')
    if protocol['activation'] is not None or protocol['study_start_utc'] is not None:
        raise ValueError('Frozen protocol must remain separate from activation state')
    if _sha(root / 'research_ordered_question_catalog.py') != protocol['sources']['native']['extractor_sha256']:
        raise ValueError('Native source extractor differs from frozen contract')
    candidate = protocol['hypothesis']['candidate_key']
    scope_key = hashlib.sha256((STUDY_ID + '|' + candidate).encode()).hexdigest()
    return {
        'version': STUDY_ID, 'protocol_sha256': PROTOCOL_SHA256,
        'timezone': 'Asia/Jerusalem', 'books': ['DOGE_only'],
        'scopes': [{'scope_key': scope_key, 'candidate_key': candidate, 'symbol': 'DOGE', 'direction': 'SHORT',
                    'candidate_definition_sha256': PROTOCOL_SHA256, 'formula_version': core.FORMULA_VERSION,
                    'window_minutes': 720, 'threshold_bps': 50}],
        'barrier_bps': 50, 'net_cost_pp': 0.1, 'poll_seconds': 10,
        'entry_expiry_seconds': 90, 'quote_max_elapsed_seconds': 5, 'max_quote_gap_seconds': 30,
        'quote_source': 'BINANCE_SPOT_BOOK_TICKER', 'quote_symbols': {'DOGE': 'DOGEUSDT'},
        'quote_endpoint': 'https://data-api.binance.vision/api/v3/ticker/bookTicker',
        'required_markers': ['alert_time_utc', 'delivered_at_utc', 'event_created_at_utc',
                             'liquidity_evidence_created_at_utc', 'membership_created_at_utc',
                             'parent_created_at_utc', 'parent_confirmed_at_utc',
                             'btc_bar_created_at_utc', 'btc_observed_close_utc'],
        'parent_policy_version': 'btc-parent-close-reversal-200bps-v1',
        'warmup_minutes': 30, 'preparation_event_ids': protocol['epoch']['preparation_event_ids'],
        'source_query_sha256': hashlib.sha256(source.SQL_TEMPLATE.encode()).hexdigest(),
        'source_adapter_sha256': _sha(root / 'research_liquidity_forward_source.py'),
        'engine_source_sha256': _sha(root / 'research_liquidity_forward_engine.py'),
        'reference_source_sha256': _sha(root / 'research_liquidity_forward_protocol.py'),
        'worker_source_sha256': _sha(root / 'research_liquidity_forward_worker.py'),
        'checkpoint_store_source_sha256': _sha(root / 'research_forward_paper_store.py'),
        'base_worker_transport_source_sha256': _sha(root / 'research_forward_paper_worker.py'),
        'seed_registry_sha256': _sha(root / 'liquidity_forward_runtime/trusted_source_code_registry.json'),
        'transport_version': TRANSPORT_VERSION,
        'required_python_runtime': 'CPython 3.11.9',
        'required_source_producer_runtime': 'CPython 3.11.9',
        'availability_clock': 'First independently received full source response with all eligibility prerequisites; recorded clocks are annotations.',
        'liquidity_evidence_marker': 'Maximum actual source/proof recorded prerequisite clock, not a legacy match row and not the response clock.',
        'bootstrap': 'First actual source population and its parent identities are audit-only and excluded.',
        'history': 'Full bounded post-epoch delivered source view. Count only valid points known in the retained response; late prior proofs do not replace frozen choices.',
        'paper_fill': 'Observed midpoint proxy; fixed all-in cost includes spread once. No execution or intragap-first-touch claim.',
        'quotes_when_needed_only': True,
    }


def _aggregate(summary):
    from research_forward_paper_worker import _aggregate as base_aggregate
    result = base_aggregate(summary)
    for name, book in summary.get('books', {}).items():
        for field in ('completed_source_history_uncertain', 'completed_without_source_or_quote_uncertainty'):
            if field in book:
                result['books'][name][field] = copy.deepcopy(book[field])
    if 'uncertainty_flag_scope' in summary:
        result['uncertainty_flag_scope'] = summary['uncertainty_flag_scope']
    for name in ('source_code_registry_count', 'source_receipt_count', 'source_closure_count',
                 'raw_source_count', 'pending_source_count', 'last_source_closure_sha256'):
        if name in summary:
            result[name] = copy.deepcopy(summary[name])
    return result


class LiquidityForwardWorker(BaseWorker):
    def __init__(self, *, package=None, store_factory=None, engine_class=None,
                 candidate_fetch=None, quote_fetch=None, first_delay=5, retry_seconds=30):
        super().__init__(package=package or PACKAGE, store_factory=store_factory,
                         engine_class=engine_class or core.PaperStudy,
                         candidate_fetch=candidate_fetch or source.fetch_candidates,
                         quote_fetch=quote_fetch or fetch_quotes,
                         first_delay=first_delay, retry_seconds=retry_seconds)
        self._health.update(study_id=STUDY_ID, protocol_sha256=PROTOCOL_SHA256,
                            phase='NOT_REGISTERED', new_study=True,
                            source_contract=core.FORMULA_VERSION,
                            skipped_quote_cycles=0, quote_requests=0)

    def _log(self, *, force=False):
        tick = time.monotonic()
        if force or self._health['state'] != self._last_logged_state or tick - self._last_log >= 60:
            print('[LIQUIDITY_FORWARD] ' + _json(self.status()), flush=True)
            self._last_log = tick
            self._last_logged_state = self._health['state']

    def _runtime_identity(self):
        root = Path(__file__).resolve().parent
        commit = self._health.get('runtime_commit')
        if not isinstance(commit, str) or not re.fullmatch(r'[0-9a-f]{40}', commit):
            raise ValueError('Full runtime commit is required for source identity')
        seeds = json.loads((self.package / 'trusted_source_code_registry.json').read_text(encoding='utf-8'))
        return {'packet_id': str(uuid.uuid4()), 'observed_at_utc': _utc(), 'runtime_commit': commit,
                'code_versions': {name: _sha(root / name) for name in core.SOURCE_CODE_FILES},
                'python_implementation': platform.python_implementation(),
                'python_version': sys.version.split()[0], 'protocol_sha256': PROTOCOL_SHA256,
                'trusted_seed_registries': seeds}

    def _open_owned(self):
        self._config = build_config()
        self._template = source.SQL_TEMPLATE
        factory = self._store_factory
        if factory is None:
            from research_forward_paper_store import PostgresCheckpoint
            factory = PostgresCheckpoint
        self._store = factory(self._dsn, STUDY_ID, self._config)
        if not self._store.acquire():
            self._discard_owned()
            self._publish(state='STANDBY', leader=False)
            self._log()
            return False
        self._engine = self._store.restore_engine(self._engine_class)
        self._engine.observe_runtime_identity(self._runtime_identity())
        proposed = {**self._health, **_aggregate(self._engine.summary()),
                    'state': 'READY_FOR_SOURCE', 'phase': 'REGISTERED_WAITING_FOR_SOURCE', 'leader': True}
        # This commits registration/restore/restart and code identities before
        # the first source read, including on recovery after uncertain COMMIT.
        self._store.checkpoint(self._engine, proposed)
        proposed.update(journal_events=self._store.journal_sequence,
                        durable_journal_seq=self._store.journal_sequence)
        self._health = proposed
        return True

    def _apply_owned(self, packet, *, candidate):
        began = _utc()
        timer = time.monotonic()
        result = self._engine.observe_candidates(packet) if candidate else self._engine.observe_quotes(packet)
        rejected = result.get('last_observation_rejected')
        proposed = {**self._health, **_aggregate(result), 'leader': True,
                    'state': 'OBSERVATION_REJECTED' if rejected else 'OBSERVING',
                    'last_processing_started_utc': began, 'last_processing_completed_utc': _utc(),
                    'last_processing_elapsed_seconds': time.monotonic() - timer,
                    'last_error_type': 'ObservationRejected' if rejected else None,
                    'last_error_code': None}
        if not rejected:
            proposed['last_source_received_utc' if candidate else 'last_quote_received_utc'] = packet['response_received_utc']
            epoch = datetime.fromisoformat(result['study_start_utc'].replace('Z', '+00:00'))
            warming = datetime.now(timezone.utc) < epoch + timedelta(minutes=30)
            proposed['phase'] = 'WARMING_UP' if warming else 'OBSERVING'
        self._store.checkpoint(self._engine, proposed)
        proposed.update(journal_events=self._store.journal_sequence,
                        durable_journal_seq=self._store.journal_sequence)
        self._health = proposed
        if rejected:
            from research_forward_paper_worker import ObservationRejected
            raise ObservationRejected('Liquidity observation rejected')

    def _quotes_needed(self):
        book = self._engine.summary().get('books', {}).get('DOGE_only', {})
        return bool(book.get('active') or book.get('reservation') or
                    any(book.get('choice_status_counts', {}).get(status, 0) for status in ('PLANNED', 'RESERVED')))

    def _cycle_owned(self):
        try:
            if self._store is None and not self._open_owned():
                return
            policy = self._engine.policy
            bootstrap = not self._engine.summary()['bootstrap_complete']
            packet = self._candidate_fetch(self._dsn, policy, self._template, bootstrap=bootstrap,
                                           eligible_keys=self._engine.eligible_event_keys(),
                                           code_registry=self._engine.source_code_registry())
            self._apply_owned(packet, candidate=True)
            if self._quotes_needed():
                quote = self._quote_fetch(policy)
                self._health = {**self._health, 'quote_requests': self._health['quote_requests'] + 1}
                self._apply_owned(quote, candidate=False)
            else:
                self._health = {**self._health, 'skipped_quote_cycles': self._health['skipped_quote_cycles'] + 1}
            proposed = {**self._health, 'cycles': self._health['cycles'] + 1, 'last_cycle_completed_utc': _utc()}
            self._store.update_health(proposed)
            self._health = proposed
            self._log()
        except Exception as error:
            safe_code = str(error) if isinstance(error, source.SourceRejected) and re.fullmatch(r'[A-Z0-9_]{1,80}', str(error)) else None
            failed = {**self._health, 'state': 'RETRYING', 'leader': False,
                      'failures': self._health['failures'] + 1,
                      'last_error_type': type(error).__name__, 'last_error_code': safe_code, 'last_failure_utc': _utc()}
            if self._store is not None:
                try:
                    self._store.update_health(failed)
                except Exception:
                    pass
            self._discard_owned()
            self._health = failed
            raise


WORKER = LiquidityForwardWorker()


async def start_worker(*, database_url=None, enabled=False, db_path=None, runtime_commit=None):
    return await WORKER.start(database_url=database_url, enabled=enabled, db_path=db_path, runtime_commit=runtime_commit)


async def stop_worker():
    await WORKER.stop()


def status():
    return WORKER.status()
