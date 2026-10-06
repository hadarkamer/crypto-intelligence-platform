"""No-I/O price adapters for the experimental execution boundary.

The caller owns collection, endpoint provenance and the existing request budget.
Only official MAINNET perpetual TRADE one-minute candles can become formula
bars. A complete source range is possible only from an exact minute boundary
through consecutive CLOSED candles; its timestamp remains the last candle close.

Existing TESTNET REST mark samples remain valid current-price observations, but
neither those samples nor trade candles prove a continuous MARK history. There
is deliberately no switch that upgrades a sampled history to complete history.
No collector, scheduler, subscription, database or exchange request is created.
"""
from copy import deepcopy

import experimental_execution_contract as contract
from . import card_lifecycle as life
from . import experimental_execution_evidence as execution_evidence
from . import r2732_conditional_stop as bars

MAX_CANDLES = 5000
MINUTE_MS = 60000
MAX_SAMPLE_AGE_MS = 15000
MARK_HISTORY_UNAVAILABLE = 'CONTINUOUS_REFERENCE_MARK_HISTORY_UNAVAILABLE'


class PriceEvidenceError(ValueError):
    """Fixed, non-sensitive evidence rejection codes."""


def capabilities():
    return dict(source_closed_trade_bars=True, source_exact_minute_ranges=True,
        source_intraminute_reference_ranges=False, testnet_current_mark=True,
        testnet_continuous_mark_history=False, creates_network_requests=False,
        creates_subscriptions=False, changes_request_budget=False)


def _identity(environment, symbol):
    if environment != 'mainnet':
        raise PriceEvidenceError('MAINNET_SOURCE_CANDLES_REQUIRED')
    life.ident(symbol, r'[A-Z][A-Z0-9]{0,19}')


def _observation(observed_at_ms, now_ms):
    life.moment(observed_at_ms)
    life.moment(now_ms)
    if observed_at_ms > now_ms:
        raise PriceEvidenceError('SOURCE_OBSERVATION_IN_FUTURE')


def normalize_closed_candles(raw, *, environment, symbol, observed_at_ms, now_ms):
    """Normalize an already collected official ``candleSnapshot`` response.

    ``observed_at_ms`` must be the collector's original conservative observation
    boundary (request start for REST), never cache retrieval or adapter call time.
    The final in-progress candle is excluded even if its OHLC is available.
    Returning bars does not by itself claim complete reference-window coverage.
    """
    try:
        _identity(environment, symbol)
        _observation(observed_at_ms, now_ms)
        if not isinstance(raw, list) or len(raw) > MAX_CANDLES:
            raise PriceEvidenceError('BOUNDED_SOURCE_CANDLE_LIST_REQUIRED')
        normalized = {}
        for row in raw:
            if (not isinstance(row, dict) or len(row) > 16
                    or row.get('s') != symbol or row.get('i') != '1m'
                    or type(row.get('t')) is not int or type(row.get('T')) is not int
                    or row['t'] % MINUTE_MS or row['T'] != row['t'] + MINUTE_MS - 1):
                raise PriceEvidenceError('EXACT_PERPETUAL_TRADE_MINUTE_REQUIRED')
            candle = dict(open_at_ms=row['t'], **{
                target: row[source] for target, source in
                (('open', 'o'), ('high', 'h'), ('low', 'l'), ('close', 'c'))})
            when, prices, _ = bars._bar(candle)
            canonical = dict(open_at_ms=when, **{
                key: format(value.normalize(), 'f') for key, value in prices.items()})
            if when > observed_at_ms:
                raise PriceEvidenceError('SOURCE_CANDLE_OPENS_AFTER_OBSERVATION')
            # Close timestamps are inclusive; closure occurs on the next ms.
            if when + MINUTE_MS > observed_at_ms:
                continue
            if when in normalized and normalized[when] != canonical:
                raise PriceEvidenceError('CONFLICTING_SOURCE_CANDLE_DUPLICATE')
            normalized[when] = canonical
        return [normalized[key] for key in sorted(normalized)]
    except PriceEvidenceError:
        raise
    except (KeyError, TypeError, ValueError, ArithmeticError):
        raise PriceEvidenceError('INVALID_SOURCE_CANDLE_EVIDENCE') from None


def source_range(raw, *, message, environment, observed_at_ms, now_ms):
    """Return an exact, fresh, complete CLOSED source range or reject it.

    No containing-candle approximation for intraminute MaxPain references, no
    carrying the last close through missing minutes, and no partial-candle
    promotion. Stale evidence retains its timestamp and cannot authorize entry.
    """
    msg = contract.validate(message)
    reference = contract.moment_ms(msg['source_at'])
    if reference % MINUTE_MS:
        raise PriceEvidenceError('EXACT_SOURCE_REFERENCE_NOT_RECOVERABLE_FROM_MINUTE_CANDLES')
    rows = normalize_closed_candles(raw, environment=environment, symbol=msg['symbol'],
        observed_at_ms=observed_at_ms, now_ms=now_ms)
    rows = [row for row in rows if row['open_at_ms'] >= reference]
    if not rows:
        raise PriceEvidenceError('NO_CLOSED_SOURCE_REFERENCE_WINDOW')
    last_close = rows[-1]['open_at_ms'] + MINUTE_MS - 1
    if not 0 <= now_ms - last_close <= MAX_SAMPLE_AGE_MS:
        raise PriceEvidenceError('SOURCE_CLOSED_RANGE_STALE')
    expected_count = (rows[-1]['open_at_ms'] - reference) // MINUTE_MS + 1
    if (rows[0]['open_at_ms'] != reference or len(rows) != expected_count
            or any(right['open_at_ms'] != left['open_at_ms'] + MINUTE_MS
                   for left, right in zip(rows, rows[1:]))):
        raise PriceEvidenceError('SOURCE_REFERENCE_WINDOW_GAP')
    return dict(environment='mainnet', symbol=msg['symbol'],
        price_source=msg['policy']['source_price'], reference_at_ms=reference,
        at_ms=last_close, history_complete=True,
        low=life.text(min(life.number(row['low'], positive=True) for row in rows)),
        high=life.text(max(life.number(row['high'], positive=True) for row in rows)),
        price=rows[-1]['close'])


def current_mark(raw, *, account, symbol, observed_at_ms, now_ms):
    """Preserve an already budgeted REST metaAndAssetCtxs sample and timestamp."""
    return execution_evidence.mark_sample(raw, account=account, symbol=symbol,
        observed_at_ms=observed_at_ms, now_ms=now_ms)


class SampledMarkProvider:
    """Fail-closed bridge for existing mark sampling, including after restart.

    ``sample_reader`` supplies the existing collector's ``(raw, observed_at_ms)``
    where raw is the official ``metaAndAssetCtxs`` response. MARK is public for
    each symbol; the requested account is bound locally for execution routing.
    It must not make an unbudgeted request. This adapter neither creates a reader
    nor assumes that persisting many current observations proves unseen extrema.
    A restored sample can still be used if fresh, never as historical authority.
    """
    def __init__(self, sample_reader):
        if not callable(sample_reader):
            raise PriceEvidenceError('EXISTING_MARK_SAMPLE_READER_REQUIRED')
        self._read = sample_reader

    def mark(self, account, symbol, *, now_ms):
        raw, observed_at_ms = self._read(account, symbol)
        return current_mark(raw, account=account, symbol=symbol,
            observed_at_ms=observed_at_ms, now_ms=now_ms)

    def mark_window(self, account, symbol, reference_at_ms, *, now_ms):
        # Validation cannot be bypassed by metadata claiming an invented feed.
        life.address(account)
        life.ident(symbol, r'[A-Z][A-Z0-9]{0,19}')
        life.moment(reference_at_ms)
        life.moment(now_ms)
        # Do not call the reader: another current quote cannot repair history.
        raise PriceEvidenceError(MARK_HISTORY_UNAVAILABLE)

    def diagnosis(self):
        # Stable serializable facts may be persisted in the caller's normal
        # audit record. Restart, dense sampling and healthy sockets do not alter
        # this capability gap, so no volatile 'history_complete' flag is kept.
        return deepcopy(dict(code=MARK_HISTORY_UNAVAILABLE,
            history_complete=False, recoverable_by_current_quote=False,
            recoverable_by_trade_candles=False,
            recoverable_by_sample_persistence=False,
            required='AUTHORITATIVE_REFERENCE_TO_CURRENT_TESTNET_MARK_HISTORY'))


class CachedPriceEvidence:
    """Read an injected existing source cache; never start a price collector.

    ``source_reader(message)`` returns ``environment``, ``symbol``,
    ``observed_at_ms`` and raw ``candles``. The trusted collector must bind this
    metadata to its actual request endpoint, and persist the original timestamp.
    No environment selection or timestamp reconstruction is performed here.
    """
    def __init__(self, source_reader, sample_reader):
        if not callable(source_reader):
            raise PriceEvidenceError('EXISTING_SOURCE_CACHE_READER_REQUIRED')
        self._source_reader = source_reader
        self._marks = SampledMarkProvider(sample_reader)

    def _source(self, message):
        msg = contract.validate(message)
        value = self._source_reader(deepcopy(msg))
        if (not isinstance(value, dict) or set(value) != {
                'environment', 'symbol', 'observed_at_ms', 'candles'}
                or value['symbol'] != msg['symbol']):
            raise PriceEvidenceError('EXACT_SOURCE_CACHE_IDENTITY_REQUIRED')
        return msg, value

    def source_range(self, message, now_ms):
        msg, value = self._source(message)
        return source_range(value['candles'], message=msg,
            environment=value['environment'], observed_at_ms=value['observed_at_ms'],
            now_ms=now_ms)

    def closed_bars(self, message, now_ms):
        msg, value = self._source(message)
        rows = normalize_closed_candles(value['candles'], symbol=msg['symbol'],
            environment=value['environment'], observed_at_ms=value['observed_at_ms'],
            now_ms=now_ms)
        reference = contract.moment_ms(msg['source_at'])
        return [row for row in rows if row['open_at_ms'] >= reference]

    def mark(self, account, symbol, now_ms):
        return self._marks.mark(account, symbol, now_ms=now_ms)

    def mark_window(self, account, symbol, reference_at_ms, now_ms):
        return self._marks.mark_window(account, symbol, reference_at_ms, now_ms=now_ms)

    def diagnosis(self):
        return self._marks.diagnosis()


class ArchivedPriceEvidence:
    """Concrete readonly producer archive bridge, with no MARK-history claim.

    Source candles advance conditional stops independently from admission. The
    live collector obtains current MARK from its shared official market
    context; this provider never creates a competing quote request.
    """
    def __init__(self, archive, *, configuration_error=None):
        from .experimental_source_archive import ReadOnlySourceArchive
        if type(archive) is not ReadOnlySourceArchive:
            raise PriceEvidenceError('EXPLICIT_SOURCE_ARCHIVE_READER_REQUIRED')
        self.archive = archive
        if configuration_error not in (None, 'SOURCE_ARCHIVE_CONFIGURATION_INVALID'):
            raise PriceEvidenceError('FIXED_SOURCE_CONFIGURATION_DIAGNOSIS_REQUIRED')
        self.configuration_error = configuration_error

    def source_range(self, message, now_ms):
        value = self.archive.read(message, now_ms=now_ms)
        return source_range(value['candles'], message=message,
            environment=value['environment'], observed_at_ms=value['observed_at_ms'], now_ms=now_ms)

    def closed_bars(self, message, now_ms):
        return self.closed_bars_since(message, now_ms)

    def closed_bars_since(self, message, now_ms, cursor_at_ms=None):
        msg = contract.validate(message)
        reference = contract.moment_ms(msg['source_at'])
        if cursor_at_ms is not None:
            life.moment(cursor_at_ms)
            if cursor_at_ms < reference - MINUTE_MS or cursor_at_ms % MINUTE_MS:
                raise PriceEvidenceError('EXACT_SOURCE_ARCHIVE_CURSOR_REQUIRED')
        start = max(reference, cursor_at_ms if cursor_at_ms is not None else reference)
        value = self.archive.read(msg, now_ms=now_ms, start_at_ms=start, require_full=False)
        rows = normalize_closed_candles(value['candles'], environment=value['environment'],
            symbol=msg['symbol'], observed_at_ms=value['observed_at_ms'], now_ms=now_ms)
        if rows and (rows[0]['open_at_ms'] != start or any(
                right['open_at_ms'] != left['open_at_ms'] + MINUTE_MS
                for left, right in zip(rows, rows[1:]))):
            raise PriceEvidenceError('SOURCE_REFERENCE_WINDOW_GAP')
        return rows

    def mark_window(self, account, symbol, reference_at_ms, now_ms):
        life.address(account)
        life.ident(symbol, r'[A-Z][A-Z0-9]{0,19}')
        life.moment(reference_at_ms)
        life.moment(now_ms)
        raise PriceEvidenceError(MARK_HISTORY_UNAVAILABLE)

    def diagnosis(self):
        return dict(code=MARK_HISTORY_UNAVAILABLE, history_complete=False,
            source_archive_configured=self.archive.configured,
            source_archive_error=self.configuration_error,
            source_closed_trade_bars=True, source_exact_minute_ranges=True,
            source_intraminute_reference_ranges=False,
            current_mark_source='SHARED_TESTNET_META_AND_ASSET_CONTEXTS',
            testnet_continuous_mark_history=False,
            fast_asset_contexts_authoritative=False,
            missing_fast_context_guarantees=['VENUE_EVENT_TIME', 'SEQUENCE_OR_REPLAY',
                'ALL_INTERMEDIATE_MARK_CHANGES'],
            creates_exchange_requests=False, creates_subscriptions=False,
            changes_request_budget=False)


def build_price_evidence(env, journal=None, budget=None, clock=None):
    """Compose without connecting or borrowing an execution/primary DB URL.

    ``journal``/``budget``/``clock`` are accepted for the startup factory
    contract; no execution storage or exchange resources are repurposed.
    Missing source configuration remains an explicit per-read failure, allowing
    already-owned venue positions to retain their original protection path.
    """
    from .experimental_source_archive import ReadOnlySourceArchive
    try:
        reader = ReadOnlySourceArchive(env.get('HL_TESTNET_EXPERIMENTAL_SOURCE_ARCHIVE_URL'))
    except PriceEvidenceError:
        # Source misconfiguration blocks source-dependent admission/advances,
        # never construction of the existing-position protection supervisor.
        return ArchivedPriceEvidence(ReadOnlySourceArchive(),
            configuration_error='SOURCE_ARCHIVE_CONFIGURATION_INVALID')
    return ArchivedPriceEvidence(reader)
