"""Read the producer's existing immutable Hyperliquid TRADE minute archive.

This connection is explicitly separate from the Testnet execution journal. It
has no HTTP fallback, collector, schema installation or write path. A missing
or delayed source archive must never become a claim that a minute was observed.
Only closed bars can be recovered here; neither intraminute extrema nor MARK
history can be reconstructed from this archive.
"""
from datetime import datetime, timezone
from urllib.parse import urlsplit, parse_qsl

import experimental_execution_contract as contract
from . import card_lifecycle as life

MINUTE_MS = 60000
MAX_ROWS = 5000
SYMBOLS = frozenset(('BTC', 'ETH', 'SOL', 'HYPE', 'DOGE', 'XRP'))
IMMUTABLE_FUNCTION_SHA256 = 'd4a6ea0229aff09b95588ded65e6cb8d0dff230251e6b8d6ee10203ac1927720'
SQL = '''SELECT route,symbol,open_time_utc,close_time_utc,open,high,low,close,
    volume,created_at_utc FROM public.research_price_archive_bars
    WHERE route=%s AND symbol=%s AND open_time_utc >= %s
    AND open_time_utc < %s ORDER BY open_time_utc LIMIT %s'''
IMMUTABILITY_SQL = '''SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_trigger AS t
    JOIN pg_catalog.pg_proc AS p ON p.oid=t.tgfoid
    JOIN pg_catalog.pg_namespace AS n ON n.oid=p.pronamespace
    WHERE t.tgrelid=to_regclass('public.research_price_archive_bars')
    AND t.tgname='research_price_archive_immutable_v1' AND NOT t.tgisinternal
    AND t.tgenabled IN ('O','A') AND t.tgtype=27
    AND t.tgqual IS NULL AND t.tgattr=''::int2vector AND t.tgnargs=0
    AND p.proname='research_price_archive_immutable_v1' AND n.nspname='public'
    AND p.pronargs=0 AND p.prorettype='trigger'::regtype
    AND encode(sha256(convert_to(p.prosrc,'UTF8')),'hex')=%s)'''


def _error(code):
    # Lazy import keeps the provider factory free of an import cycle.
    from .experimental_price_evidence import PriceEvidenceError
    return PriceEvidenceError(code)


def _millis(value):
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise _error('EXACT_ARCHIVED_CANDLE_TIMESTAMP_REQUIRED')
    number = value.timestamp() * 1000
    if not number.is_integer() or number <= 0:
        raise _error('EXACT_ARCHIVED_CANDLE_TIMESTAMP_REQUIRED')
    return int(number)


def _time(value):
    return datetime.fromtimestamp(value / 1000, timezone.utc)


class ReadOnlySourceArchive:
    """Bounded source-only SQL with a separately selected DSN and CI route.

    Production URLs require a named database and TLS. URL options, application
    search paths and multi-host connections are rejected, so they cannot
    override the read-only/time-limit options pinned by this reader.
    """
    def __init__(self, dsn=None):
        self._dsn = self._validate_dsn(dsn, ci=False) if dsn else None
        self.configured = self._dsn is not None

    @staticmethod
    def _validate_dsn(dsn, *, ci):
        try:
            if not isinstance(dsn, str) or len(dsn) > 8192:
                raise ValueError()
            parsed = urlsplit(dsn)
            if (parsed.scheme not in ('postgres', 'postgresql') or not parsed.hostname
                    or parsed.fragment or parsed.path in ('', '/') or '/' in parsed.path[1:]
                    or not parsed.username or ',' in parsed.hostname
                    or any(k != 'sslmode' for k, _ in parse_qsl(parsed.query, strict_parsing=True))):
                raise ValueError()
            options = parse_qsl(parsed.query, strict_parsing=True)
            if len(options) > 1:
                raise ValueError()
            if ci:
                if parsed.hostname not in ('127.0.0.1', 'localhost') or parsed.path != '/hl_journal_ci':
                    raise ValueError()
                if options and options != [('sslmode', 'disable')]:
                    raise ValueError()
            elif (parsed.hostname in ('127.0.0.1', 'localhost', '::1')
                    or options and options[0][1] not in ('require', 'verify-ca', 'verify-full')):
                raise ValueError()
            # Parsing the port also rejects malformed/out-of-range values.
            parsed.port
            return dsn
        except (ValueError, TypeError):
            raise _error('EXPLICIT_READONLY_SOURCE_ARCHIVE_URL_REQUIRED') from None

    @classmethod
    def for_ci(cls, dsn):
        self = cls.__new__(cls)
        self._dsn = cls._validate_dsn(dsn, ci=True)
        self.configured = True
        return self

    def _connect(self):
        import psycopg
        from psycopg.conninfo import conninfo_to_dict
        ci = urlsplit(self._dsn).hostname in ('127.0.0.1', 'localhost')
        requested = dict(parse_qsl(urlsplit(self._dsn).query)).get('sslmode')
        parameters = conninfo_to_dict(self._dsn)
        if (not set(parameters) <= {'host', 'port', 'dbname', 'user', 'password', 'sslmode'}
                or ',' in parameters.get('host', '')
                or ci and (parameters.get('host') not in ('127.0.0.1', 'localhost')
                    or parameters.get('dbname') != 'hl_journal_ci')):
            raise _error('EXPLICIT_READONLY_SOURCE_ARCHIVE_URL_REQUIRED')
        # Use explicit validated identity kwargs, matching the existing native
        # CI guard; positional DSNs remain forbidden for the runtime database.
        parameters.update(autocommit=False, connect_timeout=2,
            sslmode='disable' if ci else (requested or 'require'),
            options='-c default_transaction_read_only=on -c statement_timeout=750 '
                    '-c lock_timeout=250 -c idle_in_transaction_session_timeout=2000')
        return psycopg.connect(**parameters)

    def read(self, message, *, now_ms, start_at_ms=None, require_full=True):
        msg = contract.validate(message)
        life.moment(now_ms)
        reference = contract.moment_ms(msg['source_at'])
        if reference % MINUTE_MS:
            raise _error('EXACT_SOURCE_REFERENCE_NOT_RECOVERABLE_FROM_MINUTE_CANDLES')
        start = reference if start_at_ms is None else start_at_ms
        life.moment(start)
        if start < reference or start % MINUTE_MS or start > now_ms:
            raise _error('EXACT_SOURCE_ARCHIVE_CURSOR_REQUIRED')
        end = now_ms // MINUTE_MS * MINUTE_MS
        if require_full and end - start > MAX_ROWS * MINUTE_MS:
            raise _error('SOURCE_ARCHIVE_REFERENCE_WINDOW_TOO_LARGE')
        end = min(end, start + MAX_ROWS * MINUTE_MS)
        if not self.configured:
            raise _error('SOURCE_ARCHIVE_NOT_CONFIGURED')
        route = 'HYPERLIQUID_HYPE_PERP_TRADE_1M' if msg['symbol'] == 'HYPE' else 'HYPERLIQUID_PERP_TRADE_1M'
        if msg['symbol'] not in SYMBOLS:
            raise _error('EXACT_SOURCE_ARCHIVE_SYMBOL_REQUIRED')
        if start == end:
            return dict(environment='mainnet', symbol=msg['symbol'], observed_at_ms=now_ms, candles=[])
        try:
            with self._connect() as conn:
                # Explicit transaction mode is defence in depth for injected
                # connectors too. No role/table/schema changes are performed.
                conn.execute('SET TRANSACTION READ ONLY')
                if conn.execute(IMMUTABILITY_SQL, (IMMUTABLE_FUNCTION_SHA256,)).fetchone() != (True,):
                    raise ValueError('IMMUTABLE_ARCHIVE_SCHEMA_REQUIRED')
                rows = conn.execute(SQL, (route, msg['symbol'], _time(start), _time(end), MAX_ROWS + 1)).fetchall()
        except Exception:
            raise _error('SOURCE_ARCHIVE_READ_UNAVAILABLE') from None
        if not isinstance(rows, list) or len(rows) > MAX_ROWS:
            raise _error('BOUNDED_SOURCE_ARCHIVE_RESULT_REQUIRED')
        candles = []
        ingested_at_ms = []
        for row in rows:
            if (not isinstance(row, (tuple, list)) or len(row) != 10
                    or row[0] != route or row[1] != msg['symbol'] or row[8] is not None):
                raise _error('EXACT_SOURCE_ARCHIVE_ROUTE_REQUIRED')
            opened, closed = _millis(row[2]), _millis(row[3])
            created = row[9]
            if (not start <= opened < end or opened % MINUTE_MS
                    or closed != opened + MINUTE_MS - 1
                    or not isinstance(created, datetime) or created.tzinfo is None
                    or not closed < created.timestamp() * 1000 <= now_ms):
                raise _error('CLOSED_SOURCE_ARCHIVE_PROVENANCE_REQUIRED')
            candles.append(dict(t=opened, T=closed, s=msg['symbol'], i='1m',
                **{key: contract.price(value) for key, value in zip(('o', 'h', 'l', 'c'), row[4:8])}))
            ingested_at_ms.append(int(created.timestamp() * 1000))
        # The caller normalizes exact geometry and detects gaps/revisions. The
        # read-start time never replaces the retained candle close timestamp.
        return dict(environment='mainnet', symbol=msg['symbol'],
            observed_at_ms=max(ingested_at_ms) if ingested_at_ms else now_ms, candles=candles)
