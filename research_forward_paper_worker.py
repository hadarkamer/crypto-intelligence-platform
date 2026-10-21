"""Isolated forward observation and paper books; no orders or notifications.

All database, HTTP, checkpoint and SQLite work belongs to one executor thread.
A paper transition is checkpointed before the next external observation starts.
"""
from __future__ import annotations

import asyncio
import copy
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re
import time
import uuid
from urllib.parse import urlencode
from urllib.request import Request, urlopen

STUDY_ID = "doge-eth-first-observed-paper-render-v1"
TRANSPORT_VERSION = "forward-paper-readonly-binance-v1"
QUOTE_URL = "https://data-api.binance.vision/api/v3/ticker/bookTicker"
PACKAGE = Path(__file__).resolve().parent / "forward_paper_runtime"


def _utc():
    return datetime.now(timezone.utc).isoformat()


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _connect(dsn):
    options = dict(connect_timeout=5, application_name="forward_paper_observer",
                   keepalives=1, keepalives_idle=5, keepalives_interval=2,
                   keepalives_count=2, tcp_user_timeout=10000)
    try:
        import psycopg2
        return psycopg2.connect(dsn, **options)
    except ImportError:
        import psycopg
        return psycopg.connect(dsn, **options)


def source_sql(policy, template, *, bootstrap=False, eligible_keys=()):
    """Full overlapping read; omit only already checkpointed eligible identities."""
    original = template.strip().rstrip(";")
    boundary = original.index(", matches AS MATERIALIZED")
    start = datetime.fromisoformat(policy["study_start_utc"].replace("Z", "+00:00"))
    since = (start - timedelta(days=7) if bootstrap else start).isoformat()
    def quote(value):
        return "'" + str(value).replace("'", "''") + "'"
    requested = []
    for scope in policy["scopes"]:
        values = [quote(scope["scope_key"]), quote(scope["candidate_key"]),
                  quote(scope["symbol"]), quote(scope["direction"]),
                  quote(since) + "::timestamptz", "statement_timestamp()"]
        requested.append("(" + ",".join(values) + ")")
    base = ("WITH requested(scope_key,candidate_key,symbol,direction,period_start_utc,source_cutoff_utc) AS (VALUES "
            + ",".join(requested) + ")" + original[boundary:])
    rows = ("SELECT observed.*,c.definition_sha256 AS candidate_definition_sha256,"
            "c.formula_version AS candidate_formula_version FROM (" + base + ") observed "
            "LEFT JOIN research_ordered_formula_candidates c ON c.candidate_key=observed.candidate_key")
    skip = ""
    if eligible_keys:
        skip = " WHERE (q.scope_key || ':' || q.event_id::text) NOT IN (" + ",".join(quote(k) for k in eligible_keys) + ")"
    return ("SELECT json_build_object('server_statement_utc',statement_timestamp(),'rows',"
            "COALESCE(json_agg(row_to_json(q) ORDER BY q.symbol,q.match_source_time_utc,q.event_id),'[]'::json)) "
            "AS envelope FROM (" + rows + ") q" + skip)


def fetch_candidates(dsn, policy, template, *, bootstrap=False, eligible_keys=(), connect=None):
    conn = (connect or _connect)(dsn)
    try:
        with conn.cursor() as cursor:
            cursor.execute("SET TRANSACTION READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '5s'")
            cursor.execute("SET LOCAL TIME ZONE 'UTC'")
            started = _utc()
            timer = time.monotonic()
            cursor.execute(source_sql(policy, template, bootstrap=bootstrap, eligible_keys=eligible_keys))
            value = cursor.fetchone()[0]
            received = _utc()
            elapsed = time.monotonic() - timer
        conn.rollback()  # End the short read snapshot; no production writes.
    finally:
        conn.close()
    envelope = json.loads(value) if isinstance(value, str) else value
    if not isinstance(envelope, dict) or not isinstance(envelope.get("rows"), list):
        raise ValueError("Invalid source envelope")
    return {"poll_id": str(uuid.uuid4()), "request_started_utc": started,
            "response_received_utc": received, "monotonic_elapsed_seconds": elapsed,
            "rows": envelope["rows"], "db_server_statement_utc": envelope.get("server_statement_utc"),
            "bootstrap": bootstrap, "source": "postgres_read_only",
            "clock_scope": "Independent observer response receipt; not bot decision or commit timestamp"}


def fetch_quotes(policy, *, open_url=None):
    symbols = policy["quote_symbols"]
    query = urlencode({"symbols": json.dumps(list(symbols.values()), separators=(",", ":"))})
    request = Request(QUOTE_URL + "?" + query,
                      headers={"User-Agent": "forward-paper-observer/1.0", "Accept": "application/json"})
    started = _utc()
    timer = time.monotonic()
    with (open_url or urlopen)(request, timeout=float(policy["quote_max_elapsed_seconds"])) as response:
        payload = response.read(128 * 1024 + 1)
        http_date = response.headers.get("Date")
    received = _utc()
    elapsed = time.monotonic() - timer
    if len(payload) > 128 * 1024:
        raise ValueError("Oversized quote response")
    parsed = json.loads(payload)
    if not isinstance(parsed, list):
        raise ValueError("Invalid quote envelope")
    inverse = {value: key for key, value in symbols.items()}
    quotes = []
    for row in parsed:
        if not isinstance(row, dict) or row.get("symbol") not in inverse:
            raise ValueError("Invalid quote symbol")
        quotes.append({"symbol": inverse[row["symbol"]], "bid": row["bidPrice"], "ask": row["askPrice"],
                       "bid_quantity": row.get("bidQty"), "ask_quantity": row.get("askQty")})
    if len(quotes) != len(symbols) or len({q["symbol"] for q in quotes}) != len(symbols):
        raise ValueError("Missing or duplicate quote symbol")
    return {"packet_id": str(uuid.uuid4()), "request_started_utc": started,
            "response_received_utc": received, "monotonic_elapsed_seconds": elapsed,
            "quotes": quotes, "source": policy["quote_source"], "endpoint": QUOTE_URL,
            "exchange_quote_timestamp": None,
            "http_date_header": http_date, "response_sha256": hashlib.sha256(payload).hexdigest(),
            "timestamp_limit": "Public endpoint has no quote event timestamp; request/receipt clocks only"}


def _aggregate(summary):
    books = {}
    for name, book in summary.get("books", {}).items():
        books[name] = {
            "active": bool(book.get("active")), "reserved": bool(book.get("reservation")),
            "choice_status_counts": dict(book.get("choice_status_counts", {})),
            "completed_all": copy.deepcopy(book.get("completed_all", {})),
            "completed_path_uncertain": copy.deepcopy(book.get("completed_path_uncertain", {})),
            "completed_without_uncertainty_flag": copy.deepcopy(book.get("completed_without_uncertainty_flag", {})),
        }
    return {"bootstrap_complete": bool(summary.get("bootstrap_complete")),
            "study_start_utc": summary.get("study_start_utc"), "policy_sha256": summary.get("policy_sha256"),
            "raw_first_seen_count": summary.get("raw_first_seen_count", 0),
            "first_eligible_seen_count": summary.get("first_eligible_seen_count", 0),
            "journal_events": summary.get("journal_events", 0), "books": books}


class ObservationRejected(RuntimeError):
    pass


class ForwardPaperWorker:
    def __init__(self, *, package=None, store_factory=None, engine_class=None,
                 candidate_fetch=None, quote_fetch=None, first_delay=5, retry_seconds=30):
        self.package = Path(package) if package is not None else PACKAGE
        self._store_factory = store_factory
        self._engine_class = engine_class
        self._candidate_fetch = candidate_fetch or fetch_candidates
        self._quote_fetch = quote_fetch or fetch_quotes
        self.first_delay = first_delay
        self.retry_seconds = retry_seconds
        self._executor = None
        self._task = None
        self._stop = None
        self._dsn = None
        self._store = None
        self._engine = None
        self._config = None
        self._template = None
        self._last_log = 0.0
        self._last_logged_state = None
        self._health = {"study_id": STUDY_ID, "enabled": False, "configured": False,
                        "state": "DISABLED", "leader": False, "runtime_commit": None,
                        "last_source_received_utc": None, "last_quote_received_utc": None,
                        "last_processing_completed_utc": None, "cycles": 0, "failures": 0,
                        "last_error_type": None}

    def status(self):
        value = copy.deepcopy(self._health)
        value["running"] = bool(self._task and not self._task.done())
        value["trade_execution"] = False
        value["notifications"] = False
        return value

    def _publish(self, **values):
        self._health = {**self._health, **values}

    def _log(self, *, force=False):
        tick = time.monotonic()
        if force or self._health["state"] != self._last_logged_state or tick - self._last_log >= 60:
            print("[FORWARD_PAPER] " + _json(self.status()), flush=True)
            self._last_log = tick
            self._last_logged_state = self._health["state"]

    async def start(self, *, database_url=None, enabled=False, db_path=None, runtime_commit=None):
        # db_path remains accepted for callers, but authoritative state is PostgreSQL.
        if self._task and not self._task.done():
            return True
        commit = str(runtime_commit or "")
        commit = commit if re.fullmatch(r"[0-9a-fA-F]{7,64}", commit) else None
        self._publish(enabled=bool(enabled), configured=bool(database_url), runtime_commit=commit)
        if not enabled or not database_url:
            self._publish(state="DISABLED")
            return False
        self._dsn = database_url
        self._stop = asyncio.Event()
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="forward-paper")
        self._publish(state="STARTING")
        self._task = asyncio.create_task(self._run(), name="forward-paper-observer")
        return True

    async def _wait(self, delay):
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=max(0.001, delay))
            return True
        except asyncio.TimeoutError:
            return False

    async def _run(self):
        if await self._wait(self.first_delay):
            return
        while not self._stop.is_set():
            began = time.monotonic()
            delay = self.retry_seconds
            try:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(self._executor, self._cycle_owned)
                delay = max(0.05, float((self._config or {}).get("poll_seconds", 10)) - (time.monotonic() - began))
            except asyncio.CancelledError:
                raise
            except Exception as error:
                # Never expose exception text: database errors can include a DSN.
                self._publish(state="RETRYING", leader=False,
                              last_error_type=type(error).__name__)
                self._log(force=True)
            if await self._wait(delay):
                break

    def _open_owned(self):
        self._template = (self.package / "source_query_template.sql").read_text(encoding="utf-8").replace("\r\n", "\n")
        config = json.loads((self.package / "policy_template.json").read_text(encoding="utf-8"))
        if "study_start_utc" in config:
            raise ValueError("Cloud study activation must be generated by persistent store")
        config["source_query_sha256"] = hashlib.sha256(self._template.encode()).hexdigest()
        core = Path(__file__).with_name("research_forward_paper_engine.py").read_text(encoding="utf-8").replace("\r\n", "\n")
        config["engine_source_sha256"] = hashlib.sha256(core.encode()).hexdigest()
        config["quote_endpoint"] = QUOTE_URL
        config["transport_version"] = TRANSPORT_VERSION
        self._config = config
        factory = self._store_factory
        if factory is None:
            from research_forward_paper_store import PostgresCheckpoint
            factory = PostgresCheckpoint
        engine_class = self._engine_class
        if engine_class is None:
            from research_forward_paper_engine import PaperStudy
            engine_class = PaperStudy
        self._store = factory(self._dsn, STUDY_ID, config)
        if not self._store.acquire():
            self._discard_owned()
            self._publish(state="STANDBY", leader=False)
            self._log()
            return False
        self._engine = self._store.restore_engine(engine_class)
        self._publish(state="OBSERVING", leader=True, **_aggregate(self._engine.summary()))
        if hasattr(self._store, "journal_sequence"):
            self._publish(journal_events=self._store.journal_sequence,
                          durable_journal_seq=self._store.journal_sequence)
        return True

    def _apply_owned(self, packet, *, candidate):
        processing_started = _utc()
        tick = time.monotonic()
        result = (self._engine.observe_candidates(packet) if candidate else self._engine.observe_quotes(packet))
        completed = _utc()
        rejected = result.get("last_observation_rejected")
        proposed = {**self._health, **_aggregate(result),
                    "state": "OBSERVATION_REJECTED" if rejected else "OBSERVING",
                    "leader": True, "last_processing_completed_utc": completed,
                    "last_processing_started_utc": processing_started,
                    "last_processing_elapsed_seconds": time.monotonic() - tick,
                    "last_error_type": "ObservationRejected" if rejected else None}
        if not rejected:
            proposed["last_source_received_utc" if candidate else "last_quote_received_utc"] = packet["response_received_utc"]
        # Both the new journal records and projection commit before any next read.
        self._store.checkpoint(self._engine, proposed)
        if hasattr(self._store, "journal_sequence"):
            proposed.update(journal_events=self._store.journal_sequence,
                            durable_journal_seq=self._store.journal_sequence)
        self._health = proposed
        if rejected:
            raise ObservationRejected("Paper observation rejected")

    def _cycle_owned(self):
        try:
            if self._store is None and not self._open_owned():
                return
            policy = self._engine.policy
            bootstrap = not self._engine.summary()["bootstrap_complete"]
            packet = self._candidate_fetch(self._dsn, policy, self._template, bootstrap=bootstrap,
                                           eligible_keys=self._engine.eligible_event_keys())
            self._apply_owned(packet, candidate=True)
            quote = self._quote_fetch(policy)
            self._apply_owned(quote, candidate=False)
            proposed = {**self._health, "cycles": self._health["cycles"] + 1,
                        "last_cycle_completed_utc": _utc()}
            self._store.update_health(proposed)
            self._health = proposed
            self._log()
        except Exception as error:
            failed = {**self._health, "state": "RETRYING", "leader": False,
                      "failures": self._health["failures"] + 1,
                      "last_error_type": type(error).__name__, "last_failure_utc": _utc()}
            if self._store is not None:
                try:
                    self._store.update_health(failed)
                except Exception:
                    pass
            self._discard_owned()
            self._health = failed
            raise

    def _discard_owned(self):
        engine, store = self._engine, self._store
        self._engine = self._store = None
        for item in (engine, store):
            if item is not None:
                try:
                    item.close()
                except Exception:
                    pass

    def _shutdown_owned(self):
        if self._store is not None:
            try:
                self._store.update_health({**self._health, "state": "STOPPED", "leader": False,
                                           "last_stopped_utc": _utc()})
            except Exception:
                pass
        self._discard_owned()

    async def stop(self):
        if self._task is None:
            return
        self._stop.set()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        finally:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(self._executor, self._shutdown_owned)
            self._executor.shutdown(wait=True)
            self._executor = None
            self._task = None
            self._dsn = None
            self._publish(state="STOPPED", leader=False)


WORKER = ForwardPaperWorker()


async def start_worker(*, database_url=None, enabled=False, db_path=None, runtime_commit=None):
    return await WORKER.start(database_url=database_url, enabled=enabled,
                              db_path=db_path, runtime_commit=runtime_commit)


async def stop_worker():
    await WORKER.stop()


def status():
    return WORKER.status()

