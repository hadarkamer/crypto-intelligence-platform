"""Three-price input -> local paper broker + public Hyperliquid information.

No order endpoint, SDK, wallet, signing, production imports, or background work
on import. Explicit opt-in is required for public HTTP reads. This is a bounded
functional runner, not a complete exchange simulator or a production daemon.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from decimal import Decimal
import http.client
import json
import os
from pathlib import Path
import stat
import time
from typing import Callable

from paper_execution import PaperBroker, PaperError, SIGNAL_FIELDS, SYMBOL, ID, canonical, number, stamp, text

VERSION = "paper-feed-v1"
HOST = "api.hyperliquid.xyz"
MAX_RESPONSE = 2 * 1024 * 1024
MAX_INPUT = 1024 * 1024
MAX_LINES = 256
MAX_SAMPLE_SECONDS = 10


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _pairs(items):
    result = {}
    for key, value in items:
        if key in result:
            raise PaperError("Duplicate JSON field")
        result[key] = value
    return result


def decode(raw: str | bytes):
    def invalid(_):
        raise PaperError("Non-finite JSON number")
    try:
        return json.loads(raw, object_pairs_hook=_pairs, parse_constant=invalid)
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise PaperError("Invalid JSON") from exc


def signal_message(message: str | bytes | dict) -> dict:
    if isinstance(message, (str, bytes)):
        if len(message) > 4096:
            raise PaperError("Signal too large")
        message = decode(message)
    if (not isinstance(message, dict) or set(message) != SIGNAL_FIELDS
            or message.get("kind") != "SIGNAL"):
        raise PaperError("A complete three-price SIGNAL is required")
    # Never let external messages occupy the public-quote id namespace.
    if (not isinstance(message["event_id"], str) or not ID.fullmatch(message["event_id"])
            or message["event_id"].startswith("hlq.")):
        raise PaperError("Invalid signal id")
    if not isinstance(message["symbol"], str) or not SYMBOL.fullmatch(message["symbol"]):
        raise PaperError("Invalid symbol")
    if message["side"] not in ("LONG", "SHORT"):
        raise PaperError("Explicit LONG or SHORT is required")
    stamp(message["at"])
    if len(canonical(message).encode()) > 4096:
        raise PaperError("Signal too large")
    for key in ("entry", "stop", "take_profit"):
        number(message[key])
    entry, stop, target = (number(message[k]) for k in ("entry", "stop", "take_profit"))
    if not (stop < entry < target if message["side"] == "LONG" else target < entry < stop):
        raise PaperError("Three prices are inconsistent with direction")
    return dict(message)


class PublicInfo:
    """Only two fixed public-information request types; no arbitrary URL/body.

    HTTPSConnection validates TLS by default. It neither follows redirects nor
    inherits proxy credentials. Raw HTTP bodies and exceptions are not logged.
    """
    def __init__(self, *, allow_public_reads: bool = False):
        if allow_public_reads is not True:
            raise PaperError("Explicit allow_public_reads=True is required")
        self.calls = 0

    def read(self, kind: str, symbol: str | None = None):
        if kind == "metaAndAssetCtxs" and symbol is None:
            body = {"type": kind}
        elif kind == "l2Book" and isinstance(symbol, str) and SYMBOL.fullmatch(symbol):
            body = {"type": kind, "coin": symbol}
        else:
            raise PaperError("Only public metadata and order-book reads are permitted")
        connection = http.client.HTTPSConnection(HOST, timeout=4)
        self.calls += 1
        started = time.monotonic()
        try:
            connection.request("POST", "/info", canonical(body).encode(),
                               {"Content-Type": "application/json", "Accept": "application/json"})
            response = connection.getresponse()
            if response.status != 200:
                raise PaperError("Public information request was not successful")
            raw = response.read(MAX_RESPONSE + 1)
            if len(raw) > MAX_RESPONSE or time.monotonic() - started > MAX_SAMPLE_SECONDS:
                raise PaperError("Public information response exceeded a safety bound")
            return decode(raw)
        except (OSError, http.client.HTTPException) as exc:
            raise PaperError("Public information connection unavailable; no paper fill") from exc
        finally:
            connection.close()


def asset_contexts(response, symbols: tuple[str, ...]) -> dict:
    if (not isinstance(response, list) or len(response) != 2
            or not isinstance(response[0], dict) or not isinstance(response[1], list)):
        raise PaperError("Invalid metadata response")
    universe = response[0].get("universe")
    if not isinstance(universe, list) or len(universe) != len(response[1]):
        raise PaperError("Metadata/context alignment is invalid")
    found = {}
    for item, context in zip(universe, response[1]):
        if not isinstance(item, dict):
            raise PaperError("Invalid asset metadata")
        name = item.get("name")
        if name not in symbols:
            continue
        decimals = item.get("szDecimals")
        if (name in found or type(decimals) is not int or not 0 <= decimals <= 6
                or item.get("isDelisted", False) is not False or not isinstance(context, dict)):
            raise PaperError("Unsupported or delisted asset")
        mark = number(context.get("markPx"))
        found[name] = {"size_step": text(Decimal(1).scaleb(-decimals)),
                       "sz_decimals": decimals, "mark": text(mark)}
    if set(found) != set(symbols):
        raise PaperError("A configured symbol is unavailable; no symbol substitution")
    return found


def check_prices(message: dict, decimals: int) -> None:
    # Hyperliquid perp rule: integer prices always allowed; otherwise at most
    # five significant figures and at most 6-szDecimals decimal places.
    for key in ("entry", "stop", "take_profit"):
        value = number(message[key]).normalize()
        if value == value.to_integral_value():
            continue
        if len(value.as_tuple().digits) > 5 or value.as_tuple().exponent < -(6-decimals):
            raise PaperError("Unsupported price precision; prices are never rounded")


def book_quote(book, symbol: str, mark: str, now: datetime) -> dict:
    if not isinstance(book, dict) or book.get("coin") != symbol:
        raise PaperError("Wrong order-book symbol")
    milliseconds, levels = book.get("time"), book.get("levels")
    if type(milliseconds) is not int or not 0 < milliseconds < 4102444800000:
        raise PaperError("Invalid exchange timestamp")
    at = datetime.fromtimestamp(milliseconds / 1000, tz=timezone.utc)
    if not 0 <= (now-at).total_seconds() <= MAX_SAMPLE_SECONDS:
        raise PaperError("Stale or future exchange snapshot")
    if (not isinstance(levels, list) or len(levels) != 2
            or any(not isinstance(side, list) or not 1 <= len(side) <= 20 for side in levels)):
        raise PaperError("Empty or malformed order book")
    for index, side in enumerate(levels):
        previous = None
        for row in side:
            if not isinstance(row, dict):
                raise PaperError("Invalid order-book level")
            price = number(row.get("px"))
            number(row.get("sz"), zero_ok=True)
            if previous is not None and (price >= previous if index == 0 else price <= previous):
                raise PaperError("Unsorted or duplicate order-book levels")
            previous = price
    bid, ask = levels[0][0], levels[1][0]
    if number(bid["px"]) > number(ask["px"]):
        raise PaperError("Crossed order book")
    return {"kind": "QUOTE", "event_id": f"hlq.{symbol}.{milliseconds}", "symbol": symbol,
            "bid": text(number(bid["px"])), "ask": text(number(ask["px"])),
            "bid_size": text(number(bid["sz"], zero_ok=True)),
            "ask_size": text(number(ask["sz"], zero_ok=True)),
            "mark": mark, "at": at.isoformat()}


class PaperFeed:
    """Single-threaded adapter; .submit_signal is the explicit bot integration.

    The bot must supply real three-price fields, not a research score or a
    threshold label. External callers cannot supply quote data or receipt time.
    """
    def __init__(self, database: str | Path, *, symbols: tuple[str, ...],
                 accounts: tuple[str, ...], fee_bps: str,
                 allow_public_reads: bool = False, info=None,
                 clock: Callable[[], datetime] = utc_now):
        if (not symbols or len(symbols) > 8 or len(set(symbols)) != len(symbols)
                or any(not isinstance(s, str) or not SYMBOL.fullmatch(s) for s in symbols)):
            raise PaperError("Provide 1-8 unique exact perpetual symbols")
        self.info = info if info is not None else PublicInfo(allow_public_reads=allow_public_reads)
        self.clock, self.symbols = clock, symbols
        self.assets = asset_contexts(self.info.read("metaAndAssetCtxs"), symbols)
        self.broker = PaperBroker(database, accounts=accounts, fee_bps=fee_bps,
                                  size_steps={s: self.assets[s]["size_step"] for s in symbols})

    def close(self) -> None:
        self.broker.close()

    def submit_signal(self, message: str | bytes | dict) -> dict:
        event = signal_message(message)
        if event["symbol"] not in self.assets:
            raise PaperError("Signal symbol was not explicitly configured")
        check_prices(event, self.assets[event["symbol"]]["sz_decimals"])
        return self.broker.process(event, received_at=self.clock().isoformat())

    def poll_once(self) -> dict:
        active = sorted({t["symbol"] for t in self.broker.snapshot()["trades"] if t["status"] != "CLOSED"})
        results = []
        if not active:
            return {"mode": "paper", "results": [], "exchange_orders_sent": 0}
        try:
            # REST mark has no source timestamp. Bound this acquisition window;
            # do NOT pretend the mark and book are atomic or exactly synchronized.
            started, monotonic_start = self.clock(), time.monotonic()
            assets = asset_contexts(self.info.read("metaAndAssetCtxs"), tuple(active))
            for symbol in active:
                try:
                    if assets[symbol]["size_step"] != self.assets[symbol]["size_step"]:
                        raise PaperError("Asset precision changed; new reviewed run required")
                    book = self.info.read("l2Book", symbol)
                    now = self.clock()
                    if (not 0 <= (now-started).total_seconds() <= MAX_SAMPLE_SECONDS
                            or time.monotonic()-monotonic_start > MAX_SAMPLE_SECONDS):
                        raise PaperError("Market sample acquisition exceeded time bound")
                    event = book_quote(book, symbol, assets[symbol]["mark"], now)
                    previous = self.broker.snapshot()["last_quotes"].get(symbol)
                    if previous and stamp(event["at"]) <= stamp(previous):
                        results.append({"symbol": symbol, "status": "NO_NEW_BOOK"})
                        continue
                    result = self.broker.process(event, received_at=now.isoformat())
                    results.append({"symbol": symbol, **result})
                except PaperError as exc:
                    results.append({"symbol": symbol, "status": "WITHHELD", "reason": str(exc)})
        except PaperError as exc:
            results = [{"symbol": s, "status": "WITHHELD", "reason": str(exc)} for s in active]
        return {"mode": "paper", "exchange_orders_sent": 0, "results": results}

    def snapshot(self) -> dict:
        return {"adapter_version": VERSION, "public_read_attempts": self.info.calls,
                "exchange_orders_sent": 0, "mark_source_timestamp_available": False,
                "snapshot": self.broker.snapshot()}


def publish_signal(path: Path, message: dict) -> None:
    """Explicit bot-side hook, Linux/Render. Call ONLY with an actual price plan.

    Writes a private local inbox, never sends data to Hyperliquid. Publisher and
    runner must share the same local filesystem; no implicit cross-service path.
    """
    import fcntl
    event = signal_message(message)
    if not path.name.endswith(".paper.jsonl"):
        raise PaperError("Use a dedicated *.paper.jsonl inbox")
    raw = (canonical(event) + "\n").encode()
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        meta = os.fstat(fd)
        if (not stat.S_ISREG(meta.st_mode) or meta.st_size + len(raw) > MAX_INPUT
                or meta.st_mode & 0o077):
            raise PaperError("Inbox must be private, regular and within the size bound")
        if os.write(fd, raw) != len(raw):
            raise PaperError("Incomplete inbox write; stop and inspect locally")
        os.fsync(fd)
    finally:
        os.close(fd)


def read_inbox(path: Path) -> list[bytes]:
    """Local private JSONL only. An unfinished last line waits for next poll.

    Bounded MVP inbox, retained for replay. The source writes only SIGNAL lines.
    No HTTP listener, URL input, Telegram scraping, or production DB access.
    """
    if not path.name.endswith(".paper.jsonl"):
        raise PaperError("Use a dedicated *.paper.jsonl inbox")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        meta = os.fstat(fd)
        if not stat.S_ISREG(meta.st_mode) or meta.st_size > MAX_INPUT:
            raise PaperError("Inbox must be a bounded regular file")
        with os.fdopen(fd, "rb", closefd=False) as source:
            raw = source.read(MAX_INPUT+1)
        if len(raw) > MAX_INPUT:
            raise PaperError("Inbox exceeded size limit")
    finally:
        os.close(fd)
    lines = raw.splitlines(keepends=True)
    if len(lines) > MAX_LINES or any(len(line) > 4097 for line in lines):
        raise PaperError("Inbox exceeds message bounds")
    return [line for line in lines if line.endswith(b"\n") and line.strip()]


def intake_once(feed: PaperFeed, path: Path) -> dict:
    accepted = replayed = rejected = 0
    for raw in read_inbox(path):
        try:
            result = feed.submit_signal(raw)
            replayed += int(result["replayed"])
            accepted += int(not result["replayed"])
        except PaperError:
            # Never echo raw messages or credentials to terminal/host logs.
            rejected += 1
    return {"accepted": accepted, "replayed": replayed, "rejected": rejected}


def main() -> None:
    parser = argparse.ArgumentParser(description="Public prices -> LOCAL paper fills only")
    parser.add_argument("--allow-public-reads", action="store_true")
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--inbox", type=Path, required=True)
    parser.add_argument("--symbols", nargs="+", required=True)
    parser.add_argument("--accounts", nargs="+", default=["paper-a", "paper-b"])
    parser.add_argument("--fee-bps", required=True, help="Explicit estimate, not a verified account fee")
    parser.add_argument("--cycles", type=int, default=1)
    args = parser.parse_args()
    if not 1 <= args.cycles <= 120:
        parser.error("Use 1-120 cycles for this bounded functional run")
    os.umask(0o077)
    feed = None
    try:
        # Validate local inbox before making a single network request.
        read_inbox(args.inbox)
        feed = PaperFeed(args.database, symbols=tuple(args.symbols), accounts=tuple(args.accounts),
                         fee_bps=args.fee_bps, allow_public_reads=args.allow_public_reads)
        for index in range(args.cycles):
            try:
                intake = intake_once(feed, args.inbox)
            except (PaperError, OSError):
                intake = {"input_status": "UNAVAILABLE"}
            result = feed.poll_once()  # Continue existing paper trades if input fails.
            print(json.dumps({"cycle": index+1, "intake": intake, **result}))
            if index+1 < args.cycles:
                time.sleep(5)
    except (PaperError, OSError):
        print(json.dumps({"mode": "paper", "status": "BLOCKED", "exchange_orders_sent": 0}))
        raise SystemExit(2)
    finally:
        if feed is not None:
            feed.close()


if __name__ == "__main__":
    main()
