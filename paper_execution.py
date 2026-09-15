"""Offline, paper-only three-price executor. No network, signing, or live mode.

This is a functional simulator, NOT a backtester or a Hyperliquid fill emulator.
Only supplied chronological bid/ask/mark snapshots may create simulated fills.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_DOWN, localcontext
import hashlib
import json
from pathlib import Path
import re
import sqlite3
from typing import Any, Mapping

VERSION = "paper-execution-v1"
APP_ID = 0x50415052
D = Decimal
ID = re.compile(r"^[A-Za-z0-9_.:-]{1,100}$")
SYMBOL = re.compile(r"^[A-Z][A-Z0-9]{0,19}$")
SIGNAL_FIELDS = {"kind", "event_id", "symbol", "side", "entry", "stop", "take_profit", "at"}
QUOTE_FIELDS = {"kind", "event_id", "symbol", "bid", "ask", "bid_size", "ask_size", "mark", "at"}


class PaperError(ValueError):
    """Rejected input: no state change or external side effect."""


def number(value: Any, *, zero_ok: bool = False) -> Decimal:
    # Never accept binary floats or booleans for prices/amounts.
    if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
        raise PaperError("Numbers must be decimal strings or integers")
    if len(str(value)) > 80:
        raise PaperError("Number is too long")
    try:
        result = D(value)
    except InvalidOperation as exc:
        raise PaperError("Invalid decimal") from exc
    if not result.is_finite() or result < 0 or (result == 0 and not zero_ok):
        raise PaperError("Number must be finite and positive")
    if result > D("1e15") or (result != 0 and result < D("1e-15")):
        raise PaperError("Number outside supported paper range")
    if len(result.as_tuple().digits) > 28:
        raise PaperError("Excessive numeric precision")
    return result


def stamp(value: Any) -> datetime:
    if not isinstance(value, str) or len(value) > 40:
        raise PaperError("Timestamp must be an ISO string with timezone")
    try:
        moment = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise PaperError("Invalid timestamp") from exc
    if moment.utcoffset() is None:
        raise PaperError("Timezone is required")
    return moment.astimezone(timezone.utc)


def text(value: Decimal) -> str:
    return format(value, "f")


def canonical(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise PaperError("Input must contain JSON values only") from exc


def floor_step(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step


def order_view(trade: dict) -> list[dict]:
    """Three logical paper orders, never exchange wire payloads."""
    filled, size = D(trade["entry_filled"]), D(trade["quantity"])
    entry_status = ("QUEUED" if trade["status"] == "QUEUED" else
                    "CANCELED_REMAINDER" if trade["entry_canceled"] else
                    "FILLED" if filled == size else "PARTIAL" if filled else "WORKING")
    orders = [{"role": "ENTRY", "price": trade["entry"], "quantity": trade["quantity"],
               "status": entry_status, "reduce_only": False}]
    for role, key in (("STOP", "stop"), ("TAKE_PROFIT", "take_profit")):
        status = ("DORMANT" if not filled else
                  "CANCELED" if trade["exit_reason"] and trade["exit_reason"] != role else
                  "FILLED" if trade["status"] == "CLOSED" else
                  "TRIGGERED" if trade["exit_reason"] == role else "ARMED")
        orders.append({"role": role, "price": trade[key], "quantity": trade["entry_filled"],
                       "status": status, "reduce_only": True})
    return orders


class PaperBroker:
    """Local durable sandbox. It cannot send any exchange order.

    Account names are virtual slots, not wallets. One active signal per
    account/symbol prevents hiding same-symbol netting behind fake independence.
    Additional signals queue when slots are occupied; no signal is dropped.
    """

    def __init__(self, database: str | Path, *, accounts: tuple[str, ...],
                 size_steps: Mapping[str, str], fee_bps: str,
                 risk_usd: str = "20", mode: str = "paper") -> None:
        if mode != "paper":
            raise PaperError("Only paper mode exists; live/testnet are not implemented")
        path = Path(database)
        if not path.name.endswith(".paper.sqlite3") or path.is_symlink():
            raise PaperError("Use a dedicated non-symlink *.paper.sqlite3 file")
        if (not accounts or len(accounts) > 32 or len(set(accounts)) != len(accounts)
                or any(not isinstance(a, str) or not ID.fullmatch(a)
                       or not a.startswith("paper-") for a in accounts)):
            raise PaperError("Provide unique virtual paper-* account names")
        if not size_steps or any(not isinstance(s, str) or not SYMBOL.fullmatch(s) for s in size_steps):
            raise PaperError("Explicit symbol precision is required")
        steps = {s: text(number(v)) for s, v in size_steps.items()}
        fee = number(fee_bps, zero_ok=True)
        if fee > 1000:
            raise PaperError("Unsupported fee estimate")
        self.config = {"version": VERSION, "mode": "paper", "accounts": list(accounts),
                       "size_steps": steps, "risk_usd": text(number(risk_usd)),
                       "fee_bps": text(fee), "max_quote_age_seconds": 10,
                       "gap_warning_seconds": 30}
        self.db = sqlite3.connect(str(path), isolation_level=None, timeout=5)
        try:
            self.db.execute("PRAGMA synchronous=FULL")
            with self._transaction():
                app = self.db.execute("PRAGMA application_id").fetchone()[0]
                tables = {r[0] for r in self.db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                if tables and (app != APP_ID or tables != {"paper_state", "paper_receipts"}):
                    raise PaperError("Refusing to use a foreign database")
                if not tables:
                    if app not in (0, APP_ID):
                        raise PaperError("Foreign application id")
                    self.db.execute(f"PRAGMA application_id={APP_ID}")
                    self.db.execute("CREATE TABLE paper_state (id INTEGER PRIMARY KEY CHECK(id=1), body TEXT NOT NULL)")
                    self.db.execute("CREATE TABLE paper_receipts (id TEXT PRIMARY KEY, digest TEXT NOT NULL, result TEXT NOT NULL)")
                    state = {"config": self.config, "trades": [], "last_quotes": {}, "last_received_at": None}
                    self.db.execute("INSERT INTO paper_state VALUES (1,?)", (canonical(state),))
                if self._load()["config"] != self.config:
                    raise PaperError("Configuration changed: use a new paper database")
        except Exception:
            self.db.close()
            raise

    @contextmanager
    def _transaction(self):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def close(self) -> None:
        self.db.close()

    def _load(self) -> dict:
        return json.loads(self.db.execute("SELECT body FROM paper_state WHERE id=1").fetchone()[0])

    def snapshot(self) -> dict:
        state = self._load()
        for trade in state["trades"]:
            trade["orders"] = order_view(trade)
        return {"mode": "paper", "version": VERSION, "performance_validated": False,
                "network_calls": 0, **state}

    def process(self, event: Mapping[str, Any], *, received_at: str) -> dict:
        """Atomically process an alert or a quote; caller supplies the clock.

        Production integration must stamp receipt time itself, not trust a sender.
        This version has no public HTTP endpoint or automatic event ingestion.
        """
        if not isinstance(event, dict):
            raise PaperError("Expected an object")
        fields = SIGNAL_FIELDS if event.get("kind") == "SIGNAL" else QUOTE_FIELDS if event.get("kind") == "QUOTE" else None
        if fields is None or set(event) != fields:
            raise PaperError("Missing, unknown, or unsupported fields")
        eid = event.get("event_id")
        symbol = event.get("symbol")
        if not isinstance(eid, str) or not ID.fullmatch(eid):
            raise PaperError("Stable event_id is required")
        if not isinstance(symbol, str) or symbol not in self.config["size_steps"]:
            raise PaperError("Unknown symbol: explicit precision required")
        now, at = stamp(received_at), stamp(event["at"])
        if at > now:
            raise PaperError("Future event timestamp")
        encoded = canonical(event)
        if len(encoded) > 4096:
            raise PaperError("Input too large")
        digest = hashlib.sha256(encoded.encode()).hexdigest()
        with localcontext() as ctx:
            ctx.prec = 60
            with self._transaction():
                old = self.db.execute("SELECT digest,result FROM paper_receipts WHERE id=?", (eid,)).fetchone()
                if old:
                    if old[0] != digest:
                        raise PaperError("event_id reused with different content")
                    return {**json.loads(old[1]), "replayed": True}
                state = self._load()
                if state["last_received_at"] and now < stamp(state["last_received_at"]):
                    raise PaperError("Receipt clock moved backwards")
                state["last_received_at"] = now.isoformat()
                if event["kind"] == "SIGNAL":
                    trade = self._signal(event, now)
                    state["trades"].append(trade)
                    self._assign(state)
                    outcome = {"event_id": eid, "status": trade["status"], "account": trade["account"]}
                else:
                    outcome = self._quote(state, event, at, now)
                self.db.execute("UPDATE paper_state SET body=? WHERE id=1", (canonical(state),))
                outcome.update(mode="paper", replayed=False)
                self.db.execute("INSERT INTO paper_receipts VALUES (?,?,?)", (eid, digest, canonical(outcome)))
                return outcome

    def _signal(self, event: dict, now: datetime) -> dict:
        side = event["side"]
        if side not in ("LONG", "SHORT"):
            raise PaperError("side must be LONG or SHORT")
        entry, stop, target = (number(event[k]) for k in ("entry", "stop", "take_profit"))
        if not ((stop < entry < target) if side == "LONG" else (target < entry < stop)):
            raise PaperError("Three prices are inconsistent with direction")
        step = D(self.config["size_steps"][event["symbol"]])
        qty = floor_step(D(self.config["risk_usd"]) / abs(entry - stop), step)
        if qty <= 0 or qty > D("1e15"):
            raise PaperError("Calculated size outside supported paper range")
        return {"event_id": event["event_id"], "symbol": event["symbol"], "side": side,
                "source_at": stamp(event["at"]).isoformat(), "accepted_at": now.isoformat(),
                "account": None, "status": "QUEUED", "entry": text(entry), "stop": text(stop),
                "take_profit": text(target), "quantity": text(qty),
                "planned_price_loss_usd": text(qty * abs(entry-stop)),
                "entry_filled": "0", "entry_value": "0", "exit_filled": "0",
                "realized_gross_usd": "0", "estimated_fees_usd": "0",
                "exit_reason": None, "entry_canceled": False, "data_gap": False, "fills": []}

    def _assign(self, state: dict) -> None:
        occupied = {(t["account"], t["symbol"]) for t in state["trades"]
                    if t["status"] not in ("QUEUED", "CLOSED")}
        for trade in state["trades"]:
            if trade["status"] != "QUEUED":
                continue
            for account in self.config["accounts"]:
                key = (account, trade["symbol"])
                if key not in occupied:
                    trade.update(account=account, status="WAITING_ENTRY")
                    occupied.add(key)
                    break

    def _quote(self, state: dict, event: dict, at: datetime, now: datetime) -> dict:
        if (now-at).total_seconds() > self.config["max_quote_age_seconds"]:
            raise PaperError("Stale quote: no simulated execution")
        symbol = event["symbol"]
        bid, ask, mark = (number(event[k]) for k in ("bid", "ask", "mark"))
        if bid > ask:
            raise PaperError("Crossed quote")
        pools = {"bid": number(event["bid_size"], zero_ok=True),
                 "ask": number(event["ask_size"], zero_ok=True)}
        previous = state["last_quotes"].get(symbol)
        if previous and at <= stamp(previous):
            raise PaperError("Non-increasing quote time")
        gap = bool(previous and (at-stamp(previous)).total_seconds() > self.config["gap_warning_seconds"])
        changes = []
        self._assign(state)
        for trade in state["trades"]:
            if trade["symbol"] != symbol or trade["status"] in ("QUEUED", "CLOSED"):
                continue
            # Never fill an alert using a quote that predates its receipt.
            if at < stamp(trade["accepted_at"]):
                continue
            if gap or (previous is None and (at-stamp(trade["accepted_at"])).total_seconds() > self.config["gap_warning_seconds"]):
                trade["data_gap"] = True
            before = canonical(trade)
            self._advance(trade, bid, ask, mark, pools, at.isoformat())
            if canonical(trade) != before:
                changes.append(trade["event_id"])
        state["last_quotes"][symbol] = at.isoformat()
        # Released slots can be assigned now, but never receive a retrospective fill.
        self._assign(state)
        return {"event_id": event["event_id"], "status": "PROCESSED", "changed_signals": changes,
                "data_gap": gap}

    def _trigger(self, trade: dict, mark: Decimal) -> None:
        if trade["exit_reason"] or D(trade["entry_filled"]) <= D(trade["exit_filled"]):
            return
        long = trade["side"] == "LONG"
        stop_hit = mark <= D(trade["stop"]) if long else mark >= D(trade["stop"])
        tp_hit = mark >= D(trade["take_profit"]) if long else mark <= D(trade["take_profit"])
        if stop_hit or tp_hit:
            trade["exit_reason"] = "STOP" if stop_hit else "TAKE_PROFIT"
            trade["entry_canceled"] = D(trade["entry_filled"]) < D(trade["quantity"])

    def _advance(self, trade: dict, bid: Decimal, ask: Decimal, mark: Decimal,
                 pools: dict, at: str) -> None:
        long = trade["side"] == "LONG"
        step = D(self.config["size_steps"][trade["symbol"]])
        self._trigger(trade, mark)
        if not trade["exit_reason"]:
            key, price = ("ask", ask) if long else ("bid", bid)
            allowed = price <= D(trade["entry"]) if long else price >= D(trade["entry"])
            needed = D(trade["quantity"]) - D(trade["entry_filled"])
            qty = floor_step(min(needed, pools[key]), step) if allowed else D(0)
            if qty > 0:
                pools[key] -= qty
                self._fill(trade, "ENTRY", qty, price, at)
            self._trigger(trade, mark)
        if trade["exit_reason"]:
            key, price = ("bid", bid) if long else ("ask", ask)
            needed = D(trade["entry_filled"]) - D(trade["exit_filled"])
            qty = floor_step(min(needed, pools[key]), step)
            if qty > 0:
                pools[key] -= qty
                self._fill(trade, trade["exit_reason"], qty, price, at)
            trade["status"] = "CLOSED" if D(trade["entry_filled"]) == D(trade["exit_filled"]) else "EXITING"
        elif D(trade["entry_filled"]) > 0:
            trade["status"] = "OPEN"

    def _fill(self, trade: dict, role: str, qty: Decimal, price: Decimal, at: str) -> None:
        fee = qty * price * D(self.config["fee_bps"]) / D(10000)
        trade["estimated_fees_usd"] = text(D(trade["estimated_fees_usd"]) + fee)
        if role == "ENTRY":
            trade["entry_filled"] = text(D(trade["entry_filled"]) + qty)
            trade["entry_value"] = text(D(trade["entry_value"]) + qty * price)
        else:
            average = D(trade["entry_value"]) / D(trade["entry_filled"])
            pnl = (price-average) * qty * (1 if trade["side"] == "LONG" else -1)
            trade["exit_filled"] = text(D(trade["exit_filled"]) + qty)
            trade["realized_gross_usd"] = text(D(trade["realized_gross_usd"]) + pnl)
        trade["fills"].append({"role": role, "quantity": text(qty), "price": text(price),
                               "at": at, "estimated_fee_usd": text(fee), "simulated": True})


def demo(directory: Path) -> dict:
    """Synthetic functional test; DEMO is not a claimed listed trading symbol."""
    directory.mkdir(parents=True, exist_ok=False)
    broker = PaperBroker(directory / "demo.paper.sqlite3", accounts=("paper-a", "paper-b"),
                         size_steps={"DEMO": "0.01"}, fee_bps="5")
    try:
        events = [
            {"kind": "SIGNAL", "event_id": "demo-1", "symbol": "DEMO", "side": "LONG",
             "entry": "100", "stop": "98", "take_profit": "104", "at": "2026-01-01T00:00:00Z"},
            {"kind": "QUOTE", "event_id": "quote-1", "symbol": "DEMO", "bid": "99.9", "ask": "100",
             "bid_size": "100", "ask_size": "4", "mark": "100", "at": "2026-01-01T00:00:01Z"},
            {"kind": "QUOTE", "event_id": "quote-2", "symbol": "DEMO", "bid": "99.9", "ask": "100",
             "bid_size": "100", "ask_size": "6", "mark": "100", "at": "2026-01-01T00:00:02Z"},
            {"kind": "QUOTE", "event_id": "quote-3", "symbol": "DEMO", "bid": "104", "ask": "104.1",
             "bid_size": "100", "ask_size": "100", "mark": "104", "at": "2026-01-01T00:00:03Z"},
        ]
        results = [broker.process(e, received_at=e["at"]) for e in events]
        duplicate = broker.process(events[0], received_at=events[-1]["at"])
        report = {"warning": "SYNTHETIC FUNCTIONAL DEMO ONLY. NOT LIVE, TESTNET, OR A PROFITABILITY TEST.",
                  "events": events, "results": results, "duplicate_result": duplicate,
                  "snapshot": broker.snapshot()}
        (directory / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        return report
    finally:
        broker.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Offline paper demo only; never contacts an exchange")
    parser.add_argument("--demo-dir", type=Path, required=True, help="New empty output directory")
    args = parser.parse_args()
    report = demo(args.demo_dir)
    print(json.dumps({"mode": "paper", "synthetic": True, "network_calls": 0,
                      "report": str(args.demo_dir / "report.json"),
                      "status": report["snapshot"]["trades"][0]["status"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
