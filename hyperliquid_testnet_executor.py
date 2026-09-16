"""Bounded, opt-in Hyperliquid TESTNET adapter, separate from paper execution.

No mainnet setting, production import, daemon, bot hook or work on import.
A call submits ONE native entry+TP+SL group. Subsequent checks are read-only.
This is connection-test infrastructure, not an unattended trading service.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_DOWN, localcontext
import hashlib
import http.client
import importlib.metadata
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import time
from typing import Any
from hl_testnet_runtime.risk_policy import budget, assert_new_entry_budget

VERSION = "hl-testnet-adapter-v1"
SDK_VERSION = "0.24.0"
TESTNET_HOST = "api.hyperliquid-testnet.xyz"
SIGNAL_FIELDS = {"kind", "event_id", "symbol", "side", "entry", "stop", "take_profit", "at"}
ROLES = ("ENTRY", "TAKE_PROFIT", "STOP")
ADDRESS = re.compile(r"0x[0-9a-fA-F]{40}\Z")
IDENTITY = re.compile(r"[A-Za-z0-9_.:-]{1,100}\Z")
SYMBOL = re.compile(r"[A-Z][A-Z0-9]{0,19}\Z")
APP_ID = 0x48544E31
MAX_BYTES = 2 * 1024 * 1024


class TestnetError(ValueError):
    """Only fixed error codes: no credentials or payloads in exceptions."""


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def _json(value: Any) -> str:
    try:
        return json.dumps(value, separators=(",", ":"), allow_nan=False)
    except (ValueError, TypeError):
        raise TestnetError("INVALID_JSON") from None


def _decode(raw: bytes) -> Any:
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise TestnetError("DUPLICATE_JSON_FIELD")
            result[key] = value
        return result
    def invalid(_):
        raise TestnetError("NONFINITE_JSON")
    try:
        return json.loads(raw, object_pairs_hook=pairs, parse_constant=invalid)
    except (ValueError, UnicodeError, RecursionError):
        raise TestnetError("INVALID_JSON_RESPONSE") from None


def _num(value: Any, *, zero: bool = False, signed: bool = False) -> Decimal:
    if not isinstance(value, str) or not 1 <= len(value) <= 80:
        raise TestnetError("DECIMAL_STRING_REQUIRED")
    try:
        result = Decimal(value)
    except InvalidOperation:
        raise TestnetError("INVALID_NUMBER") from None
    if (not result.is_finite() or len(result.as_tuple().digits) > 28
            or abs(result) > Decimal("1e15")
            or (result != 0 and abs(result) < Decimal("1e-15"))
            or (not signed and result < 0) or (not zero and result == 0)):
        raise TestnetError("NUMBER_OUT_OF_RANGE")
    return result


def _wire(value: Decimal) -> str:
    # SDK signing requires removing insignificant trailing zeroes, NOT rounding.
    return format(value.normalize(), "f")


def _account(value: str) -> str:
    if not isinstance(value, str) or not ADDRESS.fullmatch(value) or int(value[2:], 16) == 0:
        raise TestnetError("INVALID_ACCOUNT_ADDRESS")
    return value.lower()


def _signal(message: dict) -> dict:
    if not isinstance(message, dict) or set(message) != SIGNAL_FIELDS or message.get("kind") != "SIGNAL":
        raise TestnetError("COMPLETE_SIGNAL_REQUIRED")
    if not isinstance(message["event_id"], str) or not IDENTITY.fullmatch(message["event_id"]):
        raise TestnetError("INVALID_SIGNAL_ID")
    if not isinstance(message["symbol"], str) or not SYMBOL.fullmatch(message["symbol"]):
        raise TestnetError("EXACT_NATIVE_PERP_SYMBOL_REQUIRED")
    if message["side"] not in ("LONG", "SHORT"):
        raise TestnetError("INVALID_DIRECTION")
    entry, stop, take = (_num(message[k]) for k in ("entry", "stop", "take_profit"))
    if not (stop < entry < take if message["side"] == "LONG" else take < entry < stop):
        raise TestnetError("INCONSISTENT_THREE_PRICES")
    try:
        if not isinstance(message["at"], str) or len(message["at"]) > 40:
            raise ValueError()
        at = datetime.fromisoformat(message["at"].replace("Z", "+00:00"))
        if at.utcoffset() is None:
            raise ValueError()
    except ValueError:
        raise TestnetError("TIMEZONE_REQUIRED") from None
    return dict(message)


def build_action(message: dict, metadata: dict, account: str, *, exit_type: str,
                 historical_risk_usd: str | None = None) -> dict:
    """Pure conversion. Default budget is $10, before costs, NOT a loss cap.

    historical_risk_usd reconstructs a frozen old $20 action for verification
    only. Both signing and transport forbid a new entry above the CURRENT $10.
    Existing IDs remain stable; changing risk cannot bypass replay protection.
    $5000 maximum notional is a laboratory bound, not the trading strategy.
    'market': supplied exit prices remain trigger prices; execution may differ.
    'limit': trigger and limit use the supplied price; exit may remain unfilled.
    'tp_limit_sl_market': owner-approved TP limit and SL market, both pre-set.
    """
    message, account = _signal(message), _account(account)
    planned_risk = budget(historical_risk_usd)
    if exit_type not in ("market", "limit", "tp_limit_sl_market"):
        raise TestnetError("EXPLICIT_EXIT_TYPE_REQUIRED")
    universe = metadata.get("universe") if isinstance(metadata, dict) else None
    if not isinstance(universe, list) or not universe or len(universe) > 10000:
        raise TestnetError("INVALID_TESTNET_METADATA")
    matches = [(i, item) for i, item in enumerate(universe)
               if isinstance(item, dict) and item.get("name") == message["symbol"]]
    if len(matches) != 1:
        raise TestnetError("SYMBOL_NOT_AVAILABLE_ON_TESTNET")
    asset, item = matches[0]
    decimals = item.get("szDecimals")
    if (type(decimals) is not int or not 0 <= decimals <= 6
            or item.get("isDelisted", False) is not False):
        raise TestnetError("INVALID_OR_DELISTED_ASSET")
    entry, stop, take = (_num(message[k]) for k in ("entry", "stop", "take_profit"))
    for price in (entry, stop, take):
        value = price.normalize()
        if value != value.to_integral_value() and (len(value.as_tuple().digits) > 5
                or value.as_tuple().exponent < -(6 - decimals)):
            raise TestnetError("PRICE_PRECISION_REJECTED_NO_ROUNDING")
    with localcontext() as ctx:
        ctx.prec = 50
        step = Decimal(1).scaleb(-decimals)
        size = ((planned_risk / abs(entry-stop)) / step).to_integral_value(rounding=ROUND_DOWN) * step
        if size <= 0 or not Decimal(10) <= size * entry <= Decimal(5000):
            raise TestnetError("OUTSIDE_LAB_SIZE_BOUNDS")
    orders = []
    for role, price in zip(ROLES, (entry, take, stop)):
        cloid = "0x" + hashlib.sha256(_json([VERSION, account, message["event_id"], role]).encode()).hexdigest()[:32]
        typ = ({"limit": {"tif": "Gtc"}} if role == "ENTRY" else
               {"trigger": {"isMarket": (exit_type == "market" or
                                         (exit_type == "tp_limit_sl_market" and role == "STOP")),
                            "triggerPx": _wire(price),
                            "tpsl": "tp" if role == "TAKE_PROFIT" else "sl"}})
        orders.append({"a": asset, "b": (message["side"] == "LONG") == (role == "ENTRY"),
                       "p": _wire(price), "s": _wire(size), "r": role != "ENTRY", "t": typ, "c": cloid})
    return {"type": "order", "orders": orders, "grouping": "normalTpsl"}


class TestnetHTTP:
    """Fixed TLS host, no redirects/proxies/custom URL; never retries a POST."""
    def __init__(self, *, allow_orders: bool = False):
        self.allow_orders = allow_orders is True
        self.public_calls = 0
        self.order_attempts = 0

    def _post(self, path: str, body: dict) -> Any:
        if TESTNET_HOST != "api.hyperliquid-testnet.xyz":
            raise TestnetError("TESTNET_HOST_CHANGED")
        if path not in ("/info", "/exchange"):
            raise TestnetError("ENDPOINT_NOT_ALLOWED")
        if path == "/exchange":
            if not self.allow_orders or body.get("action", {}).get("type") != "order":
                raise TestnetError("ORDER_TRANSPORT_DISABLED")
            assert_new_entry_budget(body['action'])
            self.order_attempts += 1
        else:
            self.public_calls += 1
        connection = http.client.HTTPSConnection("api.hyperliquid-testnet.xyz", timeout=4)
        started = time.monotonic()
        try:
            raw = _json(body).encode()
            if len(raw) > 16384:
                raise TestnetError("REQUEST_TOO_LARGE")
            connection.request("POST", path, raw, {"Content-Type": "application/json"})
            response = connection.getresponse()
            if response.status != 200:
                raise TestnetError("HTTP_RESPONSE_REQUIRES_REVIEW")
            raw = response.read(MAX_BYTES+1)
            if len(raw) > MAX_BYTES or time.monotonic()-started > 10:
                raise TestnetError("RESPONSE_BOUND_EXCEEDED")
            return _decode(raw)
        except (OSError, http.client.HTTPException):
            raise TestnetError("TRANSPORT_UNCERTAIN_DO_NOT_RESEND") from None
        finally:
            connection.close()

    def info(self, kind: str, *, user: str | None = None, oid: str | None = None) -> Any:
        if kind == "meta" and user is None and oid is None:
            body = {"type": kind}
        elif kind in ("userRole", "frontendOpenOrders", "clearinghouseState") and oid is None:
            body = {"type": kind, "user": _account(user)}
        elif kind == "orderStatus" and isinstance(oid, str) and re.fullmatch(r"0x[0-9a-f]{32}", oid):
            body = {"type": kind, "user": _account(user), "oid": oid}
        else:
            raise TestnetError("INFO_TYPE_NOT_ALLOWED")
        return self._post("/info", body)


def _wallet():
    # No default-mainnet SDK Exchange/Info objects; only the signing helper.
    key = os.environ.get("HL_TESTNET_AGENT_KEY", "")
    if not re.fullmatch(r"(?:0x)?[0-9a-fA-F]{64}", key):
        raise TestnetError("TESTNET_AGENT_KEY_REQUIRED")
    try:
        if importlib.metadata.version("hyperliquid-python-sdk") != SDK_VERSION:
            raise ValueError()
        from eth_account import Account
        return Account.from_key(key)
    except Exception:
        raise TestnetError("PINNED_TESTNET_SDK_OR_KEY_INVALID") from None


def _signed_body(wallet, action: dict, nonce: int) -> dict:
    assert_new_entry_budget(action)
    from hyperliquid.utils.signing import sign_l1_action
    expires = nonce + 30000
    # False is deliberate and mandatory: signature is valid for Testnet only.
    signature = sign_l1_action(wallet, action, None, nonce, expires, False)
    return {"action": action, "nonce": nonce, "signature": signature, "expiresAfter": expires}


class AttemptLog:
    """Durable intent-before-send. Uncertainty permanently blocks blind replay.

    The bounded connection test reserves one batch per account in this log.
    It intentionally does not recycle accounts or operate as a routing daemon.
    """
    def __init__(self, path: str | Path):
        path = Path(path)
        parent = path.parent
        if (not path.is_absolute() or not path.name.endswith(".hl-testnet.sqlite3")
                or path.is_symlink() or parent.resolve(strict=True) != parent
                or parent.stat().st_mode & 0o077 or parent.stat().st_uid != os.geteuid()
                or path.is_relative_to(Path(__file__).resolve().parent)):
            raise TestnetError("PRIVATE_TESTNET_LOG_REQUIRED")
        fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
        try:
            info = os.fstat(fd)
            if (not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077
                    or info.st_uid != os.geteuid() or info.st_nlink != 1):
                raise TestnetError("UNSAFE_TESTNET_LOG")
        finally:
            os.close(fd)
        self.db = sqlite3.connect(str(path), isolation_level=None, timeout=4)
        try:
            self.db.execute("PRAGMA synchronous=FULL")
            self.db.execute("BEGIN IMMEDIATE")
            tables = {row[0] for row in self.db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            app_id = self.db.execute("PRAGMA application_id").fetchone()[0]
            if (tables and (tables != {"testnet_attempts"} or app_id != APP_ID)) or (not tables and app_id not in (0, APP_ID)):
                raise TestnetError("FOREIGN_DATABASE")
            self.db.execute(f"PRAGMA application_id={APP_ID}")
            self.db.execute("CREATE TABLE IF NOT EXISTS testnet_attempts (id TEXT PRIMARY KEY, account TEXT UNIQUE NOT NULL, digest TEXT NOT NULL, body TEXT NOT NULL, nonce INTEGER NOT NULL, result TEXT NOT NULL)")
            self.db.execute("COMMIT")
        except BaseException:
            self.db.close()
            raise

    def close(self):
        self.db.close()

    def reserve(self, signal: dict, account: str, action: dict) -> tuple[bool, dict, int]:
        identity = signal["event_id"]
        body = {"signal": signal, "account": account, "action": action}
        digest = hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()
        self.db.execute("BEGIN IMMEDIATE")
        try:
            row = self.db.execute("SELECT digest,result,nonce FROM testnet_attempts WHERE id=?", (identity,)).fetchone()
            if row:
                if row[0] != digest:
                    raise TestnetError("EXISTING_ID_CHANGED_NO_RESEND")
                self.db.execute("COMMIT")
                return False, json.loads(row[1]), row[2]
            if self.db.execute("SELECT 1 FROM testnet_attempts WHERE account=?", (account,)).fetchone():
                raise TestnetError("TEST_ACCOUNT_ALREADY_RESERVED")
            if self.db.execute("SELECT count(*) FROM testnet_attempts").fetchone()[0] >= 32:
                raise TestnetError("LAB_LOG_FULL")
            previous = self.db.execute("SELECT COALESCE(MAX(nonce),0) FROM testnet_attempts").fetchone()[0]
            nonce = max(now_ms(), previous+1)
            result = {"status": "RESERVED_OR_UNCERTAIN", "verified": False}
            self.db.execute("INSERT INTO testnet_attempts VALUES(?,?,?,?,?,?)",
                            (identity, account, digest, _json(body), nonce, _json(result)))
            self.db.execute("COMMIT")
            return True, result, nonce
        except BaseException:
            if self.db.in_transaction:
                self.db.execute("ROLLBACK")
            raise

    def get(self, identity: str) -> dict | None:
        row = self.db.execute("SELECT body,result FROM testnet_attempts WHERE id=?", (identity,)).fetchone()
        return {**json.loads(row[0]), "result": json.loads(row[1])} if row else None

    def save(self, identity: str, result: dict):
        self.db.execute("UPDATE testnet_attempts SET result=? WHERE id=?", (_json(result), identity))


def acknowledgement(response: Any) -> str:
    """HTTP 200 or status=ok is NOT proof that the three orders were accepted."""
    try:
        if response["status"] != "ok" or response["response"]["type"] != "order":
            return "RESPONSE_REQUIRES_REVIEW"
        statuses = response["response"]["data"]["statuses"]
        if not isinstance(statuses, list) or len(statuses) != 3:
            return "RESPONSE_REQUIRES_REVIEW"
        for status in statuses:
            if status == "waitingForFill":
                continue
            if not isinstance(status, dict) or len(status) != 1:
                return "RESPONSE_REQUIRES_REVIEW"
            name = next(iter(status))
            if name not in ("resting", "filled") or not isinstance(status[name], dict):
                return "RESPONSE_REQUIRES_REVIEW"
            oid = status[name].get("oid")
            if type(oid) is not int or oid <= 0:
                raise ValueError()
        return "ACKNOWLEDGED_NOT_VERIFIED"
    except (KeyError, TypeError, ValueError):
        return "RESPONSE_REQUIRES_REVIEW"


def _position(state: Any, symbol: str) -> Decimal:
    if not isinstance(state, dict) or not isinstance(state.get("assetPositions"), list):
        raise TestnetError("INVALID_ACCOUNT_STATE")
    result, found = Decimal(0), False
    for row in state["assetPositions"]:
        if not isinstance(row, dict) or not isinstance(row.get("position"), dict):
            raise TestnetError("INVALID_ACCOUNT_STATE")
        position = row["position"]
        if position.get("coin") == symbol:
            if found:
                raise TestnetError("DUPLICATE_POSITION")
            found = True
            result = _num(position.get("szi"), zero=True, signed=True)
    return result


def verify_orders(http: TestnetHTTP, record: dict) -> dict:
    """Read each cloid and compare values. Pending children are not active protection.

    Unknown/partial/error states request inspection; never resubmit or cancel.
    A one-shot observation cannot promise future execution or ongoing protection.
    """
    action, account, signal = record["action"], record["account"], record["signal"]
    started = time.monotonic()
    findings, remaining = [], []
    for role, expected in zip(ROLES, action["orders"]):
        response = http.info("orderStatus", user=account, oid=expected["c"])
        matched, state, left = False, "UNVERIFIED", None
        try:
            wrapper, actual = response["order"], response["order"]["order"]
            state = wrapper["status"]
            if not isinstance(state, str) or len(state) > 60:
                state = "UNVERIFIED"
            matched = (response["status"] == "order" and actual["cloid"] == expected["c"]
                       and actual["coin"] == signal["symbol"]
                       and actual["side"] == ("B" if expected["b"] else "A")
                       and actual["reduceOnly"] is expected["r"]
                       and _num(actual["origSz"]) == _num(expected["s"])
                       and _num(actual["limitPx"]) == _num(expected["p"]))
            left = _num(actual["sz"], zero=True)
            if not Decimal(0) <= left <= _num(expected["s"]):
                matched = False
            if role != "ENTRY":
                trigger = expected["t"]["trigger"]
                label = ("Take Profit" if role == "TAKE_PROFIT" else "Stop") + (" Market" if trigger["isMarket"] else " Limit")
                matched = (matched and actual["isTrigger"] is True
                           and _num(actual["triggerPx"]) == _num(trigger["triggerPx"])
                           and actual["orderType"] == label)
            else:
                matched = matched and actual["isTrigger"] is False and actual["orderType"] == "Limit"
        except (KeyError, TypeError, TestnetError):
            matched = False
        findings.append({"role": role, "state": state, "fields_match": matched})
        remaining.append(left)
    position = _position(http.info("clearinghouseState", user=account), signal["symbol"])
    if time.monotonic()-started > 10:
        raise TestnetError("VERIFICATION_SAMPLE_EXPIRED")
    result = {"status": "VERIFICATION_REQUIRES_REVIEW", "verified": False,
              "protection_active": False, "orders": findings}
    if not all(item["fields_match"] for item in findings):
        return result
    states = [item["state"] for item in findings]
    size = _num(action["orders"][0]["s"])
    expected_position = size if signal["side"] == "LONG" else -size
    if states == ["open", "open", "open"] and position == 0 and remaining[0] == size:
        result.update(status="VERIFIED_WAITING_ENTRY", verified=True)
    elif states == ["filled", "open", "open"] and position == expected_position and remaining[0] == 0 and remaining[1:] == [size, size]:
        result.update(status="VERIFIED_OPEN_PROTECTED", verified=True, protection_active=True)
    elif states[0] == "open" and position != 0:
        result.update(status="PARTIAL_ENTRY_REQUIRES_REVIEW")
    elif states[0] == "filled" and position == 0 and sorted(states[1:]) == ["filled", "siblingFilledCanceled"]:
        result.update(status="VERIFIED_CLOSED", verified=True)
    return result


def submit_once(message: dict, *, account: str, journal: str | Path,
                exit_type: str, enable_testnet: bool = False) -> dict:
    """Explicit one-shot TESTNET action. No automatic connection to the live bot."""
    if enable_testnet is not True:
        return {"mode": "testnet", "status": "DISABLED", "order_batches_sent": 0}
    signal, account = _signal(message), _account(account)
    # Technical test freshness guard, not a new strategy rule.
    at = datetime.fromisoformat(signal["at"].replace("Z", "+00:00"))
    if not 0 <= (datetime.now(timezone.utc)-at).total_seconds() <= 60:
        raise TestnetError("FRESH_TEST_INSTRUCTION_REQUIRED")
    wallet = _wallet()
    if _account(wallet.address) == account:
        raise TestnetError("USE_DEDICATED_AGENT_NOT_MASTER_KEY")
    http = TestnetHTTP(allow_orders=True)
    started = time.monotonic()
    # Confirm signer really maps to this user on TESTNET, not to another account.
    role = http.info("userRole", user=wallet.address)
    if (not isinstance(role, dict) or role.get("role") != "agent"
            or not isinstance(role.get("data"), dict)
            or not isinstance(role["data"].get("user"), str)
            or role["data"]["user"].lower() != account):
        raise TestnetError("AGENT_NOT_AUTHORIZED_FOR_TEST_ACCOUNT")
    if http.info("userRole", user=account) != {"role": "user"}:
        raise TestnetError("INDEPENDENT_TEST_ACCOUNT_REQUIRED")
    action = build_action(signal, http.info("meta"), account, exit_type=exit_type)
    log = AttemptLog(journal)
    try:
        existing = log.get(signal["event_id"])
        if existing:
            # Compare via reserve, but do not query/change working positions.
            _, previous, _ = log.reserve(signal, account, action)
            return {"mode": "testnet", **previous, "replayed": True, "order_batches_sent": 0}
        orders = http.info("frontendOpenOrders", user=account)
        if not isinstance(orders, list) or orders:
            raise TestnetError("EMPTY_DEDICATED_TEST_ACCOUNT_REQUIRED")
        state = http.info("clearinghouseState", user=account)
        _position(state, signal["symbol"])
        for item in state["assetPositions"]:
            if _num(item["position"].get("szi"), zero=True, signed=True) != 0:
                raise TestnetError("EMPTY_DEDICATED_TEST_ACCOUNT_REQUIRED")
        if time.monotonic()-started > 10:
            raise TestnetError("PREFLIGHT_EXPIRED")
        fresh, previous, nonce = log.reserve(signal, account, action)
        if not fresh:
            return {"mode": "testnet", **previous, "replayed": True, "order_batches_sent": 0}
        result = {"status": "RESERVED_OR_UNCERTAIN", "verified": False}
        try:
            body = _signed_body(wallet, action, nonce)
            if now_ms() > nonce + 10000:
                raise TestnetError("SIGNING_WINDOW_EXPIRED")
            response = http._post("/exchange", body)
            ack = acknowledgement(response)
            result = {"status": ack, "verified": False}
            if ack == "ACKNOWLEDGED_NOT_VERIFIED":
                result = verify_orders(http, {"action": action, "account": account, "signal": signal})
        except Exception:
            # It may have been accepted: never retry the submission automatically.
            result = {"status": "UNCERTAIN_REQUIRES_REVIEW", "verified": False}
        log.save(signal["event_id"], result)
        return {"mode": "testnet", **result, "replayed": False, "order_batches_sent": http.order_attempts}
    finally:
        log.close()


def inspect_once(identity: str, *, journal: str | Path) -> dict:
    """Read-only reconciliation after timeout or restart. No key, no signature."""
    log = AttemptLog(journal)
    try:
        record = log.get(identity)
        if record is None:
            raise TestnetError("UNKNOWN_LOCAL_INSTRUCTION")
        http = TestnetHTTP()
        try:
            result = verify_orders(http, record)
        except Exception:
            result = {"status": "VERIFICATION_UNAVAILABLE", "verified": False}
        log.save(identity, result)
        return {"mode": "testnet", **result, "order_batches_sent": 0}
    finally:
        log.close()
