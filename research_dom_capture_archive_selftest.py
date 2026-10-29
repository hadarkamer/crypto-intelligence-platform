"""Diagnostic archive attachment tests; optional explicit-test-DSN JSONB read."""
from copy import deepcopy
from datetime import timedelta
import json
import os
from types import SimpleNamespace
from unittest.mock import patch

import research_dom_capture as capture
import research_max_pain_archive as archive
import research_max_pain_archive_selftest as fixtures


MISSING = object()
EXISTING_METADATA = {"operational_scores": {"status": "COMPLETE", "coins": {"BTC": {"score": 65}}}, "existing": "preserved"}


def _number(value):
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _capture(*, truncated=False):
    frames = {}
    for timeframe in archive.REQUIRED_TIMEFRAMES:
        cells = [
            {"text": "1", "primary": "1", "amount": ""},
            {"text": "BTC", "primary": "BTC", "amount": ""},
            {"text": "מחיר ₪ 100", "primary": "100", "amount": ""},
            {"text": "95 / 100", "primary": "95", "amount": "100"},
            {"text": "110 / 200", "primary": "110", "amount": "200"},
        ]
        if truncated and timeframe == "12h":
            cells[2]["text"] = "x" * (capture.MAX_TEXT_BYTES + 1)
        table = {"headers": list(capture.REQUIRED_HEADERS), "rows": [cells]}
        parsed_rows = [{**row, "rank": 1} for row in fixtures._raw_rows() if row["timeframe"] == timeframe]
        frames[timeframe] = {"dom_capture": capture.capture_poll(
            [table], timeframe, parsed_rows,
            metadata={"attempt": 1, "poll": 2, "active_label": timeframe,
                      "table_read_completed_at_utc": fixtures.BASE.isoformat()},
            parse_number=_number, is_symbol=lambda value: value == "BTC",
        )}
    result = capture.aggregate(archive.REQUIRED_TIMEFRAMES, frames)
    capture.validate_capture(result)
    return result


def _payload(diagnostic=MISSING, **kwargs):
    snapshot = {"ok": True, "rows": fixtures._raw_rows(), "row_count": 7,
                "missing_timeframes": [], "duplicate_pairs": []}
    if diagnostic is not MISSING:
        snapshot["dom_capture"] = diagnostic
    return archive.build_snapshot_payload(
        cycle_id="selftest:dom-archive", cycle_time_utc=fixtures.BASE,
        collection_started_at_utc=fixtures.BASE,
        collection_completed_at_utc=fixtures.BASE + timedelta(minutes=5),
        source="WATCH_SHARED", collector_version="selftest-dom-archive-v1",
        snapshot=snapshot, enriched_rows=fixtures._enriched_rows(),
        capture_metadata=kwargs.get("metadata", EXISTING_METADATA),
    )


def _attachment(payload):
    return payload["set"]["source_metadata"]["capture_metadata"]["dom_row_capture"]


def _without_diagnostics(payload):
    value = deepcopy(payload)
    value["set"].pop("payload_sha256")
    value["set"]["source_metadata"]["capture_metadata"].pop("dom_row_capture")
    return value


def _assert_hash(attachment):
    material = {key: value for key, value in attachment.items() if key != "archive_payload_sha256"}
    assert attachment["archive_payload_sha256"] == archive._sha256(material)


def _assert_jsonb_survival(payload):
    decoded = json.loads(json.dumps(payload, ensure_ascii=False, allow_nan=False))
    assert decoded == payload
    attachment = _attachment(decoded)
    _assert_hash(attachment)
    capture.validate_capture(attachment["capture"])
    frame = attachment["capture"]["payload"]["timeframes"]["12h"]
    assert frame["payload"]["selected_table"]["rows"][0][2]["text"] == "מחיר ₪ 100"
    assert frame["payload"]["row_decisions"][0]["status"] == "ACCEPTED"
    assert attachment["cycle_binding"]["cycle_id"] == payload["set"]["cycle_id"]
    assert attachment["cycle_binding"]["cycle_time_utc"] == payload["set"]["cycle_time_utc"]


def _mock_persist(payload):
    saved = {}
    class Result:
        def __init__(self, row): self.row = row
        def fetchone(self): return self.row
    class Connection:
        def __enter__(self): return self
        def __exit__(self, *args): return False
        def execute(self, sql, parameters=None):
            if sql.startswith("SELECT to_regclass"):
                return Result({"sets": "sets", "symbols": "symbols", "rows": "rows"})
            assert sql.startswith("INSERT INTO research_max_pain_snapshot_sets")
            saved["set_parameters"] = parameters
            return Result({"snapshot_set_id": 101})
        def cursor(self): return self
        def executemany(self, sql, values):
            assert sql.startswith("INSERT INTO research_max_pain_snapshot_")
            saved["inserted_rows"] = saved.get("inserted_rows", 0) + len(values)
        def commit(self): saved["committed"] = True
    with patch.object(archive, "psycopg", SimpleNamespace(connect=lambda *args, **kwargs: Connection())), patch.dict(os.environ, {"MAX_PAIN_ARCHIVE_ENABLED": "1"}):
        result = archive.persist_snapshot_payload(payload, database_url="selftest://no-network")
    assert result["persisted"] is True and saved["committed"] is True
    stored = saved["set_parameters"][archive._SET_COLUMNS.index("source_metadata")]
    stored = getattr(stored, "obj", stored)
    assert stored == payload["set"]["source_metadata"]
    assert saved["inserted_rows"] == len(payload["symbols"]) + len(payload["rows"])


def _postgres_roundtrip(payload):
    # Never consult DATABASE_URL: only a deliberately supplied test connection.
    url = os.getenv("TEST_DATABASE_URL") or os.getenv("RESEARCH_TEST_POSTGRES_URL")
    if not url:
        print("DOM capture archive PostgreSQL JSONB roundtrip: SKIP (no explicit test DSN)")
        return
    assert archive.psycopg is not None, "explicit PostgreSQL test requested but psycopg unavailable"
    with archive.psycopg.connect(url, row_factory=archive.dict_row,
                                connect_timeout=5, options="-c default_transaction_read_only=on") as conn:
        returned = conn.execute("SELECT %s::jsonb AS payload", (archive._jsonb(payload),)).fetchone()["payload"]
    assert returned == payload
    _assert_jsonb_survival(returned)
    print("DOM capture archive PostgreSQL JSONB roundtrip: PASS")


def run():
    baseline = _payload()
    absent = _attachment(baseline)
    assert absent["status"] == "UNAVAILABLE" and absent["capture"] is None
    assert absent["validation_reason_codes"] == ["SNAPSHOT_DOM_CAPTURE_MISSING"]
    valid_capture = _capture()
    original_capture = deepcopy(valid_capture)
    complete = _payload(valid_capture)
    attached = _attachment(complete)
    assert valid_capture == original_capture
    assert attached["status"] == "COMPLETE" and attached["validation_reason_codes"] == []
    assert attached["capture"] == valid_capture
    assert attached["cycle_binding"]["source"] == "WATCH_SHARED"
    assert attached["cycle_binding"]["collector_version"] == "selftest-dom-archive-v1"
    assert attached["cycle_binding"]["study_specific"] is False
    assert set(attached["cycle_binding"]["source_code_sha256"]) == {"research_dom_capture.py", "coinglass_dom_reader.py", "research_max_pain_archive.py"}
    assert all(len(value) == 64 for value in attached["cycle_binding"]["source_code_sha256"].values())
    assert _without_diagnostics(complete) == _without_diagnostics(baseline)
    assert complete["set"]["snapshot_key"] == baseline["set"]["snapshot_key"]
    assert complete["set"]["payload_sha256"] != baseline["set"]["payload_sha256"]
    assert complete["set"]["research_eligible"] is True
    assert complete["set"]["source_metadata"]["capture_metadata"]["operational_scores"] == EXISTING_METADATA["operational_scores"]
    _assert_hash(attached)
    _assert_jsonb_survival(complete)
    _mock_persist(complete)
    truncated = _payload(_capture(truncated=True))
    assert _attachment(truncated)["status"] == "PARTIAL"
    assert _attachment(truncated)["capture"]["payload"]["timeframes"]["12h"]["status"] == "TRUNCATED"
    assert _without_diagnostics(truncated) == _without_diagnostics(baseline)
    malformed_hash = deepcopy(valid_capture)
    malformed_hash["payload_sha256"] = "0" * 64
    cycle = {}; cycle["cycle"] = cycle
    huge = {"strings": ["x" * archive.DOM_ROW_CAPTURE_MAX_STRING_CHARS] * 9}
    excessive_integer = {"value": 10**10000}
    nested = {}
    for _ in range(archive.DOM_ROW_CAPTURE_MAX_DEPTH + 2): nested = {"value": nested}
    for invalid in (None, [], {"unknown": True}, malformed_hash, cycle, huge,
                    {"value": float("nan")}, {"value": float("inf")},
                    {"value": object()}, excessive_integer, nested):
        rejected = _payload(invalid)
        marker = _attachment(rejected)
        assert marker["status"] == "UNAVAILABLE" and marker["capture"] is None
        assert marker["validation_reason_codes"]
        assert _without_diagnostics(rejected) == _without_diagnostics(baseline)
        _assert_hash(marker)
    assert "DOM_CAPTURE_SIZE_BOUND" in _attachment(_payload(huge))["validation_reason_codes"]
    assert "DOM_CAPTURE_VALIDATION_FAILED" in _attachment(_payload(malformed_hash))["validation_reason_codes"]
    forged_metadata = {**EXISTING_METADATA, "dom_row_capture": cycle}
    assert _attachment(_payload(valid_capture, metadata=forged_metadata))["capture"] == valid_capture
    with patch.object(archive, "_dom_capture_code_hashes", return_value={"missing.py": None}):
        missing_identity = _payload(valid_capture)
    assert _attachment(missing_identity)["status"] == "PARTIAL"
    assert _without_diagnostics(missing_identity) == _without_diagnostics(baseline)
    with patch.object(archive, "_dom_capture_code_hashes", return_value={
        "coinglass_dom_reader.py": "0" * 64, "research_dom_capture.py": "1" * 64,
        "research_max_pain_archive.py": "2" * 64,
    }):
        wrong_identity = _payload(valid_capture)
    assert _attachment(wrong_identity)["status"] == "PARTIAL"
    assert _attachment(wrong_identity)["capture"] == valid_capture
    assert "DOM_CAPTURE_PRODUCER_CODE_IDENTITY_MISMATCH" in _attachment(wrong_identity)["validation_reason_codes"]
    assert _without_diagnostics(wrong_identity) == _without_diagnostics(baseline)
    for helper in ("_dom_capture_code_hashes", "_dom_row_capture_metadata"):
        with patch.object(archive, helper, side_effect=RuntimeError("do not persist exception details")):
            unexpected_failure = _payload(valid_capture)
        marker = _attachment(unexpected_failure)
        assert marker["status"] == "UNAVAILABLE" and marker["capture"] is None
        assert marker["validation_reason_codes"] == ["DOM_CAPTURE_ATTACHMENT_FAILED"]
        assert _without_diagnostics(unexpected_failure) == _without_diagnostics(baseline)
        _assert_hash(marker)
    _postgres_roundtrip(complete)
    print("DOM capture archive offline self-test: PASS")


if __name__ == "__main__": run()
