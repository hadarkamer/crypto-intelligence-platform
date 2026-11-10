"""Diagnostic archive attachment tests; optional explicit-test-DSN JSONB read."""
from copy import deepcopy
from datetime import timedelta
import json
import os
from types import SimpleNamespace
from unittest.mock import patch
import asyncio
from contextlib import ExitStack
from pathlib import Path
import socket
import unittest
from unittest.mock import AsyncMock, Mock

import coinglass_dom_reader as reader
from coinglass_dom_reader_selftest import table

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


def _mock_persist(payload, persist_function=None):
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
        result = (persist_function or archive.persist_snapshot_payload)(payload, database_url="selftest://no-network")
    assert result["persisted"] is True and saved["committed"] is True
    stored = saved["set_parameters"][archive._SET_COLUMNS.index("source_metadata")]
    stored = getattr(stored, "obj", stored)
    assert stored == payload["set"]["source_metadata"]
    assert saved.get("inserted_rows", 0) == len(payload["symbols"]) + len(payload["rows"])


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


class FailedPollPipelineTests(unittest.IsolatedAsyncioTestCase):
    async def test_collection_archive_failure_recovery_and_idempotency(self):
        # Only an explicitly supplied Unix socket DSN is accepted. No production
        # environment discovery or exchange/Telegram connection is permitted.
        import main
        dsn = os.environ.get('TEST_DATABASE_URL')
        if dsn:
            import psycopg
            from psycopg.conninfo import conninfo_to_dict
            self.assertTrue(conninfo_to_dict(dsn).get('host', '').startswith('/'))
            with psycopg.connect(dsn) as conn:
                conn.execute((Path(__file__).parent / 'migrations/007_max_pain_watch_archive_v1.sql').read_text())
        original_persist = archive.persist_snapshot_payload
        saved = []
        def persist(payload):
            if dsn:
                result = original_persist(payload, database_url=dsn)
            else:
                # Actual persist_snapshot_payload SQL/commit code, with the
                # existing offline connection double; no real DB claim.
                _mock_persist(payload, original_persist)
                result = {'persisted': True, 'snapshot_set_id': len(saved) + 1}
            saved.append((deepcopy(payload), result))
            return result
        def enrich(rows, *args, **kwargs):
            return {'rows': deepcopy(rows), 'skipped_symbols': []}
        for scenario in ('failure', 'recovery', 'diagnostic_failure', 'archive_failure', 'watch_failure', 'watch_recovery'):
            failed = scenario in ('failure', 'diagnostic_failure', 'watch_failure')
            pages = []
            context = SimpleNamespace(close=AsyncMock())
            browser = SimpleNamespace(new_context=AsyncMock(return_value=context), close=AsyncMock())
            manager = AsyncMock()
            manager.__aenter__.return_value = SimpleNamespace(chromium=SimpleNamespace(launch=AsyncMock(return_value=browser)))
            async def new_page(*args):
                page = SimpleNamespace(label='24h', close=AsyncMock(), wait_for_timeout=AsyncMock())
                pages.append(page)
                return page
            async def click(page, timeframe):
                page.label = timeframe
                return True
            async def label(page):
                return page.label
            async def extract(page):
                data = table(50, short=str(82000 + reader.DEFAULT_TIMEFRAMES.index(page.label) * 100))
                if failed and page.label in ('12h', '24h'):
                    for row in data['rows'][:25 if page.label == '12h' else 22]:
                        row[4] = {'text': '—', 'primary': '—', 'amount': ''}
                return [data]
            prepare = AsyncMock()
            archive_context = {'cycle_id': 'isolated-failed-poll:' + scenario,
                'cycle_time_utc': fixtures.BASE, 'source': 'WATCH_SHARED'}
            before = len(saved)
            with ExitStack() as stack:
                stack.enter_context(patch.dict(os.environ, {'MAX_PAIN_ARCHIVE_ENABLED': '1'}))
                for target, name, value in (
                    (reader, 'async_playwright', Mock(return_value=manager)),
                    (reader, '_new_ready_page', new_page),
                    (reader, '_click_timeframe_verified', click),
                    (reader, '_active_timeframe_label', label),
                    (reader, '_extract_tables', extract),
                    (reader.asyncio, 'sleep', AsyncMock()),
                    (archive, 'persist_snapshot_payload', persist if scenario != 'archive_failure' else Mock(side_effect=RuntimeError('isolated database failure'))),
                    (main.live_price_provider, 'enrich_snapshot_rows', enrich),
                    (main.live_price_provider, 'enrich_research_snapshot_rows', enrich),
                    (socket.socket, 'connect', Mock(side_effect=AssertionError('external network forbidden'))),
                ):
                    stack.enter_context(patch.object(target, name, value))
                if scenario == 'diagnostic_failure':
                    stack.enter_context(patch.object(capture, 'capture_poll', side_effect=RuntimeError('isolated capture failure')))
                if scenario.startswith('watch_'):
                    from research_watch_score_capture_selftest import derivatives
                    bot = SimpleNamespace(bot=SimpleNamespace(send_message=AsyncMock()))
                    manual = AsyncMock(return_value=0)
                    score = Mock(wraps=main.research_watch_score_capture.prepare)
                    stack.enter_context(patch.dict(os.environ, {'ALERT_DELIVERY_PROFILE': 'SELECTED_EXPERIMENTAL_ONLY'}))
                    for target, name, value in (
                        (main, 'WATCH_RUNTIME', {'chat_id': 1}),
                        (main, 'WATCH_GENERAL_ENABLED', True),
                        (main, '_get_scrape_lock', lambda: asyncio.Lock()),
                        (main, '_ensure_watch_derivatives_ready', AsyncMock(return_value={})),
                        (main.market_confidence_engine, 'capture_snapshot', derivatives),
                        (main.research_watch_score_capture, 'prepare', score),
                        (main.research_consumed_inputs, 'submit', Mock(return_value={})),
                        (main.research_watch_runtime, 'begin', Mock(return_value=None)),
                        (main.research_watch_runtime, 'stage', Mock()),
                        (main.research_watch_runtime, 'finish', Mock()),
                        (main.research_watch_runtime, 'cleanup_tail', Mock()),
                        (main.research_alert_decision_watch, 'begin', Mock(return_value=None)),
                        (main.research_alert_decision_watch, 'finish', Mock()),
                        (main.experimental_reference_price, 'prepare_reference_prices', AsyncMock(return_value={})),
                        (main.manual_formula_alert_delivery, 'run_watch', manual),
                        (main.sol_proximity_experimental_worker.WORKER, 'observe', AsyncMock()),
                        (main.doge_partial_experimental_worker.WORKER, 'observe', AsyncMock()),
                    ):
                        stack.enter_context(patch.object(target, name, value))
                    for worker in main.sol_proximity_experimental_worker.ADDITIONAL_WORKERS.values():
                        stack.enter_context(patch.object(worker, 'observe', AsyncMock()))
                    result = await main.run_watch_cycle(bot, 1, top8_only=True, general_enabled=True)
                    self.assertEqual(result['ok'], not failed, result)
                    self.assertFalse(main.WATCH_RUNTIME['scan_in_progress'])
                    self.assertEqual(main.WATCH_RUNTIME['last_cycle_status'], 'failed' if failed else 'completed')
                    self.assertEqual(score.call_count, 0 if failed else 1)
                    self.assertEqual(manual.await_count, 0 if failed else 2)
                    bot.bot.send_message.assert_not_awaited()
                elif failed:
                    with self.assertRaisesRegex(RuntimeError, 'scan incomplete'):
                        await main.collect_live_rows_for_watch(archive_context=archive_context, prepare_watch=prepare)
                    prepare.assert_not_awaited()
                else:
                    rows, result = await main.collect_live_rows_for_watch(archive_context=archive_context, prepare_watch=prepare)
                    self.assertEqual(len(rows), 350)
                    prepare.assert_awaited_once()
                    self.assertTrue(result['timeframe_integrity']['ok'])
            self.assertEqual(len(pages), 11 if failed else 7)
            for page in pages:
                page.close.assert_awaited_once()
            context.close.assert_awaited_once()
            browser.close.assert_awaited_once()
            if scenario == 'archive_failure':
                self.assertEqual(len(saved), before)
                continue
            self.assertEqual(len(saved), before + 1)
            payload, persisted = saved[-1]
            self.assertTrue(persisted['persisted'])
            if dsn:
                with psycopg.connect(dsn) as conn:
                    record = conn.execute('SELECT source_metadata, row_count, collection_status FROM research_max_pain_snapshot_sets WHERE snapshot_set_id=%s', (persisted['snapshot_set_id'],)).fetchone()
            else:
                record = json.loads(json.dumps([payload['set']['source_metadata'], payload['set']['row_count'], payload['set']['collection_status']]))
            self.assertEqual(record[0], payload['set']['source_metadata'])
            self.assertEqual(record[1], 0 if failed else 350)
            self.assertEqual(record[2], 'FAILED' if failed else 'COMPLETE')
            if scenario == 'failure':
                frames = record[0]['capture_metadata']['dom_row_capture']['capture']['payload']['timeframes']
                for tf, expected in (('24h', 22), ('12h', 25)):
                    p = frames[tf]['payload']
                    self.assertEqual(p['failure_summary']['classified_rejected_rows'], expected)
                    self.assertEqual(p['failure_summary']['rejection_reason_counts']['LONG_TARGET_SOURCE_MISSING'], expected)
                    self.assertEqual(len(p['attempt_history']), 3)
                    self.assertTrue(all(h['failure_summary']['classified_rejected_rows'] == expected for h in p['attempt_history']))
                    self.assertIsNone(p['selected_table'])
            if dsn:
                with patch.dict(os.environ, {'MAX_PAIN_ARCHIVE_ENABLED': '1'}):
                    replay = original_persist(payload, database_url=dsn)
                self.assertTrue(replay['idempotent_existing'])
                self.assertEqual(replay['snapshot_set_id'], persisted['snapshot_set_id'])


if __name__ == "__main__": run()
