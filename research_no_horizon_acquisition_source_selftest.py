"""Exact source SQL/proof and early-cutoff regressions without a database."""
from contextlib import contextmanager
from copy import deepcopy
from datetime import timedelta
import hashlib
import json
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import research_no_horizon_acquisition_source as source
import research_no_horizon_cohort as cohort
import research_no_horizon_contract as contracts
from research_no_horizon_cohort_outcomes_selftest import outcome_fixture, _seal_exports
from research_no_horizon_cohort_selftest import unsealed
from research_no_horizon_manifest_selftest import manifest_fixture


class Result:
    def __init__(self, value):
        self.value = value

    def fetchone(self):
        return self.value


class Connection:
    def __init__(self, now, response, *, read_only="on", rows=None):
        self.info = SimpleNamespace(transaction_status=0)
        self.now = now
        self.response = response
        self.read_only = read_only
        self.rows = rows
        self.calls = []
        self.source_queries = []
        self.rolled_back = False

    @contextmanager
    def transaction(self):
        self.info.transaction_status = 2
        try:
            yield
        except BaseException:
            self.rolled_back = True
            raise
        finally:
            self.info.transaction_status = 0

    def execute(self, query):
        self.calls.append(query)
        if query.startswith("SET "):
            return Result(None)
        if query.startswith("SELECT clock_timestamp()"):
            return Result({"now_utc": self.now, "read_only": self.read_only})
        raise AssertionError("Unexpected source connection SQL")

    @contextmanager
    def cursor(self):
        conn = self

        class Cursor:
            def execute(self, query):
                conn.source_queries.append(query)

            def fetchmany(self, count):
                assert count == 2
                return conn.rows if conn.rows is not None else [conn.response]

        yield Cursor()


class AcquisitionSourceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        declaration, _, exports = outcome_fixture()
        # Acquisition validates exact transport, not price outcomes. A small
        # explicitly sparse archive exercises gaps without hundreds of pages.
        candles = exports[0]["candles"][::120]
        for ordinal, export in enumerate(exports):
            left = contracts.utc(declaration["parts"][ordinal]["source_start_utc"])
            export["candles"] = [bar for bar in candles if contracts.utc(bar["open_time_utc"]) >= left]
        cls.fixture = _seal_exports(declaration, exports)

    def setUp(self):
        self.declaration, self.anchor, self.exports = deepcopy(self.fixture)
        self.now = contracts.utc(self.declaration["cutoff_utc"])
        self.patch = patch.object(source, "_configure_cursor")
        self.patch.start()
        self.addCleanup(self.patch.stop)

    def connection(self, response, *, column="chunk", now=None, **kwargs):
        text = response if isinstance(response, str) else json.dumps(response, ensure_ascii=False)
        return Connection(now or self.now, {column: text}, **kwargs)

    def response(self, task):
        manifest = self.anchor["parts"][task["part_ordinal"]]["manifest"]
        _, source_chunks, candle_chunks = manifest_fixture(self.exports[task["part_ordinal"]])
        kind = task["kind"]
        receipt = {"read_only": "on", "database_writes": False, "serializer_timezone": "UTC",
            "mvcc_snapshot": "200:205:", "fetched_at_utc": self.now.isoformat(),
            "query_identity": "no-horizon-pinned-parent-source-chunk-select-v2" if kind == "source"
                else "no-horizon-exact-candle-chunk-select-v1"}
        if kind == "source":
            receipt["parent_snapshot_mode"] = source.queries.PARENT_SNAPSHOT_MODE
            entries = []
            for ordinal in task["ordinals"]:
                chunk = source_chunks[ordinal]
                leaf = manifest["source_entries"][ordinal]
                entries.append({**chunk, **{key: leaf[key] for key in ("snapshot_set_id", "sha256", "byte_length")}})
            return {"kind": kind, "requested_count": len(entries), "returned_count": len(entries),
                "overflow": False, "entries": entries, "receipt": receipt}
        ordinal = task["ordinals"][0]
        leaf = manifest["candle_pages"][ordinal]
        return {"kind": kind, **candle_chunks[ordinal],
            **{key: leaf[key] for key in ("row_count", "sha256", "byte_length")},
            "overflow": False, "receipt": receipt}

    def read_task(self, task, response=None):
        conn = self.connection(self.response(task) if response is None else response)
        if task["kind"] == "source":
            return source.read_source_chunk(conn, self.declaration, self.anchor,
                task["part_ordinal"], task["ordinals"])
        return source.read_candle_chunk(conn, self.declaration, self.anchor,
            task["part_ordinal"], task["ordinals"][0])

    def test_exact_anchor_sql_raw_bytes_and_read_only_settings(self):
        raw = json.dumps(unsealed(self.anchor), ensure_ascii=False, indent=2)
        conn = self.connection(raw, column="anchor")
        proof = source.read_anchor(conn, self.declaration)
        self.assertEqual(proof["raw_response_text"], raw)
        self.assertEqual(source.validate_anchor_proof(self.declaration, proof), self.anchor)
        self.assertEqual(conn.source_queries, [cohort.anchor_sql(self.declaration)])
        self.assertEqual(conn.calls[0], "SET TRANSACTION READ ONLY")
        self.assertIn("SET LOCAL TIME ZONE 'UTC'", conn.calls)
        self.assertIn("SET LOCAL statement_timeout='15000ms'", conn.calls)
        self.assertEqual(conn.info.transaction_status, 0)

    def test_no_source_sql_before_cutoff_or_later_declaration_time(self):
        for later_declaration in (False, True):
            with self.subTest(later_declaration=later_declaration):
                declared = deepcopy(self.declaration)
                if later_declaration:
                    declared["declared_at_utc"] = (self.now + timedelta(hours=1)).isoformat()
                    clock = self.now
                else:
                    clock = self.now - timedelta(microseconds=1)
                conn = self.connection({}, now=clock)
                with self.assertRaises(source.SourceNotDue):
                    source.read_anchor(conn, declared)
                self.assertFalse(conn.source_queries)
                self.assertTrue(conn.rolled_back)

    def test_nonidle_or_non_read_only_connection_never_reads_sources(self):
        conn = self.connection({})
        conn.info.transaction_status = 2
        with self.assertRaisesRegex(ValueError, "IDLE"):
            source.read_anchor(conn, self.declaration)
        self.assertFalse(conn.calls)
        conn = self.connection({}, read_only="off")
        with self.assertRaisesRegex(ValueError, "NOT_READ_ONLY"):
            source.read_anchor(conn, self.declaration)
        self.assertFalse(conn.source_queries)

    def test_exact_all_part_assembly_preserves_global_input(self):
        tasks = source.leaf_tasks(self.declaration, self.anchor)
        proofs = [self.read_task(task) for task in tasks]
        for ordinal, expected in enumerate(self.exports):
            selected = [proof for proof in proofs if proof["part_ordinal"] == ordinal]
            assembled = source.assemble_part(self.declaration, self.anchor, ordinal,
                [proof for proof in selected if proof["kind"] == "source"],
                [proof for proof in selected if proof["kind"] == "candles"])
            self.assertEqual(assembled, expected)

    def test_missing_duplicate_foreign_and_extra_part_proofs_block(self):
        proofs = [self.read_task(task) for task in source.leaf_tasks(self.declaration, self.anchor)]
        sources = [proof for proof in proofs if proof["part_ordinal"] == 0 and proof["kind"] == "source"]
        candles = [proof for proof in proofs if proof["part_ordinal"] == 0 and proof["kind"] == "candles"]
        foreign = next(proof for proof in proofs if proof["part_ordinal"] == 1 and proof["kind"] == "source")
        for changed in ([], sources + sources, [foreign]):
            with self.subTest(changed=len(changed)):
                with self.assertRaises(ValueError):
                    source.assemble_part(self.declaration, self.anchor, 0, changed, candles)

    def test_source_response_extra_missing_duplicate_or_wrong_id_retains_rejection_proof(self):
        task = next(task for task in source.leaf_tasks(self.declaration, self.anchor) if task["kind"] == "source")
        for change in ("extra", "missing", "duplicate", "id", "byte", "hash"):
            with self.subTest(change=change):
                raw = self.response(task)
                if change == "extra": raw["entries"].append(deepcopy(raw["entries"][0]))
                elif change == "missing": raw["entries"].pop()
                elif change == "duplicate": raw["entries"][1] = deepcopy(raw["entries"][0])
                elif change == "id": raw["entries"][0]["snapshot_set_id"] += 999
                elif change == "byte": raw["entries"][0]["payload_text"] += " "
                else: raw["entries"][0]["sha256"] = "0" * 64
                with self.assertRaises(source.IntegrityError) as caught:
                    self.read_task(task, raw)
                self.assertEqual(json.loads(caught.exception.proof["raw_response_text"]), raw)

    def test_candle_count_overflow_and_route_payload_changes_block(self):
        task = next(task for task in source.leaf_tasks(self.declaration, self.anchor) if task["kind"] == "candles")
        for change in ("count", "overflow", "payload", "ordinal"):
            with self.subTest(change=change):
                raw = self.response(task)
                if change == "count": raw["row_count"] += 1
                elif change == "overflow": raw["overflow"] = True
                elif change == "ordinal": raw["ordinal"] += 1
                else: raw["payload_text"] = raw["payload_text"].replace("BINANCE", "OTHER")
                with self.assertRaises(source.IntegrityError):
                    self.read_task(task, raw)

    def test_fetch_provenance_requires_readonly_utc_pinned_parents_and_current_time(self):
        task = next(task for task in source.leaf_tasks(self.declaration, self.anchor) if task["kind"] == "source")
        variants = {"read_only": "off", "serializer_timezone": "Asia/Jerusalem",
            "database_writes": True, "parent_snapshot_mode": "LIVE_PARENT_LOOKUP",
            "fetched_at_utc": (self.now - timedelta(seconds=1)).isoformat(),
            "query_identity": "different-query", "mvcc_snapshot": ""}
        for field, value in variants.items():
            with self.subTest(field=field):
                raw = self.response(task)
                raw["receipt"][field] = value
                with self.assertRaises(source.IntegrityError):
                    self.read_task(task, raw)

    def test_rehashed_proof_cannot_change_sql_anchor_or_declaration_binding(self):
        task = source.leaf_tasks(self.declaration, self.anchor)[0]
        proof = self.read_task(task)
        for field, value in (("sql", proof["sql"] + "\n"), ("anchor_sha256", "0" * 64),
                             ("declaration_sha256", "0" * 64), ("proof_version", "future")):
            changed = deepcopy(proof)
            changed[field] = value
            changed["query_sha256"] = source._sha(changed["sql"])
            with self.subTest(field=field):
                with self.assertRaises(ValueError):
                    source.validate_chunk_proof(self.declaration, self.anchor, changed)

    def test_rejected_anchor_retains_raw_response_and_does_not_reanchor(self):
        raw = unsealed(self.anchor)
        raw["parts"].pop()
        conn = self.connection(raw, column="anchor")
        with self.assertRaises(source.IntegrityError) as caught:
            source.read_anchor(conn, self.declaration)
        self.assertEqual(json.loads(caught.exception.proof["raw_response_text"]), raw)
        self.assertEqual(len(conn.source_queries), 1)

    def test_oversized_raw_response_keeps_only_explicit_bounded_diagnostic(self):
        raw = "x" * 101
        conn = self.connection(raw, column="anchor")
        with patch.object(source, "MAX_ANCHOR_RESPONSE_BYTES", 100):
            with self.assertRaises(source.IntegrityError) as caught:
                source.read_anchor(conn, self.declaration)
        self.assertIsNone(caught.exception.proof)
        diagnostic = caught.exception.diagnostic
        self.assertEqual(diagnostic["raw_response_sha256"], hashlib.sha256(raw.encode()).hexdigest())
        self.assertEqual(diagnostic["raw_response_bytes"], 101)
        self.assertIs(diagnostic["raw_retained"], False)
        self.assertIs(diagnostic["truncated"], True)

    def test_invalid_task_bounds_fail_before_connection_operations(self):
        for ordinal, leaves in ((-1, [0]), (0, []), (0, list(range(9))), (0, [1, 0]), (False, [0])):
            conn = self.connection({})
            with self.subTest(ordinal=ordinal, leaves=leaves):
                with self.assertRaises(ValueError):
                    source.read_source_chunk(conn, self.declaration, self.anchor, ordinal, leaves)
                self.assertFalse(conn.calls)

    def test_raw_loader_preserves_utf8_and_outer_duplicate_json_rejects(self):
        self.assertEqual(source._raw_json('שלום'.encode()), 'שלום')
        conn = self.connection('{"anchor_version":"a","anchor_version":"b"}', column="anchor")
        with self.assertRaises(source.IntegrityError):
            source.read_anchor(conn, self.declaration)

    def test_outer_result_requires_exact_one_row_and_column(self):
        for rows in ([], [{"anchor": "{}"}, {"anchor": "{}"}], [{"foreign": "{}"}]):
            conn = self.connection({}, rows=rows)
            with self.subTest(rows=rows):
                with self.assertRaisesRegex(ValueError, "CARDINALITY"):
                    source.read_anchor(conn, self.declaration)


if __name__ == '__main__':
    unittest.main()
