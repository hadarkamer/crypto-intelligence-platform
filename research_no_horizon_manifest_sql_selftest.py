"""Pure pinned-parent SQL and unchanged exact-leaf transport regressions.

These tests do not execute SQL. Separate PostgreSQL tests verify actual server
serialization and reconstruction across parent updates and closure.
"""
from copy import deepcopy
from datetime import timedelta
import hashlib
import json
import re
import unittest

import research_no_horizon_contract as contracts
import research_no_horizon_manifest as transport
import research_no_horizon_manifest_sql as sql
from research_no_horizon_manifest_selftest import manifest_fixture


def pinned_fixture():
    anchor, sources, candles = manifest_fixture()
    anchor["receipt"]["parent_snapshot_mode"] = sql.PARENT_SNAPSHOT_MODE
    for entry, chunk in zip(anchor["source_entries"], sources):
        entry["btc_parent_payload_text"] = contracts.canonical(json.loads(chunk["payload_text"])["btc_parent"])
    return anchor, sources, candles


class ManifestSQLTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.original = pinned_fixture()

    def setUp(self):
        self.anchor, self.sources, self.candles = deepcopy(self.original)
        self.plan = {key: self.anchor[key] for key in (
            "source_start_utc", "source_end_utc", "cutoff_utc", "symbol", "route")}

    def more_entries(self, count):
        base = self.anchor["source_entries"][0]
        self.anchor["source_entries"] = [{**deepcopy(base), "ordinal": i,
            "snapshot_set_id": base["snapshot_set_id"]+i} for i in range(count)]
        self.anchor["receipt"].update(source_count=count, source_count_capped_plus_one=count)

    def test_anchor_pins_same_materialized_payload_that_full_hash_commits(self):
        query = sql.manifest_sql(self.plan)
        self.assertIn("source_json AS MATERIALIZED", query)
        self.assertIn("payload_json::text AS payload_text", query)
        self.assertIn("(payload_json->'btc_parent')::text AS btc_parent_payload_text", query)
        self.assertIn("sha256(convert_to(payload_text,'UTF8'))", query)
        self.assertIn("'btc_parent_payload_text',btc_parent_payload_text", query)
        self.assertIn("'parent_snapshot_mode','ANCHOR_PARENT_JSONB_TEXT_V1'", query)
        self.assertIn("LEFT JOIN public.research_max_pain_snapshot_sets", query)
        self.assertIn("LEFT JOIN LATERAL", query)
        self.assertEqual(query.count("i.intake_status='ACCEPTED'"), 2)
        self.assertEqual(query.count("i.consumer_version='watch-all-scan-intake-v1'"), 2)
        self.assertIn("LIMIT 257", query)
        self.assertIn("LIMIT 44641", query)
        self.assertEqual(query.count(";"), 1)
        self.assertIsNone(re.search(r"\b(INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|SET)\b", query))

    def test_chunk_uses_exact_hex_pin_without_live_parent_join(self):
        raw = '{"state_json": {"x": 0.123456789012345678901234567890, "note": "שלום \' ; DROP TABLE x; -- \\\\"}, "end_time_utc": null}'
        self.anchor["source_entries"][0]["btc_parent_payload_text"] = raw
        before = deepcopy(self.anchor)
        query = sql.source_chunk_sql(self.anchor, [0])
        encoded = re.findall(r"convert_from\(decode\('([0-9a-f]+)','hex'\),'UTF8'\)::jsonb", query)
        self.assertEqual(len(encoded), 1)
        self.assertEqual(bytes.fromhex(encoded[0]), raw.encode("utf-8"))
        self.assertNotIn(raw, query)
        self.assertNotIn("research_btc_parent_movements", query)
        self.assertNotIn("to_jsonb(p)", query)
        self.assertIn("'btc_parent',requested.btc_parent", query)
        self.assertIn("LEFT JOIN public.research_max_pain_snapshot_sets", query)
        self.assertIn("research_btc_price_bars", query)
        self.assertIn("i.intake_status='ACCEPTED'", query)
        self.assertIn("sha256(convert_to(payload_text,'UTF8'))", query)
        self.assertIn("LIMIT 9", query)
        self.assertEqual(self.anchor, before)

    def test_null_pin_is_used_directly_without_parent_fallback(self):
        self.anchor["source_entries"][0]["btc_parent_payload_text"] = "null"
        query = sql.source_chunk_sql(self.anchor, [0])
        self.assertIn("decode('6e756c6c','hex')", query)
        self.assertNotIn("research_btc_parent_movements", query)
        self.assertIn("'btc_parent',requested.btc_parent", query)

    def test_valid_pinned_manifest_still_assembles_with_unchanged_transport(self):
        sql.source_chunk_sql(self.anchor, [0])
        for ordinal in range(len(self.anchor["candle_pages"])):
            sql.candle_chunk_sql(self.anchor, ordinal)
        export = transport.assemble_export(self.anchor, self.sources, self.candles)
        transport.validate_export_binding(export)
        self.assertEqual(export["source_rows"][0]["btc_parent"],
            json.loads(self.anchor["source_entries"][0]["btc_parent_payload_text"]))
        self.assertFalse(export["source_receipt"]["db_origin_authenticated_by_this_tool"])

    def test_changed_parent_or_other_source_piece_still_fails_full_hash(self):
        for key in ("btc_parent", "scores", "btc_prior_bar", "intake", "stored_source"):
            with self.subTest(fragment=key):
                chunks = deepcopy(self.sources)
                row = json.loads(chunks[0]["payload_text"])
                row[key] = {**(row[key] or {}), "changed_after_anchor": True}
                chunks[0]["payload_text"] = contracts.canonical(row)
                self.assertNotEqual(hashlib.sha256(chunks[0]["payload_text"].encode()).hexdigest(),
                    self.anchor["source_entries"][0]["sha256"])
                with self.assertRaisesRegex(ValueError, "hash/byte"):
                    transport.assemble_export(self.anchor, chunks, self.candles)

    def test_legacy_anchor_remains_usable_but_new_builder_requires_pin_mode(self):
        legacy = deepcopy(self.anchor)
        legacy["receipt"].pop("parent_snapshot_mode")
        legacy["source_entries"][0].pop("btc_parent_payload_text")
        transport.assemble_export(legacy, self.sources, self.candles)
        for build, argument in ((sql.source_chunk_sql, [0]), (sql.candle_chunk_sql, 0)):
            with self.assertRaisesRegex(ValueError, "pinned parent"):
                build(legacy, argument)

    def test_missing_and_invalid_parent_pins_fail_before_sql_generation(self):
        invalid = (None, {}, [], 1, True, "", "[]", "true", "1", '"parent"',
            '{"x":1,"x":2}', '{"x":NaN}', '{"x":Infinity}',
            '{"x":"\\ud800"}', '{"x":"\\u0000"}')
        for pin in invalid:
            with self.subTest(pin=repr(pin)):
                self.anchor["source_entries"][0]["btc_parent_payload_text"] = pin
                with self.assertRaises(ValueError):
                    sql.source_chunk_sql(self.anchor, [0])
        self.anchor["source_entries"][0].pop("btc_parent_payload_text")
        with self.assertRaisesRegex(ValueError, "missing exact parent"):
            sql.source_chunk_sql(self.anchor, [0])

    def test_whole_anchor_is_validated_even_for_an_unrequested_source(self):
        self.more_entries(2)
        self.anchor["source_entries"][1]["btc_parent_payload_text"] = "[1]"
        with self.assertRaises(ValueError):
            sql.source_chunk_sql(self.anchor, [0])
        with self.assertRaises(ValueError):
            sql.candle_chunk_sql(self.anchor, 0)

    def test_exact_unique_bounded_source_ordinal_selection(self):
        self.more_entries(9)
        for requested in (None, [], [False], [True], [-1], [9], [0.0], ["0"], [0, 0], list(range(9))):
            with self.subTest(requested=requested):
                with self.assertRaises(ValueError):
                    sql.source_chunk_sql(self.anchor, requested)
        query = sql.source_chunk_sql(self.anchor, list(reversed(range(8))))
        self.assertIn("'requested_count',8", query)
        self.assertEqual(query.count("convert_from(decode("), 8)
        self.assertLess(query.index("(0::integer,"), query.index("(7::integer,"))

    def test_broken_manifest_counts_duplicate_missing_ordinals_and_bounds_rejected(self):
        mutations = (
            lambda a: a["source_entries"][0].update(ordinal=True),
            lambda a: a["source_entries"][0].update(snapshot_set_id=False),
            lambda a: a["source_entries"].append(deepcopy(a["source_entries"][0])),
            lambda a: a["source_entries"].clear(),
            lambda a: a["receipt"].update(source_count_capped_plus_one=2),
            lambda a: a["receipt"].update(read_only="off"),
            lambda a: a["receipt"].update(source_overflow=True),
            lambda a: a["receipt"].update(query_sha256="unknown"),
            lambda a: a["receipt"].update(page_size=True),
            lambda a: a.update(symbol="HYPE"),
        )
        for mutate in mutations:
            anchor = deepcopy(self.original[0])
            mutate(anchor)
            with self.assertRaises(ValueError):
                sql.source_chunk_sql(anchor, [0])

    def test_manifest_byte_limit_includes_parent_pins(self):
        self.anchor["source_entries"][0]["btc_parent_payload_text"] = json.dumps({"x": "x" * transport.MAX_MANIFEST_BYTES})
        with self.assertRaisesRegex(ValueError, "byte limit"):
            sql.source_chunk_sql(self.anchor, [0])

    def test_candle_query_uses_anchored_page_bounds_and_unchanged_payload(self):
        page = self.anchor["candle_pages"][0]
        query = sql.candle_chunk_sql(self.anchor, 0)
        self.assertIn(sql.timestamp(page["first_open_utc"]), query)
        self.assertIn(sql.timestamp(page["last_open_utc"]), query)
        self.assertIn("LIMIT 3", query)
        self.assertIn("'overflow',payload.row_count>2", query)
        self.assertIn("sha256(convert_to(payload.payload_text,'UTF8'))", query)
        self.assertNotIn("btc_parent", query)
        self.assertIn("c.open_time_utc+INTERVAL '1 minute'<=", query)
        self.assertIn("ORDER BY c.open_time_utc", query)

    def test_candle_ordinals_and_nonminute_page_times_fail_closed(self):
        for ordinal in (False, True, -1, 2, 0.0, "0", None):
            with self.subTest(ordinal=ordinal), self.assertRaises(ValueError):
                sql.candle_chunk_sql(self.anchor, ordinal)
        for field in ("first_open_utc", "last_open_utc"):
            bad = deepcopy(self.anchor)
            bad["candle_pages"][0][field] = (contracts.utc(bad["candle_pages"][0][field])+timedelta(seconds=1)).isoformat()
            with self.assertRaises(ValueError):
                sql.candle_chunk_sql(bad, 0)

    def test_plan_bounds_timezone_route_and_integer_types_fail_closed(self):
        changes = (
            {"symbol": "BTC';DROP TABLE x;--"}, {"symbol": []}, {"route": "OTHER"},
            {"source_start_utc": "2026-10-01T00:00:00"}, {"source_start_utc": False},
            {"source_start_utc": self.plan["source_end_utc"]},
            {"cutoff_utc": (contracts.utc(self.plan["source_start_utc"])+timedelta(days=31, seconds=1)).isoformat()},
            {"source_row_limit": True}, {"source_row_limit": 0}, {"source_row_limit": 257},
            {"max_candle_rows": 44641}, {"max_candle_rows": 1.0},
            {"page_size": False}, {"page_size": 2049},
            {"source_byte_limit": True}, {"source_byte_limit": transport.MAX_BYTES+1},
        )
        for change in changes:
            with self.subTest(change=change), self.assertRaises(ValueError):
                sql.manifest_sql({**self.plan, **change})

    def test_smaller_declared_caps_and_first_full_minute_are_honored(self):
        plan = {**self.plan, "source_row_limit": 1, "max_candle_rows": 4, "page_size": 2}
        plan["source_start_utc"] = contracts.utc(plan["source_start_utc"])+timedelta(seconds=1)
        query = sql.manifest_sql(plan)
        self.assertIn("LIMIT 2", query)
        self.assertIn("LIMIT 5", query)
        self.assertIn("'source_cap',1,'candle_cap',4,'page_size',2", query)
        first = plan["source_start_utc"].replace(second=0)+timedelta(minutes=1)
        self.assertIn(f"'{first.isoformat()}'::timestamptz AS price_start", query)


if __name__ == "__main__":
    unittest.main()
