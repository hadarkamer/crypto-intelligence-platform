"""Exact-byte manifest transport regressions using genuine captured sources."""
from copy import deepcopy
import hashlib
import json
import unittest
from unittest.mock import patch

import research_no_horizon_contract as contracts
import research_no_horizon_manifest as manifest
import research_no_horizon_source as source
import research_watch_scan_formula as formulas
from research_no_horizon_source_selftest import fixture


def _proof(text):
    raw = text.encode("utf-8")
    return {"sha256": hashlib.sha256(raw).hexdigest(), "byte_length": len(raw)}


def manifest_fixture(export=None):
    export = json.loads(formulas.canonical(fixture() if export is None else export))
    anchor = {key: export[key] for key in ("export_version", "symbol", "source_start_utc", "source_end_utc", "cutoff_utc")}
    anchor.update(transport_version=manifest.MODE, route=manifest.ROUTE, source_entries=[], candle_pages=[])
    sources, candles = [], []
    for ordinal, row in enumerate(export["source_rows"]):
        text = contracts.canonical(row)
        sources.append({"ordinal": ordinal, "payload_text": text})
        anchor["source_entries"].append({"ordinal": ordinal,
            "snapshot_set_id": row["intake"]["snapshot_set_id"], "usable_from_utc": row["intake"]["usable_from_utc"], **_proof(text)})
    for cursor in range(0, len(export["candles"]), 2):
        group = export["candles"][cursor:cursor+2]
        ordinal = len(candles)
        text = contracts.canonical(group)
        candles.append({"ordinal": ordinal, "payload_text": text})
        anchor["candle_pages"].append({"ordinal": ordinal, "first_open_utc": group[0]["open_time_utc"],
            "last_open_utc": group[-1]["open_time_utc"], "row_count": len(group), **_proof(text)})
    anchor["receipt"] = {"transaction_mode": "SINGLE_STATEMENT_READ_ONLY", "read_only": "on",
        "mvcc_snapshot": "123:125:", "extracted_at_utc": export["cutoff_utc"],
        "query_sha256": hashlib.sha256(b"fixture single SQL snapshot").hexdigest(),
        "source_count": len(sources), "candle_count": len(export["candles"]),
        "source_cap": 256, "candle_cap": 44640, "page_size": 2,
        "source_count_capped_plus_one": len(sources), "candle_count_capped_plus_one": len(export["candles"]),
        "source_overflow": False, "candle_overflow": False}
    return anchor, sources, candles


class ManifestTransport(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.original = manifest_fixture()

    def setUp(self):
        self.anchor, self.sources, self.candles = deepcopy(self.original)

    def assemble(self):
        return manifest.assemble_export(self.anchor, self.sources, self.candles)

    def rewrite_source(self, text):
        self.sources[0]["payload_text"] = text
        self.anchor["source_entries"][0].update(_proof(text))

    def rewrite_page(self, text):
        self.candles[0]["payload_text"] = text
        self.anchor["candle_pages"][0].update(_proof(text))

    def test_genuine_roundtrip_integral_floats_and_source_adapter_parity(self):
        export = self.assemble()
        self.assertIsNone(manifest.validate_export_binding(export))
        normalized = json.loads(formulas.canonical(export))
        self.assertEqual(export["source_rows"], normalized["source_rows"])
        self.assertIsNone(manifest.validate_export_binding(normalized))
        self.assertIs(type(normalized["candles"][0]["open"]), float)
        snapshot = source.build_snapshot(normalized, "FUTURES_CVD_TOTAL_65", "SHORT", "BTC", 1.5)
        legacy = source.build_snapshot(fixture(), "FUTURES_CVD_TOTAL_65", "SHORT", "BTC", 1.5)
        self.assertEqual(snapshot["source_receipt"]["counts"], legacy["source_receipt"]["counts"])
        self.assertEqual(snapshot["candles"], legacy["candles"])
        self.assertTrue(snapshot["source_coverage_complete"])
        self.assertEqual(len(snapshot["opportunities"]), 1)
        self.assertFalse(export["source_receipt"]["db_origin_authenticated_by_this_tool"])
        self.assertNotIn("payload_text", contracts.canonical(export))

    def test_raw_whitespace_unicode_and_bytes_are_bound_before_parse(self):
        row = json.loads(self.sources[0]["payload_text"])
        row["audit_note"] = "שלום"
        self.rewrite_source(json.dumps(row, ensure_ascii=False, indent=2))
        self.assertGreater(self.anchor["source_entries"][0]["byte_length"], len(self.sources[0]["payload_text"]))
        self.assemble()
        self.sources[0]["payload_text"] += " "
        with self.assertRaisesRegex(ValueError, "hash/byte"):
            self.assemble()

    def test_source_and_candle_mutation_mismatch_anchor(self):
        for chunks in (self.sources, self.candles):
            with self.subTest(kind=chunks is self.sources):
                original = chunks[0]["payload_text"]
                chunks[0]["payload_text"] = original.replace("100.0", "101.0", 1) if "100.0" in original else original+" "
                with self.assertRaisesRegex(ValueError, "hash/byte"):
                    self.assemble()
                chunks[0]["payload_text"] = original

    def test_missing_extra_duplicate_and_out_of_order_chunk_delivery(self):
        expected = self.assemble()
        self.candles.reverse()
        self.assertEqual(self.assemble(), expected)
        original = deepcopy(self.candles)
        for chunks in (original[:-1], original+[original[0]], [original[0], original[0]]):
            self.candles = chunks
            with self.assertRaises(ValueError):
                self.assemble()

    def test_duplicate_json_keys_and_nonfinite_numbers_are_rejected_after_valid_hash(self):
        for text in ('{"x":1,"x":2}', '{"x":NaN}', '{"x":Infinity}', '{"x":1e999}', '{"x":"\\ud800"}'):
            with self.subTest(text=text):
                self.rewrite_source(text)
                with self.assertRaises(ValueError):
                    self.assemble()

    def test_source_identity_and_anchor_source_order_are_checked(self):
        raw = json.loads(self.sources[0]["payload_text"])
        raw["intake"]["snapshot_set_id"] += 1
        self.rewrite_source(contracts.canonical(raw))
        with self.assertRaisesRegex(ValueError, "source identity"):
            self.assemble()
        self.anchor, self.sources, self.candles = deepcopy(self.original)
        self.anchor["source_entries"][0]["ordinal"] = 1
        with self.assertRaisesRegex(ValueError, "ordinal"):
            self.assemble()

    def test_price_page_count_bounds_duplicates_and_route_are_checked(self):
        for mutation in ("reverse", "duplicate", "route", "row_count", "bounds"):
            with self.subTest(mutation=mutation):
                self.anchor, self.sources, self.candles = deepcopy(self.original)
                bars = json.loads(self.candles[0]["payload_text"])
                if mutation == "reverse":
                    bars.reverse()
                elif mutation == "duplicate":
                    bars[1] = deepcopy(bars[0])
                elif mutation == "route":
                    bars[0]["route"] = "OTHER"
                elif mutation == "row_count":
                    bars.pop()
                else:
                    self.anchor["candle_pages"][0]["last_open_utc"] = self.anchor["candle_pages"][0]["first_open_utc"]
                self.rewrite_page(contracts.canonical(bars))
                with self.assertRaises(ValueError):
                    self.assemble()

    def test_caps_readonly_truncation_counts_and_modes_fail_closed(self):
        changes = ({"read_only": "off"}, {"transaction_mode": "REPEATABLE_READ_READ_ONLY"},
            {"source_overflow": True}, {"candle_overflow": True}, {"source_count": 2},
            {"candle_count": 5}, {"source_count_capped_plus_one": 2}, {"candle_count_capped_plus_one": 5},
            {"source_cap": 257}, {"candle_cap": 44641}, {"truncated": True},
            {"rows_complete": False}, {"query_sha256": "unknown"}, {"mvcc_snapshot": ""})
        for change in changes:
            with self.subTest(change=change):
                self.anchor = deepcopy(self.original[0])
                self.anchor["receipt"].update(change)
                with self.assertRaises(ValueError):
                    self.assemble()
        self.anchor = deepcopy(self.original[0])
        self.anchor["transport_version"] = "unknown"
        with self.assertRaises(ValueError):
            self.assemble()

    def test_byte_limits_include_raw_chunks_manifest_and_compact_export(self):
        for name, limit in (("MAX_CHUNK_BYTES", 1), ("MAX_MANIFEST_BYTES", 1), ("MAX_BYTES", 1)):
            with patch.object(manifest, name, limit):
                with self.assertRaises(ValueError):
                    self.assemble()
        raw_size = sum(entry["byte_length"] for entry in self.anchor["source_entries"]+self.anchor["candle_pages"])
        with patch.object(manifest, "MAX_BYTES", raw_size):
            with self.assertRaisesRegex(ValueError, "byte limit"):
                self.assemble()

    def test_compact_payload_receipt_and_manifest_tampering_is_detected(self):
        export = self.assemble()
        mutations = (
            lambda item: item["candles"][0].update(open=101.),
            lambda item: item["source_rows"][0]["scores"].update(extra="changed"),
            lambda item: item["source_receipt"].update(expected_accepted_rows=0),
            lambda item: item["source_receipt"]["anchor_manifest"]["receipt"].update(query_sha256="a"*64),
            lambda item: item["source_receipt"].update(canonical_payload_sha256="a"*64),
            lambda item: item["source_receipt"].update(transaction_mode="OTHER"))
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                modified = deepcopy(export)
                mutation(modified)
                with self.assertRaises(ValueError):
                    manifest.validate_export_binding(modified)

    def test_unknown_source_is_preserved_instead_of_dropped(self):
        for missing in (None, {"snapshot_set_id": None, "snapshot_key": None, "payload_sha256": None}):
            with self.subTest(missing=missing):
                data = fixture()
                data["source_rows"][0]["stored_source"] = missing
                self.anchor, self.sources, self.candles = manifest_fixture(data)
                export = self.assemble()
                snapshot = source.build_snapshot(export, "FUTURES_CVD_TOTAL_65", "SHORT", "BTC", 1.5)
                self.assertFalse(snapshot["source_coverage_complete"])
                self.assertEqual(snapshot["source_receipt"]["counts"]["UNKNOWN_SOURCE"], 1)
                self.assertEqual(len(export["source_rows"]), 1)

    def test_complete_empty_population_does_not_invent_evidence(self):
        data = fixture()
        data["source_rows"], data["candles"] = [], []
        self.anchor, self.sources, self.candles = manifest_fixture(data)
        export = self.assemble()
        manifest.validate_export_binding(export)
        self.assertEqual(export["source_rows"], [])
        self.assertEqual(export["candles"], [])
        self.assertEqual(export["source_receipt"]["expected_accepted_rows"], 0)


if __name__ == "__main__":
    unittest.main()
