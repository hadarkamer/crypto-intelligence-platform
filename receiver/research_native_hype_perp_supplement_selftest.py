"""Separate perpetual contract, MARK isolation, original source and all thresholds."""
from copy import deepcopy
from datetime import timedelta
import unittest

import research_native_hype_mark_supplement as mark
import research_native_hype_perp_supplement as perp
from research_native_hype_mark_supplement_selftest import event, membership, path, ENTRY


def perp_path(minutes=1440):
    return {**perp.SOURCE, "candles": path(minutes)["candles"]}


class NativePerpTests(unittest.TestCase):
    def test_explicit_perp_source_and_original_reference(self):
        source, member = event(), membership()
        before = deepcopy((source, member))
        result = perp.derive_measurement(source, member, perp_path(), observed_at=ENTRY+timedelta(days=1))
        self.assertEqual((source, member), before)
        measured = result["event"]
        self.assertEqual(measured["entry_price"], 100)
        self.assertEqual(measured["original_reference_price"], 88)
        self.assertEqual(measured["source_scope"], "DERIVED_NATIVE_HYPE_PERP")
        self.assertEqual(measured["price_source_contract"], perp.SOURCE)
        self.assertFalse(measured["live_union_eligible"])
        self.assertEqual(len(result["outcomes"]), 32)
        self.assertEqual(len(result["metrics"]), 4)
        for row in result["outcomes"]:
            self.assertEqual(row["source_price_kind"], "TRADE")
            self.assertEqual(row["source_price_market"], "perpetual")
            self.assertEqual(row["source_price_pair"], "HYPE-PERP")
            self.assertEqual(row["entry_policy_version"], perp.ENTRY_VERSION)
        self.assertTrue(all(m["status"] == "READY" for m in result["metrics"]))

    def test_mark_and_perp_have_distinct_identity_and_reject_wrong_source(self):
        now = ENTRY+timedelta(days=1)
        a = mark.derive_measurement(event(), membership(), path(), observed_at=now)
        b = perp.derive_measurement(event(), membership(), perp_path(), observed_at=now)
        self.assertNotEqual(a["event"]["derived_measurement_id"], b["event"]["derived_measurement_id"])
        self.assertNotEqual(a["metrics"][0]["path_sha256"], b["metrics"][0]["path_sha256"])
        with self.assertRaises(ValueError): perp.derive_measurement(event(), membership(), path(), observed_at=now)
        with self.assertRaises(ValueError): mark.derive_measurement(event(), membership(), perp_path(), observed_at=now)
        with self.assertRaises(ValueError): mark._settings("unrecognized-price-policy")

    def test_open_and_missing_prefixes_stay_distinct(self):
        p = perp_path(60)
        result = perp.derive_measurement(event(), membership(), p, observed_at=ENTRY+timedelta(minutes=60))
        self.assertEqual([m["status"] for m in result["metrics"]], ["READY", "OPEN", "OPEN", "OPEN"])
        del p["candles"][30]
        result = perp.derive_measurement(event(), membership(), p, observed_at=ENTRY+timedelta(minutes=60))
        self.assertTrue(all(m["status"] == "DATA_MISSING" for m in result["metrics"]))

    def test_common_calculator_preserves_existing_mark_path_digest_and_labels(self):
        source = event()
        new = mark.derive_measurement(source, membership(), path(), observed_at=ENTRY+timedelta(days=1))
        internal = {"symbol":"HYPE", "entry_policy_version":mark.archive.ENTRY_VERSION,
            "price_source_contract":mark.archive.SOURCE, "entry_time_utc":ENTRY,
            "analysis_direction":source["direction"], "archive_event_key":new["event"]["derived_measurement_id"]}
        _, old_labels, old_metrics = mark.archive.calculate_event(internal,
            {b["open_time_utc"]:b for b in path()["candles"]}, observed_at=ENTRY+timedelta(days=1))
        for old, current in zip([x for x in old_labels if x["signal_variant"]=="NORMAL"], new["outcomes"]):
            self.assertEqual(old["outcome_id"], current["outcome_id"])
            self.assertEqual(old["path_sha256"], current["path_sha256"])
            self.assertEqual(old["status"], current["status"])
        for old,current in zip([x for x in old_metrics if x["signal_variant"]=="NORMAL"], new["metrics"]):
            self.assertEqual(old["path_sha256"],current["path_sha256"])
            self.assertEqual(old["mfe_pct"],current["mfe_pct"])
            self.assertEqual(old["mae_pct"],current["mae_pct"])


if __name__ == "__main__": unittest.main()
