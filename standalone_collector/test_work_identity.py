"""Offline foundation checks. TESTA–TESTH are not supported market symbols."""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError, replace
import json
from pathlib import Path
import subprocess
import sys
import unittest

from work_identity import WorkSpec, build_batch


TENANT = "11111111-1111-4111-8111-111111111111"
SOURCE = "22222222-2222-4222-8222-222222222222"
BATCH = "33333333-3333-4333-8333-333333333333"
OTHER = "44444444-4444-4444-8444-444444444444"
CONFIG = "a" * 64
IMAGE = "b" * 64
ROWS = "c" * 64
COINS = tuple("TEST" + letter for letter in "ABCDEFGH")


def spec():
    return WorkSpec(TENANT, SOURCE, BATCH, "TESTA", 1, "12H", CONFIG)


def batch(coins=COINS):
    return build_batch(tenant_id=TENANT, source_scope_id=SOURCE, batch_id=BATCH,
                       coins=coins, config_digest=CONFIG)


class WorkIdentityTests(unittest.TestCase):
    def test_72_distinct_commands_and_exact_coverage(self):
        jobs = batch()
        self.assertEqual(len(jobs), 72)
        self.assertEqual(len({j.work_id for j in jobs}), 72)
        self.assertEqual({(j.coin, j.heatmap_model, j.timeframe) for j in jobs},
                         {(c, m, t) for c in COINS for m in (1, 2, 3)
                          for t in ("12H", "24H", "48H")})

    def test_repeated_and_reordered_requests_keep_identity(self):
        self.assertEqual(batch(), batch())
        self.assertEqual({j.work_id for j in batch()},
                         {j.work_id for j in batch(tuple(reversed(COINS)))})

    def test_each_identity_dimension_separates_work(self):
        original = spec()
        for change in ({"tenant_id": OTHER}, {"source_scope_id": OTHER},
                       {"batch_id": OTHER}, {"coin": "TESTB"},
                       {"heatmap_model": 2}, {"timeframe": "24H"}):
            with self.subTest(change=change):
                changed = replace(original, **change)
                self.assertNotEqual(original.work_id, changed.work_id)
                with self.assertRaises(ValueError):
                    original.require_same_command(changed.to_record())

    def test_config_change_is_conflict_not_a_second_job(self):
        original = spec()
        changed = replace(original, config_digest="d" * 64)
        self.assertEqual(original.work_id, changed.work_id)
        with self.assertRaises(ValueError):
            original.require_same_command(changed.to_record())

    def test_commands_are_immutable_and_records_are_detached(self):
        original = spec()
        with self.assertRaises(FrozenInstanceError):
            original.coin = "TESTB"
        record = original.to_record()
        record["coin"] = "TESTB"
        self.assertEqual(original.coin, "TESTA")

    def test_restart_json_round_trip(self):
        payload = json.loads(json.dumps([j.to_record() for j in batch()]))
        self.assertEqual(tuple(map(WorkSpec.from_record, payload)), batch())

    def test_a_new_interpreter_derives_same_identity(self):
        code = "import json,sys; from work_identity import WorkSpec; print(WorkSpec.from_record(json.load(sys.stdin)).work_id)"
        result = subprocess.run([sys.executable, "-I", "-c",
            "import sys; sys.path.insert(0,sys.argv[1]); " + code,
            str(Path(__file__).resolve().parent)],
            input=json.dumps(spec().to_record()), text=True, capture_output=True,
            check=True, timeout=10)
        self.assertEqual(result.stdout.strip(), spec().work_id)

    def test_parallel_generation_has_no_mutable_shared_coin(self):
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda _: batch(), range(16)))
        self.assertTrue(all(jobs == batch() for jobs in results))
        # These are construction checks, not database claim or scan tests.

    def test_duplicate_normalized_coins_are_rejected(self):
        with self.assertRaises(ValueError):
            batch(("TESTA", "testa"))

    def test_explicit_coin_list_is_required_and_bounded(self):
        for coins in ((), "BTC", ["BTC"], tuple("T" + str(i) for i in range(9))):
            with self.subTest(coins=coins), self.assertRaises(ValueError):
                batch(coins)
        self.assertEqual(len(batch(("testa",))), 9)
        self.assertEqual(batch(("testa",))[0].coin, "TESTA")

    def test_symbols_cannot_carry_paths_or_unicode_aliases(self):
        for coin in ("", " BTC", "BTC ", "BTC/USDT", "BTC&coin=ETH",
                     "../BTC", "ＢＴＣ", "A" * 21, None):
            with self.subTest(coin=coin), self.assertRaises(ValueError):
                replace(spec(), coin=coin)

    def test_wrong_types_and_unsupported_models_and_timeframes(self):
        for model in (True, 1.0, "1", 0, 4):
            with self.subTest(model=model), self.assertRaises(ValueError):
                replace(spec(), heatmap_model=model)
        for timeframe in ("6H", "12h", "12H ", None, 12):
            with self.subTest(timeframe=timeframe), self.assertRaises(ValueError):
                replace(spec(), timeframe=timeframe)

    def test_invalid_scope_and_batch_ids_are_rejected(self):
        for name in ("tenant_id", "source_scope_id", "batch_id"):
            for value in ("not-a-uuid", "00000000-0000-0000-0000-000000000000", None):
                with self.subTest(name=name, value=value), self.assertRaises(ValueError):
                    replace(spec(), **{name: value})

    def test_forged_identifier_or_record_fields_are_rejected(self):
        original = spec().to_record()
        changed = [{**original, "work_id": OTHER},
                   {**original, "schema": "old"},
                   {**original, "coin": "TESTB"},
                   {**original, "api_key": "synthetic-not-a-secret"},
                   {k: v for k, v in original.items() if k != "batch_id"}]
        for record in changed:
            with self.subTest(record=record), self.assertRaises(ValueError):
                WorkSpec.from_record(record)

    def test_same_image_in_another_job_cannot_share_analysis_key(self):
        keys = {j.analysis_key(IMAGE, ROWS) for j in batch()}
        self.assertEqual(len(keys), 72)

    def test_analysis_binding_changes_with_any_input(self):
        original = spec()
        base = original.analysis_key(IMAGE, ROWS)
        self.assertEqual(base, original.analysis_key(IMAGE, ROWS))
        self.assertNotEqual(base, original.analysis_key("d" * 64, ROWS))
        self.assertNotEqual(base, original.analysis_key(IMAGE, "d" * 64))
        self.assertNotEqual(base, replace(original, config_digest="d" * 64)
                            .analysis_key(IMAGE, ROWS))

    def test_invalid_digests_are_rejected(self):
        for value in ("", "A" * 64, "g" * 64, "a" * 63, None):
            with self.subTest(value=value), self.assertRaises(ValueError):
                replace(spec(), config_digest=value)
            with self.subTest(image=value), self.assertRaises(ValueError):
                spec().analysis_key(value, ROWS)
            with self.subTest(rows=value), self.assertRaises(ValueError):
                spec().analysis_key(IMAGE, value)


if __name__ == "__main__":
    unittest.main()
