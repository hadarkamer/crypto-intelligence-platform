"""Focused regressions for resumable, tail-safe CoinGlass CVD frontfill."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
import io
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import patch

import coinglass_flow_foundation as foundation


def _milliseconds(value: datetime) -> int:
    return int(value.timestamp() * 1000)


def _grid_rows(
    start: datetime,
    end: datetime,
    *,
    buy: float = 10.0,
    sell: float = 1.0,
):
    rows = {}
    candle = start
    while candle < end:
        rows[_milliseconds(candle)] = (buy, sell, buy - sell, 0.0)
        candle += timedelta(minutes=30)
    return rows


class FlowFrontfillTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tempdir.name) / "frontfill.db")
        self.old_schema_key = foundation._SCHEMA_INITIALIZED_FOR
        foundation._SCHEMA_INITIALIZED_FOR = None
        self.database_patch = patch.object(foundation, "DATABASE_URL", "")
        self.path_patch = patch.object(foundation, "DB_PATH", self.db_path)
        self.database_patch.start()
        self.path_patch.start()
        foundation.init_db()
        current = datetime.now(timezone.utc)
        self.request_end = current.replace(
            minute=30 if current.minute >= 30 else 0,
            second=0,
            microsecond=0,
        )

    def tearDown(self) -> None:
        self.path_patch.stop()
        self.database_patch.stop()
        foundation._SCHEMA_INITIALIZED_FOR = self.old_schema_key
        self.tempdir.cleanup()

    def _seed_tail(self, times: list[datetime]) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.executemany(
                """
                INSERT INTO futures_taker_history
                (symbol,candle_time,buy_volume_usd,sell_volume_usd,
                 api_cum_vol_delta_usd,continuous_cum_vol_delta_usd,
                 exchange_list,source,imported_at)
                VALUES (?,?,?,?,?,?,?,?,?)
                """,
                [
                    (
                        "BTC", candle.isoformat(), 20.0, 5.0, 123.0, 999.0,
                        foundation.EXCHANGE_LIST, "seeded_tail", "2001-01-01T00:00:00+00:00",
                    )
                    for candle in times
                ],
            )
            conn.commit()

    def _rows(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            return conn.execute(
                "SELECT * FROM futures_taker_history "
                "WHERE symbol='BTC' ORDER BY candle_time"
            ).fetchall()

    def _clear(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM futures_taker_history")
            conn.commit()

    def test_frontfill_is_half_open_idempotent_and_preserves_tail_refresh(self) -> None:
        old_min = self.request_end - timedelta(days=1)
        old_max = old_min + timedelta(minutes=30)
        self._seed_tail([old_min, old_max])
        calls = []

        def fetch_prefix(symbol, market, start, end):
            calls.append((start, end))
            # The provider may return the requested end. Its deliberately bad
            # values must never overwrite the adjacent stored candle.
            rows = _grid_rows(start, end)
            rows[_milliseconds(end)] = (900.0, 1.0, 900.0, 0.0)
            return rows, 1

        with patch.object(foundation, "CHUNK_DAYS", 1), patch.object(
            foundation, "_fetch_chunk", side_effect=fetch_prefix
        ), redirect_stdout(io.StringIO()):
            result = foundation.frontfill_symbol(
                "BTC", "futures", days=3, now=self.request_end
            )

        requested_start = self.request_end - timedelta(days=3)
        self.assertTrue(result["ok"])
        self.assertFalse(result["skipped"])
        self.assertEqual(result["stored_rows"], 96)
        self.assertEqual(
            calls,
            [
                (self.request_end - timedelta(days=2), old_min),
                (requested_start, self.request_end - timedelta(days=2)),
            ],
        )
        rows = self._rows()
        self.assertEqual(len(rows), 98)
        self.assertEqual(len({row["candle_time"] for row in rows}), 98)
        tail = next(
            row
            for row in rows
            if foundation._as_utc(row["candle_time"]) == old_min
        )
        self.assertEqual(
            (tail["buy_volume_usd"], tail["sell_volume_usd"]),
            (20.0, 5.0),
        )
        self.assertEqual(tail["source"], "seeded_tail")
        self.assertEqual(tail["imported_at"], "2001-01-01T00:00:00+00:00")
        # Prefix deltas are 96 * 9; raw tail deltas remain 15 + 15.
        self.assertEqual(
            [row["continuous_cum_vol_delta_usd"] for row in rows[-2:]],
            [879.0, 894.0],
        )

        with patch.object(
            foundation, "_fetch_chunk", side_effect=AssertionError("must skip")
        ), redirect_stdout(io.StringIO()):
            repeated = foundation.frontfill_symbol(
                "BTC", "futures", days=3, now=self.request_end
            )
        self.assertTrue(repeated["skipped"])
        self.assertEqual(len(self._rows()), 98)

        tail_calls = []

        def fetch_tail(symbol, market, start, end):
            tail_calls.append((start, end))
            return {
                _milliseconds(start): (20.0, 5.0, 123.0, 0.0),
                _milliseconds(start + timedelta(minutes=30)): (
                    7.0, 2.0, 128.0, 0.0
                ),
            }, 1

        with patch.object(
            foundation, "_fetch_chunk", side_effect=fetch_tail
        ), redirect_stdout(io.StringIO()):
            foundation.backfill_symbol("BTC", "futures", days=3, force=True)
        self.assertTrue(tail_calls)
        self.assertEqual(tail_calls[0][0], old_max)

    def test_partial_failure_resumes_from_new_minimum_without_a_gap(self) -> None:
        old_min = self.request_end - timedelta(days=1)
        self._seed_tail([old_min])
        first_calls = []

        def fail_second_chunk(symbol, market, start, end):
            first_calls.append((start, end))
            if len(first_calls) == 2:
                raise RuntimeError("simulated provider failure")
            return _grid_rows(start, end, buy=4.0, sell=1.0), 1

        with patch.object(foundation, "CHUNK_DAYS", 1), patch.object(
            foundation, "_fetch_chunk", side_effect=fail_second_chunk
        ), redirect_stdout(io.StringIO()):
            failed = foundation.frontfill_symbol(
                "BTC", "futures", days=4, now=self.request_end
            )
        self.assertFalse(failed["ok"])
        self.assertEqual(failed["stored_rows"], 48)
        completed_range = first_calls[0]
        self.assertEqual(
            foundation.coverage("BTC", "futures")["min_time"],
            self.request_end - timedelta(days=2),
        )

        resume_calls = []

        def fetch_resume(symbol, market, start, end):
            resume_calls.append((start, end))
            return _grid_rows(start, end, buy=4.0, sell=1.0), 1

        with patch.object(foundation, "CHUNK_DAYS", 1), patch.object(
            foundation, "_fetch_chunk", side_effect=fetch_resume
        ), redirect_stdout(io.StringIO()):
            resumed = foundation.frontfill_symbol(
                "BTC", "futures", days=4, now=self.request_end
            )
        self.assertTrue(resumed["ok"])
        self.assertNotIn(completed_range, resume_calls)
        self.assertEqual(
            resume_calls,
            [
                (
                    self.request_end - timedelta(days=3),
                    self.request_end - timedelta(days=2),
                ),
                (
                    self.request_end - timedelta(days=4),
                    self.request_end - timedelta(days=3),
                ),
            ],
        )
        rows = self._rows()
        self.assertEqual(len(rows), 145)
        self.assertEqual(len({row["candle_time"] for row in rows}), 145)
        self.assertEqual(
            foundation.coverage("BTC", "futures")["min_time"],
            self.request_end - timedelta(days=4),
        )

    def test_sparse_or_empty_chunk_never_advances_durable_minimum(self) -> None:
        old_min = self.request_end - timedelta(days=1)
        requested_start = self.request_end - timedelta(days=2)
        missing_times = {
            "first": requested_start,
            "interior": requested_start + timedelta(hours=12),
            "last": old_min - timedelta(minutes=30),
        }
        cases = list(missing_times.items()) + [("empty", None)]

        for label, missing_time in cases:
            with self.subTest(label=label):
                self._clear()
                self._seed_tail([old_min])
                provider_rows = _grid_rows(requested_start, old_min)
                if missing_time is None:
                    provider_rows = {}
                else:
                    provider_rows.pop(_milliseconds(missing_time))

                with patch.object(foundation, "CHUNK_DAYS", 1), patch.object(
                    foundation, "_fetch_chunk", return_value=(provider_rows, 1)
                ), redirect_stdout(io.StringIO()):
                    result = foundation.frontfill_symbol(
                        "BTC", "futures", days=2, now=self.request_end
                    )

                self.assertFalse(result["ok"])
                self.assertEqual(result["stored_rows"], 0)
                self.assertIn("IncompleteHistoricalGridError", result["message"])
                self.assertEqual(len(self._rows()), 1)
                self.assertEqual(
                    foundation.coverage("BTC", "futures")["min_time"],
                    old_min,
                )

    def test_sparse_preexisting_min_is_not_mistaken_for_complete_history(self) -> None:
        old_min = self.request_end - timedelta(days=1)
        requested_start = self.request_end - timedelta(days=2)
        # Reproduce the old failure mode: one row at requested_start made MIN
        # look complete despite the 47 missing candles before the dense tail.
        self._seed_tail([requested_start, old_min])
        calls = []

        def fetch_complete(symbol, market, start, end):
            calls.append((start, end))
            return _grid_rows(start, end), 1

        with patch.object(foundation, "CHUNK_DAYS", 1), patch.object(
            foundation, "_fetch_chunk", side_effect=fetch_complete
        ), redirect_stdout(io.StringIO()):
            result = foundation.frontfill_symbol(
                "BTC", "futures", days=2, now=self.request_end
            )

        self.assertTrue(result["ok"])
        self.assertFalse(result["skipped"])
        self.assertEqual(result["stored_rows"], 47)
        self.assertEqual(calls, [(requested_start, old_min)])
        rows = self._rows()
        self.assertEqual(len(rows), 49)
        self.assertEqual(
            foundation._contiguous_stored_start(
                "BTC", "futures", requested_start
            ),
            requested_start,
        )
        # INSERT ... DO NOTHING preserves the preexisting boundary sample.
        first = rows[0]
        self.assertEqual(
            (first["buy_volume_usd"], first["sell_volume_usd"]),
            (20.0, 5.0),
        )
        self.assertEqual(first["source"], "seeded_tail")

    def test_prefix_insert_never_overwrites_tail_even_with_concurrent_writer(self) -> None:
        candle = self.request_end - timedelta(days=2)
        self._seed_tail([candle])
        foundation._store_prefix(
            "BTC",
            "futures",
            {_milliseconds(candle): (900.0, 1.0, 899.0, 0.0)},
        )
        protected = self._rows()[0]
        self.assertEqual(
            (protected["buy_volume_usd"], protected["sell_volume_usd"]),
            (20.0, 5.0),
        )
        self.assertEqual(protected["source"], "seeded_tail")
        self.assertEqual(
            protected["imported_at"], "2001-01-01T00:00:00+00:00"
        )

        for attempt in range(4):
            with self.subTest(concurrent_attempt=attempt):
                self._clear()
                barrier = threading.Barrier(2)

                def write_prefix():
                    barrier.wait()
                    return foundation._store_prefix(
                        "BTC",
                        "futures",
                        {_milliseconds(candle): (900.0, 1.0, 899.0, 0.0)},
                    )

                def write_live_tail():
                    barrier.wait()
                    return foundation._store(
                        "BTC",
                        "futures",
                        {_milliseconds(candle): (20.0, 5.0, 15.0, 0.0)},
                    )

                with ThreadPoolExecutor(max_workers=2) as executor:
                    futures = [
                        executor.submit(write_prefix),
                        executor.submit(write_live_tail),
                    ]
                    for future in futures:
                        future.result(timeout=5)

                rows = self._rows()
                self.assertEqual(len(rows), 1)
                self.assertEqual(
                    (rows[0]["buy_volume_usd"], rows[0]["sell_volume_usd"]),
                    (20.0, 5.0),
                )
                self.assertEqual(rows[0]["api_cum_vol_delta_usd"], 15.0)

    def test_advisory_lock_key_is_stable_and_series_scoped(self) -> None:
        btc = foundation._series_advisory_lock_id(
            "futures_taker_history", "BTC"
        )
        self.assertEqual(
            btc,
            foundation._series_advisory_lock_id(
                "futures_taker_history", "btc"
            ),
        )
        self.assertNotEqual(
            btc,
            foundation._series_advisory_lock_id(
                "spot_taker_history", "BTC"
            ),
        )
        self.assertNotEqual(
            btc,
            foundation._series_advisory_lock_id(
                "futures_taker_history", "ETH"
            ),
        )
        self.assertGreaterEqual(btc, 0)
        self.assertLessEqual(btc, 0x7FFF_FFFF_FFFF_FFFF)


if __name__ == "__main__":
    unittest.main()
