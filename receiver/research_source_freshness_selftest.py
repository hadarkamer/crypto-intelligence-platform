"""Regression checks for the daily archive being misreported as a live outage."""
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

import research_source_freshness as audit


NOW = datetime(2026, 9, 9, 6, 35, tzinfo=timezone.utc)


def records():
    return [{"symbol": symbol, "checked_at": NOW, "collected_at": NOW,
             "price_fetched_at": NOW - timedelta(minutes=3),
             "oi_fetched_at": NOW - timedelta(minutes=3),
             "data_quality_status": "PASS",
             "price_source": "hyperliquid" if symbol == "HYPE" else "binance_spot",
             "oi_source": "coinglass_all",
             "candle_time": NOW - timedelta(hours=15),
             "imported_at": NOW - timedelta(hours=14)} for symbol in audit.SYMBOLS]


def completed(age_hours=14, **overrides):
    return {"completed_at": NOW - timedelta(hours=age_hours), "ok_count": 8,
            "total_count": 8, "source": "automatic_due", **overrides}


class SourceFreshnessTests(unittest.TestCase):
    def test_old_daily_candles_are_not_a_live_outage(self):
        report = audit.build_report(now=NOW, rows=records(), runs=[completed()])
        self.assertEqual(report["live_price_oi"]["fresh_count"], 8)
        self.assertEqual(report["live_price_oi"]["status"], "FRESH")
        self.assertEqual(report["historical_30m_backfill"]["schedule_status"], "NOT_DUE")
        self.assertEqual(report["historical_30m_backfill"]["symbols"]["BTC"]["candle_age_minutes"], 900)

    def test_recent_database_write_cannot_hide_stale_source(self):
        rows = records()
        rows[0]["price_fetched_at"] = NOW - timedelta(hours=2)
        report = audit.build_report(now=NOW, rows=rows, runs=[completed()])
        btc = report["live_price_oi"]["symbols"]["BTC"]
        self.assertEqual(btc["status"], "STALE")
        self.assertEqual(btc["oi_freshness"], "FRESH")
        self.assertEqual(report["live_price_oi"]["fresh_count"], 7)

    def test_missing_source_clock_never_falls_back_to_collection_clock(self):
        rows = records()
        rows[0]["price_fetched_at"] = None
        result = audit.build_report(now=NOW, rows=rows, runs=[])
        self.assertEqual(result["live_price_oi"]["symbols"]["BTC"]["status"], "SOURCE_TIME_UNKNOWN")

    def test_quality_missing_future_and_threshold_boundary(self):
        rows = records()
        rows[0]["price_fetched_at"] = NOW - timedelta(minutes=40)
        rows[1]["data_quality_status"] = "INVALID"
        rows[2]["oi_fetched_at"] = NOW + timedelta(minutes=2)
        rows = rows[:-1]
        states = audit.build_report(now=NOW, rows=rows, runs=[])["live_price_oi"]["symbols"]
        self.assertEqual(states["BTC"]["status"], "FRESH")
        self.assertEqual(states["ETH"]["status"], "QUALITY_NOT_PASS")
        self.assertEqual(states["SOL"]["status"], "SOURCE_TIME_IN_FUTURE")
        self.assertEqual(states["XRP"]["status"], "MISSING")

    def test_daily_due_vs_overdue_and_partial_run(self):
        for hours, expected in [(23, "NOT_DUE"), (24, "DUE_WITHIN_CHECK_INTERVAL"),
                                (25, "DUE_WITHIN_CHECK_INTERVAL"), (25.1, "OVERDUE")]:
            runs = [completed(0.1, ok_count=7), completed(hours)]
            report = audit.build_report(now=NOW, rows=records(), runs=runs)
            self.assertEqual(report["historical_30m_backfill"]["schedule_status"], expected)

    def test_no_success_or_wrong_target_count_does_not_advance_daily_clock(self):
        report = audit.build_report(now=NOW, rows=records(),
                                    runs=[completed(total_count=7, ok_count=7)])
        self.assertEqual(report["historical_30m_backfill"]["schedule_status"], "NO_SUCCESS_IN_RECENT_32_RUNS")

    def test_run_is_readonly_bounded_and_keeps_actual_hype_source(self):
        connection = MagicMock()
        connection.__enter__.return_value = connection
        connection.execute.side_effect = [SimpleNamespace(fetchall=lambda: records()),
                                          SimpleNamespace(fetchall=lambda: [completed()])]
        driver = SimpleNamespace(connect=MagicMock(return_value=connection))
        with patch.object(audit, "psycopg", driver), patch.dict(audit.os.environ, {}, clear=True):
            report = audit.run(database_url="private-not-printed")
        self.assertEqual(report["audit_status"], "OK")
        self.assertEqual(report["live_price_oi"]["symbols"]["HYPE"]["price_source"], "hyperliquid")
        self.assertEqual(connection.execute.call_count, 2)
        self.assertIn("default_transaction_read_only=on", driver.connect.call_args.kwargs["options"])
        self.assertIn("statement_timeout=3000", driver.connect.call_args.kwargs["options"])
        self.assertEqual(connection.execute.call_args_list[0].args[1], (list(audit.SYMBOLS),))

    def test_cached_health_never_reads_database_and_is_defensive(self):
        report = audit.build_report(now=NOW, rows=records(), runs=[completed()])
        with patch.object(audit, "_cache", report), patch.object(audit, "run", side_effect=AssertionError("no I/O")):
            first = audit.status(now=NOW)
            first["live_price_oi"]["symbols"].clear()
            self.assertEqual(len(audit.status(now=NOW)["live_price_oi"]["symbols"]), 8)
            self.assertFalse(audit.status(now=NOW + timedelta(seconds=300))["cache_stale"])
            self.assertTrue(audit.status(now=NOW + timedelta(seconds=301))["cache_stale"])

    def test_error_is_safe_and_refresh_does_not_keep_old_fresh_result(self):
        driver = SimpleNamespace(connect=MagicMock(side_effect=RuntimeError("secret://private-password")))
        with patch.object(audit, "psycopg", driver), patch.object(audit, "_cache", {"last_successful_check_at": NOW.isoformat()}):
            result = audit.run_refresh(database_url="private-not-printed")
            cached = audit.status()
        self.assertEqual(result["audit_status"], "ERROR")
        self.assertEqual(result["error_code"], "RuntimeError")
        self.assertNotIn("secret", str(cached))
        self.assertNotIn("live_price_oi", cached)
        self.assertEqual(cached["last_successful_check_at"], NOW.isoformat())


if __name__ == "__main__":
    unittest.main()
