"""Independent native HYPE perpetual TRADE remeasurement; never MARK or Spot.

The original alert is retained. Entry uses the next complete minute's actual
Hyperliquid HYPE perpetual trade OPEN. Its results occupy separate PERP tables.
"""
import argparse
import json
import os

import hyperliquid_perp_price_path as provider
import research_native_hype_mark_supplement as shared

ADAPTER_VERSION = "native-hype-perp-derived-outcomes-v1"
ENTRY_VERSION = "native-hype-next-full-minute-perp-open-v1"
METRIC_VERSION = "native-hype-common-window-perp-trade-1m-v1"
SOURCE_SCOPE = "DERIVED_NATIVE_HYPE_PERP"
SOURCE = provider.SOURCE


def derive_measurement(event, membership, path_result, *, observed_at):
    return shared.derive_measurement(event, membership, path_result,
        observed_at=observed_at, adapter_version=ADAPTER_VERSION)


def derive_entry(event, membership, path_result):
    return shared.derive_entry(event, membership, path_result, adapter_version=ADAPTER_VERSION)


def write_measurement(conn, measurement):
    if measurement["event"].get("adapter_version") != ADAPTER_VERSION:
        raise ValueError("PERP adapter cannot persist a different price contract")
    return shared.write_measurement(conn, measurement)


def run(*, database_url, event_ids, observed_at=None, fetch_candles=provider.fetch_closed_candles):
    return shared.run(database_url=database_url, event_ids=event_ids, observed_at=observed_at,
        fetch_candles=fetch_candles, adapter_version=ADAPTER_VERSION)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event-ids", required=True)
    parser.add_argument("--observed-at")
    args = parser.parse_args()
    url = os.environ.get("RESEARCH_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not url:
        raise SystemExit("RESEARCH_DATABASE_URL or DATABASE_URL is required")
    print(json.dumps(run(database_url=url, event_ids=[int(value) for value in args.event_ids.split(",")],
        observed_at=args.observed_at), ensure_ascii=False))
