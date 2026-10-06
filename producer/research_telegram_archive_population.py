"""Read-only, exact-ancestor replacement for the reviewed HYPE supplement."""
from contextlib import contextmanager, ExitStack, closing
from collections import Counter
import json
import sqlite3

from research_telegram_archive_backfill import canonical
import research_telegram_archive_runtime_importer as importer
import research_telegram_archive_hype_supplement as hype
from research_archive_price_policy import SPOT_SOURCE, MARK_SOURCE


def _contract(conn, run_key):
    row = conn.execute("SELECT contract_json FROM archive_reconstruction_runs WHERE run_key=?", (run_key,)).fetchone()
    if row is None:
        raise ValueError("Archive run is missing")
    contract = json.loads(row[0])
    importer.validate_contract(contract, run_key)
    return contract


@contextmanager
def population(database, *, run_key, expected_sha256=None, supplement_database=None,
        supplement_run_key=None, supplement_sha256=None):
    supplemental = (supplement_database, supplement_run_key, supplement_sha256)
    if any(supplemental) and (not all(supplemental) or not expected_sha256):
        raise ValueError("Supplement union requires both reviewed hashes and exact run keys")
    with ExitStack() as stack:
        base = stack.enter_context(closing(importer.open_reviewed_artifact(database, expected_sha256))) if expected_sha256 else stack.enter_context(closing(sqlite3.connect(database.resolve().as_uri()+"?mode=ro&immutable=1", uri=True)))
        base.row_factory = sqlite3.Row
        contract = _contract(base, run_key)
        if contract["backfill_version"] != importer.BACKFILL_VERSION:
            raise ValueError("Base population must be the original Spot run")
        events = {}
        for row in base.execute("SELECT * FROM archive_reconstructed_events WHERE run_key=? AND reconstruction_status='READY_FOR_SPOT_ENTRY_PATH' ORDER BY source_time_utc,event_key", (run_key,)):
            event = importer._event_record(row, contract=contract)["event_payload"]
            if event.get("calculation_status") != row["calculation_status"]:
                raise ValueError("Archive calculation status disagrees with payload")
            events[row["event_key"]] = event
        original_counts = dict(Counter(event.get("calculation_status", "PENDING") for event in events.values()))
        locations = {key: (base, run_key, key) for key in events}
        source_names = {key: SPOT_SOURCE for key in events}
        replacements = set()
        checked_labels = checked_metrics = 0
        sources = [{"run_key": run_key, "artifact_sha256": expected_sha256, "price_source": SPOT_SOURCE}]
        if all(supplemental):
            supplement = stack.enter_context(closing(importer.open_reviewed_artifact(supplement_database, supplement_sha256)))
            supplement_contract = _contract(supplement, supplement_run_key)
            if (supplement_contract.get("backfill_version") != hype.BACKFILL_VERSION
                    or supplement_contract.get("base_run_key") != run_key
                    or supplement_contract.get("base_artifact_sha256") != expected_sha256
                    or supplement_contract.get("prepared_stage_digest") != contract["prepared_stage_digest"]):
                raise ValueError("HYPE supplement belongs to a different original archive")
            parents = {row[0]: json.loads(row[1]) for row in base.execute("SELECT btc_parent_movement_id,parent_json FROM archive_btc_parents WHERE run_key=?", (run_key,))}
            supplement_parents = {row[0]: json.loads(row[1]) for row in supplement.execute("SELECT btc_parent_movement_id,parent_json FROM archive_btc_parents WHERE run_key=?", (supplement_run_key,))}
            if parents != supplement_parents:
                raise ValueError("HYPE supplement changed the original BTC parent evidence")
            for row in supplement.execute("SELECT * FROM archive_reconstructed_events WHERE run_key=? ORDER BY source_time_utc,event_key", (supplement_run_key,)):
                record = importer._event_record(row, contract=supplement_contract)
                event = record["event_payload"]
                key = event["base_archive_event_key"]
                if key in replacements or key not in events:
                    raise ValueError("Duplicate or missing original HYPE ancestor")
                expected = hype.derive_event(events[key], base_run_key=run_key)
                measured_fields = {"entry_price", "entry_price_source", "calculation_status"}
                if (set(expected) != set(event) or any(canonical(expected[k]) != canonical(event[k]) for k in expected if k not in measured_fields)):
                    raise ValueError("HYPE supplement changed original source features, time or BTC membership")
                if row["calculation_status"] != "COMPLETE_64_LABELS":
                    raise ValueError("HYPE supplement still contains incomplete events")
                labels = [importer._outcome_record(child, record) for child in supplement.execute("SELECT * FROM archive_delayed_entry_outcomes WHERE run_key=? AND event_key=?", (supplement_run_key, row["event_key"]))]
                metrics = [importer._metric_record(child, record) for child in supplement.execute("SELECT * FROM archive_common_window_metrics WHERE run_key=? AND event_key=?", (supplement_run_key, row["event_key"]))]
                if len(labels) != 64 or len(metrics) != 8:
                    raise ValueError("HYPE supplement is missing outcome or metric cells")
                checked_labels += len(labels)
                checked_metrics += len(metrics)
                replacements.add(key)
                events[key] = event
                locations[key] = supplement, supplement_run_key, row["event_key"]
                source_names[key] = MARK_SOURCE
            if not replacements or len(replacements) != original_counts.get("UNSUPPORTED_SPOT_SYMBOL", 0):
                raise ValueError("HYPE supplement does not exactly replace the unsupported source population")
            sources.append({"run_key": supplement_run_key, "artifact_sha256": supplement_sha256, "price_source": MARK_SOURCE})
        def children(key):
            conn, current_run, event_key = locations[key]
            labels = [(row[0], row[1], row[2], json.loads(row[3])) for row in conn.execute("SELECT signal_variant,window_minutes,threshold_bps,outcome_json FROM archive_delayed_entry_outcomes WHERE run_key=? AND event_key=?", (current_run, event_key))]
            metrics = [(row[0], row[1], json.loads(row[2])) for row in conn.execute("SELECT signal_variant,window_minutes,metrics_json FROM archive_common_window_metrics WHERE run_key=? AND event_key=?", (current_run, event_key))]
            return labels, metrics
        report = {"sources": sources, "original_calculation_counts": original_counts,
            "effective_calculation_counts": dict(Counter(event.get("calculation_status", "PENDING") for event in events.values())),
            "hype_ancestors_replaced": len(replacements), "effective_unique_source_messages": len(events),
            "duplicate_supplement_messages_added": 0, "validated_supplement_labels": checked_labels,
            "validated_supplement_metrics": checked_metrics, "original_artifact_modified": False,
            "source_scope": "ARCHIVE_ONLY", "live_union_eligible": False, "production_rows_written": 0}
        yield events, children, source_names, report
