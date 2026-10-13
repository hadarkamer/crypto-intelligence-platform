"""Synthetic real-PostgreSQL scope hydration fixture; never connects by itself.

The caller owns the explicit loopback/test-database gate, disposable schema,
connection lifetime and process measurements. Preparation belongs in a separate
process. Measured passes keep no row/cursor/observation collections between calls.
This exercises loaders and pure calculations, not prospective registration,
publication, source ingestion, production cardinalities or trading qualification.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re
from typing import Any

VERSION = "ordered-postgres-scope-fixture-v1"
ROOT = Path(__file__).resolve().parents[1]
START = datetime(2026, 9, 8, tzinfo=timezone.utc)
AS_OF = datetime(2026, 10, 1, tzinfo=timezone.utc)
MIGRATIONS = (
    "001_research_archive_v1.sql", "020_ordered_first_touch_v7.sql",
    "021_btc_parent_movements_v1.sql", "022_ordered_formula_research.sql",
    "027_ordered_formula_research_periods.sql", "029_common_window_metrics.sql",
    "030_ordered_question_search.sql",
)
TABLES = (
    "research_events", "research_ordered_formula_matches",
    "research_event_btc_movements", "research_btc_parent_movements",
    "research_ordered_first_touch_outcomes", "research_common_window_metrics",
    "research_btc_price_bars",
)
LIMITATIONS = [
    "Synthetic engineering scale, not measured production object equivalence.",
    "One NORMAL BTC/LONG/60-minute/50bps scope; no inverse or truncation replay.",
    "Actual schema migrations, loaders, v7 and full-window calculators are used.",
    "Parent memberships are synthetic fixtures, not recalculated BTC movements.",
    "No validation registration, acceptance, persistence/publication or other workers.",
    "Each pass repeats scope calculations; it does not model the worker's unchanged-input shortcut.",
    "Parity hashing is measured in its own phase and adds observer allocation/CPU cost.",
    "Preparation reference arrays are released before the measured child starts.",
    "Local Python/PostgreSQL/libc versions may differ from production.",
]


def _imports():
    import research_formula_ordered_store as store
    import research_formula_ordered_v7 as evaluator
    import research_ordered_question_store as questions
    import research_ordered_first_touch as first_touch
    import research_common_window_metrics as common
    return store, evaluator, questions, first_touch, common


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, default=str, allow_nan=False)


def _digest(value):
    return hashlib.sha256(_canonical(value).encode()).hexdigest()


def _json_value(value):
    """The actual JSONB parameter serialization used for this fixture."""
    return json.loads(_canonical(value))


def _pg_json(value):
    """Reference for to_jsonb on typed SQL outcome columns, not loader output."""
    if isinstance(value, datetime):
        # PostgreSQL trims fractional trailing zeros in to_jsonb(timestamptz).
        return re.sub(r"(\.\d*?)0+(?=[+-]\d\d:\d\d$)",r"\1",value.isoformat())
    if isinstance(value, dict):
        return {key: _pg_json(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_pg_json(item) for item in value]
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return value


def _semantic(value):
    """Compare decoded values without conflating datetime spelling/JSON numbers."""
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, dict):
        return {str(key): _semantic(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_semantic(item) for item in value]
    if isinstance(value, str) and len(value) >= 19 and value[4:5] == "-" and value[10:11] in ("T", " "):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is not None:
                return parsed.astimezone(timezone.utc).isoformat()
        except ValueError:
            pass
    if type(value) is float and value.is_integer():
        return int(value)
    return value


def _guard(conn):
    row = conn.execute("SELECT current_schema() AS schema,current_database() AS database").fetchone()
    # An additional schema guard; it never substitutes for the caller's DSN gate.
    if not re.fullmatch(r"ordered_memory_replay_[0-9a-f]{24}",row["schema"]):
        raise ValueError("Scope fixtures require an explicit disposable replay schema")
    return dict(row)


def _features(ordinal, start):
    """Populate fields consumed by actual extraction and sequence calculations."""
    from research_formula_sequence_history_selftest import event
    store, evaluator, _, _, _ = _imports()
    family = ("MAX_PAIN", "MAGNET", "COMBINED_CONFIRMATION")[ordinal % 3]
    current = event(ordinal, start, symbol="BTC", direction="LONG", family=family)
    if family == "COMBINED_CONFIRMATION":
        current["event_type"] = family
        current["source_side"] = "SHORT"
    current.update(current_price=100.0, target_price=101.0,
                   code_version="synthetic-memory", strategy_version="synthetic-v1")
    snapshot = current["engine_snapshot"]
    snapshot.update(alert_side=current["source_side"], average_score_all_timeframes=72,
        opposite_average_score_all_timeframes=57, opposite_score=55,
        score_components={"directional_alignment":25,"target_proximity":12,
                          "cluster_confidence":8,"relative_gap":5},
        directional_scores_all_timeframes={side:{tf:70+i for i,tf in enumerate(
            ("15m","30m","1h","4h","12h","24h","1w","1m"))}
            for side in ("LONG","SHORT")},
        consensus_hits=5,consensus_total=6,gap_consensus_supporting=4,gap_consensus_total=6,
        near_share_pct=70,near_amount=700,far_amount=300,distance_pct=1,
        maxpain_confirmation={"status":"CONFIRMED"},
        magnet_confirmation={"status":"CONFIRMED"}, signal_count=3,
        normal_confirmations=["a","b"],strong_confirmations=["c"],
        high_scores=["d","e"],anomaly_setups=["f"],liquidity_imbalances=["g"])
    snapshot["magnet"].update(magnet_quality=75,liquidity_edge_pct=12,count=4,
                              spread_pct=0.2,average_target=101)
    for module in snapshot["market_evidence"]["modules"].values():
        module["score"] = 70 + ordinal % 10
    features = evaluator.extract_event_features(current) | store.questions.extended_features(current)
    prior = []
    for offset in (20, 10):
        old = event(ordinal+offset, start-timedelta(minutes=offset), symbol="BTC")
        old.update(current_price=99.9, target_price=101.0)
        prior.append((old, evaluator.extract_event_features(old) | store.questions.extended_features(old)))
    features.update(store.questions.sequence_features(current, features, prior))
    return features


def _calculations(rows, windows, scope, now, truncated=False, phase=None, *, full_parity=True):
    if type(full_parity) is not bool:
        raise ValueError("full_parity must be an explicit boolean")
    store, evaluator, questions, _, common = _imports()
    emit = phase or (lambda _name: None)
    emit("input_hash")
    input_hash = questions.evaluation_input(scope,
        [{**row,"common_window_record":windows.get(row["event_id"])} for row in rows],
        now=now,population_complete=True,membership_complete=True)
    emit("summary")
    summary = evaluator.summarize_scope(rows,analysis_as_of_utc=now,
        truncated=truncated,source_coverage_complete=True)
    emit("common_metrics")
    metrics = common.aggregate_common_window_metrics(windows.values(),
        expected_count=len(rows),window_minutes=scope["window_minutes"],
        threshold_bps=scope["threshold_bps"])
    result = {
        "loaded_rows":len(rows), "common_window_rows":len(windows),
        "truncated":bool(truncated), "evaluation_input_sha256":input_hash,
        "independent_waves":summary["independent_waves"],
        "successes":summary["successes"], "failures":summary["failures"],
        "excluded_waves":summary["excluded_waves"],
        "common_coverage_complete":metrics["coverage_complete"],
        "common_valid_representatives":metrics["valid_representatives"],
    }
    if full_parity:
        emit("parity_hashes")
        result.update({
            "decoded_rows_sha256":_digest(_semantic(rows)),
            "decoded_common_windows_sha256":_digest(_semantic(windows)),
            "summary_sha256":_digest(_semantic(summary)),
            "common_metrics_sha256":_digest(_semantic(metrics)),
        })
    emit(None)
    return result


def prepare_scope(conn, rows=256, *, case_name="scope"):
    """Create a new finite population and return JSON-serializable metadata."""
    if type(rows) is not int or not 1 <= rows <= 2000:
        raise ValueError("scope rows must be an integer in 1..2000")
    identity = _guard(conn)
    store, evaluator, _, first_touch, common = _imports()
    for name in MIGRATIONS:
        conn.execute((ROOT/"migrations"/name).read_text(),prepare=False)
    if any(conn.execute("SELECT count(*) AS n FROM "+table).fetchone()["n"] for table in TABLES):
        raise ValueError("Scope preparation refuses a populated fixture schema")
    candidate = next(c for c in store.register_catalog(conn) if c["formula_id"] == "PRICE_OI_TOTAL_65")
    scope = {"scope_key":store.digest([VERSION,case_name,rows]),
        "candidate_key":candidate["formula_id"],"symbol":"BTC","direction":"LONG",
        "window_minutes":60,"threshold_bps":50,"period_key":"SINCE_20260904",
        "period_start_utc":store.PERIODS["SINCE_20260904"].isoformat()}
    conn.execute("""INSERT INTO research_ordered_formula_scopes
        (scope_key,candidate_key,symbol,direction,window_minutes,threshold_bps,period_key,period_start_utc)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",tuple(scope.values()))
    columns = conn.execute("""SELECT column_name,data_type FROM information_schema.columns
        WHERE table_schema=current_schema() AND table_name='research_ordered_first_touch_outcomes'
        ORDER BY ordinal_position""").fetchall()
    names = [column["column_name"] for column in columns]
    json_columns = {column["column_name"] for column in columns if column["data_type"] == "jsonb"}
    from psycopg import sql
    insert_label = sql.SQL("INSERT INTO research_ordered_first_touch_outcomes ({}) VALUES ({})").format(
        sql.SQL(",").join(map(sql.Identifier,names)),
        sql.SQL(",").join(sql.SQL("%s::jsonb" if name in json_columns else "%s") for name in names))
    expected_rows, expected_windows = [], {}
    feature_min, feature_max, feature_bytes = None, 0, 0
    for ordinal in range(1,rows+1):
        start = START+timedelta(minutes=10*ordinal)
        end = start+timedelta(minutes=60)
        success = ordinal % 4 != 0
        candles = [{"open_time_utc":start+timedelta(minutes=minute),
            "close_time_utc":start+timedelta(minutes=minute,seconds=59,milliseconds=999),
            "open":100.0,"high":100.6 if success else 100.1,
            "low":99.9 if success else 99.4,"close":100.0} for minute in range(60)]
        label = first_touch.calculate_ordered_first_touch_outcome(reference_price=100,
            direction="LONG",event_time=start,candles=candles,threshold_pct=0.5,
            observation_closed=True,path_complete=True)
        label.update(event_id=ordinal,window_minutes=60,
            price_source="BINANCE_SPOT_TRADE_1M",market_pair="BTCUSDT",
            data_quality_status="VERIFIED_BINANCE_SPOT_1M_CLOSED_CANDLES",
            calculation_audit={"synthetic":True,"omitted_projection_payload":"x"*1024},
            threshold_policy={"synthetic":True},created_at_utc=START,updated_at_utc=START)
        outcome = {name:label.get(name) for name in names}
        conn.execute("""INSERT INTO research_events(event_id,schema_version,event_kind,event_type,
            alert_time_utc,symbol,direction,setup_key,event_fingerprint,strategy_version,code_version,
            runtime_session_id,delivery_status,current_price,target_price,score,engine_snapshot,created_at)
            VALUES(%s,'test','ALERT','PRICE_OI_ALERT',%s,'BTC','LONG',%s,%s,'test','test','test',
            'DELIVERED',100,101,75,'{}'::jsonb,%s)""",
            (ordinal,start,store.digest(["setup",ordinal]),store.digest(["event",ordinal]),START))
        wave = "synthetic-scope-wave-"+str(ordinal)
        parent_start = start-timedelta(minutes=1)
        conn.execute("""INSERT INTO research_btc_parent_movements(btc_parent_movement_id,
            episode_policy_version,start_time_utc,end_time_utc,confirmed_at_utc,direction,
            evidence_eligible,boundary_reason,observed_through_utc,price_source,state_json)
            VALUES(%s,%s,%s,%s,%s,'UP',TRUE,'CAUSAL_CLOSE_REVERSAL',%s,
            'BINANCE_SPOT_BTCUSDT_1M','{"synthetic":true}'::jsonb)""",
            (wave,store.PARENT_POLICY,parent_start,start+timedelta(minutes=8),parent_start,end))
        conn.execute("""INSERT INTO research_btc_price_bars
            (open_time_utc,close_time_utc,open,high,low,close)
            VALUES(%s,%s,100,100.1,99.9,100)""",
            (parent_start,start-timedelta(milliseconds=1)))
        conn.execute("""INSERT INTO research_event_btc_movements(event_id,episode_policy_version,
            btc_parent_movement_id,decision_time_utc,btc_observed_close_utc,membership_status)
            VALUES(%s,%s,%s,%s,%s,'LIVE')""",
            (ordinal,store.PARENT_POLICY,wave,start,start-timedelta(milliseconds=1)))
        features = _features(ordinal,start)
        feature_min = min(feature_min,len(features)) if feature_min is not None else len(features)
        feature_max = max(feature_max,len(features))
        feature_bytes += len(_canonical(features).encode())
        match = {"candidate_key":candidate["formula_id"],"event_id":ordinal,"symbol":"BTC",
            "direction":"LONG","alert_time_utc":start,"snapshot_id":"synthetic-snapshot-"+str(ordinal),
            "entry_price":100.0,"decision_features":features,"created_at_utc":START}
        conn.execute("""INSERT INTO research_ordered_formula_matches(candidate_key,event_id,symbol,
            direction,alert_time_utc,snapshot_id,entry_price,decision_features,created_at_utc)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s)""",
            tuple(_canonical(value) if key=="decision_features" else value for key,value in match.items()))
        conn.execute(insert_label,tuple(_canonical(outcome[name]) if name in json_columns else outcome[name] for name in names))
        metric = common.calculate_common_window_metrics(symbol="BTC",reference_price=100,
            direction="LONG",event_time=start,window_minutes=60,candles=candles,observed_at=end,
            path_result={"symbol":"BTC","exchange":"binance","market":"spot","pair":"BTCUSDT",
                         "interval":"1m","interval_seconds":60})
        metric["event_id"] = ordinal
        if metric["status"] != "READY":
            raise AssertionError("Synthetic full-window calculator failed")
        conn.execute("""INSERT INTO research_common_window_metrics(event_id,window_minutes,
            method_version,status,measurement_start_utc,window_end_utc,next_attempt_at_utc,result)
            VALUES(%s,60,%s,'READY',%s,%s,%s,%s::jsonb)""",
            (ordinal,common.METHOD_VERSION,start,end,end,_canonical(metric)))
        expected_rows.append({**match,"decision_features":_json_value(features),
            "btc_parent_movement_id":wave,"membership_status":"LIVE","parent_evidence_eligible":True,
            "parent_start_time_utc":parent_start,"first_match_time":start,"features_observed_at_utc":start,
            "episode_policy_version":store.PARENT_POLICY,"window_minutes":60,"threshold_bps":50,
            "ordered_outcome":_pg_json({key:value for key,value in outcome.items()
                if key not in ("calculation_audit","threshold_policy")})})
        expected_windows[ordinal] = {**_json_value(metric),"event_id":ordinal,"window_minutes":60,
            "method_version":common.METHOD_VERSION,"status":"READY",
            "measurement_start_utc":start,"window_end_utc":end}
    expected = _calculations(expected_rows,expected_windows,scope,AS_OF,full_parity=True)
    if expected["excluded_waves"] or expected["independent_waves"] != rows or not expected["common_coverage_complete"]:
        raise AssertionError("Reference fixture failed actual downstream validation")
    metadata = {"version":VERSION,"rows":rows,"scope":scope,"candidate":candidate,
        "as_of_utc":AS_OF.isoformat(),"identity":identity,"expected":expected,
        "decision_feature_fields_min":feature_min,"decision_feature_fields_max":feature_max,
        "decision_feature_json_utf8_bytes":feature_bytes,
        "migrations":{name:hashlib.sha256((ROOT/"migrations"/name).read_bytes()).hexdigest() for name in MIGRATIONS},
        "source_cardinality":{table:rows for table in TABLES},"limitations":LIMITATIONS}
    # Read through the real loader before accepting the independent reference.
    run_scope(conn,metadata,full_parity=True)
    return metadata


def run_scope(conn, metadata, phase=None, *, full_parity=True):
    """Actual PostgreSQL hydration and calculations; return metadata only."""
    if metadata.get("version") != VERSION:
        raise ValueError("Unknown scope fixture version")
    store, _, _, _, _ = _imports()
    emit = phase or (lambda _name: None)
    now = datetime.fromisoformat(metadata["as_of_utc"])
    emit("scope_rows")
    rows,truncated = store.load_scope_rows(conn,metadata["scope"],row_limit=metadata["rows"],now=now)
    emit("common_windows")
    windows = store.common_window_rows(conn,metadata["scope"],rows)
    result = _calculations(rows,windows,metadata["scope"],now,truncated,phase,full_parity=full_parity)
    differences = {key:{"expected":metadata["expected"].get(key),"actual":value}
                   for key,value in result.items()
                   if key not in metadata["expected"] or value != metadata["expected"][key]}
    if differences:
        raise AssertionError("Scope loader/reference parity failed: "+_canonical(differences))
    rows = windows = None
    return {**result,"reference_parity":True,"full_parity_verified":full_parity,"synthetic_only":True}


def verify(conn, metadata):
    identity = _guard(conn)
    counts = {table:conn.execute("SELECT count(*) AS n FROM "+table).fetchone()["n"] for table in TABLES}
    if identity != metadata["identity"] or counts != metadata["source_cardinality"]:
        raise AssertionError("Scope fixture identity or fixed source cardinality changed")
    return {"identity":identity,"source_cardinality":counts,"fixed_source_cardinality":True}


def prepare(conn, args):
    return prepare_scope(conn,int(args.rows))


def run_pass(conn, metadata, phase=None, *, full_parity=True):
    return run_scope(conn,metadata,phase,full_parity=full_parity)
