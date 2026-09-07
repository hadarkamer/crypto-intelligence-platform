"""Small durable registry and leased outbox, separate from legacy v6 delivery."""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any
from uuid import uuid4

import research_ordered_experimental as contract
import research_ordered_validation as validation
import research_ordered_validation_store as validation_store


def available(conn: Any) -> bool:
    return conn.execute("SELECT to_regclass('research_ordered_experimental_deliveries') IS NOT NULL AND to_regclass('research_ordered_experimental_eligibility') IS NOT NULL AS ready").fetchone()["ready"]


def publish_evaluation(conn: Any, scope: dict, result: dict, *, now: datetime,
                       rows: list[dict], common_window_rows: dict) -> bool:
    """Same transaction as immutable validation; no historical replay activation."""
    registered = conn.execute("SELECT registration FROM research_ordered_validation_freezes WHERE freeze_id=%s",
                              (result["freeze_id"],)).fetchone()
    published = conn.execute('SELECT clock_timestamp() AS now').fetchone()['now']
    expires = None
    try:
        expires = contract.qualification(registered["registration"], result, evaluated_at=now, now=published)["eligible_until_utc"]
    except (ValueError, TypeError, KeyError):
        pass
    if expires is None:
        prior = conn.execute('''SELECT a.*,v.result FROM research_ordered_experimental_eligibility a
            JOIN research_ordered_validation_evaluations v USING(freeze_id,evidence_sha256)
            WHERE a.freeze_id=%s AND a.ready AND a.eligible_until_utc>%s FOR UPDATE OF a''',
            (result['freeze_id'],published)).fetchone()
        if prior and pending_additions_only(prior['result'],result,published_at=prior['published_at_utc']):
            # New unresolved trigger waves cannot invalidate the historical
            # proof merely by lacking their own future horizon. Recheck every
            # old wave against current source/label/window rows before keeping
            # that proof, and never renew its original expiration.
            ids = {i for ep in prior['result']['episodes'] for i in ep['event_ids']}
            selected = [r for r in rows if r['event_id'] in ids]
            if {r['event_id'] for r in selected} == ids:
                prepared = validation_store._enrich_rows(conn,selected,registered['registration']['binding'])
                guarded = validation.evaluate(registered['registration'],prepared,analysis_as_of_utc=published,
                    source_coverage_complete=True,common_window_rows=common_window_rows,
                    registered_attempts=result['registered_attempts'])
                guarded['representative_entries_frozen'] = True
                try:
                    contract.qualification(registered['registration'],guarded,evaluated_at=now,now=published)
                    if (validation.canonical(guarded['episodes']) == validation.canonical(prior['result']['episodes'])
                            and validation.canonical(guarded['prospective']) == validation.canonical(prior['result']['prospective'])):
                        return True
                except (ValueError,TypeError,KeyError):
                    pass
    conn.execute("""INSERT INTO research_ordered_experimental_eligibility
        (freeze_id,scope_key,evidence_sha256,evaluated_at_utc,published_at_utc,eligible_until_utc,ready)
        VALUES(%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(freeze_id) DO UPDATE SET
        evidence_sha256=EXCLUDED.evidence_sha256,evaluated_at_utc=EXCLUDED.evaluated_at_utc,
        published_at_utc=EXCLUDED.published_at_utc,
        eligible_until_utc=EXCLUDED.eligible_until_utc,ready=EXCLUDED.ready
        WHERE research_ordered_experimental_eligibility.evaluated_at_utc<=EXCLUDED.evaluated_at_utc""",
        (result["freeze_id"], scope["scope_key"], result["evidence_sha256"], now, published, expires, expires is not None))
    return expires is not None


def pending_additions_only(prior: dict, latest: dict, *, published_at: datetime) -> bool:
    if (not latest.get('source_coverage_complete') or not latest.get('representative_entries_frozen')
            or latest.get('representative_conflicts') or latest.get('freeze_id') != prior.get('freeze_id')):
        return False
    old = {ep['btc_parent_movement_id']:ep for ep in prior['episodes']}
    new = {ep['btc_parent_movement_id']:ep for ep in latest['episodes']}
    if not old.keys() < new.keys() or any(validation.canonical(ep)!=validation.canonical(new[key]) for key,ep in old.items()):
        return False
    return all(ep['status'] in ('OPEN','DATA_MISSING') and ep.get('phase')=='PROSPECTIVE'
               and validation.evidence._utc(ep['forecast_start_utc'])>validation.evidence._utc(published_at)
               for key,ep in new.items() if key not in old)


def prioritize(conn: Any, scopes: list[dict], *, now: datetime, limit: int, candidate_keys) -> list[dict]:
    """Refresh at most eight qualified scopes without expanding worker budget."""
    due = conn.execute("""SELECT s.* FROM research_ordered_experimental_eligibility a
        JOIN research_ordered_formula_scopes s ON s.scope_key=a.scope_key
        WHERE a.ready AND a.evaluated_at_utc<=%s AND s.candidate_key=ANY(%s)
        ORDER BY a.evaluated_at_utc,a.freeze_id LIMIT %s""", (now-timedelta(minutes=5), list(candidate_keys), min(8,limit))).fetchall()
    seen = {s["scope_key"] for s in due}
    return [*due, *(s for s in scopes if s["scope_key"] not in seen)][:limit]


def enqueue(conn: Any, *, now: datetime, scope_limit: int = 8) -> dict[str, int]:
    summary = {"scopes_scanned": 0, "enqueued": 0, "blocked": 0}
    destinations = conn.execute("SELECT chat_id FROM research_formula_alert_subscriptions WHERE active ORDER BY chat_id LIMIT 20").fetchall()
    if not destinations:
        return summary
    active = conn.execute("""SELECT a.*,f.registration,v.result FROM research_ordered_experimental_eligibility a
        JOIN research_ordered_validation_freezes f USING(freeze_id)
        JOIN research_ordered_validation_evaluations v USING(freeze_id,evidence_sha256)
        WHERE a.ready AND a.eligible_until_utc>%s
        ORDER BY a.last_scanned_at_utc ASC NULLS FIRST,a.freeze_id LIMIT %s
        FOR UPDATE OF a SKIP LOCKED""", (now, min(32,max(1,scope_limit)))).fetchall()
    for a in active:
        b = a["registration"]["binding"]
        # Exact indexed predicate + recent bounded window, no outcomes needed.
        triggers = conn.execute("""SELECT m.*,e.event_kind,e.delivery_status,e.event_fingerprint,
            e.event_type,e.current_price,e.engine_snapshot,
            e.direction AS source_direction,'LIVE'::text AS source_scope,
            p.btc_parent_movement_id,p.membership_status,p.episode_policy_version,
            p.decision_time_utc,p.btc_observed_close_utc,
            w.evidence_eligible AS parent_evidence_eligible,w.start_time_utc AS parent_start_time_utc,
            w.confirmed_at_utc AS parent_confirmed_at_utc
            FROM research_ordered_formula_matches m JOIN research_events e ON e.event_id=m.event_id
            JOIN research_event_btc_movements p ON p.event_id=e.event_id AND p.episode_policy_version=%s
            JOIN research_btc_parent_movements w ON w.btc_parent_movement_id=p.btc_parent_movement_id
                AND w.episode_policy_version=p.episode_policy_version
            WHERE m.candidate_key=%s AND m.direction=%s AND (%s='ALL' OR m.symbol=%s)
              AND m.alert_time_utc>%s AND m.alert_time_utc>=%s AND m.alert_time_utc<=%s
              AND e.alert_time_utc=m.alert_time_utc AND e.symbol=m.symbol
              AND e.event_kind='ALERT' AND e.delivery_status='DELIVERED'
              AND p.membership_status='LIVE' AND w.evidence_eligible
            ORDER BY m.alert_time_utc,m.event_id LIMIT 32""",
            (b["parent_policy_version"],b["candidate_key"],b["direction"],b["symbol"],b["symbol"],
             a["published_at_utc"],now-contract.TRIGGER_TTL,now)).fetchall()
        for trigger in triggers:
            try:
                payload = contract.notification(a["registration"],a["result"],trigger,
                    evaluated_at=a["evaluated_at_utc"],published_at=a['published_at_utc'],now=now)
            except (ValueError,TypeError,KeyError):
                summary["blocked"] += 1
                continue
            for destination in destinations:
                added = conn.execute("""INSERT INTO research_ordered_experimental_deliveries
                    (freeze_id,evidence_sha256,candidate_key,event_id,btc_parent_movement_id,direction,chat_id,payload,expires_at_utc)
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s) ON CONFLICT DO NOTHING RETURNING delivery_id""",
                    (a["freeze_id"],a["evidence_sha256"],b["candidate_key"],trigger["event_id"],trigger["btc_parent_movement_id"],
                     b["direction"],destination["chat_id"],validation.canonical(payload),payload["expires_at_utc"])).fetchone()
                summary["enqueued"] += int(bool(added))
        conn.execute("UPDATE research_ordered_experimental_eligibility SET last_scanned_at_utc=%s WHERE freeze_id=%s",(now,a["freeze_id"]))
        summary["scopes_scanned"] += 1
    return summary


def claim(conn: Any, *, now: datetime) -> dict | None:
    # Reclaim only a claim known not to have entered a transport call. Once
    # SENDING, process death can mean Telegram accepted it; never auto-retry.
    conn.execute("""UPDATE research_ordered_experimental_deliveries SET
        status=CASE WHEN status='SENDING' THEN 'UNKNOWN' ELSE 'PENDING' END,
        last_error=CASE WHEN status='SENDING' THEN 'TRANSPORT_RESULT_UNKNOWN_AFTER_LEASE' ELSE last_error END,
        claim_token=NULL,lease_expires_at_utc=NULL
        WHERE delivery_id IN (SELECT delivery_id FROM research_ordered_experimental_deliveries
            WHERE status IN ('CLAIMED','SENDING') AND lease_expires_at_utc<=%s LIMIT 32 FOR UPDATE SKIP LOCKED)""",(now,))
    conn.execute("""UPDATE research_ordered_experimental_deliveries SET status='CANCELLED',last_error='EXPIRED_OR_REVOKED'
        WHERE delivery_id IN (SELECT d.delivery_id FROM research_ordered_experimental_deliveries d
            WHERE d.status='PENDING' AND (d.expires_at_utc<=%s OR NOT EXISTS(
                SELECT 1 FROM research_ordered_experimental_eligibility a JOIN research_formula_alert_subscriptions s ON s.chat_id=d.chat_id
                WHERE a.freeze_id=d.freeze_id AND a.evidence_sha256=d.evidence_sha256 AND a.ready AND a.eligible_until_utc>%s AND s.active))
            LIMIT 32 FOR UPDATE SKIP LOCKED)""", (now,now))
    return conn.execute("""WITH picked AS (
        SELECT d.delivery_id FROM research_ordered_experimental_deliveries d
        JOIN research_ordered_experimental_eligibility a ON a.freeze_id=d.freeze_id AND a.evidence_sha256=d.evidence_sha256
        JOIN research_formula_alert_subscriptions s ON s.chat_id=d.chat_id
        WHERE d.status='PENDING' AND d.expires_at_utc>%s AND a.ready AND a.eligible_until_utc>%s AND s.active
        ORDER BY d.created_at_utc,d.delivery_id LIMIT 1 FOR UPDATE OF d SKIP LOCKED)
        UPDATE research_ordered_experimental_deliveries d SET status='CLAIMED',claim_token=%s,
        lease_expires_at_utc=%s FROM picked WHERE d.delivery_id=picked.delivery_id RETURNING d.*""",
        (now,now,uuid4(),now+timedelta(seconds=90))).fetchone()


def begin_send(conn: Any, item: dict, *, now: datetime) -> bool:
    # Last check uses native event/active subscription/current qualification,
    # immediately before committing SENDING and crossing the network boundary.
    row = conn.execute("""UPDATE research_ordered_experimental_deliveries d
        SET status='SENDING',attempts=attempts+1 WHERE d.delivery_id=%s AND d.claim_token=%s
          AND d.status='CLAIMED' AND d.lease_expires_at_utc>%s AND d.expires_at_utc>%s
          AND EXISTS(SELECT 1 FROM research_ordered_experimental_eligibility a
             WHERE a.freeze_id=d.freeze_id AND a.evidence_sha256=d.evidence_sha256 AND a.ready AND a.eligible_until_utc>%s)
          AND EXISTS(SELECT 1 FROM research_formula_alert_subscriptions s WHERE s.chat_id=d.chat_id AND s.active)
          AND EXISTS(SELECT 1 FROM research_events e WHERE e.event_id=d.event_id AND e.event_kind='ALERT' AND e.delivery_status='DELIVERED')
        RETURNING delivery_id""",(item["delivery_id"],item["claim_token"],now,now,now)).fetchone()
    return bool(row)


def finish(conn: Any, item: dict, *, now: datetime, message_id: int | None, error: str | None = None) -> bool:
    sent = type(message_id) is int and message_id > 0 and error is None
    return bool(conn.execute("""UPDATE research_ordered_experimental_deliveries
        SET status=%s,sent_at_utc=%s,telegram_message_id=%s,last_error=%s,claim_token=NULL,lease_expires_at_utc=NULL
        WHERE delivery_id=%s AND claim_token=%s AND status='SENDING' RETURNING delivery_id""",
        ("SENT" if sent else "UNKNOWN",now if sent else None,message_id if sent else None,
         None if sent else (error or "TRANSPORT_DID_NOT_CONFIRM_MESSAGE_ID")[:500],item["delivery_id"],item["claim_token"])).fetchone())
