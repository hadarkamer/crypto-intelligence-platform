"""Exercise period filtering and outcome-blind cohorts through the store SQL.

SQLite executes the relational selection. Only the PostgreSQL outcome JSON
projection is replaced; the period, membership, bounds, window and ordering
clauses execute unchanged. Production migration requires PostgreSQL validation.
"""
from datetime import datetime, timezone, timedelta
import re
import sqlite3

import research_formula_ordered_store as store
from research_formula_ordered_store_selftest import Capture


class RelationalStore:
    def __init__(self):
        self.db=sqlite3.connect(':memory:')
        self.db.row_factory=sqlite3.Row
        self.db.executescript('''
            CREATE TABLE research_ordered_formula_matches(candidate_key TEXT,event_id INTEGER,symbol TEXT,direction TEXT,alert_time_utc TEXT,snapshot_id TEXT);
            CREATE TABLE research_event_btc_movements(event_id INTEGER,episode_policy_version TEXT,btc_parent_movement_id TEXT,membership_status TEXT);
            CREATE TABLE research_btc_parent_movements(btc_parent_movement_id TEXT,episode_policy_version TEXT,start_time_utc TEXT,evidence_eligible BOOLEAN);
            CREATE TABLE research_ordered_first_touch_outcomes(event_id INTEGER,window_minutes INTEGER,threshold_bps INTEGER,method_version TEXT);
            CREATE TABLE research_events(event_id INTEGER,event_kind TEXT,delivery_status TEXT,direction TEXT,alert_time_utc TEXT);
            CREATE TABLE research_ordered_formula_event_checks(event_id INTEGER);
            CREATE TABLE research_ordered_feature_screens(event_id INTEGER,feature_version TEXT);
        ''')

    def execute(self,sql,params):
        assert sql.count('%s')==len(params),(sql,params)
        # Keep both horizon/threshold bind positions. The real outcome left
        # join is retained so outcome availability cannot affect selection.
        sql=re.sub(r"CASE WHEN o.event_id IS NULL THEN jsonb_build_object\('window_minutes',%s::integer,'threshold_bps',%s::integer\)\s+ELSE to_jsonb\(o\)-'calculation_audit'-'threshold_policy' END", "json_object('window_minutes',%s,'threshold_bps',%s)",sql)
        sql=sql.replace('::integer','').replace('%s','?')
        return self.db.execute(sql,[x.isoformat() if isinstance(x,datetime) else x for x in params])

    def event(self,event_id,time,wave,start,*,member='LIVE',outcome=True,checked=True):
        self.db.execute('INSERT INTO research_ordered_formula_matches VALUES(?,?,?,?,?,?)',('STRICT_TRIPLE_TOTAL_65',event_id,'BTC','LONG',time.isoformat(),str(event_id)))
        self.db.execute('INSERT INTO research_events VALUES(?,?,?,?,?)',(event_id,'ALERT','DELIVERED','LONG',time.isoformat()))
        if checked:
            self.db.execute('INSERT INTO research_ordered_formula_event_checks VALUES(?)',(event_id,))
            self.db.execute('INSERT INTO research_ordered_feature_screens VALUES(?,?)',(event_id,store.questions.VERSION))
        if member is not None:
            self.db.execute('INSERT INTO research_event_btc_movements VALUES(?,?,?,?)',(event_id,store.PARENT_POLICY,wave,member))
        if not self.db.execute('SELECT 1 FROM research_btc_parent_movements WHERE btc_parent_movement_id=?',(wave,)).fetchone():
            self.db.execute('INSERT INTO research_btc_parent_movements VALUES(?,?,?,?)',(wave,store.PARENT_POLICY,start.isoformat(),member=='LIVE'))
        if outcome:
            self.db.execute('INSERT INTO research_ordered_first_touch_outcomes VALUES(?,?,?,?)',(event_id,60,25,store.evaluator.METHOD_VERSION))


def run():
    all_period,recent=list(store.PERIODS)
    cutoff=store.PERIODS[recent]
    assert cutoff==datetime(2026,9,3,21,tzinfo=timezone.utc)
    assert store.SOURCE_START_UTC==datetime(2026,8,15,21,tzinfo=timezone.utc)
    base={'candidate_key':'STRICT_TRIPLE_TOTAL_65','symbol':'ALL','direction':'LONG','window_minutes':60,'threshold_bps':25}
    scopes=[{**base,'period_key':key,'period_start_utc':value} for key,value in store.PERIODS.items()]
    capture=Capture()
    store.register_scopes(capture,[{'formula_id':base['candidate_key']}],{'BTC'},directions=['LONG'],include_all=False)
    records=[params for _,params in capture.calls]
    assert len(records)==64 and len({record[0] for record in records})==64
    assert {record[-2] for record in records}==set(store.PERIODS)
    assert len({store.scope_formula_version(scope) for scope in scopes})==2
    for invalid in ({}, {'period_key':'LEGACY_UNSCOPED'}, {'period_key':recent,'period_start_utc':cutoff+timedelta(hours=1)}):
        try:
            store.period_contract(invalid)
        except ValueError:
            pass
        else:
            raise AssertionError('Invalid period accepted')
    db=RelationalStore()
    # Crossing wave has an alert before and after the recent cutoff. Neither
    # alert may become a new independent period wave or a later entry choice.
    db.event(1,cutoff-timedelta(minutes=30),'cross',cutoff-timedelta(hours=2))
    db.event(2,cutoff+timedelta(minutes=30),'cross',cutoff-timedelta(hours=2))
    # Earliest simultaneous matches must survive even with no outcome.
    db.event(3,cutoff+timedelta(hours=2),'inside',cutoff+timedelta(hours=1),outcome=False)
    db.event(4,cutoff+timedelta(hours=2),'inside',cutoff+timedelta(hours=1))
    db.event(5,cutoff+timedelta(hours=3),'inside',cutoff+timedelta(hours=1))
    # An event exactly at midnight belongs to the new period inclusively.
    db.event(6,cutoff,'boundary',cutoff)
    now=cutoff+timedelta(days=1)
    for scope,expected in zip(scopes,({1,3,4,6},{3,4,6})):
        rows,truncated=store.load_scope_rows(db,scope,now=now)
        assert not truncated
        assert {row['event_id'] for row in rows}==expected
        assert store.membership_complete(db,scope,now=now)
    coverage=store.period_coverage(db,scopes[1],now=now)
    assert coverage['matching_alert_count']==5
    assert coverage['boundary_excluded_alerts']==1 and coverage['boundary_excluded_waves']==1
    assert coverage['matching_snapshot_count']==5
    # Future alerts are not an available source or an unresolved missing row.
    db.event(7,now+timedelta(minutes=1),'future',now,member=None,checked=False)
    assert store.membership_complete(db,scopes[1],now=now)
    assert store.source_population_complete(db,now=now,period_key=recent)
    # A new source older than the recent period blocks only the ALL period.
    db.event(8,cutoff-timedelta(days=1),'old',cutoff-timedelta(days=2),checked=False)
    assert not store.source_population_complete(db,now=now,period_key=all_period)
    assert store.source_population_complete(db,now=now,period_key=recent)
    # Explicit BTC_DATA_MISSING is incomplete just like an absent membership.
    db.event(9,cutoff+timedelta(hours=5),'missing',cutoff,member='BTC_DATA_MISSING')
    assert not store.membership_complete(db,scopes[1],now=now)
    assert store.period_coverage(db,scopes[1],now=now)['missing_membership_alerts']==1
    # Every source row remains present; filtering affects formula evidence only.
    assert db.db.execute('SELECT COUNT(*) FROM research_events').fetchone()[0]==9
    # Both periods overlap while FRESH is still exactly the original 14 days.
    assert store.evaluator.FRESH_DAYS==14
    print('ordered formula periods: cutoff, whole-wave boundary, overlap, identity, missing/future coverage, retained source PASS')


if __name__=='__main__':
    run()
