"""Research fixtures: source semantics, threshold boundaries, missingness and frozen trials."""
from datetime import datetime,timezone,timedelta
import copy
import unittest
from unittest.mock import patch
import research_ordered_question_catalog as q
import research_ordered_question_store as qs
import research_formula_ordered_v7 as e


def historical_candidate():
    return next(c for c in q.candidates() if c['formula_id']==q.VERSION+':SPOT65_PRIOR_1h_SUPPORTS')


def postgres_coverage_probes():
    """Read-only PostgreSQL probes built from the actual production SQL builder."""
    import research_formula_ordered_store as store
    from research_ordered_validation_selftest import SCOPE,START
    candidate=historical_candidate()
    class Capture:
        def execute(self,sql,args): self.sql,self.args=sql,args;return self
        def fetchone(self): return None
    def bind(sql,args):
        parts=sql.split('%s')
        assert len(parts)==len(args)+1
        def literal(value):
            if isinstance(value,datetime): value=value.isoformat()
            return "'"+str(value).replace("'","''")+"'"
        return ''.join(part+(literal(args[i]) if i<len(args) else '') for i,part in enumerate(parts))
    base={'event.direction_mapping_valid':True,'spot_cvd.aligned_score':70}
    complete={**base,'historical.closed_1m.1h.alignment':'SUPPORTS'}
    cases={'missing_possible':base,'missing_screen':None,
        'null_unknown':{**base,'historical.closed_1m.1h.alignment':None},
        'wrong_type_unknown':{**base,'historical.closed_1m.1h.alignment':2},
        'known_false':{**base,'spot_cvd.aligned_score':10},'known_true_complete':complete}
    predicate,params=qs.historical_coverage_predicate(candidate)
    values=','.join("('%s',%s)"%(name,'NULL::jsonb' if features is None else "'"+qs.canonical(features)+"'::jsonb") for name,features in cases.items())
    probes=["WITH f(case_id,features) AS (VALUES "+values+") SELECT case_id,("+bind(predicate,params)+") AS blocks FROM f ORDER BY case_id"]
    capture=Capture();store.candidate_feature_coverage_complete(capture,SCOPE,candidate,now=START+timedelta(days=1))
    for phase,first in (('UNKNOWN',base),('ENRICHED',complete)):
        # The first event precedes the later observed success in the same wave.
        # These CTEs shadow production tables and perform no writes.
        prefix="""WITH research_events(event_id,event_kind,delivery_status,direction,alert_time_utc,symbol) AS
          (VALUES (1,'ALERT','DELIVERED','LONG','2026-09-06T13:00:00Z'::timestamptz,'BTC'),
                  (2,'ALERT','DELIVERED','LONG','2026-09-06T14:00:00Z'::timestamptz,'BTC')),
          research_ordered_feature_screens(event_id,feature_version,features) AS
          (VALUES (1,'VERSION','FIRST'::jsonb),(2,'VERSION','SECOND'::jsonb)),
          research_event_btc_movements(event_id,episode_policy_version,btc_parent_movement_id,membership_status) AS
          (VALUES (1,'POLICY','wave-1','LIVE'),(2,'POLICY','wave-1','LIVE')),
          research_btc_parent_movements(btc_parent_movement_id,episode_policy_version,evidence_eligible,start_time_utc) AS
          (VALUES ('wave-1','POLICY',TRUE,'2026-09-06T12:00:00Z'::timestamptz)) """
        prefix=prefix.replace('VERSION',q.VERSION).replace('POLICY',store.PARENT_POLICY).replace('FIRST',qs.canonical(first)).replace('SECOND',qs.canonical(complete))
        probes.append(prefix+"SELECT '"+phase+"' AS phase, blocker.event_id, blocker.event_id IS NULL AS complete FROM (VALUES(1)) seed(n) LEFT JOIN LATERAL ("+bind(capture.sql,capture.args)+") blocker ON TRUE")
    return probes


def source():
    return {'event_id':1,'symbol':'BTC','direction':'LONG','source_side':'SHORT','event_type':'MAX_PAIN_ALERT',
        'alert_time_utc':datetime(2026,9,6,21,tzinfo=timezone.utc),'current_price':100,'target_price':101,'score':78,
        'engine_snapshot':{'alert_side':'SHORT','average_score_all_timeframes':66,'opposite_average_score_all_timeframes':57,
            'opposite_score':65,'score_components':{'relative_gap':5,'target_proximity':12,'directional_alignment':25},
            'consensus_hits':5,'consensus_total':6,'near_share_pct':70,'near_amount':700,'far_amount':300}}


class Tests(unittest.TestCase):
    def test_sequence_uses_distinct_scans_and_strictly_earlier_times(self):
        now=datetime(2026,9,7,10,tzinfo=timezone.utc)
        current={'event_id':9,'symbol':'SOL','direction':'LONG','event_type':'SPOT_CVD_ALERT',
                 'alert_time_utc':now,'current_price':102,'engine_snapshot':{'watch_scan_id':'scan-3'}}
        features={'spot_cvd.aligned_score':70}
        old=[]
        for event_id,minutes,scan,score in ((1,20,'scan-1',66),(2,10,'scan-1',68),(3,5,'scan-2',69),(4,0,'scan-x',90)):
            event={**current,'event_id':event_id,'alert_time_utc':now-timedelta(minutes=minutes),
                   'current_price':100,'engine_snapshot':{'watch_scan_id':scan}}
            old.append((event,{'spot_cvd.aligned_score':score}))
        result=q.sequence_features(current,features,old)
        self.assertEqual(result['sequence.30m.spot_cvd.prior_scans_same_symbol_direction'],2)
        self.assertEqual(result['sequence.30m.spot_cvd.entry_ordinal'],3)
        self.assertEqual(result['sequence.30m.spot_cvd.score_change'],1)
        self.assertAlmostEqual(result['sequence.30m.price_progress_aligned_pct'],2)

    def test_new_catalog_is_policy_bounded(self):
        all_=e.candidate_catalog(include_extended=True)
        self.assertTrue(any(c['formula_id']==q.VERSION+':CORE_STRICT_TRIPLE_TOTAL_65' for c in all_))
        self.assertTrue(any('ENTRY_3' in c['formula_id'] for c in all_))
        self.assertTrue(any('REGIME_RANGE' in c['formula_id'] for c in all_))
        self.assertLessEqual(len(all_),300)
    def test_exact_averages_components_liquidity_and_time(self):
        f=q.extended_features(source())
        self.assertEqual(f['max_pain.average_score_all_timeframes'],66)
        self.assertEqual(f['max_pain.opposite_average_score_all_timeframes'],57)
        self.assertEqual(f['max_pain.selected_opposite_difference'],13)
        self.assertEqual(f['max_pain.components.relative_gap'],5)
        self.assertNotEqual(f['max_pain.selected_opposite_difference'],f['max_pain.components.relative_gap'])
        self.assertEqual(f['liquidity.alignment'],'SUPPORTS')
        self.assertEqual((f['time.hour_israel'],f['time.weekday_israel']),(0,0))
        self.assertEqual((f['max_pain.consensus_hits'],f['max_pain.consensus_total']),(5,6))
    def test_missing_and_zero_never_invent_ratios_or_scores(self):
        x=source();x['engine_snapshot'].update(opposite_score=0,consensus_total=0,near_share_pct=None,near_amount=None,far_amount=None)
        f=q.extended_features(x)
        self.assertNotIn('max_pain.selected_opposite_ratio',f)
        self.assertNotIn('max_pain.consensus_hits_ratio',f)
        self.assertNotIn('liquidity.alignment',f)
        self.assertEqual(f['liquidity.capture_status'],'MISSING')
        self.assertFalse(any(c for c in q.candidates() if any(x['value']=='MISSING' for x in c['conditions'])))
    def test_direction_is_inverted_only_by_source_contract(self):
        x=source();x['direction']='SHORT'
        f=q.extended_features(x)
        self.assertFalse(f['event.direction_mapping_valid'])
        self.assertNotIn('max_pain.selected_score',f)
        self.assertNotIn('liquidity.alignment',f)
        x=source();x['engine_snapshot']['near_share_pct']=95
        self.assertEqual(q.extended_features(x)['liquidity.capture_status'],'UNVERIFIED')
    def test_total_not_average_and_no_futures_label_leakage(self):
        all_=e.candidate_catalog(include_extended=True)
        self.assertEqual(len(e.candidate_catalog()),7)
        self.assertEqual(len({c['formula_id'] for c in all_}),len(all_))
        for c in all_: e._conditions(c)
        self.assertTrue(all(c['repeat_count']==1 for c in all_))
        self.assertTrue(any(c.get('research_orientation')=='INVERSE' for c in all_))
        self.assertLess(len(all_),300)
    def test_band_boundary_not_double_counted(self):
        candidates=[c for c in q.candidates() if c['formula_id'].startswith(q.VERSION+':LIQUIDITY_') and c['question_ids']==['Q02']]
        for score in (0,40,50,60,70,80,100):
            f={'event.direction_mapping_valid':True,'liquidity.selected_share_pct':score}
            self.assertEqual(sum(e.matches(c,f,'LONG') for c in candidates),1)
    def test_all_questions_preserved_and_blocked_is_not_tested(self):
        questions=q.question_map()
        self.assertTrue({f'Q{i:02}' for i in range(1,73)}<={q['question_id'] for q in questions})
        q46=next(x for x in questions if x['question_id']=='Q46')
        o=q.coverage_observation(q46,q.extended_features(source()),e.candidate_catalog(include_extended=True))
        self.assertFalse(o['statistical_test_performed'])
        self.assertEqual(o['status'],'BLOCKED_FEATURE_COVERAGE')
        self.assertTrue(any(key.startswith('historical.') for key in o['missing_features']))
    def test_same_input_clock_does_not_repeat_but_fresh_expiry_does(self):
        now=datetime(2026,9,7,tzinfo=timezone.utc)
        rows=[{'event_id':1,'alert_time_utc':now-timedelta(days=14)+timedelta(seconds=30)}]
        scope={'scope_key':'a'}
        def key(t):return qs.evaluation_input(scope,rows,now=t,population_complete=True,membership_complete=True)
        self.assertEqual(key(now),key(now+timedelta(seconds=1)))
        self.assertNotEqual(key(now),key(now+timedelta(seconds=31)))
        # Whole-parent start, not the later alert time, governs FRESH expiry.
        rows[0]['parent_start_time_utc']=now-timedelta(days=14)-timedelta(seconds=1)
        self.assertEqual(key(now),key(now+timedelta(seconds=31)))
    def test_metric_written_after_pass_clock_is_revisited_when_available(self):
        now=datetime(2026,9,7,tzinfo=timezone.utc)
        rows=[{'event_id':1,'alert_time_utc':now-timedelta(hours=2),
               'common_window_record':{'status':'READY','window_end_utc':now-timedelta(hours=1),
                                       'observed_at_utc':now+timedelta(seconds=2)}}]
        def key(t):return qs.evaluation_input({'scope_key':'a'},rows,now=t,population_complete=True,membership_complete=True)
        self.assertEqual(key(now),key(now+timedelta(seconds=1)))
        self.assertNotEqual(key(now),key(now+timedelta(seconds=2)))
        self.assertEqual(key(now+timedelta(seconds=2)),key(now+timedelta(seconds=3)))
    def test_unknown_earlier_history_blocks_rate_and_freeze_until_enriched(self):
        import research_formula_ordered_store as store
        import research_ordered_validation_store as validation_store
        from research_ordered_validation_selftest import SCOPE,START,item
        from research_ordered_validation_store_selftest import AdapterFixture,Cursor
        candidate=historical_candidate();scope={**SCOPE,'candidate_key':candidate['formula_id']}
        early=item(1,wave='wave-1',status='FAILURE')
        later=item(2,wave='wave-1',parent_start=early['parent_start_time_utc'],status='SUCCESS')
        base={'event.direction_mapping_valid':True,'spot_cvd.aligned_score':70}
        complete={**base,'historical.closed_1m.1h.alignment':'SUPPORTS'}
        features={1:base,2:complete}
        class CohortFixture(AdapterFixture):
            def execute(self,sql,args=()):
                if sql.startswith('SELECT e.event_id,m.btc_parent_movement_id'):
                    self.coverage_query,self.coverage_args=sql,args
                    missing=next((eid for eid in (1,2) if qs.potential_match_with_missing_history(features[eid],candidate)),None)
                    return Cursor([{'event_id':missing,'btc_parent_movement_id':'wave-1'}] if missing else [])
                if 'INSERT INTO research_ordered_validation_evaluations' in sql or 'SET evidence=' in sql:
                    return Cursor()
                return super().execute(sql,args)
        conn=CohortFixture()
        def run():
            gate=store.candidate_feature_coverage_complete(conn,scope,candidate,now=START+timedelta(days=1))
            rows=[{**row,'decision_features':features[row['event_id']]} for row in (early,later)
                  if e.matches(candidate,features[row['event_id']],'LONG')]
            raw=e.summarize_scope(rows,analysis_as_of_utc=START+timedelta(days=1),source_coverage_complete=gate['complete'])
            with patch.object(validation_store,'_enrich_rows',side_effect=lambda conn,rows,binding:rows):
                validated=validation_store.evaluate_scope(conn,scope,rows,now=START+timedelta(days=1),
                    candidate_definition=candidate,source_coverage_complete=gate['complete'],common_window_rows={},registered_attempts=1)
            qs.apply_validation_cohorts(raw,validated)
            return gate,raw,validated
        gate,raw,validated=run()
        self.assertEqual(gate['potentially_matching_unknown_event_id'],1)
        self.assertIsNone(raw['hit_rate_pct'])
        self.assertFalse(validated['representative_entries_frozen'])
        self.assertEqual(conn.waves,{})
        features[1]=complete
        gate,raw,validated=run()
        self.assertTrue(gate['complete'])
        self.assertEqual((raw['successes'],raw['failures'],raw['hit_rate_pct']),(0,1,0))
        self.assertTrue(validated['representative_entries_frozen'])
        self.assertEqual(conn.waves['wave-1']['representative_event_ids'],[1])
        self.assertFalse(conn.waves['wave-1']['representative_conflict'])
        # Unrelated/base families remain usable without waiting for past data.
        features[1]=base
        self.assertTrue(store.candidate_feature_coverage_complete(conn,scope,e.candidate_catalog()[0],now=START)['complete'])
        inverse={**candidate,'research_orientation':'INVERSE'}
        store.candidate_feature_coverage_complete(conn,{**scope,'direction':'SHORT'},inverse,now=START+timedelta(days=1))
        self.assertEqual(conn.coverage_args[3],'LONG')
    def test_generated_history_predicate_retains_unknown_and_known_false(self):
        candidate=historical_candidate()
        base={'event.direction_mapping_valid':True,'spot_cvd.aligned_score':70}
        for missing in ({},None,{'historical.closed_1m.1h.alignment':None},{'historical.closed_1m.1h.alignment':2}):
            features=None if missing is None else {**base,**missing}
            self.assertTrue(qs.potential_match_with_missing_history(features,candidate))
        self.assertFalse(qs.potential_match_with_missing_history({**base,'spot_cvd.aligned_score':10},candidate))
        self.assertFalse(qs.potential_match_with_missing_history({**base,'historical.closed_1m.1h.alignment':'SUPPORTS'},candidate))
        probes=postgres_coverage_probes()
        self.assertEqual(len(probes),3)
        self.assertTrue(all('%s' not in sql for sql in probes))
        self.assertIn('IS DISTINCT FROM',probes[0])
        self.assertIn('LEFT JOIN research_event_btc_movements',probes[1])
    def test_fresh_probability_and_asymmetry_use_same_population(self):
        counts={status:0 for status in e.WAVE_STATUSES};counts.update(SUCCESS=2,FAILURE=1)
        fresh={'resolved_waves':3,'successes':2,'failures':1,'hit_rate_pct':200/3,'wilson_95_lower_pct':20,
            'status_counts':counts,'common_window_complete':True,'common_window_asymmetry_ratio':2.5}
        whole={**fresh,'resolved_waves':4,'successes':3,'hit_rate_pct':75,'source_coverage_complete':True,
            'common_window_asymmetry_ratio':1.1,'fresh':fresh}
        result={};qs.apply_validation_cohorts(result,{'all_period_metrics':whole})
        self.assertEqual((result['count_route'],result['sample_size'],result['hit_rate_pct']),('FRESH',3,200/3))
        self.assertEqual(result['common_window_metrics']['common_window_asymmetry_ratio'],2.5)
    def test_inverse_missing_schema_is_missing_data_and_cached_requests_recover(self):
        import research_formula_ordered_store as store
        import research_ordered_inverse_store as inverse
        from research_formula_ordered_periods_selftest import RelationalStore
        db=RelationalStore();cutoff=store.PERIODS['SINCE_20260904']
        for event_id in (1,2):
            db.event(event_id,cutoff+timedelta(hours=1),str(event_id),cutoff)
        key=q.VERSION+':INVERSE:PRICE_OI_TOTAL_65'
        db.db.execute('UPDATE research_ordered_formula_matches SET candidate_key=?,direction=?',(key,'SHORT'))
        scope={'candidate_key':key,'symbol':'ALL','direction':'SHORT','window_minutes':60,'threshold_bps':25,'period_key':'SINCE_20260904'}
        with patch.object(inverse,'available',return_value=False),patch.object(inverse,'load_inverse_outcomes',side_effect=AssertionError('Missing033 must not be queried')):
            rows,_=store.load_scope_rows(db,scope,now=cutoff+timedelta(days=1))
        self.assertEqual({r['event_id'] for r in rows},{1,2})
        self.assertTrue(all(r['outcome_event_id'] is None and 'status' not in r['ordered_outcome'] for r in rows))
        db.db.executescript('''CREATE TABLE research_ordered_formula_worker_state(worker_key TEXT PRIMARY KEY,last_event_id INTEGER DEFAULT 0,updated_at_utc TEXT);
          CREATE TABLE research_ordered_inverse_requests(linked_source_event_id INTEGER,inverse_version TEXT);''')
        original_execute=db.execute
        # SQLite executes selection/cursor semantics; PG lock behavior is not
        # claimed by this single-connection relational fixture.
        db.execute=lambda sql,args:original_execute(sql.replace('NOW()','CURRENT_TIMESTAMP').replace(' FOR UPDATE',''),args)
        self.assertEqual(store.inverse_reconciliation_ids(db,limit=1),[1])
        db.db.execute('INSERT INTO research_ordered_inverse_requests VALUES(?,?)',(1,inverse.VERSION))
        self.assertEqual(store.inverse_reconciliation_ids(db,limit=1),[2])
        db.db.execute('INSERT INTO research_ordered_inverse_requests VALUES(?,?)',(2,inverse.VERSION))
        self.assertEqual(store.inverse_reconciliation_ids(db,limit=1),[])
        db.db.execute('DELETE FROM research_ordered_inverse_requests WHERE linked_source_event_id=1')
        self.assertEqual(store.inverse_reconciliation_ids(db,limit=1),[1])


if __name__=='__main__':
    import sys,json
    if '--postgres-probes' in sys.argv: print(json.dumps(postgres_coverage_probes()))
    else: unittest.main()
