"""Synthetic golden-contract tests against the unchanged frozen paper engine."""
from __future__ import annotations
import copy
import hashlib
import json
import unittest
from datetime import datetime, timedelta, timezone

from research_forward_paper_engine import PaperStudy
from research_forward_source_audit import audit_source

BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)
MARKERS = ['alert_time_utc','delivered_at_utc','event_created_at_utc','match_created_at_utc',
           'membership_created_at_utc','parent_created_at_utc','parent_confirmed_at_utc',
           'btc_bar_created_at_utc','btc_observed_close_utc']
def stamp(seconds):return (BASE+timedelta(seconds=seconds)).isoformat().replace('+00:00','Z')
def digest(v):return hashlib.sha256(json.dumps(v,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()).hexdigest()

def definitions():
    result=[]
    for symbol in ('DOGE','ETH'):
        conditions=([{'feature':'sequence.30m.liquidity.entry_ordinal','operator':'==','value':2}] if symbol=='DOGE' else
                    [{'feature':family+'.aligned_score','operator':'>=','value':65} for family in ('price_oi','futures_cvd')])
        key=symbol.lower()+'-c'
        definition={'candidate':{'formula_id':key,'conditions':conditions,
                    'research_orientation':'NORMAL' if symbol=='DOGE' else 'INVERSE'},'formula_version':'test-formula'}
        result.append({'candidate_key':key,'definition':definition,'definition_sha256':digest(definition),'formula_version':'test-formula'})
    return result

def policy():
    return {'version':'synthetic-source-contract','study_start_utc':stamp(0),
            'books':['DOGE_only','ETH_only','first_per_parent'], 'required_markers':MARKERS,
            'scopes':[{'scope_key':'doge','candidate_key':'doge-c','symbol':'DOGE','direction':'SHORT','window_minutes':720,'candidate_definition_sha256':definitions()[0]['definition_sha256'],'formula_version':'test-formula'},
                      {'scope_key':'eth','candidate_key':'eth-c','symbol':'ETH','direction':'LONG','window_minutes':60,'candidate_definition_sha256':definitions()[1]['definition_sha256'],'formula_version':'test-formula'}],
            'barrier_bps':50,'net_cost_pp':.1,'entry_expiry_seconds':90,'quote_max_elapsed_seconds':5,
            'max_quote_gap_seconds':30,'quote_source':'BINANCE_SPOT_BOOK_TICKER'}

def event(event_id=1,symbol='DOGE',parent='p1',at=5,marker=None):
    marker=at if marker is None else marker
    r={'scope_key':symbol.lower(),'candidate_key':symbol.lower()+'-c','symbol':symbol,
       'formula_direction':'SHORT' if symbol=='DOGE' else 'LONG', 'event_id':event_id,'source_event_id':event_id,
       'candidate_definition_sha256':definitions()[0 if symbol=='DOGE' else 1]['definition_sha256'],'candidate_formula_version':'test-formula',
       'event_kind':'ALERT','delivery_status':'DELIVERED','membership_status':'LIVE','parent_evidence_eligible':True,
       'btc_parent_movement_id':parent,'parent_start_time_utc':stamp(.5),'match_source_time_utc':stamp(at)}
    r.update({key:stamp(marker) for key in MARKERS})
    r['alert_time_utc']=stamp(at);r['parent_confirmed_at_utc']=stamp(.5);r['btc_observed_close_utc']=stamp(at-.1)
    r.update({'source_direction':'SHORT','sheet_snapshot_id':'scan-'+str(event_id),'current_price':100,
              'event_type':'MAX_PAIN_ALERT' if symbol=='DOGE' else 'PRICE_OI_ALERT'})
    return r

def candidate(poll,received,rows,bootstrap=False,requested=None):
    return {'poll_id':poll,'request_started_utc':stamp(received-.01 if requested is None else requested),
            'response_received_utc':stamp(received),'rows':rows,'bootstrap':bootstrap,'source':'test-readonly'}

def quote(identifier,received):
    return {'packet_id':identifier,'request_started_utc':stamp(received-.01),'response_received_utc':stamp(received),
            'monotonic_elapsed_seconds':.01,'source':'BINANCE_SPOT_BOOK_TICKER',
            'quotes':[{'symbol':s,'bid':'100','ask':'100'} for s in ('DOGE','ETH')]}

def capture(engine):
    policy_json=copy.deepcopy(engine.policy);cfg=copy.deepcopy(policy_json);cfg.pop('study_start_utc')
    state=engine._load();summary=engine.summary()
    records=[]
    for row in engine.db.execute('SELECT event_id,kind,utc,input_key,payload_json,payload_sha256 FROM journal ORDER BY event_id'):
        records.append(dict(zip(('seq','kind','utc','input_key','payload_json','payload_sha256'),row)))
        records[-1]['payload_json']=json.loads(records[-1]['payload_json'])
    inputs=[r for r in records if r['kind'] in ('INPUT_CANDIDATES','INPUT_QUOTES')]
    source=[r for r in records if r['kind']=='INPUT_CANDIDATES']
    rejected=[r for r in records if r['kind']=='INPUT_REJECTED']
    selected=[]
    for i,row in enumerate(inputs):
        if row['kind']!='INPUT_CANDIDATES':continue
        p=row['payload_json']
        if not bool(p.get('bootstrap')) and not p['rows']:continue
        item=copy.deepcopy(row);item['input_ordinal']=i+1
        item['next_input_seq']=inputs[i+1]['seq'] if i+1<len(inputs) else None
        item['rejected_input_count_before']=sum(r['seq']<row['seq'] for r in rejected)
        item['observation_sequence']=item['input_ordinal']-item['rejected_input_count_before']
        selected.append(item)
    decisions=[copy.deepcopy(r) for r in records if r['kind'] in {'BOOTSTRAP_EXCLUDED','PARENT_CHOICE_FROZEN','POLICY_SOURCE_DRIFT','INPUT_REJECTED'}]
    counts={'candidate_packets_total':len(source),'candidate_nonempty_packets_total':sum(bool(r['payload_json']['rows']) for r in source),
            'candidate_empty_packets_total':sum(not r['payload_json']['rows'] for r in source),
            'candidate_bootstrap_packets_total':sum(bool(r['payload_json'].get('bootstrap')) for r in source),
            'candidate_selected_packets_total':len(selected),'quote_packets_total':sum(r['kind']=='INPUT_QUOTES' for r in records),
            'total_input_packets':len(inputs),'rejected_input_packets':len(rejected),'source_decision_rows_total':len(decisions),
            'total_journal_rows':len(records),'min_journal_seq':records[0]['seq'],'max_journal_seq':records[-1]['seq'],
            'candidate_packet_rows':sum(len(r['payload_json']['rows']) for r in selected),'invalid_rows':0,'candidate_invalid_rows_packets_total':0,
            'max_candidate_selected_seq':max((r['seq'] for r in selected),default=None),
            'max_source_decision_seq':max((r['seq'] for r in decisions),default=None)}
    snapshot={'study_id':'source-contract-test','journal_seq':records[-1]['seq'],'policy_json':policy_json,
              'policy_sha256':digest(policy_json),'config_sha256':digest(cfg),'state_json':state,
              'updated_at_utc':stamp(100),'last_status':{'first_eligible_seen_count':summary['first_eligible_seen_count'],
                'books':{k:{'completed_all':v['completed_all']} for k,v in summary['books'].items()}}}
    evidence={'source_journal_seq':snapshot['journal_seq'],'selection_complete':True,'packets':selected,'decisions':decisions,'counts':counts}
    baseline={k:snapshot[k] for k in ('study_id','policy_sha256','config_sha256')};baseline['study_start_utc']=policy_json['study_start_utc']
    return snapshot,evidence,baseline

def source_fixture():
    """Portable minimal integrated fixture: one actual frozen-engine bootstrap."""
    engine=PaperStudy(':memory:',policy(),resume=False)
    try:
        engine.observe_candidates(candidate('bootstrap',1,[],True))
        return capture(engine)
    finally:engine.close()

def predicate_envelope(s,e):
    """Artificial retained positive maps, separate from source gate evidence."""
    matches={}
    for packet in e['packets']:
        for row in packet['payload_json']['rows']:
            key=row['scope_key']+':'+str(row['event_id'])
            features={'event.symbol':row['symbol'],'event.analysis_direction':row['source_direction'],
                      'event.event_type':row['event_type']}
            if row['symbol']=='DOGE':
                features.update({'sequence.30m.liquidity.entry_ordinal':2,
                    'sequence.30m.liquidity.prior_scans_same_symbol_direction':1,'sequence.capture_status':'READY',
                    'sequence.current_families':'LIQUIDITY','liquidity.capture_status':'VALID',
                    'liquidity.source_contract':'MAX_PAIN_CAPTURED_AMOUNTS_V1','liquidity.selected_share_pct':60,
                    'liquidity.selected_amount':60,'liquidity.opposite_amount':40,'liquidity.alignment':'SUPPORTS'})
            else:
                for family,score in [('price_oi',70),('futures_cvd',80)]:
                    features.update({family+'.aligned_score':score,family+'.total_score':score,family+'.direction':'SHORT'})
            matches[key]={'scope_key':row['scope_key'],'candidate_key':row['candidate_key'],'event_id':row['event_id'],
                'match_present':True,'source_present':True,'symbol':row['symbol'],'direction':row['formula_direction'],
                'alert_time_utc':row['match_source_time_utc'],'snapshot_id':row['sheet_snapshot_id'],'entry_price':row['current_price'],
                'created_at_utc':row['match_created_at_utc'],'decision_features':features,'source_direction':row['source_direction'],
                'source_symbol':row['symbol'],'source_alert_time_utc':row['alert_time_utc']}
    return {'matches':list(matches.values()),'definitions':definitions(),'expected_keys':sorted(matches),
            'counts':{'expected_positive_keys':len(matches),'matched_rows':len(matches),'definition_rows':2},
            'source_journal_seq':s['journal_seq'],'read_utc':stamp(100),'extraction_complete':True}


class SourceTests(unittest.TestCase):
    def setUp(self):
        self.engine=PaperStudy(':memory:',policy(),resume=False)
        self.addCleanup(self.engine.close)
    def boot(self,rows=None):self.engine.observe_candidates(candidate('bootstrap',1,rows or [],True))
    def audit(self):
        s,e,_=capture(self.engine);result=audit_source(s,e)
        self.assertEqual(result['status'],'PASS',result['failures'])
        return result,s,e

    def test_empty_bootstrap_and_full_core_integration(self):
        from research_forward_evaluation import assess,safe_summary
        s,e,b=source_fixture();a=assess(s,[],expected_baseline=b,source_evidence=e,predicate_evidence=predicate_envelope(s,e))
        self.assertEqual(a['status'],'PASS',a['failures'])
        self.assertTrue(a['audit_scope']['raw_source_eligibility_replayed'])
        self.assertTrue(a['audit_scope']['source_choice_selection_replayed'])
        self.assertTrue(a['audit_scope']['formula_predicates_recomputed'])
        self.assertFalse(a['audit_scope']['raw_market_features_reconstructed'])
        self.assertFalse(a['audit_scope']['complete_journal_replayed'])
        self.assertTrue(safe_summary(a)['source_eligibility_replayed'])

    def test_bootstrap_exclusion_is_reconstructed_not_seeded(self):
        old=event(1,parent='old');self.boot([old]);self.engine.observe_candidates(candidate('later',10,[old,event(2,parent='old')]))
        a,s,e=self.audit();self.assertEqual(a['summary']['bootstrap_event_count'],1);self.assertEqual(a['summary']['bootstrap_parent_count'],1)
        self.assertEqual(a['summary']['eligible_distinct_events'],0)
        changed=copy.deepcopy(s);changed['state_json']['bootstrap_parent_ids']=[]
        self.assertEqual(audit_source(changed,e)['status'],'FAIL')

    def test_incomplete_membership_retry_and_reason_is_retained(self):
        self.boot();r=event();r['membership_status']='MISSING'
        self.engine.observe_candidates(candidate('incomplete',10,[r]));r['membership_status']='LIVE'
        self.engine.observe_candidates(candidate('ready',20,[r]));a,s,e=self.audit()
        self.assertEqual(a['summary']['delayed_eligible_events'],1)
        self.assertEqual(a['summary']['max_readiness_wait_seconds'],10)
        self.assertEqual(s['state_json']['first_seen']['doge:1']['last_ineligible_reason'],'PARENT_MEMBERSHIP')
        self.assertEqual(s['state_json']['books']['DOGE_only']['choices']['p1']['selected_at_utc'],stamp(20))

    def test_within_receipt_marker_tie_eventid_and_late_older_id_never_replace(self):
        self.boot();self.engine.observe_candidates(candidate('same',10,[event(9,'DOGE',marker=7),event(5,'ETH',marker=7)]))
        self.engine.observe_candidates(candidate('late',20,[event(1,'DOGE',at=3,marker=3)]));a,s,e=self.audit()
        self.assertEqual(s['state_json']['books']['first_per_parent']['choices']['p1']['event_id'],5)
        self.assertEqual(s['state_json']['books']['DOGE_only']['choices']['p1']['event_id'],9)

    def test_global_successful_input_sequence_includes_empty_polls_and_quotes(self):
        self.boot();self.engine.observe_candidates(candidate('empty',2,[]));self.engine.observe_quotes(quote('q1',3))
        self.engine.observe_candidates(candidate('new',10,[event()]));a,s,e=self.audit()
        self.assertEqual(s['state_json']['books']['DOGE_only']['choices']['p1']['selection_sequence'],4)
        tamper=copy.deepcopy(e);tamper['packets'][-1]['input_ordinal']-=1
        self.assertEqual(audit_source(s,tamper)['status'],'FAIL')

    def test_future_marker_and_missing_timestamp_retry_without_backdating(self):
        self.boot();future=event(marker=15);missing=event(2,parent='p2');missing.pop('btc_bar_created_at_utc')
        self.engine.observe_candidates(candidate('waiting',10,[future,missing]));missing['btc_bar_created_at_utc']=stamp(5)
        self.engine.observe_candidates(candidate('ready',20,[future,missing]));a,s,e=self.audit()
        self.assertEqual(a['summary']['eligible_distinct_events'],2)
        self.assertEqual(a['summary']['ineligible_observation_reason_counts'],{'FUTURE_SOURCE_MARKER':1,'MISSING_OR_INVALID_TIMESTAMP':1})
        self.assertTrue(all(v['first_eligible_seen_utc']==stamp(20) for v in s['state_json']['first_seen'].values()))

    def test_identity_delivery_parent_and_causal_gates(self):
        changes=[('candidate_key','wrong','CANDIDATE_KEY'),('symbol','ETH','SCOPE_IDENTITY'),
                 ('source_event_id',99,'SOURCE_ID'),('delivery_status','PENDING','NOT_DELIVERED_ALERT'),
                 ('btc_parent_movement_id',None,'PARENT_ID_MISSING'),('parent_start_time_utc',stamp(0),'OLD_PARENT'),
                 ('parent_confirmed_at_utc',stamp(6),'SOURCE_CAUSALITY'),('match_source_time_utc',stamp(4),'MATCH_SOURCE_TIME')]
        self.boot()
        for i,(key,value,reason) in enumerate(changes):
            r=event(10+i,parent='p'+str(i));r[key]=value
            self.engine.observe_candidates(candidate('bad'+str(i),10+i,[r]))
        a,_,_=self.audit()
        for _,_,reason in changes:self.assertGreaterEqual(a['summary']['ineligible_observation_reason_counts'].get(reason,0),1)

    def test_boolean_event_id_fails_source_id_gate(self):
        self.boot();r=event();r['event_id']=True;r['source_event_id']=True
        self.engine.observe_candidates(candidate('bool',10,[r]));a,_,_=self.audit()
        self.assertEqual(a['summary']['ineligible_observation_reason_counts'],{'SOURCE_ID':1})

    def test_source_identity_drift_decision_replayed_but_core_fails(self):
        from research_forward_evaluation import assess
        self.boot();r=event();r['candidate_definition_sha256']='drift'
        self.engine.observe_candidates(candidate('drift',10,[r]));a,s,e=self.audit();_,_,b=capture(self.engine)
        self.assertEqual(a['summary']['ineligible_observation_reason_counts'],{'POLICY_SOURCE_DRIFT':1})
        self.assertEqual(assess(s,[],expected_baseline=b,source_evidence=e)['status'],'FAIL')

    def test_recorded_input_rejection_rolls_back_and_requires_review(self):
        self.engine.observe_quotes(quote('before-bootstrap',.5))
        self.boot();self.engine.observe_candidates(candidate('bad-clock',10,[event(9)],requested=11))
        self.engine.observe_candidates(candidate('good',20,[event()]));s,e,b=capture(self.engine);a=audit_source(s,e)
        self.assertEqual(a['status'],'PENDING_SOURCE_EVIDENCE',a['failures'])
        self.assertEqual(a['failures'],[]);self.assertTrue(a['summary']['rejection_review_required'])
        self.assertNotIn('doge:9',s['state_json']['first_seen'])
        self.assertEqual(s['state_json']['books']['DOGE_only']['choices']['p1']['selection_sequence'],2)
        from research_forward_evaluation import assess
        full=assess(s,[],expected_baseline=b,source_evidence=e)
        self.assertEqual(full['status'],'PENDING_SOURCE_EVIDENCE')
        self.assertFalse(full['audit_scope']['raw_source_eligibility_replayed'])

    def test_quote_rejection_is_not_whole_input_rejection(self):
        self.boot();p=quote('slow',3);p['monotonic_elapsed_seconds']=6
        self.engine.observe_quotes(p);self.engine.observe_candidates(candidate('good',10,[event()]));a,s,e=self.audit()
        self.assertEqual(a['summary']['recorded_rejected_inputs'],0)
        self.assertEqual(s['state_json']['books']['DOGE_only']['choices']['p1']['selection_sequence'],3)

    def test_missing_packet_payload_tamper_and_extra_row_counts_fail_closed(self):
        self.boot();self.engine.observe_candidates(candidate('good',10,[event()]));_,s,e=self.audit()
        bad=copy.deepcopy(e);bad['packets'].pop()
        self.assertEqual(audit_source(s,bad)['status'],'FAIL')
        bad=copy.deepcopy(e);bad['packets'][-1]['payload_json']['rows'][0]['event_id']=999
        self.assertEqual(audit_source(s,bad)['status'],'FAIL')
        for key in ('candidate_packet_rows','max_candidate_selected_seq','max_source_decision_seq','invalid_rows','candidate_invalid_rows_packets_total'):
            bad=copy.deepcopy(e);bad['counts'][key]+=1
            self.assertEqual(audit_source(s,bad)['status'],'FAIL',key)

    def test_decision_moved_past_next_input_or_missing_next_metadata_fails(self):
        self.boot();self.engine.observe_candidates(candidate('good',10,[event()]));self.engine.observe_candidates(candidate('empty',11,[]));_,s,e=self.audit()
        bad=copy.deepcopy(e);d=next(v for v in bad['decisions'] if v['kind']=='PARENT_CHOICE_FROZEN');d['seq']=bad['packets'][-1]['next_input_seq']
        bad['decisions'].sort(key=lambda x:x['seq']);bad['counts']['max_source_decision_seq']=max(v['seq'] for v in bad['decisions'])
        self.assertEqual(audit_source(s,bad)['status'],'FAIL')
        bad=copy.deepcopy(e);bad['packets'][-1].pop('next_input_seq')
        self.assertEqual(audit_source(s,bad)['status'],'FAIL')

    def test_same_receipt_duplicate_row_creates_one_first_eligible_and_choice(self):
        self.boot();r=event();self.engine.observe_candidates(candidate('dupe-row',10,[r,copy.deepcopy(r)]));a,_,_=self.audit()
        self.assertEqual(a['summary']['eligible_distinct_events'],1);self.assertEqual(a['summary']['frozen_choice_count'],2)


if __name__=='__main__':
    suite=unittest.defaultTestLoader.loadTestsFromTestCase(SourceTests)
    result=unittest.TextTestRunner(verbosity=2).run(suite)
    if not result.wasSuccessful():raise SystemExit(1)
