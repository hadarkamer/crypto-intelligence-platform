"""Regression gates for incomplete cohorts, FRESH denominators and idle exports."""
from datetime import datetime, timezone, timedelta
import json
import research_formula_ordered_store as store
import research_formula_ordered_v7 as evaluator
from research_formula_ordered_v7_selftest import row as evidence_row, nondecisive_row, AS_OF


class Capture:
    def __init__(self):
        self.calls=[]
    def execute(self,sql,params):
        self.calls.append((sql,params))
        return self
    def cursor(self):
        return self
    def __enter__(self):
        return self
    def __exit__(self,*args):
        return None
    def executemany(self,sql,rows):
        self.calls.extend((sql,row) for row in rows)


def run():
    now=datetime(2026,9,6,tzinfo=timezone.utc)
    scope={'scope_key':'scope','candidate_key':'STRICT_TRIPLE_TOTAL_65','symbol':'ALL','direction':'LONG','window_minutes':60,'threshold_bps':25,'result':{},'period_key':'SINCE_20260904','period_start_utc':store.PERIODS['SINCE_20260904']}
    result=evaluator.summarize_scope([],analysis_as_of_utc=now)
    result.update(independent_waves=4,fresh_independent_waves=3,count_route='FRESH',count_eligible=True,sample_size=3,successes=2,failures=1,hit_rate_pct=200/3,
                  source_population_complete=True,membership_population_complete=True)
    capture=Capture()
    store.persist_scope(capture,scope,[],result,now=now)
    payload=json.loads(next(params[2] for sql,params in capture.calls if 'INSERT INTO research_sheet_upsert_outbox' in sql))
    assert payload['row']['independent_episodes']==3
    assert payload['row']['successes']+payload['row']['failures']==3
    assert payload['row']['meets_min_5'] is False
    stable_summary=json.loads(capture.calls[0][1][0])
    # Advancing the scheduler clock must not create a new pending generation
    # of an otherwise identical complete Sheet row.
    repeated=Capture()
    output=store.persist_scope(repeated,{**scope,'result':stable_summary},[],result,now=now+timedelta(minutes=1))
    assert output=={'episodes':0,'upserts':0}
    assert len(repeated.calls)==1
    incomplete=Capture()
    broken={**result,'independent_waves':5,'source_population_complete':False,'count_eligible':False,'count_route':'INCOMPLETE_DECISION_POPULATION'}
    store.persist_scope(incomplete,scope,[],broken,now=now)
    row=json.loads(next(params[2] for sql,params in incomplete.calls if 'INSERT INTO research_sheet_upsert_outbox' in sql))['row']
    assert row['independent_episodes']==0 and row['successes']==row['failures']==0
    assert row['hit_rate']==row['asymmetry_ratio']==''
    assert row['median_mfe_pct']==row['median_mae_pct']==''
    assert row['meets_min_5'] is False and row['status']=='INCOMPLETE_DECISION_POPULATION'
    assert 'provisional' in row['chat_summary']
    states=[evidence_row(1,wave='success'),evidence_row(2,wave='failure',success=False)]
    states += [nondecisive_row(i+3,status=status,wave=status)
               for i,status in enumerate(('OPEN','AMBIGUOUS','NO_TOUCH','DATA_MISSING'))]
    audited=evaluator.summarize_scope(states,analysis_as_of_utc=AS_OF)
    exported=Capture()
    store.persist_scope(exported,{**scope,'threshold_bps':50},states,audited,now=AS_OF)
    payloads=[json.loads(params[2]) for sql,params in exported.calls if 'INSERT INTO research_sheet_upsert_outbox' in sql]
    formula=next(payload['row'] for payload in payloads if payload['sheet']=='Formula_Results')
    assert formula['open_episodes']==1 and audited['excluded_waves']==4
    assert formula['successes']==formula['failures']==1
    assert formula['independent_episodes']==2 and formula['hit_rate']==50
    for status in evaluator.WAVE_STATUSES:
        assert f'"{status}":1' in formula['chat_summary']
    episodes=[payload['row'] for payload in payloads if payload['sheet']=='Episodes']
    assert {episode['result'] for episode in episodes}==set(evaluator.WAVE_STATUSES)
    assert len(episodes)==6
    ambiguous=next(episode for episode in episodes if episode['result']=='AMBIGUOUS')
    assert json.loads(ambiguous['audit_note'])['representative_outcome_statuses'][0]['source_status']=='UNRESOLVED'
    print('ordered Formula store: separated wave statuses, FRESH denominator, stable generation, incomplete cohort gates PASS')


if __name__=='__main__':
    run()
