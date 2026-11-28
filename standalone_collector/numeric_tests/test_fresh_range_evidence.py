"""Fresh scoped source checks run before acknowledgements and at the database boundary."""
from copy import deepcopy
from hashlib import sha256
import json
from pathlib import Path
import tempfile
from unittest import TestCase
from unittest.mock import MagicMock
from uuid import uuid4

from model1_execution import StageFailure
from modern_map_basic_numbers import basic_source_url, build_basic_snapshot, valid_range_selection
from modern_map_checkpoint import CheckpointStore, SOURCE
from modern_map_task import _process_basic_btc, numeric_result

PNG = b'\x89PNG\r\n\x1a\nsynthetic'
TIME = '2026-11-26T15:00:00Z'
TARGET = ('BTC', 2, '1W')


def inputs():
    rows = [dict(raw_price='110',raw_value='10.0M',raw_distance_pct='+10.00%'),
            dict(raw_price='90',raw_value='10.0M',raw_distance_pct='-10.00%')]
    market = dict(raw_total='100M',raw_above_total='55.1M',raw_below_total='44.9M',
                  raw_realized_total='100.0K',raw_realized_longs='65.0K',raw_realized_shorts='35.0K',
                  raw_largest_value='10.0M',raw_largest_price=None,raw_above_pct='55%',raw_below_pct='45%',
                  raw_price_step='5.00',raw_column_period='5m',raw_last_price=None,source_time=TIME,sidebar_levels=rows)
    closing = dict(market, raw_last_price='100',symbol='BTC',heatmap_model=2,timeframe='1W',mode='Coin',
                   column_time=TIME,same_document=True,settings_verified=True,same_market_labels=True,
                   sidebar_rows=2,chart_regions=2,native_bins_inspected=30,range_control={'label':'7 days','value':'7d'})
    sample = dict(prior_time='2026-11-26T14:55:00Z',latest_time=TIME,period_seconds=300,prior_lag_seconds=300,
                  prior_x=90,latest_x=97,pointer_y=55,prior_axis_sources=['canvas'],latest_axis_sources=['dom'],
                  prior_tooltip='5 min before the latest · 5 min per column',
                  latest_tooltip='Latest column · 5 min per column')
    regions = [dict(side='below',colour='yellow',prices=[90]),dict(side='above',colour='green',prices=[110,115])]
    return market,dict(symbol='BTC',model=2,timeframe='1W',source_url=basic_source_url('BTC',2),source_time_after=TIME,
        column_time=TIME,column_time_after=TIME,raw_last_price='100',captured_at='2026-11-26T15:00:04Z',
        source_sha256=sha256(PNG).hexdigest(),chart_regions=regions,native_bins_inspected=30,
        end_observation=closing,cursor_proof={'schema':'coinglass.cursor.v1', 'opening':sample, 'closing':deepcopy(sample)})


class FreshRangeEvidenceTests(TestCase):
    def setUp(self):
        self.market,self.args=inputs()
        self.snapshot=build_basic_snapshot(self.market,**self.args,require_fresh_evidence=True)
        self.job=str(uuid4())

    def test_explicit_durations_must_match_while_opaque_or_absent_internal_values_are_supported(self):
        for value in (None,'7d','1W','week-7','48'):
            self.assertTrue(valid_range_selection('1W',{'label':'7 days','value':value}))
        for evidence in (None,{}, {'label':'7 days'}, {'label':'48 hour','value':'1W'},
                         {'label':'7 days','value':'48h'}, {'label':'7 days','value':'6 hours'},
                         {'label':'7 days','value':'1 year/48h'}, {'label':'7 days','value':48}):
            self.assertFalse(valid_range_selection('1W',evidence))

    def test_fresh_builder_and_job_do_not_acknowledge_missing_or_contradictory_observations(self):
        for case in ('missing_both','missing_range','missing_cursor','wrong_label','wrong_value','wrong_model'):
            args=deepcopy(self.args)
            snapshot=deepcopy(self.snapshot)
            closing=args['end_observation']
            stored=snapshot['source_evidence']['closing']
            if case in ('missing_both','missing_range'):
                del closing['range_control'], stored['range_control']
            if case in ('missing_both','missing_cursor'):
                del args['cursor_proof'], snapshot['source_evidence']['cursor_proof']
            if case in ('wrong_label','wrong_value'):
                key,value=('label','48 hour') if case=='wrong_label' else ('value','48h')
                closing['range_control'][key]=stored['range_control'][key]=value
            if case=='wrong_model':
                args['source_url']=snapshot['source_url']=basic_source_url('BTC',3)
            with self.subTest(case=case), self.assertRaises(StageFailure):
                build_basic_snapshot(self.market,**args,require_fresh_evidence=True)
            with self.subTest(case=case), tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp); acknowledgements=[]
                def capture(s,m,t,image,*,observe,base_only):
                    image.write_bytes(PNG)
                    observe('verified_basic',snapshot)
                    return snapshot
                with self.assertRaises(StageFailure):
                    _process_basic_btc(root,self.job,acknowledgements.append,base_only=True,target=TARGET,capture_fn=capture)
                self.assertEqual(acknowledgements,[])
                self.assertFalse((root/SOURCE).exists())

    def test_database_broker_enforces_fresh_receipt_and_can_restore_the_preexisting_record(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            (root/'image.png').write_bytes(PNG)
            job_store=MagicMock()
            db=job_store.connect.return_value.__enter__.return_value
            cs=CheckpointStore(job_store,self.job,*TARGET)
            old=deepcopy(self.snapshot)
            del old['source_evidence']['cursor_proof'],old['source_evidence']['closing']['range_control']
            for snapshot,previous,allowed in ((old,None,False),(old,True,True),(self.snapshot,None,True)):
                record=dict(snapshot=snapshot,run_id=self.job,image_sha256=sha256(PNG).hexdigest())
                (root/SOURCE).write_text(json.dumps(record))
                def execute(sql,args):
                    from unittest.mock import Mock
                    if 'SELECT id,' in sql:
                        return Mock(**{'fetchone.return_value':dict(id=self.job,fixed_cycle_id='scoped')})
                    if 'FOR UPDATE' in sql:
                        return Mock(**{'fetchone.return_value':{'checkpoint':{'source':record}} if previous else None})
                    return Mock()
                db.execute.side_effect=execute
                db.execute.reset_mock()
                with self.subTest(allowed=allowed,previous=previous):
                    if allowed:
                        cs.commit(b'S',root)
                    else:
                        with self.assertRaises(StageFailure):
                            cs.commit(b'S',root)
                        self.assertFalse(any('INSERT' in c.args[0] for c in db.execute.call_args_list))
            self.assertEqual(numeric_result(old,self.job,PNG,restored=True)['basic_snapshot'],old)
