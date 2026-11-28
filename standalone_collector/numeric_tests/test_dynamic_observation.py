"""Only a proved source change earns a second, self-contained observation."""
import hashlib
import json
from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import Mock, patch
from uuid import uuid4

from model1_execution import StageFailure
from modern_map_basic import capture_basic, changed_observation, prove_observations
from modern_map_basic_numbers import build_basic_snapshot, validate_basic_snapshot
from modern_map_task import _process_basic_btc
from test_fresh_range_evidence import PNG, TARGET, TIME, inputs


class DynamicObservationTests(TestCase):
    def test_missing_optional_maximum_preserves_complete_core_and_roundtrips(self):
        opening,args = inputs()
        source = deepcopy(opening)
        baseline = build_basic_snapshot(opening, **args, require_fresh_evidence=True)
        closing = dict(args['end_observation'],raw_largest_value=None)
        first,last = prove_observations(opening, closing, base_only=True)
        result = build_basic_snapshot(first, **dict(args,end_observation=last),require_fresh_evidence=True)
        self.assertIsNone(result['raw_largest_value'])
        self.assertEqual(opening, source)
        self.assertEqual(result['levels'], baseline['levels'])
        self.assertEqual(result['coverage'], baseline['coverage'])
        self.assertEqual(result['reference_price'], baseline['reference_price'])
        self.assertEqual(validate_basic_snapshot(result,require_fresh_evidence=True),result)
        conflicting = dict(opening,raw_largest_value='200')
        with self.assertRaises(StageFailure) as raised:
            prove_observations(conflicting, closing, base_only=True)
        self.assertFalse(hasattr(raised.exception,'verified_change'))

    def test_valid_market_change_only(self):
        opening,_ = inputs()
        when = '2026-11-26T15:01:00Z'
        with self.assertRaises(StageFailure) as raised:
            prove_observations(opening,dict(opening,source_time=when),base_only=True)
        self.assertEqual(raised.exception.verified_change,dict(condition='source_epoch_or_values_changed',
                         field='source_time',opening_source_time=TIME,closing_source_time=when))
        for change in ({'raw_above_total':None}, {'raw_total':'1M'}, {'source_time':'2026-11-26T14:50:00Z'},
                       {'raw_total':'98M','ambiguous_fields':['raw_above_total']}):
            with self.subTest(change=change),self.assertRaises(StageFailure) as rejected:
                prove_observations(opening,dict(opening,**change),base_only=True)
            self.assertFalse(hasattr(rejected.exception,'verified_change'))

    def run_job(self,folder,handler):
        page = object()
        job = str(uuid4())
        acknowledgements = []
        def dispatch(s,m,t,path,*,observe,base_only):
            return capture_basic(page,s,m,t,path,observe=observe,base_only=base_only)
        with patch('modern_map_basic._capture_basic_observation',side_effect=handler) as read:
            _process_basic_btc(folder,job,acknowledgements.append,base_only=True,target=TARGET,capture_fn=dispatch)
        return read,job,acknowledgements

    def test_one_complete_reobservation_same_job_same_page_and_one_durable_save(self):
        market,args = inputs()
        verified = build_basic_snapshot(market,**args,require_fresh_evidence=True)
        def read(page,s,m,t,path,*,observe,restarted,motion_budget,**kwargs):
            if restarted is None:
                observe('overview',dict(raw_total='1000B'))
                path.write_bytes(b'discarded chart')
                motion_budget.add('opening')
                changed_observation('closing_column_changed',closing_column_time=TIME)
            self.assertEqual(restarted['closing_column_time'],TIME)
            self.assertEqual(motion_budget,{'opening'})
            self.assertFalse(path.exists())
            path.write_bytes(PNG)
            observe('overview',market)
            observe('verified_basic',verified)
            return verified
        with TemporaryDirectory() as directory:
            output=Path(directory)
            reader, job, acknowledgements = self.run_job(output,read)
            first,second = reader.call_args_list
            self.assertIs(first.args[0],second.args[0])
            self.assertEqual(first.args[1:5],second.args[1:5])
            self.assertEqual(acknowledgements,[b'S'])
            persisted=json.loads((output/'result.json').read_text())
            self.assertEqual(persisted['basic_snapshot'],verified)
            self.assertEqual(persisted['evidence']['sha256'],hashlib.sha256(PNG).hexdigest())

    def test_second_change_stops_without_writing_first_epoch_or_old_photo(self):
        first_photo=b'discarded'
        def read(page,s,m,t,path,*,observe,restarted,**kwargs):
            if restarted is None:
                path.write_bytes(first_photo)
                observe('overview',dict(raw_total='1000B'))
                observe('hover',dict(raw_step='1'))
                changed_observation('closing_reference_changed',column_time=TIME,
                                    discarded_image_sha256=hashlib.sha256(first_photo).hexdigest())
            changed_observation('closing_column_changed',closing_column_time=TIME)
        with TemporaryDirectory() as directory:
            output=Path(directory)
            with self.assertRaises(StageFailure) as failure:
                self.run_job(output,read)
            self.assertEqual(failure.exception.basic_reason,'closing_column_changed')
            partial=json.loads((output/'measured-partial.json').read_text())
            self.assertEqual(partial['overview'],{})
            self.assertEqual(partial['hover'],[])
            self.assertIsNone(partial['image_sha256'])
            self.assertEqual(partial['basic_reobserve']['discarded_image_sha256'],hashlib.sha256(first_photo).hexdigest())
            self.assertFalse((output/'result.json').exists())

    def test_missing_time_and_account_gate_do_not_trigger_reobservation(self):
        for error in (StageFailure('source_login_required'),StageFailure('source_not_readable','hover_evidence_mismatch')):
            with self.subTest(reason=error.code), TemporaryDirectory() as directory:
                entry = Mock(side_effect=error)
                with self.assertRaises(StageFailure):
                    self.run_job(Path(directory),entry)
                self.assertEqual(entry.call_count,1)
