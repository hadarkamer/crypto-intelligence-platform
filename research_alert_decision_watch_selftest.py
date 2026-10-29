"""Native differential and failure-isolation checks; no bot/network/database."""
import ast
import asyncio
from copy import deepcopy
from datetime import datetime,timezone
import hashlib
import html
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any,Dict,List,Optional
import unittest
from unittest.mock import patch

import research_alert_decision_watch as bridge
import research_alert_decision_capture as capture
import research_alert_decision_store as store

ROOT=Path(__file__).resolve().parent
CYCLE='shared-watch:2026-10-29T19:02:15.123456+00:00'
PINS={
 '_confirmation_state_key':'b0a764c547219ee6733db4435b85c6b058d317e2b69704710b30bc715461df15',
 '_confirmation_transition_message':'fc0eb610cc08969bb15a553e65ef9c8db0fbf054f876bdd300142092d36d32c5',
 '_score_confirmation_transition_message':'2ca9651b37feef9cd5d1c481050c50e0c3892fe9ebc85a91723aa2603fcd3e37',
 '_high_score_83_transition_message':'41098e2f18a322360056f2d62ab9bb37a74c3b56af594871b8a7c5da942781b5',
 '_special_transition_messages':'6132155260bf2f6f9484b3b10de733e0f9931958ec89e39fce41d349ce8cbcb3',
 '_derivatives_high_transition_messages':'f29c59d6d27f076dd422439ff39594d710a59f5ee65bcdf8a8119f5980dc50a1',
 '_spot_family_high_transition_messages':'2826d653c8ba4401ab3d36602bd384757dd37e2c993e265b7e0a161e8a8165b6',
 '_collect_special_transition_messages':'2ad36a81bf046d3f17e6ebea2a3d0eb2ca1e48960c3f40a5088116c92afd4c3a',
 '_combined_confirmation_candidates':'c7b64bf4d1d5bb3a03df79926e8450921293db5c133b3f7ccd33e36ff82648d1',
 '_combined_confirmation_message':'6ef87f8a6147b64596102303872ab1f2989c4c075676f26b77f2a7f8fccc5fae',
 '_collect_combined_confirmation_messages':'3d48d5f412c3f8fb08b2cb5dedb5865e461461ab530001102fdedd8048a99fac',
}
WATCH_PIN='5204585b97d326225d59d23faf868bd2f395a170fba6188811406a1d9c4e7c5d'


def native_scope():
    tree=ast.parse((ROOT/'main.py').read_text(encoding='utf-8'))
    nodes=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in PINS and n.name!='_combined_confirmation_candidates']
    state=dict(confirmation={},score83={},derivatives_high={},spot_high={})
    env=dict(Any=Any,Dict=Dict,List=List,Optional=Optional,datetime=datetime,timezone=timezone,html=html,
        SCORE_CONFIRMATION_THRESHOLD=65.,SCORE_CONFIRMATION_RESET_THRESHOLD=60.,
        SPECIAL_HIGH_SCORE_THRESHOLD=83.,DERIVATIVES_HIGH_THRESHOLD=65.,DERIVATIVES_HIGH_RESET_THRESHOLD=60.,
        CONFIRMATION_STATE=state['confirmation'],SCORE_CONFIRMATION_STATE={},HIGH_SCORE_83_STATE=state['score83'],
        DERIVATIVES_HIGH_STATE=state['derivatives_high'],SPOT_FAMILY_HIGH_STATE=state['spot_high'],
        COMBINED_CONFIRMATION_STATE={},RTL_MARK='',fmt=str,fmt_price=str,
        _flow_direction_icon=str,_direction_display_he=str,_display_text_he=str,
        research_event_runtime=SimpleNamespace(capture_combined_state_changes=lambda *a,**k:None))
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'unchanged-native-batches','exec'),env)
    return env,state


def items():
    model=dict(positioning=dict(available=True,score=71.,direction='BULLISH'),
        futures_flow=dict(available=True,score=69.,direction='BULLISH'),
        spot_flow=dict(available=True,score=72.,direction='BULLISH',time_families={
            key:dict(quality=.71,direction='BULLISH',label=key,windows=['30m','1h'])
            for key in ('now','short','medium','long')}))
    return [dict(symbol='DOGE',timeframe=tf,side='SHORT',score=score,target_price=.09,distance_pct=2.,
        maxpain_confirmation=dict(status=status,label='known label'),market_evidence=dict(modules=deepcopy(model)))
        for tf,score,status in [('12h',84.,'CONFIRMED'),('24h',40.,'BELOW_SCORE')]]


def candidates():
    return [dict(key='DOGE|SHORT',symbol='DOGE',side='SHORT',signal_keys={'a','b','c'},signal_count=3,
        top_item=dict(score=84.,timeframe='12h'),normal_confirmations=[{'timeframe':'12h'}],
        strong_confirmations=[],high_scores=[{'timeframe':'12h','score':84.}],
        anomaly_setups=[{'timeframe':'24h','count':3}],liquidity_imbalances=[{'timeframe':'12h','share_pct':70.}],
        derivatives_high=[{'title':'Futures','score':69.}],magnet=dict(status='CONFIRMED',magnet_quality=80.,liquidity_edge_pct=20.,members=['12h','24h']))]


def begin():
    handle=bridge.begin(CYCLE,general_enabled=True,top8_only=True)
    bridge.bind_score(dict(cycle_id=CYCLE,payload_sha256='a'*64,status='PARTIAL',input_universe_sha256='b'*64))
    bridge.gate(True)
    return handle


class WatchBridgeTests(unittest.TestCase):
    def finish(self,handle):
        saved=[]
        with patch.object(store,'submit',side_effect=lambda capsule:saved.append(capsule) or {'status':'QUEUED'}):
            bridge.finish(handle,cycle_status='completed')
        self.assertEqual(len(saved),1)
        store.prepare(saved[0])
        return saved[0]

    def test_native_bodies_and_erased_watch_are_original(self):
        tree=ast.parse((ROOT/'main.py').read_text(encoding='utf-8'))
        found={n.name:n for n in tree.body if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef))}
        for name,pin in PINS.items():
            self.assertEqual(hashlib.sha256(ast.dump(found[name],include_attributes=False).encode()).hexdigest(),pin,name)
        class EraseDiagnostics(ast.NodeTransformer):
            def visit_Assign(self,node):
                if any(isinstance(t,ast.Name) and t.id=='alert_decision_handle' for t in node.targets): return None
                return self.generic_visit(node)
            def visit_Expr(self,node):
                if isinstance(node.value,ast.Call) and isinstance(node.value.func,ast.Attribute) and isinstance(node.value.func.value,ast.Name) and node.value.func.value.id=='research_alert_decision_watch': return None
                return self.generic_visit(node)
            def visit_Call(self,node):
                if isinstance(node.func,ast.Attribute) and isinstance(node.func.value,ast.Name) and node.func.value.id=='research_alert_decision_watch' and node.func.attr in ('special_batch','combined_batch'):
                    assert isinstance(node.args[0],ast.Lambda)
                    return node.args[0].body
                return self.generic_visit(node)
        original=EraseDiagnostics().visit(deepcopy(found['run_watch_cycle']))
        self.assertEqual(hashlib.sha256(ast.dump(original,include_attributes=False).encode()).hexdigest(),WATCH_PIN)

    def test_special_projection_and_observation_match_native_repeated_calls(self):
        source=items(); baseline,baseline_states=native_scope(); current,states=native_scope()
        projected=bridge.restore_special_source(bridge.special_source(source))
        h=begin()
        for _ in range(2):
            expected=baseline['_collect_special_transition_messages'](deepcopy(source),include_score65=False)
            actual=bridge.special_batch(lambda:current['_collect_special_transition_messages'](projected,include_score65=False),projected,states)
            self.assertEqual(actual,expected);self.assertEqual(states,baseline_states)
        capsule=self.finish(h)
        self.assertEqual(capsule['status'],'COMPLETE')
        records=capsule['payload']['records'];self.assertEqual(len(records),2)
        self.assertNotIn('known label',json.dumps(records[0]['result']))

    def test_falsy_models_and_truthy_confirmation_projection(self):
        for modules in (None,False,0,'',{}):
            source=items();source[0]['market_evidence']['modules']=modules
            projected=bridge.restore_special_source(bridge.special_source(source))
            a,sa=native_scope();b,sb=native_scope()
            self.assertEqual(a['_collect_special_transition_messages'](source,include_score65=False),b['_collect_special_transition_messages'](projected,include_score65=False))
            self.assertEqual(sa,sb)
        source=items();source[0]['maxpain_confirmation']={'unselected':1};source[0]['market_evidence']['confirmation']={'status':'CONFIRMED'}
        a,sa=native_scope();b,sb=native_scope()
        projected=bridge.restore_special_source(bridge.special_source(source))
        self.assertEqual(a['_collect_special_transition_messages'](source,include_score65=False),b['_collect_special_transition_messages'](projected,include_score65=False));self.assertEqual(sa,sb)

    def test_combined_projection_stale_pruning_and_nonempty_magnet(self):
        for unknown_magnet in (False,True):
            raw=candidates()
            if unknown_magnet: raw[0]['magnet']={'unselected':1}
            projected=bridge.combined_source(raw,event_time=datetime(2026,10,29,tzinfo=timezone.utc))['candidates']
            a,_=native_scope();b,_=native_scope()
            a['COMBINED_CONFIRMATION_STATE']['STALE']={'old'};b['COMBINED_CONFIRMATION_STATE']['STALE']={'old'}
            h=begin()
            for _ in range(2):
                expected=a['_collect_combined_confirmation_messages']([],[],include_metadata=True,event_time=datetime(2026,10,29,tzinfo=timezone.utc),precomputed_candidates=raw)
                actual=bridge.combined_batch(lambda:b['_collect_combined_confirmation_messages']([],[],include_metadata=True,event_time=datetime(2026,10,29,tzinfo=timezone.utc),precomputed_candidates=projected),projected,b['COMBINED_CONFIRMATION_STATE'],event_time=datetime(2026,10,29,tzinfo=timezone.utc))
                self.assertEqual([x['text'] for x in actual],[x['text'] for x in expected]);self.assertEqual(a['COMBINED_CONFIRMATION_STATE'],b['COMBINED_CONFIRMATION_STATE'])
            self.assertEqual(self.finish(h)['status'],'COMPLETE')

    def test_diagnostic_failures_never_replace_native_result_or_error(self):
        class DiagnosticInterrupt(BaseException): pass
        for name in ('begin_decision','end_decision','stage'):
            h=begin();result=object();calls=[]
            with patch.object(capture,name,side_effect=DiagnosticInterrupt()):
                self.assertIs(bridge.special_batch(lambda:calls.append(1) or result,items(),{}),result)
            self.assertEqual(calls,[1]);self.finish(h)
        h=begin();error=asyncio.CancelledError();calls=[]
        def fail(): calls.append(1);raise error
        with patch.object(capture,'end_decision',side_effect=DiagnosticInterrupt()):
            with self.assertRaises(asyncio.CancelledError) as caught: bridge.special_batch(fail,items(),{})
        self.assertIs(caught.exception,error);self.assertEqual(calls,[1]);self.finish(h)

    def test_projection_and_seal_bounds_are_visible_and_native_once(self):
        huge=items();huge[0]['market_evidence']['modules']['positioning']['direction']='x'*1_000_000
        with self.assertRaises(ValueError): bridge.special_source(huge)
        raw=candidates()*4
        for row in raw: row['magnet']['members']=['x'*4096]*16
        with self.assertRaises(ValueError): bridge.combined_source(raw)
        h=begin();calls=[]
        value=bridge.special_batch(lambda:calls.append(1) or [],huge,{})
        self.assertEqual(value,[]);self.assertEqual(calls,[1]);capsule=self.finish(h)
        self.assertEqual(capsule['status'],'PARTIAL');self.assertEqual(capsule['payload']['native_invocation_count'],1)

    def test_skipped_gate_and_failed_begin_do_not_consume_native_state(self):
        h=begin();bridge.gate(False);cap=self.finish(h)
        self.assertEqual(cap['payload']['stage_status']['special_native_batch'],'SKIPPED_GATE')
        outer=capture.begin(CYCLE)
        try:
            with patch.object(capture,'begin',return_value=None): h=bridge.begin(CYCLE,general_enabled=True,top8_only=True)
            calls=[];self.assertEqual(bridge.special_batch(lambda:calls.append(1) or 7,items(),{}),7)
            self.assertEqual(calls,[1]);self.assertEqual(self.finish(h)['status'],'UNAVAILABLE')
            self.assertIsNotNone(capture._current())
        finally: capture.reset(outer)

    def test_finish_store_and_reset_failures_do_not_escape(self):
        h=begin()
        with patch.object(capture,'seal',side_effect=RuntimeError('diagnostic')),patch.object(store,'submit',side_effect=RuntimeError('diagnostic')):
            self.assertIsNone(bridge.finish(h,cycle_status='failed'))
        self.assertIsNone(bridge._CURRENT.get())

    def test_typical_49_item_top8_batch_fits_complete(self):
        source=[]
        for index,symbol in enumerate(('BTC','ETH','SOL','HYPE','DOGE','BNB','XRP')):
            for tf in ('12h','24h','48h','3d','1w','2w','1m'):
                item=deepcopy(items()[1]);item.update(symbol=symbol,timeframe=tf)
                item['market_evidence']['modules']['positioning']['score']=66.+index
                source.append(item)
        env,states=native_scope();h=begin()
        result=bridge.special_batch(lambda:env['_collect_special_transition_messages'](source,include_score65=False),source,states)
        self.assertTrue(result)
        capsule=self.finish(h)
        self.assertEqual(capsule['status'],'COMPLETE')
        self.assertLess(len(capture.canonical_bytes(capsule)),capture.MAX_CAPSULE_BYTES)

    def test_native_render_failure_preserves_partial_mutation_and_exception(self):
        env,_=native_scope();raw=candidates();second=deepcopy(raw[0]);second['key']='ETH|LONG';raw.append(second)
        error=ValueError('native renderer')
        render=env['_combined_confirmation_message']
        def fail_second(candidate):
            if candidate['key']=='ETH|LONG': raise error
            return render(candidate)
        env['_combined_confirmation_message']=fail_second
        state=env['COMBINED_CONFIRMATION_STATE'];state['STALE']={'old'};h=begin()
        with self.assertRaises(ValueError) as caught:
            bridge.combined_batch(lambda:env['_collect_combined_confirmation_messages']([],[],precomputed_candidates=raw),raw,state,event_time=datetime(2026,10,29,tzinfo=timezone.utc))
        self.assertIs(caught.exception,error)
        self.assertEqual(state,{'DOGE|SHORT':{'a','b','c'}})
        capsule=self.finish(h)
        self.assertEqual(capsule['payload']['records'][0]['result']['outcome'],'RAISED')
        self.assertEqual(capsule['payload']['stage_status']['combined_native_batch'],'ERROR')

    def test_failed_bridge_activation_restores_prior_capture_context(self):
        outer=capture.begin(CYCLE);previous=capture._current()
        def broken_set(value): raise RuntimeError('bridge context unavailable')
        try:
            with patch.object(bridge,'_CURRENT',SimpleNamespace(get=lambda:None,set=broken_set)):
                self.assertIsNone(bridge.begin(CYCLE,general_enabled=True,top8_only=True))
            self.assertIs(capture._current(),previous)
        finally: capture.reset(outer)


if __name__=='__main__':unittest.main()
