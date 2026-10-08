"""Disabled wire boundary: fake signer/transport and real local safety gates."""
from copy import deepcopy
import unittest
from unittest.mock import patch
import experimental_execution_contract as contract
from experimental_execution_fixtures import r2732_message
from . import experimental_execution_dispatch as boundary
from . import card_lifecycle as life, filled_quantity_dispatch as wire
from .test_two_account_execution import ENV, C, D

class Permit:
    def __init__(self): self.used=False
    def validate(self):
        if self.used: raise AssertionError('PERMIT_USED')
    def check(self): self.validate(); self.used=True

class Signer:
    domain='software'; address=D
    def __init__(self): self.calls=[]; self.hook=None
    def sign(self,action,vault,nonce,expiry,mainnet):
        self.calls.append((deepcopy(action),vault,nonce,expiry,mainnet))
        if self.hook: self.hook(action)
        return {'r':'0x1','s':'0x2','v':27}

class Transport:
    domain='software'
    def __init__(self,context): self.context=context; self.calls=[]; self.fail=False
    def current_context(self): return deepcopy(self.context)
    def exchange(self,host,path,body):
        self.calls.append((host,path,deepcopy(body)))
        if self.fail: raise TimeoutError('SIMULATED_LOST_REPLY')
        return dict(status='ok',response=dict(type='order',data=dict(statuses=[dict(resting=dict(oid=10))])))

def fixture():
    decision=1780000200000; decision-=decision%900000
    source=r2732_message(entry=2.0,decision_ms=decision,as_of_ms=decision+60000)
    now=contract.moment_ms(source['source_at'])+1; cid=source['occurrence_id']
    meta={'universe':[dict(name='XRP',szDecimals=1,maxLeverage=20)]}
    order=dict(a=0,b=False,p='2',s='1000',r=False,t={'limit':{'tif':'Ioc'}},c='0x'+'a'*32)
    proposal=dict(version='isolated',card_id=cid,account=C,role='short_account',symbol='XRP',leg='ENTRY',operation='ENTRY',quantity='1000',action=dict(type='order',orders=[order],grouping='na'),source_at=source['source_at'],source_expires_at=source['expires_at'],basis='e'*64,observed_at_ms=now)
    request=dict(request_id='b'*64,domain='software',bucket='c'*64,proposal=proposal,nonce=now,attempt_at_ms=now,prepared_at_ms=now,attempts=1,phase='OUTCOME_UNKNOWN',reply=None,observed_oid=None)
    plan=dict(symbol='XRP',side='SHORT',entry='2',stop='2.01',take_profit='1.84')
    context=dict(source=source,current_source=dict(source=source,plan_digest=contract.plan_digest(source),entry_permission='WAITING',cancellation=None),env=deepcopy(ENV),agent=D,host=boundary.HOST,metadata=meta,
        safety=dict(account=C,role='short_account',at_ms=now,entry_enabled=True,emergency_healthy=True,feed_reconciled=True,entry_circuit_clear=True,supervisor_at_ms=now,not_before_ms=now-60001),
        buckets=[],open_orders=[],positions={'assetPositions':[]},market=dict(environment='testnet',symbol='XRP',at_ms=now,mark_price='2'),
        entry_account=dict(account=C,agent=D,at_ms=now,action_headroom=5))
    return request,context,now


def approved_fixture(*, side='LONG', entry='93.544', mark='93.544'):
    """The approved alert's quoted limit, independent of any source history."""
    import approved_alert_contract as approved
    from approved_alert_fixtures import maxpain_alert
    from .experimental_execution_prices import prepare
    source=maxpain_alert(entry=entry)
    if side=='SHORT':
        source.update(side='SHORT',stop='96',take_profit='91.405',original_target='90')
        source['proof']['episode_key']='90'
        source['occurrence_id']=approved.occurrence_id(source)
        source=approved.validate(source)
    request,context,_=fixture();now=approved.moment_ms(source['approved_at'])+10000
    role='long_account' if side=='LONG' else 'short_account'
    route=boundary.roles.route_for(ENV,role)
    meta={'universe':[dict(name='HYPE',szDecimals=2,maxLeverage=10)]}
    levels=prepare(source,meta)['execution']
    p=request['proposal'];p.update(card_id=source['occurrence_id'],account=route['account'],
        role=role,symbol='HYPE',quantity='1',source_at=source['source_at'],
        source_expires_at=source['expires_at'],observed_at_ms=now)
    p['action']['orders'][0].update(b=side=='LONG',p=levels['entry'],s='1',t=dict(limit=dict(tif='Gtc')))
    request.update(nonce=now,attempt_at_ms=now,prepared_at_ms=now)
    plan={k:levels[k] for k in ('symbol','side','entry','stop','take_profit')}
    context.update(source=source,current_source=dict(source=source,plan_digest=approved.plan_digest(source),
        entry_permission='WAITING',cancellation=None),metadata=meta,agent=route['agent'],
        market=dict(environment='testnet',symbol='HYPE',at_ms=now,mark_price=mark),
        entry_account=dict(account=route['account'],agent=route['agent'],at_ms=now,action_headroom=5))
    context['safety'].update(account=route['account'],role=role,at_ms=now,
        supervisor_at_ms=now,not_before_ms=approved.moment_ms(source['created_at'])-1)
    return request,context,now

class DispatchBoundaryTests(unittest.TestCase):
    def setUp(self):
        for target in ('http.client.HTTPSConnection','socket.create_connection','socket.socket.connect','hl_testnet_runtime.two_account_execution.wallet_for_role'):
            guard=patch(target,side_effect=AssertionError('OFFLINE_NO_KEYS')); guard.start(); self.addCleanup(guard.stop)
        self.request,self.context,self.now=fixture(); self.signer=Signer(); self.transport=Transport(self.context); self.port=boundary.DisabledDispatchPort()
    def admission(self):
        a=wire.TransportAdmission(self.request['proposal'],Permit()); a.bind(self.request); return a
    def send(self,admission=None):
        return self.port.rehearse(self.request,self.context,admission=admission or self.admission(),signer=self.signer,transport=self.transport,clock=lambda:self.now)
    def test_real_entrypoint_is_unconditionally_disabled(self):
        with self.assertRaisesRegex(boundary.BoundaryError,'LIVE_DISPATCH_NOT_RELEASED'): self.port.send(self.request,enable_testnet=True)
        self.assertEqual(self.signer.calls,[]); self.assertEqual(self.transport.calls,[])
    def test_exact_testnet_wire_shape_after_all_gates(self):
        result=self.send(); self.assertFalse(result['dispatch_enabled']); self.assertEqual(result['order_requests_sent'],0); self.assertEqual(result['simulated_transport_requests'],1)
        self.assertFalse(self.signer.calls[0][-1]); self.assertEqual(self.transport.calls[0][:2],(boundary.HOST,'/exchange'))
        body=self.transport.calls[0][2]; self.assertEqual(body['expiresAfter'],self.now+15000); self.assertEqual(body['action'],self.request['proposal']['action']); self.assertEqual(result['normalized_reply']['state'],'ACCEPTED_UNVERIFIED')
    def test_same_admission_cannot_send_twice_even_after_response_loss(self):
        admission=self.admission(); self.transport.fail=True
        with self.assertRaises(TimeoutError): self.send(admission)
        self.assertEqual(self.request['phase'],'OUTCOME_UNKNOWN')
        with self.assertRaises(wire.DispatchError): self.send(admission)
        self.assertEqual(len(self.transport.calls),1)
    def test_each_new_entry_safety_gate_is_required(self):
        for field in ('entry_enabled','emergency_healthy','feed_reconciled','entry_circuit_clear'):
            with self.subTest(field=field):
                self.context['safety'][field]=False
                with self.assertRaisesRegex(boundary.BoundaryError,'NEW_ENTRY_SAFETY_GATE_CLOSED'): self.send()
                self.context['safety'][field]=True
        self.assertEqual(self.signer.calls,[])
    def test_source_retirement_and_frozen_plan_changes_block_before_signing(self):
        self.context['current_source']['entry_permission']='RETIRED'
        with self.assertRaisesRegex(boundary.BoundaryError,'SOURCE_ENTRY_PERMISSION_RETIRED'): self.send()
        self.context['current_source']['entry_permission']='WAITING'; self.context['current_source']['plan_digest']='0'*64
        with self.assertRaisesRegex(boundary.BoundaryError,'FROZEN_PROSPECTIVE_SOURCE_CHANGED'): self.send()
        self.assertEqual(self.signer.calls,[])
    def test_actual_mark_must_come_from_testnet_and_within_exits(self):
        self.context['market']['environment']='mainnet'
        with self.assertRaisesRegex(boundary.BoundaryError,'ACTUAL_TESTNET_MARK_REQUIRED'): self.send()
        self.context['market']['environment']='testnet'; self.context['market']['mark_price']='2.02'
        with self.assertRaisesRegex(boundary.BoundaryError,'OUTSIDE_FROZEN_EXITS'): self.send()
    def test_unknown_account_order_and_position_close_entry_lane(self):
        self.context['open_orders']=[dict(coin='XRP',oid=44)]
        with self.assertRaisesRegex(wire.DispatchError,'UNOWNED_ACCOUNT_ORDER'): self.send()
        self.context['open_orders']=[]; self.context['positions']={'assetPositions':[dict(position=dict(coin='XRP',szi='-1'))]}
        with self.assertRaisesRegex(wire.DispatchError,'UNOWNED_ACCOUNT_POSITION'): self.send()
    def test_quantity_over_current_risk_is_not_permitted_by_wire_shape(self):
        self.request['proposal']['quantity']='1000.1'; self.request['proposal']['action']['orders'][0]['s']='1000.1'
        with self.assertRaisesRegex(boundary.risk_policy.RiskError,'EXCEEDS_CURRENT'): self.send()
        self.assertEqual(self.signer.calls,[])
    def test_wrong_signer_and_real_transport_fail_before_any_signing(self):
        self.signer.address=C
        with self.assertRaisesRegex(boundary.BoundaryError,'ROLE_SIGNER_MISMATCH'): self.send()
        self.signer.address=D; self.transport.domain='testnet'
        with self.assertRaisesRegex(boundary.BoundaryError,'ISOLATED_SIGNER_AND_TRANSPORT_REQUIRED'): self.send()
        self.assertEqual(self.signer.calls,[])
    def test_entry_account_must_bind_signer_and_preserve_exit_headroom(self):
        original=deepcopy(self.context['entry_account'])
        for change in (dict(account=D),dict(agent=C),dict(action_headroom=0),dict(action_headroom=True)):
            with self.subTest(change=change):
                self.context['entry_account']={**original,**change}
                with self.assertRaisesRegex(boundary.BoundaryError,'EXACT_CURRENT_ENTRY_ACCOUNT_REQUIRED'): self.send()
        self.context['entry_account']=original
        self.context['entry_account']['at_ms']=self.now-15001
        with self.assertRaisesRegex(boundary.BoundaryError,'ENTRY_ACCOUNT_SAMPLE_EXPIRED'): self.send()
        self.assertEqual(self.signer.calls,[])
    def test_final_after_signing_source_recheck(self):
        self.signer.hook=lambda _:self.context['current_source'].update(entry_permission='RETIRED')
        with self.assertRaisesRegex(boundary.BoundaryError,'SOURCE_ENTRY_PERMISSION_RETIRED'): self.send()
        self.assertEqual(len(self.signer.calls),1); self.assertEqual(self.transport.calls,[])
    def test_signer_cannot_mutate_action(self):
        self.signer.hook=lambda action:action['orders'][0].update(s='2000')
        with self.assertRaisesRegex(boundary.BoundaryError,'SIGNER_MUTATED'): self.send()
        self.assertEqual(self.transport.calls,[])
    def test_signing_delay_cannot_extend_attempt_freshness(self):
        self.signer.hook=lambda _:setattr(self,'now',self.now+5001)
        with self.assertRaisesRegex(boundary.BoundaryError,'DURABLE_ATTEMPT_EXPIRED'): self.send()
        self.assertEqual(self.transport.calls,[])
    def test_approved_exact_limit_accepts_already_touched_current_mark(self):
        for side,mark in (('LONG','93.544'),('LONG','93'),('SHORT','93.544'),('SHORT','94')):
            with self.subTest(side=side,mark=mark):
                request,context,now=approved_fixture(side=side,mark=mark)
                result=boundary.review(request,context,now_ms=now)
                self.assertEqual(result['action']['orders'][0]['p'],'93.544')
                self.assertEqual(result['action']['orders'][0]['t'],{'limit':{'tif':'Gtc'}})
                self.assertNotIn('source_range',context)

    def test_approved_limit_waits_at_quote_without_chasing_market(self):
        request,context,now=approved_fixture(mark='94')
        result=boundary.review(request,context,now_ms=now)
        self.assertEqual(result['action']['orders'][0]['p'],'93.544')
        request['proposal']['action']['orders'][0]['p']='94'
        with self.assertRaisesRegex(boundary.BoundaryError,'EXACT_FROZEN_ENTRY'):
            boundary.review(request,context,now_ms=now)

    def test_approved_rounding_never_accepts_worse_than_alert_limit(self):
        for side,quoted,expected,worse in (('LONG','93.5446','93.544','93.545'),
                                          ('SHORT','93.5444','93.545','93.544')):
            with self.subTest(side=side):
                request,context,now=approved_fixture(side=side,entry=quoted)
                result=boundary.review(request,context,now_ms=now)
                self.assertEqual(result['action']['orders'][0]['p'],expected)
                self.assertEqual(context['source']['entry'],quoted)
                request['proposal']['action']['orders'][0]['p']=worse
                with self.assertRaisesRegex(boundary.BoundaryError,'EXACT_FROZEN_ENTRY'):
                    boundary.review(request,context,now_ms=now)

    def test_approved_alert_expiry_still_blocks_first_submission(self):
        request,context,now=approved_fixture()
        expires=boundary.contract.moment_ms(context['source']['expires_at'])
        request.update(nonce=expires,attempt_at_ms=expires,prepared_at_ms=expires)
        request['proposal']['observed_at_ms']=expires
        with self.assertRaisesRegex(boundary.BoundaryError,'SOURCE_WINDOW_EXPIRED'):
            boundary.review(request,context,now_ms=expires)

    def test_approved_stale_mark_outside_exits_and_account_gates_still_apply(self):
        for change,expected in (('mark','OUTSIDE_FROZEN_EXITS'),('stale','MARK_EXPIRED'),
                                ('safety','SAFETY_GATE'),('account','ENTRY_ACCOUNT')):
            with self.subTest(change=change):
                request,context,now=approved_fixture()
                if change=='mark':context['market']['mark_price']='91'
                elif change=='stale':context['market']['at_ms']=now-15001
                elif change=='safety':context['safety']['feed_reconciled']=False
                else:context['entry_account']['account']=D
                with self.assertRaisesRegex(boundary.BoundaryError,expected):
                    boundary.review(request,context,now_ms=now)

if __name__=='__main__': unittest.main()
