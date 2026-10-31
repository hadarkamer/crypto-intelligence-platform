"""Bounded lifetime regressions using current worker ASTs and controlled fakes.

No production module import, database, network, procfs, large allocation or
production garbage collection. The weakrefs observe tiny synthetic cycle graphs.
"""
import ast
from copy import deepcopy
import gc
from pathlib import Path
import threading
import time
import unittest
import weakref

STAMP='2026-10-31T15:00:00+00:00'
WAIT_SECONDS=1.0


class StopFixture(BaseException): pass
class FixtureTimeout(BaseException): pass
class NativeSignal(BaseException): pass
class Payload:
    __slots__=('__weakref__',)


def nodes():
    tree=ast.parse(Path(__file__).with_name('research_consumed_inputs.py').read_text(encoding='utf-8'))
    found={node.name:node for node in tree.body if isinstance(node,ast.FunctionDef) and node.name in {'_run','_persist_one'}}
    if set(found)!={'_run','_persist_one'}: raise AssertionError('current worker functions missing')
    return found


def cycle(payload,index):
    return {'cycle_id':'synthetic-cycle-'+str(index),'root_sha256':str(index)*64,
            'manifest':{'summary':{'status':'CAPTURED'}},'objects':{'synthetic':payload}}


def wait_for(predicate):
    deadline=time.monotonic()+.5
    while not predicate() and time.monotonic()<deadline: time.sleep(.001)
    if not predicate(): raise AssertionError('bounded fixture wait timed out')


class Queue:
    def __init__(self,items,log,*,block_second=False,ack_error=None):
        self.items=list(items); self.log=log; self.block_second=block_second; self.ack_error=ack_error
        self.get_calls=0; self.ack_attempts=0; self.ack_done=0
        self.second_pending=threading.Event(); self.release_second=threading.Event()
        self.empty_pending=threading.Event(); self.release_empty=threading.Event()
    def get(self):
        self.get_calls+=1; self.log.append('GET:'+str(self.get_calls))
        if self.get_calls==2 and self.block_second:
            self.second_pending.set()
            if not self.release_second.wait(WAIT_SECONDS): raise FixtureTimeout()
        if self.items:
            # pop removes the queue's own reference before the worker receives it.
            return self.items.pop(0)
        self.empty_pending.set(); self.release_empty.wait(WAIT_SECONDS); raise StopFixture()
    def task_done(self):
        self.ack_attempts+=1; self.log.append('ACK:'+str(self.ack_attempts))
        if self.ack_error is not None: raise self.ack_error
        self.ack_done+=1
    def release(self): self.release_second.set(); self.release_empty.set()


class Store:
    def __init__(self,log,*,blocked_calls=(),mode='success',signal=None):
        self.log=log; self.blocked_calls=set(blocked_calls); self.mode=mode; self.signal=signal
        self.calls=0; self.entered={i:threading.Event() for i in (1,2)}
        self.release_calls={i:threading.Event() for i in (1,2)}
    def persist(self,value):
        self.calls+=1; index=self.calls; self.log.append('PERSIST:'+str(index)); self.entered[index].set()
        if index in self.blocked_calls and not self.release_calls[index].wait(WAIT_SECONDS): raise FixtureTimeout()
        if self.mode=='handled': raise RuntimeError('synthetic handled persistence failure')
        if self.mode=='base': raise self.signal
        return {'fake_ack':True}
    def release(self):
        for event in self.release_calls.values(): event.set()


class Harness:
    def __init__(self,items,*,blocked_calls=(),mode='success',signal=None,block_second=False,ack_error=None,old_body=False):
        self.log=[]; self.received_pairs=[]; self.expected={id(value):id(size) for value,size in items}
        self.metrics={'persisted':0,'last':None,'queued_bytes':sum(size for _,size in items),'queued_items':len(items)}
        self.failures=[]; self.errors=[]; self.stopped=False
        self.queue=Queue(items,self.log,block_second=block_second,ack_error=ack_error)
        self.store=Store(self.log,blocked_calls=blocked_calls,mode=mode,signal=signal)
        current=nodes(); run=deepcopy(current['_run'])
        if old_body:
            run.body[0].body=[part for part in run.body[0].body if not (isinstance(part,ast.Delete) and any(isinstance(target,ast.Name) and target.id=='cycle' for target in part.targets))]
        namespace={'__builtins__':{'Exception':Exception,'type':type},'_QUEUE':self.queue,'_STORE':self.store,
                   '_METRICS':self.metrics,'_METRICS_LOCK':threading.Lock(),'utc_now':lambda:STAMP,
                   '_metric_failure':self.metric_failure}
        module=ast.fix_missing_locations(ast.Module(body=[deepcopy(current['_persist_one']),run],type_ignores=[]))
        exec(compile(module,'<current-worker-lifetime-fixture>','exec'),namespace)
        persist=namespace['_persist_one']
        def checked_persist(value,size):
            if id(value) not in self.expected or self.expected[id(value)]!=id(size): raise AssertionError('cycle or size identity changed')
            self.received_pairs.append((id(value),id(size)))
            return persist(value,size)
        namespace['_persist_one']=checked_persist
        self.run=namespace['_run']; self.thread=threading.Thread(target=self.target,name='consumed-lifetime-fixture',daemon=True)
    def metric_failure(self,name,cycle_id):
        self.failures.append((name,cycle_id)); self.log.append('FAIL:'+name)
    def target(self):
        try: self.run()
        except StopFixture: self.stopped=True
        except BaseException as error: self.errors.append(error)
    def start(self): self.started=time.monotonic(); self.thread.start()
    def pending(self,event):
        if not event.wait(.5): raise AssertionError('worker did not reach expected boundary')
        if not self.thread.is_alive() or self.errors or self.stopped: raise AssertionError('worker no longer pending')
    def __enter__(self): return self
    def __exit__(self,*args):
        self.queue.release(); self.store.release()
        if self.thread.ident is not None: self.thread.join(WAIT_SECONDS)
        if self.thread.is_alive(): raise AssertionError('fixture thread did not join')
        if hasattr(self,'started') and time.monotonic()-self.started>=2.: raise AssertionError('fixture exceeded2s bound')
        # Tracebacks are retained only for identity assertions inside this fixture.
        for error in self.errors: error.__traceback__=None
        self.errors.clear()


class LifetimeTests(unittest.TestCase):
    def test_only_normal_path_release_follows_successful_task_done(self):
        expected=ast.parse('''def _run():
    while True:
        cycle, size = _QUEUE.get()
        _persist_one(cycle, size)
        _QUEUE.task_done()
        del cycle
''').body[0]
        self.assertEqual(ast.dump(nodes()['_run'],include_attributes=False),ast.dump(expected,include_attributes=False))

    def exercise_release(self,mode):
        payload=Payload(); ref=weakref.ref(payload); value=cycle(payload,1); size=int('100001'); expected_pair=(id(value),id(size))
        with Harness([(value,size)],blocked_calls=(1,),mode=mode) as h:
            del value,payload
            h.start(); h.pending(h.store.entered[1]); self.assertFalse(h.queue.items)
            gc.collect(); self.assertIsNotNone(ref(),'in-flight persistence lost its payload')
            h.store.release_calls[1].set(); h.pending(h.queue.empty_pending)
            gc.collect(); self.assertIsNone(ref(),'last cycle remained while waiting for the next get')
            self.assertTrue(h.thread.is_alive()); self.assertFalse(h.stopped)
            self.assertEqual(h.received_pairs,[expected_pair]); self.assertEqual(h.queue.get_calls,2)
            self.assertEqual(h.queue.ack_attempts,1); self.assertEqual(h.queue.ack_done,1)
            self.assertEqual(h.metrics['queued_items'],0); self.assertEqual(h.metrics['queued_bytes'],0)
            if mode=='handled':
                self.assertEqual(h.metrics['persisted'],0); self.assertEqual(h.failures,[('RuntimeError','synthetic-cycle-1')])
                self.assertEqual(h.log,['GET:1','PERSIST:1','FAIL:RuntimeError','ACK:1','GET:2'])
            else:
                self.assertEqual(h.metrics['persisted'],1); self.assertEqual(h.failures,[])
                self.assertEqual(h.log,['GET:1','PERSIST:1','ACK:1','GET:2'])
        gc.collect(); self.assertIsNone(ref())

    def test_success_keeps_inflight_graph_then_releases_before_next_blocked_get(self): self.exercise_release('success')
    def test_handled_persist_failure_keeps_inflight_graph_then_releases_before_next_get(self): self.exercise_release('handled')

    def test_old_body_causally_retains_the_same_graph_at_the_same_boundary(self):
        payload=Payload(); ref=weakref.ref(payload); value=cycle(payload,1)
        with Harness([(value,100001)],old_body=True) as h:
            del value,payload
            h.start(); h.pending(h.queue.empty_pending); gc.collect()
            self.assertIsNotNone(ref()); self.assertTrue(h.thread.is_alive()); self.assertFalse(h.stopped)
            self.assertEqual(h.queue.ack_done,1); self.assertFalse(h.queue.items)
        gc.collect(); self.assertIsNone(ref())

    def test_two_queued_cycles_release_each_graph_without_releasing_next_inflight(self):
        p1,p2=Payload(),Payload(); ref1,ref2=weakref.ref(p1),weakref.ref(p2)
        c1,c2=cycle(p1,1),cycle(p2,2); size1,size2=int('100001'),int('200002'); expected=[(id(c1),id(size1)),(id(c2),id(size2))]
        with Harness([(c1,size1),(c2,size2)],block_second=True,blocked_calls=(2,)) as h:
            del c1,c2,p1,p2
            h.start(); h.pending(h.queue.second_pending); gc.collect()
            self.assertIsNone(ref1()); self.assertIsNotNone(ref2()); self.assertEqual(h.queue.ack_done,1)
            h.queue.release_second.set(); h.pending(h.store.entered[2]); gc.collect()
            self.assertIsNone(ref1()); self.assertIsNotNone(ref2()); self.assertFalse(h.queue.items)
            h.store.release_calls[2].set(); h.pending(h.queue.empty_pending); gc.collect()
            self.assertIsNone(ref1()); self.assertIsNone(ref2()); self.assertTrue(h.thread.is_alive())
            self.assertEqual(h.received_pairs,expected); self.assertEqual(h.queue.ack_done,2)
            self.assertEqual(h.metrics['persisted'],2); self.assertEqual(h.metrics['queued_items'],0); self.assertEqual(h.metrics['queued_bytes'],0)
            self.assertEqual(h.log,['GET:1','PERSIST:1','ACK:1','GET:2','PERSIST:2','ACK:2','GET:3'])

    def test_unhandled_baseexception_identity_and_no_queue_ack_are_unchanged(self):
        for old in (False,True):
            with self.subTest(old_body=old):
                payload=Payload(); value=cycle(payload,1); signal=NativeSignal('synthetic native signal')
                with Harness([(value,100001)],mode='base',signal=signal,old_body=old) as h:
                    del value,payload
                    h.start(); wait_for(lambda:not h.thread.is_alive())
                    self.assertEqual(len(h.errors),1); self.assertIs(h.errors[0],signal)
                    self.assertEqual(h.queue.get_calls,1); self.assertEqual(h.queue.ack_attempts,0)
                    self.assertEqual(h.metrics['queued_items'],0); self.assertEqual(h.metrics['queued_bytes'],0)
                    self.assertEqual(h.log,['GET:1','PERSIST:1']); self.assertEqual(h.failures,[])

    def test_task_done_baseexception_identity_and_attempt_order_are_unchanged(self):
        for old in (False,True):
            with self.subTest(old_body=old):
                payload=Payload(); value=cycle(payload,1); signal=NativeSignal('synthetic ack signal')
                with Harness([(value,100001)],ack_error=signal,old_body=old) as h:
                    del value,payload
                    h.start(); wait_for(lambda:not h.thread.is_alive())
                    self.assertEqual(len(h.errors),1); self.assertIs(h.errors[0],signal)
                    self.assertEqual(h.queue.get_calls,1); self.assertEqual(h.queue.ack_attempts,1); self.assertEqual(h.queue.ack_done,0)
                    self.assertEqual(h.metrics['persisted'],1); self.assertEqual(h.log,['GET:1','PERSIST:1','ACK:1'])


if __name__=='__main__': unittest.main()
