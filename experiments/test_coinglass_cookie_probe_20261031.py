import unittest
from coinglass_cookie_probe_20261031 import observe_call


class ObservationTests(unittest.TestCase):
    def test_success_keeps_original_return_and_calls_once(self):
        calls=[]; marker=object(); page=object(); record={}
        def original(actual, **kwargs):
            calls.append((actual,kwargs));return marker
        result=observe_call(original,page,record,lambda:{'visible':False},lambda label:{'label':label},model=3)
        self.assertIs(result,marker)
        self.assertEqual(calls,[(page,{'model':3})])
        self.assertTrue(record['original_succeeded'])

    def test_observation_failures_do_not_replace_source_exception(self):
        source_error=RuntimeError('unchanged source failure');calls=[];record={}
        def original(page, **kwargs):calls.append(1);raise source_error
        def bad(*args):raise ValueError('observer unavailable')
        with self.assertRaises(RuntimeError) as got:
            observe_call(original,None,record,bad,bad,model=3)
        self.assertIs(got.exception,source_error)
        self.assertEqual(calls,[1])
        self.assertEqual(record['after']['observation_error_class'],'ValueError')
        self.assertNotIn('observer unavailable',str(record))

    def test_before_after_states_keep_action_order(self):
        calls=[];record={}
        def original(page, **kwargs):calls.append('original')
        def snapshot():calls.append('state');return len(calls)
        def picture(label):calls.append(label);return {'label':label}
        observe_call(original,None,record,snapshot,picture,model=3)
        self.assertEqual(calls,['state','before','state','original','state','after'])


if __name__=='__main__':unittest.main()
