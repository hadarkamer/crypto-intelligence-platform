"""Exercise the actual batch button and renderer in Chromium, on loopback only."""
from pathlib import Path
import json, os, time
import requests
from playwright.sync_api import sync_playwright
OUT=Path(os.environ['QUEUE_LAB_EVIDENCE'])
URL='http://127.0.0.1:21880'
def call(route,data=None):
    r=requests.get(URL+route) if data is None else requests.post(URL+route,json=data)
    r.raise_for_status();return r.json()
def row(rows,field,tf):
    return next(r for r in next(v['value']['rows'] for v in rows if v['field_id']==field) if r['timeframe']==tf)
checks={}; timings={}; started=time.monotonic()
with sync_playwright() as p:
    browser=p.chromium.launch(headless=True)
    context=browser.new_context()
    context.route('**/*',lambda route:route.continue_() if route.request.url.startswith(URL+'/') else route.abort())
    page=context.new_page(); errors=[]
    page.on('pageerror',lambda e:errors.append(str(e)))
    page.on('dialog',lambda d:d.accept())
    page.goto(URL)
    button=page.get_by_role('button',name='איסוף כל מפות CoinGlass',exact=True)
    button.wait_for();page.wait_for_function("!Array.from(document.querySelectorAll('button')).find(b=>b.textContent==='איסוף כל מפות CoinGlass').disabled")
    def wait_batch(expected, timeout=600000):
        page.wait_for_function("()=>document.querySelector('[role=status]') && Array.from(document.querySelectorAll('button')).some(b=>b.textContent==='איסוף כל מפות CoinGlass' && !b.disabled)",timeout=timeout)
        actual=page.get_by_role('status').inner_text()
        if f'התקדמות: {expected}/9' not in actual:
            (OUT/'failure-ui.html').write_text(page.content())
            (OUT/'failure-dump.json').write_text(json.dumps(call('/dump'),indent=2))
            raise AssertionError(actual)
    call('/wrong-model',{});checks['wrong_model_rejected_before_scan']=True
    t=time.monotonic()
    # Two synchronous activations exercise the real hook's runningRef guard.
    button.evaluate('(b)=>{b.click();b.click()}')
    wait_batch(9)
    timings['baseline_9_wall_seconds']=round(time.monotonic()-t,3)
    first=call('/dump')
    assert len(first['jobs'])==9 and len(first['runs'])==9
    checks['double_click_nine_jobs_nine_commits']=True
    fx=call('/fixture')
    def verify_ui(expected):
        page.wait_for_function("expected=>JSON.stringify(JSON.parse(document.querySelector('#rendered-data').textContent))===JSON.stringify(expected)",arg=expected,timeout=10000)
        expected_inputs={r['field']:r['prices'] for r in call('/expected-inputs')}
        for f in fx['fields']:
            rows=next(v['value']['rows'] for v in expected if v['field_id']==f['id'])
            actual_inputs=page.locator('[data-field=\"'+f['id']+'\"] fieldset input[placeholder=\"מחיר\"]').evaluate_all('(inputs)=>inputs.map(i=>i.value).filter(Boolean)')
            assert sorted(actual_inputs)==sorted(expected_inputs[f['id']]),(actual_inputs,expected_inputs[f['id']])
            assert sorted(r['timeframe'] for r in rows)==['12H','24H','48H']
            for r in rows:
                assert r['hover_snapshot']['heatmap_model']==int(f['field_key'][-1])
                assert r['hover_snapshot']['timeframe']==r['timeframe']
                assert len(r['levels'])==1
                assert page.locator('[data-field="'+f['id']+'"]').inner_text().count(r['timeframe'])>=1
        assert len({r['hover_snapshot']['artifact_id'] for f in fx['fields'] for r in next(v['value']['rows'] for v in expected if v['field_id']==f['id'])})==9
    verify_ui(first['rows']);checks['editable_price_boxes_match_saved_map_values']=True
    page.screenshot(path=str(OUT/'baseline-ui.png'),full_page=True)
    call('/repeat-poll',{});call('/tampered-ticket',{})
    assert len(call('/dump')['runs'])==9
    checks['repeated_poll_no_duplicate_write']=checks['tampered_ticket_rejected']=True
    page.reload();verify_ui(first['rows']);checks['reload_same_persisted_values']=True
    # A second fresh cycle must preserve the old correct 24H on bad AI identity.
    call('/control',{'now':'2026-11-01T16:03:00Z','failedMap':'3:24H'})
    page.reload();button=page.get_by_role('button',name='איסוף כל מפות CoinGlass',exact=True)
    t=time.monotonic();button.click()
    wait_batch(8)
    button.wait_for();page.wait_for_function("!Array.from(document.querySelectorAll('button')).find(b=>b.textContent==='איסוף כל מפות CoinGlass').disabled")
    timings['cycle_with_bad_identity_wall_seconds']=round(time.monotonic()-t,3)
    partial=call('/dump');assert len(partial['jobs'])==18 and len(partial['runs'])==17
    m3=fx['fields'][2]['id']; artifact=lambda rows,tf:row(rows,m3,tf)['hover_snapshot']['artifact_id']
    assert artifact(partial['rows'],'24H')==artifact(first['rows'],'24H')
    for tf in ['12H','48H']:assert artifact(partial['rows'],tf)!=artifact(first['rows'],tf)
    checks['wrong_ai_identity_rejected_old_value_preserved']=True
    call('/control',{'now':'2026-11-01T16:03:00Z','failedMap':None})
    t=time.monotonic();button.click()
    wait_batch(9,180000)
    timings['single_failed_map_retry_wall_seconds']=round(time.monotonic()-t,3)
    final=call('/dump');assert len(final['jobs'])==19 and len(final['runs'])==18
    assert artifact(final['rows'],'24H')!=artifact(first['rows'],'24H')
    for f in fx['fields']:
        for tf in ['12H','24H','48H']:
            if f['id']==m3 and tf=='24H':continue
            assert row(final['rows'],f['id'],tf)==row(partial['rows'],f['id'],tf)
    checks['retry_only_failed_map']=True
    verify_ui(final['rows']);page.reload();verify_ui(final['rows'])
    # Compare every save with its exact input map/model/timeframe and evidence.
    for run in final['runs']:
        job=next(j for j in final['jobs'] if j['id']==run['analysis']['collector_run_id'])
        f=next(f for f in fx['fields'] if f['id']==run['field_id'])
        assert job['model']==int(f['field_key'][-1])
        assert job['timeframe']==run['timeframe']
    checks['every_commit_has_matching_job_model_timeframe']=True
    note=next(v['value'] for v in final['rows'] if v['field_id']==fx['fields'][0]['id'])
    assert note['note']=='Existing human note' and note['images']==[{'storage_path':'keep-image'}]
    assert {v['field_id'] for v in final['rows']}=={f['id'] for f in fx['fields']}
    checks['manual_note_image_and_unrelated_fields_preserved']=True
    assert not errors,errors
    checks['browser_no_uncaught_errors']=True
    page.screenshot(path=str(OUT/'final-ui.png'),full_page=True)
    (OUT/'rendered.html').write_text(page.content())
    browser.close()
(OUT/'browser-report.json').write_text(json.dumps({'status':'passed','checks':checks,'timings':timings,'total_wall_seconds':round(time.monotonic()-started,3),'fixture':fx},indent=2))
call('/finish',{})
print(json.dumps({'stage':'actual-browser','checks':checks,'timings':timings}),flush=True)
