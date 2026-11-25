"""Read-only, per-job QA observer; no source request, field values, DOM text or raw URL stored."""
from pathlib import Path
from urllib.parse import urlsplit
import json
import os
import stat
import tempfile
from uuid import UUID

REPORT = Path('/tmp/coinglass-btc-pilot-diagnostics/first-ui.json')
FORM_CODES = {'login_attempt_not_reserved','login_captcha_required','login_credentials_missing',
 'login_credentials_rejected','login_diagnostics_enabled','login_form_unavailable','login_form_untrusted',
 'login_network_transient','login_outcome_unconfirmed','login_rate_limited','login_submitted','login_verification_required'}
ERROR_CODES = {'source_login_required','source_not_readable','source_capture_failed','source_timeout',
 'source_navigation_changed','source_rate_limited','source_map_unavailable','state_revision_conflict',
 'state_store_failed','session_transport_failed','session_input_invalid','session_snapshot_failed',
 'session_observation_failed','auth_challenge','auth_code_required','auth_invalid_credentials',
 'auth_rate_limited','auth_gate_after_verification','auth_unconfirmed','auth_form_unavailable',
 'auth_configuration_invalid','auth_retry_exhausted','auth_protected_proof_required','auth_in_progress',
 'auth_retry_pending','auth_attempt_not_reserved','worker_crashed'}
SELECTORS = {
 'email_visible':'input[type=email],input[autocomplete=username],input[placeholder="Email" i]',
 'password_visible':'input[type=password]:not([autocomplete=new-password])',
 'challenge_visible':'iframe[src*="captcha" i],iframe[src*="challenges.cloudflare.com" i],[data-sitekey]',
 'code_visible':'input[autocomplete=one-time-code],input[name=otp i]',
}


def path_kind(url):
    try:
        p=urlsplit(url)
        if p.scheme=='about' and p.path=='blank':return 'blank'
        if p.scheme!='https' or p.netloc.lower() not in ('coinglass.com','www.coinglass.com'):
            return 'other_origin'
        path=p.path.rstrip('/')
        return {'/login':'official_login','':'official_home','/LiquidationHeatMap/BTC':'btc_map'}.get(path,'other_official')
    except (ValueError,TypeError):
        return 'unknown'


def private_write(values):
    folder=REPORT.parent
    try:
        folder.mkdir(mode=0o700,exist_ok=True)
        info=folder.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid!=os.geteuid() or info.st_mode & 0o077:
            return
        fd,path=tempfile.mkstemp(dir=folder,prefix='.pending-')
        try:
            with os.fdopen(fd,'w') as out:
                json.dump(values,out,separators=(',',':'))
            os.replace(path,REPORT)
        finally:
            Path(path).unlink(missing_ok=True)
    except OSError:
        pass


def install():
    import dedicated_map_flow as flow
    from session_capture import CaptureSession
    try: ident=str(UUID(os.environ['COINGLASS_SESSION_RUN_ID']))
    except (KeyError,TypeError,ValueError):return
    data={'job_id':ident,'attached':False,'submit_reserved':False,'form_reported_submitted':False,
          'first_navigation':None,'initial_controls':None,'before_submit_controls':None,
          'final_controls':None,'page_closed':False,'form_result':None,'stage_failure':None}
    private_write(data)
    attach, finish = CaptureSession.attach, CaptureSession.finish
    note, submit = CaptureSession.note_source_error, flow.submit_official_login
    def controls(page):
        shot={'path':path_kind(page.url)}
        if shot['path'] not in ('official_login','btc_map','official_home','other_official'):return shot
        for key,selector in SELECTORS.items():
            try:
                choices=page.locator(selector)
                shot[key]=any(choices.nth(i).is_visible() for i in range(min(choices.count(),4)))
            except Exception:shot[key]=None
        for key,label in (('verification_prompt','Verify you are human'),
                          ('security_check','Performing security verification'),
                          ('request_blocked','Sorry, you have been blocked')):
            try: shot[key]=page.get_by_text(label,exact=False).first.is_visible()
            except Exception: shot[key]=None
        try: shot['interstitial_title']=page.title().strip().lower() in ('just a moment...', 'attention required! | cloudflare')
        except Exception: shot['interstitial_title']=None
        return shot
    def observe(key,page):
        try:data[key]=controls(page);private_write(data)
        except Exception:pass
    def attach_probe(self,context,page):
        result=attach(self,context,page)
        def on_response(response):
            try:
                req=response.request
                if (data['first_navigation'] is None and req.is_navigation_request()
                        and req.frame==page.main_frame and 100<=response.status<=599):
                    data['first_navigation']={'path':path_kind(response.url),'http_status':int(response.status)}
                    private_write(data)
            except Exception:pass
        def on_ready(*_):
            if data['initial_controls'] is None:observe('initial_controls',page)
        def on_close(*_):
            data['page_closed']=True;private_write(data)
        try:
            page.on('response',on_response);page.on('domcontentloaded',on_ready);page.on('close',on_close)
            data['attached']=True;private_write(data)
        except Exception:pass
        return result
    def finish_probe(self,context):
        page=getattr(self,'page',None)
        if page is not None:observe('final_controls',page)
        return finish(self,context)
    def source_error(self,error):
        code=getattr(error,'code',None)
        data['stage_failure']=code if isinstance(code,str) and code in ERROR_CODES else 'unclassified'
        private_write(data);return note(self,error)
    def form_probe(page,*args,**kwargs):
        before=kwargs.get('before_submit')
        if callable(before):
            def mark():
                observe('before_submit_controls',page)
                decision=before()
                if decision is not False:
                    data['submit_reserved']=True;private_write(data)
                return decision
            kwargs['before_submit']=mark
        result=submit(page,*args,**kwargs)
        observe('after_form_controls',page)
        data['form_result']=result.code if result.code in FORM_CODES else 'unclassified'
        data['form_reported_submitted']=bool(result.submitted)
        private_write(data)
        return result
    CaptureSession.attach=attach_probe
    CaptureSession.finish=finish_probe
    CaptureSession.note_source_error=source_error
    flow.submit_official_login=form_probe
