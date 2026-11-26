"""Single-instance, single-job QA endpoint used by an identified private app CI run only."""
import hashlib
import hmac
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
from pathlib import Path
import secrets
import stat
from threading import Lock
import time
from urllib.parse import urlsplit
from uuid import UUID

ROOT = Path('/tmp/cg-budget')
ROUTE = '/api/collection/model1/jobs'
HEALTH = {'ok': True, 'supported_models': [1], 'supported_symbols': ['BTC'],
          'supported_timeframes': ['12H'], 'selection_policy': 'prominent_right_edge.v1'}


def read_checked(name, limit):
    fd = os.open(ROOT/name, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, 'rb') as file:
        meta = os.fstat(file.fileno())
        if not stat.S_ISREG(meta.st_mode) or stat.S_IMODE(meta.st_mode) != 0o600 or meta.st_uid != os.geteuid() or not 0 < meta.st_size <= limit:
            raise ValueError('invalid_private_record')
        return json.loads(file.read(limit+1))


def write_once(name, obj):
    raw = json.dumps(obj, separators=(',', ':')).encode()
    tmp = ROOT/('.qa-wire-' + secrets.token_hex(16))
    fd = os.open(tmp, os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_WRONLY, 0o600)
    try:
        with os.fdopen(fd, 'wb') as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.link(tmp, ROOT/name, follow_symlinks=False)
    finally:
        tmp.unlink(missing_ok=True)


class Gate:
    def __init__(self, service):
        self.service = service
        self.lock = Lock()
        self.token_hash = None

    def pin(self):
        directory = ROOT.lstat()
        if not stat.S_ISDIR(directory.st_mode) or directory.st_uid != os.geteuid() or stat.S_IMODE(directory.st_mode) not in (0o700,0o2700):
            raise ValueError('untrusted_directory')
        config=read_checked('trial.json',4096)
        if config['service'] != self.service or not config['created_epoch'] < time.time() < config['created_epoch'] + 13*60:
            raise ValueError('inactive_trial')
        pin=read_checked('ui-ready.json',4096)
        if pin['service'] != self.service or pin['deadline'] < time.time():
            raise ValueError('app_listener_inactive')
        return pin

    def exchange(self, jwt):
        from ci_identity import verify
        with self.lock:
            pin=self.pin()
            if (ROOT/'ui-session.json').exists() or len(jwt)>12288 or not verify(jwt,pin):
                raise PermissionError()
            token=secrets.token_urlsafe(36)
            write_once('ui-session.json', {'run_id':pin['run_id'],'run_attempt':1,'at':time.time()})
            self.token_hash=hashlib.sha256(token.encode('ascii')).digest()
            return {'token':token}

    def authenticated(self, value):
        self.pin()
        return self.token_hash is not None and len(value)<128 and hmac.compare_digest(
            self.token_hash, hashlib.sha256(value.encode('ascii')).digest())

    def event(self):
        try:return read_checked('ui_job.json',2048)
        except FileNotFoundError:return None

    def submit(self, body):
        if type(body) is not dict or 'request_id' not in body or set(body)-{'request_id','symbol','timeframe','attempt'}:
            raise ValueError('invalid_request')
        uid=body['request_id']
        if type(uid) is not str or str(UUID(uid))!=uid or body.get('symbol','BTC')!='BTC' or body.get('timeframe')!='12H' or type(body.get('attempt',1)) is not int or body.get('attempt',1)!=1:
            raise ValueError('invalid_selection')
        with self.lock:
            if (prior:=self.event()) is not None:
                if prior['job_id']!=uid:raise FileExistsError()
                return self.status(uid)
            event=dict(service=self.service,job_id=uid,symbol='BTC',model=1,timeframe='12H',origin='test_application')
            write_once('ui_job.json',event)
            write_once('ui-origin.json',{'job_id':uid,'at':time.time(),'run_id':self.pin()['run_id'],'source':'private_test_application'})
            return self.status(uid)

    def status(self, uid):
        job=self.event()
        if not job or job['job_id']!=uid:raise FileNotFoundError()
        result=None
        error=None
        state='running' if (ROOT/'trial-started').exists() else 'queued'
        try:final=read_checked('ui_final.json',512*1024)
        except FileNotFoundError:final=None
        if final:
            if final.get('job_id')!=uid or final.get('status') not in ('ready','failed'):raise ValueError('invalid_final')
            result,error,state=final.get('result'),final.get('error'),final['status']
            if state=='ready' and (not isinstance(result,dict) or result.get('run_id')!=uid or
                (result.get('symbol'),result.get('heatmap_model'),result.get('timeframe'))!=('BTC',1,'12H')):raise ValueError('invalid_result')
            try:write_once('ui-final-read.json',dict(job_id=uid,status=state,at=time.time()))
            except FileExistsError:pass
        return dict(job_id=uid,schema_version='coinglass-model1.v1',symbol='BTC',timeframe='12H',
                    status=state,result=result,error=error)

def make_server(port,service):
    gate=Gate(service)
    class Handler(BaseHTTPRequestHandler):
        server_version='QA'
        sys_version=''
        def log_message(self,*_):pass
        def send(self,code,data):
            raw=json.dumps(data,allow_nan=False,separators=(',',':')).encode()
            self.send_response(code)
            self.send_header('Content-Type','application/json')
            self.send_header('Content-Length',str(len(raw)))
            self.send_header('Cache-Control','no-store')
            self.send_header('Connection','close');self.end_headers()
            if self.command!='HEAD':self.wfile.write(raw)
        def handle_request(self):
            try:
                parsed=urlsplit(self.path);path=parsed.path
                if self.command in ('GET','HEAD') and path=='/health':return self.send(200,HEALTH)
                if self.command=='GET' and path=='/qa/ready':
                    try:gate.pin()
                    except (FileNotFoundError,ValueError):return self.send(503,{'ready':False})
                    return self.send(200,{'ready':True})
                auth=self.headers.get('Authorization','')
                if not auth.startswith('Bearer '):return self.send(401,{'error':'unauthorized'})
                if self.command=='POST' and path=='/qa/session' and not parsed.query:
                    if int(self.headers.get('Content-Length','0'))>2:return self.send(400,{'error':'invalid_request'})
                    return self.send(200,gate.exchange(auth[7:]))
                if not gate.authenticated(auth[7:]):raise PermissionError()
                if self.command=='POST' and path==ROUTE and not parsed.query:
                    length=int(self.headers.get('Content-Length','0'))
                    if not 0<length<=1024 or self.headers.get_content_type()!='application/json':raise ValueError('invalid_request')
                    return self.send(202,gate.submit(json.loads(self.rfile.read(length))))
                if self.command=='GET' and path.startswith(ROUTE+'/'):
                    uid=path[len(ROUTE)+1:]
                    if uid=='latest':return self.send(404,{'error':'no_cached_result'})
                    if parsed.query not in ('','symbol=BTC'):raise ValueError('invalid_query')
                    return self.send(200,gate.status(uid))
                return self.send(404,{'error':'not_found'})
            except (PermissionError,UnicodeError):return self.send(403,{'error':'unauthorized'})
            except FileNotFoundError:return self.send(404,{'error':'not_found'})
            except FileExistsError:return self.send(409,{'error':'single_job_only'})
            except (ValueError,KeyError,TypeError,OverflowError):return self.send(400,{'error':'invalid_request'})
            except (OSError,TimeoutError):return self.send(503,{'error':'unavailable'})
        do_POST=do_GET=do_HEAD=handle_request
    class Server(HTTPServer):
        def get_request(self):
            sock,peer=super().get_request();sock.settimeout(4);return sock,peer
        def handle_error(self,*_):pass
    return Server(('0.0.0.0',port),Handler)
