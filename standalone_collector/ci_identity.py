"""Authenticate a single pinned GitHub Actions QA run. Never expose token bytes or claims."""
import base64
import http.client
import json
import re
import ssl
import threading
import time

ISSUER='https://token.actions.githubusercontent.com'
HOST='token.actions.githubusercontent.com'
KEYS_PATH='/.well-known/jwks'
KEYS_LIMIT=131072
_LOCK=threading.Lock()
_CACHED=None
_UNTIL=0.0

def unique(pairs):
    obj=dict(pairs)
    if len(obj)!=len(pairs):raise ValueError()
    return obj

def decode(data,limit):
    if not isinstance(data,str) or not 1<=len(data)<=limit or re.fullmatch('[a-zA-Z0-9_-]+',data) is None:raise ValueError()
    return base64.b64decode(data+'='*(-len(data)%4),altchars=b'-_',validate=True)

def published_keys():
    global _CACHED,_UNTIL
    with _LOCK:
        if _CACHED is not None and time.monotonic()<_UNTIL:return _CACHED
        conn=http.client.HTTPSConnection(HOST,timeout=2.5,context=ssl.create_default_context())
        try:
            conn.request('GET',KEYS_PATH,headers={'Accept':'application/json'})
            resp=conn.getresponse()
            if resp.status!=200:raise ValueError()
            data=resp.read(KEYS_LIMIT+1)
            if len(data)>KEYS_LIMIT:raise ValueError()
        finally:conn.close()
        raw=json.loads(data,object_pairs_hook=unique)
        keys=raw['keys']
        if not isinstance(keys,list) or not 1<=len(keys)<=64:raise ValueError()
        valid={}
        for key in keys:
            if not isinstance(key,dict) or key.get('kty')!='RSA' or key.get('use','sig')!='sig' or key.get('alg','RS256')!='RS256':continue
            kid=key['kid']
            if not isinstance(kid,str) or kid in valid:raise ValueError()
            valid[kid]=key
        if not valid:raise ValueError()
        _CACHED=valid
        _UNTIL=time.monotonic()+180
        return valid

def signature(key,message,sig):
    n=int.from_bytes(decode(key['n'],1400),'big');e=int.from_bytes(decode(key['e'],12),'big')
    if n.bit_length() not in (2048,3072,4096) or e<3 or e%2==0 or len(sig)!=(n.bit_length()+7)//8:raise ValueError()
    try:
        from Crypto.PublicKey import RSA
        from Crypto.Hash import SHA256
        from Crypto.Signature import pkcs1_15
    except ImportError:
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.asymmetric import padding,rsa
        rsa.RSAPublicNumbers(e,n).public_key().verify(sig,message,padding.PKCS1v15(),hashes.SHA256())
    else:pkcs1_15.new(RSA.construct((n,e))).verify(SHA256.new(message),sig)

def verify(token,pin):
    try:
        if not isinstance(token,str) or len(token)>24576 or not isinstance(pin,dict):return False
        h,p,s=token.split('.')
        header=json.loads(decode(h,4096),object_pairs_hook=unique)
        if header.get('alg')!='RS256' or header.get('typ','JWT')!='JWT':return False
        if any(k in header for k in ('jwk','jku','x5u','crit','b64')):return False
        kid=header['kid']
        if not isinstance(kid,str) or not re.fullmatch(r'[A-Za-z0-9._+=/-]{1,160}',kid):return False
        claims=json.loads(decode(p,20000),object_pairs_hook=unique)
        required={'iss':ISSUER,'aud':pin['aud'],'event_name':'pull_request','run_attempt':'1',
                  'repository_visibility':'private','repository':pin['repository'],'repository_id':str(pin['repo_id']),
                  'workflow_ref':pin['workflow_ref'],'workflow_sha':pin.get('workflow_sha',pin['sha']),
                  'sha':pin['sha'],'run_id':str(pin['run_id']),'ref':pin['ref']}
        if 'repo_owner_id' in pin:required['repository_owner_id']=str(pin['repo_owner_id'])
        elif 'repository_owner' in pin:required['repository_owner']=pin['repository_owner']
        if any(not isinstance(v,str) or not v or claims.get(k)!=v for k,v in required.items()):return False
        if not re.fullmatch('[0-9a-f]{40}',required['sha']) or not re.fullmatch('[0-9a-f]{40}',required['workflow_sha']):return False
        if not re.fullmatch(r'[1-9][0-9]*',required['repository_id']) or not re.fullmatch(r'[1-9][0-9]*',required['run_id']):return False
        if not required['workflow_ref'].startswith(required['repository']+'/.github/workflows/'):return False
        exp,nbf,iat=(claims[k] for k in ('exp','nbf','iat'))
        if any(type(t) is not int for t in (exp,nbf,iat)):return False
        now=time.time()
        if exp<=now or iat>now+30 or nbf>now+30 or iat>=exp or nbf>=exp:return False
        key=published_keys().get(kid)
        if key is None:return False
        signature(key,(h+'.'+p).encode('ascii'),decode(s,1500))
        return True
    except Exception:return False
