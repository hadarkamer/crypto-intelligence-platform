"""Private durable handoff for the single verified, schema-limited market observation."""
import hashlib
import hmac
import os
import re
from uuid import UUID

from qa_aesgcm import AESGCM
from retained_evidence import MAX_IMAGE, MAX_JSON, bounded_read, safe_root, unpack


def _binding(key, job, digest):
    if len(key) != 32 or str(UUID(job)) != job or not re.fullmatch('[0-9a-f]{64}', digest):
        raise ValueError('market_reference_invalid')
    domain = b'coinglass.qa.market.evidence.v1\0'
    identity = domain + job.encode('ascii') + b'\0' + digest.encode('ascii')
    oid = 0x80000000 | int.from_bytes(hmac.digest(key, identity, 'sha256')[:4], 'big')
    return AESGCM(hmac.digest(key, domain + b'encryption', 'sha256')), identity, oid


def _owner_only(conn, oid):
    row = conn.execute("""SELECT m.lomowner=(SELECT oid FROM pg_catalog.pg_roles WHERE rolname=current_user)
         AND current_setting('lo_compat_privileges')='off'
         AND NOT EXISTS (SELECT 1 FROM pg_catalog.aclexplode(coalesce(m.lomacl,pg_catalog.acldefault('L',m.lomowner))) a
         WHERE a.grantee<>m.lomowner) AS owner_only
         FROM pg_catalog.pg_largeobject_metadata m WHERE m.oid=%s::oid""", (oid,)).fetchone()
    if row is not None and row['owner_only'] is not True:
        raise ValueError('market_acl_invalid')
    return row is not None


def retrieve(conn, key, job, digest, oid):
    codec, aad, expected_oid = _binding(key, job, digest)
    if oid != expected_oid:
        raise ValueError('market_reference_invalid')
    with conn.transaction():
        if not _owner_only(conn, oid):
            raise ValueError('market_reference_missing')
        bound = MAX_IMAGE + MAX_JSON + 548
        envelope = bytes(conn.execute('SELECT pg_catalog.lo_get(%s::oid,0,%s) AS payload',
                         (oid, bound + 1)).fetchone()['payload'])
    if not 28 < len(envelope) <= bound:
        raise ValueError('market_envelope_invalid')
    blob = codec.decrypt(envelope[:12], envelope[12:], aad)
    if hashlib.sha256(blob).hexdigest() != digest:
        raise ValueError('market_digest_invalid')
    unpack(blob, job)
    return blob


def retain_verified(conn, key, job, digest):
    blob = bounded_read(safe_root()/(job+'.map'), MAX_IMAGE+MAX_JSON+520)
    return store_verified_bytes(conn,key,job,digest,blob)


def store_verified_bytes(conn,key,job,digest,blob):
    codec, aad, oid = _binding(key, job, digest)
    if hashlib.sha256(blob).hexdigest() != digest:
        raise ValueError('market_digest_invalid')
    unpack(blob, job)
    with conn.transaction():
        if not _owner_only(conn, oid):
            nonce = os.urandom(12)
            cipher = nonce + codec.encrypt(nonce, blob, aad)
            stored_oid = conn.execute('SELECT pg_catalog.lo_from_bytea(%s::oid,%s::bytea) AS oid',
                                      (oid,cipher)).fetchone()['oid']
            if stored_oid != oid or not _owner_only(conn, oid):
                raise ValueError('market_private_write_failed')
    if not hmac.compare_digest(retrieve(conn,key,job,digest,oid),blob):
        raise ValueError('market_readback_failed')
    return {'oid':oid,'job':job,'sha256':digest,'bytes':len(blob),'encrypted':True,
            'owner_only':True,'readback':True}
