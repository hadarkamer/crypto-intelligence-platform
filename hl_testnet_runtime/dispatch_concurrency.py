"""Serialize same-process public reconciliation and actions per market bucket.

Database revision, nonce and intent fences still protect separate processes.
Reentrant locks let an emergency cycle call normal refresh without deadlocking;
unrelated markets do not wait behind each other's network reads.
"""
from functools import wraps
import threading

_guard = threading.Lock()
_lanes = {}


def market_lane(method):
    @wraps(method)
    def run(self, bucket, *args, **kwargs):
        with _guard:
            lane = _lanes.setdefault((self.store.domain, bucket), threading.RLock())
        with lane:
            return method(self, bucket, *args, **kwargs)
    return run
