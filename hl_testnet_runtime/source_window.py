"""A source keeps its own timestamp and bounded ORIGINAL outbox expiry.

Legacy callers retain the 60-second test window. An explicitly supervised
attempt may supply the source outbox's recorded expiry (at most 10 minutes),
plus a shorter operator-authorization deadline. Neither timestamp is reset.
"""
from datetime import datetime, timezone, timedelta


def timestamp(value):
    if not isinstance(value, str) or len(value) > 40:
        raise ValueError('UTC_TIME_REQUIRED')
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.utcoffset() is None:
        raise ValueError('UTC_TIME_REQUIRED')
    return result.astimezone(timezone.utc)


def source_fresh(at, source_expires_at=None, approval_expires_at=None, *, now=None):
    try:
        instant = datetime.now(timezone.utc) if now is None else now
        if at.utcoffset() is None or instant.utcoffset() is None:
            return False
        end = at + timedelta(seconds=60)
        if source_expires_at is not None:
            end = timestamp(source_expires_at)
            if not 0 < (end-at).total_seconds() <= 600:
                return False
        if approval_expires_at is not None:
            end = min(end, timestamp(approval_expires_at))
        return at <= instant < end
    except (TypeError, ValueError, OverflowError, AttributeError):
        return False
