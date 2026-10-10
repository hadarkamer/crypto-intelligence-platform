"""Bounded store facts; never log a DSN, SQL text or exception message."""
import json
import re

OPERATIONS=frozenset({'start','get','latest','evidence','readiness'})
EXCEPTION_CLASSES=frozenset({
    'Error','OperationalError','InterfaceError','DatabaseError','ProgrammingError',
    'IntegrityError','DataError','InternalError','NotSupportedError',
    'UndefinedTable','UndefinedColumn','LockNotAvailable','QueryCanceled',
    'InvalidDatetimeFormat','DatetimeFieldOverflow','TooManyConnections',
    'CannotConnectNow','AdminShutdown','CrashShutdown','InvalidPassword',
    'InvalidAuthorizationSpecification','ConnectionException','ConnectionDoesNotExist',
    'ConnectionFailure','CheckViolation','UniqueViolation','ForeignKeyViolation',
    'DeadlockDetected','SerializationFailure','TypeError','ValueError','KeyError',
    'AttributeError','OSError','TimeoutError',
})


def log_store_failure(operation,error):
    kind=type(error).__name__
    try:state=getattr(error,'sqlstate',None)
    except Exception:state=None
    payload={
        'operation':operation if operation in OPERATIONS else 'unknown',
        'exception_class':kind if kind in EXCEPTION_CLASSES else 'Exception',
        'sqlstate':state if isinstance(state,str) and re.fullmatch(r'[0-9A-Z]{5}',state) else None,
    }
    print('HEATMAP_STORE_FAILURE '+json.dumps(payload),flush=True)


def store_readiness(store):
    if store is None:return {'checked':False,'ready':False}
    try:
        with store.connect() as connection:
            row=connection.execute('SELECT 1 AS ok').fetchone()
            ready=isinstance(row,dict) and row.get('ok')==1
        return {'checked':True,'ready':ready}
    except Exception as error:
        log_store_failure('readiness',error)
        return {'checked':True,'ready':False}
