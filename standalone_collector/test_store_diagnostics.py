"""Exercise installed job routes and health without a real database or source."""
import asyncio
import contextlib
import io
import json
import os
from pathlib import Path
from types import SimpleNamespace
import sys
import unittest
from unittest.mock import AsyncMock,Mock,patch

from aiohttp import web
from psycopg.errors import UndefinedColumn

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE/'runtime'))
import collection_bridge as bridge
import store_diagnostics as diagnostics
import app as collector_app

TOKEN='synthetic-token-for-offline-test-only-0000'
JOB='00000000-0000-4000-8000-000000000001'


def handler(application,path,method='GET'):
    return next(route.handler for route in application.router.routes()
        if route.method==method and route.resource.canonical==path)


class StoreDiagnosticsTests(unittest.TestCase):
    def store(self):
        return Mock(dsn='synthetic',hourly_limit=2)

    def test_installed_route_errors_keep_503_and_log_only_fixed_metadata(self):
        for action in ('start','get','latest','evidence'):
            with self.subTest(action=action):
                store=self.store();getattr(store,action).side_effect=UndefinedColumn('SECRET DSN SQL BODY')
                application=web.Application()
                with patch.object(bridge,'JobStore',return_value=store):
                    bridge.register_collection_routes(application,store=store,token=TOKEN,enabled=True,start_worker=False)
                body=json.dumps({'request_id':JOB,'timeframe':'12H'}).encode()
                request=SimpleNamespace(headers={'Authorization':'Bearer '+TOKEN},
                    content_length=len(body),content=SimpleNamespace(read=AsyncMock(side_effect=[body,b''])),
                    match_info={'job_id':JOB},query={'timeframe':'12H'})
                suffix={'start':'','get':'/{job_id}','latest':'/latest','evidence':'/{job_id}/evidence'}[action]
                with contextlib.redirect_stdout(io.StringIO()) as output:
                    response=asyncio.run(handler(application,'/api/collection/model3/jobs'+suffix,
                        'POST' if action=='start' else 'GET')(request))
                self.assertEqual(response.status,503)
                self.assertEqual(json.loads(response.body)['error']['code'],'store_unavailable')
                payload=json.loads(output.getvalue().split(' ',1)[1])
                self.assertEqual(payload,{'operation':action,'exception_class':'UndefinedColumn','sqlstate':'42703'})
                self.assertNotIn('SECRET',output.getvalue()+response.text)
                self.assertNotIn(TOKEN,output.getvalue()+response.text)

    def test_known_no_result_error_is_not_store_failure(self):
        store=self.store();store.get.return_value=None
        application=web.Application()
        with patch.object(bridge,'JobStore',return_value=store):
            bridge.register_collection_routes(application,store=store,token=TOKEN,enabled=True,start_worker=False)
        request=SimpleNamespace(headers={'Authorization':'Bearer '+TOKEN},match_info={'job_id':JOB})
        with contextlib.redirect_stdout(io.StringIO()) as output:
            response=asyncio.run(handler(application,'/api/collection/model3/jobs/{job_id}')(request))
        self.assertEqual(response.status,404);self.assertEqual(output.getvalue(),'')

    def test_unrecognized_exception_metadata_never_prints_its_private_text(self):
        SecretClass=type('PRIVATE_TOKEN_CLASS',(Exception,),{})
        error=SecretClass('PRIVATE_MESSAGE');error.sqlstate='secret-connection-value'
        with contextlib.redirect_stdout(io.StringIO()) as output:
            diagnostics.log_store_failure('private-operation',error)
        payload=json.loads(output.getvalue().split(' ',1)[1])
        self.assertEqual(payload,{'operation':'unknown','exception_class':'Exception','sqlstate':None})
        self.assertNotIn('PRIVATE',output.getvalue());self.assertNotIn('secret',output.getvalue())

    def test_readiness_queries_once_without_initialization_or_writes(self):
        store=self.store();connection=Mock()
        connection.__enter__=Mock(return_value=connection);connection.__exit__=Mock(return_value=False)
        store.connect.return_value=connection
        connection.execute.return_value.fetchone.return_value={'ok':1}
        self.assertEqual(diagnostics.store_readiness(store),{'checked':True,'ready':True})
        store.connect.assert_called_once_with();connection.execute.assert_called_once_with('SELECT 1 AS ok')
        store.initialize.assert_not_called();store.start.assert_not_called()
        self.assertEqual(diagnostics.store_readiness(None),{'checked':False,'ready':False})

    def test_health_separates_configured_mode_from_failed_store_connection(self):
        store=self.store();store.connect.side_effect=UndefinedColumn('SECRET connection details')
        environment={'DATABASE_URL':'synthetic','OPENAI_API_KEY':'synthetic','COINGLASS_COLLECTOR_TOKEN':TOKEN,
            'COLLECTION_BRIDGE_ENABLED':'true'}
        with patch.dict(os.environ,environment),patch.object(collector_app,'source_session_configured',return_value=True),\
            patch.object(bridge,'JobStore',return_value=store):
            application=collector_app.create_app()
        with contextlib.redirect_stdout(io.StringIO()) as output:
            response=asyncio.run(handler(application,'/health')(None))
        result=json.loads(response.body)
        self.assertEqual(response.status,200);self.assertEqual(result['mode'],'collection')
        self.assertEqual(result['store_connectivity'],{'checked':True,'ready':False})
        self.assertEqual(result['supported_models'],[1,2,3])
        self.assertNotIn('SECRET',response.text+output.getvalue())
        self.assertIn('"operation": "readiness"',output.getvalue())
        store.initialize.assert_not_called();store.claim.assert_not_called()


if __name__=='__main__':unittest.main()
