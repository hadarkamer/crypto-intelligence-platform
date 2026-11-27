"""Readiness-only source substitute: real Firefox/reader, no remote requests."""
from pathlib import Path
from playwright.async_api import Browser
import json


def install():
    original = Browser.new_context
    html = Path(__file__).with_name('local-source.html').read_text()
    destination = Path('/receipts/source-requests.jsonl')

    async def new_context(browser, *args, **kwargs):
        context = await original(browser, *args, **kwargs)
        async def local(route):
            request = route.request
            with destination.open('a') as out:
                out.write(json.dumps({'url':request.url, 'method':request.method,
                                      'remote_request_sent':False})+'\n')
            if (request.url=='https://www.coinglass.com/LiquidationHeatMap/BTC?mode=coin'
                    and request.method=='GET'):
                await route.fulfill(status=200,content_type='text/html; charset=utf-8',body=html)
            else:
                await route.abort()
        await context.route('**/*',local)
        return context
    Browser.new_context = new_context
