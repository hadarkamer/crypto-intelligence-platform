"""Readiness-only source substitute: real Firefox/reader, no remote requests."""
from pathlib import Path
from playwright.async_api import Browser
import json


def install():
    # Preserve the real exception and synthetic page before the unchanged
    # worker closes a failed tab. Never return a substituted success value.
    from modern_map_cycle_browser import SharedAccount
    import traceback
    import time
    original_read = SharedAccount.read
    async def observed_read(account, page, target, output, observe):
        job = Path(output).parent.name
        begin = time.monotonic()
        try:
            return await original_read(account, page, target, output, observe)
        except Exception as error:
            record = dict(job=job, model=target.model, timeframe=target.timeframe,
                          code=getattr(error, 'code', type(error).__name__),
                          reason=getattr(error, 'basic_reason', None) or getattr(error, 'reason', None),
                          trace=traceback.format_exc(), elapsed_ms=round((time.monotonic()-begin)*1000))
            Path('/receipts/failure-'+job+'.json').write_text(json.dumps(record))
            await page.raw.screenshot(path='/receipts/failure-'+job+'.png')
            raise
    SharedAccount.read = observed_read
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
