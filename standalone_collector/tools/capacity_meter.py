"""Passive instrumentation for the isolated capacity test. No job submission, provider calls, or account data."""
from collections import Counter
from contextlib import contextmanager, ExitStack
from contextvars import ContextVar
from functools import wraps
import json
import os
from urllib.parse import urlsplit
import re
from pathlib import Path
from statistics import mean, median
from threading import Event, Thread
from time import monotonic
from unittest.mock import patch
from uuid import UUID

from resource_sample import ResourceSample

active_map = ContextVar("measured_map", default=None)


def safe_id(value):
    return str(UUID(str(value)))


# The single pilot supervisor calls this before each QA button/job submission.
# A partial/uncertain button response consumes its slot. Restart never resets
# spent authorizations in the same dedicated QA receipts directory.
COHORT = (('BTC',1,'12H'),('DOGE',2,'24H'),('XRP',3,'48H'),('ETH',3,'1W'))


def reserve_job(directory, tabs, target):
    target = tuple(target)
    if tabs not in (1,2,4) or target not in COHORT:
        raise ValueError('outside_capacity_proposal')
    directory = Path(directory)
    if tabs > 1 and not (directory/f'passed-{tabs//2}').is_file():
        raise ValueError('prior_arm_not_accepted')
    path = directory/f'arm-{tabs}-slot-{COHORT.index(target)}'
    fd = os.open(path, os.O_WRONLY|os.O_CREAT|os.O_EXCL, 0o600)
    os.close(fd)
    return path


def accept_arm(directory,tabs,rows,started,ended):
    if tabs not in (1,2,4):
        raise ValueError('outside_capacity_proposal')
    directory = Path(directory)
    report = summarize(rows,started,ended)
    targets = {(r['symbol'],r['model'],r['timeframe']) for r in rows}
    if (report['refreshed'] != 4 or targets != set(COHORT) or
            not all((directory/f'arm-{tabs}-slot-{i}').is_file() for i in range(4))):
        raise ValueError('arm_not_complete')
    # Supervisor calls only after quality, database, rate and memory gates.
    (directory/f'passed-{tabs}').touch(exist_ok=False)
    return report


@contextmanager
def watch_host(receive, interval=0.45):
    """One sampler across the arm, including all child tabs and OCR. Never add overlapping per-tab resource totals."""
    sampler = ResourceSample()
    stop = Event()
    def collect():
        while not stop.wait(interval):
            sampler.update(force=True)
    thread = Thread(target=collect, daemon=True)
    thread.start()
    try:
        yield
    finally:
        stop.set()
        thread.join()
        report = sampler.report()
        report.pop('samples', None)
        receive(report)


def summarize(rows, arm_started, arm_ended):
    """Monotonic timestamps from one supervisor observing queue, DB receipt and full UI refresh."""
    if not arm_ended > arm_started or not 1 <= len(rows) <= 4:
        raise ValueError('invalid_arm')
    keys, jobs, wall, queues, views = set(), set(), [], [], []
    for row in rows:
        job = safe_id(row['job'])
        key = (row['symbol'], row['model'], row['timeframe'])
        if job in jobs or key in keys:
            raise ValueError('duplicate_map_in_arm')
        jobs.add(job)
        keys.add(key)
        queued, claimed = row['queued'], row.get('claimed')
        commit, displayed = row.get('committed'), row.get('refreshed')
        marks = [queued, *(v for v in (claimed, commit, displayed) if v is not None)]
        if marks != sorted(marks) or marks[0] < arm_started or marks[-1] > arm_ended:
            raise ValueError('invalid_event_order')
        if claimed is not None:
            queues.append(claimed-queued)
        if displayed is not None:
            if commit is None or claimed is None:
                raise ValueError('display_without_accepted_source')
            wall.append(displayed-queued)
            views.append(displayed-commit)
    return dict(admitted=len(rows), refreshed=len(wall), total_seconds=arm_ended-arm_started,
                refreshed_maps_per_minute=len(wall)*60/(arm_ended-arm_started),
                map_wall_mean_seconds=mean(wall) if wall else None,
                map_wall_median_seconds=median(wall) if wall else None,
                map_wall_max_seconds=max(wall, default=None), map_wall_p95_seconds=None,
                queue_wait_seconds=queues, commit_to_refresh_seconds=views)


class BrowserMeter:
    """Test-only adapter. Attribute requests when issued and count physical navigation separately from map completions."""
    def __init__(self, receive):
        self.receive = receive
        self.host = Counter(observed_official_login_posts=0, form_submission_prevented=0)
        self.maps = {}
        self.pages = {}
        self.contexts = {}
        self.peak_tabs = 0
        self.open_pages = set()

    def count(self, key, n=1):
        self.host[key] += n
        current = active_map.get()
        if current is not None:
            self.maps[current][key] += n

    def attach(self, context):
        if context in self.contexts:
            return
        def on_page(page):
            if page in self.open_pages:
                return
            self.open_pages.add(page)
            self.host['tabs_opened'] += 1
            self.peak_tabs = max(self.peak_tabs, len(self.open_pages))
            page.once('close', lambda *_: self.closed(page))
        def on_request(request):
            self.host['http_requests'] += 1
            # Inspect host and route class only. Never retain URLs, query
            # strings, form data, headers, responses or account identifiers.
            parsed = urlsplit(request.url)
            if (request.method == 'POST' and parsed.scheme == 'https' and parsed.hostname and
                    (parsed.hostname == 'coinglass.com' or parsed.hostname.endswith('.coinglass.com')) and
                    re.search(r'(?:^|/)(?:login|signin|sign-in)(?:/|$)', parsed.path, re.I)):
                self.host['observed_official_login_posts'] += 1
            try:
                frame = request.frame
                page = frame.page
            except Exception:
                # Service worker or an early navigation request without frame.
                self.host['requests_without_frame'] += 1
                return
            counters = self.maps.get(self.pages.get(page))
            if counters is not None:
                counters['http_requests'] += 1
            if request.is_navigation_request() and frame == page.main_frame:
                self.host['main_document_requests'] += 1
                if counters is not None:
                    counters['main_document_requests'] += 1
                if request.redirected_from is not None:
                    self.host['main_document_redirects'] += 1
        self.contexts[context] = (on_page, on_request)
        context.on('page', on_page)
        context.on('request', on_request)
        for page in context.pages:
            on_page(page)

    def closed(self,page):
        if page in self.open_pages:
            self.open_pages.remove(page)
            self.host['tabs_closed'] += 1

    @contextmanager
    def installed(self):
        """Install only in the QA browser child. The controller and database credentials stay isolated."""
        from modern_map_cycle_browser import SharedAccount, ReaderPageAPI
        from playwright.async_api import Browser, BrowserType
        import modern_map_axis_ocr
        import dedicated_map_flow
        read, lookup, ocr = SharedAccount.read, ReaderPageAPI.__getattr__, modern_map_axis_ocr._tsv
        from model1_execution import StageFailure
        async def decline_renewal(account,page,original):
            # Stop before declaring/reserving an auth incident, opening the
            # login page or asking the parent process for the credentials.
            # 'blocked' belongs to this one QA browser in memory only.
            account.blocked = 'source_login_required'
            monitor.count('saved_session_expired')
            raise StageFailure('source_login_required')
        def submit_form(*a,**kw):
            monitor.count('form_submission_prevented')
            raise StageFailure('source_login_required')
        launch, close, new_context = BrowserType.launch, Browser.close, Browser.new_context
        monitor = self
        async def open_browser(browser_type, *a, **kw):
            result = await launch(browser_type,*a,**kw)
            monitor.host['browser_launches'] += 1
            return result
        async def close_browser(browser, *a, **kw):
            result = await close(browser,*a,**kw)
            monitor.host['browser_closes'] += 1
            return result
        async def context_of(browser, *a, **kw):
            instance = await new_context(browser,*a,**kw)
            monitor.attach(instance)
            return instance
        async def observed(account,page,target,output,observe):
            job = safe_id(Path(output).parent.name)
            monitor.maps.setdefault(job, Counter())
            handle = active_map.set(job)
            monitor.pages[page.raw] = job
            def event(kind, value):
                if kind == 'stage':
                    name = value.get('name')
                    if name in ('read_market_overview', 'recheck_market_update', 'read_numeric_price_axis',
                                'basic_classify_latest_edge', 'build_basic_result',
                                'basic_identity_start','basic_identity_end','read_latest_crosshair_time'):
                        monitor.count(name+'_calls')
                        if type(value.get('elapsed_ms')) is int:
                            monitor.count(name+'_ms', value['elapsed_ms'])
                if observe:
                    return observe(kind, value)
            t0 = monotonic()
            try:
                result = await read(account, page, target, output, event)
                monitor.count('verified_reads')
                return result
            finally:
                monitor.count('browser_read_ms',round(1000*(monotonic()-t0)))
                active_map.reset(handle)
                monitor.pages.pop(page.raw,None)
        def attr(obj, name):
            member = lookup(obj,name)
            if not callable(member):
                return member
            key = {('Page','screenshot'):'screenshots', ('Mouse','move'):'pointer_moves',
                   ('Page','goto'):'goto_calls', ('Page','reload'):'reload_calls',
                   ('Page','evaluate'):'page_evaluates'}.get((type(obj.raw).__name__,name))
            if key is None:
                return member
            @wraps(member)
            def counted(*args, **kw):
                monitor.count(key)
                result = member(*args, **kw)
                if key == 'screenshots' and isinstance(result, bytes):
                    monitor.count('screenshot_bytes',len(result))
                return result
            return counted
        def recognize(*args,**kwargs):
            t0 = monotonic()
            monitor.count('ocr_calls')
            try:
                return ocr(*args, **kwargs)
            finally:
                monitor.count('ocr_ms',round(1000*(monotonic()-t0)))
        with ExitStack() as hooks:
            for obj, name, value in ((BrowserType,'launch',open_browser), (Browser,'new_context',context_of),
                            (Browser,'close',close_browser),(SharedAccount,'read',observed),
                            (SharedAccount,'authenticate',decline_renewal),
                            (ReaderPageAPI,'__getattr__',attr),(modern_map_axis_ocr,'_tsv',recognize),
                            (dedicated_map_flow,'submit_official_login',submit_form)):
                hooks.enter_context(patch.object(obj,name,value))
            try:
                yield
            finally:
                for ctx, (on_page, on_req) in self.contexts.items():
                    ctx.remove_listener('page',on_page)
                    ctx.remove_listener('request',on_req)
                self.receive(dict(host=dict(self.host),peak_tabs=self.peak_tabs,maps=self.maps))


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Report rates after a capacity arm. Input contains supervisor timestamps only.')
    parser.add_argument('observations',type=Path)
    args = parser.parse_args()
    data=json.loads(args.observations.read_text())
    print(json.dumps(summarize(data['rows'],data['started'],data['ended'])))
