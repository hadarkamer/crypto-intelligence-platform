"""Test-only observations around unchanged Model 3 close and hover operations.

No retries, changed deadlines, raw browser errors, cookies, or AI/database calls.
"""
import json
import time


def emit(value):
    print('MODEL3_DIAGNOSTIC '+json.dumps(value, separators=(',', ':')), flush=True)


def install():
    import heatmap_hover as h
    from capture_failure_observer import click_failure
    close = h.close_heatmap_settings
    capture = h.capture_column

    class Locator:
        def __init__(self, actual, role):
            self.actual, self.role = actual, role

        def get_by_role(self, role, **kwargs):
            return Locator(self.actual.get_by_role(role, **kwargs), role)

        def __getattr__(self, name):
            method = getattr(self.actual, name)
            if name not in ('count', 'is_visible', 'input_value', 'click', 'wait_for'):
                return method

            def observe(*args, **kwargs):
                start = time.monotonic()
                event = {'phase':'settings_close','role':self.role,'operation':name}
                try:
                    value = method(*args, **kwargs)
                    event['status'] = 'completed'
                    if type(value) in (bool, int):event['value'] = value
                    elif name == 'input_value':event['is_unmerged'] = value == '1'
                    return value
                except Exception as exc:
                    # Retain only fixed facts, never the underlying error text.
                    text = str(exc).lower()
                    event.update(status='failed', exception_class=type(exc).__name__,
                                 click_failure=click_failure(exc))
                    event['call_log_flags'] = {key: phrase in text for key,phrase in (
                        ('click_action_started','performing click action'),
                        ('click_action_done','click action done'),
                        ('waiting_navigation','waiting for scheduled navigations'),
                        ('waiting_visible','waiting for element to be visible'),
                        ('waiting_locator','waiting for locator'))}
                    raise
                finally:
                    event['seconds'] = round(time.monotonic()-start,4)
                    emit(event)
            return observe

    class ClosePage:
        def __init__(self, page):self.page = page
        def get_by_role(self, role, **kwargs):
            return Locator(self.page.get_by_role(role, **kwargs), role)

    def close_observed(page):
        try:
            return close(ClosePage(page))
        finally:
            emit({'phase':'settings_close','operation':'finished'})

    class HoverPage:
        def __init__(self, page):
            self.page = page
            self.measurements = {}
            parent = self
            class Mouse:
                def __getattr__(self, name):
                    return parent.wrap('mouse.'+name, getattr(page.mouse,name))
            self.mouse = Mouse()

        def wrap(self, name, function):
            def measured(*args, **kwargs):
                started = time.monotonic()
                try:return function(*args, **kwargs)
                finally:
                    seconds = time.monotonic()-started
                    row=self.measurements.setdefault(name,{'count':0,'seconds':0,'max_seconds':0})
                    row['count']+=1; row['seconds']+=seconds
                    row['max_seconds']=max(row['max_seconds'],seconds)
            return measured

        def __getattr__(self, name):
            value=getattr(self.page,name)
            return self.wrap(name,value) if name=='evaluate' else value

    def capture_observed(page, *args, **kwargs):
        proxy=HoverPage(page)
        try:return capture(proxy,*args,**kwargs)
        finally:
            emit({'phase':'hover','operations':{name:{k:round(v,4) for k,v in row.items()}
                    for name,row in proxy.measurements.items()}})

    h.close_heatmap_settings=close_observed
    h.capture_column=capture_observed
