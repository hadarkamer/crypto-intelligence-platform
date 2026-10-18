"""Test-only timings for unchanged cookie dismissal; never log cookie values."""
import time


def install():
    from model3_diagnostic import install as install_existing, emit
    install_existing()
    import heatmap_hover as h
    from capture_failure_observer import click_failure
    dismiss = h.dismiss_cookie_banner

    class Locator:
        def __init__(self, actual, role):
            self.actual, self.role = actual, role

        def get_by_role(self, role, **kwargs):
            return Locator(self.actual.get_by_role(role, **kwargs), role)

        def nth(self, index):
            return Locator(self.actual.nth(index), self.role)

        def __getattr__(self, name):
            method = getattr(self.actual, name)
            if name not in ('count', 'is_visible', 'click', 'wait_for'):
                return method

            def observe(*args, **kwargs):
                started = time.monotonic()
                event = {'phase': 'cookie_dismissal', 'role': self.role, 'operation': name}
                try:
                    value = method(*args, **kwargs)
                    event['status'] = 'completed'
                    if type(value) in (bool, int):
                        event['value'] = value
                    return value
                except Exception as exc:
                    event.update(status='failed', exception_class=type(exc).__name__,
                                 click_failure=click_failure(exc))
                    text = str(exc).lower()
                    event['call_log_flags'] = {key: phrase in text for key, phrase in (
                        ('click_action_started', 'performing click action'),
                        ('click_action_done', 'click action done'),
                        ('waiting_navigation', 'waiting for scheduled navigations'),
                        ('waiting_visible', 'waiting for element to be visible'),
                        ('waiting_locator', 'waiting for locator'))}
                    raise
                finally:
                    event['seconds'] = round(time.monotonic() - started, 4)
                    emit(event)
            return observe

    class CookiePage:
        def __init__(self, page):
            self.page = page

        def locator(self, selector):
            return Locator(self.page.locator(selector), 'container')

        def get_by_text(self, text):
            return Locator(self.page.get_by_text(text), 'banner_text')

    def observed(page):
        try:
            return dismiss(CookiePage(page))
        finally:
            emit({'phase': 'cookie_dismissal', 'operation': 'finished'})

    h.dismiss_cookie_banner = observed
