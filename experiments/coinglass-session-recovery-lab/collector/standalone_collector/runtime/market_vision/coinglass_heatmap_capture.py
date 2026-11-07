"""Capture public CoinGlass liquidation heatmap screenshots for visual AI analysis.

This POC uses the public web page exactly as a browser would: it opens the page,
selects the requested timeframe when possible, and captures a screenshot. It does
not call private CoinGlass endpoints or attempt to recover hidden numeric data.
"""
from __future__ import annotations
from capture_readiness import record_source_reload
import model3_rendering as _model3_rendering
from capture_readiness import CaptureNetworkDiagnostics, wait_for_render, verify_saved_render, require_unblocked_source, geometry_from_state
from capture_failure_observer import observe_capture_failure
from model1_execution import capture_operation
from heatmap_models import HEATMAP_MODEL, MODEL_LABEL, SOURCE_URL, SCHEMA_VERSION, select_visible_control
import json
import os
import re
from pathlib import Path
from typing import Iterable
from playwright.sync_api import Locator, Page, TimeoutError as PlaywrightTimeoutError, sync_playwright
COINGLASS_HEATMAP_URL = SOURCE_URL

def _first_visible(locator: Locator) -> Locator | None:
    for idx in range(locator.count()):
        item = locator.nth(idx)
        try:
            if item.is_visible():
                return item
        except Exception:
            continue
    return None

def _dismiss_common_overlays(page: Page) -> None:
    matches = page.get_by_role('button', name=re.compile('^(?:Accept|Accept all|Allow all|I agree|Agree|Got it|OK|Close)$', re.I))
    for index in range(min(matches.count(), 8)):
        try:
            control = matches.nth(index)
            visible = control.is_visible()
        except Exception:
            continue
        if visible:
            try:
                control.click(timeout=1500)
                page.wait_for_timeout(300)
            except Exception:
                pass
            return

def _dismiss_capture_blockers(page: Page) -> None:
    require_unblocked_source(page, HEATMAP_MODEL, phase='overlay_dismissal')
    _dismiss_common_overlays(page)
    require_unblocked_source(page, HEATMAP_MODEL, phase='overlay_dismissal')
    login_text = page.get_by_text(re.compile('log\\s*in\\s+to\\s+unlock\\s+full\\s+data', re.I))
    login_visible = _first_visible(login_text)
    if login_visible is None:
        return
    dialog = login_visible.locator("xpath=ancestor::*[@role='dialog'][1]")
    scopes = [dialog.first] if dialog.count() else []
    scopes.append(page.locator('body'))
    close_patterns = [re.compile('^close$', re.I), re.compile('^dismiss$', re.I), re.compile('^not now$', re.I), re.compile('^maybe later$', re.I)]
    for scope in scopes:
        for pattern in close_patterns:
            try:
                button = _first_visible(scope.get_by_role('button', name=pattern))
                if button is not None:
                    button.click(timeout=1500)
                    page.wait_for_timeout(500)
                    if _first_visible(login_text) is None:
                        return
            except Exception:
                pass
        try:
            icon_close = _first_visible(scope.locator('button[aria-label*="close" i], button[title*="close" i], [role="button"][aria-label*="close" i]'))
            if icon_close is not None:
                icon_close.click(timeout=1500)
                page.wait_for_timeout(500)
                if _first_visible(login_text) is None:
                    return
        except Exception:
            pass
    try:
        page.keyboard.press('Escape')
        page.wait_for_timeout(500)
    except Exception:
        pass

def _select_symbol_mode(page: Page) -> None:
    try:
        select_visible_control(page, 'Symbol')
    except Exception:
        pass

def _select_model_one(page: Page) -> None:
    try:
        select_visible_control(page, f'Model {HEATMAP_MODEL}')
    except Exception:
        pass

def _select_timeframe(page: Page, timeframe: str) -> None:
    wanted = timeframe.strip().lower()
    labels = {f'{hours}{suffix}': f'{hours} hour' for hours in (12, 24, 48) for suffix in ('h', ' hour', ' hours')}
    if wanted not in labels:
        raise ValueError('Unsupported heatmap timeframe')
    visible_label = labels[wanted]
    wanted_pattern = re.compile(f'^{re.escape(visible_label)}$', re.I)
    any_hour = re.compile('\\b(12|24|48)\\s*hour\\b', re.I)
    with capture_operation('timeframe_trigger_lookup'):
        selected = _first_visible(page.locator('[role="combobox"], [aria-haspopup="listbox"]').filter(has_text=wanted_pattern))
    if selected is not None:
        return
    try:
        page.keyboard.press('Escape')
        page.wait_for_timeout(200)
    except Exception:
        pass
    with capture_operation('timeframe_trigger_lookup'):
        trigger: Locator | None = _first_visible(page.locator('[role="combobox"]').filter(has_text=any_hour))
    if trigger is None:
        with capture_operation('timeframe_trigger_lookup'):
            trigger = _first_visible(page.locator('button[aria-haspopup="listbox"], [aria-haspopup="listbox"]').filter(has_text=any_hour))
    if trigger is None:
        with capture_operation('timeframe_trigger_lookup'):
            label = _first_visible(page.get_by_text(any_hour))
        if label is not None:
            with capture_operation('timeframe_trigger_lookup'):
                ancestor = label.locator("xpath=ancestor-or-self::*[@role='combobox' or @aria-haspopup='listbox'][1]")
            if ancestor.count() and ancestor.first.is_visible():
                trigger = ancestor.first
    if trigger is None:
        raise RuntimeError('Could not find the visible CoinGlass timeframe selector')
    try:
        try:
            with capture_operation('timeframe_open'):
                trigger.click(timeout=15000)
        except Exception:
            with capture_operation('timeframe_open'):
                trigger.click(timeout=15000, force=True)
        page.wait_for_timeout(300)
        with capture_operation('timeframe_option_lookup'):
            option = _first_visible(page.get_by_role('option', name=wanted_pattern))
        if option is None:
            with capture_operation('timeframe_option_lookup'):
                option = _first_visible(page.locator('[role="option"]').filter(has_text=wanted_pattern))
        if option is None:
            raise RuntimeError(f"Visible option '{visible_label}' did not appear after opening selector")
        with capture_operation('timeframe_select'):
            option.click(timeout=15000)
        page.wait_for_timeout(1500)
        with capture_operation('timeframe_confirmation'):
            selected = _first_visible(page.locator('[role="combobox"], [aria-haspopup="listbox"]').filter(has_text=wanted_pattern))
        if selected is None:
            raise RuntimeError(f"Timeframe selection did not settle on '{visible_label}'")
    except Exception as exc:
        raise RuntimeError(f'Could not select timeframe {visible_label}: {exc}') from exc

def _wait_for_heatmap(page: Page) -> None:
    title = page.get_by_text(re.compile('BTC.*Liquidation Heatmap', re.I))
    try:
        title.first.wait_for(state='visible', timeout=25000)
    except PlaywrightTimeoutError:
        return
    page.wait_for_timeout(5000)

def _load_authenticated_storage_state() -> dict | None:
    raw = os.getenv('COINGLASS_STORAGE_STATE_JSON', '').strip()
    if not raw:
        return None
    try:
        state = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError('COINGLASS_STORAGE_STATE_JSON is not valid JSON') from exc
    if not isinstance(state, dict):
        raise RuntimeError('COINGLASS_STORAGE_STATE_JSON must contain a JSON object')
    return state

def _load_cookie_header() -> list[dict]:
    """Convert an encrypted browser Cookie request header into Playwright cookies."""
    raw = os.getenv('COINGLASS_COOKIE_HEADER', '').strip()
    if not raw:
        return []
    cookies: list[dict] = []
    for chunk in raw.split(';'):
        chunk = chunk.strip()
        if not chunk or '=' not in chunk:
            continue
        name, value = chunk.split('=', 1)
        name = name.strip()
        if not name:
            continue
        cookies.append({'name': name, 'value': value.strip(), 'domain': '.coinglass.com', 'path': '/', 'secure': True, 'sameSite': 'Lax'})
    return cookies

def capture_heatmaps(output_dir: str | Path, *, timeframes: Iterable[str]=('12h', '24h'), url: str=COINGLASS_HEATMAP_URL) -> list[dict[str, str]]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, str]] = []
    with sync_playwright() as p:
        with capture_operation('browser_launch'):
            browser = p.chromium.launch(headless=True)
        context_options = {'viewport': {'width': 1600, 'height': 1300 if HEATMAP_MODEL == 3 else 1000}, 'device_scale_factor': 1, 'locale': 'en-US', 'timezone_id': 'UTC', 'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36'}
        storage_state = _load_authenticated_storage_state()
        if storage_state is not None:
            context_options['storage_state'] = storage_state
        with capture_operation('browser_context'):
            context = browser.new_context(**context_options)
            _model3_rendering.configure_context(context, HEATMAP_MODEL)
        cookie_header_cookies = _load_cookie_header()
        if cookie_header_cookies:
            with capture_operation('source_session_setup'):
                context.add_cookies(cookie_header_cookies)
        from model1_execution import prepare_context, control
        prepare_context(context)
        with capture_operation('page_creation'):
            page = context.new_page()
            source_diagnostics = CaptureNetworkDiagnostics(page)
        try:
            with capture_operation('navigation'):
                page.goto(url, wait_until='domcontentloaded', timeout=60000)
            with capture_operation('overlay_dismissal'):
                _dismiss_capture_blockers(page)
            with capture_operation('wait_for_chart'):
                _wait_for_heatmap(page)
            with capture_operation('model_selection'):
                _select_model_one(page)
            with capture_operation('symbol_selection'):
                _select_symbol_mode(page)
            for timeframe in timeframes:
                for _capture_attempt in range(2):
                    try:
                        with capture_operation('overlay_dismissal'):
                            _dismiss_capture_blockers(page)
                        with _model3_rendering.preparation(page, HEATMAP_MODEL):
                            with capture_operation('timeframe_selection'):
                                control(_select_timeframe, page, timeframe)
                            with capture_operation('wait_for_chart'):
                                _wait_for_heatmap(page)
                            with capture_operation('overlay_dismissal'):
                                _dismiss_capture_blockers(page)
                            with capture_operation('render_settle'):
                                page.wait_for_timeout(500)
                            path = out / f'coinglass_btc_heatmap_{timeframe}.png'
                            from model1_legend import prepare_legend_for_capture
                            with capture_operation('legend_preparation'):
                                prepare_legend_for_capture(page)
                            if HEATMAP_MODEL == 3:
                                from heatmap_hover import close_heatmap_settings
                                with capture_operation('overlay_dismissal'):
                                    close_heatmap_settings(page)
                                page.evaluate('() => window.scrollTo(0, 0)')
                            with capture_operation('overlay_dismissal'):
                                from heatmap_hover import dismiss_cookie_banner
                                dismiss_cookie_banner(page, model=HEATMAP_MODEL)
                            with capture_operation('render_readiness'):
                                render_state = wait_for_render(page, timeframe, HEATMAP_MODEL)
                                require_unblocked_source(page, HEATMAP_MODEL, phase='render_readiness')
                            with capture_operation('chart_geometry'):
                                chart_rect = geometry_from_state(render_state)
                        with _model3_rendering.observation(page, HEATMAP_MODEL, chart_rect) as _prepare_model3_frame:
                            with capture_operation('screenshot'):
                                _prepare_model3_frame()
                                page.screenshot(path=str(path), full_page=HEATMAP_MODEL != 3)
                            with capture_operation('render_readiness'):
                                verify_saved_render(page, timeframe, HEATMAP_MODEL, render_state, path, source_diagnostics)
                            from heatmap_hover import require_cookie_banner_hidden
                            require_cookie_banner_hidden(page)
                            from heatmap_hover import capture_column
                            with capture_operation('hover_collection'):
                                hover_snapshot = capture_column(page, path, chart_rect, model=HEATMAP_MODEL, timeframe=timeframe, source_axis=render_state.get('price_axis_ticks'))
                            results.append({'hover_snapshot': hover_snapshot, 'image': str(path), 'timeframe': timeframe, 'liquidity_threshold': 0.85 if HEATMAP_MODEL == 1 else None, 'price_geometry': chart_rect})
                    except Exception as _capture_error:
                        if _capture_attempt or getattr(_capture_error, 'code', None) != 'source_not_readable' or getattr(_capture_error, '_model1_capture_phase', None) != 'render_readiness' or (getattr(_capture_error, '_model1_source_readiness', {}).get('reason') == 'request-limit'):
                            raise
                        record_source_reload(out, timeframe, _capture_error)
                        with capture_operation('navigation'):
                            page.reload(wait_until='domcontentloaded', timeout=60000)
                        with capture_operation('overlay_dismissal'):
                            _dismiss_capture_blockers(page)
                        with capture_operation('wait_for_chart'):
                            _wait_for_heatmap(page)
                        with capture_operation('model_selection'):
                            _select_model_one(page)
                        with capture_operation('symbol_selection'):
                            _select_symbol_mode(page)
                    else:
                        break
        except Exception as exc:
            try:
                observe_capture_failure(page, HEATMAP_MODEL, source_diagnostics, exc)
            except Exception:
                pass
            raise
        finally:
            try:
                with capture_operation('context_close'):
                    context.close()
            finally:
                with capture_operation('browser_close'):
                    browser.close()
    return results
