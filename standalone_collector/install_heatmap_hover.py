"""Install hover evidence into the same capture, one analysis and existing store."""
from pathlib import Path
from install_prominent_levels import once
from install_original_flow import VISION_INSTRUCTIONS
from heatmap_hover import MODEL2_GREEN_BLUE_HUE

HOVER_INSTRUCTIONS = """
Observe and explain the supplied map; do not give trade advice or predictions.
Return exactly ONE scan for the requested model/timeframe. A detail crop is from
the same screenshot, not an additional observation. Transcribe the visible
minimum/maximum PRICE-axis ticks into price_axis_range with their numeric text.

Candidate pools were located in the same screenshot and calculated from a
complete current-column DOM hover sweep. Return exactly one prominent_levels
entry for EVERY candidate_id. y_top/y_bottom are the CENTRES of the first/last
included price rows, not the outside boundaries of a band (0=plot top, 1=bottom).
Use plot_bounds and row_height_pixels to locate these rows in the full screenshot;
the enlarged detail uses the same pixels. Verify colour and continuity at the
LATEST RIGHT EDGE, not earlier portions of the band. If any candidate is unsupported,
mark confidence=low; never silently omit it or widen its boundaries. The code
will reject disagreement rather than present uncertain quantities as verified.
Do not add pools from historical columns or bridge a weak gap between bands.

For confirmed candidates keep the supplied shape, intensity and exact prices.
Yellow=many, green=normal, blue=few; a larger green sum never upgrades its colour.
strength_score is visual prominence in 0..1, not dollars or probability.
For a point use prices=[price_low]; for a range use prices=[] and preserve both
bounds. Set continuous and weak_gap_present from the visible band. Numeric
evidence comes from the verified DOM hover table, even if the numbers are not
printed in the PNG: use price_precision=verified_label, evidence_basis=tooltip,
uncertainty_usd=0, and quote the supplied prices in price_evidence.
The source_row_ids, sums, percentages and exact pool prices are calculated by
code before this request. Do not recalculate, invent or replace them.
Candidate confidence describes its visible colour, row membership and continuity.
Assess it separately from confidence in the last-candle current-price range.
An absent printed price label does not weaken verified DOM numeric evidence.

The current-price reference is a visual range from the last candle; never use
a candidate price or convenient midpoint as an exact current quote. Code assigns
above/below and omits candidates intersecting that validated reference range.
Still return those candidates for validation rather than silently omitting them.
Keep exact_dollar_totals_available=false: displayed K/M/B values are rounded.
Liquidation Leverage in source_metric units is NOT a verified dollar amount.
The denominator covers only the displayed latest column, not historical sums,
all market liquidity or actual realized liquidations. Never infer money from
colour or the legend. Keep short_summary concise and observational.
"""

MODEL2_REFERENCE = f'''
price_axis_range.price_evidence must quote BOTH low and high as full decimal
numbers exactly matching those numeric fields. A generic phrase such as "visible
axis ticks" is insufficient. Read the labels from the image; do not invent them
or substitute hover-row prices for axis labels.

The current-price reference is source_candle.close, read from the visible OHLC
tooltip on this same map's latest column. Its source_time matches the liquidity
rows and it was unchanged on a second read. It is the displayed candle Close,
not a real-time market quote. Do not estimate a current price or price range
from pixels. Code assigns above/below and omits candidates touching that Close.
Still verify every candidate visually; candle evidence does not raise pool
confidence. Keep the summary consistent with the supplied numeric Close.

For this Model 2 classic palette, green means clear green/yellow-green, not the
cyan/teal transition into blue. The code's normalized HSV hue intervals are
green [0.24, {MODEL2_GREEN_BLUE_HUE}) and blue/teal [{MODEL2_GREEN_BLUE_HUE}, 0.66).
Source colour controls: RGB(48,173,118) is green; RGB(35,140,137) and
RGB(33,149,135) are blue/teal. Saturation and brightness must also be sufficient.
Verify candidate row membership using that same colour definition. A blue/teal
row must not extend a green pool. Two or more consecutive clear green rows form
a green range; a single green row is not such a range. The existing candidates
apply this rule before analysis. This does not authorize excluding a supplied
candidate or raising confidence: still reject unclear boundaries or continuity.
'''


def install(runtime: Path):
    path = runtime/'market_vision/coinglass_heatmap_capture.py'
    text = path.read_text()
    text = once(text, "'locale': 'en-US',", "'locale': 'en-US', 'timezone_id': 'UTC',")
    # Chromium's beyond-viewport capture changed Model 3's displayed price axis
    # (77k–87k -> 89k–99k). Keep the width and fit its 800px plot plus header in
    # the viewport, then capture that viewport without beyond-viewport painting.
    text = once(text, "'height': 1000}", "'height': 1300 if HEATMAP_MODEL == 3 else 1000}")
    text = once(text, 'page.screenshot(path=str(path), full_page=True)',
        'page.screenshot(path=str(path), full_page=HEATMAP_MODEL != 3)')
    text = once(text, "                with capture_operation('chart_geometry'):\n",
        "                if HEATMAP_MODEL == 3:\n"
        "                    from heatmap_hover import close_heatmap_settings\n"
        "                    with capture_operation('overlay_dismissal'):\n"
        "                        close_heatmap_settings(page)\n"
        "                    page.evaluate('() => window.scrollTo(0, 0)')\n"
        "                with capture_operation('chart_geometry'):\n")
    anchor = "                    page.screenshot("
    text = once(text, anchor,
        "                    from heatmap_hover import dismiss_cookie_banner\n"
        "                    dismiss_cookie_banner(page, model=HEATMAP_MODEL)\n" + anchor)
    anchor = '                    verify_saved_render(page, timeframe, HEATMAP_MODEL, render_state, path, source_diagnostics)\n'
    text = once(text, anchor, anchor+
        "                from heatmap_hover import require_cookie_banner_hidden\n"
        "                require_cookie_banner_hidden(page)\n"
        "                from heatmap_hover import capture_column\n"
        "                with capture_operation('hover_collection'):\n"
        "                    hover_snapshot = capture_column(page, path, chart_rect, model=HEATMAP_MODEL, timeframe=timeframe, source_axis=render_state.get('price_axis_ticks'))\n")
    text = once(text, "results.append({'image': str(path),", "results.append({'hover_snapshot': hover_snapshot, 'image': str(path),")
    compile(text,str(path),'exec'); path.write_text(text)

    path = runtime/'market_vision/openai_heatmap_scanner.py'
    text = path.read_text()
    # Retained Model 2 / 24H and 48H bands failed at low reasoning but passed
    # at medium with identical evidence and strict pool validation.
    text = once(text, '"reasoning": {"effort": "low"},',
        '"reasoning": {"effort": "medium" if HEATMAP_MODEL == 2 else "low"},')
    text = once(text, '        content.extend(detail_content(image))',
        '        content.extend(detail_content(image))\n'
        '        from heatmap_hover import model_content\n'
        '        content.extend(model_content(image))')
    text += '''
_hover_level = HEATMAP_SCHEMA['properties']['scans']['items']['properties']['prominent_levels']['items']
_hover_level['properties']['candidate_id'] = {'type':'string','pattern':'^c[0-9]+$'}
_hover_level['required'].append('candidate_id')
'''
    # Replace the image-only extraction instructions. Appending a conflicting
    # hover exception left GPT free to widen/omit precomputed candidates.
    # Keep the existing identity/readability and current-price range safeguards.
    text += '\nSYSTEM_PROMPT = '+repr(VISION_INSTRUCTIONS)+' + RANGE_INSTRUCTIONS + '+repr(HOVER_INSTRUCTIONS)+' + "\\nRequested heatmap: " + MODEL_LABEL\n'
    model2_instructions=HOVER_INSTRUCTIONS.replace(
        'Assess it separately from confidence in the last-candle current-price range.',
        'Numeric candle evidence does not determine candidate confidence.')
    start=model2_instructions.index('The current-price reference is a visual range')
    end=model2_instructions.index('Keep exact_dollar_totals_available=false:')
    model2_instructions=model2_instructions[:start]+MODEL2_REFERENCE+model2_instructions[end:]
    text += '\nfrom heatmap_models import HEATMAP_MODEL\nif HEATMAP_MODEL == 2:\n'
    text += '    _scan = HEATMAP_SCHEMA["properties"]["scans"]["items"]\n'
    text += '    for _key in ("current_price_range", "current_price_estimate", "current_price_confidence"):\n'
    text += '        _scan["properties"].pop(_key, None)\n'
    text += '        _scan["required"].remove(_key)\n'
    text += '    SYSTEM_PROMPT = '+repr(VISION_INSTRUCTIONS+model2_instructions)+' + "\\nRequested heatmap: " + MODEL_LABEL\n'
    # The detail remains useful for pool verification, but its old range-only
    # instruction must not contradict the source-based reference in Model 2.
    text += '    import price_detail_input as _detail\n'
    text += '    _detail.DETAIL_TEXT = '+repr('Enlarged saved pixels from the SAME screenshot. Verify candidate colours and continuity at the latest right edge. Return one scan. Read the current reference from source_candle.close, not from this image.')+'\n'
    compile(text,str(path),'exec'); path.write_text(text)

    path = runtime/'collection_model1_task.py'
    text = path.read_text()
    text = once(text, "    stage('analysis')\n",
        "    from heatmap_hover import prepare_candidates\n"
        "    prepare_candidates(images)\n"
        "    stage('analysis')\n")
    text = once(text, "    stage('validation')\n",
        "    stage('validation')\n"
        "    from heatmap_hover import ground_analysis, attach_measurements\n"
        "    raw = ground_analysis(raw, images[0]['hover_snapshot'])\n")
    anchor = '    result = normalize(raw, timeframe=timeframe, run_id=run_id, captured_at=captured_at, image=image)'
    text = once(text, anchor, anchor+"\n    result = attach_measurements(result, raw, images[0]['hover_snapshot'], image=image)")
    compile(text,str(path),'exec'); path.write_text(text)

    path = runtime/'collection_bridge.py'
    text = path.read_text()
    text = once(text, 'MAX_RESULT = 64 * 1024', 'MAX_RESULT = 256 * 1024')
    text = once(text, "AND result->>'selection_policy'='prominent_right_edge.v1'",
        "AND result->>'selection_policy'='prominent_right_edge.v1'\n"
        "                AND result->'hover_snapshot'->>'version'='displayed-hover.v1'")
    text = once(text, 'result.get("selection_policy") != "prominent_right_edge.v1" or',
        'result.get("selection_policy") != "prominent_right_edge.v1" or\n'
        '                (result.get("hover_snapshot") or {}).get("version") != "displayed-hover.v1" or')
    compile(text,str(path),'exec'); path.write_text(text)
