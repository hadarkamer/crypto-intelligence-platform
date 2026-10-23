"""Install hover evidence into the same capture, one analysis and existing store."""
from pathlib import Path
from install_prominent_levels import once
from install_original_flow import VISION_INSTRUCTIONS

HOVER_INSTRUCTIONS = """
Observe and explain the supplied map; do not give trade advice or predictions.
Return exactly ONE scan for the requested model/timeframe. A detail crop is from
the same screenshot, not an additional observation. For Models 1/2 transcribe
the visible PRICE-axis limits into price_axis_range with their numeric text.
Model 3 supplies verified DOM price_axis_ticks; code owns its axis values.

Code computes pool colours, row membership, continuity, quantities and prices
from the same original PNG and a complete verified DOM hover column. Code
rechecks that evidence before accepting the result. These fields are NOT yours
to reclassify. Return exactly one prominent_levels entry for EVERY candidate_id,
containing only candidate_id, right_edge_visible and strength_score.
Check the actual symbol, model, timeframe, readability and visible context.
For each candidate, confirm that its location is visible and unobstructed at the
LATEST RIGHT EDGE. Set right_edge_visible=false if an overlay, cropped/missing
area or unreadable map prevents that context check. A difference in green shades
is not an obstruction. Do not substitute subjective colour confidence for this
visibility check. Report actual context problems in limitations; never omit a
candidate. Code rejects missing, duplicate or obstructed candidates.
y_top/y_bottom locate the sampled first/last included rows, not band boundaries.
Interior samples are row centres; the outermost chart rows use visible edge
anchors. Use plot_bounds and row_height_pixels to locate them.
strength_score is relative visual prominence in 0..1, not money or probability.

Interpret the supplied code-computed colours and values without changing them.
Yellow=many, green=normal, blue=few; a larger green sum never upgrades its colour.
Numbers come from verified displayed tooltip text, even when absent from the
PNG. Do not recalculate or replace prices, source_row_ids, sums or percentages.

When requested, price_axis_range.price_evidence must quote BOTH limits as full decimal
numbers exactly matching those numeric fields. Read the visible labels; never
substitute hover-row prices for axis labels.
For Models 1/2 the reference is source_candle.close from the visible OHLC tooltip
on this same map's latest column, with the same source_time as the liquidity
rows. Code verifies it on a second read. This is the displayed candle Close,
not a real-time market quote. Do not estimate a current price or price range
from pixels. Code assigns above/below and omits candidates touching that Close.
Keep the summary consistent with the supplied Close and return every candidate,
including one touching the Close, for the context check.
Model 3 does not expose OHLC on candle hover. Its source_price_range conservatively
bounds the last painted candle body using adjacent measured tooltip row prices
and verified DOM axis ticks. Keep it a range; never call its midpoint an exact
Close. Code omits pools touching this interval. Check visually that the supplied
reference covers the last candle; mark readable=false if it does not. Do not
return or invent price reference fields: code supplies and validates them.
Keep exact_dollar_totals_available=false: displayed K/M/B values are rounded.
Liquidation Leverage in source_metric units is NOT a verified dollar amount.
The denominator covers only the displayed latest column, not historical sums,
all market liquidity or actual realized liquidations. Never infer money from
colour or the legend. Keep short_summary concise and observational.
"""

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
    # Preserve the existing request budget while changing verification ownership.
    text = once(text, '"reasoning": {"effort": "low"},',
        '"reasoning": {"effort": "medium" if HEATMAP_MODEL == 2 else "low"},')
    text = once(text, '        content.extend(detail_content(image))',
        '        content.extend(detail_content(image))\n'
        '        from heatmap_hover import model_content\n'
        '        content.extend(model_content(image))')
    text += '''
_hover_scan = HEATMAP_SCHEMA['properties']['scans']['items']
_hover_scan['properties']['prominent_levels']['items'] = {
    'type':'object','additionalProperties':False,
    'properties':{'candidate_id':{'type':'string','pattern':'^c[0-9]+$'},
        'right_edge_visible':{'type':'boolean'},
        'strength_score':{'type':'number','minimum':0,'maximum':1}},
    'required':['candidate_id','right_edge_visible','strength_score']}
'''
    # Replace the image-only extraction instructions. Appending a conflicting
    # hover exception left GPT free to widen/omit precomputed candidates.
    # Remove image-price guesses. Code supplies Close for Models 1/2 and a
    # source-row envelope for Model 3; legacy result validation remains available.
    text += '\nfrom heatmap_models import HEATMAP_MODEL\n'
    text += 'for _key in ("current_price_range", "current_price_estimate", "current_price_confidence"):\n'
    text += '    _hover_scan["properties"].pop(_key, None)\n'
    text += '    _hover_scan["required"].remove(_key)\n'
    text += 'if HEATMAP_MODEL == 3:\n'
    text += '    _hover_scan["properties"].pop("price_axis_range")\n'
    text += '    _hover_scan["required"].remove("price_axis_range")\n'
    text += 'SYSTEM_PROMPT = '+repr(VISION_INSTRUCTIONS+HOVER_INSTRUCTIONS)+' + "\\nRequested heatmap: " + MODEL_LABEL\n'
    text += 'import price_detail_input as _detail\n'
    text += '_detail.DETAIL_TEXT = '+repr('Enlarged saved pixels from the SAME screenshot. Check map context and visibility of candidate locations at the latest right edge; code owns colour and row membership. Return one scan. Use the supplied source_candle.close or source_price_range; do not estimate a new reference from this image.')+'\n'
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
        "    raw = ground_analysis(raw, images[0]['hover_snapshot'], image_path=images[0]['image'], source_sha256=hashlib.sha256(image).hexdigest())\n")
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
