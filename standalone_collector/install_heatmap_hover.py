"""Install hover evidence into the same capture, one analysis and existing store."""
from pathlib import Path
from install_prominent_levels import once
from install_original_flow import VISION_INSTRUCTIONS

HOVER_INSTRUCTIONS = """
Observe and explain the supplied map; do not give trade advice or predictions.
Return exactly ONE scan for the requested model/timeframe. A detail crop is from
the same screenshot, not an additional observation. Transcribe the visible
minimum/maximum PRICE-axis ticks into price_axis_range with their numeric text.

Candidate pools were located in the same screenshot and calculated from a
complete current-column DOM hover sweep. Return exactly one prominent_levels
entry for EVERY candidate_id. Visually verify its y_top/y_bottom (0=plot top,
1=bottom), colour and continuity. If any candidate is not supported by the image,
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


def install(runtime: Path):
    path = runtime/'market_vision/coinglass_heatmap_capture.py'
    text = path.read_text()
    text = once(text, "'locale': 'en-US',", "'locale': 'en-US', 'timezone_id': 'UTC',")
    text = once(text, "                with capture_operation('chart_geometry'):\n",
        "                if HEATMAP_MODEL == 3:\n"
        "                    from heatmap_hover import close_heatmap_settings\n"
        "                    with capture_operation('overlay_dismissal'):\n"
        "                        close_heatmap_settings(page)\n"
        "                with capture_operation('chart_geometry'):\n")
    anchor = "                    page.screenshot("
    text = once(text, anchor,
        "                    from heatmap_hover import dismiss_cookie_banner\n"
        "                    dismiss_cookie_banner(page)\n" + anchor)
    anchor = '                    verify_saved_render(page, timeframe, HEATMAP_MODEL, render_state, path, source_diagnostics)\n'
    text = once(text, anchor, anchor+
        "                from heatmap_hover import require_cookie_banner_hidden\n"
        "                require_cookie_banner_hidden(page)\n"
        "                from heatmap_hover import capture_column\n"
        "                with capture_operation('hover_collection'):\n"
        "                    hover_snapshot = capture_column(page, path, chart_rect, model=HEATMAP_MODEL, timeframe=timeframe)\n")
    text = once(text, "results.append({'image': str(path),", "results.append({'hover_snapshot': hover_snapshot, 'image': str(path),")
    compile(text,str(path),'exec'); path.write_text(text)

    path = runtime/'market_vision/openai_heatmap_scanner.py'
    text = path.read_text()
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
    compile(text,str(path),'exec'); path.write_text(text)

    path = runtime/'collection_model1_task.py'
    text = path.read_text()
    text = once(text, "    stage('analysis')\n",
        "    from heatmap_hover import prepare_candidates\n"
        "    prepare_candidates(images)\n"
        "    stage('analysis')\n")
    text = once(text, '    remember_analysis(raw)',
        "    from heatmap_hover import ground_analysis, attach_measurements\n"
        "    raw = ground_analysis(raw, images[0]['hover_snapshot'])\n"
        "    remember_analysis(raw)")
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
