"""Install hover evidence into the same capture, one analysis and existing store."""
from pathlib import Path
from install_prominent_levels import once


def install(runtime: Path):
    path = runtime/'market_vision/coinglass_heatmap_capture.py'
    text = path.read_text()
    text = once(text, "'locale': 'en-US',", "'locale': 'en-US', 'timezone_id': 'UTC',")
    anchor = '                    verify_saved_render(page, timeframe, HEATMAP_MODEL, render_state, path, source_diagnostics)\n'
    text = once(text, anchor, anchor+
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
SYSTEM_PROMPT = SYSTEM_PROMPT.replace(
    'Never fabricate a tooltip or row label absent from the PNG.',
    'Use numeric tooltip evidence only from the supplied computed candidate table; never invent it.')
SYSTEM_PROMPT += """
Candidate pools were located in the same screenshot and calculated from a
complete current-column DOM hover sweep. Return exactly one prominent_levels
entry for EVERY candidate_id. Visually verify its y_top/y_bottom (0=plot top,
1=bottom), colour and continuity. If any candidate is not supported by the image,
mark confidence=low; never silently omit it or widen its boundaries. The code
will reject disagreement rather than present uncertain quantities as verified.
Keep the supplied shape, intensity and prices for confirmed candidates. Existing
colour rules remain: yellow=many, green=normal. Arithmetic never upgrades green
to yellow. The source_row_ids, sums, percentages and exact prices are calculated
by code before this request. Do not recalculate, invent or replace them.
The current price reference is still an honest visual range from the last candle;
never substitute a candidate price, hover price or convenient midpoint for it.
Liquidation Leverage in source_metric units is NOT a verified dollar amount.
Do not call the displayed column total all market liquidity, a historical sum,
or actual realized liquidations. Do not invent dollar totals or percentages.
"""
'''
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
