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
_hover_level['properties']['source_row_ids'] = {'type':'array','minItems':1,'maxItems':1600,'items':{'type':'string','pattern':'^r[0-9]+$'}}
_hover_level['required'].append('source_row_ids')
SYSTEM_PROMPT = SYSTEM_PROMPT.replace(
    'Never fabricate a tooltip or row label absent from the PNG.',
    'Use numeric tooltip evidence only from the supplied verified hover table; never invent it.')
SYSTEM_PROMPT += """
The verified hover row table supplements the same screenshot. For each selected
visual pool, return source_row_ids matching its location in the image, using
y_fraction (0=plot top, 1=bottom) and exact displayed prices. Select every row
inside a continuous pool; do not skip intervening rows or reuse rows across pools.
A thin yellow point selects one row. A continuous range selects two or more.
Never widen a visual pool to collect more value. Existing colour rules remain:
yellow=many, green=normal; arithmetic never upgrades green to yellow.
Tooltip table prices are valid numeric source evidence even when not printed in
the screenshot. They supersede axis interpolation for selected rows. The code
will replace selected pool prices and calculate values from their source IDs.
Liquidation Leverage in source_metric units is NOT a verified dollar amount.
Do not call the displayed column total all market liquidity, a historical sum,
or actual realized liquidations. Do not invent dollar totals or percentages.
"""
'''
    compile(text,str(path),'exec'); path.write_text(text)

    path = runtime/'collection_model1_task.py'
    text = path.read_text()
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
