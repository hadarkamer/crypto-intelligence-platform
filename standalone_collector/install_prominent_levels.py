"""Install prominent-row extraction only into the standalone runtime copy."""
from pathlib import Path

def once(text,old,new):
    if text.count(old)!=1:raise RuntimeError('Prominent-level source structure changed')
    return text.replace(old,new,1)

def install(runtime:Path):
    path=runtime/'market_vision/openai_heatmap_scanner.py';text=path.read_text()
    text=once(text,'"max_output_tokens": 3500,','"max_output_tokens": 12000,')
    text+='\nfrom prominent_levels import install_schema, INSTRUCTIONS\n'
    text+='install_schema(HEATMAP_SCHEMA)\nSYSTEM_PROMPT += INSTRUCTIONS\n'
    compile(text,str(path),'exec');path.write_text(text,encoding='utf-8')
    path=runtime/'collection_model1_task.py';text=path.read_text()
    text=once(text,'def normalize(', 'def _normalize_legacy_levels(')
    text=once(text,'    result = normalize(raw, timeframe=timeframe, run_id=run_id, captured_at=captured_at, image=image)',
        '    if not isinstance(raw.get("scans"), list) or len(raw["scans"]) != 1 or "prominent_levels" not in raw["scans"][0]:\n'
        '        raise StageFailure("analysis_response_invalid")\n'
        '    result = normalize(raw, timeframe=timeframe, run_id=run_id, captured_at=captured_at, image=image)')
    text+='\n\ndef normalize(raw, *, timeframe, run_id, captured_at, image):\n'
    text+='    from prominent_levels import normalize_prominent\n'
    text+='    scans = raw.get("scans") if isinstance(raw, dict) else None\n'
    text+='    modern = isinstance(scans, list) and any(isinstance(s, dict) and "prominent_levels" in s for s in scans)\n'
    text+='    validator = normalize_prominent if modern else _normalize_legacy_levels\n'
    text+='    return validator(raw, timeframe=timeframe, run_id=run_id, captured_at=captured_at, image=image)\n'
    # A definition appended after __main__ is unavailable during live child
    # execution. Move the sole entry point after all installed definitions.
    marker="if __name__ == '__main__':\n    sys.exit(run_task(main, *sys.argv[1:]))\n"
    text=once(text,marker,'')+'\n'+marker
    compile(text,str(path),'exec');path.write_text(text,encoding='utf-8')
    path=runtime/'collection_bridge.py';text=path.read_text()
    text=once(text,"OR (status='ready'\n",
        "OR (status='ready'\n                AND result->>'selection_policy'='prominent_right_edge.v1'\n")
    text=once(text,'result.get("source_url") != source_url(heatmap_model) or',
        'result.get("source_url") != source_url(heatmap_model) or\n'
        '                result.get("selection_policy") != "prominent_right_edge.v1" or')
    compile(text,str(path),'exec');path.write_text(text,encoding='utf-8')
