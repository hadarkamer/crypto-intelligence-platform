"""Add the opt-in measured-map path to the generated standalone worker only."""
import ast
from pathlib import Path
import shutil


def replace_once(text, old, new):
    if text.count(old)!=1:raise ValueError('Modern capture integration anchor changed')
    return text.replace(old,new)


def install(runtime):
    base=Path(__file__).resolve().parent
    for name in ('modern_map_evidence.py','modern_map_axis.py','modern_map_surface.py','modern_map_measure_panel.py','modern_map_overview.py','modern_map_tooltip.py','modern_map_cells.py',
                 'modern_map_reader.py','modern_map_task.py','collection_metrics.py',
                 'dedicated_login_form.py','dedicated_map_flow.py'):
        shutil.copyfile(base/name,runtime/name)
    path=runtime/'collection_model1_task.py'
    source=path.read_text()
    fn=next(f for f in ast.parse(source).body if isinstance(f,ast.FunctionDef) and f.name=='main')
    lines=source.splitlines(keepends=True)
    lines.insert(fn.body[0].lineno-1,"    import os\n    if os.getenv('COINGLASS_MODERN_UI_ENABLED','').lower()=='true':\n"
                 "        from modern_map_task import run_modern_task\n        return run_modern_task(timeframe, run_id, output)\n")
    source=''.join(lines)
    compile(source,str(path),'exec')
    path.write_text(source)
    path=runtime/'market_vision/openai_heatmap_scanner.py'
    source=path.read_text()
    i=source.index('def analyze_heatmap_images(')
    j=source.index('def analyze_heatmap_image(',i)
    fn=source[i:j]
    fn=replace_once(fn,'    timeout_seconds: int = 120,','    timeout_seconds: int = 120,\n'
                    '    evidence_schema: dict | None = None,\n    evidence_instructions: str | None = None,\n    before_request=None,')
    fn=replace_once(fn,'        count += 1',"        if evidence_schema is not None:\n"
                    "            content.append({'type':'input_text','text':'Candidate locations: '+json.dumps(image['measure_candidates'])})\n"
                    '        count += 1')
    fn=replace_once(fn,'"instructions": SYSTEM_PROMPT','"instructions": evidence_instructions or SYSTEM_PROMPT')
    fn=replace_once(fn,'"schema": HEATMAP_SCHEMA','"schema": evidence_schema or HEATMAP_SCHEMA')
    fn=replace_once(fn,'    response = requests.post(', "    if evidence_schema is not None:\n"
                    "        from collection_metrics import provider_request\n        provider_request()\n    if before_request is not None:\n        before_request()\n    response = requests.post(")
    source=source[:i]+fn+source[j:]
    compile(source,str(path),'exec')
    path.write_text(source)
