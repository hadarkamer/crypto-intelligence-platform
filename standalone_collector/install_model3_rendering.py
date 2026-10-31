"""Apply the existing Model 3 policy during preparation and observation."""
import ast
from pathlib import Path
import shutil

HERE = Path(__file__).resolve().parent
FILES = ('model3_rendering.py', 'model3_clean_frame.py', 'model3_render_gate.js')


def expand(text):
    tree = ast.parse(text)
    capture = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'capture_heatmaps')
    if any(isinstance(n, ast.Import) and any(x.name == 'model3_rendering' for x in n.names) for n in tree.body):
        raise RuntimeError('model3_rendering_already_installed')
    contexts = [n for n in ast.walk(capture) if isinstance(n, ast.Assign)
                and isinstance(n.value, ast.Call) and ast.unparse(n.value.func) == 'browser.new_context']
    loops = [n for n in ast.walk(capture) if isinstance(n, ast.For)
             and isinstance(n.target, ast.Name) and n.target.id == 'timeframe']
    if len(contexts) != 1 or len(loops) != 1:
        raise RuntimeError('model3_rendering_capture_layout_changed')
    if not any(isinstance(n, ast.Name) and n.id == 'context' for n in contexts[0].targets):
        raise RuntimeError('model3_rendering_context_layout_changed')
    class ContextSetup(ast.NodeTransformer):
        def visit_Assign(self, node):
            if node is contexts[0]:
                return [node, ast.parse('_model3_rendering.configure_context(context, HEATMAP_MODEL)').body[0]]
            return node
    ContextSetup().visit(capture)
    loop = loops[0]
    positions = [i for i, node in enumerate(loop.body)
                 if any(isinstance(n, ast.Call) and ast.unparse(n.func) == 'page.screenshot' for n in ast.walk(node))]
    if len(positions) != 1:
        raise RuntimeError('model3_rendering_screenshot_layout_changed')
    start = positions[0]
    tail = loop.body[start:]
    if not isinstance(tail[0], ast.With) or len(tail[0].body) != 1:
        raise RuntimeError('model3_rendering_screenshot_wrapper_changed')
    screenshot = tail[0].body[0]
    if not isinstance(screenshot, ast.Expr) or not isinstance(screenshot.value, ast.Call):
        raise RuntimeError('model3_rendering_screenshot_call_changed')
    if ast.unparse(screenshot.value.func) != 'page.screenshot':
        raise RuntimeError('model3_rendering_screenshot_call_changed')
    tail[0].body.insert(0, ast.parse('_prepare_model3_frame()').body[0])
    scope = ast.parse('with _model3_rendering.observation(page, HEATMAP_MODEL, chart_rect) as _prepare_model3_frame:\n    pass').body[0]
    scope.body = tail
    loop.body[start:] = [scope]
    selection = [i for i, node in enumerate(loop.body[:start])
                 if any(isinstance(n, ast.Call) and ast.unparse(n.func) == 'control'
                        and n.args and ast.unparse(n.args[0]) == '_select_timeframe'
                        for n in ast.walk(node))]
    if len(selection) != 1:
        raise RuntimeError('model3_rendering_selection_layout_changed')
    early = ast.parse('with _model3_rendering.preparation(page, HEATMAP_MODEL):\n    pass').body[0]
    early.body = loop.body[selection[0]:start]
    loop.body[selection[0]:start] = [early]
    index = next(i+1 for i, n in enumerate(tree.body) if isinstance(n, ast.ImportFrom) and n.module == '__future__')
    tree.body.insert(index, ast.Import(names=[ast.alias(name='model3_rendering', asname='_model3_rendering')]))
    result = ast.unparse(ast.fix_missing_locations(tree)) + '\n'
    for target in ('page.screenshot', 'capture_column', 'verify_saved_render'):
        before = sum(isinstance(n, ast.Call) and ast.unparse(n.func) == target for n in ast.walk(ast.parse(text)))
        after = sum(isinstance(n, ast.Call) and ast.unparse(n.func) == target for n in ast.walk(ast.parse(result)))
        if before != after or before != 1:
            raise RuntimeError('model3_rendering_original_call_count_changed')
    compile(result, '<model3-rendering>', 'exec')
    return result


def install(runtime: Path):
    target = runtime / 'market_vision/coinglass_heatmap_capture.py'
    changed = expand(target.read_text())
    for name in FILES:
        shutil.copyfile(HERE / name, runtime / name)
    target.write_text(changed, encoding='utf-8')
