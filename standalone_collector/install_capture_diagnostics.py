"""Wrap existing standalone capture operations with safe timing diagnostics."""
import ast
from pathlib import Path

CALL_PHASES={
    'p.chromium.launch':'browser_launch','browser.new_context':'browser_context',
    'context.add_cookies':'source_session_setup','context.new_page':'page_creation',
    'page.goto':'navigation','_dismiss_capture_blockers':'overlay_dismissal',
    '_wait_for_heatmap':'wait_for_chart','_select_model_one':'model_selection',
    '_select_symbol_mode':'symbol_selection','control':'timeframe_selection',
    'page.wait_for_timeout':'render_settle','prepare_legend_for_capture':'legend_preparation',
    'page.evaluate':'chart_geometry','page.screenshot':'screenshot',
    'context.close':'context_close','browser.close':'browser_close',
}

def expand_capture_diagnostics(text):
    tree=ast.parse(text)
    capture=next(node for node in tree.body if isinstance(node,ast.FunctionDef) and node.name=='capture_heatmaps')
    class Operations(ast.NodeTransformer):
        def wrap(self,node):
            if isinstance(node.value,ast.Call):
                phase=CALL_PHASES.get(ast.unparse(node.value.func))
                if phase:
                    return ast.With(items=[ast.withitem(context_expr=ast.Call(
                        func=ast.Name(id='capture_operation',ctx=ast.Load()),
                        args=[ast.Constant(phase)],keywords=[]))],body=[node])
            return node
        def visit_Expr(self,node):return self.wrap(node)
        def visit_Assign(self,node):return self.wrap(node)
    capture.body=[Operations().visit(node) for node in capture.body]
    # The future import remains first; add the diagnostic import after it.
    index=next(i+1 for i,node in enumerate(tree.body)
        if isinstance(node,ast.ImportFrom) and node.module=='__future__')
    tree.body.insert(index,ast.ImportFrom(module='model1_execution',
        names=[ast.alias(name='capture_operation')],level=0))
    return ast.unparse(ast.fix_missing_locations(tree))+'\n'

def install(runtime:Path):
    path=runtime/'market_vision/coinglass_heatmap_capture.py'
    text=expand_capture_diagnostics(path.read_text())
    compile(text,str(path),'exec');path.write_text(text,encoding='utf-8')
