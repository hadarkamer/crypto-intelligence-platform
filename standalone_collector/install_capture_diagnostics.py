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


def expand_capture_efficiency(text):
    tree=ast.parse(text)
    common=next(node for node in tree.body if isinstance(node,ast.FunctionDef) and node.name=='_dismiss_common_overlays')
    labels=next(node.value for node in common.body if isinstance(node,ast.Assign)
        and any(isinstance(target,ast.Name) and target.id=='labels' for target in node.targets))
    if ast.literal_eval(labels)!=['Accept','Accept all','Allow all','I agree','Agree','Got it','OK','Close']:
        raise RuntimeError('Common overlay labels changed')
    replacement=ast.parse(r'''
def _dismiss_common_overlays(page: Page) -> None:
    # One exact-name source query, at most eight inspected buttons and one
    # ordinary click. The caller checks the source-data gate before and after.
    matches=page.get_by_role('button',name=re.compile(r'^(?:Accept|Accept all|Allow all|I agree|Agree|Got it|OK|Close)$',re.I))
    for index in range(min(matches.count(),8)):
        try:
            control=matches.nth(index)
            visible=control.is_visible()
        except Exception:
            continue
        if visible:
            try:
                control.click(timeout=1500)
                page.wait_for_timeout(300)
            except Exception:
                pass
            return
''').body[0]
    tree.body[tree.body.index(common)]=replacement
    select=next(node for node in tree.body if isinstance(node,ast.FunctionDef) and node.name=='_select_timeframe')
    class TimeframeOperations(ast.NodeTransformer):
        selected=0
        def wrap(self,node,phase):
            return ast.With(items=[ast.withitem(context_expr=ast.Call(
                func=ast.Name(id='capture_operation',ctx=ast.Load()),args=[ast.Constant(phase)],keywords=[]))],body=[node])
        def assignment(self,node,names):
            if not isinstance(node.value,ast.Call):return node
            function=ast.unparse(node.value.func)
            if 'selected' in names and function=='_first_visible':
                self.selected+=1
                return self.wrap(node,'timeframe_trigger_lookup' if self.selected==1 else 'timeframe_confirmation')
            if any(name in ('trigger','label','ancestor') for name in names):
                return self.wrap(node,'timeframe_trigger_lookup')
            if 'option' in names:return self.wrap(node,'timeframe_option_lookup')
            return node
        def visit_Assign(self,node):
            return self.assignment(node,[target.id for target in node.targets if isinstance(target,ast.Name)])
        def visit_AnnAssign(self,node):
            return self.assignment(node,[node.target.id] if isinstance(node.target,ast.Name) else [])
        def visit_Expr(self,node):
            if isinstance(node.value,ast.Call):
                phase={'trigger.click':'timeframe_open','option.click':'timeframe_select'}.get(ast.unparse(node.value.func))
                if phase:return self.wrap(node,phase)
            return node
    operations=TimeframeOperations()
    select.body=[operations.visit(node) for node in select.body]
    if operations.selected!=2:raise RuntimeError('Timeframe confirmation layout changed')
    return ast.unparse(ast.fix_missing_locations(tree))+'\n'

def install(runtime:Path):
    path=runtime/'market_vision/coinglass_heatmap_capture.py'
    text=expand_capture_efficiency(expand_capture_diagnostics(path.read_text()))
    compile(text,str(path),'exec');path.write_text(text,encoding='utf-8')
