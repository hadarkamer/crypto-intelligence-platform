"""Add a read-only render gate around the existing single original screenshot."""
import ast
from pathlib import Path


def expand_capture_readiness(text):
    tree=ast.parse(text)
    capture=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='capture_heatmaps')
    dismiss=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='_dismiss_capture_blockers')
    # Observe the source's actual data gate before a normal overlay dismissal
    # can remove the explanatory dialog and leave a blurred/loading chart.
    dismiss.body[:0]=ast.parse("require_unblocked_source(page, HEATMAP_MODEL, phase='overlay_dismissal')").body
    common=[i for i,node in enumerate(dismiss.body) if isinstance(node,ast.Expr)
        and isinstance(node.value,ast.Call) and ast.unparse(node.value.func)=='_dismiss_common_overlays']
    if len(common)!=1:raise RuntimeError('Common overlay guard insertion point changed')
    dismiss.body[common[0]+1:common[0]+1]=ast.parse(
        "require_unblocked_source(page, HEATMAP_MODEL, phase='overlay_dismissal')").body
    inserted={'page':0,'shot':0}
    class Gate(ast.NodeTransformer):
        def visit_Assign(self,node):
            if isinstance(node.value,ast.Call) and ast.unparse(node.value.func)=='context.new_page':
                inserted['page']+=1
                return [node,*ast.parse('source_diagnostics = CaptureNetworkDiagnostics(page)').body]
            return node
        def visit_With(self,node):
            self.generic_visit(node)
            if (len(node.body)==1 and isinstance(node.body[0],ast.Expr)
                and isinstance(node.body[0].value,ast.Call)
                and ast.unparse(node.body[0].value.func)=='page.screenshot'):
                inserted['shot']+=1
                before=ast.parse("with capture_operation('render_readiness'):\n    render_state = wait_for_render(page, timeframe, HEATMAP_MODEL)\n    require_unblocked_source(page, HEATMAP_MODEL, phase='render_readiness')").body
                after=ast.parse("with capture_operation('render_readiness'):\n    verify_saved_render(page, timeframe, HEATMAP_MODEL, render_state, path, source_diagnostics)").body
                return [*before,node,*after]
            return node
    capture.body=[Gate().visit(n) for n in capture.body]
    if inserted!={'page':1,'shot':1}:raise RuntimeError('Capture readiness insertion points changed')
    index=next(i+1 for i,n in enumerate(tree.body) if isinstance(n,ast.ImportFrom) and n.module=='__future__')
    tree.body.insert(index,ast.ImportFrom(module='capture_readiness',names=[ast.alias(name=n)
        for n in ('CaptureNetworkDiagnostics','wait_for_render','verify_saved_render','require_unblocked_source')],level=0))
    return ast.unparse(ast.fix_missing_locations(tree))+'\n'


def install(runtime:Path):
    path=runtime/'market_vision/coinglass_heatmap_capture.py'
    text=expand_capture_readiness(path.read_text());compile(text,str(path),'exec')
    path.write_text(text,encoding='utf-8')
