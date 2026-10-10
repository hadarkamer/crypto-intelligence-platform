"""Add diagnostics to the standalone job routes without changing their errors."""
from pathlib import Path


def once(text,old,new):
    if text.count(old)!=1:raise RuntimeError('Store diagnostic source structure changed')
    return text.replace(old,new,1)


def install(runtime:Path):
    path=runtime/'collection_bridge.py';text=path.read_text()
    text=once(text,'from aiohttp import web',
        'from aiohttp import web\nfrom store_diagnostics import log_store_failure')
    text=once(text,
        '        except Exception:\n'
        '            return web.json_response({"error": {"code": "store_unavailable", "message": "Job store unavailable"}}, status=503)',
        '        except Exception as error:\n'
        '            log_store_failure(action,error)\n'
        '            return web.json_response({"error": {"code": "store_unavailable", "message": "Job store unavailable"}}, status=503)')
    compile(text,str(path),'exec');path.write_text(text,encoding='utf-8')
