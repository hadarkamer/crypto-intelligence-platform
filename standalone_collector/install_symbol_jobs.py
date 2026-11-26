"""Adapt only the final generated, app-owned bridge to scoped CoinGlass jobs."""
import ast
from pathlib import Path
import shutil


def install(runtime):
    path = runtime/'collection_bridge.py'
    text = path.read_text()
    tree = ast.parse(text)
    store, = [item for item in tree.body if isinstance(item, ast.ClassDef) and item.name == 'JobStore']
    # Replace the implementations in the generated copy; the bot's JobStore remains separate.
    lines = text.splitlines(keepends=True)
    aliases = {'start': 'start_job', 'latest': 'latest_job', 'get': 'get_job', 'evidence': 'get_evidence'}
    methods = [node for node in store.body if isinstance(node, ast.FunctionDef) and node.name in aliases]
    if len(methods) != len(aliases):
        raise RuntimeError('Unexpected job store layout')
    for node in reversed(methods):
        lines[node.lineno-1:node.end_lineno] = [f'    {node.name} = symbol_jobs.{aliases[node.name]}\n']
    text = ''.join(lines)

    def substitute(old, new, expected=1):
        nonlocal text
        if text.count(old) != expected:
            raise RuntimeError('Unexpected collector bridge source at ' + old[:80])
        text = text.replace(old, new)

    substitute('from aiohttp import web', 'from aiohttp import web\nimport symbol_jobs')
    # Migrate dimensions before creating the active-work index. Repeated boots
    # must not recreate the old two-column unique index across modern cycles.
    index = next((line for line in text.splitlines() if 'CREATE UNIQUE INDEX IF NOT EXISTS ai_collection_bridge_one_active_model_tf' in line), None)
    if index is None:
        raise RuntimeError('Missing bridge active-map index')
    substitute(index, '            symbol_jobs.initialize_dimensions(c)')
    substitute("return {'schema_version': schema_version(row.get('heatmap_model', 1)),",
               "return {'symbol': row.get('symbol', 'BTC'), 'attempt': row.get('attempt', 1), "
               "'request_id': str(row['logical_request_id']) if row.get('logical_request_id') else None, "
               "'schema_version': schema_version(row.get('heatmap_model', 1)),")
    substitute('            authorize(request)\n', """            authorize(request)
            scope = (symbol_jobs.verify_symbol(request.query['symbol']),) if 'symbol' in request.query else ()
            if action == 'start' and scope:
                raise BridgeError('invalid_request', 'Send the job symbol in the JSON body')
""")
    substitute('db.latest, tf)', 'db.latest, tf, *scope)')
    substitute('db.get, jid)', 'db.get, jid, *scope)')
    substitute('db.evidence, jid)', 'db.evidence, jid, *scope)')
    substitute('rid, tf = request_fields(json.loads(raw))', 'dimensions = symbol_jobs.request_dimensions(json.loads(raw))')
    substitute('db.start, rid, tf)', 'db.start, *dimensions)')
    substitute('heatmap_model: int=1, session=None)', "heatmap_model: int=1, session=None, symbol='BTC')")
    env_line, = [line for line in text.splitlines() if line.strip() == '_session_child_env = child_environment(heatmap_model)']
    indent = env_line[:len(env_line)-len(env_line.lstrip())]
    substitute(env_line, '\n'.join((env_line,
        indent+"_session_child_env['COLLECTION_HEATMAP_SYMBOL'] = symbol_jobs.verify_symbol(symbol)",
        indent+'_modern_map = symbol_jobs.modern_enabled()')))
    guard = next((line for line in text.splitlines() if "if result.get('schema_version') != schema_version(heatmap_model)" in line), None)
    if guard is None:
        raise RuntimeError('Missing result provenance guard')
    substitute(guard, guard[:len(guard)-len(guard.lstrip())] + 'if not symbol_jobs.valid_result(result, image, jid, tf, heatmap_model, symbol, _modern_map):')
    substitute("run_existing_scanner(row['timeframe'], jid, row.get('heatmap_model', 1))",
               "run_existing_scanner(row['timeframe'], jid, row.get('heatmap_model', 1), symbol=row.get('symbol', 'BTC'))")
    # A service that has only numerical collection configured must leave jobs
    # requiring model analysis queued for their existing configured worker.
    substitute("WHERE status='queued'", "WHERE status='queued' AND (NOT %s OR (symbol,heatmap_model,timeframe)=('BTC',1,'12H'))")
    substitute('ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING *").fetchone()',
               'ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING *", (symbol_jobs.numeric_only(),)).fetchone()')
    # Only the measured BTC pilot receives the private numeric checkpoint channel.
    substitute('from aiohttp import web\nimport symbol_jobs', 'from aiohttp import web\nimport symbol_jobs\nimport modern_map_checkpoint')
    scope = "async with modern_map_checkpoint.workspace(jid,tf,heatmap_model,symbol,JobStore,symbol_jobs.modern_enabled()) as (tmp,_market_channel):"
    anchor = next(line for line in text.splitlines() if 'with tempfile.TemporaryDirectory(' in line and 'collection-model1-' in line)
    indent = anchor[:len(anchor)-len(anchor.lstrip())]
    # Beginning an account lease is unnecessary when the prior source was already committed.
    before = "            if session is not None:\n                await session.begin()\n                _session_begun = True\n"
    substitute(before,'')
    substitute(anchor,indent+scope+'\n'+indent+'    if _market_channel is not None and _market_channel.restored:\n'+indent+'        session = None\n'+indent+'    if session is not None:\n'+indent+'        await session.begin()\n'+indent+'        _session_begun = True')
    spawn = next(line for line in text.splitlines() if 'await asyncio.create_subprocess_exec(' in line and 'collection_model1_task.py' in line)
    launch_indent = spawn[:len(spawn)-len(spawn.lstrip())]
    args = '_session_transport.pass_fds if _session_transport else ()'
    if spawn.count('pass_fds='+args) != 1:
        raise RuntimeError('Unexpected measured checkpoint fd wiring')
    substitute(spawn, launch_indent+'_pass_fds = '+args+'\n'+launch_indent+'if _market_channel is not None:\n'+
               launch_indent+'    _pass_fds = _market_channel.attach(_session_child_env,_pass_fds)\n'+
               spawn.replace('pass_fds='+args, 'pass_fds=_pass_fds'))
    sql_names = {'image_png=EXCLUDED.image_png,diagnostic=EXCLUDED.diagnostic':'RETAIN_DIAGNOSTIC',
                 "WHERE created_at<now()-interval '6 hours' AND image_png IS NOT NULL":'EXPIRE_IMAGES'}
    for token, name in sql_names.items():
        literal, = [n for n in ast.walk(ast.parse(text)) if isinstance(n,ast.Constant)
                    and isinstance(n.value,str) and token in n.value]
        substitute(ast.get_source_segment(text,literal),'modern_map_checkpoint.'+name)
    shutil.copyfile(Path(__file__).with_name('modern_map_checkpoint.py'), runtime/'modern_map_checkpoint.py')
    compile(text, str(path), 'exec')
    path.write_text(text)
    shutil.copyfile(Path(__file__).with_name('symbol_jobs.py'), runtime/'symbol_jobs.py')
