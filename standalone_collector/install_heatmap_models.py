"""Final build adapter for Models1–3. Root bot files are never edited.

This adds an identity dimension, not a relaxed validator. Source sessions,
normal chart controls, private evidence, numeric range checks and quota logic
remain the existing implementation. Defaults preserve the Model1 contract.
"""
from pathlib import Path
import ast

IMPORT='from heatmap_models import HEATMAP_MODEL, MODEL_LABEL, SOURCE_URL, SCHEMA_VERSION\n'
URL='https://www.coinglass.com/pro/futures/LiquidationHeatMap?coin=BTC&type=symbol'


def once(text,old,new):
    if text.count(old)!=1:
        raise RuntimeError('Model adapter expected one match: '+old[:100])
    return text.replace(old,new,1)


def save(path,text):
    compile(text,str(path),'exec')
    path.write_text(text,encoding='utf-8')


def identity_module(text):
    text=once(text,'from __future__ import annotations\n','from __future__ import annotations\n'+IMPORT)
    tree=ast.parse(text)
    # Replace only identity constants and output dictionary identity values.
    # Integer prices, validation thresholds and confidence checks are untouched.
    class Identity(ast.NodeTransformer):
        def visit_Assign(self,node):
            self.generic_visit(node)
            names=[x.id for x in node.targets if isinstance(x,ast.Name)]
            if any(x in ('SOURCE','COINGLASS_HEATMAP_URL') for x in names):
                node.value=ast.Name(id='SOURCE_URL',ctx=ast.Load())
            if 'SCHEMA' in names:node.value=ast.Name(id='SCHEMA_VERSION',ctx=ast.Load())
            return node
        def visit_Dict(self,node):
            self.generic_visit(node)
            for i,key in enumerate(node.keys):
                if isinstance(key,ast.Constant) and key.value=='heatmap_model':
                    if isinstance(node.values[i],ast.Constant) and node.values[i].value==1:
                        node.values[i]=ast.Name(id='HEATMAP_MODEL',ctx=ast.Load())
            return node
        def visit_Compare(self,node):
            self.generic_visit(node)
            # All remaining literal Model1 comparisons are observed identity.
            node.comparators=[ast.Name(id='MODEL_LABEL',ctx=ast.Load())
                if isinstance(x,ast.Constant) and x.value=='Model 1' else x for x in node.comparators]
            return node
    return ast.unparse(ast.fix_missing_locations(Identity().visit(tree)))+'\n'


def install(runtime:Path):
    # Child pipeline identity is initialized before default capture URL is bound.
    for name in ('market_vision/coinglass_heatmap_capture.py','collection_model1_task.py','model1_price_range.py'):
        path=runtime/name;text=path.read_text()
        if name.endswith('coinglass_heatmap_capture.py'):
            text=once(text,'re.compile(r"^Model 1$", re.I)','re.compile(rf"^Model {HEATMAP_MODEL}$", re.I)')
            # Do not claim Model1's threshold for other model displays.
            text=once(text,'"liquidity_threshold": 0.85','"liquidity_threshold": (0.85 if HEATMAP_MODEL == 1 else None)')
        save(path,identity_module(text))
    path=runtime/'market_vision/openai_heatmap_scanner.py';text=path.read_text()
    text+='\nfrom heatmap_models import MODEL_LABEL\n'
    text+='HEATMAP_SCHEMA["properties"]["scans"]["items"]["properties"]["observed_model"]["enum"] = [MODEL_LABEL, "other", "unknown"]\n'
    text+='SYSTEM_PROMPT += "\\nRequested heatmap: " + MODEL_LABEL + ". Verify that exact active model in the screenshot. Another model is not acceptable. Never infer a missing model label."\n'
    save(path,text)

    path=runtime/'collection_bridge.py';text=path.read_text()
    text=once(text,'from aiohttp import web','from aiohttp import web\nfrom heatmap_models import model_number, schema_version, source_url, child_environment')
    text=once(text,'return {"schema_version": SCHEMA, "job_id": str(row["id"]),',
        'return {"schema_version": schema_version(row.get("heatmap_model", 1)), "job_id": str(row["id"]),')
    text=once(text,'def __init__(self, dsn: str, hourly_limit: int = 4):',
        'def __init__(self, dsn: str, hourly_limit: int = 4, heatmap_model: int = 1):')
    text=once(text,'        self.dsn = dsn','        self.dsn = dsn\n        self.heatmap_model = model_number(heatmap_model)')
    jobs_end='                finished_at timestamptz, result jsonb, image_png bytea, error_code text)""")'
    text=once(text,jobs_end,jobs_end+'\n'
        '            c.execute("ALTER TABLE public.ai_collection_bridge_jobs ADD COLUMN IF NOT EXISTS heatmap_model integer NOT NULL DEFAULT 1 CHECK (heatmap_model IN (1,2,3))")\n'
        '            c.execute("DROP INDEX IF EXISTS public.ai_collection_bridge_one_active_tf")')
    text=once(text,'ai_collection_bridge_one_active_tf\n                ON public.ai_collection_bridge_jobs(timeframe)',
        'ai_collection_bridge_one_active_model_tf\n                ON public.ai_collection_bridge_jobs(heatmap_model,timeframe)')
    request_end='                created_at timestamptz NOT NULL DEFAULT now())""")'
    text=once(text,request_end,request_end+'\n'
        '            c.execute("ALTER TABLE public.ai_collection_bridge_requests ADD COLUMN IF NOT EXISTS heatmap_model integer NOT NULL DEFAULT 1 CHECK (heatmap_model IN (1,2,3))")')
    text=once(text,'SELECT r.timeframe AS requested_timeframe, j.*',
        'SELECT r.timeframe AS requested_timeframe, r.heatmap_model AS requested_model, j.*')
    text=once(text,'if prior["requested_timeframe"] != tf:',
        'if prior["requested_timeframe"] != tf or prior.get("requested_model", 1) != self.heatmap_model:')
    text=once(text,'WHERE timeframe=%s\n                AND (status',
        'WHERE heatmap_model=%s AND timeframe=%s\n                AND (status')
    text=once(text,'ORDER BY created_at DESC LIMIT 1""", (tf,)).fetchone()',
        'ORDER BY created_at DESC LIMIT 1""", (self.heatmap_model, tf)).fetchone()')
    text=once(text,'INSERT INTO public.ai_collection_bridge_jobs(id,timeframe,status)\n                    VALUES(%s,%s,\'queued\') RETURNING *""", (str(uuid4()), tf)',
        'INSERT INTO public.ai_collection_bridge_jobs(id,timeframe,status,heatmap_model)\n                    VALUES(%s,%s,\'queued\',%s) RETURNING *""", (str(uuid4()), tf, self.heatmap_model)')
    text=once(text,'INSERT INTO public.ai_collection_bridge_requests(id,timeframe,job_id) VALUES(%s,%s,%s)", (rid, tf, row["id"])',
        'INSERT INTO public.ai_collection_bridge_requests(id,timeframe,job_id,heatmap_model) VALUES(%s,%s,%s,%s)", (rid, tf, row["id"], self.heatmap_model)')
    text=once(text,'SELECT id,timeframe,status,result,error_code FROM public.ai_collection_bridge_jobs WHERE id=%s", (jid,)',
        'SELECT id,timeframe,status,result,error_code,heatmap_model FROM public.ai_collection_bridge_jobs WHERE id=%s AND heatmap_model=%s", (jid,self.heatmap_model)')
    text=once(text,"WHERE j.id=%s AND j.status IN ('ready','failed')",
        "WHERE j.id=%s AND j.heatmap_model=%s AND j.status IN ('ready','failed')")
    text=once(text,"AND j.created_at > now()-interval '6 hours'\"\"\", (jid,)",
        "AND j.created_at > now()-interval '6 hours'\"\"\", (jid,self.heatmap_model)")
    text=once(text,'async def run_existing_scanner(tf: str, jid: str)',
        'async def run_existing_scanner(tf: str, jid: str, heatmap_model: int = 1)')
    text=once(text,'            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)',
        '            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL, env=child_environment(heatmap_model))')
    text=once(text,'result.get("schema_version") != SCHEMA or result.get("run_id") != jid',
        'result.get("schema_version") != schema_version(heatmap_model) or result.get("heatmap_model") != heatmap_model or result.get("run_id") != jid')
    text=once(text,'result.get("source_url") != SOURCE','result.get("source_url") != source_url(heatmap_model)')
    text=once(text,'await run_existing_scanner(row["timeframe"], jid)',
        'await run_existing_scanner(row["timeframe"], jid, row.get("heatmap_model",1))')
    text=once(text,'start_worker: bool = True) -> None:',
        'start_worker: bool = True, heatmap_model: int = 1) -> None:')
    text=once(text,'    active = (os.getenv(',
        '    heatmap_model = model_number(heatmap_model)\n    active = (os.getenv(')
    text=once(text,'JobStore(dsn, int(os.getenv("COLLECTION_BRIDGE_HOURLY_LIMIT", "4")))',
        'JobStore(dsn, int(os.getenv("COLLECTION_BRIDGE_HOURLY_LIMIT", "4")), heatmap_model=heatmap_model)')
    # Only route literals are parameterized. Source URLs still come from the allowlist.
    for old,new in [
        ('"/api/collection/model1/jobs"','f"/api/collection/model{heatmap_model}/jobs"'),
        ('"/api/collection/model1/jobs/latest"','f"/api/collection/model{heatmap_model}/jobs/latest"'),
        ('"/api/collection/model1/jobs/{job_id}"','f"/api/collection/model{heatmap_model}/jobs/{{job_id}}"'),
        ('"/api/collection/model1/jobs/{job_id}/evidence"','f"/api/collection/model{heatmap_model}/jobs/{{job_id}}/evidence"')]:
        text=once(text,old,new)
    # One shared worker claims rows across all models. Additional routes only
    # bind a model-scoped view of that queue; they never start extra workers.
    text+='\n\n_register_one_model = register_collection_routes\n'
    text+='def register_collection_routes(app, *, store=None, token=None, enabled=None, start_worker=True):\n'
    text+='    _register_one_model(app,store=store,token=token,enabled=enabled,start_worker=start_worker,heatmap_model=1)\n'
    text+='    for model in (2,3):\n'
    text+='        scoped = JobStore(store.dsn,store.hourly_limit,heatmap_model=model) if store is not None else None\n'
    text+='        _register_one_model(app,store=scoped,token=token,enabled=enabled,start_worker=False,heatmap_model=model)\n'
    save(path,text)

    path=runtime/'model1_cache_policy.py';text=path.read_text()
    text=once(text,'SELECT id,timeframe,status,result,error_code','SELECT id,timeframe,status,result,error_code,heatmap_model')
    text=once(text,"WHERE timeframe=%s AND status='ready'", "WHERE heatmap_model=%s AND timeframe=%s AND status='ready'")
    text=once(text,'LIMIT 1""", (timeframe,)).fetchone()',
        'LIMIT 1""", (getattr(store,"heatmap_model",1),timeframe)).fetchone()')
    save(path,text)
