"""App-only interval and evidence integration. No source or inference during build."""
from pathlib import Path
import ast


def replace_once(text,old,new):
    if text.count(old)!=1:raise RuntimeError('Range adapter source structure changed')
    return text.replace(old,new,1)


def install(runtime:Path):
    # Extend known error enums without changing any authentication behavior.
    path=runtime/'model1_execution.py';text=path.read_text()
    text+='\nCODES = CODES | frozenset({"price_range_uncertain", "price_range_too_wide", "no_unambiguous_zones", "evidence_store_failed"})\n'
    path.write_text(text,encoding='utf-8')

    path=runtime/'market_vision/openai_heatmap_scanner.py';text=path.read_text()
    text+='\nfrom model1_price_range import RANGE_SCHEMA, RANGE_INSTRUCTIONS\n'
    text+='_range_scan = HEATMAP_SCHEMA["properties"]["scans"]["items"]\n'
    text+='_range_scan["properties"]["current_price_range"] = RANGE_SCHEMA\n'
    text+='_range_scan["required"].append("current_price_range")\n'
    text+='SYSTEM_PROMPT += RANGE_INSTRUCTIONS\n'
    compile(text,str(path),'exec');path.write_text(text,encoding='utf-8')

    # The detail is the SAME frame; a range does not claim an exact printed price.
    path=runtime/'price_detail_input.py';text=path.read_text()
    text+='\nDETAIL_TEXT += "\\nFor current_price_range, confidently bound the last candle close using the visible ticks; range confidence is separate from exact-price confidence. Do not invent an exact price. A narrow range is acceptable when an exact label is absent.\\n"\n'
    path.write_text(text,encoding='utf-8')

    path=runtime/'collection_model1_task.py';text=path.read_text()
    old_ast=next(n for n in ast.parse(text).body if isinstance(n,ast.FunctionDef) and n.name=='normalize')
    text=replace_once(text,'def normalize(', 'def _normalize_point(')
    wrapper='''def normalize(raw, *, timeframe, run_id, captured_at, image):
    from model1_price_range import normalize_with_range
    return normalize_with_range(raw, timeframe=timeframe, run_id=run_id,
        captured_at=captured_at, image=image, point_validator=_normalize_point)


'''
    text=replace_once(text,'def main(',wrapper+'def main(')
    text=replace_once(text,'    remember_analysis(raw)\n',
        '    remember_analysis(raw)\n'
        '    from model1_evidence_format import save_analysis_snapshot\n'
        '    save_analysis_snapshot(raw, root, images[0])\n')
    point_ast=next(n for n in ast.parse(text).body if isinstance(n,ast.FunctionDef) and n.name=='_normalize_point')
    point_ast.name='normalize'
    if ast.dump(old_ast,include_attributes=False)!=ast.dump(point_ast,include_attributes=False):
        raise RuntimeError('Legacy exact-point validator changed')
    compile(text,str(path),'exec');path.write_text(text,encoding='utf-8')

    path=runtime/'collection_bridge.py';text=path.read_text()
    marker='            c.execute("REVOKE ALL ON public.ai_collection_bridge_jobs, public.ai_collection_bridge_requests FROM PUBLIC")'
    addition='''
            c.execute("""CREATE TABLE IF NOT EXISTS public.ai_collection_bridge_evidence (
                job_id uuid PRIMARY KEY REFERENCES public.ai_collection_bridge_jobs(id) ON DELETE CASCADE,
                created_at timestamptz NOT NULL DEFAULT now(), image_png bytea,
                diagnostic jsonb NOT NULL)""")
            c.execute("REVOKE ALL ON public.ai_collection_bridge_evidence FROM PUBLIC")
'''
    text=replace_once(text,marker,marker+addition)
    method='''    def retain_evidence(self, jid, image, diagnostic):
        from psycopg.types.json import Jsonb
        with self.connect() as c:
            c.execute("""INSERT INTO public.ai_collection_bridge_evidence(job_id,image_png,diagnostic)
                VALUES(%s,%s,%s) ON CONFLICT(job_id) DO UPDATE SET
                image_png=EXCLUDED.image_png,diagnostic=EXCLUDED.diagnostic""",
                (jid,image,Jsonb(diagnostic)))
            c.execute("""UPDATE public.ai_collection_bridge_evidence SET image_png=NULL
                WHERE created_at<now()-interval '6 hours' AND image_png IS NOT NULL""")

'''
    text=replace_once(text,'    def start(self, rid:',method+'    def start(self, rid:')
    old_sql='''SELECT image_png FROM public.ai_collection_bridge_jobs
                WHERE id=%s AND status='ready' AND created_at > now()-interval '6 hours' '''.rstrip()
    new_sql='''SELECT COALESCE(j.image_png,e.image_png) AS image_png
                FROM public.ai_collection_bridge_jobs j
                LEFT JOIN public.ai_collection_bridge_evidence e ON e.job_id=j.id
                WHERE j.id=%s AND j.status IN ('ready','failed')
                AND j.created_at > now()-interval '6 hours' '''.rstrip()
    text=replace_once(text,old_sql,new_sql)
    marker='            await asyncio.wait_for(process.wait(), timeout=300)'
    retain='''
            # Preserve private evidence before TemporaryDirectory removes the files.
            # A stored screenshot is diagnostic evidence, NOT acceptance of prices.
            from model1_evidence_format import evidence_payload
            retained_image, diagnostic = evidence_payload(tmp, tf)
            try:
                evidence_store = JobStore(os.getenv("DATABASE_URL", ""))
                await asyncio.to_thread(evidence_store.retain_evidence, jid, retained_image, diagnostic)
            except Exception:
                raise BridgeError("evidence_store_failed", "Private evidence could not be retained", 503) from None
            print("MODEL1_EVIDENCE " + json.dumps({"job_id": jid,
                "image_retained": retained_image is not None, "diagnostic_retained": True}), flush=True)
'''
    text=replace_once(text,marker,marker+retain)
    compile(text,str(path),'exec');path.write_text(text,encoding='utf-8')
