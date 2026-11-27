"""Install protocol fences in the pinned generated 33bad ledger, after its normal build."""
from hashlib import sha256
from pathlib import Path

HERE = Path(__file__).resolve().parent


def install(runtime):
    bridge = runtime/'collection_bridge.py'
    cache = runtime/'model1_cache_policy.py'
    text = bridge.read_text()
    latest = cache.read_text()
    if (sha256(bridge.read_bytes()).hexdigest() != 'ccb98befa94c322888522771e435da32ceaa5ff16decf9882ef65c0e1e1f0543'
        or sha256(cache.read_bytes()).hexdigest() != '08c25329224c25fc2ef67030fb2d01e9209d75f8f321a3e8038cc65f9501d54e'):
        raise ValueError('Unexpected legacy ledger runtime')

    def change(old, new, count=1):
        nonlocal text
        if text.count(old) != count:
            raise RuntimeError('Unexpected legacy ledger anchor: ' + old[:55])
        text = text.replace(old,new)

    change('from store_diagnostics import log_store_failure','import scoped_http\nfrom store_diagnostics import log_store_failure')
    change("""c.execute(\"\"\"CREATE UNIQUE INDEX IF NOT EXISTS ai_collection_bridge_one_active_model_tf
                ON public.ai_collection_bridge_jobs(heatmap_model,timeframe) WHERE status IN ('queued','running')\"\"\")""",
           'scoped_http.initialize(c)')
    change('if row and "48H" not in', 'if row and "1W" not in')
    change("CHECK(timeframe IN ('12H','24H','48H'))", "CHECK(timeframe IN ('12H','24H','48H','1W'))", 2)

    change('if prior["requested_timeframe"] != tf', 'if prior["fixed_cycle_id"] is not None or prior["requested_timeframe"] != tf')
    change("WHERE status='running'", "WHERE fixed_cycle_id IS NULL AND status='running'", 3)
    change("SET image_png=NULL WHERE created_at <", "SET image_png=NULL WHERE fixed_cycle_id IS NULL AND created_at <")
    change("DELETE FROM public.ai_collection_bridge_jobs WHERE created_at <", "DELETE FROM public.ai_collection_bridge_jobs WHERE fixed_cycle_id IS NULL AND created_at <")
    change("WHERE created_at<now()-interval '6 hours' AND image_png IS NOT NULL",
           "WHERE created_at<now()-interval '6 hours' AND image_png IS NOT NULL\n"
           "                AND EXISTS (SELECT 1 FROM public.ai_collection_bridge_jobs j WHERE j.id=job_id AND j.fixed_cycle_id IS NULL)")
    change("SELECT * FROM public.ai_collection_bridge_jobs WHERE heatmap_model=%s AND timeframe=%s",
           "SELECT * FROM public.ai_collection_bridge_jobs WHERE fixed_cycle_id IS NULL AND heatmap_model=%s AND timeframe=%s")
    change("WHERE id=%s AND heatmap_model=%s", "WHERE id=%s AND fixed_cycle_id IS NULL AND heatmap_model=%s")
    change("WHERE j.id=%s AND j.heatmap_model=%s", "WHERE j.id=%s AND j.fixed_cycle_id IS NULL AND j.heatmap_model=%s")
    change("WHERE id=%s AND status='running'", "WHERE id=%s AND fixed_cycle_id IS NULL AND status='running'")
    change("WHERE id=(SELECT id FROM public.ai_collection_bridge_jobs WHERE status='queued'",
           "WHERE id=(SELECT id FROM public.ai_collection_bridge_jobs WHERE fixed_cycle_id IS NULL AND status='queued'")
    change('            row = c.execute("""UPDATE public.ai_collection_bridge_jobs SET status=\'running\',started_at=now()',
           '            if scoped_http.earlier_fixed_head(c):\n                c.commit()\n                c.close()\n                return None\n'
           '            row = c.execute("""UPDATE public.ai_collection_bridge_jobs SET status=\'running\',started_at=now()')

    change('            if action == "latest":\n                tf = request.query.get("timeframe")',
           '            symbol = getattr(request, "query", {}).get("symbol", "BTC")\n'
           '            if action == "latest":\n'
           '                if symbol != "BTC" or "fixed_cycle_id" in request.query:\n'
           '                    raise BridgeError("no_cached_result", "No saved observation for this scope", 404)\n'
           '                tf = request.query.get("timeframe")')
    change('                    rid, tf = request_fields(json.loads(raw))', '                    body = json.loads(raw)')
    change('                value = await asyncio.to_thread(db.start, rid, tf)',
           '                if isinstance(body, dict) and "fixed_cycle_id" in body:\n'
           '                    value = await scoped_http.start(db, body)\n'
           '                else:\n'
           '                    rid, tf = request_fields(body)\n'
           '                    value = await asyncio.to_thread(db.start, rid, tf)')
    change('                    image = await asyncio.to_thread(db.evidence, jid)',
           '                    image = await asyncio.to_thread(db.evidence, jid) if symbol == "BTC" else None\n'
           '                    if image is None and isinstance(db, JobStore):\n'
           '                        image = await asyncio.to_thread(scoped_http.exact, db, jid, symbol, True)')
    change('                value = await asyncio.to_thread(db.get, jid)',
           '                value = await asyncio.to_thread(db.get, jid) if symbol == "BTC" else None\n'
           '                if value is None and isinstance(db, JobStore):\n'
           '                    value = await asyncio.to_thread(scoped_http.exact, db, jid, symbol)')
    change('            body, headers = error_response(exc)',
           '            body, headers = error_response(exc)\n'
           '            if getattr(exc,"fixed_cycle_rate_limit",False):\n'
           '                body["error"].update(retry_after_seconds=exc.retry_after_seconds,retry_at=exc.retry_at)\n'
           '                headers["Retry-After"] = str(exc.retry_after_seconds)')
    source = "WHERE heatmap_model=%s AND timeframe=%s AND status='ready'"
    if latest.count(source) != 1:
        raise ValueError('Unexpected legacy latest lookup')
    latest = latest.replace(source, "WHERE fixed_cycle_id IS NULL AND heatmap_model=%s AND timeframe=%s AND status='ready'")
    compile(text,str(bridge),'exec')
    compile(latest,str(cache),'exec')
    bridge.write_text(text)
    cache.write_text(latest)


if __name__ == '__main__':
    install(HERE/'runtime')
