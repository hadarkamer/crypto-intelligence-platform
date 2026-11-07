  api = load("src/lib/coinglassCollector.ts");
  const core = load("src/lib/coinglassCollectCore.ts");
  const remote = load("src/lib/coinglassCollector.server.ts");
  const deps = {supabase:db,userId:fx.owner,config:remote.readCollectorConfig,
    startJob:remote.startCollectorJob,readJob:remote.readCollectorJob,
    capabilities:remote.readCollectorCapabilities, now:()=>clock};
  const {createServer} = await import('node:http');
  let lastTicket = null;
  const dump = async () => ({rows:await values(),
    runs:await q('select id,session_id,field_id,timeframe,analysis from public.v2_liquidity_ai_runs where session_id=$1 order by created_at,id',[sid]),
    jobs:[...jobs.values()],events,counters});
  const server = createServer(async (req,res)=>{
    try {
      assert.equal(req.socket.remoteAddress,'127.0.0.1');
      let input=''; for await (const c of req) input+=c;
      const d=input?JSON.parse(input):{};
      let output;
      if(req.url==='/') {res.setHeader('content-type','text/html');res.end('<!doctype html><html lang="he" dir="rtl"><meta charset="utf-8"><title>BTC isolated experiment</title><body><div id="root"></div><script type="module" src="/app.js"></script></body></html>');return;}
      if(req.url==='/app.js'){res.setHeader('content-type','text/javascript');res.end(readFileSync(path.join(root,'.github/labs/btc-full-flow/dist/ui.js')));return;}
      if(req.url==='/fixture') output={owner:fx.owner,sid,fields:fx.maps,clock};
      else if(req.url==='/values') output=await values();
      else if(req.url==='/expected-inputs') output=(await values()).map(v=>({field:v.field_id,prices:(v.value.rows??[]).flatMap(row=>(row.levels??[]).map(level=>api.savedLiquidityPriceText(row,level)))}));
      else if(req.url==='/baseline'){
        assert.ok(fx.maps.some(f=>f.id===d.fieldId));
        output=await db.from('v2_collection_values').select('updated_at').eq('session_id',sid).eq('field_id',d.fieldId).maybeSingle();
      } else if(req.url==='/start') {
        output=await core.startCollection(deps,d);
        if(output.ticket) lastTicket=output.ticket;
        events.push({kind:'start',at:performance.now(),input:d,output:{...output,ticket:undefined}});
      } else if(req.url==='/poll') {
        output=await core.pollCollection(deps,d);
        if(!['running','queued'].includes(output.status)) events.push({kind:'terminal',at:performance.now(),output:{...output,ticket:undefined}});
      } else if(req.url==='/repeat-poll') {
        output=await core.pollCollection(deps,{ticket:lastTicket});
        assert.equal(output.status,'duplicate');
      } else if(req.url==='/wrong-model') {
        const count=jobs.size;
        output=await core.startCollection(deps,{sessionId:sid,fieldId:fx.maps[0].id,expectedModel:3,timeframe:'12H',requestId:crypto.randomUUID(),baselineUpdatedAt:null});
        assert.equal(output.status,'failed');assert.equal(jobs.size,count);
      } else if(req.url==='/tampered-ticket') {
        output=await core.pollCollection(deps,{ticket:lastTicket+'x'});assert.equal(output.status,'failed');
      } else if(req.url==='/control') {
        await advance(d.now); await control(d.failedMap??null);output={ok:true};
      } else if(req.url==='/dump') output=await dump();
      else if(req.url==='/finish') {
        output=await dump();writeFileSync(path.join(process.env.QUEUE_LAB_EVIDENCE,'application-evidence.json'),JSON.stringify(output,null,2));
        setTimeout(()=>server.close(),30);
      } else throw Error('Unknown local route '+req.url);
      res.setHeader('content-type','application/json');res.end(JSON.stringify(output));
    } catch(error){console.error(error);res.statusCode=500;res.end(JSON.stringify({error:String(error.stack)}));}
  });
  await new Promise(resolve=>server.listen(21880,'127.0.0.1',resolve));
  console.log('BTC_LOOPBACK_READY');
  await new Promise(resolve=>server.once('close',resolve));
