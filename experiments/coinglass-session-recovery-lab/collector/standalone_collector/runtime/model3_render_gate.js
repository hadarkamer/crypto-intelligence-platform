/* Lab only. Loaded before the guarded Model3 public chunk.
 * attach() is inert: automatic painting is unchanged until enable().
 * Only the chart application's automatic render listener is rate-limited.
 * Shared/system tickers, resize/data updates, DOM and direct renders are intact.
 * No price, liquidity, pool or other market data is inspected or exposed.
 */
(() => {
  if (globalThis.__cg29RenderLab) throw new Error('render_lab_already_installed');
  const LOW=-25; // Verified Pixi8.21.0 source constant, guarded by asset hashes.
  const entries=[];
  const attached=new WeakMap();
  const now=performance.now.bind(performance);
  const get=key=>{const e=entries.find(e=>e.key===key);if(!e)throw new Error('render_lab_unknown_chart');return e;};
  const alive=e=>{
    if(e.app.ticker!==e.ticker||e.app.renderer!==e.renderer||e.renderer.canvas!==e.canvas||e.app.render!==e.render)
      throw new Error('render_lab_chart_changed');
  };
  const describe=e=>{
    let rect=null,id='',className='',width=null,height=null;
    try{const c=e.canvas,r=c.getBoundingClientRect();
      rect={x:r.x,y:r.y,width:r.width,height:r.height};
      id=typeof c.id==='string'?c.id.slice(0,256):'';
      className=typeof c.className==='string'?c.className.slice(0,256):'';
      width=c.width;height=c.height;
    }catch(_){}
    return {key:e.key,enabled:e.enabled,experimental_fps:e.enabled?1000/e.periodMs:null,
      canvas:{id,class:className,width,height,rect},ticker_started:e.ticker.started,
      ticker_count:e.ticker.count,stats:{...e.stats}};
  };
  function attach(app){
    if(attached.has(app))return attached.get(app).key;
    if(!app||!app.ticker||!app.renderer||!app.renderer.canvas||typeof app.render!=='function'||
       typeof app.ticker.add!=='function'||typeof app.ticker.remove!=='function'||
       typeof app.renderer.render!=='function')throw new Error('render_lab_unsupported_chart');
    const e={key:entries.length+1,app,ticker:app.ticker,renderer:app.renderer,canvas:app.renderer.canvas,
      render:app.render,rendererRender:app.renderer.render,
      rendererDescriptor:Object.getOwnPropertyDescriptor(app.renderer,'render'),
      enabled:false,restored:false,periodMs:0,lastAutoPaint:-Infinity,
      stats:{render_calls:0,render_ms:0,automatic_callbacks:0,automatic_paints:0,skipped_paints:0,forced_paints:0}};
    e.observedRender=function(...args){
      const started=now();
      try{return Reflect.apply(e.rendererRender,this,args);}
      finally{e.stats.render_calls++;e.stats.render_ms+=now()-started;}
    };
    e.gated=function(...args){
      e.stats.automatic_callbacks++;
      const at=now();
      if(at-e.lastAutoPaint>=e.periodMs){
        e.lastAutoPaint=at;e.stats.automatic_paints++;
        return Reflect.apply(e.render,this,args);
      }
      e.stats.skipped_paints++;
    };
    app.renderer.render=e.observedRender;
    if(app.renderer.render!==e.observedRender)throw new Error('render_lab_renderer_not_wrappable');
    entries.push(e);attached.set(app,e);
    return e.key;
  }
  function disable(key){
    const e=get(key);alive(e);
    if(e.enabled){
      const count=e.ticker.count;
      e.ticker.remove(e.gated,e.app);
      if(e.ticker.count!==count-1)throw new Error('render_lab_gate_listener_changed');
      e.ticker.add(e.render,e.app,LOW);
      e.enabled=false;e.periodMs=0;
      if(e.ticker.count!==count)throw new Error('render_lab_restore_listener_count');
    }
    return describe(e);
  }
  function enable(key,fps=0.5){
    const e=get(key);alive(e);
    if(e.restored)throw new Error('render_lab_already_restored');
    if(!Number.isFinite(fps)||fps<0.1||fps>5)throw new Error('render_lab_invalid_experimental_fps');
    if(e.enabled)disable(key);
    const count=e.ticker.count;
    e.ticker.remove(e.render,e.app);
    const removed=count-e.ticker.count;
    if(removed!==1){
      for(let i=0;i<removed;i++)e.ticker.add(e.render,e.app,LOW);
      throw new Error('render_lab_original_listener_changed');
    }
    e.periodMs=1000/fps;e.lastAutoPaint=-Infinity;
    try{e.ticker.add(e.gated,e.app,LOW);}
    catch(error){e.ticker.add(e.render,e.app,LOW);e.periodMs=0;throw error;}
    e.enabled=true;
    if(e.ticker.count!==count)throw new Error('render_lab_gate_listener_count');
    return describe(e);
  }
  function forcePaint(key){
    const e=get(key);alive(e);
    e.stats.forced_paints++;
    const result=Reflect.apply(e.render,e.app,[]);
    // Avoid a redundant automatic repaint immediately after this fresh frame.
    // Explicit redraws and all non-render ticker callbacks continue normally.
    if(e.enabled)e.lastAutoPaint=now();
    return {key:e.key,render_calls:e.stats.render_calls,forced_paints:e.stats.forced_paints};
  }
  function restoreAll(){
    const results=[];
    for(const e of entries){
      if(e.restored){results.push({key:e.key,restored:true});continue;}
      try{
        disable(e.key);
        if(e.renderer.render!==e.observedRender)throw new Error('render_lab_renderer_changed');
        if(e.rendererDescriptor)Object.defineProperty(e.renderer,'render',e.rendererDescriptor);
        else delete e.renderer.render;
        e.restored=true;
        results.push({key:e.key,restored:true});
      }catch(error){results.push({key:e.key,restored:false,reason:error.message});}
    }
    return results;
  }
  Object.defineProperty(globalThis,'__cg29RenderLab',{configurable:true,
    value:{attach,enable,disable,forcePaint,restoreAll,snapshot:()=>entries.map(describe)}});
})();
