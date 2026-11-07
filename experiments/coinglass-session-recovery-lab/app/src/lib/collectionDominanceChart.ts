import type { ScreenshotCrop } from "./collectionScreenshotCrop.server";
import {
  dominanceMetricsFromBars,
  type DominanceDailyMetrics,
} from "./collectionDominanceMetrics.ts";

export type DominanceChartSymbol =
  "BTC.D" | "USDT.D" | `USDT.D+${string}` | `USDC.D+${string}` | `DAI.D+${string}`;

export function dominanceChartSourceSymbol(symbol: DominanceChartSymbol): string {
  const parts = symbol.split("+");
  if (
    !(parts.length === 1 && ["BTC.D", "USDT.D"].includes(parts[0])) &&
    !(
      parts.length >= 2 &&
      parts.length <= 3 &&
      new Set(parts).size === parts.length &&
      parts.every((part) => ["USDT.D", "USDC.D", "DAI.D"].includes(part))
    )
  )
    throw new Error("סמל הדומיננס אינו נתמך.");
  return parts.map((part) => `CRYPTOCAP:${part}`).join("+");
}

export type VerifiedDominanceChart = DominanceDailyMetrics & {
  version: "tradingview_daily.v2";
  symbol: DominanceChartSymbol;
  interval: "1D";
  width: number;
  height: number;
  capturedAt: string;
  latestBarAt: string;
  dominancePercent: number;
  barCount: number;
};

/** Read only the public Data Window at observed candle positions. The screenshot
 * is taken by the provider between prepare/verify; no chart is redrawn. */
function dominanceBrowserScript(symbol: DominanceChartSymbol, verify: boolean): string {
  const sourceSymbol = dominanceChartSourceSymbol(symbol);
  return `(async () => {
    const symbol=${JSON.stringify(symbol)},sourceSymbol=${JSON.stringify(sourceSymbol)},verify=${verify};
    const measured=(${dominanceMetricsFromBars.toString()});
    const stage=verify?'dominance_chart_verify':'dominance_chart';
    const fail=reason=>JSON.stringify({stage,ok:false,reason});
    const deadline=Date.now()+30000,pause=ms=>new Promise(resolve=>setTimeout(resolve,ms));
    const visible=n=>{const r=n.getBoundingClientRect(),s=getComputedStyle(n);return r.width>0&&r.height>0&&r.right>0&&r.bottom>0&&r.left<innerWidth&&r.top<innerHeight&&s.visibility!=='hidden'&&s.display!=='none';};
    const one=selector=>{const nodes=[...document.querySelectorAll(selector)].filter(visible);if(nodes.length!==1)throw Error('ambiguous_chart');return nodes[0];};
    const dataTab=()=>[...document.querySelectorAll('[role="tab"]')].filter(visible).find(e=>e.textContent.trim()==='Data window');
    const row=()=>{
      const values=['Date','Open','High','Low','Close'].map(name=>one('[data-test-id-value-title="'+name+'"]').nextElementSibling?.innerText.trim());
      const date=/^(Sun|Mon|Tue|Wed|Thu|Fri|Sat) (\\d{2}) (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) '(\\d{2})$/.exec(values[0]);
      if(!date)throw Error('unreadable_date');
      const month=['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'].indexOf(date[3]);
      const at=Date.UTC(2000+Number(date[4]),month,Number(date[2])),parsed=new Date(at);
      if(parsed.getUTCMonth()!==month||parsed.getUTCDate()!==Number(date[2])||['Sun','Mon','Tue','Wed','Thu','Fri','Sat'][parsed.getUTCDay()]!==date[1])throw Error('invalid_date');
      const prices=values.slice(1).map(v=>{if(!/^\\d+(?:\\.\\d+)?%$/.test(v))throw Error('unreadable_price');return Number(v.slice(0,-1));});
      return [at/1000,...prices];
    };
    try {
      if(!verify)delete window.__collectionDominanceCapture;
      const u=new URL(location.href);
      const rawSymbols=u.search.slice(1).split('&').filter(pair=>decodeURIComponent(pair.split('=')[0])==='symbol');
      const locatedSymbol=rawSymbols.length===1?decodeURIComponent(rawSymbols[0].slice(rawSymbols[0].indexOf('=')+1)):null;
      if(u.protocol!=='https:'||u.hostname!=='www.tradingview.com'||u.port||u.username||u.password||u.pathname!=='/chart/'||u.searchParams.getAll('symbol').length!==1||u.searchParams.getAll('interval').length!==1||locatedSymbol!==sourceSymbol||u.searchParams.get('interval')!=='D')return fail('wrong_source');
      if(!verify){
        if(!dataTab())one('button[data-name="object_tree"]').click();
        while(!dataTab()&&Date.now()<deadline)await pause(100);
        if(!dataTab())throw Error('data_window_unavailable');
        if(dataTab().getAttribute('aria-selected')!=='true')dataTab().click();
        while(![...document.querySelectorAll('[data-test-id-value-title="Date"]')].some(visible)&&Date.now()<deadline)await pause(100);
      }
      const pane=one('.chart-gui-wrapper'),canvas=pane.querySelector('canvas:not([data-qa-id])'),target=pane.querySelector('canvas[data-qa-id="pane-top-canvas"]'),widget=pane.closest('.chart-widget');
      const identity=()=>{
        if(!canvas||!target||!widget||!visible(canvas)||!visible(target)||!visible(widget)||target.getAttribute('aria-label')!=='Chart for '+sourceSymbol+', 1 day'||dataTab()?.getAttribute('aria-selected')!=='true'||!/^\\d{2}:\\d{2}:\\d{2} UTC$/.test(one('[data-name="time-zone-menu"]').innerText.trim()))throw Error('wrong_chart');
        const r=widget.getBoundingClientRect(),p=canvas.getBoundingClientRect(),d=devicePixelRatio;
        if(r.left<0||r.top<0||r.right>innerWidth||r.bottom>innerHeight||p.width<600||p.height<350||canvas.width>3000||canvas.height>2000)throw Error('invalid_dimensions');
        return {sourceWidth:Math.round(innerWidth*d),sourceHeight:Math.round(innerHeight*d),cropX:Math.floor(r.left*d),cropY:Math.floor(r.top*d),width:Math.ceil(r.right*d)-Math.floor(r.left*d),height:Math.ceil(r.bottom*d)-Math.floor(r.top*d),pane:[p.left,p.top,p.width,p.height,canvas.width,canvas.height]};
      };
      const geometry=identity();
      const stable=async()=>{await pause(50);const a=row();await pause(50);const b=row();if(JSON.stringify(a)!==JSON.stringify(b))throw Error('unstable_candle');return b;};
      if(verify){
        const prior=window.__collectionDominanceCapture;
        if(!prior||prior.canvas!==canvas||prior.target!==target||prior.symbol!==symbol||JSON.stringify(prior.geometry)!==JSON.stringify(geometry)||JSON.stringify(await stable())!==JSON.stringify(prior.chart.dailyBars.at(-1)))throw Error('chart_changed');
        delete window.__collectionDominanceCapture;
        return JSON.stringify({stage,ok:true,chart:prior.chart,crop:prior.crop});
      }
      const r=canvas.getBoundingClientRect(),w=canvas.width,h=canvas.height,pixels=canvas.getContext('2d').getImageData(0,0,w,h).data;
      const colored=at=>pixels[at+3]>100&&(pixels[at+1]>100&&pixels[at]<pixels[at+1]*.8&&pixels[at+2]<pixels[at+1]||pixels[at]>150&&pixels[at+1]<pixels[at]*.7&&pixels[at+2]<pixels[at]*.7);
      const groups=[];
      for(let x=0;x<w;x++){
        let run=0,hit=false;for(let y=0;y<h;y++){run=colored((y*w+x)*4)?run+1:0;if(run>=2){hit=true;break;}}
        if(hit){const last=groups.at(-1);if(last&&last[1]===x-1)last[1]=x;else groups.push([x,x]);}
      }
      const points=groups.filter(g=>g[1]-g[0]+1<w*.1).map(g=>r.left+(g[0]+g[1]+1)/2*r.width/w).reverse();
      const rows=[];
      const hover=x=>target.dispatchEvent(new MouseEvent('mousemove',{bubbles:true,clientX:x,clientY:r.top+r.height*.4}));
      for(const x of points){
        if(Date.now()>=deadline)throw Error('daily_chart_unavailable');
        hover(x);const current=await stable(),previous=rows.at(-1);
        if(previous&&current[0]===previous[0]){if(JSON.stringify(current)!==JSON.stringify(previous))throw Error('changed_candle');continue;}
        if(previous&&previous[0]-current[0]!==86400)throw Error('daily_history_gap');
        rows.push(current);if(rows.length===40)break;
      }
      if(rows.length<35)throw Error('daily_history_unavailable');
      hover(points[0]);const latest=await stable();
      if(latest[0]!==rows[0][0]||JSON.stringify(geometry)!==JSON.stringify(identity()))throw Error('chart_changed');
      rows[0]=latest;
      const captured=Date.now(),metrics=measured(rows.reverse(),captured);
      if(!metrics||captured>=deadline)throw Error('stale_or_invalid');
      const {pane:ignored,...crop}=geometry;
      const chart={version:'tradingview_daily.v2',symbol,interval:'1D',width:crop.width,height:crop.height,capturedAt:new Date(captured).toISOString(),latestBarAt:new Date(latest[0]*1000).toISOString(),barCount:rows.length,...metrics};
      window.__collectionDominanceCapture={symbol,canvas,target,geometry,crop,chart};
      return JSON.stringify({stage,ok:true,chart,crop});
    }catch(error){delete window.__collectionDominanceCapture;return fail(error?.message||'capture_failed');}
  })()`;
}
export function dominanceChartCaptureScript(symbol: DominanceChartSymbol): string {
  return dominanceBrowserScript(symbol, false);
}
export function dominanceChartVerifyScript(symbol: DominanceChartSymbol): string {
  return dominanceBrowserScript(symbol, true);
}

export function verifiedDominanceChart(
  returns: unknown[],
  expected: DominanceChartSymbol,
  screenshots: unknown,
  now = Date.now(),
): { image: string; chart: VerifiedDominanceChart; crop: ScreenshotCrop } {
  const entries = returns
    .map((raw) => {
      const value = raw && typeof raw === "object" && "value" in raw ? raw.value : raw;
      try {
        return typeof value === "string" ? JSON.parse(value) : value;
      } catch {
        return null;
      }
    })
    .filter((value) => ["dominance_chart", "dominance_chart_verify"].includes(value?.stage));
  const prepare = entries.filter((value) => value.stage === "dominance_chart");
  const verify = entries.filter((value) => value.stage === "dominance_chart_verify");
  const entry = prepare.length === 1 && verify.length === 1 ? prepare[0] : null;
  const chart = entry?.chart,
    crop = entry?.crop;
  const capturedAt = Date.parse(chart?.capturedAt),
    barAt = Date.parse(chart?.latestBarAt);
  const measured = dominanceMetricsFromBars(chart?.dailyBars, capturedAt);
  if (
    entry?.ok !== true ||
    verify[0]?.ok !== true ||
    JSON.stringify(verify[0]?.chart) !== JSON.stringify(chart) ||
    JSON.stringify(verify[0]?.crop) !== JSON.stringify(crop) ||
    !Array.isArray(screenshots) ||
    screenshots.length !== 2 ||
    typeof screenshots[1] !== "string" ||
    !screenshots[1] ||
    !crop ||
    [crop.sourceWidth, crop.sourceHeight, crop.cropX, crop.cropY, crop.width, crop.height].some(
      (value) => !Number.isInteger(value),
    ) ||
    crop.sourceWidth < 640 ||
    crop.sourceWidth > 4096 ||
    crop.sourceHeight < 400 ||
    crop.sourceHeight > 4000 ||
    crop.cropX < 0 ||
    crop.cropY < 0 ||
    crop.cropX + crop.width > crop.sourceWidth ||
    crop.cropY + crop.height > crop.sourceHeight ||
    crop.width !== chart?.width ||
    crop.height !== chart?.height ||
    chart?.version !== "tradingview_daily.v2" ||
    chart.symbol !== expected ||
    chart.interval !== "1D" ||
    !Number.isInteger(chart.width) ||
    chart.width < 640 ||
    chart.width > 3000 ||
    !Number.isInteger(chart.height) ||
    chart.height < 400 ||
    chart.height > 2000 ||
    !Number.isFinite(chart.dominancePercent) ||
    chart.dominancePercent < 0 ||
    chart.dominancePercent > 100 ||
    !Number.isInteger(chart.barCount) ||
    chart.barCount < 35 ||
    chart.barCount < chart.dailyBars?.length ||
    !measured ||
    chart.dominancePercent !== measured.dominancePercent ||
    (["1D", "1W", "1M"] as const).some(
      (period) =>
        chart.changes?.[period] !== measured.changes[period] ||
        chart.baselines?.[period]?.at !== measured.baselines[period].at ||
        chart.baselines?.[period]?.close !== measured.baselines[period].close,
    ) ||
    chart.trend !== measured.trend ||
    measured.dailyBars.at(-1)![0] * 1000 !== barAt ||
    !Number.isFinite(capturedAt) ||
    capturedAt > now + 60000 ||
    now - capturedAt > 10 * 60 * 1000 ||
    !Number.isFinite(barAt) ||
    barAt > capturedAt + 60000 ||
    capturedAt - barAt > 36 * 60 * 60 * 1000 ||
    barAt % 86400000 !== 0
  )
    throw new Error("לא אומת צילום גרף יומי של הדומיננס.");
  return { image: screenshots[1], chart, crop };
}
