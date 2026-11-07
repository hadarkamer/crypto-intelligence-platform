import { OPEN_INTEREST_POINTS, OPEN_INTEREST_SOURCE } from "./collectionOpenInterestChart";

/** Inspect only the public rendered source chart. Never read account/network state,
 * synthesize a series, change source values, or pass an access challenge. */
const HELPERS = String.raw`
 const norm = x => typeof x === 'string' ? x.trim().replace(/\s+/g,' ') : '';
 const visible = e => {
  if (!e || !e.isConnected) return false;
  const r=e.getBoundingClientRect(); if(!(r.width>0&&r.height>0)) return false;
  for(let n=e;n;n=n.parentElement){const s=getComputedStyle(n);if(s.display==='none'||s.visibility==='hidden'||s.visibility==='collapse'||s.opacity!==''&&Number(s.opacity)===0)return false;}
  return true;
 };
 const fail = reason => {throw new Error(reason);};
 const gated = () => {
  if(/^(Attention Required|Just a moment|Access Denied)/i.test(document.title)||/Sorry, you have been blocked|Verify you are human/i.test(document.body.innerText.slice(0,1500)))return true;
  return [...document.querySelectorAll('[role="dialog"],.MuiModal-root')].some(e=>visible(e)&&/log\s*in|sign\s*in|unlock|upgrade|subscribe/i.test(e.innerText));
 };
 const cardFor = () => {
  if(location.href !== '${OPEN_INTEREST_SOURCE}' || gated())fail('wrong_or_gated_source');
  const h=[...document.querySelectorAll('h2')].filter(e=>norm(e.innerText)==='Exchange BTC Open Interest (USD)');
  if(h.length!==1)fail('aggregate_heading_missing');
  const card=h[0].parentElement.parentElement;
  if(card.querySelectorAll('h2').length!==1||card.querySelectorAll('[_echarts_instance_]').length!==1)fail('ambiguous_chart');
  return {card,heading:h[0],element:card.querySelector('[_echarts_instance_]')};
 };
 const instance = element => {
  if(window.echarts?.getInstanceByDom){const c=window.echarts.getInstanceByDom(element);if(c?.getDom()===element)return c;}
  const key=Object.keys(element).find(k=>k.startsWith('__reactFiber$')||k.startsWith('__reactInternalInstance$'));let fiber=key&&element[key];
  for(let i=0;fiber&&i<32;i++,fiber=fiber.return){const w=fiber.stateNode;if(w?.ele===element&&w.echarts?.getInstanceByDom){const c=w.echarts.getInstanceByDom(element);if(c?.getDom()===element)return c;}}
  fail('chart_api_unavailable');
 };
 const date = text => {
  const m=/^(\d{1,2}) (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) (\d{4}), (\d{2}):(\d{2})$/.exec(text);
  if(!m)fail('invalid_date');const month=['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'].indexOf(m[2]);
  const d=new Date(+m[3],month,+m[1],+m[4],+m[5]);
  if(d.getFullYear()!==+m[3]||d.getMonth()!==month||d.getDate()!==+m[1]||d.getHours()!==+m[4]||d.getMinutes()!==+m[5])fail('invalid_date');return d.toISOString();
 };
 const metrics = () => {
  const rows=[...document.querySelectorAll('tr')].filter(r=>[...r.querySelectorAll('td')].some(c=>norm(c.innerText)==='All'));
  if(rows.length!==1)fail('aggregate_metrics_missing');const row=rows[0];let table=row;
  for(let i=0;i<8&&table&&!table.querySelector('thead');i++)table=table.parentElement;
  if(!table)fail('metric_headers_missing');const headers=[...table.querySelectorAll('thead th')].map(e=>norm(e.innerText));const cells=[...row.querySelectorAll('td')].map(e=>norm(e.innerText));
  const oi=headers.indexOf('OI'),change=headers.indexOf('OI Change (24h)'),rate=headers.indexOf('Rate');
  if(oi<0||change<0||rate<0||cells[rate]!=='100%')fail('metric_headers_missing');
  const total=/^\$([0-9]+(?:\.[0-9]+)?)([KMBT])$/.exec(cells[oi]),p=/^([+−-]?[0-9]+(?:\.[0-9]+)?)%$/.exec(cells[change]);
  if(!total||!p)fail('invalid_metrics');return {tableTotalOiUsd:+total[1]*({K:1e3,M:1e6,B:1e9,T:1e12})[total[2]],change24hPercent:Number(p[1].replace('−','-'))};
 };
 const read = () => {
  const {card,heading,element}=cardFor(),chart=instance(element);if(!visible(element)||chart.isDisposed?.())fail('hidden_chart');
  const controls=[...card.querySelectorAll('[role="combobox"]')];
  if(controls.length!==3||norm(controls[0].innerText)!=='Exchanges'||norm(controls[1].innerText)!=='1 day'||norm(controls[2].innerText)!=='USD')fail('wrong_chart_settings');
  let exchanges;try{exchanges=JSON.parse(controls[0].parentElement.querySelector('input').value);}catch{fail('unverified_exchanges');}
  if(!Array.isArray(exchanges)||!["CME","Binance","Bybit","OKX"].every(x=>exchanges.includes(x)))fail('unverified_exchanges');
  const option=chart.getOption();if(!Array.isArray(option.series)||option.series.length!==2)fail('unverified_series');
  const oi=option.series.find(s=>s.name==='Open Interest'&&s.type==='bar'&&(s.yAxisIndex??0)===0),price=option.series.find(s=>s.name==='BTC Price'&&s.type==='line'&&s.yAxisIndex===1);
  const labels=option.xAxis?.[0]?.data;
  if(!oi||!price||option.xAxis.length!==1||option.xAxis[0].type!=='category'||option.yAxis?.length!==2||option.yAxis.some(a=>a.type!=='value')||!Array.isArray(labels)||labels.length<${OPEN_INTEREST_POINTS}||labels.length>20000||oi.data?.length!==labels.length||price.data?.length!==labels.length)fail('unverified_series');
  if(option.legend?.some(l=>l.selected?.['Open Interest']===false||l.selected?.['BTC Price']===false))fail('hidden_series');
  const start=labels.length-${OPEN_INTEREST_POINTS},points=labels.slice(start).map((label,i)=>({at:date(label),oiUsd:oi.data[start+i],btcUsd:price.data[start+i]}));
  if(points.some(p=>typeof p.oiUsd!=='number'||!Number.isFinite(p.oiUsd)||p.oiUsd<=0||typeof p.btcUsd!=='number'||!Number.isFinite(p.btcUsd)||p.btcUsd<=0))fail('invalid_values');
  return {card,heading,element,chart,option,points,exchanges,...metrics(),startIndex:start,endIndex:labels.length-1,totalSourcePoints:labels.length};
 };
 const crop = card => {
  const r=card.getBoundingClientRect(),x=Math.floor(r.left),y=Math.floor(r.top),w=Math.ceil(r.right)-x,h=Math.ceil(r.bottom)-y;
  if(devicePixelRatio!==1||innerWidth!==1400||innerHeight!==900||x<0||y<0||x+w>innerWidth||y+h>innerHeight||w<1000||h<450)fail('unreadable_chart');
  for(const [px,py] of [[x+w/2,y+15],[x+w/2,y+h/2],[x+w/2,y+h-15]])if(!card.contains(document.elementFromPoint(px,py)))fail('obscured_chart');
  return {sourceWidth:innerWidth,sourceHeight:innerHeight,cropX:x,cropY:y,width:w,height:h};
 };
 const signature = s => JSON.stringify({points:s.points,exchanges:s.exchanges,totalSourcePoints:s.totalSourcePoints,tableTotalOiUsd:s.tableTotalOiUsd,change24hPercent:s.change24hPercent});
 const rendered = s => {
  const zoom=s.option.dataZoom;if(!Array.isArray(zoom)||zoom.length!==2||zoom.some(z=>(z.xAxisIndex??0)!==0||z.startValue!==s.startIndex||z.endValue!==s.endIndex||z.filterMode!=='filter'))fail('unverified_window');
  const model=s.chart.getModel();for(const item of model.getSeries()){const d=item.getData();if(d.count()!==${OPEN_INTEREST_POINTS}||d.getRawIndex(0)!==s.startIndex||d.getRawIndex(d.count()-1)!==s.endIndex)fail('unverified_rendered_points');}
 };
`;

export const OPEN_INTEREST_PREPARE_SCRIPT = String.raw`(async () => {
 const finish=x=>JSON.stringify({stage:'open_interest_prepare',...x});
 ${HELPERS}
 window.__collectionOpenInterest=null;
 try {
  const {card}=cardFor();
  for(const [index,label] of [[1,'1 day'],[2,'USD']]){
   const controls=[...card.querySelectorAll('[role="combobox"]')];if(controls.length!==3)fail('wrong_chart_settings');
   if(norm(controls[index].innerText)!==label){controls[index].click();await new Promise(r=>setTimeout(r,200));const choices=[...document.querySelectorAll('[role="option"]')].filter(e=>norm(e.innerText)===label&&visible(e));if(choices.length!==1)fail('setting_unavailable');choices[0].click();await new Promise(r=>setTimeout(r,1200));}
  }
  let s;for(let i=0;i<30;i++){try{s=read();break;}catch(e){if(i===29)throw e;await new Promise(r=>setTimeout(r,300));}}
  s.chart.dispatchAction({type:'dataZoom',startValue:s.startIndex,endValue:s.endIndex});
  window.scrollBy(0,s.heading.getBoundingClientRect().top-120);await new Promise(r=>setTimeout(r,600));
  s=read();rendered(s);
  const proof={version:'btc_open_interest.v1',sourceUrl:'${OPEN_INTEREST_SOURCE}',symbol:'BTC',scope:'aggregate',unit:'USD',interval:'1d',capturedAt:new Date().toISOString(),exchanges:s.exchanges,totalOiUsd:s.points.at(-1).oiUsd,tableTotalOiUsd:s.tableTotalOiUsd,change24hPercent:s.change24hPercent,points:s.points,totalSourcePoints:s.totalSourcePoints,startIndex:s.startIndex,endIndex:s.endIndex,...crop(s.card)};
  window.__collectionOpenInterest={chart:s.chart,element:s.element,signature:signature(s),proof};return finish({ok:true,proof});
 }catch(e){return finish({ok:false,reason:e.message});}
})()`;

export const OPEN_INTEREST_VERIFY_SCRIPT = String.raw`(() => {
 const finish=x=>JSON.stringify({stage:'open_interest_verify',...x});
 ${HELPERS}
 try{
  const state=window.__collectionOpenInterest;if(!state)fail('capture_not_prepared');const s=read();rendered(s);
  if(s.chart!==state.chart||s.element!==state.element||signature(s)!==state.signature||JSON.stringify(crop(s.card))!==JSON.stringify(Object.fromEntries(['sourceWidth','sourceHeight','cropX','cropY','width','height'].map(k=>[k,state.proof[k]]))))fail('changed_capture');
  return finish({ok:true,proof:state.proof});
 }catch(e){return finish({ok:false,reason:e.message});}
})()`;
