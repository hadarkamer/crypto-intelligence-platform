/** Read the public chart's displayed tooltips when ECharts is bundled rather
 * than exposed on window. No application state, credentials or private APIs. */
export type EtfCandle = { date: string; inflowMusd: number };
export type VerifiedEtfSnapshot = {
  inflowMusd: number;
  date: string;
  candles?: EtfCandle[];
};

export const ETF_TOOLTIP_SCRIPT = `(async () => {
 const finish = result => JSON.stringify({stage:'etf_tooltips',...result});
 const shown = node => {
  const rect=node.getBoundingClientRect();
  if(rect.width<=0||rect.height<=0||rect.right<=0||rect.bottom<=0||rect.left>=innerWidth||rect.top>=innerHeight) return false;
  for(let p=node;p;p=p.parentElement){const s=getComputedStyle(p);if(s.display==='none'||s.visibility==='hidden'||s.visibility==='collapse'||s.opacity!==''&&Number(s.opacity)===0) return false;}
  return true;
 };
 const visible = node => {
  const rect = node.getBoundingClientRect();
  return rect.width>=200&&rect.height>=100&&shown(node);
 };
 const amount = raw => {
  const m=raw.replace(/,/g,'').match(/^(-?)\\$([0-9]+(?:\\.[0-9]+)?)([KMB]?)$/i);
  return m ? Number(m[2])*(m[1] ? -1 : 1)*({K:.001,M:1,B:1000,'':.000001}[m[3].toUpperCase()]) : null;
 };
 const started=Date.now(); let requestedMs=0, reads=0;
 const pause = async (ms=16) => {
  requestedMs+=ms;
  if(requestedMs>30000||Date.now()-started>30000) throw new Error('read_budget_exceeded');
  await new Promise(resolve=>setTimeout(resolve,ms));
  if(Date.now()-started>30000) throw new Error('read_budget_exceeded');
 };
 const recentPublicRange = async card => {
  // In the provider's headless browser the public MUI popup can retain its
  // transition opacity of zero despite complete, laid-out calendar controls.
  // This exception is confined to that structured public date picker; source
  // charts and numerical tooltips still require the original opacity check.
  const calendarControl=node=>{
   const rect=node.getBoundingClientRect();
   if(rect.width<=0||rect.height<=0||rect.right<=0||rect.bottom<=0||rect.left>=innerWidth||rect.top>=innerHeight) return false;
   for(let p=node;p;p=p.parentElement){const s=getComputedStyle(p);if(s.display==='none'||s.visibility==='hidden'||s.visibility==='collapse') return false;}
   return true;
  };
  const buttons=[...card.querySelectorAll('button')].filter(n=>/^Date\\s+Range\\b/i.test(n.innerText.trim())&&calendarControl(n));
  if(buttons.length===0) return;
  if(buttons.length!==1) throw new Error('range_unavailable');
  const rangeButton=buttons[0], months=['January','February','March','April','May','June','July','August','September','October','November','December'];
  const monthPattern=new RegExp('^('+months.join('|')+') (\\\\d{4})$');
  const calendar=()=>{
   const headers=[...document.querySelectorAll('button')].filter(n=>calendarControl(n)&&monthPattern.test(n.innerText.trim()));
   if(headers.length!==2) return null;
   const columns=headers.map(header=>{const m=header.innerText.trim().match(monthPattern);return {header,column:header.parentElement?.parentElement,month:Number(m[2])*12+months.indexOf(m[1])};});
   if(!columns[0].column||!columns[1].column||columns[0].column.parentElement!==columns[1].column.parentElement||Math.abs(columns[0].month-columns[1].month)!==1) return null;
   return {columns,root:columns[0].column.parentElement};
  };
  const waitCalendar=async()=>{
   // Wait for the laid-out structured picker, never toggle the range button
   // again or silently keep the previous date range.
   for(let attempt=0;attempt<6;attempt++){
    await pause(250); const current=calendar(); if(current) return current;
   }
   throw new Error('range_unavailable');
  };
  rangeButton.click(); await waitCalendar();
  const today=new Date(Date.now()), midnight=Date.UTC(today.getUTCFullYear(),today.getUTCMonth(),today.getUTCDate());
  const from=new Date(midnight-45*86400000), until=new Date(midnight+86400000);
  const choose=async date=>{
   const target=date.getUTCFullYear()*12+date.getUTCMonth();
   for(let step=0;step<=3;step++){
    const current=calendar()||await waitCalendar();
    const column=current.columns.find(c=>c.month===target);
    if(column){
     const days=[...column.column.querySelectorAll('button')].filter(n=>calendarControl(n)&&n.innerText.trim()===String(date.getUTCDate())&&!n.disabled&&n.getAttribute('aria-disabled')!=='true');
     if(days.length!==1) throw new Error('range_unavailable');
     days[0].click(); await pause(250); return;
    }
    if(step===3) throw new Error('range_unavailable');
    const direction=target<Math.min(...current.columns.map(c=>c.month))?'Left':'Right';
    const nav=[...current.root.querySelectorAll('button')].filter(n=>calendarControl(n)&&!n.disabled&&n.querySelector('[data-testid="KeyboardArrow'+direction+'RoundedIcon"]'));
    if(nav.length!==1) throw new Error('range_unavailable');
    nav[0].click(); await waitCalendar();
   }
  };
  await choose(from); await choose(until); await pause(1000);
  const normalize=s=>s.replace(/[\\s,]/g,'').replace(/[\\u2010-\\u2015]/g,'-').toLowerCase();
  const currentCharts=[...document.querySelectorAll('[_echarts_instance_]')].filter(visible);
  if(currentCharts.length!==1) throw new Error('range_unavailable');
  const currentCard=currentCharts[0].parentElement.parentElement;
  const expected=['short','long'].flatMap(month=>['numeric','2-digit'].map(day=>{
   const format=new Intl.DateTimeFormat('en-US',{month,day,year:'numeric',timeZone:'UTC'});
   return normalize(format.format(from))+'-'+normalize(format.format(until));
  }));
  const captions=[...currentCard.querySelectorAll('button')].filter(calendarControl).filter(n=>expected.includes(normalize(n.innerText)));
  if(captions.length!==1) throw new Error('range_mismatch');
 };
 try {
  if(window.__collectionEtfVerified === true) return finish({ok:false,reason:'already_captured'});
  const source=new URL(location.href);
  const bitcoinPage=source.pathname==='/assets/etf/Total_Crypto_Spot_ETF_Fund_Flow' && source.searchParams.size===1 && source.searchParams.get('page')==='usBTC' || /^\\/assets\\/etf\\/us-btc-spot\\/?$/.test(source.pathname) && source.searchParams.size===0;
  if (source.protocol!=='https:'||source.hostname!=='sosovalue.com'||source.username||source.password||source.port||!bitcoinPage||!/Total Bitcoin Spot ETF Net Inflow/i.test(document.body.innerText)) return finish({ok:false,reason:'wrong_source'});
  const charts=[...document.querySelectorAll('[_echarts_instance_]')].filter(visible);
  if(charts.length!==1) return finish({ok:false,reason:'ambiguous_chart'});
  await recentPublicRange(charts[0].parentElement.parentElement);
  const updated=[...document.querySelectorAll('[_echarts_instance_]')].filter(visible);
  if(updated.length!==1) return finish({ok:false,reason:'ambiguous_chart'});
  const chart=updated[0], canvas=chart.querySelector('canvas[data-zr-dom-id]'), card=chart.parentElement.parentElement;
  if (!canvas) return finish({ok:false,reason:'chart_unavailable'});
  const daily=[...card.querySelectorAll('.tab-item')].find(n=>/^daily$/i.test(n.innerText.trim()));
  if(!daily||!daily.className.includes('!text-primary')) return finish({ok:false,reason:'not_daily'});
  const buttons=[...card.querySelectorAll('button')].filter(n=>/^Daily Total Net Inflow\\s/i.test(n.innerText.trim()));
  if(buttons.length!==1) return finish({ok:false,reason:'inflow_unavailable'});
  const header=amount(buttons[0].innerText.trim().replace(/^Daily Total Net Inflow\\s*/i, '').trim());
  if(header===null) return finish({ok:false,reason:'unknown_units'});
  const rect=canvas.getBoundingClientRect();
  // Read the desktop chart: a narrow history compresses distinct candles into
  // the same pointer position, so it cannot provide reliable daily values.
  if(rect.width<600) return finish({ok:false,reason:'chart_too_narrow'});
  // Chrome rounds MouseEvent coordinates to integers. PointerEvent preserves
  // subpixels while its unchanged mouse event type reaches the same listener.
  if(typeof PointerEvent!=='function'||new PointerEvent('mousemove',{clientX:.5}).clientX!==.5) return finish({ok:false,reason:'pointer_precision_unavailable'});
  const move=(x,y,type='mousemove',buttons=0)=>{
   const event=new PointerEvent(type,{bubbles:true,clientX:x,clientY:y,buttons,button:0,pointerType:'mouse',isPrimary:true});
   if(event.clientX!==x||event.clientY!==y) throw new Error('pointer_precision_unavailable');
   canvas.dispatchEvent(event);
  };
  const tooltip=()=>{
   const rows=[...document.querySelectorAll('.zIndex999')].filter(shown).map(node=>{
    const m=node.innerText.replace(/\\s+/g,' ').trim().match(/^(\\d{4}\\/\\d{2}\\/\\d{2}) Daily Total Net Inflow: (-?\\$[0-9,.]+[KMB]?) Total Net Assets:/i);
    if(!m) return null;
    const value=amount(m[2]);
    return value===null ? null : {date:m[1].replace(/\\//g,'-'),inflowMusd:value};
   }).filter(Boolean);
   return rows.length===1 ? rows[0] : null;
  };
  const values=new Map();
  const sweep=async limit=>{
   if(!canvas.isConnected||chart.querySelector('canvas[data-zr-dom-id]')!==canvas) throw new Error('changed_data');
   const width=canvas.width, height=canvas.height, scaleX=rect.width/width, scaleY=rect.height/height;
   if(!Number.isInteger(width)||!Number.isInteger(height)||width<=0||height<=0||width*height>16000000) throw new Error('pixels_unavailable');
   let pixels;
   try {pixels=canvas.getContext('2d')?.getImageData(0,0,width,height).data;} catch {throw new Error('pixels_unavailable');}
   if(!pixels||pixels.length!==width*height*4) throw new Error('pixels_unavailable');
   // Public rendered pixels locate the bars only. Dates and money are read
   // exclusively from their tooltips. The lower slider is outside this ROI.
   const color=at=>pixels[at+3]>100 ? pixels[at+1]>140&&pixels[at+1]>pixels[at]*1.4&&pixels[at+1]>pixels[at+2]*1.2 ? 1 : pixels[at]>150&&pixels[at]>pixels[at+1]*1.5&&pixels[at]>pixels[at+2]*1.5 ? -1 : 0 : 0;
   const columns=[], bottom=Math.floor(height*.85);
   for(let x=0;x<width;x++){
    const runs=[]; let kind=0, start=0;
    for(let y=0;y<=bottom;y++){
     const next=y<bottom?color((y*width+x)*4):0;
     if(next!==kind){if(kind) runs.push({kind,start,end:y-1});kind=next;start=y;}
    }
    columns.push(runs);
   }
   const groups=[];
   for(let x=0;x<width;){
    if(!columns[x].length){x++;continue;}
    const start=x; while(x<width&&columns[x].length) x++;
    const end=x-1, center=Math.floor((start+end)/2);
    if((end-start+1)*scaleX<=rect.width*.15) groups.push({x:center,runs:columns[center]});
   }
   // Genuine positive/negative bars meet the same zero baseline. This keeps
   // colored legend glyphs out of the candidates without discarding tiny bars.
   const tolerance=Math.max(1,Math.ceil(3/scaleY)), votes=new Map();
   for(const group of groups){
    const seen=new Set();
    for(const run of group.runs){const base=run.kind>0?run.end:run.start;for(let y=base-tolerance;y<=base+tolerance;y++) seen.add(y);}
    for(const y of seen) votes.set(y,(votes.get(y)||0)+1);
   }
   const baseline=[...votes].sort((a,b)=>b[1]-a[1])[0];
   if(!baseline||baseline[1]<21) throw new Error('unreadable_candles');
   const points=groups.map(group=>{
    const run=group.runs.filter(r=>Math.abs((r.kind>0?r.end:r.start)-baseline[0])<=tolerance).sort((a,b)=>(b.end-b.start)-(a.end-a.start))[0];
    return run?{x:rect.left+(group.x+.5)*scaleX,y:rect.top+(Math.floor((run.start+run.end)/2)+.5)*scaleY}:null;
   }).filter(Boolean);
   if(points.length<21) throw new Error('unreadable_candles');
   const gaps=points.slice(1).map((p,i)=>p.x-points[i].x), ordered=[...gaps].sort((a,b)=>a-b), spacing=ordered[Math.floor(ordered.length/2)];
   // A missing zero-height or unreadable category must not be replaced by an
   // older day. Fail closed when the last 21 plotted positions contain a gap.
   if(!spacing||gaps.slice(-20).some(gap=>gap<spacing*.65||gap>spacing*1.5)) throw new Error('unreadable_candles');
   const rows=[],xs=[];
   move(rect.right,rect.top,'mouseout'); await pause(120);
   if(tooltip()) throw new Error('stale_tooltip');
   for(const point of points.slice(-limit).reverse()){
    if(!canvas.isConnected||chart.querySelector('canvas[data-zr-dom-id]')!==canvas||canvas.width!==width||canvas.height!==height) throw new Error('changed_data');
    if(++reads>1800) throw new Error('read_budget_exceeded');
    // Repeated mouseout schedules a delayed hide in the real source. Keep
    // moving through bars and wait for a genuinely older, stable tooltip.
    move(point.x,point.y); let candidate=null, accepted=null;
    for(let attempt=0;attempt<8;attempt++){
     await pause(40);
     if(!canvas.isConnected||chart.querySelector('canvas[data-zr-dom-id]')!==canvas||canvas.width!==width||canvas.height!==height) throw new Error('changed_data');
     const row=tooltip(); if(!row) continue;
     if(values.has(row.date)&&values.get(row.date)!==row.inflowMusd) throw new Error('changed_data');
     const previous=rows[rows.length-1];
     if(previous&&row.date>previous.date) throw new Error('stale_tooltip');
     if(previous&&row.date===previous.date){candidate=null;continue;}
     if(candidate){
      if(row.date!==candidate.date) throw new Error('stale_tooltip');
      if(row.inflowMusd!==candidate.inflowMusd) throw new Error('changed_data');
      accepted=row;break;
     }
     candidate=row;
    }
    if(!accepted) throw new Error('unreadable_candles');
    values.set(accepted.date,accepted.inflowMusd); rows.push(accepted); xs.push(point.x);
   }
   return {rows,xs};
  };
  const initial=await sweep(21);
  if(initial.rows.length!==21||initial.rows[0].inflowMusd!==header) return finish({ok:false,reason:'incomplete_values'});
  const candles=[...initial.rows].reverse();
  // Pixels cannot prove how many zero-height categories a screenshot contains.
  // Return the verified dates and amounts for the compact chart/text renderer.
  move(rect.right,rect.top,'mouseout'); await pause(120);
  return finish({ok:true,series_name:'Daily Total Net Inflow',unit:'USD_MILLIONS',candles,last_date:candles[20].date,last_amount_musd:header});
 } catch(error) { return finish({ok:false,reason:['read_budget_exceeded','stale_tooltip','changed_data','pointer_precision_unavailable','range_unavailable','range_mismatch','pixels_unavailable','unreadable_candles'].includes(error?.message)?error.message:'tooltip_unavailable'}); }
})()`;

function validDate(raw: unknown): raw is string {
  return (
    typeof raw === "string" &&
    /^\d{4}-\d{2}-\d{2}$/.test(raw) &&
    Number.isFinite(Date.parse(raw)) &&
    new Date(raw).toISOString().slice(0, 10) === raw
  );
}

export function validatedEtfCandles(raw: unknown): EtfCandle[] | null {
  if (!Array.isArray(raw) || raw.length < 2 || raw.length > 21) return null;
  const rows: EtfCandle[] = [];
  for (const row of raw) {
    if (
      !row ||
      !validDate(row.date) ||
      typeof row.inflowMusd !== "number" ||
      !Number.isFinite(row.inflowMusd) ||
      (rows.length && row.date <= rows[rows.length - 1].date)
    )
      return null;
    rows.push({ date: row.date, inflowMusd: row.inflowMusd });
  }
  return rows;
}

/** Preserve the existing reader's failure, without exposing page/provider text. */
export function etfTooltipFailureMessage(returns: unknown[]): string {
  const messages: Record<string, string> = {
    range_unavailable: "בורר טווח התאריכים במקור לא היה זמין.",
    range_mismatch: "טווח התאריכים שנבחר במקור לא תאם לטווח המבוקש.",
    unreadable_candles: "לא ניתן היה לקרוא 21 נרות רצופים ומאומתים.",
    incomplete_values: "מספר הערכים או הערך האחרון לא תאמו לנתוני המקור.",
    changed_data: "נתוני הגרף השתנו במהלך הקריאה.",
    stale_tooltip: "חלונית הנתונים לא התעדכנה לנר שנבחר.",
    read_budget_exceeded: "קריאת נתוני הגרף חרגה מהזמן שהוקצה לה.",
    chart_too_narrow: "הגרף במקור היה צר מדי להפרדה בין הנרות.",
    wrong_source: "לא אומת עמוד נתוני Bitcoin ETF המבוקש.",
    ambiguous_chart: "לא זוהה גרף יחיד וברור במקור.",
    chart_unavailable: "הגרף במקור לא נטען לקריאה.",
    not_daily: "הגרף במקור לא הציג נתונים יומיים.",
    inflow_unavailable: "לא זוהתה סדרת זרימת הכספים היומית.",
    unknown_units: "יחידות הסכומים במקור לא אומתו.",
    pixels_unavailable: "לא ניתן היה לקרוא את תמונת הגרף.",
    pointer_precision_unavailable: "הדפדפן לא תמך בבחירת נר בדיוק הנדרש.",
    tooltip_unavailable: "חלוניות נתוני הנרות לא היו זמינות לקריאה.",
  };
  for (const raw of returns.slice(-8)) {
    const value = raw && typeof raw === "object" && "value" in raw ? raw.value : raw;
    let entry;
    try {
      entry = typeof value === "string" ? JSON.parse(value) : value;
    } catch {
      continue;
    }
    if (
      entry?.stage === "etf_tooltips" &&
      entry.ok === false &&
      typeof entry.reason === "string" &&
      Object.hasOwn(messages, entry.reason)
    )
      return messages[entry.reason];
  }
  return "לא התקבלה סיבת כשל מאומתת מקורא הגרף.";
}

export function verifiedEtfTooltips(returns: unknown[]): {
  snapshot: VerifiedEtfSnapshot;
  image?: string;
  visibleCandles?: 21;
} | null {
  const entries = returns
    .map((raw) => {
      const value = raw && typeof raw === "object" && "value" in raw ? raw.value : raw;
      try {
        return typeof value === "string" ? JSON.parse(value) : value;
      } catch {
        return null;
      }
    })
    .filter((raw) => raw?.stage === "etf_tooltips");
  if (entries.length !== 1) return null;
  const raw = entries[0],
    candles = validatedEtfCandles(raw.candles);
  if (
    raw.ok !== true ||
    raw.series_name !== "Daily Total Net Inflow" ||
    raw.unit !== "USD_MILLIONS" ||
    !candles ||
    candles.length !== 21 ||
    raw.last_date !== candles[20].date ||
    raw.last_amount_musd !== candles[20].inflowMusd
  )
    return null;
  const image =
    raw.visible_candles === 21 &&
    typeof raw.image === "string" &&
    /^data:image\/png;base64,[A-Za-z0-9+/]+={0,2}$/.test(raw.image)
      ? raw.image
      : undefined;
  return {
    snapshot: { date: raw.last_date, inflowMusd: raw.last_amount_musd, candles },
    image,
    ...(image ? { visibleCandles: 21 as const } : {}),
  };
}

export function etfEvidence(snapshot: VerifiedEtfSnapshot): string {
  return `Daily total net inflow ${snapshot.date}: ${snapshot.inflowMusd} million USD`;
}

export function compactMarketDollars(millions: number): string {
  const dollars = Math.abs(millions) * 1_000_000;
  const [divisor, unit] =
    dollars >= 1_000_000_000
      ? [1_000_000_000, "B"]
      : dollars >= 1_000_000
        ? [1_000_000, "M"]
        : dollars >= 1000
          ? [1000, "K"]
          : [1, ""];
  return `${millions < 0 ? "−" : millions > 0 ? "+" : ""}${new Intl.NumberFormat("en-US", { maximumFractionDigits: 2 }).format(dollars / divisor)}${unit}$`;
}
