/** Capture the source's own ratio and evidence components; never redraw them. */
const HELPERS = `
 const text = value => (value || '').replace(/\\s+/g,' ').trim();
 const fail = reason => JSON.stringify({stage,ok:false,reason});
 const identity = () => {
  const url = new URL(location.href);
  const params=url.searchParams;
  return url.protocol==='https:'&&url.hostname==='consensus.cryptoquant.com'&&!url.port&&!url.username&&!url.password&&url.pathname==='/consensus/btc'&&[...params].length===3&&params.get('period')==='7d'&&params.get('mode')==='absolute'&&params.get('window')==='7d'&&!url.hash&&/^Weekly Bitcoin Analyst Consensus \\(BTC\\)/.test(document.title);
 };
 const avgPattern=/^Absolute · (\\d+(?:\\.\\d+)?)% Bullish · (\\d+(?:\\.\\d+)?)% Bearish · 7-day avg as of (\\w+ \\d+)(?: ·|$)/i;
 const readColumns = section => {
  const badges=[...section.querySelectorAll('*')].filter(e=>/^(BEARISH|BULLISH) \\(\\d+\\)$/.test(text(e.innerText))&&e.children.length===0);
  if(badges.length!==2)return null;
  const result={};
  for(const badge of badges){
   const label=text(badge.innerText),match=label.match(/^(BEARISH|BULLISH) \\((\\d+)\\)$/);
   if(result[match[1]])return null;
   let col=badge;
   while(col.parentElement&&col.parentElement!==section&&!/cq-cols/.test(col.parentElement.className))col=col.parentElement;
   if(!col.parentElement||!col.parentElement.classList.contains('cq-cols'))return null;
   const more=[...col.querySelectorAll('button,a')].filter(e=>/^Load more/i.test(text(e.innerText))&&e.offsetParent);
   const items=[...col.querySelectorAll('button,li,[role=button]')].filter(e=>e.offsetParent&&text(e.innerText).length>15&&!/^Load more/i.test(text(e.innerText)));
   const titles=items.map(e=>text(e.innerText));
   if(more.length>1||+match[2]>30||titles.length>+match[2]||new Set(titles).size!==titles.length||titles.some(t=>t.length>1000))return null;
   result[match[1]]={count:+match[2],titles,more};
  }
  return result.BEARISH&&result.BULLISH&&result.BEARISH.count+result.BULLISH.count>0?result:null;
 };
 const columnData = section => {
  const columns=readColumns(section);
  return columns&&Object.values(columns).every(c=>!c.more.length&&c.titles.length===c.count)
   ?{BEARISH:columns.BEARISH.titles,BULLISH:columns.BULLISH.titles}:null;
 };
 const observe = (ratio,section,average,wrapper,capturedAt) => {
  if(!identity()||!ratio.isConnected||!section.isConnected||!average.isConnected||!wrapper.isConnected)return null;
  const avg=text(average.innerText).match(avgPattern);
  const lists=columnData(section);
  if(!avg||!lists||ratio.dataset.bandMode!=='absolute'||average.dataset.bandMode!=='absolute'||+avg[1]>100||+avg[2]>100||Math.abs(+avg[1]+ +avg[2]-100)>0.01)return null;
  const parts=avg[3].match(/^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) (\\d{1,2})$/);
  if(!parts)return null;
  const stamp=Date.parse(capturedAt),month=['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'].indexOf(parts[1]);
  let year=new Date(stamp).getUTCFullYear(),asOf=Date.UTC(year,month,+parts[2]);
  if(asOf>stamp+86400000)asOf=Date.UTC(--year,month,+parts[2]);
  if(new Date(asOf).getUTCMonth()!==month||new Date(asOf).getUTCDate()!==+parts[2]||asOf>stamp+86400000||stamp-asOf>4*86400000)return null;
  const bars=[...ratio.querySelectorAll('div')].filter(e=>{const r=e.getBoundingClientRect();return r.width>300&&r.height>=6&&r.height<=16});
  const a=ratio.getBoundingClientRect(),b=section.getBoundingClientRect(),c=average.getBoundingClientRect(),box=wrapper.getBoundingClientRect();
  const sourceWidth=innerWidth,sourceHeight=Math.max(innerHeight,Math.ceil(document.documentElement.scrollHeight),Math.ceil(document.body.scrollHeight));
  const cropX=Math.floor(box.left),cropY=Math.floor(box.top+scrollY),width=Math.ceil(box.right)-cropX,height=Math.ceil(box.bottom+scrollY)-cropY;
  if(devicePixelRatio!==1||bars.length<2||a.width<1000||a.height<50||b.width<1000||b.height<50||cropX<0||cropY<0||width<1000||height<200||cropX+width>sourceWidth||cropY+height>sourceHeight||sourceHeight>4000||sourceWidth>4096||a.left<box.left||b.left<box.left||a.right>box.right||b.right>box.right||a.top<box.top||b.top<a.bottom||b.bottom>box.bottom||c.left<box.left||c.right>box.right||c.top<box.top||c.bottom>box.bottom)return null;
  return {version:'unbias_weekly.v1',period:'7d',sourceUrl:'https://consensus.cryptoquant.com/consensus/btc?period=7d',capturedAt,asOf:avg[3],bullishPercent:+avg[1],bearishReasons:lists.BEARISH,bullishReasons:lists.BULLISH,sourceWidth,sourceHeight,cropX,cropY,width,height};
 };
`;
export const UNBIAS_WEEKLY_PREPARE_SCRIPT = `(async () => {
 const stage='unbias_weekly_prepare'; ${HELPERS}
 window.__collectionUnbiasWeekly=null;
 try {
  if(!identity())return fail('wrong_weekly_source');
  const ratios=[...document.querySelectorAll('section.narrative-shrink')];
  const sections=[...document.querySelectorAll('section.cq-section')].filter(s=>/^CONSENSUS VIEW/.test(text(s.innerText)));
  if(ratios.length!==1||sections.length!==1||ratios[0].parentElement!==sections[0].parentElement)return fail('ambiguous_source_components');
  const ratio=ratios[0],section=sections[0];
  // Expand only the source's consensus columns before moving React-owned nodes.
  const deadline=Date.now()+3000;
  while(!columnData(section)){
   const columns=readColumns(section);
   if(!columns)return fail('incomplete_ratio_or_reasons');
   const pending=Object.entries(columns).filter(([,c])=>c.more.length&&c.titles.length<c.count);
   if(!pending.length||Date.now()>=deadline)return fail('incomplete_ratio_or_reasons');
   for(const [,c] of pending)c.more[0].click();
   let changed=false;
   while(!changed&&Date.now()<deadline){
    await new Promise(resolve=>setTimeout(resolve,100));
    const next=readColumns(section);
    if(!next)return fail('incomplete_ratio_or_reasons');
    changed=pending.every(([name,c])=>next[name].titles.length>c.titles.length);
   }
   if(!changed)return fail('incomplete_ratio_or_reasons');
  }
  const averages=[...document.querySelectorAll('p,div,span')].filter(e=>{const r=e.getBoundingClientRect();return r.width>0&&r.height>0&&avgPattern.test(text(e.innerText))&&![...e.children].some(child=>avgPattern.test(text(child.innerText)));});
  if(averages.length!==1)return fail('weekly_average_unavailable');
  const average=averages[0];
  const toolbar=ratio.querySelector('div.justify-end');if(toolbar)toolbar.style.display='none';
  const wrapper=document.createElement('div');wrapper.id='cq-source-compact';wrapper.className=ratio.parentElement.className;
  wrapper.style.cssText='width:1246px;margin:0;padding:24px 24px 16px;box-sizing:content-box;background:#fff';
  document.body.prepend(wrapper);wrapper.append(ratio);if(!ratio.contains(average))wrapper.append(average);wrapper.append(section);
  for(const node of [ratio,average,section]){node.style.position='relative';node.style.top='auto';node.style.transform='none';node.style.marginTop='0';node.style.height='auto';node.style.minHeight='0';node.style.overflow='visible';}
  section.style.marginTop='12px';
  for(const node of [...document.body.children])if(node!==wrapper&&node.tagName!=='SCRIPT')node.style.display='none';
  document.documentElement.style.background='#fff';document.body.style.cssText+=';margin:0;padding:0;background:#fff;min-height:0';window.scrollTo(0,0);
  const proof=observe(ratio,section,average,wrapper,new Date().toISOString());
  if(!proof)return fail('incomplete_ratio_or_reasons');
  window.__collectionUnbiasWeekly={ratio,section,average,wrapper,proof};
  return JSON.stringify({stage,ok:true,proof});
 }catch{return fail('weekly_capture_unavailable');}
})()`;
export const UNBIAS_WEEKLY_VERIFY_SCRIPT = `(() => {
 const stage='unbias_weekly_verify'; ${HELPERS}
 try {
  const saved=window.__collectionUnbiasWeekly;
  if(!saved)return fail('weekly_prepare_missing');
  const proof=observe(saved.ratio,saved.section,saved.average,saved.wrapper,saved.proof.capturedAt);
  if(!proof||JSON.stringify(proof)!==JSON.stringify(saved.proof))return fail('weekly_source_changed');
  return JSON.stringify({stage,ok:true,proof});
 }catch{return fail('weekly_verification_unavailable');}
})()`;
