import {
  ECONOMIC_CALENDAR_SOURCE,
  economicCalendarWindow,
  isEconomicCalendarSource,
  parseEconomicCalendarCapture,
  type EconomicCalendarSnapshot,
} from "./collectionEconomicCalendar";

const sessionIdPattern = /^[a-zA-Z0-9_-]{8,128}$/;
const object = (value: unknown): Record<string, unknown> | null =>
  value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;

export function economicCalendarBrowserScript(startedAt: string): string {
  const bounds = economicCalendarWindow(startedAt);
  const previous = new Date(Date.parse(`${bounds.startDate}T12:00:00Z`) - 86_400_000)
    .toISOString()
    .slice(0, 10);
  const last = new Date(Date.parse(`${bounds.endDate}T12:00:00Z`) - 86_400_000)
    .toISOString()
    .slice(0, 10);
  const uiDate = (date: string) => `${date.slice(5, 7)}/${date.slice(8, 10)}/${date.slice(0, 4)}`;
  return `
const out={version:1,sourceUrl:${JSON.stringify(ECONOMIC_CALENDAR_SOURCE)},responses:[],rangeSelection:'not-found',continuation:'limit'};
const pending=[];const endpoint='/pd-instruments/v1/calendars/economic/events/occurrences';
let requestedCursor=null;const deadline=Date.now()+140000;
const fields=['event_id','short_name','long_name','importance','currency','country_id','page_link','description'];
const occurrenceFields=['event_id','occurrence_id','occurrence_time'];
const pick=(value,keys)=>Object.fromEntries(keys.filter(key=>key in value).map(key=>[key,value[key]]));
const listener=response=>{let url;try{url=new URL(response.url())}catch{return}
 if(url.hostname!=='endpoints.investing.com'||url.pathname!==endpoint||pending.length>=16)return;
 const cursor=requestedCursor;
 pending.push((async()=>{const row={host:url.hostname,path:url.pathname,status:response.status(),query:{
  domain_id:url.searchParams.get('domain_id'),limit:url.searchParams.get('limit'),
  start_date:url.searchParams.get('start_date'),end_date:url.searchParams.get('end_date'),cursor}};
  if(cursor&&!Array.from(url.searchParams.values()).includes(cursor)){row.invalid=true;out.responses.push(row);return}
  try{const body=await response.json();if(!Array.isArray(body.events)||!Array.isArray(body.occurrences)||body.events.length>1000||body.occurrences.length>1000)throw Error('schema');
   row.body={events:body.events.map(value=>pick(value,fields)),occurrences:body.occurrences.map(value=>pick(value,occurrenceFields)),next_page_cursor:body.next_page_cursor};
  }catch{row.invalid=true}out.responses.push(row)})());};
page.on('response',listener);
const pause=ms=>new Promise(resolve=>setTimeout(resolve,ms));
const visible=async locator=>{const all=[];for(let i=0;i<await locator.count();i++)if(await locator.nth(i).isVisible())all.push(locator.nth(i));return all};
const wait=async(before,ms)=>{const start=Date.now();while(Date.now()-start<ms){await Promise.allSettled(pending);if(out.responses.slice(before).some(value=>value.status===200&&value.body))return true;await pause(250)}return false};
try{
 await page.goto(${JSON.stringify(ECONOMIC_CALENDAR_SOURCE)},{waitUntil:'domcontentloaded',timeout:40000});
 const loaded=new URL(page.url());if(loaded.protocol!=='https:'||loaded.hostname!=='www.investing.com'||!/^\\/economic-calendar\\/?$/.test(loaded.pathname))throw Error('source');
 await wait(0,10000);
 // Include the previous UI date even if today's query claims to span midnight:
 // its first page may stop before then. Filter the exact Israel bounds later.
 const custom=await visible(page.getByText('Custom dates',{exact:true}));
 if(custom.length===1){
  await custom[0].click();out.rangeSelection='opened';const start=page.locator('#date-picker-start-day'),end=page.locator('#date-picker-end-day');
  if(await start.count()===1&&await end.count()===1&&await start.getAttribute('placeholder')==='MM/DD/YYYY'&&await end.getAttribute('placeholder')==='MM/DD/YYYY'){
   const from=${JSON.stringify(uiDate(previous))};
   const to=${JSON.stringify(uiDate(last))};await start.fill('');await start.pressSequentially(from,{delay:25});
   await end.fill('');await end.pressSequentially(to,{delay:25});await end.blur();
   const apply=await visible(page.getByRole('button',{name:/^apply$/i}));
   if(apply.length===1&&await start.inputValue()===from&&await end.inputValue()===to){const before=out.responses.length;await apply[0].click();out.rangeSelection=await wait(before,20000)?'loaded':'no-response'}
  }
 }
 await Promise.allSettled(pending);
 // Follow the public UI's own pagination, verifying the exact cursor it sends.
 // One click per new page; failed or non-advancing pages are never retried.
 const seen=new Set();
 while(out.responses.length<16&&Date.now()<deadline){
  const last=out.responses.at(-1);const cursor=last?.body?.next_page_cursor;
  if(last?.status!==200||!cursor||typeof cursor!=='string'||seen.has(cursor)){out.continuation='end';break}
  const more=page.getByRole('button',{name:'Load more',exact:true});
  out.continuation='control';
  try{await more.waitFor({state:'visible',timeout:Math.min(15000,deadline-Date.now())});
   if(await more.count()!==1){out.continuation='ambiguous';break}
   seen.add(cursor);requestedCursor=cursor;const before=out.responses.length;
   // A response can arrive before React enables the next-page button.
   // Let click's actionability wait finish; do not mistake rendering for EOF.
   out.continuation='click';await more.click({timeout:Math.min(15000,deadline-Date.now())});
   out.continuation='response';if(!await wait(before,Math.min(15000,deadline-Date.now())))break;
   out.continuation='limit';
  }catch{break}
 }
 await Promise.allSettled(pending);
 out.capturedAt=new Date().toISOString();
}finally{page.off('response',listener)}
console.log(JSON.stringify(out));
`;
}

async function browserRequest(
  method: "POST" | "DELETE",
  path: string,
  body?: Record<string, unknown>,
  timeout = 45_000,
): Promise<Record<string, unknown>> {
  const key = process.env.FIRECRAWL_API_KEY;
  const direct = key?.startsWith("fc-") === true;
  const gateway = process.env.LOVABLE_API_KEY;
  if (!key || (!direct && !gateway)) throw new Error("חיבור ספק היומן הכלכלי אינו מוגדר בשרת.");
  const base = direct
    ? "https://api.firecrawl.dev"
    : "https://connector-gateway.lovable.dev/firecrawl";
  let response: Response;
  try {
    response = await fetch(`${base}/v2/interact${path}`, {
      method,
      redirect: "manual",
      signal: AbortSignal.timeout(timeout),
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${direct ? key : gateway}`,
        ...(!direct ? { "X-Connection-Api-Key": key } : {}),
      },
      ...(body ? { body: JSON.stringify(body) } : {}),
    });
  } catch {
    throw new Error("שירות קריאת היומן הכלכלי לא הגיב בזמן; היומן הקיים נשמר.");
  }
  if (method === "DELETE" && [404, 410].includes(response.status)) {
    await response.body?.cancel();
    return { success: true };
  }
  if (!response.ok) {
    await response.body?.cancel();
    throw new Error(`קריאת היומן הכלכלי נכשלה (HTTP ${response.status}); היומן הקיים נשמר.`);
  }
  const reader = response.body?.getReader();
  if (!reader) throw new Error("תשובת ספק היומן ריקה; היומן הקיים נשמר.");
  let bytes = 0,
    text = "";
  const decoder = new TextDecoder();
  try {
    for (;;) {
      const chunk = await reader.read();
      if (chunk.done) break;
      bytes += chunk.value.byteLength;
      if (bytes > 8 * 1024 * 1024) throw new Error("תשובת היומן גדולה מדי; היומן הקיים נשמר.");
      text += decoder.decode(chunk.value, { stream: true });
    }
    text += decoder.decode();
    let result: Record<string, unknown> | null;
    try {
      result = object(JSON.parse(text));
    } catch {
      result = null;
    }
    if (!result) throw new Error("תשובת ספק היומן אינה תקינה; היומן הקיים נשמר.");
    return result;
  } finally {
    await reader.cancel().catch(() => undefined);
    reader.releaseLock();
  }
}

export function economicCalendarBrowserResult(body: Record<string, unknown>): unknown {
  if (
    body.success === false ||
    body.killed === true ||
    (body.exitCode !== undefined && body.exitCode !== 0) ||
    (typeof body.error === "string" && body.error.trim()) ||
    (typeof body.stderr === "string" && body.stderr.trim())
  )
    throw new Error("דפדפן היומן לא השלים קריאה תקינה; היומן הקיים נשמר.");
  const outputs: Record<string, unknown>[] = [];
  for (const value of [body.stdout, body.result, body.output]) {
    if (
      value === undefined ||
      value === null ||
      (typeof value === "string" && ["", "undefined", "null"].includes(value.trim()))
    )
      continue;
    let parsed: unknown;
    try {
      parsed = typeof value === "string" ? JSON.parse(value.trim()) : value;
    } catch {
      throw new Error("נתוני היומן שחזרו מהדפדפן אינם תקינים; היומן הקיים נשמר.");
    }
    const candidate = object(parsed);
    if (!candidate || candidate.version !== 1 || !Array.isArray(candidate.responses))
      throw new Error("מבנה נתוני דפדפן היומן לא זוהה; היומן הקיים נשמר.");
    outputs.push(candidate);
  }
  if (
    !outputs.length ||
    outputs.some((value) => JSON.stringify(value) !== JSON.stringify(outputs[0]))
  )
    throw new Error("דפדפן היומן לא החזיר נתונים חד משמעיים; היומן הקיים נשמר.");
  return outputs[0];
}

export async function readEconomicCalendarSnapshot(
  sourceUrl: string,
  startedAt: string,
): Promise<EconomicCalendarSnapshot> {
  if (!isEconomicCalendarSource(sourceUrl)) throw new Error("מקור היומן הכלכלי אינו נתמך.");
  const code = economicCalendarBrowserScript(startedAt);
  const session = await browserRequest("POST", "", {
    ttl: 300,
    activityTtl: 300,
    streamWebView: false,
    blockAds: true,
  });
  const id =
    typeof session.id === "string" && sessionIdPattern.test(session.id) ? session.id : null;
  if (!id) throw new Error("לא נפתח דפדפן תקין לקריאת היומן; היומן הקיים נשמר.");
  try {
    if (session.success === false)
      throw new Error("ספק היומן לא פתח דפדפן תקין; היומן הקיים נשמר.");
    const result = await browserRequest(
      "POST",
      `/${id}/execute`,
      { language: "node", code, timeout: 150 },
      165_000,
    );
    const capture = economicCalendarBrowserResult(result);
    const snapshot = parseEconomicCalendarCapture(capture, startedAt);
    if (snapshot.partial) {
      const details = object(capture);
      const reason =
        {
          control: "כפתור טעינת ההמשך לא הופיע",
          ambiguous: "כפתור טעינת ההמשך לא זוהה באופן חד משמעי",
          click: "כפתור טעינת ההמשך לא נעשה זמין ללחיצה",
          response: "המקור לא החזיר את מנת ההמשך",
          end: "המקור סיים את המנות לפני כיסוי הטווח המבוקש",
          limit: "טעינת ההמשך הגיעה למגבלת הזמן או המנות",
        }[String(details?.continuation)] ?? "לא אומתה השלמת כל מנות המקור";
      const pages = Array.isArray(details?.responses) ? details.responses.length : 0;
      throw new Error(`סריקה חלקית: ${reason} (${pages} מנות); היומן הקיים נשמר.`);
    }
    return snapshot;
  } finally {
    await browserRequest("DELETE", `/${id}`).catch(() =>
      console.warn("economic_calendar_browser_cleanup_failed"),
    );
  }
}
