import {
  instagramBootstrapReelPayloads,
  instagramMediaResourceKey,
  instagramReelInventory,
  instagramStoryAccount,
} from "./collectionInstagramStories";

export const INSTAGRAM_CAPTURE_FILE = "/tmp/collection-instagram-capture.json";
export const INSTAGRAM_CONNECTION_MARKER = "collection-instagram-connection.v1";
// Instagram's navigation changes independently of its authenticated session.
// Keep the same check for the handoff and every later scan, including host redirects.
const profileStateScript = String.raw`({account}) => {
 const url = new URL(location.href);
 const exact = url.protocol === 'https:' && !url.port && !url.username && !url.password &&
  ['instagram.com','www.instagram.com'].includes(url.hostname) &&
  url.pathname.replace(/\/$/,'').toLowerCase() === '/' + account;
 const login = !!document.querySelector('input[name="username"],input[name="password"]') ||
  /\/(accounts\/(login|suspended)|challenge|checkpoint)(\/|$)/.test(url.pathname);
 const signedIn = exact && Array.from(document.querySelectorAll('a[href]')).some(link => {
  if (!link.getClientRects().length) return false;
  let href; try { href = new URL(link.getAttribute('href'),url); } catch { return false; }
  return href.protocol === 'https:' && !href.port && !href.username && !href.password &&
   ['instagram.com','www.instagram.com'].includes(href.hostname) &&
   (/^\/direct\//.test(href.pathname) || /^\/accounts\/(edit|onetap)\/?$/.test(href.pathname));
 });
 return {exact,login,signedIn};
}`;
const profileSessionScript = String.raw`const verifyProfile = async (activation = null) => {
 // A profile can hydrate long after DOMContentLoaded. Stop promptly on a login page.
 await page.waitForFunction(({account}) => {
  const state = (${profileStateScript})({account});
  return !state.exact || state.login || state.signedIn;
 },{account},{timeout:6000}).catch(() => {});
 return await page.evaluate(async ({account,marker,activation}) => {
  const state = (${profileStateScript})({account});
  let authenticated = state.exact && state.signedIn && !state.login;
  if (state.exact && !state.login && !authenticated) {
   // Some Instagram layouts use buttons for navigation. Ask Instagram in this
   // same browser, without reading or exporting any cookie or viewer identity.
   const controller = new AbortController();
   const timeout = setTimeout(() => controller.abort(),4000);
   try {
    const response = await fetch('/api/v1/accounts/current_user/?edit=true',{
     credentials:'same-origin',redirect:'error',signal:controller.signal,
     headers:{'X-IG-App-ID':'936619743392459','X-Requested-With':'XMLHttpRequest'}
    });
    if (response.ok && /application\/json/i.test(response.headers.get('content-type') || '')) {
     const text = await response.text();
     if (text.length <= 262144) {
      const body = JSON.parse(text);
      authenticated = body?.status === 'ok' && /^[1-9]\d{0,24}$/.test(String(body.user?.pk ?? '')) &&
       typeof body.user?.username === 'string' && /^[a-zA-Z0-9._]{1,30}$/.test(body.user.username);
     }
    }
   } catch {} finally { clearTimeout(timeout); }
  }
  // Recheck location/login after the probe; a challenge must never save a connection.
  const final = (${profileStateScript})({account});
  authenticated = authenticated && final.exact && !final.login;
  if (activation && authenticated) localStorage.setItem(marker,activation);
  return {account,authenticated,activation:authenticated ? localStorage.getItem(marker) : null};
 },{account,marker:${JSON.stringify(INSTAGRAM_CONNECTION_MARKER)},activation});
};`;
const safeAccount = (account: string) => {
  if (instagramStoryAccount(`https://www.instagram.com/${account}/`) !== account)
    throw new Error("מקור אינסטגרם אינו תקין.");
  return JSON.stringify(account);
};

export function instagramLoginScript(account: string, activation?: string): string {
  return String.raw`const account = ${safeAccount(account)};
${profileSessionScript}
await page.goto('https://www.instagram.com/' + account + '/', {waitUntil:'domcontentloaded',timeout:25000});
const result = await verifyProfile(${JSON.stringify(activation ?? null)});
console.log(JSON.stringify(result));`;
}

export function instagramCaptureScript(account: string, activation: string): string {
  return String.raw`const account = ${safeAccount(account)};
${profileSessionScript}
const inventory = ${instagramReelInventory.toString()};
const bootstrapPayloads = ${instagramBootstrapReelPayloads.toString()};
const mediaResourceKey = ${instagramMediaResourceKey.toString()};
const startedAt = Date.now();
const fail = reason => { throw new Error('IG_STORIES:' + reason); };
let latest = null, revision = 0, phase = 0;
const requestPhases = new WeakMap();
const pending = new Set();
const observeRequest = request => { requestPhases.set(request,phase); };
const observe = response => {
 const task = (async () => {
  const responsePhase = requestPhases.get(response.request());
  if (responsePhase !== phase) return;
  const url = new URL(response.url());
  if (url.protocol !== 'https:' || !['instagram.com','www.instagram.com','i.instagram.com'].includes(url.hostname) ||
      !/\/feed\/reels_media\//.test(url.pathname) || response.status() !== 200) return;
  const text = await response.text();
  if (responsePhase !== phase) return;
  if (text.length > 2000000) return;
  let json; try { json = JSON.parse(text); } catch { return; }
  const entries = inventory(json,account,Date.now());
  if (entries) { latest = entries; revision++; }
 })().catch(() => {});
 pending.add(task); task.finally(() => pending.delete(task));
};
page.on('request',observeRequest);
page.on('response',observe);
try {
 const readBootstrap = async () => {
  const currentPhase = phase;
  const before = new URL(page.url());
  const base = '/stories/' + account + '/';
  if (before.protocol !== 'https:' || before.port || before.username || before.password ||
      !['instagram.com','www.instagram.com'].includes(before.hostname) || !before.pathname.startsWith(base)) fail('slide_identity');
  const scripts = await page.evaluate(() => {
   const elements = Array.from(document.querySelectorAll('script[type="application/json"]'));
   if (elements.length > 256) return null;
   const texts = elements.map(element => element.textContent || '').filter(text => !!text.trim());
   if (texts.reduce((sum,text) => sum + text.length,0) > 2000000) return null;
   return texts;
  });
  const after = new URL(page.url());
  // Instagram may remove a navigation query while this same story document hydrates.
  if (currentPhase !== phase || after.protocol !== 'https:' || after.port || after.username || after.password ||
      !['instagram.com','www.instagram.com'].includes(after.hostname) || after.origin !== before.origin ||
      after.pathname !== before.pathname || after.hash !== before.hash) fail('slide_identity');
  if (!Array.isArray(scripts)) fail('inventory_unverified');
  let verified = null, matches = 0;
  for (const text of scripts) {
   let json;
   try { json = JSON.parse(text); }
   catch {
    if (text.includes('adp_PolarisStoriesV3ReelPageStandaloneQueryRelayPreloader_')) fail('inventory_unverified');
    continue;
   }
   const payloads = bootstrapPayloads(json);
   if (!payloads) fail('inventory_unverified');
   for (const payload of payloads) {
    if (++matches > 1) fail('inventory_unverified');
    const entries = inventory(payload,account,Date.now());
    if (!entries) fail('inventory_unverified');
    verified = entries;
   }
  }
  if (verified) { latest = verified; revision++; }
  return verified;
 };
 const openStoryGate = async (storyId = null) => {
  const gate = page.getByRole('button',{name:'View story',exact:true});
  const count = await gate.count();
  if (!count) return false;
  if (count !== 1) fail('inventory_unverified');
  const url = new URL(page.url());
  const base = '/stories/' + account + '/';
  const suffix = url.pathname.startsWith(base) ? url.pathname.slice(base.length) : null;
  if (url.protocol !== 'https:' || url.port || url.username || url.password ||
      !['instagram.com','www.instagram.com'].includes(url.hostname) || suffix === null ||
      (suffix !== '' && !/^[1-9]\d{5,24}\/$/.test(suffix)) ||
      (storyId && suffix !== storyId + '/')) fail('slide_identity');
  if (await page.locator('input[name="username"],input[name="password"]').count()) fail('login_required');
  await gate.click({timeout:10000});
  return true;
 };
 const exactPage = async storyId => {
  const url = new URL(page.url());
  if (url.protocol !== 'https:' || !['instagram.com','www.instagram.com'].includes(url.hostname) || url.port || url.username || url.password ||
      url.pathname !== '/stories/' + account + '/' + storyId + '/') fail('slide_identity');
  const login = await page.locator('input[name="username"],input[name="password"]').count();
  if (login) fail('login_required');
 };
 const openProfile = async () => {
  phase++;
  const before = revision;
  await page.goto('https://www.instagram.com/' + account + '/', {waitUntil:'domcontentloaded',timeout:25000});
  const auth = await verifyProfile();
  if (!auth.authenticated || auth.activation !== ${JSON.stringify(activation)}) fail('login_required');
  // The current profile ring can be a button, an absolute link, or load late.
  // Open Instagram's account-scoped viewer directly and still require its full
  // owner-bound inventory; a visible ring or a missing link proves no item count.
  await page.goto('https://www.instagram.com/stories/' + account + '/',{
   waitUntil:'domcontentloaded',timeout:20000
  });
  let openedGate = false;
  for (let i=0;i<40;i++) {
   const fromBootstrap = await readBootstrap();
   if (fromBootstrap) return {entries:fromBootstrap,marker:auth.activation};
   if (!openedGate) openedGate = await openStoryGate();
   await page.waitForTimeout(150); await Promise.all([...pending]);
   if (revision > before && latest) {
    if (await page.locator('input[name="username"],input[name="password"]').count()) fail('login_required');
    return {entries:latest,marker:auth.activation};
   }
  }
  fail('inventory_unverified');
 };
 const initial = await openProfile();
 const expected = initial.entries;
 if (expected.some(item => item.isVideo)) fail('unsupported_video');
 const signatureOf = entries => JSON.stringify(entries.map(({id,takenAt,isVideo}) => ({id,takenAt,isVideo})));
 const signature = signatureOf(expected);
 const slides = []; let bytes = 0;
 for (const item of expected) {
  if (Date.now() - startedAt >= 165000) fail('incomplete');
  const url = 'https://www.instagram.com/stories/' + account + '/' + item.id + '/';
  await page.goto(url,{waitUntil:'domcontentloaded',timeout:20000});
  await page.waitForTimeout(900);
  await readBootstrap();
  // Hold on the actual media while taking a crop; never include the login/navigation page.
  const image = page.locator('img[src]:visible');
  let box = null, openedGate = false;
  for (let i=0;i<40;i++) {
   if (Date.now() - startedAt >= 165000) fail('incomplete');
   if (!openedGate) openedGate = await openStoryGate(item.id);
   await page.waitForTimeout(150); await Promise.all([...pending]);
   await exactPage(item.id);
   if (signatureOf(latest) !== signature) fail('inventory_changed');
   const candidates = await image.evaluateAll(elements => elements.map(el => {
    const box = el.getBoundingClientRect();
    return {src:el.currentSrc || el.src,complete:el.complete,x:box.x,y:box.y,width:box.width,height:box.height,naturalWidth:el.naturalWidth,naturalHeight:el.naturalHeight};
   }).filter(box => box.complete && box.width >= 240 && box.height >= 320 && box.height / box.width >= 1.2 &&
    box.height / box.width <= 2.5 && box.naturalWidth >= 240 && box.naturalHeight >= 320));
   const trusted = latest.find(entry => entry.id === item.id);
   const resources = new Set(trusted.mediaUrls.map(mediaResourceKey));
   const matching = candidates.filter(box => {
    const key = mediaResourceKey(box.src);
    return key !== null && resources.has(key);
   });
   if (await page.locator('video:visible').count()) fail('slide_unreadable');
   if (matching.length === 1) { box = matching[0]; break; }
  }
  if (!box) fail('slide_unreadable');
  await page.mouse.move(box.x + box.width/2,box.y + box.height/2);
  await page.mouse.down();
  let png;
  try {
   await exactPage(item.id);
   png = await page.screenshot({type:'png',clip:{x:box.x,y:box.y,width:box.width,height:box.height}});
   const stillShown = await image.evaluateAll((elements,box) => elements.some(el => {
    const rect = el.getBoundingClientRect();
    return el.complete && (el.currentSrc || el.src) === box.src &&
     ['x','y','width','height'].every(key => Math.abs(rect[key]-box[key]) <= 1);
   }),box);
   if (!stillShown) fail('slide_identity');
  }
  finally { await page.mouse.up(); }
  await exactPage(item.id);
  bytes += png.length;
  if (png.length > 2097152 || bytes > 25165824) fail('incomplete');
  slides.push({id:item.id,url,publishedAt:new Date(item.takenAt*1000).toISOString(),isVideo:false,image:'data:image/png;base64,' + png.toString('base64')});
 }
 const final = await openProfile();
 if (signatureOf(final.entries) !== signature || final.marker !== initial.marker) fail('inventory_changed');
 const result = {version:'instagram-stories.v1',account,capturedAt:new Date().toISOString(),expectedIds:expected.map(item=>item.id),slides,activation:initial.marker};
 const payload = Buffer.from(JSON.stringify(result));
 await require('node:fs/promises').writeFile(${JSON.stringify(INSTAGRAM_CAPTURE_FILE)},payload,{mode:0o600});
 console.log(JSON.stringify({bytes:payload.length,sha256:require('node:crypto').createHash('sha256').update(payload).digest('hex')}));
} finally { page.off('response',observe); page.off('request',observeRequest); }`;
}
