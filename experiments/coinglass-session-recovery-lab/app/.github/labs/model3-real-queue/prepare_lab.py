"""Adapt the reviewed isolated application lab to real PostgreSQL and HTTP.

The base lab is hash pinned. Product code is never edited. All changed boundaries
are explicit below: DB transport, local collector transport, test clock and wait.
"""
import hashlib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
original = ROOT / '.github/labs/coinglass-cadence.mjs'
assert hashlib.sha256(original.read_bytes()).hexdigest() == '00a0e60c4f3fd38e1d0ef6d38bdb7c6f9c52739f83e154ce496117c6904a2010'
text = original.read_text()
text = text.replace('readFileSync, readdirSync, mkdtempSync, rmSync', 'readFileSync, readdirSync, mkdtempSync, rmSync, writeFileSync')
start, end = text.index('if (!process.env.PGLITE_ROOT)'), text.index('const loadSql')
text = text[:start] + '''const { default: pgModule } = await import(pathToFileURL(path.join(process.env.QUEUE_LAB_NODE, "node_modules/pg/lib/index.js")));
const clientPg = new pgModule.Client({connectionString: process.env.QUEUE_LAB_APP_DSN});
await clientPg.connect();
const pg = {query: (...args) => clientPg.query(...args), exec: (s) => clientPg.query(s), close: () => clientPg.end()};
const dataDir = mkdtempSync(path.join(os.tmpdir(), "queue-lab-empty-"));
async function control(failedMap = null) {
  const response = await fetch("http://127.0.0.1:21879/lab/control", {method:"POST", headers:{"content-type":"application/json"}, body:JSON.stringify({now:new Date(clock).toISOString(), failedMap})});
  assert.equal(response.status, 200);
}
''' + text[end:]
text = text.replace('clock = Date.parse(value);', 'clock = Date.parse(value);\n  await control();')
start, end = text.index('  const jobs = new Map(),'), text.index('  const notices = [];')
text = text[:start] + '''  const jobs = new Map();
  let api;
  let dedupChecked = false;
  const seenTerminal = new Set();
  const provider = async (raw, init = {}) => {
    const url = new URL(raw);
    assert.equal(url.origin, "https://collector.example.com");
    const target = "http://127.0.0.1:21879" + url.pathname + url.search;
    const response = await fetch(target, init);
    const body = await response.clone().json();
    if (url.pathname === "/health") return response;
    const model = Number(url.pathname.match(/model([123])/)[1]);
    if (init.method === "POST") {
      counters.post.push({model, ...JSON.parse(init.body)});
      assert.ok(response.ok, JSON.stringify(body));
      if (!dedupChecked) {
        const same = await fetch(target, init);
        assert.equal((await same.json()).job_id, body.job_id, "Same request is idempotent in real queue");
        const freshRequest = {...JSON.parse(init.body), request_id:crypto.randomUUID()};
        const active = await fetch(target, {...init, body:JSON.stringify(freshRequest)});
        assert.equal((await active.json()).job_id, body.job_id, "Active model/timeframe reuses real job");
        dedupChecked = true;
      }
    } else counters.reads.push(body.job_id);
    if (body.job_id) jobs.set(body.job_id, {id:body.job_id, model, timeframe:body.timeframe, status:body.status});
    if (["ready", "failed"].includes(body.status) && !seenTerminal.has(body.job_id)) {
      seenTerminal.add(body.job_id);
      counters.source.push({model, timeframe:body.timeframe, id:body.job_id});
      if (body.status === "ready") counters.ai.push(body.job_id);
      console.log(JSON.stringify({stage:"real-collector-job", model, timeframe:body.timeframe, id:body.job_id, status:body.status, error:body.error}));
    }
    return response;
  };
''' + text[end:]
text = text.replace('wait: async () => {},', 'wait: async () => new Promise(r => setTimeout(r, 500)),')
text = text.replace('setTimeout(fn, Math.min(ms, 1), ...args)', 'setTimeout(fn, Math.min(ms, 500), ...args)')
text = text.replace('failedMap = "2:24H";', 'await control("3:24H");')
text = text.replace('m2 = fx.maps[1]', 'm2 = fx.maps[2]')
text = text.replace('failedMap = null;', 'await control();')
text = text.replace('await new Promise((r) => setTimeout(r, 600));', '''await new Promise((r) => setTimeout(r, 500));
  });
  for (let n=0; n<360 && !notices.some(x => ["success","error"].includes(x.kind)); n++) {
    await React.act(async () => {await new Promise(r => setTimeout(r,500));});
  }
  await React.act(async () => {
    await new Promise(r => setTimeout(r,100));''')
text = text.replace('  await React.act(async () => mount.unmount());', '''  writeFileSync(path.join(process.env.QUEUE_LAB_EVIDENCE, "rendered.html"), document.documentElement.outerHTML);
  writeFileSync(path.join(process.env.QUEUE_LAB_EVIDENCE, "saved-and-rendered.json"), JSON.stringify({afterRetry, uiRows, notices}, null, 2));
  await React.act(async () => mount.unmount());''')
text = text.replace('syntheticSourceObservations:', 'realScannerChildren:').replace('syntheticAiResults:', 'validatedFixtureAiResults:')
text = text.replace('      scheduledCycles: 2,', '      transport: "real HTTP collector + native PostgreSQL + actual worker/subprocess",\n      dedupChecked,\n      scheduledCycles: 2,')
assert 'savedResult(' not in text and 'PGlite.create' not in text and 'job.status =' not in text
(ROOT / '.github/labs/model3-real-queue.generated.mjs').write_text(text)
