"""Generate a window-only driver in a disposable clone of the exact Decision Hub candidate."""
import hashlib
import os
from pathlib import Path
import subprocess
import sys

APP_COMMIT = "aa96f7c4d1f81b7a4f9c4bece6b058ecf4699a4f"
VERIFY = "29b450dfde13795ce9cc3a2f2f7dfef37a956fb4"
RUN = "f65721fd3d2fd6bb083180e80caef9a3c016cf63"
HERE = Path(__file__).resolve().parent


def verified(path, blob):
    data = path.read_bytes()
    if hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest() != blob:
        raise ValueError("unexpected upstream fixture blob " + path.name)
    return data.decode()


def once(text, before, after):
    if text.count(before) != 1:
        raise ValueError("upstream fixture no longer matches a reviewed anchor")
    return text.replace(before, after)


HARDEN = """
import { constants, closeSync, fstatSync, lstatSync, openSync, writeFileSync } from "node:fs";
import { verifyHostedFixture } from "./target-visual-receipt.mjs";
const handoff = "/run/coinglass-window-handoff";
const own = process.getuid?.();
const dir = lstatSync(handoff);
assert.ok(own && dir.isDirectory() && dir.uid === own && (dir.mode & 0o777) === 0o700, "disposable private handoff required");
const signalDone = () => {
  try { writeFileSync(join(handoff, "done"), "done\\n", {flag:"wx",mode:0o600}); }
  catch (error: any) { if (error.code !== "EEXIST" && error.code !== "ENOENT") throw error; }
};
process.once("exit", signalDone);
const registration = openSync(join(handoff, "bridge"), constants.O_RDONLY | constants.O_NOFOLLOW);
let bridge: string;
try {
  const st = fstatSync(registration);
  assert.ok(st.isFile() && st.uid === own && (st.mode & 0o777) === 0o400 && st.size <= 160);
  bridge = readFileSync(registration, "utf8").trim();
  assert.match(bridge, /^[A-Za-z0-9_-]{40,128}$/);
} finally { closeSync(registration); }
const viewer = "https://collector.example.org";
"""

ARM = """
assert.match(actor,/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i);
writeFileSync(join(handoff,"actor"),actor+"\\n",{flag:"wx",mode:0o600});
let windowReady = false;
const until = Date.now() + 120_000;
while (Date.now() < until) {
  try {
    const response = await fetch(viewer+"/api/collection/coinglass/session-status", {
      headers: {Authorization: "Bearer "+bridge}, redirect:"error",signal:AbortSignal.timeout(3000),
    });
    if (response.ok) {
      const value = await response.json();
      if (value.status === "waiting" && value.can_launch === true && typeof value.incident_id === "string") {
        windowReady = true;
        break;
      }
    }
  } catch { /* The trusted native bootstrap and TLS proxy may still be starting. */ }
  await Bun.sleep(750);
}
assert.ok(windowReady,"disposable native operator window failed its startup check");
"""

SERVER_ENV = """
// Do not pass the fixture stack descriptor, privileged database keys, or host environment to SSR.
const serverEnv: Record<string,string> = {};
for (const key of ["PATH","HOME","TMPDIR","LANG","SSL_CERT_FILE","NODE_EXTRA_CA_CERTS"]) {
  const value = process.env[key]; if (value) serverEnv[key] = value;
}
Object.assign(serverEnv, {
  __VITE_ADDITIONAL_SERVER_ALLOWED_HOSTS:"app.example.org",
  VITE_SUPABASE_URL: app + "/qa-supabase", VITE_SUPABASE_PUBLISHABLE_KEY:stack.ANON_KEY,
  SUPABASE_URL: api.origin, SUPABASE_PUBLISHABLE_KEY:stack.ANON_KEY,
  COINGLASS_OPERATOR_ID:actor, COINGLASS_WORKSPACE_ID:workspace,
  COINGLASS_COLLECTOR_URL:viewer, COINGLASS_COLLECTOR_TOKEN:bridge,
});
"""

BROWSER = """
  // System Chromium is the isolated native desktop. The distinct QA client uses its own vendor policy location.
  const clientEnv: Record<string,string> = {};
  for (const k of ["HOME","PATH","TMPDIR","LANG"]) if (process.env[k]) clientEnv[k]=process.env[k]!;
  browser = await chromium.launch({executablePath:"/usr/bin/google-chrome-stable",chromiumSandbox:true,headless:true,env:clientEnv});
  const context = await browser.newContext({viewport:{width:1440,height:1050},serviceWorkers:"block"});
  // Context-wide interception covers the popup's initial navigation and each sibling tab.
  // Host firewall/QA network confinement is also required independently of Playwright.
  await context.route("**/*", (route: any) => {
    const url = new URL(route.request().url());
    if (url.origin === app || (url.origin === viewer &&
        (url.pathname === "/coinglass/operator" || url.pathname.startsWith("/coinglass/operator/")))) {
      return route.continue();
    }
    return route.abort();
  });
  await context.routeWebSocket("**/*", (socket: any) => {
    const url = new URL(socket.url());
    if ((url.origin==="wss://collector.example.org" && url.pathname==="/coinglass/operator/display") ||
        url.origin==="wss://app.example.org") return socket.connectToServer();
    return socket.close();
  });
  const page = await context.newPage();
  page.setDefaultTimeout(45000);
"""

VISUAL = """
  const launch = page.getByRole("button",{name:"התחברות ל־CoinGlass והמשך איסוף",exact:true});
  await launch.waitFor({state:"visible",timeout:15000});
  assert.equal(await launch.isEnabled(),true);
  const receipt = await verifyHostedFixture(page);
  assert.ok(receipt.real_canvas_changed_after_pointer_input_twice &&
    receipt.owner_button_disabled_while_attached && receipt.competing_viewer_disconnected_while_original_controls &&
    receipt.close_abandon_and_reopen);
  console.log(JSON.stringify({dummy_only:true,api_login_and_worksheet_access:true,
    app_commit:"aa96f7c4d1f81b7a4f9c4bece6b058ecf4699a4f",...receipt}));
"""


def generated(source):
    out = once(source, 'const stack =', HARDEN + '\nconst stack =')
    out = once(out, 'const app = "http://127.0.0.1:4317";', 'const app = "https://app.example.org";')
    # Retain disposable GoTrue actor, template, mapped field, worksheet and auth guard in-place.
    # Only the old independent horizon-write fixture and its assertions are outside this rehearsal.
    out = out[:out.index('const oldRow =')] + ARM + out[out.index('process.env.COINGLASS_OPERATOR_ID = actor;'):]
    out = once(out, 'const { CG_LOCAL_STACK: _privateMetadata, ...serverEnv } = process.env;', SERVER_ENV)
    out = once(out, '"--port", "4317"', '"--port", "4317", "--strictPort"')
    out = out[:out.index('  browser = await chromium.launch')] + BROWSER + out[out.index('  await page.goto(app);'):]
    out = once(out, 'url.origin === api.origin &&\n      url.pathname === "/auth/v1/token"',
               'url.origin === app &&\n      url.pathname === "/qa-supabase/auth/v1/token"')
    out = once(out, 'api.origin + "/auth/v1/user"', 'app + "/qa-supabase/auth/v1/user"')
    out = once(out, '"/rest/v1/rpc/v2_can_edit_workspace"', '"/qa-supabase/rest/v1/rpc/v2_can_edit_workspace"')
    out = out[:out.index('  async function verifyRows(')] + VISUAL + out[out.rindex('} finally {'):]
    return out


def db_preparer(source):
    # Use the upstream disposable DB/migration prefix. The trusted setup account alone has Docker access.
    setup = source[:source.index("export VITE_SUPABASE_URL=")]
    setup = once(setup, "set -euo pipefail", """set -euo pipefail
test "$(id -u)" = 0
: "${QA_APP_UID:?numeric UID of the unprivileged fixture app runner}"
case "$QA_APP_UID" in *[!0-9]*|'') exit 2;; esac
test "$QA_APP_UID" -gt 0
getent passwd "$QA_APP_UID" >/dev/null
umask 077
handoff=/run/coinglass-window-appdb
mkdir -m 0711 "$handoff"
chmod 0711 "$handoff"
""")
    setup = once(setup, '  rm -rf "$scratch_dir"', '  rm -f "$handoff/stack.json"\n  rmdir "$handoff"\n  rm -rf "$scratch_dir"')
    setup += """jq -ce '{API_URL,ANON_KEY,SERVICE_ROLE_KEY} | select(.API_URL=="http://127.0.0.1:54321" and (.ANON_KEY|type)=="string" and (.SERVICE_ROLE_KEY|type)=="string")' "$CG_LOCAL_STACK" > "$scratch_dir/app-keys.json"
install -o "$QA_APP_UID" -m 0400 "$scratch_dir/app-keys.json" "$handoff/stack.pending"
mv -T "$handoff/stack.pending" "$handoff/stack.json"
echo 'Disposable DB is ready. Stop this bootstrap after the target test. It cleans up automatically at 15 minutes.'
sleep 900
"""
    return setup


def runner():
    return """#!/usr/bin/env bash
set -euo pipefail
test "$(id -u)" -ne 0
if test -S /var/run/docker.sock && test -w /var/run/docker.sock; then
  echo 'Use a QA app user without Docker privileges.' >&2
  exit 1
fi
test -x /usr/bin/google-chrome-stable
test -f "${CG_QA_PLAYWRIGHT:?preinstall playwright-core 1.56.1 outside the isolated run}"
test -r "${NODE_EXTRA_CA_CERTS:?trusted local fixture CA required for command-line TLS}"
export CG_LOCAL_STACK=/run/coinglass-window-appdb/stack.json
test -r "$CG_LOCAL_STACK"
export CG_PLAYWRIGHT="$CG_QA_PLAYWRIGHT"
exec bun .github/coinglass-auth-browser/target-window.ts
"""


def stage(root):
    ref = subprocess.check_output(["git","-C",str(root),"rev-parse","HEAD"],text=True).strip()
    if ref != APP_COMMIT:
        raise ValueError("expected reviewed Decision Hub app commit")
    subprocess.run(["git","-C",str(root),"diff","--quiet","HEAD","--","src",".github/coinglass-auth-browser"],check=True)
    folder = root/".github"/"coinglass-auth-browser"
    outputs = {
        folder/"target-window.ts": generated(verified(folder/"verify.ts",VERIFY)),
        folder/"run-target-window.sh": runner(),
        folder/"prepare-target-db.sh": db_preparer(verified(folder/"run.sh",RUN)),
        folder/"target-visual-receipt.mjs": (HERE/"visual-receipt.mjs").read_text(),
    }
    for path, data in outputs.items():
        if path.is_symlink() or (path.exists() and path.read_text() != data):
            raise ValueError("inconsistent previous target stage " + path.name)
    for path, data in outputs.items():
        if not path.exists():
            with path.open("x",encoding="utf-8") as f: f.write(data)
            if path.suffix == ".sh": os.chmod(path,0o700)
    print("Staged isolated Decision Hub window driver under .github/coinglass-auth-browser")


if __name__ == "__main__":
    if len(sys.argv) != 3 or sys.argv[1] != "stage":
        raise SystemExit("usage: stage-app.py stage /absolute/path/to/disposable/app-checkout")
    stage(Path(sys.argv[2]).resolve())
