"""Generate a loopback-only backend from the pinned, reviewed SQL lab.
Product files are never rewritten. Replacements are lab transport adapters.
"""
from pathlib import Path
import subprocess, sys
ROOT = Path(__file__).resolve().parents[3]
LAB = Path(__file__).resolve().parent
subprocess.run([sys.executable, str(ROOT/'.github/labs/model3-real-queue/prepare_lab.py')], check=True)
p = ROOT/'.github/labs/model3-real-queue.generated.mjs'
s = p.read_text()
s = s.replace('const clientPg =', "pgModule.types.setTypeParser(1184, value => new Date(value).toISOString());\nconst clientPg =")
s = s.replace('        single() {', '        limit() { return this; },\n        maybeSingle() { single=true; return run(); },\n        single() {')
s = s.replace('`${k}${op}${param(v)}`', '`${k.replace("->>", "->>\\\'")+ (k.includes("->>") ? "\\\'" : "")}${op}${param(v)}`')
s = s.replace('select ${columns} from public.${table} where ${where}', '''select ${columns.includes('section:v2_template_sections') ? "id,title,field_type,settings,active,(select jsonb_build_object('template_id',s.template_id) from public.v2_template_sections s where s.id=section_id) as section" : columns} from public.${table} where ${where}''')
s = s.replace('        Buffer,', '        Buffer, btoa, atob,')
s = s.replace('const notices = [];', '''const events=[];
  const notices = [];''')
start=s.index('  api = load("src/lib/coinglassCollector.ts");')
s=s[:start]+(LAB/'backend-tail.mjs').read_text()+'\n}\n'
out=ROOT/'.github/labs/btc-full-flow.generated.mjs'
out.write_text(s)
p.unlink()
