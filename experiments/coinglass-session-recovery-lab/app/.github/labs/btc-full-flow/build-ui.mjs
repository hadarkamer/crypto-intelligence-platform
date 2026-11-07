import path from 'node:path';
import {existsSync} from 'node:fs';
const root=process.cwd(), lab=path.join(root,'.github/labs/btc-full-flow');
const transport=path.join(lab,'transport.ts');
const result=await Bun.build({entrypoints:[path.join(lab,'ui.tsx')],outdir:path.join(lab,'dist'),target:'browser',
 plugins:[{name:'loopback-only-boundaries',setup(build){
 build.onResolve({filter:/^(@tanstack\/react-start|@\/integrations\/supabase\/client|@\/lib\/coinglassCollect.functions)$/},()=>({path:transport}));
 build.onResolve({filter:/^@\//},({path:p})=>({path:['','.ts','.tsx','/index.ts','/index.tsx'].map(e=>path.join(root,'src',p.slice(2))+e).find(existsSync)}));
 }}]});
if(!result.success){console.error(result.logs);process.exit(1);}
