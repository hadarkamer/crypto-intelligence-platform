import React from 'react';
import {createRoot} from 'react-dom/client';
import {QueryClient,QueryClientProvider,useQuery} from '@tanstack/react-query';
import {CoinglassSheetBatch} from '@/components/v2/CoinglassSheetBatch';
import {StructuredView,LiquidityTableField} from '@/components/StructuredTables';
import {call} from './transport';
const fx=await call('/fixture');
const forbidden=()=>{throw Error('Manual writes are not part of this isolated experiment')};
const actions={addLiquidityRow:forbidden,createLiquidityLevel:forbidden,deleteLiquidityRow:forbidden,updateLiquidityLevel:forbidden,deleteLiquidityLevel:forbidden,createNewsRow:forbidden,addNewsRow:forbidden,updateNewsRow:forbidden,deleteNewsRow:forbidden};
const client=new QueryClient({defaultOptions:{queries:{retry:false}}});
function App(){
 const query=useQuery({queryKey:['v2-collection-values',fx.sid],queryFn:()=>call('/values')});
 const rows=query.data??[];
 return <><h1>הזן אוטומטית BTC — סביבת ניסוי בלבד</h1><p>3 מודלים × 12H / 24H / 48H. מקור וניתוח מבוקרים.</p>
 <CoinglassSheetBatch sessionId={fx.sid} targets={fx.fields.map((f:any,i:number)=>({fieldId:f.id,model:i+1}))} onDone={()=>{}}/>
 {fx.fields.map((f:any,i:number)=><section key={f.id} data-model={i+1} data-field={f.id}><h2>BTC — מודל {i+1}</h2>
 <fieldset disabled data-editor={i+1}><LiquidityTableField questionId={f.id} rows={rows.find((r:any)=>r.field_id===f.id)?.value.rows??[]} actions={actions}/></fieldset>
 <StructuredView snapshot={{type:'liquidity_table',rows:rows.find((r:any)=>r.field_id===f.id)?.value.rows??[]}}/>
 </section>)}<script id="rendered-data" type="application/json">{JSON.stringify(rows)}</script></>;
}
createRoot(document.getElementById('root')!).render(<QueryClientProvider client={client}><App/></QueryClientProvider>);
