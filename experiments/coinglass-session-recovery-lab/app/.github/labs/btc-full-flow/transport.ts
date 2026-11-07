export async function call(route:string,data?:unknown){
  const r=await fetch(route,data===undefined?undefined:{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(data)});
  const result=await r.json(); if(!r.ok)throw new Error(result.error);return result;
}
export const useServerFn=(fn:unknown)=>fn;
export const startModel1Collection=({data}:any)=>call('/start',data);
export const pollModel1Collection=({data}:any)=>call('/poll',data);
export const supabase={
 auth:{getUser:async()=>({data:{user:{id:(await call('/fixture')).owner}}}),onAuthStateChange:()=>({data:{subscription:{unsubscribe(){}}}})},
 from(table:string){if(table!=='v2_collection_values')throw Error(table);let fieldId='';return {
 select(){return this},eq(k:string,v:string){if(k==='field_id')fieldId=v;return this},maybeSingle:()=>call('/baseline',{fieldId})};}
};
