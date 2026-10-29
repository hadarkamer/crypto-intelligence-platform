// Uses the exact captured Pixi8.21.0 Ticker module, not a rewritten ticker.
// No browser or network. Rendering is a deterministic callback fixture.
const fs=require('node:fs'),vm=require('node:vm'),assert=require('node:assert/strict'),crypto=require('node:crypto');
const pixiPath=process.argv[2]||'/tmp/cg29-m3-review/10679-f5d0c63436a34cc4.js';
const pixi=fs.readFileSync(pixiPath);
assert.equal(crypto.createHash('sha256').update(pixi).digest('hex'),'4c6bfb003a2a7713014a21c00bea7bc61b58a06294c6c56bbc76881910ee9150');
let clock=0,rafCalls=0,cancelCalls=0,geometryReads=0;
const nativeRAF=()=>++rafCalls,nativeCancel=()=>cancelCalls++;
const env={self:{},performance:{now:()=>clock},requestAnimationFrame:nativeRAF,cancelAnimationFrame:nativeCancel};
const context=vm.createContext(env);vm.runInContext(pixi.toString(),context);
const modules=env.self.webpackChunk_N_E[0][1],cache={};
function requireModule(id){
  if(cache[id])return cache[id];
  const exports={};cache[id]=exports;modules[id]({exports},exports,requireModule);return exports;
}
requireModule.d=(exports,map)=>{for(const[k,get]of Object.entries(map))Object.defineProperty(exports,k,{enumerable:true,get})};
const Ticker=requireModule(41230).R;
const ticker=new Ticker();const shared=new Ticker();
let renders=0,otherUpdates=0,sharedUpdates=0;const order=[];
const originalResult={rendered:true};
const canvas={id:'verified-map',className:'pixi',width:1600,height:1300,
 getBoundingClientRect(){geometryReads++;return{x:30,y:100,width:1460,height:800}}};
class Renderer{constructor(){this.canvas=canvas}render(arg){assert.equal(this,renderer);assert.equal(arg.container,app.stage);renders++;order.push('paint');return originalResult}}
const renderer=new Renderer();
const app={ticker,renderer,stage:{},render(){return this.renderer.render({container:this.stage})}};
const rendererPrototypeRender=renderer.render;
ticker.add(()=>{otherUpdates++;order.push('state')},null,0);
ticker.add(app.render,app,-25);
shared.add(()=>sharedUpdates++);
ticker.start();shared.start();
function frames(count,step=50){for(let i=0;i<count;i++){clock+=step;ticker.update(clock);shared.update(clock)}}
vm.runInContext(fs.readFileSync(__dirname+'/render_gate.js','utf8'),context);
const hook=env.__cg29RenderLab,key=hook.attach(app);
assert.equal(key,1);assert.equal(hook.attach(app),key);assert.equal(ticker.count,2);
const aRenders=renders,aUpdates=otherUpdates;frames(20);
assert.equal(renders-aRenders,20,'inert attachment preserves every original automatic paint');
assert.equal(otherUpdates-aUpdates,20);
assert.equal(geometryReads,0,'no implicit canvas geometry in callbacks');
for(let i=0;i<order.length;i+=2)assert.deepEqual(order.slice(i,i+2),['state','paint'],'original render priority preserved');
const rateBefore=[ticker.minFPS,ticker.maxFPS,ticker.speed,ticker.started];
hook.enable(key,.5);
assert.deepEqual([ticker.minFPS,ticker.maxFPS,ticker.speed,ticker.started],rateBefore,'ticker itself is unchanged');
assert.equal(ticker.count,2);
const bRenders=renders,bOther=otherUpdates,bShared=sharedUpdates;
frames(80); //4sec ->2paints at0.5Hz, all80normal/shared callbacks still execute.
assert.equal(renders-bRenders,2);
assert.equal(otherUpdates-bOther,80);assert.equal(sharedUpdates-bShared,80);
const beforeDirect=renders;
assert.equal(app.render(),originalResult,'direct source render return/receiver/argument preserved');
assert.equal(renders,beforeDirect+1,'direct source render is not throttled');
const beforeForce=renders;hook.forcePaint(key);assert.equal(renders,beforeForce+1,'fresh screenshot paint is unconditional');
const afterForce=renders;frames(1);assert.equal(renders,afterForce,'no duplicate auto paint just after forced paint');
const stats=hook.snapshot()[0];assert.equal(stats.enabled,true);assert.equal(stats.experimental_fps,.5);
assert.equal(stats.stats.skipped_paints,79);
hook.disable(key);assert.equal(ticker.count,2);assert.equal(hook.snapshot()[0].enabled,false);
const cRenders=renders,cOther=otherUpdates;frames(20);
assert.equal(renders-cRenders,20,'original automatic paint cadence returns after disable');
assert.equal(otherUpdates-cOther,20);
assert.deepEqual([ticker.minFPS,ticker.maxFPS,ticker.speed,ticker.started],rateBefore);
assert.equal(env.requestAnimationFrame,nativeRAF);assert.equal(env.cancelAnimationFrame,nativeCancel);
assert.throws(()=>hook.enable(key,0),/invalid_experimental_fps/);
assert.throws(()=>hook.enable(key,NaN),/invalid_experimental_fps/);
assert.equal(hook.restoreAll()[0].restored,true);
assert.equal(renderer.render,rendererPrototypeRender);
assert.equal(Object.hasOwn(renderer,'render'),false,'original inherited renderer method restored');
assert.equal(hook.restoreAll()[0].restored,true,'restoration idempotent');
assert.throws(()=>hook.enable(key,.5),/already_restored/);
console.log(JSON.stringify({passed:true,actual_pixi_ticker_sha256:'4c6bfb003a2a7713014a21c00bea7bc61b58a06294c6c56bbc76881910ee9150',
 inert_attachment_original_paints:20,candidate_paints_in_four_seconds:2,normal_callbacks_preserved:80,shared_callbacks_preserved:80,
 direct_renders_preserved:true,fresh_forced_paint:true,restored_original_paints:20,global_raf_unchanged:true,
 ticker_rates_started_speed_unchanged:true,render_descriptor_restored:true,market_data_accesses:0},null,2));
