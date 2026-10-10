'use strict';
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
const {test}=require('node:test');
const source=fs.readFileSync(path.join(__dirname,'../web/app.js'),'utf8');
const tick=()=>new Promise(resolve=>setImmediate(resolve));

async function dashboard(){
  const elements=new Map(),timers=new Map(),intervals=new Map(),listeners=new Map();
  let timerId=0;
  function element(id){
    if(!elements.has(id)){
      let html='';
      elements.set(id,{
        dataset:{},style:{},classList:{toggle(){}},value:'',writes:0,
        addEventListener(){},setAttribute(){},removeAttribute(){},reset(){},
        querySelectorAll(){return [];},contains(){return false;},
        get innerHTML(){return html;},set innerHTML(value){html=value;this.writes++;this.afterWrite?.();},
      });
    }
    return elements.get(id);
  }
  const document={visibilityState:'visible',activeElement:null,
    getElementById:element,querySelector:element,querySelectorAll:()=>[],
    addEventListener:(name,fn)=>listeners.set(name,fn)};
  const context=vm.createContext({document,window:{addEventListener(){}},location:{hash:''},
    AbortController,fetch:async()=>{throw new Error('initially disconnected');},
    setTimeout:(fn,ms)=>{const id=++timerId;timers.set(id,{fn,ms});return id;},
    clearTimeout:id=>timers.delete(id),setInterval:(fn,ms)=>intervals.set(ms,fn),
  });
  vm.runInContext(source,context);
  await tick();
  return {context,document,element,timers,intervals,listeners,run:code=>vm.runInContext(code,context)};
}

test('unchanged regions keep DOM nodes, expanded details and focus',async()=>{
  const h=await dashboard(),region=h.element('decisions-body');
  let detail={dataset:{key:'decision:1'},open:true},focus={dataset:{focusKey:'decision:1'},focus(){this.focused=true;}};
  region.querySelectorAll=selector=>selector==='[data-focus-key]'?[focus]:[detail];
  region.contains=node=>node===focus;
  h.document.activeElement=focus;
  region.afterWrite=()=>{detail={dataset:{key:'decision:1'},open:false};focus={dataset:{focusKey:'decision:1'},focus(){this.focused=true;}};};
  h.run("replaceRegion('decisions-body','first')");
  assert.equal(detail.open,true);assert.equal(focus.focused,true);
  const preserved=detail;region.scrollTop=73;
  h.run("replaceRegion('decisions-body','first')");
  assert.equal(region.writes,1);assert.equal(detail,preserved);assert.equal(region.scrollTop,73);
  h.document.activeElement=focus;
  h.run("replaceRegion('decisions-body','updated')");
  assert.equal(region.writes,2);assert.equal(detail.open,true);assert.equal(focus.focused,true);
});

test('read timeout also covers a stalled response body and clears its timer',async()=>{
  const h=await dashboard();
  h.context.fetch=async(_path,{signal})=>({ok:true,json:()=>new Promise((_resolve,reject)=>{
    signal.addEventListener('abort',()=>reject(new Error('body aborted')),{once:true});
  })});
  const request=h.run("api('/api/dashboard','GET',undefined,20)");
  await tick();
  [...h.timers.values()].find(timer=>timer.ms===20).fn();
  await assert.rejects(request,/请求超时，请稍后重试/);
  assert.equal(h.timers.size,0);
});

test('write timeout reports an uncertain outcome without retrying the operation',async()=>{
  const h=await dashboard();let calls=0;
  h.context.fetch=(_path,options)=>{calls++;assert.equal(options.method,'POST');assert.equal(options.credentials,'same-origin');assert.equal(options.headers['x-genius-request'],'1');assert.equal(options.body,'{"running":true}');
    return new Promise((_resolve,reject)=>options.signal.addEventListener('abort',()=>reject(new Error('aborted')),{once:true}));};
  const request=h.run("api('/api/control','POST',{running:true},20)");
  [...h.timers.values()].find(timer=>timer.ms===20).fn();
  await assert.rejects(request,/操作结果待确认/);assert.equal(calls,1);assert.equal(h.timers.size,0);
});

test('HTTP authentication errors retain their status and release the timeout',async()=>{
  const h=await dashboard();
  h.context.fetch=async()=>({ok:false,status:401,json:async()=>({error:'管理员验证已过期'})});
  await assert.rejects(h.run("api('/api/control','POST',{})"),error=>error.status===401&&error.message==='管理员验证已过期');
  assert.equal(h.timers.size,0);
});

test('a timed out dashboard refresh releases busy and permits a later refresh',async()=>{
  const h=await dashboard();let calls=0;
  h.context.fetch=(_path,{signal})=>{calls++;return new Promise((_resolve,reject)=>signal.addEventListener('abort',()=>reject(new Error('aborted')),{once:true}));};
  const first=h.run('refresh()');
  await h.run('refresh()');assert.equal(calls,1);
  [...h.timers.values()].find(timer=>timer.ms===10000).fn();await first;
  assert.equal(h.run('busy'),false);
  const second=h.run('refresh()');assert.equal(calls,2);
  [...h.timers.values()].find(timer=>timer.ms===10000).fn();await second;
});

test('hidden pages skip scheduled requests and resume immediately when visible',async()=>{
  const h=await dashboard();
  h.run('globalThis.polls=0;globalThis.events=0;refresh=()=>polls++;loadEvents=()=>events++;');
  h.document.visibilityState='hidden';h.intervals.get(2500)();h.intervals.get(15000)();
  assert.equal(h.run('polls'),0);assert.equal(h.run('events'),0);
  h.document.visibilityState='visible';h.listeners.get('visibilitychange')();
  assert.equal(h.run('polls'),1);assert.equal(h.run('events'),1);
  h.intervals.get(2500)();h.intervals.get(15000)();
  assert.equal(h.run('polls'),2);assert.equal(h.run('events'),2);
});

test('event requests coalesce and old responses cannot overwrite a newly selected mode',async()=>{
  const h=await dashboard(),pending=[];
  h.context.fetch=()=>new Promise(resolve=>pending.push(message=>resolve({ok:true,json:async()=>[{time:1,level:'info',symbol:null,message,detail:{}}]})));
  h.run("state={mode:'paper'}");
  const oldPaper=h.run('loadEvents()');await h.run('loadEvents()');assert.equal(pending.length,1);
  h.run("state={mode:'live'}");const live=h.run('loadEvents()');
  h.run("state={mode:'paper'}");const newPaper=h.run('loadEvents()');
  assert.equal(pending.length,3);
  pending[2]('current paper');await newPaper;
  pending[0]('old paper');await oldPaper;
  pending[1]('old live');await live;
  assert.match(h.element('events-body').innerHTML,/current paper/);
  assert.doesNotMatch(h.element('events-body').innerHTML,/old paper|old live/);
  assert.equal(h.element('events-body').writes,1);assert.equal(h.timers.size,0);
  assert.equal(h.run('eventsLoading'),null);
});
