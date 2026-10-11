'use strict';
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
const {test}=require('node:test');
const source=fs.readFileSync(path.join(__dirname,'../web/app.js'),'utf8');
const tick=()=>new Promise(resolve=>setImmediate(resolve));

test('flat exchange differences show local quantity and settlement reason instead of an external zero position',async()=>{
  const h=await dashboard();
  h.run(`state={mode:'live',paused:true,market_at:Date.now(),account_at:Date.now(),external_positions:{WLDUSDC:'0'},external_orders:[],account_differences:[{symbol:'WLDUSDC',local_quantity:'987.3',exchange_quantity:'0',orders:[],reason:'平仓成交数量尚未匹配'}]};renderReconciliation();renderMonitoring();`);
  assert.equal(h.element('reconciliation-note').hidden,false);
  assert.match(h.element('reconciliation-note').textContent,/本地 做多 987.3，交易所 已无持仓/);
  assert.match(h.element('reconciliation-note').textContent,/平仓成交数量尚未匹配/);
  assert.doesNotMatch(h.element('reconciliation-note').textContent,/外部持仓.*0/);
  assert.equal(h.element('monitor-state').textContent,'账户差异待核对');
  h.run(`state.position_views=[{symbol:'WLDUSDC',side:'LONG',quantity:'987.3',opened_at:Date.now(),leverage:10,stop_loss_triggered:true,accounting_complete:true}];renderPositions();`);
  assert.match(h.element('positions-body').innerHTML,/止损等待核对/);
  assert.doesNotMatch(h.element('positions-body').innerHTML,/止损处理中/);
  h.run('state.account_differences=[];state.external_positions={};renderReconciliation();');
  assert.equal(h.element('reconciliation-note').hidden,true);
});

test('account differences identify untracked order IDs and retain older API compatibility',async()=>{
  const h=await dashboard();
  h.run(`state={external_positions:{},external_orders:['WLDUSDC'],account_differences:[{symbol:'WLDUSDC',orders:[{order_id:43,client_id:'manual-close',status:'NEW',quantity:'10',executed_quantity:'3'}]}]};renderReconciliation();`);
  assert.match(h.element('reconciliation-note').textContent,/#43 \/ manual-close（已挂单，剩余 7）/);
  h.run(`state={external_positions:{WLDUSDC:'0'},external_orders:[],positions:[{symbol:'WLDUSDC',side:'SHORT',quantity:'987.3'}]};renderReconciliation();`);
  assert.match(h.element('reconciliation-note').textContent,/本地 做空 987.3，交易所 已无持仓/);
});

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

test('monitoring displays zero counters, cooldowns and pending account reconciliation',async()=>{
  const h=await dashboard();
  h.run(`state={mode:'testnet',paused:false,account_sync_failed:false,account_update_pending:true,market_at:Date.now(),account_at:Date.now(),runtime:{scan:{running:false,runs:0,failed_runs:0,slow_runs:0,last_duration_ms:null,last_start_interval_ms:null,max_duration_ms:0},position_management:{running:true,runs:12,failed_runs:2,slow_runs:1,last_duration_ms:6200,last_start_interval_ms:5000,max_duration_ms:7000}},exchange_rate_limit:{weight_limit:2400,estimated_used_weight:2200,remaining_weight:200,regular_remaining_weight:0,budget_rejections:3,cooldown_rejections:4,cooldown_remaining_ms:60000,next_weight_release_in_ms:0},user_stream:{enabled:true,connected:false,events:5,reconnects:2,last_event_at:0,last_error:'<unsafe>'}};renderMonitoring();`);
  assert.match(h.element('monitor-scan').innerHTML,/等待首轮/);
  assert.match(h.element('monitor-scan').innerHTML,/0 \/ 0 \/ 0/);
  assert.match(h.element('monitor-position').innerHTML,/执行中/);
  assert.match(h.element('monitor-position').innerHTML,/6.2 s/);
  assert.equal(h.element('monitor-state').textContent,'限流退避');
  assert.match(h.element('monitor-budget-detail').innerHTML,/60.0 s/);
  assert.match(h.element('monitor-stream').innerHTML,/REST 兜底/);
  assert.match(h.element('monitor-stream').innerHTML,/&lt;unsafe&gt;/);
  assert.ok(h.element('monitor-weight').value>90);
  h.run('state.exchange_rate_limit.cooldown_remaining_ms=0;renderMonitoring();');
  assert.equal(h.element('monitor-state').textContent,'账户对账中');
  h.run('state.account_sync_failed=true;renderMonitoring();');
  assert.equal(h.element('monitor-state').textContent,'账户同步异常');
});

test('monitoring distinguishes funding and fill reconciliation from a user pause',async()=>{
  const h=await dashboard();
  h.run("state={mode:'live',paused:false,market_at:Date.now(),account_at:Date.now(),funding_sync_pending:true,entry_data_pending:true};renderMonitoring();");
  assert.equal(h.element('monitor-state').textContent,'资金费核对中');
  assert.match(h.element('monitor-state').className,/uncertain/);
  h.run('state.funding_sync_pending=false;renderMonitoring();');
  assert.equal(h.element('monitor-state').textContent,'成交费用核对中');
  h.run('state.entry_data_pending=false;renderMonitoring();');
  assert.equal(h.element('monitor-state').textContent,'运行中');
  h.run('state.paused=true;renderMonitoring();');
  assert.equal(h.element('monitor-state').textContent,'新增开仓已暂停');
});

test('monitoring preserves the hard pause reason after data recovers and renders it as text',async()=>{
  const h=await dashboard();
  h.run(`state={mode:'live',paused:true,scan_pause_reason:'账户差异：<order> 本地数量 0.6，交易所数量 0.5',market_at:Date.now(),account_at:Date.now()};renderMonitoring();`);
  assert.equal(h.element('monitor-state').textContent,'新增开仓已暂停');
  assert.match(h.element('monitor-freshness').textContent,/暂停原因：账户差异：<order> 本地数量 0.6/);
  h.run('state.paused=false;renderMonitoring();');
  assert.doesNotMatch(h.element('monitor-freshness').textContent,/暂停原因/);
});

test('monitoring handles absent metrics and paper mode without misleading values',async()=>{
  const h=await dashboard();
  h.run("state={mode:'paper',paused:true,market_at:0,account_at:0};renderMonitoring();");
  assert.match(h.element('monitor-scan').innerHTML,/等待统计/);
  assert.match(h.element('monitor-stream').innerHTML,/本机模拟/);
  assert.equal(h.element('monitor-weight-value').textContent,'等待统计');
  assert.equal(h.element('monitor-state').textContent,'数据待更新');
  assert.doesNotMatch(h.element('monitor-budget-detail').innerHTML,/NaN|undefined/);
  assert.match(h.element('monitor-freshness').textContent,/尚未同步/);
});

test('partial orders display the fee blocker and clear it after reconciliation',async()=>{
  const h=await dashboard();
  h.run("state={open_orders:[{symbol:'WLDUSDC',client_id:'test-order',role:'take_profit',status:'PARTIALLY_FILLED',order_side:'SELL',price:'101',quantity:'1',remaining_quantity:'0.6',accounted_quantity:'0.4',created_at:1,settlement_issue:'成交明细查询失败：<unsafe>'}]};renderOrders();");
  assert.match(h.element('pending-body').innerHTML,/部分成交/);
  assert.match(h.element('pending-body').innerHTML,/费用核对：成交明细查询失败：&lt;unsafe&gt;/);
  h.run('state.open_orders[0].settlement_issue=null;renderOrders();');
  assert.doesNotMatch(h.element('pending-body').innerHTML,/费用核对：/);
});

test('monitoring shows public market subscriptions and unsaved cooldowns',async()=>{
  const h=await dashboard();
  h.run("state={mode:'paper',paused:true,market_at:Date.now(),market_stream:{connected:true,subscriptions:3,last_error:null},exchange_rate_limit:{weight_limit:2400,estimated_used_weight:0,cooldown_remaining_ms:60000,cooldown_persistence_pending:true}};renderMonitoring();");
  assert.match(h.element('monitor-stream').innerHTML,/行情已连接 · 3 个订阅/);
  assert.match(h.element('monitor-budget-detail').innerHTML,/保存待重试/);
  assert.equal(h.element('monitor-state').textContent,'退避状态待保存');
  h.run("state.market_stream={connected:false,subscriptions:0,last_error:'<unsafe>'};state.exchange_rate_limit.cooldown_persistence_pending=false;renderMonitoring();");
  assert.match(h.element('monitor-stream').innerHTML,/行情 REST 兜底/);
  assert.doesNotMatch(h.element('monitor-stream').innerHTML,/<unsafe>/);
  assert.equal(h.element('monitor-state').textContent,'限流退避');
});

test('background mode notice shows retained exposure and account differences',async()=>{
  const h=await dashboard();
  h.run("state={background_modes:[{mode:'testnet',managing:true,configured:true,positions:2,pending_orders:1,external_positions:1,external_orders:1,account_sync_failed:false}]};renderBackgroundModes();");
  const note=h.element('background-modes-note');
  assert.equal(note.hidden,false);
  assert.match(note.textContent,/测试网 · 后台风控管理中 · 2 个持仓 \/ 1 笔待确认订单/);
  assert.match(note.textContent,/2 项账户差异待人工核对/);
  assert.match(note.textContent,/新增开仓已暂停/);
  h.run('state.background_modes[0].account_sync_failed=true;renderBackgroundModes();');
  assert.match(note.textContent,/账户同步异常/);
  h.run('state.background_modes[0].configured=false;renderBackgroundModes();');
  assert.match(note.textContent,/等待原账户密钥/);
  h.run('state.background_modes[0].configured=true;state.background_modes[0].account_sync_failed=false;state.background_modes[0].managing=false;renderBackgroundModes();');
  assert.match(note.textContent,/后台管理未启动/);
  h.run('state={};renderBackgroundModes();');
  assert.equal(note.hidden,true);
});
