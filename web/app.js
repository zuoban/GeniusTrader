'use strict';
const $=id=>document.getElementById(id);
let state=null,markets=[],selected=new Set(),editing=null,closing=null,busy=false;
let auth={initialized:false,authenticated:false},pendingAdmin=null,setupPrompted=false;
let assetsHidden=false,watchlistFilter='all';
let eventsMode=null,eventsLoading=null;
const renderedRegions=new WeakMap();
const credentialTestRevision={testnet:0,live:0,jev:0};
const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const num=(v,d=2)=>Number(v||0).toLocaleString('en-US',{minimumFractionDigits:d,maximumFractionDigits:d});
const price=v=>v==null?'—':Number(v).toLocaleString('en-US',{maximumFractionDigits:8});
const quoteAsset=symbol=>symbol.endsWith('USDC')?'USDC':'USDT';
const baseAsset=symbol=>symbol.replace(/(?:USDT|USDC)$/,'');
const color=v=>Number(v)>0?'positive':Number(v)<0?'negative':'';
function toast(message,error=false){$('toast').textContent=message;$('toast').className='toast'+(error?' error':'');$('toast').hidden=false;clearTimeout(toast.timer);toast.timer=setTimeout(()=>{$('toast').hidden=true;},6000);}
async function api(path,method='GET',body,timeoutMs=45000){
  const headers={'x-genius-request':'1'};if(body!==undefined)headers['content-type']='application/json';
  const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),timeoutMs);
  try{
    const r=await fetch(path,{method,headers,credentials:'same-origin',signal:controller.signal,body:body===undefined?undefined:JSON.stringify(body)});
    const data=await r.json().catch(err=>{if(controller.signal.aborted)throw err;return {error:'服务返回格式错误'};});
    if(!r.ok){const error=new Error(data.error||'请求失败 '+r.status);error.status=r.status;throw error;}return data;
  }catch(err){
    if(controller.signal.aborted)throw new Error(method==='GET'?'请求超时，请稍后重试':'请求超时，操作结果待确认，请刷新状态后核对');
    throw err;
  }finally{clearTimeout(timer);}
}
async function act(fn){try{await fn();await refresh();}catch(err){if(err.status===401){await refresh();showAuth(null,err.message);}else toast(err.message,true);}}
function renderAccess(){
  $('access-label').textContent=auth.authenticated?'管理员 · 可操作':'游客 · 只读';
  $('access-label').className='access-label'+(auth.authenticated?' admin':'');
  $('admin-btn').textContent=auth.authenticated?'退出管理':auth.initialized?'管理员登录':'初始化管理员';
  $('setup-banner').hidden=auth.initialized;
}
function showAuth(next=null,message=''){
  pendingAdmin=next;const setup=!auth.initialized,f=$('auth-form');f.reset();
  $('auth-title').textContent=setup?'设置管理员密码':'管理员验证';
  $('auth-eyebrow').textContent=setup?'INITIALIZE WORKSPACE':'ADMIN ACCESS';
  $('auth-note').textContent=setup?'首次使用请设置 12–128 个字符的管理员密码。设置后才能编辑配置与控制交易。':'输入管理员密码后继续操作，登录会话到期后需要重新验证。';
  f.elements.password.autocomplete=setup?'new-password':'current-password';f.elements.password.minLength=setup?12:1;
  $('auth-confirm-label').hidden=!setup;f.elements.confirm_password.required=setup;
  $('auth-submit').textContent=setup?'设置密码并进入 ↗':'验证并继续 ↗';
  $('auth-error').hidden=!message;$('auth-error').textContent=message;
  if(!$('auth-dialog').open)$('auth-dialog').showModal();
}
async function withAdmin(fn){
  try{auth=await api('/api/auth/status');renderAccess();if(auth.authenticated)await fn();else showAuth(fn);}
  catch(err){toast(err.message,true);}
}
async function refresh(){
  if(busy)return;busy=true;try{const previousMode=state?.mode;state=await api('/api/dashboard','GET',undefined,10000);render();if(previousMode!==state.mode){eventsMode=null;replaceRegion('events-body','<div class="empty">正在载入当前模式事件…</div>');loadEvents();}$('connection-label').textContent='● 本地控制台已连接';}catch(err){$('connection-label').textContent='○ 控制台连接中断';$('asset-sync').textContent='本地连接中断 · 数据未更新';$('asset-sync').className='subtle sync-delayed';$('monitor-state').textContent='控制台连接中断';$('monitor-state').className='monitor-state uncertain';$('monitor-freshness').textContent='连接中断 · 运行统计和账户数据等待刷新';}finally{busy=false;}
}
function renderAssets(){
  const s=state,a=s.account_assets,unit=a?.multi_assets_margin?'USD':'USDT';
  const money=(value,d=4)=>assetsHidden?'••••••':num(value,d);
  const native=value=>assetsHidden?'••••••':price(value);
  $('account-kind').textContent={paper:'模拟账户',testnet:'测试网账户',live:'实盘账户'}[s.mode];
  document.querySelectorAll('.asset-currency').forEach(el=>{el.textContent=unit;});
  for(const [id,field] of [['asset-margin','total_margin_balance'],['asset-wallet','total_wallet_balance'],['asset-pnl','total_unrealized_profit'],['asset-available','available_balance'],['asset-initial','total_initial_margin']]){
    $(id).textContent=a?money(a[field]):'—';
  }
  $('asset-pnl').className=a&&!assetsHidden?color(a.total_unrealized_profit):'';
  $('asset-summary-note').textContent=s.mode==='paper'?'模拟账户 · 钱包余额 + 未实现盈亏':!a?'连接账户后展示币安资产汇总':a.multi_assets_margin?'联合保证金模式 · 币安 USD 汇总':'单资产保证金模式 · 币安 USDT 汇总';
  const delayed=Boolean(s.account_assets_error)||(s.mode!=='paper'&&s.account_assets_at>0&&Date.now()-s.account_assets_at>30000);
  $('asset-sync').className='subtle'+(delayed?' sync-delayed':'');
  $('asset-sync').textContent=s.mode==='paper'?'本机模拟资产':delayed?(a?'同步延迟 · 显示上次成功数据':'资产同步失败 · 等待重试'):a?'更新于 '+new Date(s.account_assets_at).toLocaleTimeString('zh-CN',{hour12:false})+' · 约 10 秒同步':s.settings?.[s.mode]?.configured?'正在同步账户资产…':'请先保存当前账户的 API 密钥';
  const assets=(a?.assets||[]).filter(x=>Number(x.wallet_balance)!==0||Number(x.unrealized_profit)!==0||Number(x.margin_balance)!==0).sort((x,y)=>Number(y.wallet_value_usd||0)-Number(x.wallet_value_usd||0)||x.asset.localeCompare(y.asset));
  const positive=assets.filter(x=>Number(x.wallet_balance)>0),valued=positive.every(x=>x.wallet_value_usd!=null&&Number.isFinite(Number(x.wallet_value_usd)));
  const total=valued?positive.reduce((sum,x)=>sum+Number(x.wallet_value_usd),0):0;
  const palette=['#d5eda1','#83b4df','#ecbc72','#b39ad9','#7cc4b4','#ec8b82'];
  const known={USDT:'#d5eda1',USDC:'#83b4df',BTC:'#ecbc72',ETH:'#b39ad9',BNB:'#7cc4b4'};
  const swatches=new Map(assets.map((x,i)=>[x.asset,known[x.asset]||palette[i%palette.length]]));
  let angle=0;const segments=[];
  if(total>0)for(const x of positive){const end=angle+Number(x.wallet_value_usd)/total*360;segments.push(swatches.get(x.asset)+' '+angle+'deg '+end+'deg');angle=end;}
  $('asset-donut').style.background=segments.length?'conic-gradient('+segments.join(',')+')':'var(--line)';
  $('asset-count').textContent=a?String(assets.length):'—';
  const shares=assets.map(x=>total>0&&Number(x.wallet_balance)>0?num(Number(x.wallet_value_usd)/total*100)+'%':'—');
  $('asset-donut').setAttribute('aria-label',total>0?'钱包资产分布：'+assets.map((x,i)=>x.asset+' '+shares[i]).join('，'):!a?'资产分布等待账户同步':!valued?'资产估值暂不可用':'暂无正余额资产');
  replaceRegion('asset-legend',assets.length?assets.map((x,i)=>'<li><div><i class="asset-dot" aria-hidden="true"></i><b>'+esc(x.asset)+'</b><span class="asset-share">'+shares[i]+'</span></div><small>'+native(x.wallet_balance)+' '+esc(x.asset)+'</small></li>').join(''):'<li class="asset-empty">'+(a?'暂无非零余额资产':'连接账户后展示资产分布')+'</li>');
  $('asset-legend').querySelectorAll('.asset-dot').forEach((dot,i)=>{dot.style.background=swatches.get(assets[i].asset);});
  $('asset-distribution-note').textContent=!valued?'估值暂不可用 · 展示原币余额，占比等待更新':s.mode==='paper'?'模拟 USDT 钱包 · 按 1 USDT ≈ 1 USD 展示':'按正钱包余额的 USD 估值计算占比';
  replaceRegion('asset-details-body',assets.length?assets.map(x=>'<tr><td>'+esc(x.asset)+'</td><td>'+native(x.wallet_balance)+'</td><td class="'+(assetsHidden?'':color(x.unrealized_profit))+'">'+native(x.unrealized_profit)+'</td><td>'+native(x.margin_balance)+'</td><td>'+(x.wallet_value_usd==null?'—':money(x.wallet_value_usd,2))+'</td></tr>').join(''):'<tr><td colspan="5">'+(a?'暂无非零余额资产':'等待账户同步')+'</td></tr>');
}
const duration=value=>value==null||!Number.isFinite(Number(value))?'—':Number(value)<1000?num(value,0)+' ms':num(Number(value)/1000,1)+' s';
const count=value=>value==null||!Number.isFinite(Number(value))?'—':num(value,0);
function renderMonitoring(){
  const s=state,now=Date.now(),limit=s.exchange_rate_limit,stream=s.user_stream,market=s.market_stream;
  const cycle=(title,value,target)=>{
    if(!value)return '<label>'+title+'</label><strong>等待统计</strong><p>尚未获取运行数据</p>';
    const status=value.running?'执行中':value.runs>0?'等待下一轮':'等待首轮';
    return '<label>'+title+' <span>目标 '+target+' s</span></label><strong>'+status+'</strong><dl><div><dt>最近耗时 / 实际间隔</dt><dd>'+duration(value.last_duration_ms)+' / '+duration(value.last_start_interval_ms)+'</dd></div><div><dt>轮次 / 异常 / 超时</dt><dd>'+count(value.runs)+' / '+count(value.failed_runs)+' / '+count(value.slow_runs)+'</dd></div></dl><p>最长耗时 '+duration(value.max_duration_ms)+'</p>';
  };
  replaceRegion('monitor-scan',cycle('开仓扫描',s.runtime?.scan,10));
  replaceRegion('monitor-position',cycle('持仓风控',s.runtime?.position_management,5));
  const known=limit&&Number(limit.weight_limit)>0,ratio=known?Math.min(100,Math.max(0,Number(limit.estimated_used_weight)/Number(limit.weight_limit)*100)):0;
  $('monitor-weight-value').textContent=known?count(limit.estimated_used_weight)+' / '+count(limit.weight_limit):'等待统计';
  $('monitor-weight').value=Number.isFinite(ratio)?ratio:0;
  $('monitor-weight').className=ratio>=90?'budget-high':'';
  const cooldown=Number(limit?.cooldown_remaining_ms||0);
  replaceRegion('monitor-budget-detail',known?'<dl><div><dt>普通 / 总剩余权重</dt><dd>'+count(limit.regular_remaining_weight)+' / '+count(limit.remaining_weight)+'</dd></div><div><dt>预算 / 退避拒绝次数</dt><dd>'+count(limit.budget_rejections)+' / '+count(limit.cooldown_rejections)+'</dd></div></dl><p class="'+(cooldown>0||limit.cooldown_persistence_pending?'uncertain':'')+'">'+(limit.cooldown_persistence_pending?'退避状态保存待重试 · 请求暂缓':cooldown>0?'限流退避 · 剩余 '+duration(cooldown):'下笔权重释放 '+duration(limit.next_weight_release_in_ms))+'</p>':'<p>尚未获取请求预算</p>');
  const streamLabel=s.mode==='paper'?'本机模拟':!stream?'等待连接状态':!stream.enabled?'账户未配置':stream.connected?'已连接':'REST 兜底 · 重连中';
  const marketLabel=!market?'等待行情连接状态':market.connected?'行情已连接 · '+count(market.subscriptions)+' 个订阅':market.last_error?'行情 REST 兜底 · 重连中':market.subscriptions===0?'行情等待订阅':'行情连接中';
  replaceRegion('monitor-stream','<label>账户事件连接</label><strong class="'+(stream?.enabled&&!stream.connected?'uncertain':'')+'">'+streamLabel+'</strong><dl><div><dt>推送 / 重连次数</dt><dd>'+count(stream?.events)+' / '+count(stream?.reconnects)+'</dd></div><div><dt>最近账户事件</dt><dd>'+timestamp(stream?.last_event_at)+'</dd></div></dl><p>'+esc(s.mode==='paper'?'止盈与止损在本机轮询':stream?.last_error||'REST 对账持续运行')+'</p><p class="'+(market?.last_error?'uncertain':'')+'">'+esc(marketLabel)+'</p>');
  const age=(at)=>!at?'尚未同步':now<Number(at)?'时间异常':duration(now-Number(at))+' 前';
  const stale=!s.market_at||now-Number(s.market_at)>=15000||now<Number(s.market_at)||s.mode!=='paper'&&(!s.account_at||now-Number(s.account_at)>=15000||now<Number(s.account_at));
  const status=s.account_sync_failed?'账户同步异常':s.account_update_pending?'账户对账中':stale?'数据待更新':s.paused?'新增开仓已暂停':'运行中';
  $('monitor-state').textContent=limit?.cooldown_persistence_pending?'退避状态待保存':cooldown>0?'限流退避':status;
  $('monitor-state').className='monitor-state'+(limit?.cooldown_persistence_pending||cooldown>0||s.account_sync_failed||stale?' uncertain':'');
  $('monitor-freshness').textContent='行情 '+age(s.market_at)+' · '+(s.mode==='paper'?'模拟账户': '完整账户 '+age(s.account_at))+' · 异常轮次包含风控等待，累计次数在引擎重启后重置';
}
function render(){
  const s=state,c=s.config;
  auth=s.auth;renderAccess();
  if(!auth.initialized&&!setupPrompted){setupPrompted=true;showAuth();}
  $('mode').textContent={paper:'PAPER · 模拟交易',testnet:'TESTNET · 币安测试网',live:'LIVE · 币安实盘'}[s.mode];
  $('mode').className='mode'+(s.mode==='live'?' live':'');$('health').textContent=s.health;
  $('run-btn').textContent=s.paused?'启动开仓扫描 ↗':'暂停新增开仓 Ⅱ';
  renderAssets();renderMonitoring();
  $('realized').textContent=num(s.realized_net);$('realized').className=color(s.realized_net);
  $('unrealized').textContent=num(s.unrealized);$('unrealized').className=color(s.unrealized);
  $('exposure').textContent=num(s.exposure_usdt);$('exposure-cap').textContent='上限 '+num(c.max_exposure_usdt)+' USDT + USDC';
  const slots=new Set(s.positions.map(p=>p.symbol).concat(s.pending.filter(o=>o.role==='entry').map(o=>o.symbol))).size;
  $('slot-count').textContent=slots+' / '+c.max_positions;$('available').textContent=s.mode!=='paper'&&!s.account_at?'账户尚未连接':s.mode==='paper'?'模拟可用 '+num(s.available_usdt)+' USDT':s.margin_balances?.multi_assets_margin?'可用保证金 '+num(s.margin_balances.available_balance)+' USD':'可用 '+['USDT','USDC'].map(asset=>num(s.margin_balances?.assets.find(b=>b.asset===asset)?.available_balance)+' '+asset).join(' / ');
  $('strategy-label').textContent='JEV';
  $('notional-label').textContent=price(c.notional_usdt)+' 计价币';$('profit-label').textContent=price(c.profit_usdt)+' 计价币';$('stop-loss-label').textContent=stopLossLabel(c.stop_loss_usdt,'计价币');
  renderPositions();renderOrders();renderWatchlist();renderDecisions();
  const externalPositions=Object.entries(s.external_positions).map(([symbol,quantity])=>symbol+' ('+price(quantity)+')');
  const externalOrders=s.external_orders;
  $('reconciliation-note').hidden=!externalPositions.length&&!externalOrders.length;
  $('reconciliation-note').textContent='账户差异待核对 · '+[externalPositions.length?'外部持仓：'+externalPositions.join('、'):'',externalOrders.length?'外部挂单：'+externalOrders.join('、'):''].filter(Boolean).join('；')+'。新增开仓暂停，请核对账户。';
  renderBackgroundModes();
  $('update-time').textContent='SYNC '+new Date(s.updated_at).toLocaleTimeString('zh-CN',{hour12:false});
}
function renderBackgroundModes(){
  const modes=state.background_modes||[];
  $('background-modes-note').hidden=!modes.length;
  $('background-modes-note').textContent=modes.map(m=>{
    const label={paper:'模拟',testnet:'测试网',live:'实盘'}[m.mode]||m.mode;
    const status=!m.configured?'等待原账户密钥':m.account_sync_failed?'账户同步异常':m.managing?'后台风控管理中':'后台管理未启动';
    const differences=Number(m.external_positions||0)+Number(m.external_orders||0);
    return label+' · '+status+' · '+m.positions+' 个持仓 / '+m.pending_orders+' 笔待确认订单'+(differences?' / '+differences+' 项账户差异待人工核对':'');
  }).join('；')+'。后台模式新增开仓已暂停，可切回对应模式查看和处理。';
}
const actionLabel=action=>({LONG:'做多',SHORT:'做空',WAIT:'等待'}[action]||action);
const roleLabel=role=>({entry:'开仓',take_profit:'Maker 止盈',stop_loss:'止损平仓',manual_close:'手动平仓'}[role]||role);
const statusLabel=status=>({NEW:'已挂单',PARTIALLY_FILLED:'部分成交',FILLED:'已成交',SUBMITTING:'提交中',UNKNOWN:'状态待确认',SETTLING:'费用结算中',CANCELED:'已撤销',EXPIRED:'已过期',REJECTED:'已拒绝'}[status]||status);
const stopLossLabel=(value,asset)=>Number(value)>0?'最大净亏 '+price(value)+' '+asset:'未启用';
const sideClass=action=>action==='LONG'?'long':action==='SHORT'?'short':'wait';
const actionBadge=action=>'<span class="decision-badge '+sideClass(action)+'">'+esc(actionLabel(action))+'</span>';
const timestamp=value=>value?new Date(value).toLocaleString('zh-CN',{month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',second:'2-digit',hour12:false}):'—';
const confidence=value=>value==null?'未返回置信度':num(Number(value)*100,1)+'% 置信度';
const decisionReason=d=>d.reason||d.source;
function replaceRegion(id,html){
  const region=$(id);
  if(renderedRegions.get(region)===html)return;
  const open=new Set([...region.querySelectorAll('details[open][data-key]')].map(d=>d.dataset.key));
  const focused=region.contains(document.activeElement)?document.activeElement.dataset.focusKey:null;
  region.innerHTML=html;
  renderedRegions.set(region,html);
  for(const detail of region.querySelectorAll('details[data-key]'))detail.open=open.has(detail.dataset.key);
  if(focused)[...region.querySelectorAll('[data-focus-key]')].find(el=>el.dataset.focusKey===focused)?.focus({preventScroll:true});
}
function renderPositions(){
  const positions=state.position_views;
  $('position-count').textContent=positions.length;$('positions-empty').hidden=positions.length>0;
  replaceRegion('positions-body',positions.map(p=>'<tr><td><b>'+esc(p.symbol)+'</b><small>'+actionBadge(p.side)+' '+p.leverage+'× · '+Math.max(0,Math.floor((Date.now()-p.opened_at)/60000))+' 分钟</small></td><td>'+price(p.quantity)+'<small>@ '+price(p.entry_price)+'</small></td><td class="'+(p.unrealized==null?'':color(p.unrealized))+'">'+(p.unrealized==null?'—':num(p.unrealized))+' '+quoteAsset(p.symbol)+'<small>'+(p.quote_stale?'报价待更新':'目标净赚 '+price(p.target_profit)+' '+quoteAsset(p.symbol))+'</small></td><td>'+price(p.tp_price)+'<small>'+(p.tp_client_id?'GTX · 只减仓':'等待挂单')+'</small><small>止损：'+stopLossLabel(p.stop_loss_usdt,quoteAsset(p.symbol))+'</small></td><td class="status">'+(p.stop_loss_triggered?'止损处理中':Number(p.quantity)===0?'结算中':p.accounting_complete?'管理中':'费用核对中')+'</td><td><button class="text-btn" data-action="close" data-symbol="'+esc(p.symbol)+'" data-focus-key="close:'+esc(p.symbol)+'">平仓</button></td></tr>').join(''));
}
function renderOrders(){
  const orders=state.open_orders;
  $('order-count').textContent=orders.length;$('orders-empty').hidden=orders.length>0;
  replaceRegion('pending-body',orders.map(o=>'<article class="order-item"><div class="order-heading"><b>'+esc(o.symbol)+'</b><span class="order-role">'+esc(roleLabel(o.role))+'</span><span class="order-status '+(o.status==='UNKNOWN'?'uncertain':'')+'">'+esc(statusLabel(o.status))+'</span></div><div class="order-values"><span><small>'+(o.order_side==='BUY'?'买入':'卖出')+'价格</small><b>'+(o.price==null?'市价':price(o.price))+'</b></span><span><small>剩余 / 委托数量</small><b>'+price(o.remaining_quantity)+' / '+price(o.quantity)+'</b></span></div><div class="order-meta"><time>'+timestamp(o.created_at)+'</time><span title="已累计核对成交量">已核对 '+price(o.accounted_quantity)+'</span></div><details data-key="'+esc(o.client_id)+'"><summary data-focus-key="order:'+esc(o.client_id)+'">委托标识</summary><code>'+esc(o.client_id)+'</code></details></article>').join(''));
}
function renderWatchlist(){
  if(!state)return;
  const cards=state.watchlist,search=$('watchlist-search').value.trim().toUpperCase().replace('/','');
  const visible=cards.filter(c=>c.symbol.includes(search)&&(watchlistFilter==='all'||watchlistFilter==='positions'&&Boolean(c.position||c.external_position)||watchlistFilter==='orders'&&Boolean(c.orders.length||c.external_order)||watchlistFilter==='disabled'&&!c.enabled));
  $('pool-count').textContent=cards.length;$('pool-empty').hidden=cards.length>0;$('pool-no-results').hidden=!cards.length||visible.length>0;
  $('watchlist-summary').textContent='展示 '+visible.length+' / '+cards.length+' 个币对 · '+cards.filter(c=>c.enabled).length+' 个已启用';
  replaceRegion('pool-body',visible.map(coin=>{
    const asset=quoteAsset(coin.symbol),base=baseAsset(coin.symbol);
    const p=coin.position,d=coin.latest_decision,hasExternal=coin.external_position!=null||coin.external_order;
    const status=hasExternal?'账户差异':p?.stop_loss_triggered?'止损处理中':p?'持仓管理中':coin.orders.length?'委托跟踪中':!coin.enabled?'已停用':state.paused?'扫描已暂停':'等待收盘评估';
    const btn=(action,label,glyph)=>'<button type="button" class="icon-btn" data-action="'+action+'" data-symbol="'+esc(coin.symbol)+'" data-focus-key="'+esc(coin.symbol)+':'+action+'" aria-label="'+label+' '+esc(coin.symbol)+'" title="'+label+'">'+glyph+'</button>';
    const position=p?'<div class="card-position"><div class="card-section-label"><span>持仓 '+actionBadge(p.side)+' <b>'+p.leverage+'×</b></span><strong class="'+(p.unrealized==null?'':color(p.unrealized))+'">'+(p.unrealized==null?'—':num(p.unrealized))+' <small>'+asset+'</small></strong></div><div class="card-stats"><span>数量<b>'+price(p.quantity)+'</b></span><span>开仓均价<b>'+price(p.entry_price)+'</b></span><span>止盈价<b>'+price(p.tp_price)+'</b></span></div><div class="card-stop-loss">止损：'+stopLossLabel(p.stop_loss_usdt,asset)+'</div></div>':'<div class="card-flat"><span>当前空仓</span><span>单币 '+price(coin.notional_usdt)+' / 止盈 '+price(coin.profit_usdt)+' '+asset+'</span><span>止损：'+stopLossLabel(coin.stop_loss_usdt,asset)+'</span></div>';
    const orders=coin.orders.length?coin.orders.map(o=>'<div class="card-order"><span>'+esc(roleLabel(o.role))+' · '+(o.order_side==='BUY'?'买':'卖')+'</span><b>'+(o.price==null?'市价':price(o.price))+'</b><small>'+esc(statusLabel(o.status))+' · 剩余 '+price(o.remaining_quantity)+'</small></div>').join(''):'<div class="card-no-order">暂无挂单</div>';
    const decision=d?'<div class="card-decision-heading"><span>最近决策</span><time>'+timestamp(d.time)+'</time></div><div class="card-decision-result">'+actionBadge(d.action)+'<span>'+confidence(d.confidence)+'</span><small title="'+esc(d.model||d.source)+'">'+esc(d.model||'JEV')+'</small></div><p class="card-decision-source" title="'+esc(decisionReason(d))+'">'+esc(decisionReason(d))+'</p>':'<div class="card-decision-heading"><span>最近决策</span><span>尚未评估</span></div><p class="card-decision-source">等待启用扫描与新的收盘信号</p>';
    return '<article class="watch-card'+(!coin.enabled?' disabled':'')+'"><div class="watch-card-head"><div class="symbol-identity"><span class="coin-code">'+esc(base.slice(0,3))+'</span><div><h3>'+esc(base)+'<span> / '+asset+'</span></h3><small class="coin-state'+(hasExternal?' uncertain':'')+'"><i class="state-dot'+(!coin.enabled?' off':'')+'"></i>'+status+'</small></div></div><div class="coin-actions">'+btn('toggle',coin.enabled?'停用扫描':'启用扫描',coin.enabled?'Ⅱ':'▷')+btn('edit','单币配置','⚙')+btn('remove','移出自选','×')+'</div></div><div class="card-quote"><strong>'+price(coin.price)+'</strong><span class="quote-state'+(coin.quote_stale?' delayed':'')+'">'+(coin.quote_stale?'报价待更新':coin.quote?'点差 '+num((Number(coin.quote.ask)-Number(coin.quote.bid))/Number(coin.price)*10000,2)+' bps':'暂无报价')+'</span></div>'+position+'<div class="card-orders"><div class="card-section-label">挂单 <span>'+coin.orders.length+'</span></div>'+orders+'</div><div class="card-decision">'+decision+'</div>'+(hasExternal?'<p class="card-external">'+(coin.external_position!=null?'外部持仓 '+price(coin.external_position)+' ':'')+(coin.external_order?'存在外部挂单':'')+' · 待核对</p>':'')+'</article>';
  }).join(''));
}
function renderDecisions(){
  const decisions=state.recent_decisions;
  $('decision-count').textContent=decisions.length;
  replaceRegion('decisions-body',decisions.length?decisions.map(d=>{
    const probabilities=Object.entries(d.probabilities).map(([action,value])=>'<span>'+esc(actionLabel(action))+' <b>'+num(value*100,1)+'%</b></span>').join('');
    const indicators=Object.entries(d.indicators).map(([key,value])=>'<span>'+esc(key)+' <b>'+num(value,4)+'</b></span>').join('');
    return '<article class="decision-item"><div class="decision-top"><div><b>'+esc(d.symbol)+'</b>'+actionBadge(d.action)+'</div><time>'+timestamp(d.time)+'</time></div><div class="decision-summary"><span>'+confidence(d.confidence)+'</span><span>'+esc(d.model||'JEV')+'</span></div><p>'+esc(decisionReason(d))+'</p><details data-key="decision:'+state.mode+':'+d.id+'"><summary data-focus-key="decision:'+d.id+'">概率、指标与用量 <span>＋</span></summary><div class="decision-probabilities">'+(probabilities||'服务未返回概率')+'</div>'+(indicators?'<div class="decision-indicators">'+indicators+'</div>':'')+'<div class="decision-usage">'+(d.model_action?'<span>模型原始选择 '+esc(actionLabel(d.model_action))+'</span>':'')+'<span>K 线收盘 '+timestamp(d.bar_close)+'</span><span>输入 '+num(d.input_tokens,0)+' / 输出 '+num(d.output_tokens,0)+' tokens</span><span>请求 '+num(d.request_bytes,0)+' bytes</span></div></details></article>';
  }).join(''):'<div class="empty"><div class="empty-mark">⌁</div><h3>尚无决策记录</h3><p>启动扫描后，等待下一根 5 分钟 K 线收盘。</p></div>');
}

async function loadEvents(){
  const mode=state?.mode;if(!mode)return;
  if(eventsLoading?.mode===mode)return;
  const request={mode};eventsLoading=request;
  try{
    const events=await api('/api/events?exclude_decisions=true','GET',undefined,10000);if(state.mode!==mode||eventsLoading!==request)return;
    const signature=mode+JSON.stringify(events.map(e=>[e.time,e.level,e.symbol,e.message]));
    if(eventsMode===signature)return;eventsMode=signature;
    const logs=events.filter(e=>e.level!=='decision');
    replaceRegion('events-body',logs.length?logs.map((e,i)=>'<div class="event '+esc(e.level)+'"><time>'+timestamp(e.time)+'</time><span class="event-symbol">'+esc(e.symbol||'SYSTEM')+'</span><div class="event-message">'+esc(e.message)+'<details data-key="event:'+mode+':'+e.time+':'+i+'"><summary>查看详情</summary><pre>'+esc(JSON.stringify(e.detail,null,2))+'</pre></details></div></div>').join(''):'<div class="empty">尚无运行事件。</div>');
  }catch(err){if(eventsLoading===request&&state?.mode===mode)toast(err.message,true);}
  finally{if(eventsLoading===request)eventsLoading=null;}
}

async function openMarkets(){selected=new Set();$('market-search').value='';$('market-dialog').showModal();await loadMarkets();}
async function loadMarkets(){try{markets=await api('/api/markets');renderMarkets();}catch(err){toast(err.message,true);}}
function renderMarkets(){
  const search=$('market-search').value.trim().toUpperCase().replace('/',''),existing=new Set(state?.pool.map(c=>c.symbol)||[]);
  $('market-list').innerHTML=markets.filter(m=>m.symbol.includes(search)).slice(0,150).map(m=>'<label class="market-row"><input type="checkbox" data-market="'+esc(m.symbol)+'" '+(selected.has(m.symbol)?'checked ':'')+(existing.has(m.symbol)?'disabled':'')+'><b>'+esc(m.symbol)+'</b><span>24h '+num(Number(m.quote_volume)/1000000)+'M '+quoteAsset(m.symbol)+'</span><span>'+(existing.has(m.symbol)?'已添加':price(m.last_price))+'</span></label>').join('')||'<div class="empty">暂无匹配币对，请等待行情同步或更新排名。</div>';
  $('selected-count').textContent='已选择 '+selected.size+' 个币对';
}
const input=(key,label,value,extra='')=>'<label>'+label+'<input name="'+key+'" value="'+esc(value)+'" '+extra+'></label>';
const select=(key,label,value,options)=>'<label>'+label+'<select name="'+key+'" aria-label="'+esc(label)+'">'+Object.entries(options).map(([k,v])=>'<option value="'+k+'" '+(value===k?'selected':'')+'>'+v+'</option>').join('')+'</select></label>';
const section=(title,fields,note='',extra='')=>'<div class="form-section"><h3>'+title+'</h3><div class="form-grid">'+fields.join('')+'</div>'+(note?'<p class="form-note">'+note+'</p>':'')+extra+'</div>';
const numeric=(key,label,value,min,max,step='1')=>input(key,label,value,'type="number" required min="'+min+'" max="'+max+'" step="'+step+'"');
function showSettings(){
  if(!state)return;const c=state.config,s=c.strategy,j=c.jev;
  $('settings-fields').innerHTML=
    section('仓位与风控',[
      numeric('notional_usdt','单币名义金额 / 计价币',c.notional_usdt,1,100000,'0.01'),
      numeric('profit_usdt','单笔净止盈 / 计价币',c.profit_usdt,.01,100000,'0.01'),
      numeric('stop_loss_usdt','最大净亏损 / 计价币（0 关闭）',c.stop_loss_usdt??'0',0,100000,'0.01'),
      numeric('max_positions','最大同时持仓数',c.max_positions,1,50),
      numeric('max_exposure_usdt','总仓位上限 / USDT + USDC',c.max_exposure_usdt,1,10000000,'0.01'),
      numeric('leverage','杠杆',c.leverage,1,20),numeric('cooldown_bars','平仓后冷却 / K 线根数',c.cooldown_bars,0,288),
      select('allowed_direction','允许方向',c.allowed_direction,{both:'做多与做空',long:'仅做多',short:'仅做空'}),
      numeric('max_spread_bps','点差上限 / bps',c.max_spread_bps,.1,1000,'.1')
    ],'金额、净止盈和最大净亏损使用币对的计价币（USDT 或 USDC）；总敞口按两种稳定币 1:1 合并。止损为每笔交易的累计净亏损，计入已记录手续费、资金费及部分平仓盈亏，并预估剩余仓位的 Taker 平仓费用。达到阈值后确认撤销止盈单，再只减仓市价平仓。0 关闭；设置仅影响后续新开仓。服务运行时约每 5 秒检查一次，暂停扫描仍继续管理；滑点或网络延迟可能使实际亏损超过阈值。')+
    section('JEV 行情指标 · 固定 5 分钟',[
      numeric('fast','快线周期',s.fast,2,149),numeric('slow','慢线周期',s.slow,3,150),
      numeric('signal','MACD 信号周期',s.signal,2,100),numeric('period','RSI / 布林带周期',s.period,2,150),
      numeric('lower','RSI 下阈值',s.lower,0,100,'.1'),numeric('upper','RSI 上阈值',s.upper,0,100,'.1'),
      numeric('deviations','布林带标准差倍数',s.deviations,.1,10,'.1'),numeric('signal_max_age_seconds','信号有效期 / 秒',c.signal_max_age_seconds,1,120)
    ],'EMA、MACD、RSI 和布林带共同作为 JEV 的行情输入，由 JEV 选择做多、做空或等待。')+
    section('TypeSafe AI · Jev',[
      input('model','模型',j.model,'required'),numeric('min_confidence','最低置信度',j.min_confidence,0,1,'.01'),
      numeric('lookback_bars','行情回看 / 根',j.lookback_bars,100,500),numeric('forecast_bars','判断窗口 / 根',j.forecast_bars,1,288),
      numeric('timeout_seconds','请求超时 / 秒',j.timeout_seconds,1,30)
    ],(state.jev_available?'Jev 密钥已配置。':'请在「账户与连接」中配置 Jev API Key。')+' 开仓按扣除点差与手续费后的方向优势判断，允许部分指标分歧；净止盈用于退出，不要求在判断窗口内达成。默认置信度门槛为 0.55；旧配置保留原值，可填入推荐参数后保存。测试连接只验证模型列表；决策超时包含连接、上传和结果读取，建议至少 15 秒。置信度不是交易胜率；判断窗口不触发自动平仓。','<button class="quiet" type="button" id="jev-recommended">填入推荐参数：置信度 0.55 / 超时至少 15 秒</button>')+
    section('模拟费用',[
      numeric('maker_fee','Maker 费率 / 小数',c.maker_fee,0,.1,'.00001'),numeric('taker_fee','Taker 费率 / 小数',c.taker_fee,0,.1,'.00001'),
      numeric('paper_balance_usdt','初始模拟余额 / USDT',c.paper_balance_usdt,1,10000000,'.01')
    ],'模拟 USDC 合约与 USDT 钱包按 1:1 记账。修改模拟本金按差额调整余额，并保留累计收益。测试网 / 实盘开仓时读取账户费率；模拟不包含队列竞争、强平和资金费。');
  renderConnections();renderSystem();settingsTab('trading');$('settings-dialog').showModal();
}
function settingsTab(name){
  for(const [tab,panel] of [['trading','settings-form'],['connections','connections-form'],['system','system-panel']]){
    $(panel).hidden=name!==tab;$(tab+'-tab').setAttribute('aria-selected',String(name===tab));
  }
}
function renderSystem(){
  const s=state.server;
  $('server-fields').innerHTML=section('服务与会话',[
    numeric('port','控制台端口',s.port,1024,65535),
    numeric('session_minutes','管理员会话 / 分钟',s.session_minutes,5,1440)
  ],'系统配置保存到同一 SQLite。端口在服务重启后生效；会话时长用于后续登录。当前服务仅监听本机。');
  $('password-form').reset();
}
const secretInput=(name,label,configured,mask)=>input(name,label,'','type="password" autocomplete="new-password" maxlength="512" placeholder="'+esc(configured?'已保存'+(mask?' '+mask:'')+' · 留空保留':'尚未配置')+'"');
const checkInput=(name,label,checked=false)=>'<label class="check-line"><input type="checkbox" name="'+name+'" '+(checked?'checked':'')+'><span>'+label+'</span></label>';
const credentialTestControls=mode=>'<div class="credential-test-actions"><span>留空测试已保存值 · 填写后测试当前值</span><button class="quiet" type="button" data-credential-test="'+mode+'" aria-label="测试'+(mode==='jev'?'Jev 密钥':(mode==='testnet'?'测试网':'实盘')+'账户密钥')+'">测试连接 ↗</button></div><p class="credential-test-result" id="'+mode+'-test-result" role="status" aria-live="polite" aria-atomic="true" hidden></p>';
const credentialPatch=(form,mode)=>({key:form.elements[mode+'_key'].value,secret:form.elements[mode+'_secret'].value,clear:form.elements['clear_'+mode].checked});
function renderConnections(){
  const s=state.settings;if(!s)return;
  for(const mode of ['testnet','live','jev'])credentialTestRevision[mode]++;
  $('connections-fields').innerHTML=
    section('执行模式',[
      select('execution_mode','交易模式',s.mode,{paper:'模拟交易 · Paper',testnet:'币安测试网 · Testnet',live:'币安实盘 · Live'}),
      checkInput('live_enabled','启用实盘交易',s.live_enabled)
    ],'切换交易模式后，两边新增开仓均暂停，原模式的持仓与订单继续后台管理，可随时切回查看。测试网和实盘首次同步账户或检查连接时，会自动将该账户所有合约设为单向持仓；已有持仓或挂单导致设置失败时显示原因。任一模式有持仓、待确认订单或账户差异时，不能更换该模式的币安账户密钥。三个模式的币池、仓位和交易参数分别保存。')+
    section('币安测试网账户',[
      secretInput('testnet_key','测试网 API Key',s.testnet.configured,s.testnet.key_masked),
      secretInput('testnet_secret','测试网 API Secret',s.testnet.secret_configured),
      checkInput('clear_testnet','清除已保存的测试网账户')
    ],'连接币安官方测试网。API Key 与 Secret 首次填写时需要成对保存。测试仅验证账户读取，不保存密钥或调整持仓模式。',credentialTestControls('testnet'))+
    section('币安实盘账户',[
      secretInput('live_key','实盘 API Key',s.live.configured,s.live.key_masked),
      secretInput('live_secret','实盘 API Secret',s.live.secret_configured),
      checkInput('clear_live','清除已保存的实盘账户')
    ],'实盘密钥与测试网密钥分别保存。测试仅验证账户读取，不保存密钥或调整持仓模式。保存连接不会自动启动开仓。',credentialTestControls('live'))+
    section('TypeSafe AI · Jev 密钥',[
      secretInput('jev_key','Jev API Key',s.jev_configured,s.jev_key_masked),
      checkInput('clear_jev','清除已保存的 Jev 密钥')
    ],'密钥写入本机 SQLite，页面不回填原文。留空保留已保存值，勾选清除才删除。Jev 密钥可在持仓期间修改，保存后暂停新增开仓，已有仓位继续管理。测试仅验证密钥并读取可用模型，不保存输入或触发模型推理计费。',credentialTestControls('jev'));
  $('connection-result').hidden=true;
}
function showCoin(coin){
  editing=coin;const o=coin.config||{};
  $('coin-title').textContent=coin.symbol+' · 单币配置';
  $('coin-fields').innerHTML=section('覆盖配置',[
    input('notional','名义金额 / 留空继承',o.notional_usdt||'','type="number" min="1" step=".01"'),
    input('profit','净止盈 / 留空继承',o.profit_usdt||'','type="number" min=".01" step=".01"'),
    input('stop_loss','最大净亏损 / 留空继承，0 关闭',o.stop_loss_usdt??'','type="number" min="0" step=".01"'),
  ],'金额、净止盈和最大净亏损单位为 '+quoteAsset(coin.symbol)+'。止损留空继承全局设置，填 0 单独关闭；仅影响新仓位。止损由运行中的服务检查并市价平仓。决策统一使用 JEV。单币指标参数可通过 /api/pool 配置。');
  $('coin-dialog').showModal();
}
const addCoin=symbol=>{
  const normalized=symbol.trim().toUpperCase().replace('/','');
  if(state?.pool.some(c=>c.symbol===normalized))throw new Error('币对已在自选中');
  return api('/api/pool','POST',{symbol,enabled:true,config:null});
};
$('run-btn').addEventListener('click',()=>withAdmin(()=>act(async()=>{if(!state)return;await api('/api/control','POST',{running:state.paused});toast(state.paused?'已启动扫描':'已暂停新增开仓');})));
['settings-btn','config-btn'].forEach(id=>$(id).addEventListener('click',()=>withAdmin(showSettings)));
['add-btn','empty-add'].forEach(id=>$(id).addEventListener('click',()=>withAdmin(openMarkets)));
$('market-search').addEventListener('input',renderMarkets);
$('market-list').addEventListener('change',e=>{const s=e.target.dataset.market;if(!s)return;e.target.checked?selected.add(s):selected.delete(s);$('selected-count').textContent='已选择 '+selected.size+' 个币对';});
$('markets-refresh').addEventListener('click',()=>act(async()=>{await api('/api/markets/refresh','POST');await loadMarkets();toast('成交额排名已更新');}));
$('manual-add-btn').addEventListener('click',()=>act(async()=>{const s=$('market-search').value.trim();if(!s)throw new Error('请输入币对');await addCoin(s);await refresh();renderMarkets();toast('已加入自选');}));
$('batch-add-btn').addEventListener('click',()=>act(async()=>{if(!selected.size)throw new Error('请先选择币对');const b=$('batch-add-btn');b.disabled=true;try{for(const s of [...selected]){await addCoin(s);selected.delete(s);} $('market-dialog').close();toast('所选币对已加入自选');}finally{b.disabled=false;renderMarkets();}}));
$('pool-body').addEventListener('click',e=>{
  const b=e.target.closest('button[data-action]');if(!b)return;const coin=state.pool.find(c=>c.symbol===b.dataset.symbol);if(!coin)return;
  withAdmin(async()=>{
    if(b.dataset.action==='remove')await act(async()=>{await api('/api/pool/'+encodeURIComponent(coin.symbol),'DELETE');toast('已移出自选，已有持仓继续管理');});
    if(b.dataset.action==='toggle')await act(()=>api('/api/pool','POST',{...coin,enabled:!coin.enabled}));
    if(b.dataset.action==='edit')showCoin(coin);
  });
});
$('coin-form').addEventListener('submit',e=>{
  e.preventDefault();act(async()=>{const f=e.target;
    await api('/api/pool','POST',{...editing,config:{notional_usdt:f.elements.notional.value||null,profit_usdt:f.elements.profit.value||null,stop_loss_usdt:f.elements.stop_loss.value||null,strategy:editing.config?.strategy||null,decision_mode:null,allowed_direction:editing.config?.allowed_direction||null}});
    $('coin-dialog').close();toast('单币配置已保存');
  });
});
$('settings-form').addEventListener('click',e=>{
  if(e.target.id!=='jev-recommended')return;
  const f=e.currentTarget;
  f.elements.min_confidence.value='0.55';
  f.elements.timeout_seconds.value=String(Math.max(15,Number(f.elements.timeout_seconds.value)||15));
  toast('已填入推荐参数，点击保存交易配置后生效');
});
$('settings-form').addEventListener('submit',e=>{
  e.preventDefault();act(async()=>{const c=structuredClone(state.config),f=e.target;
    ['notional_usdt','profit_usdt','stop_loss_usdt','max_exposure_usdt','maker_fee','taker_fee','max_spread_bps','paper_balance_usdt'].forEach(k=>{c[k]=f.elements[k].value;});
    ['max_positions','leverage','cooldown_bars','signal_max_age_seconds'].forEach(k=>{c[k]=Number(f.elements[k].value);});
    c.decision_mode='jev';c.allowed_direction=f.elements.allowed_direction.value;
    ['fast','slow','signal','period','lower','upper','deviations'].forEach(k=>{c.strategy[k]=Number(f.elements[k].value);});
    c.jev.model=f.elements.model.value;['min_confidence','lookback_bars','forecast_bars','timeout_seconds'].forEach(k=>{c.jev[k]=Number(f.elements[k].value);});
    await api('/api/config','PUT',c);$('settings-dialog').close();toast('配置已保存，后续新开仓生效');
  });
});
$('trading-tab').addEventListener('click',()=>settingsTab('trading'));
$('connections-tab').addEventListener('click',()=>settingsTab('connections'));
$('system-tab').addEventListener('click',()=>settingsTab('system'));
$('server-form').addEventListener('submit',e=>{
  e.preventDefault();act(async()=>{
    const f=e.target;
    const result=await api('/api/server','PUT',{port:Number(f.elements.port.value),session_minutes:Number(f.elements.session_minutes.value)});
    toast(result.message);
  });
});
$('password-form').addEventListener('submit',async e=>{
  e.preventDefault();const f=e.target,b=f.querySelector('button[type="submit"]');b.disabled=true;
  try{
    auth=await api('/api/auth/password','POST',{current_password:f.elements.current_password.value,password:f.elements.password.value,confirm_password:f.elements.confirm_password.value});
    f.reset();renderAccess();toast('管理员密码已更新，其他会话已退出');await refresh();
  }catch(err){f.reset();if(err.status===401){await refresh();if(!auth.authenticated)showAuth(null,err.message);else toast(err.message,true);}else toast(err.message,true);}
  finally{b.disabled=false;}
});
$('connections-form').addEventListener('submit',e=>{
  e.preventDefault();act(async()=>{
    const f=e.target,b=$('connections-save');b.disabled=true;
    try{
      const patch={mode:f.elements.execution_mode.value,live_enabled:f.elements.live_enabled.checked,
        testnet:credentialPatch(f,'testnet'),live:credentialPatch(f,'live'),
        jev_key:f.elements.jev_key.value,clear_jev:f.elements.clear_jev.checked};
      state.settings=await api('/api/settings','PUT',patch);renderConnections();toast('账户与连接已保存到 SQLite');
    }finally{b.disabled=false;}
  });
});
$('connections-fields').addEventListener('input',e=>{
  if(['jev_key','clear_jev'].includes(e.target.name)){credentialTestRevision.jev++;$('jev-test-result').hidden=true;return;}
  const mode=e.target.name?.match(/^(testnet|live)_(key|secret)$|^clear_(testnet|live)$/);
  if(!mode)return;const account=mode[1]||mode[3];credentialTestRevision[account]++;$(account+'-test-result').hidden=true;
});
$('connections-fields').addEventListener('click',e=>{
  const b=e.target.closest('button[data-credential-test]');if(!b||b.disabled)return;
  act(async()=>{
    const mode=b.dataset.credentialTest,f=$('connections-form'),r=$(mode+'-test-result');
    const jev=mode==='jev',credentials=jev?{key:f.elements.jev_key.value,clear:f.elements.clear_jev.checked}:credentialPatch(f,mode),revision=++credentialTestRevision[mode];
    const label=jev?'Jev':mode==='testnet'?'测试网':'实盘',edited=Boolean(credentials.key.trim()||credentials.secret?.trim());
    const show=(status,message)=>{if(revision!==credentialTestRevision[mode])return;r.hidden=false;r.dataset.state=status;r.textContent=message;};
    b.disabled=true;b.textContent='正在测试…';b.setAttribute('aria-busy','true');
    try{
      if(credentials.clear)throw new Error(jev?'请先取消清除 Jev 密钥，再测试连接':'请先取消清除账户，再测试密钥');
      show('pending','正在验证'+label+(edited?'当前填写的密钥':'已保存的密钥')+'…');
      const result=await api(jev?'/api/settings/jev/test':'/api/settings/test','POST',jev?{jev_key:credentials.key,clear_jev:credentials.clear}:{mode,credentials});
      show(jev||result.can_trade?'success':'warning',result.message+(edited?' 当前填写的密钥尚未保存，请保存后生效。':''));
    }catch(err){show('error',err.message);if(err.status===401)throw err;}
    finally{b.disabled=false;b.textContent='测试连接 ↗';b.removeAttribute('aria-busy');}
  });
});
$('connection-check').addEventListener('click',()=>act(async()=>{
  const b=$('connection-check'),r=$('connection-result');b.disabled=true;r.hidden=false;r.textContent='正在检查币安连接并确认单向持仓…';
  try{const result=await api('/api/settings/check','POST');r.textContent=result.message;}
  catch(err){r.textContent=err.message;throw err;}finally{b.disabled=false;}
}));
$('positions-body').addEventListener('click',e=>{const b=e.target.closest('button[data-action="close"]');if(!b)return;withAdmin(()=>{closing=b.dataset.symbol;$('close-message').textContent='将手动平掉 '+closing+' 的剩余持仓。当前模式：'+state.mode;$('close-dialog').showModal();});});
$('confirm-close').addEventListener('click',()=>act(async()=>{const b=$('confirm-close');b.disabled=true;try{await api('/api/positions/'+encodeURIComponent(closing)+'/close','POST');$('close-dialog').close();toast('平仓请求已处理，请查看订单状态');}finally{b.disabled=false;}}));
$('journal-refresh').addEventListener('click',loadEvents);
$('decision-refresh').addEventListener('click',refresh);
$('watchlist-search').addEventListener('input',renderWatchlist);
document.querySelector('.watchlist-filters').addEventListener('click',e=>{
  const button=e.target.closest('button[data-filter]');if(!button)return;
  watchlistFilter=button.dataset.filter;
  document.querySelectorAll('[data-filter]').forEach(b=>b.setAttribute('aria-pressed',String(b===button)));
  renderWatchlist();
});
$('auth-form').addEventListener('submit',async e=>{
  e.preventDefault();const f=e.target,b=$('auth-submit'),setup=!auth.initialized;b.disabled=true;$('auth-error').hidden=true;
  try{
    const body={password:f.elements.password.value};if(setup)body.confirm_password=f.elements.confirm_password.value;
    auth=await api('/api/auth/'+(setup?'setup':'login'),'POST',body);
    const next=pendingAdmin;pendingAdmin=null;$('auth-dialog').close();f.reset();renderAccess();await refresh();
    toast(setup?'管理员初始化完成':'管理员验证通过');if(next)await next();
  }catch(err){
    if(err.status===409){auth=await api('/api/auth/status');showAuth(pendingAdmin,err.message);}
    else{$('auth-error').textContent=err.message;$('auth-error').hidden=false;f.elements.password.value='';f.elements.confirm_password.value='';}
  }finally{b.disabled=false;}
});
$('auth-dialog').addEventListener('close',()=>{$('auth-form').reset();pendingAdmin=null;});
$('guest-btn').addEventListener('click',()=>{$('auth-dialog').close();});
$('asset-visibility').addEventListener('click',()=>{
  assetsHidden=!assetsHidden;const button=$('asset-visibility'),label=assetsHidden?'显示资产金额':'隐藏资产金额';
  button.setAttribute('aria-pressed',String(assetsHidden));button.setAttribute('aria-label',label);button.title=label;
  if(state)renderAssets();
});
$('setup-btn').addEventListener('click',()=>showAuth());
$('admin-btn').addEventListener('click',async()=>{
  if(!auth.authenticated){await withAdmin(()=>{});return;}
  try{
    auth=await api('/api/auth/logout','POST');
    for(const dialog of document.querySelectorAll('dialog[open]'))dialog.close();
    renderAccess();await refresh();toast('已退出管理；交易运行状态保持不变');
  }catch(err){toast(err.message,true);}
});
setInterval(()=>{const s=Math.ceil((300000-Date.now()%300000)/1000);$('countdown').textContent=String(Math.floor(s/60)).padStart(2,'0')+':'+String(s%60).padStart(2,'0');},1000);
function renderNavigation(){
  const current=location.hash||'#overview';
  document.querySelectorAll('.rail .nav[href]').forEach(link=>{
    const active=link.getAttribute('href')===current;link.classList.toggle('active',active);
    if(active)link.setAttribute('aria-current','location');else link.removeAttribute('aria-current');
  });
}
window.addEventListener('hashchange',renderNavigation);
document.addEventListener('visibilitychange',()=>{if(document.visibilityState==='visible'){refresh();loadEvents();}});
renderNavigation();refresh();
setInterval(()=>{if(document.visibilityState==='visible')refresh();},2500);
setInterval(()=>{if(document.visibilityState==='visible')loadEvents();},15000);
