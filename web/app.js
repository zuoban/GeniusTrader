'use strict';
const $=id=>document.getElementById(id);
const names={ema_cross:'EMA CROSS',macd:'MACD',bollinger:'BOLLINGER',rsi:'RSI'};
const modeNames={builtin:'内置策略',jev:'Jev',hybrid:'组合确认'};
let state=null,markets=[],selected=new Set(),editing=null,closing=null,busy=false;
let auth={initialized:false,authenticated:false},pendingAdmin=null,setupPrompted=false;
let assetsHidden=false;
const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const num=(v,d=2)=>Number(v||0).toLocaleString('en-US',{minimumFractionDigits:d,maximumFractionDigits:d});
const price=v=>v==null?'—':Number(v).toLocaleString('en-US',{maximumFractionDigits:8});
const color=v=>Number(v)>0?'positive':Number(v)<0?'negative':'';
function toast(message,error=false){$('toast').textContent=message;$('toast').className='toast'+(error?' error':'');$('toast').hidden=false;clearTimeout(toast.timer);toast.timer=setTimeout(()=>{$('toast').hidden=true;},6000);}
async function api(path,method='GET',body){
  const headers={'x-genius-request':'1'};if(body!==undefined)headers['content-type']='application/json';
  const r=await fetch(path,{method,headers,credentials:'same-origin',body:body===undefined?undefined:JSON.stringify(body)});
  const data=await r.json().catch(()=>({error:'服务返回格式错误'}));
  if(!r.ok){const error=new Error(data.error||'请求失败 '+r.status);error.status=r.status;throw error;}return data;
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
  if(busy)return;busy=true;try{state=await api('/api/state');render();$('connection-label').textContent='● 本地控制台已连接';}catch(err){$('connection-label').textContent='○ 控制台连接中断';$('asset-sync').textContent='本地连接中断 · 数据未更新';$('asset-sync').className='subtle sync-delayed';}finally{busy=false;}
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
  $('asset-legend').innerHTML=assets.length?assets.map((x,i)=>'<li><div><i class="asset-dot" aria-hidden="true"></i><b>'+esc(x.asset)+'</b><span class="asset-share">'+shares[i]+'</span></div><small>'+native(x.wallet_balance)+' '+esc(x.asset)+'</small></li>').join(''):'<li class="asset-empty">'+(a?'暂无非零余额资产':'连接账户后展示资产分布')+'</li>';
  $('asset-legend').querySelectorAll('.asset-dot').forEach((dot,i)=>{dot.style.background=swatches.get(assets[i].asset);});
  $('asset-distribution-note').textContent=!valued?'估值暂不可用 · 展示原币余额，占比等待更新':s.mode==='paper'?'模拟 USDT 钱包 · 按 1 USDT ≈ 1 USD 展示':'按正钱包余额的 USD 估值计算占比';
  $('asset-details-body').innerHTML=assets.length?assets.map(x=>'<tr><td>'+esc(x.asset)+'</td><td>'+native(x.wallet_balance)+'</td><td class="'+(assetsHidden?'':color(x.unrealized_profit))+'">'+native(x.unrealized_profit)+'</td><td>'+native(x.margin_balance)+'</td><td>'+(x.wallet_value_usd==null?'—':money(x.wallet_value_usd,2))+'</td></tr>').join(''):'<tr><td colspan="5">'+(a?'暂无非零余额资产':'等待账户同步')+'</td></tr>';
}
function render(){
  const s=state,c=s.config;
  auth=s.auth;renderAccess();
  if(!auth.initialized&&!setupPrompted){setupPrompted=true;showAuth();}
  $('mode').textContent={paper:'PAPER · 模拟交易',testnet:'TESTNET · 币安测试网',live:'LIVE · 币安实盘'}[s.mode];
  $('mode').className='mode'+(s.mode==='live'?' live':'');$('health').textContent=s.health;
  $('run-btn').textContent=s.paused?'启动开仓扫描 ↗':'暂停新增开仓 Ⅱ';
  renderAssets();
  $('realized').textContent=num(s.realized_net);$('realized').className=color(s.realized_net);
  $('unrealized').textContent=num(s.unrealized);$('unrealized').className=color(s.unrealized);
  $('exposure').textContent=num(s.exposure_usdt);$('exposure-cap').textContent='上限 '+num(c.max_exposure_usdt)+' USDT';
  const slots=new Set(s.positions.map(p=>p.symbol).concat(s.pending.filter(o=>o.role==='entry').map(o=>o.symbol))).size;
  $('slot-count').textContent=slots+' / '+c.max_positions;$('available').textContent=s.mode!=='paper'&&!s.account_at?'账户尚未连接':'可用保证金 '+num(s.available_usdt)+' USDT';
  $('strategy-label').textContent=names[c.strategy.kind]+' / '+modeNames[c.decision_mode];
  $('notional-label').textContent=price(c.notional_usdt)+' USDT';$('profit-label').textContent=price(c.profit_usdt)+' USDT';
  $('position-count').textContent=s.positions.length;$('positions-empty').hidden=s.positions.length>0;
  $('positions-body').innerHTML=s.positions.map(p=>{
    const q=s.quotes[p.symbol],mid=q?(Number(q.bid)+Number(q.ask))/2:Number(p.entry_price);
    const pnl=(p.side==='LONG'?1:-1)*Number(p.quantity)*(mid-Number(p.entry_price));
    return '<tr><td>'+esc(p.symbol)+'<small><span class="side '+(p.side==='SHORT'?'short':'')+'">'+p.side+'</span> '+Math.floor((Date.now()-p.opened_at)/60000)+' 分钟</small></td><td>'+price(p.quantity)+'<small>@ '+price(p.entry_price)+'</small></td><td class="'+color(pnl)+'">'+num(pnl)+'<small>目标净赚 '+price(p.target_profit)+'</small></td><td>'+price(p.tp_price)+'<small>'+(p.tp_client_id?'GTX · REDUCE ONLY':'等待挂单')+'</small></td><td class="status">'+(Number(p.quantity)===0?'结算中':p.accounting_complete?'管理中':'费用核对中')+'</td><td><button class="text-btn" data-action="close" data-symbol="'+esc(p.symbol)+'">平仓</button></td></tr>';
  }).join('');
  $('pending-panel').hidden=!s.pending.length;
  $('pending-body').innerHTML=s.pending.map(o=>'<div class="pending-row"><span>'+esc(o.symbol)+' · '+esc(o.role)+'</span><span>'+esc(o.status)+'</span><span>'+price(o.quantity)+' / '+price(o.price)+'</span></div>').join('');
  $('pool-count').textContent=s.pool.length;$('pool-empty').hidden=s.pool.length>0;
  $('pool-body').innerHTML=s.pool.map(coin=>{
    const p=s.positions.find(p=>p.symbol===coin.symbol),pending=s.pending.some(o=>o.symbol===coin.symbol&&o.role==='entry'),d=s.decisions[coin.symbol];
    const status=p?'持仓 · '+p.side:pending?'开仓跟踪中':!coin.enabled?'已停用':d?'最近 '+d.action:'等待收盘信号';
    const kind=coin.config?.strategy?.kind||c.strategy.kind;
    const btn=(action,label,glyph)=>'<button class="icon-btn" data-action="'+action+'" data-symbol="'+esc(coin.symbol)+'" aria-label="'+label+' '+esc(coin.symbol)+'">'+glyph+'</button>';
    return '<div class="coin"><span class="coin-code">'+esc(coin.symbol.replace('USDT','').slice(0,3))+'</span><div class="coin-info"><b>'+esc(coin.symbol)+'</b><small>'+esc(status)+' / '+names[kind]+'</small></div><div class="coin-actions">'+btn('toggle',coin.enabled?'停用':'启用',coin.enabled?'Ⅱ':'▷')+btn('edit','配置','⚙')+btn('remove','移出币池','×')+'</div></div>';
  }).join('');
  $('update-time').textContent='SYNC '+new Date().toLocaleTimeString('zh-CN',{hour12:false});
}
async function loadEvents(){
  try{
    const events=await api('/api/events');
    $('events-body').innerHTML=events.length?events.map(e=>'<div class="event '+esc(e.level)+'"><time>'+new Date(e.time).toLocaleTimeString('zh-CN',{hour12:false})+'</time><span class="event-symbol">'+esc(e.symbol||'SYSTEM')+'</span><div class="event-message">'+esc(e.message)+'<details><summary>查看输入与结果</summary><pre>'+esc(JSON.stringify(e.detail,null,2))+'</pre></details></div></div>').join(''):'<div class="empty">尚无决策记录。</div>';
  }catch(err){toast(err.message,true);}
}
async function openMarkets(){selected=new Set();$('market-search').value='';$('market-dialog').showModal();await loadMarkets();}
async function loadMarkets(){try{markets=await api('/api/markets');renderMarkets();}catch(err){toast(err.message,true);}}
function renderMarkets(){
  const search=$('market-search').value.trim().toUpperCase().replace('/',''),existing=new Set(state?.pool.map(c=>c.symbol)||[]);
  $('market-list').innerHTML=markets.filter(m=>m.symbol.includes(search)).slice(0,150).map(m=>'<label class="market-row"><input type="checkbox" data-market="'+esc(m.symbol)+'" '+(selected.has(m.symbol)?'checked ':'')+(existing.has(m.symbol)?'disabled':'')+'><b>'+esc(m.symbol)+'</b><span>24h '+num(Number(m.quote_volume)/1000000)+'M USDT</span><span>'+(existing.has(m.symbol)?'已添加':price(m.last_price))+'</span></label>').join('')||'<div class="empty">暂无匹配币对，请等待行情同步或更新排名。</div>';
  $('selected-count').textContent='已选择 '+selected.size+' 个币对';
}
const input=(key,label,value,extra='')=>'<label>'+label+'<input name="'+key+'" value="'+esc(value)+'" '+extra+'></label>';
const select=(key,label,value,options)=>'<label>'+label+'<select name="'+key+'" aria-label="'+esc(label)+'">'+Object.entries(options).map(([k,v])=>'<option value="'+k+'" '+(value===k?'selected':'')+'>'+v+'</option>').join('')+'</select></label>';
const section=(title,fields,note='')=>'<div class="form-section"><h3>'+title+'</h3><div class="form-grid">'+fields.join('')+'</div>'+(note?'<p class="form-note">'+note+'</p>':'')+'</div>';
const numeric=(key,label,value,min,max,step='1')=>input(key,label,value,'type="number" required min="'+min+'" max="'+max+'" step="'+step+'"');
function showSettings(){
  if(!state)return;const c=state.config,s=c.strategy,j=c.jev;
  $('settings-fields').innerHTML=
    section('仓位与止盈',[
      numeric('notional_usdt','单币名义金额 / USDT',c.notional_usdt,1,100000,'0.01'),
      numeric('profit_usdt','单笔净止盈 / USDT',c.profit_usdt,.01,100000,'0.01'),
      numeric('max_positions','最大同时持仓数',c.max_positions,1,50),
      numeric('max_exposure_usdt','总仓位上限 / USDT',c.max_exposure_usdt,1,10000000,'0.01'),
      numeric('leverage','杠杆',c.leverage,1,20),numeric('cooldown_bars','平仓后冷却 / K 线根数',c.cooldown_bars,0,288),
      select('allowed_direction','允许方向',c.allowed_direction,{both:'做多与做空',long:'仅做多',short:'仅做空'}),
      numeric('max_spread_bps','点差上限 / bps',c.max_spread_bps,.1,1000,'.1')
    ],'名义金额不再乘杠杆。退出采用 Maker 止盈或手动平仓。')+
    section('决策引擎 · 固定 5 分钟',[
      select('decision_mode','决策模式',c.decision_mode,modeNames),select('kind','内置策略',s.kind,{ema_cross:'EMA 双均线交叉',macd:'MACD 交叉',bollinger:'布林带回归',rsi:'RSI 回归'}),
      numeric('fast','快线周期',s.fast,2,149),numeric('slow','慢线周期',s.slow,3,150),
      numeric('signal','MACD 信号周期',s.signal,2,100),numeric('period','RSI / 布林带周期',s.period,2,150),
      numeric('lower','RSI 下阈值',s.lower,0,100,'.1'),numeric('upper','RSI 上阈值',s.upper,0,100,'.1'),
      numeric('deviations','布林带标准差倍数',s.deviations,.1,10,'.1'),numeric('signal_max_age_seconds','信号有效期 / 秒',c.signal_max_age_seconds,1,120)
    ])+
    section('TypeSafe AI · Jev',[
      input('model','模型',j.model,'required'),numeric('min_confidence','最低置信度',j.min_confidence,0,1,'.01'),
      numeric('lookback_bars','行情回看 / 根',j.lookback_bars,100,500),numeric('forecast_bars','判断窗口 / 根',j.forecast_bars,1,288),
      numeric('timeout_seconds','请求超时 / 秒',j.timeout_seconds,1,30)
    ],(state.jev_available?'Jev 密钥已配置。':'请在「账户与连接」中配置 Jev API Key。')+' 置信度不是交易胜率；判断窗口不触发自动平仓。')+
    section('模拟费用',[
      numeric('maker_fee','Maker 费率 / 小数',c.maker_fee,0,.1,'.00001'),numeric('taker_fee','Taker 费率 / 小数',c.taker_fee,0,.1,'.00001'),
      numeric('paper_balance_usdt','初始模拟余额 / USDT',c.paper_balance_usdt,1,10000000,'.01')
    ],'修改模拟本金按差额调整余额，并保留累计收益。测试网 / 实盘开仓时读取账户费率；模拟不包含队列竞争、强平和资金费。');
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
function renderConnections(){
  const s=state.settings;if(!s)return;
  $('connections-fields').innerHTML=
    section('执行模式',[
      select('execution_mode','交易模式',s.mode,{paper:'模拟交易 · Paper',testnet:'币安测试网 · Testnet',live:'币安实盘 · Live'}),
      checkInput('live_enabled','启用实盘交易',s.live_enabled)
    ],'切换模式或当前币安账户后会暂停新增开仓。测试网和实盘首次同步账户或检查连接时，会自动将该账户所有合约设为单向持仓；已有持仓或挂单导致切换失败时显示原因。有持仓、待确认订单或账户差异时不能更换当前币安账户；可提前保存其他模式的账户。三个模式的币池、仓位和交易参数分别保存。')+
    section('币安测试网账户',[
      secretInput('testnet_key','测试网 API Key',s.testnet.configured,s.testnet.key_masked),
      secretInput('testnet_secret','测试网 API Secret',s.testnet.secret_configured),
      checkInput('clear_testnet','清除已保存的测试网账户')
    ],'连接币安官方测试网。API Key 与 Secret 首次填写时需要成对保存。')+
    section('币安实盘账户',[
      secretInput('live_key','实盘 API Key',s.live.configured,s.live.key_masked),
      secretInput('live_secret','实盘 API Secret',s.live.secret_configured),
      checkInput('clear_live','清除已保存的实盘账户')
    ],'实盘密钥与测试网密钥分别保存。保存连接不会自动启动开仓。')+
    section('TypeSafe AI · Jev 密钥',[
      secretInput('jev_key','Jev API Key',s.jev_configured,s.jev_key_masked),
      checkInput('clear_jev','清除已保存的 Jev 密钥')
    ],'密钥写入本机 SQLite，页面不回填原文。留空保留已保存值，勾选清除才删除。Jev 密钥可在持仓期间修改，保存后暂停新增开仓，已有仓位继续管理。连接检查仅验证币安；Jev 在决策时验证，不在此触发模型计费。');
  $('connection-result').hidden=true;
}
function showCoin(coin){
  editing=coin;const o=coin.config||{};
  $('coin-title').textContent=coin.symbol+' · 单币配置';
  $('coin-fields').innerHTML=section('覆盖配置',[
    input('notional','名义金额 / 留空继承',o.notional_usdt||'','type="number" min="1" step=".01"'),
    input('profit','净止盈 / 留空继承',o.profit_usdt||'','type="number" min=".01" step=".01"'),
    select('strategy','策略',o.strategy?.kind||'',{'':'继承全局',ema_cross:'EMA 交叉',macd:'MACD',bollinger:'布林带回归',rsi:'RSI'}),
    select('mode','决策模式',o.decision_mode||'',{'':'继承全局',...modeNames})
  ],'完整单币策略参数可通过 /api/pool 配置。');
  $('coin-dialog').showModal();
}
const addCoin=symbol=>{
  const normalized=symbol.trim().toUpperCase().replace('/','');
  if(state?.pool.some(c=>c.symbol===normalized))throw new Error('币对已在币池中');
  return api('/api/pool','POST',{symbol,enabled:true,config:null});
};
$('run-btn').addEventListener('click',()=>withAdmin(()=>act(async()=>{if(!state)return;await api('/api/control','POST',{running:state.paused});toast(state.paused?'已启动扫描':'已暂停新增开仓');})));
['settings-btn','config-btn'].forEach(id=>$(id).addEventListener('click',()=>withAdmin(showSettings)));
['add-btn','empty-add'].forEach(id=>$(id).addEventListener('click',()=>withAdmin(openMarkets)));
$('market-search').addEventListener('input',renderMarkets);
$('market-list').addEventListener('change',e=>{const s=e.target.dataset.market;if(!s)return;e.target.checked?selected.add(s):selected.delete(s);$('selected-count').textContent='已选择 '+selected.size+' 个币对';});
$('markets-refresh').addEventListener('click',()=>act(async()=>{await api('/api/markets/refresh','POST');await loadMarkets();toast('成交额排名已更新');}));
$('manual-add-btn').addEventListener('click',()=>act(async()=>{const s=$('market-search').value.trim();if(!s)throw new Error('请输入币对');await addCoin(s);await refresh();renderMarkets();toast('已加入币池');}));
$('batch-add-btn').addEventListener('click',()=>act(async()=>{if(!selected.size)throw new Error('请先选择币对');const b=$('batch-add-btn');b.disabled=true;try{for(const s of [...selected]){await addCoin(s);selected.delete(s);} $('market-dialog').close();toast('所选币对已加入币池');}finally{b.disabled=false;renderMarkets();}}));
$('pool-body').addEventListener('click',e=>{
  const b=e.target.closest('button[data-action]');if(!b)return;const coin=state.pool.find(c=>c.symbol===b.dataset.symbol);if(!coin)return;
  withAdmin(async()=>{
    if(b.dataset.action==='remove')await act(async()=>{await api('/api/pool/'+encodeURIComponent(coin.symbol),'DELETE');toast('已移出币池，已有持仓继续管理');});
    if(b.dataset.action==='toggle')await act(()=>api('/api/pool','POST',{...coin,enabled:!coin.enabled}));
    if(b.dataset.action==='edit')showCoin(coin);
  });
});
$('coin-form').addEventListener('submit',e=>{
  e.preventDefault();act(async()=>{const f=e.target,k=f.elements.strategy.value;
    const strategy=k?(editing.config?.strategy?.kind===k?editing.config.strategy:{...state.config.strategy,kind:k,...(k==='macd'?{fast:12,slow:26}:k==='bollinger'?{period:20}:{})}):null;
    await api('/api/pool','POST',{...editing,config:{notional_usdt:f.elements.notional.value||null,profit_usdt:f.elements.profit.value||null,strategy,decision_mode:f.elements.mode.value||null,allowed_direction:editing.config?.allowed_direction||null}});
    $('coin-dialog').close();toast('单币配置已保存');
  });
});
$('settings-form').addEventListener('submit',e=>{
  e.preventDefault();act(async()=>{const c=structuredClone(state.config),f=e.target;
    ['notional_usdt','profit_usdt','max_exposure_usdt','maker_fee','taker_fee','max_spread_bps','paper_balance_usdt'].forEach(k=>{c[k]=f.elements[k].value;});
    ['max_positions','leverage','cooldown_bars','signal_max_age_seconds'].forEach(k=>{c[k]=Number(f.elements[k].value);});
    ['decision_mode','allowed_direction'].forEach(k=>{c[k]=f.elements[k].value;});
    c.strategy.kind=f.elements.kind.value;['fast','slow','signal','period','lower','upper','deviations'].forEach(k=>{c.strategy[k]=Number(f.elements[k].value);});
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
        testnet:{key:f.elements.testnet_key.value,secret:f.elements.testnet_secret.value,clear:f.elements.clear_testnet.checked},
        live:{key:f.elements.live_key.value,secret:f.elements.live_secret.value,clear:f.elements.clear_live.checked},
        jev_key:f.elements.jev_key.value,clear_jev:f.elements.clear_jev.checked};
      state.settings=await api('/api/settings','PUT',patch);renderConnections();toast('账户与连接已保存到 SQLite');
    }finally{b.disabled=false;}
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
refresh();loadEvents();setInterval(refresh,2500);setInterval(loadEvents,15000);
