use crate::{
    config::*,
    domain::*,
    exchange::{AccountInfo, Binance, ExchangeError, ExchangeOrder},
    jev::Jev,
    store::Store,
    strategy::{self, Decision},
};
use anyhow::{Context, Result, ensure};
use futures_util::{StreamExt, stream};
use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::sync::{Mutex, RwLock};

pub struct Engine {
    pub mode: Mode,
    pub store: Store,
    pub exchange: Binance,
    pub jev: RwLock<Arc<Jev>>,
    pub inner: Mutex<Inner>,
    pub(crate) reconcile_lock: Mutex<()>,
}
pub struct EngineTasks(Vec<tokio::task::JoinHandle<()>>);
impl Drop for EngineTasks {
    fn drop(&mut self) {
        for task in &self.0 {
            task.abort();
        }
    }
}
pub struct Inner {
    pub config: Config,
    pub book: Book,
    pub paused: bool,
    pub catalog: BTreeMap<String, SymbolInfo>,
    pub quotes: BTreeMap<String, Quote>,
    pub market_at: i64,
    pub account_at: i64,
    pub available: Decimal,
    pub account_assets: Option<AccountInfo>,
    pub account_assets_at: i64,
    pub account_assets_error: Option<String>,
    pub external_positions: BTreeMap<String, Decimal>,
    pub external_orders: Vec<String>,
    pub decisions: BTreeMap<String, Decision>,
    pub revision: u64,
    pub health: String,
}
#[derive(Serialize)]
pub struct Snapshot {
    pub mode: Mode,
    pub paused: bool,
    pub health: String,
    pub market_at: i64,
    pub account_at: i64,
    pub available_usdt: Decimal,
    pub account_assets: Option<AccountInfo>,
    pub account_assets_at: i64,
    pub account_assets_error: Option<String>,
    pub config: Config,
    pub pool: Vec<PoolCoin>,
    pub positions: Vec<Position>,
    pub pending: Vec<PendingOrder>,
    pub quotes: BTreeMap<String, Quote>,
    pub decisions: BTreeMap<String, Decision>,
    pub closed_trades: Vec<ClosedTrade>,
    pub external_positions: BTreeMap<String, Decimal>,
    pub external_orders: Vec<String>,
    pub realized_net: Decimal,
    pub unrealized: Decimal,
    pub exposure_usdt: Decimal,
    pub jev_available: bool,
}
impl Inner {
    fn occupied(&self, symbol: &str) -> bool {
        self.book.positions.contains_key(symbol)
            || self.book.pending.values().any(|o| o.symbol == symbol)
            || self.external_positions.contains_key(symbol)
            || self.external_orders.iter().any(|s| s == symbol)
    }
    fn exposure(&self) -> Decimal {
        let held: Decimal = self
            .book
            .positions
            .values()
            .map(|p| {
                p.quantity
                    * self
                        .quotes
                        .get(&p.symbol)
                        .map(|q| q.mid())
                        .unwrap_or(p.entry_price)
            })
            .sum();
        let reserved: Decimal = self
            .book
            .pending
            .values()
            .filter(|o| o.role == OrderRole::Entry)
            .map(|o| {
                (o.quantity - o.accounted_quantity).max(Decimal::ZERO)
                    * self
                        .quotes
                        .get(&o.symbol)
                        .map(|q| q.mid())
                        .unwrap_or(Decimal::ZERO)
            })
            .sum();
        held + reserved
    }
    fn slots(&self) -> usize {
        let mut symbols: Vec<&str> = self.book.positions.keys().map(String::as_str).collect();
        symbols.extend(
            self.book
                .pending
                .values()
                .filter(|o| o.role == OrderRole::Entry)
                .map(|o| o.symbol.as_str()),
        );
        symbols.sort();
        symbols.dedup();
        symbols.len()
    }
}
impl Engine {
    pub fn new(
        mode: Mode,
        store: Store,
        config: Config,
        exchange: Binance,
        jev: Jev,
    ) -> Result<Arc<Self>> {
        let (config, mut book) = store.load()?.unwrap_or_else(|| {
            let book = Book {
                paper_cash: config.paper_balance_usdt,
                funding_cursor: now_ms(),
                ..Default::default()
            };
            (config, book)
        });
        config.validate()?;
        if mode == Mode::Paper {
            // An unsent paper intent has no exchange side effect. Never open it after restart.
            book.pending
                .retain(|_, o| o.role != OrderRole::Entry || o.status != "SUBMITTING");
        }
        store.save(mode, &config, &book)?;
        let available = if mode == Mode::Paper {
            book.paper_cash
        } else {
            Decimal::ZERO
        };
        let health = if mode != Mode::Paper && !exchange.configured() {
            "请在账户与连接中配置币安 API Key 和 Secret"
        } else {
            "正在同步行情"
        }
        .into();
        Ok(Arc::new(Self {
            mode,
            store,
            exchange,
            jev: RwLock::new(Arc::new(jev)),
            reconcile_lock: Mutex::new(()),
            inner: Mutex::new(Inner {
                config,
                book,
                paused: true,
                catalog: BTreeMap::new(),
                quotes: BTreeMap::new(),
                market_at: 0,
                account_at: 0,
                available,
                account_assets: None,
                account_assets_at: 0,
                account_assets_error: None,
                external_positions: BTreeMap::new(),
                external_orders: vec![],
                decisions: BTreeMap::new(),
                revision: 0,
                health,
            }),
        }))
    }
    fn save(&self, i: &Inner) -> Result<()> {
        self.store.save(self.mode, &i.config, &i.book)
    }
    pub fn event(&self, level: &str, symbol: Option<&str>, message: &str, detail: Value) {
        let e = Event {
            time: now_ms(),
            level: level.into(),
            symbol: symbol.map(str::to_string),
            message: message.into(),
            detail,
        };
        if let Err(err) = self.store.event(&e) {
            tracing::error!("event persistence failed: {err}");
        }
    }
    pub async fn snapshot(&self) -> Snapshot {
        let i = self.inner.lock().await;
        let unrealized = i
            .book
            .positions
            .values()
            .map(|p| {
                p.unrealized(
                    i.quotes
                        .get(&p.symbol)
                        .map(|q| q.mid())
                        .unwrap_or(p.entry_price),
                )
            })
            .sum();
        Snapshot {
            mode: self.mode,
            paused: i.paused,
            health: i.health.clone(),
            market_at: i.market_at,
            account_at: i.account_at,
            available_usdt: i.available,
            account_assets: if self.mode == Mode::Paper {
                Some(AccountInfo::paper(
                    i.book.paper_cash,
                    unrealized,
                    paper_available(&i),
                ))
            } else {
                i.account_assets.clone()
            },
            account_assets_at: if self.mode == Mode::Paper {
                now_ms()
            } else {
                i.account_assets_at
            },
            account_assets_error: i.account_assets_error.clone(),
            config: i.config.clone(),
            pool: i.book.pool.values().cloned().collect(),
            positions: i.book.positions.values().cloned().collect(),
            pending: i.book.pending.values().cloned().collect(),
            quotes: i.quotes.clone(),
            decisions: i.decisions.clone(),
            closed_trades: i
                .book
                .closed_trades
                .iter()
                .rev()
                .take(100)
                .cloned()
                .collect(),
            external_positions: i.external_positions.clone(),
            external_orders: i.external_orders.clone(),
            realized_net: i.book.closed_trades.iter().map(|t| t.net_profit).sum(),
            unrealized,
            exposure_usdt: i.exposure(),
            jev_available: self.jev.read().await.available(),
        }
    }
    pub async fn configure(&self, c: Config) -> Result<()> {
        c.validate()?;
        let mut i = self.inner.lock().await;
        for coin in i.book.pool.values() {
            c.for_coin(coin)?;
        }
        let mut book = i.book.clone();
        if self.mode == Mode::Paper {
            book.paper_cash += c.paper_balance_usdt - i.config.paper_balance_usdt;
        }
        self.store.save(self.mode, &c, &book)?;
        i.config = c;
        i.book = book;
        if self.mode == Mode::Paper {
            i.available = paper_available(&i);
        }
        i.revision += 1;
        Ok(())
    }
    pub async fn set_pool(&self, mut coin: PoolCoin) -> Result<()> {
        coin.symbol = normalize_symbol(&coin.symbol)?;
        let mut i = self.inner.lock().await;
        ensure!(
            i.catalog.contains_key(&coin.symbol),
            "不是可交易的 USDT 永续币对"
        );
        i.config.for_coin(&coin)?;
        ensure!(
            i.book.pool.contains_key(&coin.symbol) || i.book.pool.len() < 200,
            "币池最多 200 个候选币"
        );
        let mut book = i.book.clone();
        book.pool.insert(coin.symbol.clone(), coin);
        self.store.save(self.mode, &i.config, &book)?;
        i.book = book;
        i.revision += 1;
        Ok(())
    }
    pub async fn remove_coin(&self, symbol: &str) -> Result<()> {
        let symbol = normalize_symbol(symbol)?;
        let mut i = self.inner.lock().await;
        let mut book = i.book.clone();
        book.pool.remove(&symbol);
        self.store.save(self.mode, &i.config, &book)?;
        i.book = book;
        i.revision += 1;
        Ok(())
    }
    pub async fn set_running(&self, run: bool) -> Result<()> {
        let mut i = self.inner.lock().await;
        if run {
            ensure!(
                self.mode == Mode::Paper || self.exchange.configured(),
                "请在账户与连接中配置币安 API Key 和 Secret"
            );
            ensure!(now_ms() - i.market_at < 15_000, "行情尚未同步或已过期");
            ensure!(
                self.mode == Mode::Paper || now_ms() - i.account_at < 15_000,
                "账户尚未对账"
            );
            ensure!(
                i.external_positions.is_empty() && i.external_orders.is_empty(),
                "存在外部持仓或订单，请先处理"
            );
            for coin in i.book.pool.values().filter(|c| c.enabled) {
                let c = i.config.for_coin(coin)?;
                ensure!(
                    c.decision_mode == DecisionMode::Builtin || self.jev.read().await.available(),
                    "币种策略需要 Jev API key"
                );
            }
        }
        i.paused = !run;
        i.revision += 1;
        drop(i);
        self.event(
            "info",
            None,
            if run {
                "已启动新开仓扫描"
            } else {
                "已暂停新开仓；持仓继续管理"
            },
            json!({}),
        );
        Ok(())
    }
    pub fn spawn(self: &Arc<Self>) -> EngineTasks {
        let mut tasks = Vec::new();
        let e = self.clone();
        tasks.push(tokio::spawn(async move {
            let mut t = tokio::time::interval(Duration::from_secs(10));
            t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                t.tick().await;
                e.refresh_account_assets().await;
            }
        }));
        let e = self.clone();
        tasks.push(tokio::spawn(async move {
            let mut t = tokio::time::interval(Duration::from_secs(2));
            t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                t.tick().await;
                if let Err(err) = e.refresh_market().await {
                    e.fault("行情同步失败", err).await;
                }
            }
        }));
        let e = self.clone();
        tasks.push(tokio::spawn(async move {
            let mut t = tokio::time::interval(Duration::from_secs(900));
            t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            t.tick().await;
            loop {
                t.tick().await;
                if let Err(err) = e.exchange.sync_time().await {
                    e.fault("时间同步失败", err).await;
                }
                if let Err(err) = e.refresh_catalog().await {
                    e.fault("交易规则同步失败", err).await;
                }
            }
        }));
        let e = self.clone();
        tasks.push(tokio::spawn(async move {
            let mut t = tokio::time::interval(Duration::from_secs(5));
            t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                t.tick().await;
                if let Err(err) = e.manage_positions().await {
                    e.fault("持仓管理异常", err).await;
                }
            }
        }));
        let e = self.clone();
        tasks.push(tokio::spawn(async move {
            let mut t = tokio::time::interval(Duration::from_secs(10));
            t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                t.tick().await;
                e.scan().await;
            }
        }));
        EngineTasks(tasks)
    }
    pub(crate) async fn refresh_account_assets(&self) {
        if self.mode == Mode::Paper || !self.exchange.configured() {
            return;
        }
        let result = self.exchange.account_info().await;
        let mut i = self.inner.lock().await;
        match result {
            Ok(account) => {
                i.account_assets = Some(account);
                i.account_assets_at = now_ms();
                i.account_assets_error = None;
            }
            Err(error) => {
                i.account_assets_error = Some(error.to_string());
            }
        }
    }
    async fn fault(&self, message: &str, error: anyhow::Error) {
        self.event("error", None, message, json!({"error": error.to_string()}));
        let mut i = self.inner.lock().await;
        i.health = format!("{message}: {error}");
        i.paused = true;
        i.revision += 1;
    }
    async fn refresh_market(&self) -> Result<()> {
        if self.inner.lock().await.catalog.is_empty() {
            self.exchange.sync_time().await?;
            let catalog = self.exchange.symbols().await?;
            self.inner.lock().await.catalog = catalog;
        }
        let quotes = self.exchange.quotes().await?;
        let mut i = self.inner.lock().await;
        i.quotes = quotes;
        i.market_at = now_ms();
        if self.mode == Mode::Paper || now_ms() - i.account_at < 15_000 {
            i.health = "行情正常 · 5m 收盘决策 · 无自动止损".into();
        }
        Ok(())
    }
    pub async fn refresh_catalog(&self) -> Result<()> {
        let c = self.exchange.symbols().await?;
        self.inner.lock().await.catalog = c;
        Ok(())
    }
    async fn scan(self: &Arc<Self>) {
        let work = {
            let i = self.inner.lock().await;
            if i.paused
                || now_ms() - i.market_at > 15_000
                || self.mode != Mode::Paper && now_ms() - i.account_at > 15_000
                || i.slots() >= i.config.max_positions
                || i.exposure() >= i.config.max_exposure_usdt
            {
                return;
            }
            i.book
                .pool
                .values()
                .filter(|c| c.enabled && !i.occupied(&c.symbol))
                .cloned()
                .map(|coin| (coin, i.revision))
                .collect::<Vec<_>>()
        };
        stream::iter(work)
            .for_each_concurrent(4, |(coin, revision)| {
                let e = self.clone();
                async move {
                    if let Err(err) = e.evaluate_coin(coin.clone(), revision).await {
                        e.event("warn", Some(&coin.symbol), "本轮等待", waiting_detail(&err));
                    }
                }
            })
            .await;
    }
    async fn evaluate_coin(&self, coin: PoolCoin, revision: u64) -> Result<()> {
        let mut c = self.inner.lock().await.config.for_coin(&coin)?;
        {
            let i = self.inner.lock().await;
            let expected_close = now_ms() / BAR_MS * BAR_MS - 1;
            if i.paused
                || i.revision != revision
                || i.occupied(&coin.symbol)
                || i.slots() >= i.config.max_positions
                || now_ms() - expected_close > c.signal_max_age_seconds as i64 * 1000
                || i.book
                    .last_bar
                    .get(&coin.symbol)
                    .is_some_and(|last| *last >= expected_close)
            {
                return Ok(());
            }
        }
        let bars = self
            .exchange
            .candles(&coin.symbol, c.jev.lookback_bars + 1)
            .await?;
        strategy::validate_bars(&bars, now_ms())?;
        let bar = bars.last().context("无 K 线")?.close_time;
        ensure!(
            now_ms() - bar <= c.signal_max_age_seconds as i64 * 1000,
            "最新收盘信号已过期"
        );
        let quote = {
            let mut i = self.inner.lock().await;
            if i.paused
                || i.revision != revision
                || i.occupied(&coin.symbol)
                || i.book
                    .last_bar
                    .get(&coin.symbol)
                    .is_some_and(|last| *last >= bar)
                || i.book
                    .cooldown_until
                    .get(&coin.symbol)
                    .is_some_and(|t| *t > bar)
            {
                return Ok(());
            }
            let quote = i.quotes.get(&coin.symbol).context("无盘口")?.clone();
            ensure_entry_quote(&quote, c.max_spread_bps, now_ms())?;
            i.book.last_bar.insert(coin.symbol.clone(), bar);
            self.save(&i)?;
            quote
        };
        let registry = strategy::registry();
        let (maker_fee, taker_fee) = if self.mode == Mode::Paper {
            (c.maker_fee, c.taker_fee)
        } else {
            self.exchange.fees(&coin.symbol).await?
        };
        c.maker_fee = maker_fee;
        c.taker_fee = taker_fee;
        let builtin = registry
            .get(c.strategy.kind.as_str())
            .context("策略未注册")?
            .evaluate(&bars, &c.strategy)?;
        let decision = self
            .jev
            .read()
            .await
            .decide(&coin.symbol, &bars, &quote, &c, builtin)
            .await?;
        self.event("decision", Some(&coin.symbol), "5 分钟收盘决策",
            json!({"bar_close": bar, "config": c, "quote": quote, "candles": bars, "decision": decision}));
        self.inner
            .lock()
            .await
            .decisions
            .insert(coin.symbol.clone(), decision.clone());
        if !c.allowed_direction.allows(decision.action) {
            return Ok(());
        }
        if self.mode != Mode::Paper {
            self.exchange.leverage(&coin.symbol, c.leverage).await?;
        }
        let order = {
            let mut i = self.inner.lock().await;
            if i.paused
                || i.revision != revision
                || i.occupied(&coin.symbol)
                || !i.book.pool.get(&coin.symbol).is_some_and(|p| p.enabled)
            {
                return Ok(());
            }
            ensure!(
                now_ms() - bar <= c.signal_max_age_seconds as i64 * 1000
                    && now_ms() - i.market_at < 15_000,
                "决策已过期"
            );
            if self.mode != Mode::Paper {
                ensure!(now_ms() - i.account_at < 15_000, "账户对账过期");
            }
            ensure!(i.slots() < i.config.max_positions, "持仓名额已满");
            ensure!(
                i.exposure() + c.notional_usdt <= i.config.max_exposure_usdt,
                "总敞口额度不足"
            );
            ensure!(
                i.book.positions.keys().all(|s| i.quotes.contains_key(s))
                    && i.book
                        .pending
                        .values()
                        .filter(|o| o.role == OrderRole::Entry)
                        .all(|o| i.quotes.contains_key(&o.symbol)),
                "已有敞口缺少盘口，暂停新增仓位"
            );
            let q = i.quotes.get(&coin.symbol).context("无盘口")?;
            ensure_entry_quote(q, c.max_spread_bps, now_ms())?;
            let price = if decision.action == Action::Long {
                q.ask
            } else {
                q.bid
            };
            let rule = i.catalog.get(&coin.symbol).context("交易规则缺失")?;
            let qty = floor_step(c.notional_usdt / price, rule.market_step);
            ensure!(
                qty >= rule.market_min_qty
                    && qty <= rule.market_max_qty
                    && qty * price >= rule.min_notional,
                "配置金额不满足交易规则，未自动扩大金额"
            );
            let reserved: Decimal = i
                .book
                .pending
                .values()
                .filter(|o| o.role == OrderRole::Entry)
                .map(|o| {
                    o.quantity * i.quotes.get(&o.symbol).map(|q| q.mid()).unwrap_or(price)
                        / Decimal::from(o.leverage)
                        + o.quantity * price * o.taker_fee
                })
                .sum();
            let available = if self.mode == Mode::Paper {
                paper_available(&i)
            } else {
                i.available - reserved
            };
            ensure!(
                available >= qty * price / Decimal::from(c.leverage) + qty * price * taker_fee,
                "可用保证金不足"
            );
            i.book.sequence += 1;
            let order = PendingOrder {
                client_id: format!("gt-e-{}-{}", bar, i.book.sequence),
                symbol: coin.symbol.clone(),
                side: decision.action,
                role: OrderRole::Entry,
                quantity: qty,
                price: None,
                created_at: now_ms(),
                signal_time: bar,
                target_profit: c.profit_usdt,
                maker_fee,
                taker_fee,
                status: "SUBMITTING".into(),
                accounted_quantity: Decimal::ZERO,
                leverage: c.leverage,
            };
            i.book
                .pending
                .insert(order.client_id.clone(), order.clone());
            self.save(&i)?;
            order
        };
        if self.mode == Mode::Paper {
            self.paper_entry(order).await?;
        } else {
            self.submit_order(order).await?;
        }
        Ok(())
    }
    async fn paper_entry(&self, o: PendingOrder) -> Result<()> {
        let mut i = self.inner.lock().await;
        let q = i.quotes.get(&o.symbol).context("无盘口")?;
        let price = if o.side == Action::Long { q.ask } else { q.bid };
        let fee = o.quantity * price * o.taker_fee;
        let p = Position {
            symbol: o.symbol.clone(),
            side: o.side,
            quantity: o.quantity,
            original_quantity: o.quantity,
            entry_price: price,
            opened_at: now_ms(),
            signal_time: o.signal_time,
            target_profit: o.target_profit,
            entry_fee: fee,
            exit_fee: Decimal::ZERO,
            realized_gross: Decimal::ZERO,
            funding_cost: Decimal::ZERO,
            maker_fee: o.maker_fee,
            tp_price: None,
            tp_client_id: None,
            observed_trade_ids: vec![],
            owned_order_ids: vec![],
            accounting_complete: true,
            leverage: o.leverage,
        };
        i.book.paper_cash -= fee;
        i.book.positions.insert(o.symbol.clone(), p);
        i.book.pending.remove(&o.client_id);
        i.available = paper_available(&i);
        self.save(&i)?;
        drop(i);
        self.event(
            "order",
            Some(&o.symbol),
            "模拟开仓成交",
            json!({"quantity": o.quantity, "price": price, "fee": fee}),
        );
        Ok(())
    }
    async fn submit_order(&self, o: PendingOrder) -> Result<()> {
        match self.exchange.submit(&o).await {
            Ok(order) => self.apply_order(order).await,
            Err(e) => {
                let rejected = e
                    .downcast_ref::<ExchangeError>()
                    .is_some_and(|x| !x.uncertain);
                let mut i = self.inner.lock().await;
                if rejected {
                    i.book.pending.remove(&o.client_id);
                    if let Some(p) = i.book.positions.get_mut(&o.symbol)
                        && p.tp_client_id.as_deref() == Some(&o.client_id)
                    {
                        p.tp_client_id = None;
                        p.tp_price = None;
                    }
                } else if let Some(p) = i.book.pending.get_mut(&o.client_id) {
                    p.status = "UNKNOWN".into();
                }
                self.save(&i)?;
                drop(i);
                self.event(
                    "error",
                    Some(&o.symbol),
                    if rejected {
                        "订单被拒绝"
                    } else {
                        "订单状态未知，已锁定并等待查询"
                    },
                    json!({"client_id": o.client_id, "error": e.to_string()}),
                );
                Ok(())
            }
        }
    }
    async fn apply_order(&self, remote: ExchangeOrder) -> Result<()> {
        let pending = self
            .inner
            .lock()
            .await
            .book
            .pending
            .get(&remote.client_order_id)
            .cloned();
        let Some(o) = pending else {
            return Ok(());
        };
        ensure!(
            remote.symbol == o.symbol
                && remote.orig_qty == o.quantity
                && remote.executed_qty >= Decimal::ZERO
                && remote.executed_qty <= o.quantity,
            "订单回报与已持久化委托不一致"
        );
        let fetch = remote.executed_qty > o.accounted_quantity
            || remote.terminal() && remote.executed_qty > Decimal::ZERO;
        let fills = if fetch {
            Some(self.exchange.fills(&o.symbol, remote.order_id).await)
        } else {
            None
        };
        let mut i = self.inner.lock().await;
        let Some(current) = i.book.pending.get(&o.client_id).cloned() else {
            return Ok(());
        };
        if remote.executed_qty < current.accounted_quantity {
            return Ok(());
        }
        let delta = (remote.executed_qty - current.accounted_quantity).max(Decimal::ZERO);
        if o.role == OrderRole::Entry && delta > Decimal::ZERO {
            let avg = if remote.avg_price > Decimal::ZERO {
                remote.avg_price
            } else {
                remote.cum_quote / remote.executed_qty
            };
            let p = i
                .book
                .positions
                .entry(o.symbol.clone())
                .or_insert_with(|| Position {
                    symbol: o.symbol.clone(),
                    side: o.side,
                    quantity: Decimal::ZERO,
                    original_quantity: Decimal::ZERO,
                    entry_price: avg,
                    opened_at: o.created_at,
                    signal_time: o.signal_time,
                    target_profit: o.target_profit,
                    entry_fee: Decimal::ZERO,
                    exit_fee: Decimal::ZERO,
                    realized_gross: Decimal::ZERO,
                    funding_cost: Decimal::ZERO,
                    maker_fee: o.maker_fee,
                    tp_price: None,
                    tp_client_id: None,
                    observed_trade_ids: vec![],
                    owned_order_ids: vec![],
                    accounting_complete: false,
                    leverage: o.leverage,
                });
            p.quantity += delta;
            p.original_quantity = remote.executed_qty;
            p.entry_price = avg;
            p.entry_fee = remote.executed_qty * avg * o.taker_fee;
            p.accounting_complete = false;
            if !p.owned_order_ids.contains(&remote.order_id) {
                p.owned_order_ids.push(remote.order_id);
            }
        } else if o.role != OrderRole::Entry
            && delta > Decimal::ZERO
            && let Some(p) = i.book.positions.get_mut(&o.symbol)
        {
            p.quantity = (p.quantity - delta).max(Decimal::ZERO);
            p.accounting_complete = false;
            if !p.owned_order_ids.contains(&remote.order_id) {
                p.owned_order_ids.push(remote.order_id);
            }
        }
        let mut settled = remote.executed_qty == Decimal::ZERO;
        if let Some(Ok(fills)) = &fills
            && fills
                .iter()
                .all(|f| f.commission_asset == "USDT" && f.order_id == remote.order_id)
            && fills.iter().map(|f| f.qty).sum::<Decimal>() == remote.executed_qty
        {
            if let Some(p) = i.book.positions.get_mut(&o.symbol) {
                if o.role == OrderRole::Entry {
                    p.entry_fee = fills.iter().map(|f| f.commission).sum();
                }
                for f in fills {
                    if !p.observed_trade_ids.contains(&f.id) {
                        if o.role != OrderRole::Entry {
                            p.exit_fee += f.commission;
                            p.realized_gross += f.realized_pnl;
                        }
                        p.observed_trade_ids.push(f.id);
                    }
                }
                p.accounting_complete = true;
            }
            settled = true;
        }
        if let Some(order) = i.book.pending.get_mut(&o.client_id) {
            order.accounted_quantity = order.accounted_quantity.max(remote.executed_qty);
            order.status = if remote.terminal() && !settled {
                "SETTLING".into()
            } else {
                remote.status.clone()
            };
        }
        if remote.terminal() && settled {
            i.book.pending.remove(&o.client_id);
            if let Some(p) = i.book.positions.get_mut(&o.symbol)
                && p.tp_client_id.as_deref() == Some(&o.client_id)
            {
                p.tp_client_id = None;
                p.tp_price = None;
            }
        }
        self.save(&i)?;
        drop(i);
        if fetch && !settled {
            let mut i = self.inner.lock().await;
            i.paused = true;
            i.revision += 1;
            drop(i);
            self.event(
                "warn",
                Some(&o.symbol),
                "费用尚未核实，保留结算状态；仅支持 USDT 手续费记账",
                json!({"client_id": o.client_id}),
            );
        }
        Ok(())
    }
    async fn manage_positions(&self) -> Result<()> {
        let _guard = self.reconcile_lock.lock().await;
        if self.mode != Mode::Paper {
            if !self.exchange.configured() {
                return Ok(());
            }
            if self.inner.lock().await.account_at == 0 {
                self.exchange.preflight().await?;
            }
            let pending = self
                .inner
                .lock()
                .await
                .book
                .pending
                .values()
                .cloned()
                .collect::<Vec<_>>();
            for o in pending {
                match self.exchange.order(&o.symbol, &o.client_id).await {
                    Ok(remote) => {
                        let partial = o.role == OrderRole::Entry
                            && remote.executed_qty > Decimal::ZERO
                            && !remote.terminal();
                        self.apply_order(remote).await?;
                        if partial
                            && let Ok(cancelled) =
                                self.exchange.cancel(&o.symbol, &o.client_id).await
                        {
                            self.apply_order(cancelled).await?;
                        }
                    }
                    Err(e) => self.event(
                        "warn",
                        Some(&o.symbol),
                        "订单查询未确认，继续锁定",
                        json!({"client_id": o.client_id, "error": e.to_string()}),
                    ),
                }
            }
            self.sync_account().await?;
            self.sync_funding().await?;
        }
        self.finalize_closed().await?;
        let positions = self
            .inner
            .lock()
            .await
            .book
            .positions
            .values()
            .filter(|p| p.quantity > Decimal::ZERO)
            .cloned()
            .collect::<Vec<_>>();
        for p in positions {
            if let Err(e) = self.ensure_tp(&p.symbol).await {
                self.event(
                    "warn",
                    Some(&p.symbol),
                    "止盈管理等待",
                    json!({"error": e.to_string()}),
                );
            }
        }
        if self.mode == Mode::Paper {
            self.paper_match().await?;
            self.finalize_closed().await?;
        }
        Ok(())
    }
    async fn sync_account(&self) -> Result<()> {
        let remote = self.exchange.positions().await?;
        let orders = self.exchange.open_orders().await?;
        let available = self.exchange.available_balance().await?;
        let mut i = self.inner.lock().await;
        let mut external = BTreeMap::new();
        let mut actual = BTreeMap::new();
        for p in remote {
            ensure!(p.position_side == "BOTH", "账户出现双向持仓");
            actual.insert(p.symbol.clone(), p.position_amt);
            if p.position_amt == Decimal::ZERO {
                continue;
            }
            if let Some(local) = i.book.positions.get(&p.symbol) {
                if p.position_amt.abs() != local.quantity
                    || (p.position_amt > Decimal::ZERO) != (local.side == Action::Long)
                {
                    external.insert(p.symbol.clone(), p.position_amt);
                }
            } else if !i
                .book
                .pending
                .values()
                .any(|o| o.symbol == p.symbol && o.role == OrderRole::Entry)
            {
                external.insert(p.symbol.clone(), p.position_amt);
            }
        }
        // External/manual changes are never silently adopted or canceled.
        for p in i.book.positions.values() {
            if p.quantity > Decimal::ZERO && !actual.contains_key(&p.symbol) {
                external.insert(p.symbol.clone(), Decimal::ZERO);
            }
        }
        i.external_orders = orders
            .iter()
            .filter(|o| !i.book.pending.contains_key(&o.client_order_id))
            .map(|o| o.symbol.clone())
            .collect();
        i.external_positions = external;
        i.available = available;
        i.account_at = now_ms();
        if !i.external_positions.is_empty() || !i.external_orders.is_empty() {
            i.paused = true;
            i.health = "账户与系统记录不一致，暂停新开仓，保留订单".into();
        }
        Ok(())
    }
    async fn sync_funding(&self) -> Result<()> {
        let cursor = {
            let i = self.inner.lock().await;
            if now_ms() - i.book.funding_last_poll < 60_000 {
                return Ok(());
            }
            i.book.funding_cursor.saturating_sub(5 * 60_000)
        };
        let rows = self.exchange.funding(cursor).await?;
        ensure!(rows.len() < 1000, "资金费分页上限，需要人工对账");
        let mut i = self.inner.lock().await;
        let end = now_ms();
        for r in rows {
            let id = format!("{}:{}", r["tranId"], r["symbol"]);
            if i.book.funding_ids.contains(&id) {
                continue;
            }
            let Some(symbol) = r["symbol"].as_str() else {
                continue;
            };
            let amount = crate::exchange::decimal(&r["income"])?;
            let time = r["time"].as_i64().context("资金费时间无效")?;
            ensure!(r["asset"] == "USDT", "资金费资产不是 USDT");
            if let Some(p) = i.book.positions.get_mut(symbol)
                && p.opened_at <= time
            {
                p.funding_cost -= amount;
            }
            for t in &mut i.book.closed_trades {
                if t.symbol == symbol && t.opened_at <= time && time < t.closed_at {
                    t.net_profit += amount;
                }
            }
            i.book.funding_ids.push(id);
        }
        i.book.funding_cursor = end;
        i.book.funding_last_poll = end;
        self.save(&i)?;
        Ok(())
    }
    async fn ensure_tp(&self, symbol: &str) -> Result<()> {
        let cancel = {
            let i = self.inner.lock().await;
            let p = i.book.positions.get(symbol).context("持仓不存在")?;
            let rule = i.catalog.get(symbol).context("交易规则未就绪")?;
            let target = p.target_price(rule.tick_size)?;
            if let Some(id) = &p.tp_client_id {
                if let Some(o) = i.book.pending.get(id) {
                    if matches!(o.status.as_str(), "UNKNOWN" | "SUBMITTING" | "SETTLING") {
                        return Ok(());
                    }
                    if o.quantity - o.accounted_quantity != p.quantity
                        || o.price.is_some_and(|v| {
                            if p.side == Action::Long {
                                v < target
                            } else {
                                v > target
                            }
                        })
                    {
                        Some(o.clone())
                    } else {
                        return Ok(());
                    }
                } else {
                    None
                }
            } else {
                None
            }
        };
        if let Some(o) = cancel {
            if self.mode == Mode::Paper {
                let mut i = self.inner.lock().await;
                i.book.pending.remove(&o.client_id);
                if let Some(p) = i.book.positions.get_mut(symbol) {
                    p.tp_client_id = None;
                    p.tp_price = None;
                }
                self.save(&i)?;
            } else {
                let r = self.exchange.cancel(symbol, &o.client_id).await?;
                self.apply_order(r).await?;
            }
        }
        let order = {
            let mut i = self.inner.lock().await;
            if i.book
                .pending
                .values()
                .any(|o| o.symbol == symbol && o.role != OrderRole::Entry)
            {
                return Ok(());
            }
            let p = i.book.positions.get(symbol).context("持仓不存在")?.clone();
            if p.quantity <= Decimal::ZERO {
                return Ok(());
            }
            ensure!(
                !i.external_positions.contains_key(symbol),
                "外部持仓变更，等待对账"
            );
            let rule = i.catalog.get(symbol).context("交易规则缺失")?;
            let quote = i.quotes.get(symbol).context("盘口缺失")?;
            ensure!(now_ms() - quote.time < 15_000, "盘口过期");
            let target = p.target_price(rule.tick_size)?;
            let price = if p.side == Action::Long {
                ceil_step(target.max(quote.ask), rule.tick_size)
            } else {
                floor_step(target.min(quote.bid), rule.tick_size)
            };
            ensure!(
                p.quantity >= rule.min_qty
                    && p.quantity <= rule.max_qty
                    && floor_step(p.quantity, rule.step_size) == p.quantity,
                "剩余持仓不满足数量步长，请人工处理"
            );
            i.book.sequence += 1;
            let o = PendingOrder {
                client_id: format!("gt-t-{}-{}", now_ms(), i.book.sequence),
                symbol: symbol.into(),
                side: p.side,
                role: OrderRole::TakeProfit,
                quantity: p.quantity,
                price: Some(price),
                created_at: now_ms(),
                signal_time: p.signal_time,
                target_profit: p.target_profit,
                maker_fee: p.maker_fee,
                taker_fee: i.config.taker_fee,
                status: if self.mode == Mode::Paper {
                    "NEW"
                } else {
                    "SUBMITTING"
                }
                .into(),
                accounted_quantity: Decimal::ZERO,
                leverage: p.leverage,
            };
            let p = i.book.positions.get_mut(symbol).unwrap();
            p.tp_price = Some(price);
            p.tp_client_id = Some(o.client_id.clone());
            i.book.pending.insert(o.client_id.clone(), o.clone());
            self.save(&i)?;
            o
        };
        if self.mode != Mode::Paper {
            self.submit_order(order).await?;
        }
        Ok(())
    }
    async fn paper_match(&self) -> Result<()> {
        let mut i = self.inner.lock().await;
        let positions = i.book.positions.values().cloned().collect::<Vec<_>>();
        for p in positions {
            let Some(id) = &p.tp_client_id else {
                continue;
            };
            let Some(o) = i.book.pending.get(id).cloned() else {
                continue;
            };
            let Some(q) = i.quotes.get(&p.symbol).cloned() else {
                continue;
            };
            if q.time <= o.created_at
                || now_ms() - q.time > 15_000
                || i.book
                    .paper_match_times
                    .get(id)
                    .is_some_and(|time| *time >= q.time)
            {
                continue;
            }
            let price = o.price.context("止盈价格缺失")?;
            let crossed = if p.side == Action::Long {
                q.bid > price
            } else {
                q.ask < price
            };
            if !crossed {
                continue;
            }
            // A later quote must trade strictly THROUGH the limit; touch alone never fills.
            let rule = i.catalog.get(&p.symbol).context("交易规则缺失")?;
            let depth = if p.side == Action::Long {
                q.bid_qty
            } else {
                q.ask_qty
            };
            let qty = floor_step(depth.min(p.quantity), rule.step_size);
            if qty <= Decimal::ZERO {
                continue;
            }
            i.book.paper_match_times.insert(id.clone(), q.time);
            let fee = qty * price * p.maker_fee;
            let gross = p.side.sign() * qty * (price - p.entry_price);
            if let Some(local) = i.book.positions.get_mut(&p.symbol) {
                local.quantity -= qty;
                local.exit_fee += fee;
                local.realized_gross += gross;
            }
            i.book.paper_cash += gross - fee;
            if let Some(order) = i.book.pending.get_mut(id) {
                order.accounted_quantity += qty;
            }
            if qty == p.quantity {
                i.book.pending.remove(id);
                i.book.paper_match_times.remove(id);
                if let Some(local) = i.book.positions.get_mut(&p.symbol) {
                    local.tp_client_id = None;
                    local.tp_price = None;
                }
            }
        }
        i.available = paper_available(&i);
        self.save(&i)?;
        Ok(())
    }
    async fn finalize_closed(&self) -> Result<()> {
        let mut i = self.inner.lock().await;
        let closed = i
            .book
            .positions
            .values()
            .filter(|p| {
                p.quantity == Decimal::ZERO
                    && p.accounting_complete
                    && !i.book.pending.values().any(|o| o.symbol == p.symbol)
            })
            .cloned()
            .collect::<Vec<_>>();
        for p in closed {
            let at = now_ms();
            i.book.positions.remove(&p.symbol);
            let cooldown = (at / BAR_MS + i.config.cooldown_bars as i64 + 1) * BAR_MS - 1;
            i.book.cooldown_until.insert(p.symbol.clone(), cooldown);
            i.book.closed_trades.push(ClosedTrade {
                symbol: p.symbol.clone(),
                side: p.side,
                opened_at: p.opened_at,
                closed_at: at,
                net_profit: p.net_realized(),
                accounting_complete: p.accounting_complete,
            });
            self.event(
                "trade",
                Some(&p.symbol),
                "已完成平仓，等待新信号",
                json!({"net_profit": p.net_realized()}),
            );
        }
        self.save(&i)?;
        Ok(())
    }
    pub async fn manual_close(&self, symbol: &str) -> Result<()> {
        let symbol = normalize_symbol(symbol)?;
        let _guard = self.reconcile_lock.lock().await;
        let pending = self
            .inner
            .lock()
            .await
            .book
            .pending
            .values()
            .filter(|o| o.symbol == symbol)
            .cloned()
            .collect::<Vec<_>>();
        for o in pending {
            if self.mode == Mode::Paper {
                self.inner.lock().await.book.pending.remove(&o.client_id);
            } else {
                let r = self.exchange.cancel(&symbol, &o.client_id).await?;
                self.apply_order(r).await?;
            }
        }
        let order = {
            let mut i = self.inner.lock().await;
            ensure!(
                !i.book.pending.values().any(|o| o.symbol == symbol),
                "订单未确认撤销"
            );
            let p = i.book.positions.get(&symbol).context("持仓不存在")?.clone();
            ensure!(p.quantity > Decimal::ZERO, "持仓正在结算");
            let q = i.quotes.get(&symbol).context("盘口缺失")?.clone();
            ensure!(now_ms() - q.time < 15_000, "盘口过期");
            if self.mode == Mode::Paper {
                let price = if p.side == Action::Long { q.bid } else { q.ask };
                let gross = p.side.sign() * p.quantity * (price - p.entry_price);
                let fee = p.quantity * price * i.config.taker_fee;
                let local = i.book.positions.get_mut(&symbol).unwrap();
                local.quantity = Decimal::ZERO;
                local.realized_gross += gross;
                local.exit_fee += fee;
                local.tp_client_id = None;
                local.tp_price = None;
                i.book.paper_cash += gross - fee;
                self.save(&i)?;
                None
            } else {
                ensure!(
                    now_ms() - i.account_at < 15_000 && !i.external_positions.contains_key(&symbol),
                    "账户未对账"
                );
                i.book.sequence += 1;
                let o = PendingOrder {
                    client_id: format!("gt-c-{}-{}", now_ms(), i.book.sequence),
                    symbol: symbol.clone(),
                    side: p.side,
                    role: OrderRole::ManualClose,
                    quantity: p.quantity,
                    price: None,
                    created_at: now_ms(),
                    signal_time: p.signal_time,
                    target_profit: p.target_profit,
                    maker_fee: p.maker_fee,
                    taker_fee: i.config.taker_fee,
                    status: "SUBMITTING".into(),
                    accounted_quantity: Decimal::ZERO,
                    leverage: p.leverage,
                };
                i.book.pending.insert(o.client_id.clone(), o.clone());
                self.save(&i)?;
                Some(o)
            }
        };
        if let Some(o) = order {
            self.submit_order(o).await?;
        }
        self.finalize_closed().await?;
        Ok(())
    }
}
#[derive(Debug, Serialize)]
struct EntryQuoteError {
    reason: &'static str,
    quote: Quote,
    quote_age_ms: i64,
    max_quote_age_ms: i64,
    spread_bps: Decimal,
    max_spread_bps: Decimal,
}
impl std::fmt::Display for EntryQuoteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.reason == "stale_quote" {
            write!(
                f,
                "盘口过期（距上次更新 {} 毫秒，必须小于 {} 毫秒）",
                self.quote_age_ms, self.max_quote_age_ms
            )
        } else {
            write!(
                f,
                "点差过大（约 {} bps，上限 {} bps；买价 {}，卖价 {}）",
                self.spread_bps.round_dp(2).normalize(),
                self.max_spread_bps.normalize(),
                self.quote.bid.normalize(),
                self.quote.ask.normalize()
            )
        }
    }
}
impl std::error::Error for EntryQuoteError {}
fn ensure_entry_quote(quote: &Quote, max_spread_bps: Decimal, now: i64) -> Result<()> {
    let quote_age_ms = now - quote.time;
    let spread_bps = quote.spread_bps();
    let reason = if quote_age_ms >= 15_000 {
        "stale_quote"
    } else if spread_bps > max_spread_bps {
        "spread_exceeded"
    } else {
        return Ok(());
    };
    Err(EntryQuoteError {
        reason,
        quote: quote.clone(),
        quote_age_ms,
        max_quote_age_ms: 15_000,
        spread_bps,
        max_spread_bps,
    }
    .into())
}
fn waiting_detail(error: &anyhow::Error) -> Value {
    let mut detail = json!({"error":error.to_string()});
    if let Some(check) = error.downcast_ref::<EntryQuoteError>() {
        detail["quote_check"] = json!(check);
    }
    detail
}
fn paper_available(i: &Inner) -> Decimal {
    let floating: Decimal = i
        .book
        .positions
        .values()
        .map(|p| {
            p.unrealized(
                i.quotes
                    .get(&p.symbol)
                    .map(|q| q.mid())
                    .unwrap_or(p.entry_price),
            )
        })
        .sum();
    let margin: Decimal = i
        .book
        .positions
        .values()
        .map(|p| p.quantity * p.entry_price / Decimal::from(p.leverage))
        .sum();
    let pending: Decimal = i
        .book
        .pending
        .values()
        .filter(|o| o.role == OrderRole::Entry)
        .map(|o| {
            o.quantity
                * i.quotes
                    .get(&o.symbol)
                    .map(|q| q.mid())
                    .unwrap_or(Decimal::ZERO)
                / Decimal::from(o.leverage)
        })
        .sum();
    i.book.paper_cash + floating - margin - pending
}
pub fn normalize_symbol(raw: &str) -> Result<String> {
    let mut s = raw.trim().to_ascii_uppercase();
    if let Some(base) = s.strip_suffix("/USDT") {
        s = format!("{base}USDT");
    }
    ensure!(
        s.chars().count() >= 5
            && s.chars().count() <= 24
            && s.chars().all(char::is_alphanumeric)
            && s.ends_with("USDT"),
        "请输入 BTCUSDT 或 BTC/USDT 形式的币对"
    );
    Ok(s)
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }
    fn config() -> Config {
        Config::default()
    }
    #[test]
    fn entry_quote_checks_preserve_age_and_spread_boundaries() {
        let now = 100_000;
        let mut quote = Quote {
            bid: d("9995"),
            ask: d("10005"),
            bid_qty: Decimal::ONE,
            ask_qty: Decimal::ONE,
            time: now - 14_999,
        };
        assert_eq!(quote.spread_bps(), d("10"));
        ensure_entry_quote(&quote, d("10"), now).unwrap();
        quote.time = now - 15_000;
        let error = ensure_entry_quote(&quote, d("10"), now).unwrap_err();
        let detail = waiting_detail(&error);
        assert!(detail["error"].as_str().unwrap().contains("盘口过期"));
        assert_eq!(detail["quote_check"]["reason"], "stale_quote");
        assert_eq!(detail["quote_check"]["quote_age_ms"], 15_000);
        assert_eq!(detail["quote_check"]["max_quote_age_ms"], 15_000);
        quote.time = now;
        quote.ask = d("10006");
        let error = ensure_entry_quote(&quote, d("10"), now).unwrap_err();
        let detail = waiting_detail(&error);
        assert!(detail["error"].as_str().unwrap().contains("点差过大"));
        assert_eq!(detail["quote_check"]["reason"], "spread_exceeded");
        assert_eq!(detail["quote_check"]["quote_age_ms"], 0);
        assert_eq!(detail["quote_check"]["max_spread_bps"], "10");
    }
    #[test]
    fn fresh_fxs_quote_with_wide_spread_records_prices_and_waits() {
        let now = 100_000;
        let quote = Quote {
            bid: d("0.303500"),
            ask: d("0.335200"),
            bid_qty: d("796509.4"),
            ask_qty: d("28418200.1"),
            time: now - 483,
        };
        let error = ensure_entry_quote(&quote, d("10"), now).unwrap_err();
        let detail = waiting_detail(&error);
        assert!(detail["error"].as_str().unwrap().contains("992.64 bps"));
        assert_eq!(detail["quote_check"]["quote_age_ms"], 483);
        assert_eq!(detail["quote_check"]["quote"]["bid"], "0.303500");
        assert_eq!(detail["quote_check"]["quote"]["ask"], "0.335200");
        assert_eq!(
            detail["quote_check"]["spread_bps"],
            quote.spread_bps().to_string()
        );
        assert_eq!(detail["quote_check"]["max_spread_bps"], "10");
        let ordinary = waiting_detail(&anyhow::anyhow!("其他等待原因"));
        assert_eq!(ordinary, json!({"error":"其他等待原因"}));
    }
    fn paper() -> Arc<Engine> {
        Engine::new(
            Mode::Paper,
            Store::open(":memory:", Mode::Paper).unwrap(),
            config(),
            Binance::new(Mode::Paper).unwrap(),
            Jev::new().unwrap(),
        )
        .unwrap()
    }
    fn entry(side: Action) -> PendingOrder {
        PendingOrder {
            client_id: "test-entry".into(),
            symbol: "XUSDT".into(),
            side,
            role: OrderRole::Entry,
            quantity: Decimal::ONE,
            price: None,
            created_at: now_ms(),
            signal_time: now_ms() / BAR_MS * BAR_MS - 1,
            target_profit: d("0.5"),
            maker_fee: d("0.0002"),
            taker_fee: d("0.0005"),
            status: "SUBMITTING".into(),
            accounted_quantity: Decimal::ZERO,
            leverage: 2,
        }
    }
    async fn seed(e: &Engine) {
        let mut i = e.inner.lock().await;
        i.catalog.insert(
            "XUSDT".into(),
            SymbolInfo {
                symbol: "XUSDT".into(),
                quote_volume: d("1000000"),
                last_price: d("100"),
                tick_size: d("0.01"),
                step_size: d("0.1"),
                market_step: d("0.1"),
                min_qty: d("0.1"),
                market_min_qty: d("0.1"),
                min_notional: d("5"),
                max_qty: d("10000"),
                market_max_qty: d("10000"),
            },
        );
        i.quotes.insert(
            "XUSDT".into(),
            Quote {
                bid: d("100"),
                ask: d("100"),
                bid_qty: d("10"),
                ask_qty: d("10"),
                time: now_ms(),
            },
        );
        i.market_at = now_ms();
    }
    #[tokio::test]
    async fn paper_long_and_short_complete_partial_maker_cycle_while_paused_and_removed() {
        for side in [Action::Long, Action::Short] {
            let e = paper();
            seed(&e).await;
            e.set_pool(PoolCoin {
                symbol: "XUSDT".into(),
                enabled: true,
                config: None,
            })
            .await
            .unwrap();
            let order = entry(side);
            {
                let mut i = e.inner.lock().await;
                i.book
                    .pending
                    .insert(order.client_id.clone(), order.clone());
                assert!(i.occupied("XUSDT"));
                assert_eq!(i.slots(), 1);
            }
            e.paper_entry(order).await.unwrap();
            assert!(e.inner.lock().await.occupied("XUSDT"));
            e.ensure_tp("XUSDT").await.unwrap();
            let (id, limit, created) = {
                let i = e.inner.lock().await;
                let p = &i.book.positions["XUSDT"];
                let id = p.tp_client_id.clone().unwrap();
                (
                    id.clone(),
                    p.tp_price.unwrap(),
                    i.book.pending[&id].created_at,
                )
            };
            e.remove_coin("XUSDT").await.unwrap();
            e.set_running(false).await.unwrap();
            {
                let mut i = e.inner.lock().await;
                let quote = i.quotes.get_mut("XUSDT").unwrap();
                quote.bid = limit;
                quote.ask = limit;
                quote.time = created + 1;
            }
            e.paper_match().await.unwrap();
            assert_eq!(
                e.inner.lock().await.book.positions["XUSDT"].quantity,
                Decimal::ONE
            );
            {
                let mut i = e.inner.lock().await;
                let q = i.quotes.get_mut("XUSDT").unwrap();
                let crossed = limit + side.sign() * d("0.01");
                q.bid = crossed;
                q.ask = crossed;
                q.bid_qty = d("0.4");
                q.ask_qty = d("0.4");
                q.time = created + 2;
            }
            e.manage_positions().await.unwrap();
            {
                let i = e.inner.lock().await;
                assert_eq!(i.book.positions["XUSDT"].quantity, d("0.6"));
                assert_eq!(i.book.pending[&id].accounted_quantity, d("0.4"));
            }
            e.manage_positions().await.unwrap();
            assert_eq!(
                e.inner.lock().await.book.positions["XUSDT"].quantity,
                d("0.6"),
                "same quote must not consume depth twice"
            );
            {
                let mut i = e.inner.lock().await;
                let q = i.quotes.get_mut("XUSDT").unwrap();
                q.bid_qty = d("10");
                q.ask_qty = d("10");
                q.time = created + 3;
            }
            e.manage_positions().await.unwrap();
            let i = e.inner.lock().await;
            assert!(i.book.positions.is_empty());
            assert!(i.book.pending.is_empty());
            assert!(!i.occupied("XUSDT"));
            assert_eq!(i.book.closed_trades.len(), 1);
            assert!(i.book.closed_trades[0].net_profit >= d("0.5"));
            assert_eq!(
                i.book.paper_cash - config().paper_balance_usdt,
                i.book.closed_trades[0].net_profit
            );
            assert!(i.book.cooldown_until["XUSDT"] > now_ms());
            assert!(i.paused);
        }
    }
    #[tokio::test]
    async fn previously_evaluated_bar_skips_network_and_order_submission() {
        let e = paper();
        {
            let mut i = e.inner.lock().await;
            i.paused = false;
            i.book
                .last_bar
                .insert("XUSDT".into(), now_ms() / BAR_MS * BAR_MS - 1);
        }
        e.evaluate_coin(
            PoolCoin {
                symbol: "XUSDT".into(),
                enabled: true,
                config: None,
            },
            0,
        )
        .await
        .unwrap();
        assert!(e.inner.lock().await.book.pending.is_empty());
        assert!(e.inner.lock().await.book.positions.is_empty());
    }
    #[tokio::test]
    async fn manual_close_accounts_taker_costs_and_leaves_no_tp() {
        let e = paper();
        seed(&e).await;
        e.paper_entry(entry(Action::Long)).await.unwrap();
        e.ensure_tp("XUSDT").await.unwrap();
        {
            let mut i = e.inner.lock().await;
            let q = i.quotes.get_mut("XUSDT").unwrap();
            q.bid = d("98");
            q.ask = d("98.01");
        }
        e.manual_close("XUSDT").await.unwrap();
        let i = e.inner.lock().await;
        assert!(i.book.positions.is_empty());
        assert!(i.book.pending.is_empty());
        assert_eq!(i.book.closed_trades[0].net_profit, d("-2.099"));
    }
    #[tokio::test]
    async fn config_change_preserves_existing_margin_and_floating_losses_reduce_available() {
        let e = paper();
        seed(&e).await;
        e.paper_entry(entry(Action::Long)).await.unwrap();
        let mut c = config();
        c.leverage = 20;
        e.configure(c).await.unwrap();
        let mut i = e.inner.lock().await;
        assert_eq!(i.book.positions["XUSDT"].leverage, 2);
        assert_eq!(paper_available(&i), d("4949.95"));
        let q = i.quotes.get_mut("XUSDT").unwrap();
        q.bid = d("98");
        q.ask = d("98");
        assert_eq!(paper_available(&i), d("4947.95"));
        drop(i);
        let mut c = config();
        c.paper_balance_usdt = d("1000");
        e.configure(c).await.unwrap();
        let i = e.inner.lock().await;
        assert_eq!(i.book.paper_cash, d("999.95"));
        assert_eq!(i.available, d("947.95"));
    }
    #[tokio::test]
    async fn paper_asset_summary_includes_floating_loss_and_initial_margin() {
        let e = paper();
        seed(&e).await;
        e.paper_entry(entry(Action::Long)).await.unwrap();
        {
            let mut i = e.inner.lock().await;
            let q = i.quotes.get_mut("XUSDT").unwrap();
            q.bid = d("98");
            q.ask = d("98");
        }
        let snapshot = e.snapshot().await;
        let account = snapshot.account_assets.unwrap();
        assert_eq!(account.total_wallet_balance, d("4999.95"));
        assert_eq!(account.total_unrealized_profit, d("-2"));
        assert_eq!(account.total_margin_balance, d("4997.95"));
        assert_eq!(account.available_balance, d("4947.95"));
        assert_eq!(account.total_initial_margin, d("50"));
        assert_eq!(account.assets[0].asset, "USDT");
    }
    #[tokio::test]
    async fn unknown_entry_survives_restart_and_reconciles_without_resubmission() {
        use axum::{Json, Router, extract::State, http::StatusCode, routing::get};
        use std::sync::atomic::{AtomicUsize, Ordering};
        let submissions = Arc::new(AtomicUsize::new(0));
        async fn submit(
            State(count): State<Arc<AtomicUsize>>,
            body: String,
        ) -> (StatusCode, Json<Value>) {
            count.fetch_add(1, Ordering::SeqCst);
            assert!(body.contains("signature="));
            assert!(body.contains("newClientOrderId="));
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"code": -1007, "msg": "local simulated unknown execution"})),
            )
        }
        let remote = || json!({"symbol":"XUSDT","clientOrderId":"test-entry","orderId":42,"status":"FILLED","executedQty":"1","avgPrice":"100","origQty":"1","cumQuote":"100"});
        let response = remote();
        let app = Router::new()
            .route("/fapi/v1/order", get(move || { let v = response.clone(); async move { Json(v) } }).post(submit))
            .route("/fapi/v1/userTrades", get(|| async { Json(json!([{"id": 1, "orderId":42,"qty":"1","price":"100","commission":"0.05","commissionAsset":"USDT","realizedPnl":"0","time":1,"side":"BUY"}])) }))
            .with_state(submissions.clone());
        let (url, server) = crate::test_support::mock_server(app).await;
        let e = Engine::new(
            Mode::Testnet,
            Store::open(":memory:", Mode::Testnet).unwrap(),
            config(),
            Binance::mock(url.clone()),
            Jev::new().unwrap(),
        )
        .unwrap();
        seed(&e).await;
        let order = entry(Action::Long);
        {
            let mut i = e.inner.lock().await;
            i.book
                .pending
                .insert(order.client_id.clone(), order.clone());
            e.save(&i).unwrap();
        }
        e.submit_order(order).await.unwrap();
        let (config, book) = e.store.load().unwrap().unwrap();
        assert_eq!(book.pending["test-entry"].status, "UNKNOWN");
        let store = Store::open(":memory:", Mode::Testnet).unwrap();
        store.save(Mode::Testnet, &config, &book).unwrap();
        let restored = Engine::new(
            Mode::Testnet,
            store,
            config,
            Binance::mock(url),
            Jev::new().unwrap(),
        )
        .unwrap();
        assert!(restored.inner.lock().await.occupied("XUSDT"));
        assert!(restored.snapshot().await.paused);
        let r = restored
            .exchange
            .order("XUSDT", "test-entry")
            .await
            .unwrap();
        restored.apply_order(r.clone()).await.unwrap();
        // Duplicate notifications must neither increase position size nor charge fees twice.
        restored.apply_order(r).await.unwrap();
        let i = restored.inner.lock().await;
        assert!(i.book.pending.is_empty());
        assert_eq!(i.book.positions["XUSDT"].quantity, Decimal::ONE);
        assert_eq!(i.book.positions["XUSDT"].entry_fee, d("0.05"));
        assert!(i.book.positions["XUSDT"].accounting_complete);
        assert_eq!(submissions.load(Ordering::SeqCst), 1);
        server.abort();
    }
    #[tokio::test]
    async fn non_usdt_commission_blocks_settlement_and_new_entries() {
        use axum::{Json, Router, routing::get};
        let app = Router::new().route("/fapi/v1/userTrades", get(|| async { Json(json!([{"id": 1, "orderId":42,"qty":"1","price":"100","commission":"0.001","commissionAsset":"BNB","realizedPnl":"0","time":1,"side":"BUY"}])) }));
        let (url, server) = crate::test_support::mock_server(app).await;
        let e = Engine::new(
            Mode::Testnet,
            Store::open(":memory:", Mode::Testnet).unwrap(),
            config(),
            Binance::mock(url),
            Jev::new().unwrap(),
        )
        .unwrap();
        {
            let mut i = e.inner.lock().await;
            let o = entry(Action::Long);
            i.book.pending.insert(o.client_id.clone(), o);
            i.paused = false;
        }
        let r: ExchangeOrder = serde_json::from_value(json!({"symbol":"XUSDT","clientOrderId":"test-entry","orderId":42,"status":"FILLED","executedQty":"1","avgPrice":"100","origQty":"1","cumQuote":"100"})).unwrap();
        e.apply_order(r).await.unwrap();
        let i = e.inner.lock().await;
        assert!(i.paused);
        assert_eq!(i.book.pending["test-entry"].status, "SETTLING");
        assert!(!i.book.positions["XUSDT"].accounting_complete);
        assert!(i.occupied("XUSDT"));
        server.abort();
    }
    #[tokio::test]
    async fn deletion_and_pause_preserve_pending_unknown_orders() {
        let e = Engine::new(
            Mode::Paper,
            Store::open(":memory:", Mode::Paper).unwrap(),
            config(),
            Binance::new(Mode::Paper).unwrap(),
            Jev::new().unwrap(),
        )
        .unwrap();
        let mut i = e.inner.lock().await;
        let coin = PoolCoin {
            symbol: "BTCUSDT".into(),
            enabled: true,
            config: None,
        };
        i.book.pool.insert(coin.symbol.clone(), coin);
        let o = PendingOrder {
            client_id: "unknown".into(),
            symbol: "BTCUSDT".into(),
            side: Action::Long,
            role: OrderRole::Entry,
            quantity: Decimal::ONE,
            price: None,
            created_at: 0,
            signal_time: 0,
            target_profit: Decimal::ONE,
            maker_fee: Decimal::ZERO,
            taker_fee: Decimal::ZERO,
            status: "UNKNOWN".into(),
            accounted_quantity: Decimal::ZERO,
            leverage: 2,
        };
        i.book.pending.insert(o.client_id.clone(), o);
        assert!(i.occupied("BTCUSDT"));
        drop(i);
        e.remove_coin("BTCUSDT").await.unwrap();
        e.set_running(false).await.unwrap();
        assert!(e.inner.lock().await.book.pending.contains_key("unknown"));
    }
    #[test]
    fn normalizes_symbols_but_rejects_paths() {
        assert_eq!(normalize_symbol(" btc/usdt ").unwrap(), "BTCUSDT");
        assert_eq!(normalize_symbol("龙虾USDT").unwrap(), "龙虾USDT");
        assert!(normalize_symbol("../USDT").is_err());
    }
}
