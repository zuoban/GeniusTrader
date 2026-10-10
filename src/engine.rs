use crate::{
    config::*,
    domain::*,
    exchange::{
        AccountInfo, Binance, ExchangeError, ExchangeOrder, Fill, MarginBalances, RateLimitSnapshot,
    },
    jev::Jev,
    market_stream::MarketStreamStatus,
    monitoring::{POSITION_INTERVAL, RuntimeMetrics, SCAN_INTERVAL},
    store::Store,
    strategy::{self, Decision},
    user_stream::{StreamStatus, UserEvent},
};
use anyhow::{Context, Result, ensure};
use futures_util::{StreamExt, stream};
use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, Notify, RwLock};

pub struct Engine {
    pub mode: Mode,
    pub store: Store,
    pub exchange: Binance,
    pub jev: RwLock<Arc<Jev>>,
    pub inner: Mutex<Inner>,
    runtime: Mutex<RuntimeMetrics>,
    pub(crate) reconcile_lock: Mutex<()>,
    pub(crate) user_stream: Mutex<StreamStatus>,
    pub(crate) account_notify: Notify,
    pub(crate) market_stream: Mutex<MarketStreamStatus>,
}
pub struct EngineTasks(Vec<tokio::task::JoinHandle<()>>);
#[derive(Clone, Copy)]
enum ExitCheck {
    Account,
    Symbol,
}
struct ExitConfirmation {
    quantity: Decimal,
    side: Action,
    started: Instant,
}
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
    pub account_sync_failed: bool,
    pub account_update_pending: bool,
    pub funding_sync_pending: bool,
    funding_sync_error: Option<String>,
    account_generation: u64,
    pub available: Decimal,
    realized_net: Decimal,
    pub margin_balances: Option<MarginBalances>,
    pub account_assets: Option<AccountInfo>,
    pub account_assets_at: i64,
    pub account_assets_error: Option<String>,
    pub external_positions: BTreeMap<String, Decimal>,
    pub external_orders: Vec<String>,
    pub decisions: BTreeMap<String, Decision>,
    pub revision: u64,
    pub health: String,
    persistence_pending: bool,
    scan_cursor: Option<String>,
    market_retry_at: Option<Instant>,
}
#[derive(Serialize)]
pub struct Snapshot {
    pub mode: Mode,
    pub paused: bool,
    pub health: String,
    pub market_at: i64,
    pub account_at: i64,
    pub account_sync_failed: bool,
    pub account_update_pending: bool,
    pub funding_sync_pending: bool,
    pub entry_data_pending: bool,
    pub entry_blockers: Vec<String>,
    pub available_usdt: Decimal,
    pub margin_balances: Option<MarginBalances>,
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
    pub runtime: RuntimeMetrics,
    pub exchange_rate_limit: RateLimitSnapshot,
    pub user_stream: StreamStatus,
    pub market_stream: MarketStreamStatus,
}
impl Inner {
    fn wants_market(&self, symbol: &str) -> bool {
        self.book.pool.contains_key(symbol)
            || self.book.positions.contains_key(symbol)
            || self.book.pending.values().any(|o| o.symbol == symbol)
    }
    fn market_symbols(&self) -> BTreeSet<String> {
        self.book
            .pool
            .keys()
            .chain(self.book.positions.keys())
            .chain(self.book.pending.values().map(|o| &o.symbol))
            .cloned()
            .collect()
    }
    fn market_health(&mut self, mode: Mode) {
        let symbols = self.market_symbols();
        self.quotes.retain(|symbol, _| symbols.contains(symbol));
        self.market_at = if symbols.is_empty() {
            now_ms()
        } else {
            symbols
                .iter()
                .map(|s| self.quotes.get(s).map_or(0, |q| q.time))
                .min()
                .unwrap_or(0)
        };
        if self.entry_data_ready()
            && (0..15_000).contains(&(now_ms() - self.market_at))
            && (mode == Mode::Paper
                || (!self.account_sync_failed
                    && !self.account_update_pending
                    && (0..15_000).contains(&(now_ms() - self.account_at))))
        {
            self.health = "行情正常 · 5m 收盘决策 · 持仓风控管理中".into();
        }
    }
    fn scan_allowed(&self, mode: Mode, now: i64) -> bool {
        !self.paused
            && self.entry_data_ready()
            && (0..15_000).contains(&(now - self.market_at))
            && (mode == Mode::Paper
                || (!self.account_sync_failed
                    && !self.account_update_pending
                    && (0..15_000).contains(&(now - self.account_at))))
            && self.slots() < self.config.max_positions
            && self.exposure() < self.config.max_exposure_usdt
    }
    fn entry_data_ready(&self) -> bool {
        !self.funding_sync_pending
            && self.book.positions.values().all(|p| p.accounting_complete)
            && self
                .book
                .pending
                .values()
                .all(|o| o.status != "SETTLING" && o.settlement_issue.is_none())
    }
    fn entry_blockers(&self) -> Vec<String> {
        let mut reasons = Vec::new();
        if self.funding_sync_pending {
            reasons.push(format!(
                "资金费同步未完成：{}",
                self.funding_sync_error.as_deref().unwrap_or("等待重试")
            ));
        }
        for order in self.book.pending.values() {
            if order.settlement_issue.is_some() || order.status == "SETTLING" {
                reasons.push(format!(
                    "{} 成交费用尚未核实（{}）：{}",
                    order.symbol,
                    order.client_id,
                    order
                        .settlement_issue
                        .as_deref()
                        .unwrap_or("等待重新查询成交明细")
                ));
            }
        }
        for position in self
            .book
            .positions
            .values()
            .filter(|p| !p.accounting_complete)
        {
            if !self.book.pending.values().any(|o| {
                o.symbol == position.symbol
                    && (o.settlement_issue.is_some() || o.status == "SETTLING")
            }) {
                reasons.push(format!(
                    "{} 成交费用尚未核实，等待重新查询成交明细",
                    position.symbol
                ));
            }
        }
        reasons
    }
    fn scan_ready(&self, coin: &PoolCoin, now: i64) -> bool {
        let bar = now / BAR_MS * BAR_MS - 1;
        coin.enabled
            && !self.occupied(&coin.symbol)
            && now - bar <= self.config.signal_max_age_seconds as i64 * 1000
            && self
                .book
                .last_bar
                .get(&coin.symbol)
                .is_none_or(|last| *last < bar)
            && self
                .book
                .cooldown_until
                .get(&coin.symbol)
                .is_none_or(|until| *until <= bar)
    }
    fn scan_work(&self, now: i64) -> Vec<(PoolCoin, u64)> {
        let mut work: Vec<_> = self
            .book
            .pool
            .values()
            .filter(|coin| self.scan_ready(coin, now))
            .cloned()
            .map(|coin| (coin, self.revision))
            .collect();
        if let Some(cursor) = &self.scan_cursor {
            let next = work.partition_point(|(coin, _)| coin.symbol <= *cursor);
            work.rotate_left(next);
        }
        work
    }
    fn available_for_entry(&self, symbol: &str, mode: Mode) -> Result<Decimal> {
        if mode == Mode::Paper {
            return Ok(paper_available(self));
        }
        let asset = settlement_asset(symbol).context("不支持的结算币种")?;
        let balances = self
            .margin_balances
            .as_ref()
            .context("保证金余额尚未同步")?;
        let available = if balances.multi_assets_margin {
            balances.available_balance
        } else {
            balances
                .assets
                .iter()
                .find(|b| b.asset == asset)
                .map(|b| b.available_balance)
                .unwrap_or(Decimal::ZERO)
        };
        let mut reserved = Decimal::ZERO;
        for order in self.book.pending.values().filter(|o| {
            o.role == OrderRole::Entry
                && (balances.multi_assets_margin || settlement_asset(&o.symbol) == Some(asset))
        }) {
            let price = self
                .quotes
                .get(&order.symbol)
                .context("待确认订单缺少盘口")?
                .mid();
            reserved += order.quantity * price / Decimal::from(order.leverage)
                + order.quantity * price * order.taker_fee;
        }
        Ok(available - reserved)
    }
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
        } else {
            // Older books have only the position-level flag. Preserve its uncertainty
            // on each affected order so unchanged partial fills are queried again.
            for order in book.pending.values_mut() {
                if order.settlement_issue.is_none()
                    && (order.status == "SETTLING"
                        || order.accounted_quantity > Decimal::ZERO
                            && book
                                .positions
                                .get(&order.symbol)
                                .is_some_and(|p| !p.accounting_complete))
                {
                    order.settlement_issue =
                        Some("历史成交费用尚未核实，等待重新查询成交明细".into());
                }
            }
        }
        let realized_net = store.save(mode, &config, &book)?;
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
        let stream_enabled = mode != Mode::Paper && exchange.configured();
        let exchange = exchange.with_store(store.clone());
        Ok(Arc::new(Self {
            mode,
            store,
            exchange,
            jev: RwLock::new(Arc::new(jev)),
            reconcile_lock: Mutex::new(()),
            runtime: Mutex::new(RuntimeMetrics::default()),
            user_stream: Mutex::new(StreamStatus {
                enabled: stream_enabled,
                ..Default::default()
            }),
            account_notify: Notify::new(),
            market_stream: Mutex::new(MarketStreamStatus::default()),
            inner: Mutex::new(Inner {
                config,
                book,
                paused: true,
                catalog: BTreeMap::new(),
                quotes: BTreeMap::new(),
                market_at: 0,
                account_at: 0,
                account_sync_failed: false,
                account_update_pending: false,
                funding_sync_pending: false,
                funding_sync_error: None,
                account_generation: 0,
                available,
                realized_net,
                margin_balances: None,
                account_assets: None,
                account_assets_at: 0,
                account_assets_error: None,
                external_positions: BTreeMap::new(),
                external_orders: vec![],
                decisions: BTreeMap::new(),
                revision: 0,
                health,
                persistence_pending: false,
                scan_cursor: None,
                market_retry_at: None,
            }),
        }))
    }
    fn save(&self, i: &mut Inner) -> Result<()> {
        // A failed save leaves memory ahead of SQLite. Even an unchanged later poll
        // must retry persistence before it can be treated as a no-op.
        i.persistence_pending = true;
        i.realized_net = self.store.save(self.mode, &i.config, &i.book)?;
        let remove = i.book.closed_trades.len().saturating_sub(100);
        i.book.closed_trades.drain(..remove);
        i.book.funding_ids.clear();
        i.book.funding_income.clear();
        i.persistence_pending = false;
        Ok(())
    }
    fn save_if_changed(&self, i: &mut Inner, changed: bool) -> Result<()> {
        if changed || i.persistence_pending {
            self.save(i)?;
        }
        Ok(())
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
        let jev_available = self.jev.read().await.available();
        let runtime = self.runtime.lock().await.clone();
        let exchange_rate_limit = self.exchange.rate_limit_snapshot().await;
        let user_stream = self.user_stream.lock().await.clone();
        let market_stream = self.market_stream.lock().await.clone();
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
            account_sync_failed: i.account_sync_failed,
            account_update_pending: i.account_update_pending,
            funding_sync_pending: i.funding_sync_pending,
            entry_data_pending: !i.entry_data_ready(),
            entry_blockers: i.entry_blockers(),
            available_usdt: i.available,
            margin_balances: i.margin_balances.clone(),
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
            realized_net: i.realized_net,
            unrealized,
            exposure_usdt: i.exposure(),
            jev_available,
            runtime,
            exchange_rate_limit,
            user_stream,
            market_stream,
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
        i.realized_net = self.store.save(self.mode, &c, &book)?;
        i.config = c;
        i.book = book;
        i.persistence_pending = false;
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
            "不是可交易的 USDT / USDC 永续币对，请确认币对已上市并已同步行情"
        );
        i.config.for_coin(&coin)?;
        ensure!(
            i.book.pool.contains_key(&coin.symbol) || i.book.pool.len() < 200,
            "币池最多 200 个候选币"
        );
        let mut book = i.book.clone();
        book.pool.insert(coin.symbol.clone(), coin);
        i.realized_net = self.store.save(self.mode, &i.config, &book)?;
        i.book = book;
        i.persistence_pending = false;
        i.revision += 1;
        Ok(())
    }
    pub async fn remove_coin(&self, symbol: &str) -> Result<()> {
        let symbol = normalize_symbol(symbol)?;
        let mut i = self.inner.lock().await;
        let mut book = i.book.clone();
        book.pool.remove(&symbol);
        i.realized_net = self.store.save(self.mode, &i.config, &book)?;
        i.book = book;
        i.persistence_pending = false;
        i.revision += 1;
        Ok(())
    }
    pub async fn set_running(&self, run: bool) -> Result<()> {
        let jev_available = !run || self.jev.read().await.available();
        let mut i = self.inner.lock().await;
        if run {
            ensure!(
                i.entry_data_ready(),
                "资金费或成交费用尚未核实：{}",
                i.entry_blockers().join("；")
            );
            ensure!(
                self.mode == Mode::Paper || self.exchange.configured(),
                "请在账户与连接中配置币安 API Key 和 Secret"
            );
            ensure!(now_ms() - i.market_at < 15_000, "行情尚未同步或已过期");
            ensure!(
                self.mode == Mode::Paper
                    || (!i.account_sync_failed
                        && !i.account_update_pending
                        && (0..15_000).contains(&(now_ms() - i.account_at))),
                "账户尚未对账"
            );
            ensure!(
                i.external_positions.is_empty() && i.external_orders.is_empty(),
                "存在外部持仓或订单，请先处理"
            );
            ensure!(
                jev_available,
                "请在账户与连接中配置 Jev API Key 后再启动扫描"
            );
            for coin in i.book.pool.values().filter(|c| c.enabled) {
                i.config.for_coin(coin)?;
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
        tasks.push(tokio::spawn(
            async move { crate::market_stream::run(e).await },
        ));
        if self.mode != Mode::Paper && self.exchange.configured() {
            let e = self.clone();
            tasks.push(tokio::spawn(
                async move { crate::user_stream::run(e).await },
            ));
        }
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
            let mut t = tokio::time::interval(POSITION_INTERVAL);
            t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = t.tick() => {},
                    _ = e.account_notify.notified() => {
                        // Coalesce bursts; periodic REST reconciliation stays active.
                        tokio::time::sleep(Duration::from_millis(250)).await;
                    }
                }
                if let Err(err) = e.manage_positions().await {
                    e.fault("持仓管理异常", err).await;
                }
            }
        }));
        let e = self.clone();
        tasks.push(tokio::spawn(async move {
            let mut t = tokio::time::interval(SCAN_INTERVAL);
            t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                t.tick().await;
                e.scan().await;
            }
        }));
        EngineTasks(tasks)
    }
    pub(crate) async fn stream_gap(&self) {
        {
            let mut i = self.inner.lock().await;
            i.account_sync_failed = true;
            i.account_generation = i.account_generation.saturating_add(1);
            i.paused = true;
            i.revision += 1;
            i.health = "账户推送中断，暂停新增开仓，REST 继续核对".into();
        }
        self.event("warn", None, "账户推送中断，等待重连与完整对账", json!({}));
        self.account_notify.notify_one();
    }
    pub(crate) async fn handle_user_event(&self, event: UserEvent) -> Result<()> {
        if matches!(event, UserEvent::Expired | UserEvent::Other) {
            return Ok(());
        }
        self.mark_account_pending().await;
        let _guard = self.reconcile_lock.lock().await;
        // The previous lock owner may have reconciled the first invalidation
        // while this event waited. Keep its effects dirty until a later read.
        self.mark_account_pending().await;
        match event {
            UserEvent::Order { order, .. } => {
                let local = self
                    .inner
                    .lock()
                    .await
                    .book
                    .pending
                    .get(&order.client_id)
                    .cloned();
                if let Some(local) = local {
                    ensure!(
                        order.position_side == "BOTH"
                            && order.side
                                == if local.role == OrderRole::Entry {
                                    local.side.entry_side()
                                } else {
                                    local.side.exit_side()
                                },
                        "订单推送与委托方向不一致"
                    );
                    // A delayed nonterminal push must not undo terminal settlement.
                    if local.status != "SETTLING" || order.report().terminal() {
                        self.apply_order(order.report()).await?;
                        self.finalize_closed().await?;
                    }
                } else if !order.report().terminal() {
                    let mut i = self.inner.lock().await;
                    if !i.external_orders.contains(&order.symbol) {
                        i.external_orders.push(order.symbol);
                    }
                    i.paused = true;
                    i.revision += 1;
                    i.health = "收到外部订单推送，暂停新开仓，等待完整对账".into();
                }
            }
            UserEvent::Risk => {
                let mut i = self.inner.lock().await;
                i.paused = true;
                i.revision += 1;
                i.health = "收到保证金风险推送，暂停新开仓，等待完整对账".into();
            }
            UserEvent::Account => {}
            UserEvent::Expired | UserEvent::Other => return Ok(()),
        }
        // Pushes contain deltas rather than a complete margin/order snapshot.
        // Prevent use of the old snapshot until REST has reconciled all fields.
        Ok(())
    }
    async fn mark_account_pending(&self) {
        let mut i = self.inner.lock().await;
        i.account_update_pending = true;
        i.account_generation = i.account_generation.saturating_add(1);
        drop(i);
        self.account_notify.notify_one();
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
        if let Some(wait) = error
            .downcast_ref::<ExchangeError>()
            .and_then(ExchangeError::budget_retry_after)
        {
            self.event(
                "warn",
                None,
                "同步请求等待权重额度，稍后自动重试",
                json!({"context": message, "error": error.to_string(), "retry_after_ms": wait.as_millis() as u64}),
            );
            self.inner.lock().await.health = format!("等待请求权重额度：{message}");
            // Account/funding/settlement readiness gates block entries until
            // reconciliation succeeds; a local unsent request keeps user intent.
            return;
        }
        self.event("error", None, message, json!({"error": error.to_string()}));
        let mut i = self.inner.lock().await;
        i.health = format!("{message}: {error}");
        i.paused = true;
        i.revision += 1;
    }
    pub(crate) async fn market_symbols(&self) -> BTreeSet<String> {
        self.inner.lock().await.market_symbols()
    }
    pub(crate) async fn apply_market_quote(&self, symbol: String, quote: Quote) -> bool {
        let mut i = self.inner.lock().await;
        if quote.validate().is_err()
            || !i.wants_market(&symbol)
            || i.quotes.get(&symbol).is_some_and(|previous| {
                quote_is_older(&quote, previous)
                    || quote
                        .update_id
                        .zip(previous.update_id)
                        .is_some_and(|(new, old)| new == old)
            })
        {
            return false;
        }
        i.quotes.insert(symbol, quote);
        // Aggregate freshness is refreshed by the two-second supervisor, keeping
        // high-frequency updates O(log N) rather than rescanning the entire pool.
        true
    }
    async fn refresh_market(&self) -> Result<()> {
        {
            let mut i = self.inner.lock().await;
            // Keep WebSocket freshness current even while REST awaits quota.
            // Entry checks still reject stale quotes without changing user intent.
            i.market_health(self.mode);
            if i.market_retry_at
                .is_some_and(|retry| Instant::now() < retry)
            {
                return Ok(());
            }
            i.market_retry_at = None;
        }
        match self.refresh_market_once().await {
            Err(error) => {
                let Some(wait) = error
                    .downcast_ref::<ExchangeError>()
                    .and_then(ExchangeError::budget_retry_after)
                else {
                    return Err(error);
                };
                self.inner.lock().await.market_retry_at = Some(Instant::now() + wait);
                self.event(
                    "warn",
                    None,
                    "行情请求等待权重额度，稍后自动重试",
                    json!({"error": error.to_string(), "retry_after_ms": wait.as_millis() as u64}),
                );
                Ok(())
            }
            result => result,
        }
    }
    async fn refresh_market_once(&self) -> Result<()> {
        if self.inner.lock().await.catalog.is_empty() {
            self.exchange.sync_time().await?;
            let catalog = self.exchange.symbols().await?;
            self.inner.lock().await.catalog = catalog;
        }
        let connected = self.market_stream.lock().await.connected;
        let symbols = {
            let mut i = self.inner.lock().await;
            i.market_health(self.mode);
            i.market_symbols()
                .into_iter()
                .filter(|symbol| {
                    !connected
                        || i.quotes
                            .get(symbol)
                            .is_none_or(|q| !(0..8_000).contains(&(now_ms() - q.time)))
                })
                .collect::<Vec<_>>()
        };
        let quotes = self.exchange.quotes_for(&symbols).await?;
        let mut i = self.inner.lock().await;
        let required = i.market_symbols();
        for (symbol, quote) in quotes {
            if required.contains(&symbol)
                && !i
                    .quotes
                    .get(&symbol)
                    .is_some_and(|previous| quote_is_older(&quote, previous))
            {
                i.quotes.insert(symbol, quote);
            }
        }
        i.market_health(self.mode);
        Ok(())
    }
    pub async fn refresh_catalog(&self) -> Result<()> {
        let c = self.exchange.symbols().await?;
        self.inner.lock().await.catalog = c;
        Ok(())
    }
    async fn scan(self: &Arc<Self>) {
        let started = Instant::now();
        self.runtime.lock().await.scan.start(now_ms(), started);
        let failed = self.scan_once().await;
        let elapsed = started.elapsed();
        self.runtime
            .lock()
            .await
            .scan
            .finish(now_ms(), elapsed, SCAN_INTERVAL, failed);
        if elapsed > SCAN_INTERVAL {
            tracing::warn!(mode = ?self.mode, elapsed_ms = elapsed.as_millis() as u64,
                target_ms = SCAN_INTERVAL.as_millis() as u64, "扫描周期超过目标间隔");
        }
    }
    async fn scan_once(self: &Arc<Self>) -> bool {
        let work = {
            let i = self.inner.lock().await;
            if !i.scan_allowed(self.mode, now_ms()) {
                return false;
            }
            i.scan_work(now_ms())
        };
        let failed = std::sync::atomic::AtomicBool::new(false);
        stream::iter(work)
            .for_each_concurrent(4, |(coin, revision)| {
                let e = self.clone();
                let failed = &failed;
                async move {
                    {
                        let mut i = e.inner.lock().await;
                        // Recheck queued candidates after each slow request. Only dispatched
                        // work advances the cursor, so the next bar starts with unserved coins.
                        let now = now_ms();
                        if !i.scan_allowed(e.mode, now)
                            || i.revision != revision
                            || !i.scan_ready(&coin, now)
                        {
                            return;
                        }
                        i.scan_cursor = Some(coin.symbol.clone());
                    }
                    if let Err(err) = e.evaluate_coin(coin.clone(), revision).await {
                        failed.store(true, std::sync::atomic::Ordering::Relaxed);
                        e.event("warn", Some(&coin.symbol), "本轮等待", waiting_detail(&err));
                    }
                }
            })
            .await;
        failed.into_inner()
    }
    async fn decide(
        &self,
        symbol: &str,
        bars: &[Candle],
        quote: &Quote,
        c: &Config,
    ) -> Result<Decision> {
        let jev = self.jev.read().await.clone();
        jev.decide(symbol, bars, quote, c).await
    }
    async fn evaluate_coin(&self, coin: PoolCoin, revision: u64) -> Result<()> {
        let mut c = self.inner.lock().await.config.for_coin(&coin)?;
        {
            let i = self.inner.lock().await;
            let expected_close = now_ms() / BAR_MS * BAR_MS - 1;
            if i.paused
                || !i.entry_data_ready()
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
                || !i.entry_data_ready()
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
            self.save(&mut i)?;
            quote
        };
        let (maker_fee, taker_fee) = if self.mode == Mode::Paper {
            (c.maker_fee, c.taker_fee)
        } else {
            self.exchange.fees(&coin.symbol).await?
        };
        c.maker_fee = maker_fee;
        c.taker_fee = taker_fee;
        let decision = self.decide(&coin.symbol, &bars, &quote, &c).await?;
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
                || !i.entry_data_ready()
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
                ensure!(
                    !i.account_sync_failed
                        && !i.account_update_pending
                        && (0..15_000).contains(&(now_ms() - i.account_at)),
                    "账户对账过期或同步失败"
                );
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
            let available = i.available_for_entry(&coin.symbol, self.mode)?;
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
                stop_loss_usdt: c.stop_loss_usdt,
                maker_fee,
                taker_fee,
                status: "SUBMITTING".into(),
                accounted_quantity: Decimal::ZERO,
                settlement_issue: None,
                leverage: c.leverage,
            };
            i.book
                .pending
                .insert(order.client_id.clone(), order.clone());
            self.save(&mut i)?;
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
            stop_loss_usdt: o.stop_loss_usdt,
            stop_loss_triggered: false,
            entry_fee: fee,
            exit_fee: Decimal::ZERO,
            realized_gross: Decimal::ZERO,
            funding_cost: Decimal::ZERO,
            maker_fee: o.maker_fee,
            taker_fee: o.taker_fee,
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
        self.save(&mut i)?;
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
                self.save(&mut i)?;
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
        let (pending, legacy_unsettled) = {
            let i = self.inner.lock().await;
            (
                i.book.pending.get(&remote.client_order_id).cloned(),
                i.book
                    .positions
                    .get(&remote.symbol)
                    .is_some_and(|p| !p.accounting_complete),
            )
        };
        let Some(o) = pending else {
            return Ok(());
        };
        ensure!(
            remote.symbol == o.symbol
                && remote.orig_qty == o.quantity
                && remote.executed_qty >= Decimal::ZERO
                && remote.executed_qty <= o.quantity
                && remote.avg_price >= Decimal::ZERO
                && remote.cum_quote >= Decimal::ZERO,
            "订单回报与已持久化委托不一致"
        );
        let fetch = remote.executed_qty > o.accounted_quantity
            || remote.executed_qty > Decimal::ZERO
                && (remote.terminal() || o.settlement_issue.is_some() || legacy_unsettled);
        let fills = if fetch {
            Some(self.exchange.fills(&o.symbol, remote.order_id).await)
        } else {
            None
        };
        let (settlement_issue, incomplete_fills) = match &fills {
            Some(Ok(fills)) => fill_settlement_issue(fills, &remote, &o),
            Some(Err(error)) => (Some(format!("成交明细查询失败：{error}")), false),
            None => (None, false),
        };
        let mut i = self.inner.lock().await;
        let Some(current) = i.book.pending.get(&o.client_id).cloned() else {
            return Ok(());
        };
        if remote.executed_qty < current.accounted_quantity {
            return Ok(());
        }
        if !fetch
            && !remote.terminal()
            && remote.executed_qty == current.accounted_quantity
            && remote.status == current.status
        {
            return self.save_if_changed(&mut i, false);
        }
        let delta = (remote.executed_qty - current.accounted_quantity).max(Decimal::ZERO);
        if o.role == OrderRole::Entry && delta > Decimal::ZERO {
            let avg = if remote.avg_price > Decimal::ZERO {
                remote.avg_price
            } else if remote.cum_quote > Decimal::ZERO {
                remote.cum_quote / remote.executed_qty
            } else if let Some(Ok(fills)) = &fills
                && fills.iter().all(|f| {
                    f.order_id == remote.order_id
                        && f.qty > Decimal::ZERO
                        && f.price > Decimal::ZERO
                })
                && fills.iter().map(|f| f.qty).sum::<Decimal>() == remote.executed_qty
                && fills.iter().map(|f| f.id).collect::<BTreeSet<_>>().len() == fills.len()
            {
                fills.iter().map(|f| f.qty * f.price).sum::<Decimal>() / remote.executed_qty
            } else {
                // Never fabricate a zero/limit/market quote as the execution price.
                // The persisted intent stays locked and is queried on the next cycle.
                let error = match fills {
                    Some(Err(error)) => error.context("成交均价尚未确认，保留订单等待查询"),
                    _ => anyhow::anyhow!("成交均价尚未确认，保留订单等待查询"),
                };
                i.book
                    .pending
                    .get_mut(&o.client_id)
                    .unwrap()
                    .settlement_issue = Some(error.to_string());
                self.save(&mut i)?;
                return Err(error);
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
                    stop_loss_usdt: o.stop_loss_usdt,
                    stop_loss_triggered: false,
                    entry_fee: Decimal::ZERO,
                    exit_fee: Decimal::ZERO,
                    realized_gross: Decimal::ZERO,
                    funding_cost: Decimal::ZERO,
                    maker_fee: o.maker_fee,
                    taker_fee: o.taker_fee,
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
            && settlement_issue.is_none()
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
            }
            settled = true;
        }
        if let Some(order) = i.book.pending.get_mut(&o.client_id) {
            order.accounted_quantity = order.accounted_quantity.max(remote.executed_qty);
            order.settlement_issue = settlement_issue.clone();
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
        // A successful read for one order must not clear another order's fee wait.
        let symbol_unsettled = i.book.pending.values().any(|order| {
            order.symbol == o.symbol
                && (order.status == "SETTLING" || order.settlement_issue.is_some())
        });
        if fetch && let Some(position) = i.book.positions.get_mut(&o.symbol) {
            position.accounting_complete = settled && !symbol_unsettled;
        }
        self.save(&mut i)?;
        drop(i);
        if fetch && !settled {
            let budget_wait = fills.as_ref().and_then(|result| {
                result.as_ref().err().and_then(|error| {
                    error
                        .downcast_ref::<ExchangeError>()
                        .and_then(ExchangeError::budget_retry_after)
                })
            });
            let mut i = self.inner.lock().await;
            if budget_wait.is_none() && !incomplete_fills {
                i.paused = true;
                i.revision += 1;
            }
            i.health = format!(
                "{} 成交费用尚未核实：{}",
                o.symbol,
                settlement_issue.as_deref().unwrap_or("等待重新查询")
            );
            drop(i);
            self.event(
                "warn",
                Some(&o.symbol),
                if budget_wait.is_some() {
                    "成交费用等待权重额度，保留结算状态并自动核对"
                } else if incomplete_fills {
                    "成交明细尚未完整，保留委托并自动重查"
                } else {
                    "成交费用尚未核实，保留委托并自动重查"
                },
                json!({"client_id": o.client_id, "order_id":remote.order_id, "status":remote.status,
                    "settlement_asset": settlement_asset(&o.symbol), "reason":settlement_issue,
                    "executed_quantity":remote.executed_qty,
                    "fills_count": fills.as_ref().and_then(|r|r.as_ref().ok()).map(Vec::len),
                    "fills_quantity": fills.as_ref().and_then(|r|r.as_ref().ok()).map(|f|f.iter().map(|f|f.qty).sum::<Decimal>()),
                    "commission_by_asset":fills.as_ref().and_then(|r|r.as_ref().ok()).map(|fills| {
                        let mut fees = BTreeMap::<String, Decimal>::new();
                        for fill in fills { *fees.entry(fill.commission_asset.clone()).or_default() += fill.commission; }
                        fees
                    }),
                    "retry_after_ms": budget_wait.map(|wait| wait.as_millis() as u64)}),
            );
        }
        Ok(())
    }
    async fn manage_positions(&self) -> Result<()> {
        let started = Instant::now();
        self.runtime
            .lock()
            .await
            .position_management
            .start(now_ms(), started);
        let result = self.manage_positions_once().await;
        let elapsed = started.elapsed();
        self.runtime.lock().await.position_management.finish(
            now_ms(),
            elapsed,
            POSITION_INTERVAL,
            result.as_ref().map_or(true, |degraded| *degraded),
        );
        if elapsed > POSITION_INTERVAL {
            tracing::warn!(mode = ?self.mode, elapsed_ms = elapsed.as_millis() as u64,
                target_ms = POSITION_INTERVAL.as_millis() as u64, "持仓管理周期超过目标间隔");
        }
        result.map(|_| ())
    }
    async fn reconcile_pending(&self) -> Result<()> {
        let pending = self
            .inner
            .lock()
            .await
            .book
            .pending
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut reports = stream::iter(pending)
            .map(|order| async move {
                let result = self.exchange.order(&order.symbol, &order.client_id).await;
                (order, result)
            })
            .buffered(4);
        while let Some((o, result)) = reports.next().await {
            // Only reads overlap. Apply reports and any cancellation in the original
            // order so partial fills cannot race settlement or order replacement.
            match result {
                Ok(remote) => {
                    let partial = o.role == OrderRole::Entry
                        && remote.executed_qty > Decimal::ZERO
                        && !remote.terminal();
                    self.apply_order(remote).await?;
                    if partial
                        && let Ok(cancelled) = self.exchange.cancel(&o.symbol, &o.client_id).await
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
        Ok(())
    }
    async fn manage_positions_once(&self) -> Result<bool> {
        let _guard = self.reconcile_lock.lock().await;
        let mut degraded = false;
        if self.mode != Mode::Paper {
            if !self.exchange.configured() {
                return Ok(false);
            }
            let startup = if self.inner.lock().await.account_at == 0 {
                self.exchange.preflight().await.map(|_| ())
            } else {
                Ok(())
            };
            // Persistence/settlement failures still abort this cycle. An account
            // read failure must not prevent confirmation of already sent orders.
            self.reconcile_pending().await?;
            let account = match startup {
                Ok(()) => self.sync_account().await,
                Err(error) => Err(error),
            };
            match account {
                Err(error) => {
                    degraded = true;
                    self.inner.lock().await.account_sync_failed = true;
                    self.fault("账户同步失败，暂停新开仓；止损需按币对独立核对", error)
                        .await;
                }
                // An event superseding a read is normal; block entries and new
                // take-profit orders until the already queued reconciliation.
                Ok(false) => degraded = true,
                Ok(true) => {
                    match self.sync_funding().await {
                        Ok(()) => {
                            let mut i = self.inner.lock().await;
                            i.funding_sync_pending = false;
                            i.funding_sync_error = None;
                        }
                        Err(error) => {
                            {
                                let mut i = self.inner.lock().await;
                                i.funding_sync_pending = true;
                                i.funding_sync_error = Some(error.to_string());
                            }
                            // Keep exits available using recorded funding if only the income endpoint fails.
                            self.fault("资金费同步失败，持仓风控使用已记录资金费", error)
                                .await;
                        }
                    }
                }
            }
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
            let check = if degraded {
                ExitCheck::Symbol
            } else {
                ExitCheck::Account
            };
            match self.ensure_stop_loss(&p.symbol, check).await {
                Ok(true) => continue,
                Ok(false) => {}
                Err(error) => {
                    self.event(
                        "warn",
                        Some(&p.symbol),
                        "止损管理等待",
                        json!({"error": error.to_string()}),
                    );
                    continue;
                }
            }
            if degraded {
                continue;
            }
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
        Ok(degraded)
    }
    async fn sync_account(&self) -> Result<bool> {
        let generation = self.inner.lock().await.account_generation;
        let (remote, orders, balances) = tokio::try_join!(
            self.exchange.positions(),
            self.exchange.open_orders(),
            self.exchange.margin_balances(),
        )?;
        let mut i = self.inner.lock().await;
        if generation != i.account_generation {
            return Ok(false);
        }
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
        i.available = if balances.multi_assets_margin {
            balances.available_balance
        } else {
            balances
                .assets
                .iter()
                .find(|b| b.asset == "USDT")
                .map(|b| b.available_balance)
                .unwrap_or(Decimal::ZERO)
        };
        i.margin_balances = Some(balances);
        i.account_at = now_ms();
        i.account_sync_failed = false;
        i.account_update_pending = false;
        if !i.external_positions.is_empty() || !i.external_orders.is_empty() {
            i.paused = true;
            i.health = "账户与系统记录不一致，暂停新开仓，保留订单".into();
        }
        Ok(true)
    }
    async fn sync_funding(&self) -> Result<()> {
        let cursor = {
            let i = self.inner.lock().await;
            if !i.funding_sync_pending && now_ms() - i.book.funding_last_poll < 60_000 {
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
            if i.book.funding_ids.contains(&id) || self.store.has_funding(&id)? {
                continue;
            }
            let Some(symbol) = r["symbol"].as_str() else {
                continue;
            };
            let amount = crate::exchange::decimal(&r["income"])?;
            let time = r["time"].as_i64().context("资金费时间无效")?;
            // Untracked contracts must not block funding reconciliation for our positions.
            if !i.book.positions.contains_key(symbol) && !self.store.has_closed_symbol(symbol)? {
                continue;
            }
            ensure!(
                r["asset"].as_str() == settlement_asset(symbol),
                "资金费资产与合约结算币种不一致"
            );
            i.persistence_pending = true;
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
            i.book.funding_income.push(FundingIncome {
                id: id.clone(),
                symbol: symbol.into(),
                asset: r["asset"].as_str().unwrap().into(),
                time,
                amount,
            });
            i.book.funding_ids.push(id);
        }
        i.book.funding_cursor = end;
        i.book.funding_last_poll = end;
        self.save(&mut i)?;
        Ok(())
    }
    async fn ensure_stop_loss(&self, symbol: &str, check: ExitCheck) -> Result<bool> {
        let detail = {
            let mut i = self.inner.lock().await;
            let Some(p) = i.book.positions.get(symbol) else {
                return Ok(false);
            };
            if p.quantity <= Decimal::ZERO
                || !p.stop_loss_triggered && p.stop_loss_usdt == Decimal::ZERO
            {
                return Ok(false);
            }
            if p.stop_loss_triggered {
                None
            } else {
                let quote = i.quotes.get(symbol).context("止损盘口缺失")?;
                quote.validate()?;
                ensure!(
                    (0..15_000).contains(&(now_ms() - quote.time)),
                    "止损盘口过期"
                );
                let exit_price = if p.side == Action::Long {
                    quote.bid
                } else {
                    quote.ask
                };
                let estimated_net = p.estimated_exit_net(exit_price);
                if estimated_net > -p.stop_loss_usdt {
                    return Ok(false);
                }
                let detail = json!({"estimated_net": estimated_net, "stop_loss_usdt": p.stop_loss_usdt,
                    "exit_price": exit_price, "quote_time": quote.time, "settlement_asset": settlement_asset(symbol)});
                i.book
                    .positions
                    .get_mut(symbol)
                    .unwrap()
                    .stop_loss_triggered = true;
                self.save(&mut i)?;
                Some(detail)
            }
        };
        if let Some(detail) = detail {
            self.event(
                "risk",
                Some(symbol),
                "已触发净亏损止损，确认撤单后市价减仓",
                detail,
            );
        }
        self.close_position(symbol, OrderRole::StopLoss, check)
            .await?;
        Ok(true)
    }
    async fn ensure_tp(&self, symbol: &str) -> Result<()> {
        let cancel = {
            let i = self.inner.lock().await;
            if self.mode != Mode::Paper && (i.account_sync_failed || i.account_update_pending) {
                return Ok(());
            }
            let p = i.book.positions.get(symbol).context("持仓不存在")?;
            if p.stop_loss_triggered {
                return Ok(());
            }
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
                i.book.paper_match_times.remove(&o.client_id);
                i.book.paper_match_exchange_times.remove(&o.client_id);
                i.book.paper_match_update_ids.remove(&o.client_id);
                if let Some(p) = i.book.positions.get_mut(symbol) {
                    p.tp_client_id = None;
                    p.tp_price = None;
                }
                self.save(&mut i)?;
            } else {
                let r = self.exchange.cancel(symbol, &o.client_id).await?;
                self.apply_order(r).await?;
            }
        }
        let order = {
            let mut i = self.inner.lock().await;
            if self.mode != Mode::Paper && (i.account_sync_failed || i.account_update_pending) {
                return Ok(());
            }
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
            let exchange_time = quote.exchange_time;
            let update_id = quote.update_id;
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
                stop_loss_usdt: p.stop_loss_usdt,
                maker_fee: p.maker_fee,
                taker_fee: p.taker_fee,
                status: if self.mode == Mode::Paper {
                    "NEW"
                } else {
                    "SUBMITTING"
                }
                .into(),
                accounted_quantity: Decimal::ZERO,
                settlement_issue: None,
                leverage: p.leverage,
            };
            let p = i.book.positions.get_mut(symbol).unwrap();
            p.tp_price = Some(price);
            p.tp_client_id = Some(o.client_id.clone());
            i.book.pending.insert(o.client_id.clone(), o.clone());
            if self.mode == Mode::Paper
                && let Some(time) = exchange_time
            {
                i.book
                    .paper_match_exchange_times
                    .insert(o.client_id.clone(), time);
            }
            if self.mode == Mode::Paper
                && let Some(id) = update_id
            {
                i.book
                    .paper_match_update_ids
                    .insert(o.client_id.clone(), id);
            }
            self.save(&mut i)?;
            o
        };
        if self.mode != Mode::Paper {
            self.submit_order(order).await?;
        }
        Ok(())
    }
    async fn paper_match(&self) -> Result<()> {
        let mut i = self.inner.lock().await;
        let mut changed = false;
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
                || !(0..15_000).contains(&(now_ms() - q.time))
                || (q.update_id.is_none()
                    && i.book
                        .paper_match_times
                        .get(id)
                        .is_some_and(|time| *time >= q.time))
            {
                continue;
            }
            if q.exchange_time
                .zip(i.book.paper_match_exchange_times.get(id).copied())
                .is_some_and(|(new, old)| new < old)
            {
                continue;
            }
            if let Some(update_id) = q.update_id {
                if let Some(previous) = i.book.paper_match_update_ids.get(id) {
                    if update_id <= *previous {
                        continue;
                    }
                } else {
                    i.book.paper_match_update_ids.insert(id.clone(), update_id);
                    if let Some(time) = q.exchange_time {
                        i.book.paper_match_exchange_times.insert(id.clone(), time);
                    }
                    i.persistence_pending = true;
                    changed = true;
                    continue;
                }
            } else if i.book.paper_match_update_ids.contains_key(id) {
                // Do not refill liquidity from an unidentifiable fallback quote.
                continue;
            } else if let Some(time) = q.exchange_time {
                if let Some(previous) = i.book.paper_match_exchange_times.get(id) {
                    if time <= *previous {
                        continue;
                    }
                } else {
                    // Upgraded portfolios have no source timestamp baseline. Observe one
                    // before filling, rather than treating an old snapshot as new liquidity.
                    i.book.paper_match_exchange_times.insert(id.clone(), time);
                    i.persistence_pending = true;
                    changed = true;
                    continue;
                }
            } else if i.book.paper_match_exchange_times.contains_key(id) {
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
            i.persistence_pending = true;
            i.book.paper_match_times.insert(id.clone(), q.time);
            if let Some(update_id) = q.update_id {
                i.book.paper_match_update_ids.insert(id.clone(), update_id);
            }
            if let Some(time) = q.exchange_time {
                i.book.paper_match_exchange_times.insert(id.clone(), time);
            }
            changed = true;
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
                i.book.paper_match_exchange_times.remove(id);
                i.book.paper_match_update_ids.remove(id);
                if let Some(local) = i.book.positions.get_mut(&p.symbol) {
                    local.tp_client_id = None;
                    local.tp_price = None;
                }
            }
        }
        i.available = paper_available(&i);
        self.save_if_changed(&mut i, changed)?;
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
        if closed.is_empty() {
            return self.save_if_changed(&mut i, false);
        }
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
                if p.stop_loss_triggered { "已完成止损平仓，等待新信号" } else { "已完成平仓，等待新信号" },
                json!({"net_profit": p.net_realized(), "stop_loss_triggered": p.stop_loss_triggered}),
            );
        }
        self.save(&mut i)?;
        Ok(())
    }
    pub async fn manual_close(&self, symbol: &str) -> Result<()> {
        let symbol = normalize_symbol(symbol)?;
        let _guard = self.reconcile_lock.lock().await;
        self.close_position(&symbol, OrderRole::ManualClose, ExitCheck::Account)
            .await
    }
    async fn confirm_exit(&self, symbol: &str) -> Result<ExitConfirmation> {
        let started = Instant::now();
        let (positions, orders) = tokio::try_join!(
            self.exchange.positions_for(symbol),
            self.exchange.open_orders_for(symbol),
        )?;
        let i = self.inner.lock().await;
        ensure!(
            !i.external_positions.contains_key(symbol)
                && !i.external_orders.iter().any(|s| s == symbol),
            "已发现外部持仓或订单，等待完整账户对账"
        );
        let p = i.book.positions.get(symbol).context("持仓不存在")?;
        ensure!(positions.len() == 1, "止损持仓数量或持仓模式尚未确认");
        let remote = &positions[0];
        ensure!(
            remote.symbol == symbol
                && remote.position_side == "BOTH"
                && remote.position_amt.abs() == p.quantity
                && p.quantity > Decimal::ZERO
                && (remote.position_amt > Decimal::ZERO) == (p.side == Action::Long),
            "止损持仓与系统记录不一致，等待完整账户对账"
        );
        let pending: Vec<_> = i
            .book
            .pending
            .values()
            .filter(|o| o.symbol == symbol)
            .collect();
        ensure!(
            pending.iter().all(|o| o.role == OrderRole::TakeProfit
                && !matches!(o.status.as_str(), "UNKNOWN" | "SUBMITTING" | "SETTLING")),
            "订单状态或费用尚未确认，止损意图已保留"
        );
        ensure!(
            orders.len() == pending.len()
                && pending.iter().all(|local| orders
                    .iter()
                    .filter(|remote| remote.client_order_id == local.client_id)
                    .count()
                    == 1)
                && orders.iter().all(|remote| remote.symbol == symbol
                    && matches!(remote.status.as_str(), "NEW" | "PARTIALLY_FILLED")
                    && pending
                        .iter()
                        .any(|local| remote.client_order_id == local.client_id
                            && remote.orig_qty == local.quantity
                            && remote.executed_qty == local.accounted_quantity)),
            "止损挂单与系统记录不一致，等待订单确认"
        );
        ensure!(
            started.elapsed() < Duration::from_secs(15),
            "止损独立核对已过期"
        );
        Ok(ExitConfirmation {
            quantity: p.quantity,
            side: p.side,
            started,
        })
    }
    // Caller holds reconcile_lock, shared with polling and manual closes.
    async fn close_position(&self, symbol: &str, role: OrderRole, check: ExitCheck) -> Result<()> {
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
        ensure!(
            !pending
                .iter()
                .any(|o| matches!(o.role, OrderRole::ManualClose | OrderRole::StopLoss)),
            "已有平仓委托，继续查询原订单"
        );
        if role == OrderRole::StopLoss {
            ensure!(
                !pending
                    .iter()
                    .any(|o| matches!(o.status.as_str(), "UNKNOWN" | "SUBMITTING" | "SETTLING")),
                "订单状态或费用尚未确认，止损意图已保留"
            );
            let i = self.inner.lock().await;
            ensure!(
                !i.external_positions.contains_key(symbol)
                    && !i.external_orders.iter().any(|s| s == symbol),
                "外部持仓或订单变更，等待完整账户对账"
            );
            let q = i.quotes.get(symbol).context("止损盘口缺失")?;
            q.validate()?;
            ensure!((0..15_000).contains(&(now_ms() - q.time)), "止损盘口过期");
        }
        let independently_checked = self.mode != Mode::Paper && matches!(check, ExitCheck::Symbol);
        let mut confirmation = if independently_checked {
            Some(self.confirm_exit(symbol).await?)
        } else {
            None
        };
        let cancelled = !pending.is_empty();
        for o in pending {
            if self.mode == Mode::Paper {
                let mut i = self.inner.lock().await;
                i.book.pending.remove(&o.client_id);
                i.book.paper_match_times.remove(&o.client_id);
                i.book.paper_match_exchange_times.remove(&o.client_id);
                i.book.paper_match_update_ids.remove(&o.client_id);
                i.persistence_pending = true;
            } else {
                let r = self.exchange.cancel(symbol, &o.client_id).await?;
                self.apply_order(r).await?;
            }
        }
        if independently_checked
            && cancelled
            && self
                .inner
                .lock()
                .await
                .book
                .positions
                .get(symbol)
                .is_some_and(|p| p.quantity > Decimal::ZERO)
        {
            // Cancellation may report a partial fill. Verify the updated quantity
            // and absence of orders before creating a reduce-only market intent.
            confirmation = Some(self.confirm_exit(symbol).await?);
        }
        let order = {
            let mut i = self.inner.lock().await;
            ensure!(
                !i.book.pending.values().any(|o| o.symbol == symbol),
                "订单未确认撤销"
            );
            let p = i.book.positions.get(symbol).context("持仓不存在")?.clone();
            if p.quantity == Decimal::ZERO {
                drop(i);
                self.finalize_closed().await?;
                return Ok(());
            }
            let q = i.quotes.get(symbol).context("盘口缺失")?.clone();
            q.validate()?;
            ensure!((0..15_000).contains(&(now_ms() - q.time)), "盘口过期");
            let taker_fee = if role == OrderRole::StopLoss {
                p.taker_fee
            } else {
                i.config.taker_fee
            };
            if self.mode == Mode::Paper {
                let price = if p.side == Action::Long { q.bid } else { q.ask };
                let gross = p.side.sign() * p.quantity * (price - p.entry_price);
                let fee = p.quantity * price * taker_fee;
                let local = i.book.positions.get_mut(symbol).unwrap();
                local.quantity = Decimal::ZERO;
                local.realized_gross += gross;
                local.exit_fee += fee;
                local.tp_client_id = None;
                local.tp_price = None;
                i.book.paper_cash += gross - fee;
                i.available = paper_available(&i);
                self.save(&mut i)?;
                None
            } else {
                if let Some(confirmation) = confirmation {
                    ensure!(
                        confirmation.started.elapsed() < Duration::from_secs(15)
                            && confirmation.quantity == p.quantity
                            && confirmation.side == p.side,
                        "止损独立核对已过期或持仓已变化"
                    );
                } else {
                    ensure!(
                        !i.account_sync_failed
                            && !i.account_update_pending
                            && (0..15_000).contains(&(now_ms() - i.account_at))
                            && !i.external_positions.contains_key(symbol),
                        "账户未对账"
                    );
                }
                i.book.sequence += 1;
                let o = PendingOrder {
                    client_id: format!(
                        "gt-{}-{}-{}",
                        if role == OrderRole::StopLoss {
                            "s"
                        } else {
                            "c"
                        },
                        now_ms(),
                        i.book.sequence
                    ),
                    symbol: symbol.into(),
                    side: p.side,
                    role,
                    quantity: p.quantity,
                    price: None,
                    created_at: now_ms(),
                    signal_time: p.signal_time,
                    target_profit: p.target_profit,
                    stop_loss_usdt: p.stop_loss_usdt,
                    maker_fee: p.maker_fee,
                    taker_fee,
                    status: "SUBMITTING".into(),
                    accounted_quantity: Decimal::ZERO,
                    settlement_issue: None,
                    leverage: p.leverage,
                };
                i.book.pending.insert(o.client_id.clone(), o.clone());
                self.save(&mut i)?;
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
fn fill_settlement_issue(
    fills: &[Fill],
    remote: &ExchangeOrder,
    order: &PendingOrder,
) -> (Option<String>, bool) {
    let mut ids = BTreeSet::new();
    if fills.iter().any(|fill| {
        fill.order_id != remote.order_id
            || fill.qty <= Decimal::ZERO
            || fill.price <= Decimal::ZERO
            || !ids.insert(fill.id)
    }) {
        return (
            Some("成交明细的订单、数量、价格或成交标识不一致".into()),
            false,
        );
    }
    let quantity: Decimal = fills.iter().map(|fill| fill.qty).sum();
    if quantity != remote.executed_qty {
        return (
            Some(format!(
                "成交明细数量未匹配：已查询 {quantity}，订单已成交 {}",
                remote.executed_qty
            )),
            quantity < remote.executed_qty,
        );
    }
    let Some(asset) = settlement_asset(&order.symbol) else {
        return (Some("合约结算币种不支持".into()), false);
    };
    let foreign: BTreeSet<_> = fills
        .iter()
        .filter(|fill| fill.commission != Decimal::ZERO && fill.commission_asset != asset)
        .map(|fill| fill.commission_asset.as_str())
        .collect();
    if !foreign.is_empty() {
        return (
            Some(format!(
                "非零手续费币种无法按 {asset} 记账：实际 {}",
                foreign.into_iter().collect::<Vec<_>>().join("、")
            )),
            false,
        );
    }
    (None, false)
}
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
    for asset in ["USDT", "USDC"] {
        if let Some(base) = s.strip_suffix(&format!("/{asset}")) {
            s = format!("{base}{asset}");
            break;
        }
    }
    ensure!(
        s.chars().count() >= 5
            && s.chars().count() <= 24
            && s.chars().all(char::is_alphanumeric)
            && settlement_asset(&s).is_some(),
        "请输入 BTCUSDT、BTC/USDT、BTCUSDC 或 BTC/USDC 形式的币对"
    );
    Ok(s)
}
fn quote_is_older(quote: &Quote, previous: &Quote) -> bool {
    quote
        .exchange_time
        .zip(previous.exchange_time)
        .is_some_and(|(new, old)| new < old)
        || quote
            .update_id
            .zip(previous.update_id)
            .is_some_and(|(new, old)| new < old)
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
            exchange_time: None,
            update_id: None,
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
            exchange_time: None,
            update_id: None,
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
    async fn read_budget_engine(
        report_path: &'static str,
        used: &'static str,
    ) -> (
        Arc<Engine>,
        Arc<std::sync::atomic::AtomicBool>,
        tokio::task::JoinHandle<()>,
    ) {
        use axum::{Json, Router, http::Uri};
        use std::sync::atomic::{AtomicBool, Ordering};
        let limited = Arc::new(AtomicBool::new(true));
        let report = limited.clone();
        let app = Router::new().fallback(move |uri: Uri| {
            let report = report.clone();
            async move {
                let value = match uri.path() {
                    "/fapi/v1/time" => json!({"serverTime": now_ms()}),
                    "/fapi/v2/account" => json!({"multiAssetsMargin": false, "availableBalance": "1000",
                        "assets": [{"asset": "USDT", "availableBalance": "1000"}]}),
                    "/fapi/v1/order" => json!({"symbol":"XUSDT", "clientOrderId":"test-entry", "orderId":42,
                        "status":"FILLED", "executedQty":"1", "origQty":"1", "avgPrice":"100", "cumQuote":"100"}),
                    "/fapi/v1/userTrades" => json!([{"id":1, "orderId":42, "qty":"1", "price":"100",
                        "commission":"0.05", "commissionAsset":"USDT", "realizedPnl":"0", "time":1, "side":"BUY"}]),
                    _ => json!([]),
                };
                let weight = if uri.path() == report_path && report.load(Ordering::SeqCst) { used } else { "0" };
                ([("x-mbx-used-weight-1m", weight)], Json(value))
            }
        });
        let (base, server) = crate::test_support::mock_server(app).await;
        let e = Engine::new(
            Mode::Live,
            Store::open(":memory:", Mode::Live).unwrap(),
            config(),
            Binance::mock(base),
            Jev::new().unwrap(),
        )
        .unwrap();
        seed(&e).await;
        {
            let mut i = e.inner.lock().await;
            i.book.pool.insert(
                "XUSDT".into(),
                PoolCoin {
                    symbol: "XUSDT".into(),
                    enabled: true,
                    config: None,
                },
            );
            i.paused = false;
            i.account_at = now_ms();
            i.book.funding_last_poll = now_ms();
        }
        e.market_stream.lock().await.connected = true;
        (e, limited, server)
    }
    #[tokio::test]
    async fn account_budget_wait_blocks_entries_then_recovers_without_changing_scan_intent() {
        for manual_pause in [false, true] {
            let (e, limited, server) = read_budget_engine("/fapi/v1/time", "2386").await;
            e.exchange.sync_time().await.unwrap();
            let account_at = e.snapshot().await.account_at;
            e.manage_positions().await.unwrap();
            e.refresh_market().await.unwrap();
            {
                let i = e.inner.lock().await;
                assert!(!i.paused);
                assert!(i.account_sync_failed);
                assert_eq!(i.account_at, account_at);
                assert!(!i.scan_allowed(Mode::Live, now_ms()));
                assert!(i.health.contains("权重额度"));
            }
            assert!(
                e.set_running(true)
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("账户尚未对账")
            );
            if manual_pause {
                e.set_running(false).await.unwrap();
            }
            limited.store(false, std::sync::atomic::Ordering::SeqCst);
            e.exchange.expire_mock_weight_window().await;
            e.manage_positions().await.unwrap();
            let i = e.inner.lock().await;
            assert!(!i.account_sync_failed);
            assert_eq!(i.paused, manual_pause);
            assert_eq!(i.scan_allowed(Mode::Live, now_ms()), !manual_pause);
            server.abort();
        }
    }
    #[tokio::test]
    async fn funding_budget_wait_blocks_entries_until_funding_is_reconciled() {
        let (e, limited, server) = read_budget_engine("/fapi/v2/account", "2155").await;
        e.inner.lock().await.book.funding_last_poll = 0;
        e.manage_positions().await.unwrap();
        e.refresh_market().await.unwrap();
        {
            let i = e.inner.lock().await;
            assert!(!i.paused);
            assert!(!i.account_sync_failed);
            assert!(i.funding_sync_pending);
            assert!(!i.scan_allowed(Mode::Live, now_ms()));
            assert_eq!(i.book.funding_last_poll, 0);
        }
        assert!(e.snapshot().await.entry_data_pending);
        assert!(
            e.set_running(true)
                .await
                .unwrap_err()
                .to_string()
                .contains("费用尚未核实")
        );
        limited.store(false, std::sync::atomic::Ordering::SeqCst);
        e.exchange.expire_mock_weight_window().await;
        e.manage_positions().await.unwrap();
        let i = e.inner.lock().await;
        assert!(!i.paused);
        assert!(!i.funding_sync_pending);
        assert!(i.book.funding_last_poll > 0);
        assert!(i.scan_allowed(Mode::Live, now_ms()));
        server.abort();
    }
    #[tokio::test]
    async fn fill_budget_wait_preserves_quantity_and_blocks_entries_until_fees_are_confirmed() {
        let (e, limited, server) = read_budget_engine("/fapi/v1/time", "2399").await;
        {
            let mut i = e.inner.lock().await;
            let o = entry(Action::Long);
            i.book.pending.insert(o.client_id.clone(), o);
            e.save(&mut i).unwrap();
        }
        let remote: ExchangeOrder = serde_json::from_value(json!({"symbol":"XUSDT", "clientOrderId":"test-entry", "orderId":42,
            "status":"FILLED", "executedQty":"1", "origQty":"1", "avgPrice":"100", "cumQuote":"100"})).unwrap();
        e.exchange.sync_time().await.unwrap();
        e.apply_order(remote).await.unwrap();
        {
            let i = e.inner.lock().await;
            assert!(!i.paused);
            assert_eq!(i.book.pending["test-entry"].status, "SETTLING");
            assert_eq!(i.book.positions["XUSDT"].quantity, Decimal::ONE);
            assert_eq!(i.book.positions["XUSDT"].entry_price, d("100"));
            assert!(!i.book.positions["XUSDT"].accounting_complete);
            assert!(!i.scan_allowed(Mode::Live, now_ms()));
        }
        limited.store(false, std::sync::atomic::Ordering::SeqCst);
        e.exchange.expire_mock_weight_window().await;
        e.reconcile_pending().await.unwrap();
        let i = e.inner.lock().await;
        assert!(!i.paused);
        assert!(i.book.pending.is_empty());
        assert_eq!(i.book.positions["XUSDT"].quantity, Decimal::ONE);
        assert_eq!(i.book.positions["XUSDT"].entry_fee, d("0.05"));
        assert!(i.book.positions["XUSDT"].accounting_complete);
        assert!(i.scan_allowed(Mode::Live, now_ms()));
        server.abort();
    }
    #[tokio::test]
    async fn order_without_price_fields_uses_confirmed_fills_and_never_resubmits() {
        use axum::{Json, Router, routing::get};
        use std::sync::atomic::{AtomicUsize, Ordering};
        let submissions = Arc::new(AtomicUsize::new(0));
        let count = submissions.clone();
        let filled = Arc::new(Mutex::new(false));
        let state = filled.clone();
        let response = || {
            json!({"symbol":"XUSDT", "clientOrderId":"test-entry", "orderId":42,
            "status":"FILLED", "executedQty":"1", "origQty":"1", "cumQty":"1"})
        };
        let app = Router::new()
            .route("/fapi/v1/order", get(move || async move { Json(response()) }).post(move || {
                let count = count.clone(); async move { count.fetch_add(1, Ordering::SeqCst); Json(response()) }
            }))
            .route("/fapi/v1/userTrades", get(move || { let state = state.clone(); async move {
                Json(if *state.lock().await {
                    json!([{"id":1,"orderId":42,"qty":"0.4","price":"100","commission":"0.02","commissionAsset":"USDT","realizedPnl":"0","time":1,"side":"BUY"},
                        {"id":2,"orderId":42,"qty":"0.6","price":"101","commission":"0.03","commissionAsset":"USDT","realizedPnl":"0","time":1,"side":"BUY"}])
                } else { json!([]) })
            }}));
        let (base, server) = crate::test_support::mock_server(app).await;
        let e = Engine::new(
            Mode::Live,
            Store::open(":memory:", Mode::Live).unwrap(),
            config(),
            Binance::mock(base),
            Jev::new().unwrap(),
        )
        .unwrap();
        let o = entry(Action::Long);
        {
            let mut i = e.inner.lock().await;
            i.paused = false;
            i.book.pending.insert(o.client_id.clone(), o.clone());
            e.save(&mut i).unwrap();
        }
        // No price is fabricated when the response and fills cannot establish one.
        assert!(
            e.submit_order(o)
                .await
                .unwrap_err()
                .to_string()
                .contains("成交均价尚未确认")
        );
        assert!(e.snapshot().await.positions.is_empty());
        assert_eq!(
            e.store.load().unwrap().unwrap().1.pending["test-entry"].status,
            "SUBMITTING"
        );
        *filled.lock().await = true;
        e.reconcile_pending().await.unwrap();
        e.reconcile_pending().await.unwrap();
        let i = e.inner.lock().await;
        assert!(!i.paused);
        assert!(i.book.pending.is_empty());
        assert_eq!(i.book.positions["XUSDT"].quantity, Decimal::ONE);
        assert_eq!(i.book.positions["XUSDT"].entry_price, d("100.6"));
        assert_eq!(i.book.positions["XUSDT"].entry_fee, d("0.05"));
        assert_eq!(i.book.positions["XUSDT"].observed_trade_ids.len(), 2);
        assert!(i.book.positions["XUSDT"].accounting_complete);
        assert_eq!(submissions.load(Ordering::SeqCst), 1);
        server.abort();
    }
    #[tokio::test]
    async fn pending_queries_overlap_but_reports_apply_in_order_and_failed_orders_stay_locked() {
        use axum::{Json, Router, extract::Query, http::StatusCode, routing::get};
        use tokio::sync::{Semaphore, mpsc};
        let gates: Arc<Vec<_>> = Arc::new((0..6).map(|_| Semaphore::new(0)).collect());
        let (started, mut starts) = mpsc::channel(6);
        let (finished, mut finishes) = mpsc::channel(6);
        let controls = gates.clone();
        let app = Router::new().route("/fapi/v1/order", get(move |Query(query): Query<BTreeMap<String, String>>| {
            let controls = controls.clone(); let started = started.clone(); let finished = finished.clone();
            async move {
                let id: usize = query["origClientOrderId"].parse().unwrap();
                started.send(id).await.unwrap();
                controls[id].acquire().await.unwrap().forget();
                finished.send(id).await.unwrap();
                if id == 3 {
                    (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"code":-1000})))
                } else {
                    (StatusCode::OK, Json(json!({"symbol":query["symbol"],"clientOrderId":query["origClientOrderId"],"orderId":id+1,"status":"NEW","executedQty":"0","avgPrice":"0","origQty":"1","cumQuote":"0"})))
                }
            }
        }));
        let (base, server) = crate::test_support::mock_server(app).await;
        let e = Engine::new(
            Mode::Testnet,
            Store::open(":memory:", Mode::Testnet).unwrap(),
            config(),
            Binance::mock(base),
            Jev::new().unwrap(),
        )
        .unwrap();
        {
            let mut i = e.inner.lock().await;
            for id in 0..6 {
                let mut order = entry(Action::Long);
                order.client_id = id.to_string();
                order.symbol = format!("S{id}USDT");
                order.status = if id == 3 { "UNKNOWN" } else { "SUBMITTING" }.into();
                i.book.pending.insert(order.client_id.clone(), order);
            }
            e.save(&mut i).unwrap();
        }
        let reconcile = tokio::spawn({
            let e = e.clone();
            async move { e.reconcile_pending().await }
        });
        let mut active = Vec::new();
        for _ in 0..4 {
            active.push(
                tokio::time::timeout(Duration::from_secs(1), starts.recv())
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        active.sort();
        assert_eq!(active, vec![0, 1, 2, 3]);
        assert!(starts.try_recv().is_err());
        for id in [1, 2, 3] {
            gates[id].add_permits(1);
        }
        for _ in 0..3 {
            tokio::time::timeout(Duration::from_secs(1), finishes.recv())
                .await
                .unwrap()
                .unwrap();
        }
        // Later replies have arrived, but the first persisted intent must be applied first.
        assert_eq!(e.inner.lock().await.book.pending["1"].status, "SUBMITTING");
        assert!(starts.try_recv().is_err());
        gates[0].add_permits(1);
        for _ in 0..2 {
            let id = tokio::time::timeout(Duration::from_secs(1), starts.recv())
                .await
                .unwrap()
                .unwrap();
            assert!(matches!(id, 4 | 5));
            gates[id].add_permits(1);
        }
        tokio::time::timeout(Duration::from_secs(2), reconcile)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let saved = e.store.load().unwrap().unwrap().1;
        for id in 0..6 {
            assert_eq!(
                saved.pending[&id.to_string()].status,
                if id == 3 { "UNKNOWN" } else { "NEW" }
            );
        }
        assert!(e.inner.lock().await.occupied("S3USDT"));
        assert!(saved.positions.is_empty());
        server.abort();
    }
    #[tokio::test]
    async fn account_reads_overlap_and_only_publish_a_complete_snapshot() {
        use axum::{
            Json, Router,
            http::{StatusCode, Uri},
        };
        use tokio::sync::{Semaphore, mpsc};
        for fail in [false, true] {
            let release = Arc::new(Semaphore::new(0));
            let controls = release.clone();
            let (started, mut starts) = mpsc::channel(3);
            let app = Router::new().fallback(move |uri: Uri| {
                let release = controls.clone();let started = started.clone();
                async move {
                    let path = uri.path().to_owned();started.send(path.clone()).await.unwrap();
                    release.acquire().await.unwrap().forget();
                    if path == "/fapi/v2/account" {
                        if fail { (StatusCode::BAD_REQUEST, Json(json!({"code":-1000}))) }
                        else { (StatusCode::OK, Json(json!({"multiAssetsMargin":false,"availableBalance":"250","assets":[{"asset":"USDT","availableBalance":"250"}]}))) }
                    } else { (StatusCode::OK, Json(json!([]))) }
                }
            });
            let (base, server) = crate::test_support::mock_server(app).await;
            let e = Engine::new(
                Mode::Testnet,
                Store::open(":memory:", Mode::Testnet).unwrap(),
                config(),
                Binance::mock(base),
                Jev::new().unwrap(),
            )
            .unwrap();
            {
                let mut i = e.inner.lock().await;
                i.account_at = 123;
                i.available = d("100");
            }
            let sync = tokio::spawn({
                let e = e.clone();
                async move { e.sync_account().await }
            });
            let mut paths = Vec::new();
            for _ in 0..3 {
                paths.push(
                    tokio::time::timeout(Duration::from_secs(1), starts.recv())
                        .await
                        .unwrap()
                        .unwrap(),
                );
            }
            paths.sort();
            assert_eq!(
                paths,
                vec![
                    "/fapi/v1/openOrders",
                    "/fapi/v2/account",
                    "/fapi/v3/positionRisk"
                ]
            );
            assert_eq!(e.inner.lock().await.account_at, 123);
            release.add_permits(3);
            let result = tokio::time::timeout(Duration::from_secs(2), sync)
                .await
                .unwrap()
                .unwrap();
            let i = e.inner.lock().await;
            if fail {
                assert!(result.is_err());
                assert_eq!(i.account_at, 123);
                assert_eq!(i.available, d("100"));
                assert!(i.margin_balances.is_none());
            } else {
                result.unwrap();
                assert!(i.account_at > 123);
                assert_eq!(i.available, d("250"));
            }
            server.abort();
        }
    }
    #[tokio::test]
    async fn scan_rotates_unserved_coins_and_filters_ineligible_work_before_requests() {
        let e = paper();
        let mut i = e.inner.lock().await;
        for index in 0..200 {
            let symbol = format!("S{index:03}USDT");
            i.book.pool.insert(
                symbol.clone(),
                PoolCoin {
                    symbol,
                    enabled: true,
                    config: None,
                },
            );
        }
        let mut served = std::collections::BTreeSet::new();
        // Slow calls only allow eight dispatches per signal window. Even failed calls
        // must not send the next bar back to the alphabetically first eight coins.
        for round in 1..=25 {
            let now = round * BAR_MS + 10;
            let work = i.scan_work(now);
            for (coin, _) in work.iter().take(8) {
                served.insert(coin.symbol.clone());
                i.scan_cursor = Some(coin.symbol.clone());
            }
            let cursor = i.scan_cursor.clone();
            assert!(i.scan_work(round * BAR_MS + 60_000).is_empty());
            assert_eq!(i.scan_cursor, cursor);
        }
        assert_eq!(served.len(), 200);
        let now = 26 * BAR_MS + 10;
        let bar = now / BAR_MS * BAR_MS - 1;
        i.book.pool.get_mut("S000USDT").unwrap().enabled = false;
        i.external_positions.insert("S001USDT".into(), Decimal::ONE);
        i.book.last_bar.insert("S002USDT".into(), bar);
        i.book
            .cooldown_until
            .insert("S003USDT".into(), bar + BAR_MS);
        i.scan_cursor = Some("S010USDT".into());
        i.book.pool.remove("S010USDT");
        i.revision = 42;
        let work = i.scan_work(now);
        assert_eq!(work.len(), 195);
        assert_eq!(work.first().unwrap().0.symbol, "S011USDT");
        assert!(work.iter().all(|(_, revision)| *revision == 42));
        assert!(!work.iter().any(|(c, _)| matches!(
            c.symbol.as_str(),
            "S000USDT" | "S001USDT" | "S002USDT" | "S003USDT"
        )));
    }
    #[tokio::test]
    async fn repeated_exchange_books_do_not_refill_paper_depth_even_after_restart() {
        for side in [Action::Long, Action::Short] {
            let mut e = paper();
            seed(&e).await;
            e.inner
                .lock()
                .await
                .quotes
                .get_mut("XUSDT")
                .unwrap()
                .exchange_time = Some(100);
            e.paper_entry(entry(side)).await.unwrap();
            e.ensure_tp("XUSDT").await.unwrap();
            let id = e.inner.lock().await.book.positions["XUSDT"]
                .tp_client_id
                .clone()
                .unwrap();
            for (source_time, expected) in [
                (Some(100), "1"),
                (Some(101), "0.6"),
                (Some(101), "0.6"),
                (Some(99), "0.6"),
                (None, "0.6"),
                (Some(102), "0.2"),
                (Some(103), "0"),
            ] {
                tokio::time::sleep(Duration::from_millis(2)).await;
                {
                    let mut i = e.inner.lock().await;
                    let limit = i.book.pending[&id].price.unwrap();
                    i.book.pending.get_mut(&id).unwrap().created_at = now_ms() - 100;
                    let q = i.quotes.get_mut("XUSDT").unwrap();
                    q.time = now_ms() - 1;
                    q.exchange_time = source_time;
                    q.bid = if side == Action::Long {
                        limit + Decimal::ONE
                    } else {
                        limit - Decimal::from(2)
                    };
                    q.ask = q.bid + Decimal::ONE;
                    q.bid_qty = d("0.4");
                    q.ask_qty = d("0.4");
                }
                e.paper_match().await.unwrap();
                assert_eq!(
                    e.inner.lock().await.book.positions["XUSDT"].quantity,
                    d(expected)
                );
                if source_time == Some(101) && expected == "0.6" {
                    // Reload the persisted consumed source timestamp, not just in-memory state.
                    e = Engine::new(
                        Mode::Paper,
                        e.store.clone(),
                        config(),
                        Binance::new(Mode::Paper).unwrap(),
                        Jev::new().unwrap(),
                    )
                    .unwrap();
                    seed(&e).await;
                }
            }
            let saved = e.store.load().unwrap().unwrap().1;
            assert!(!saved.paper_match_times.contains_key(&id));
            assert!(!saved.paper_match_exchange_times.contains_key(&id));
        }
    }
    #[tokio::test]
    async fn legacy_paper_order_observes_source_baseline_before_filling() {
        let e = paper();
        seed(&e).await;
        e.paper_entry(entry(Action::Long)).await.unwrap();
        e.ensure_tp("XUSDT").await.unwrap();
        prepare_partial_quote(&e).await;
        e.inner
            .lock()
            .await
            .quotes
            .get_mut("XUSDT")
            .unwrap()
            .exchange_time = Some(100);
        e.paper_match().await.unwrap();
        assert_eq!(
            e.store.load().unwrap().unwrap().1.positions["XUSDT"].quantity,
            Decimal::ONE
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
        e.inner
            .lock()
            .await
            .quotes
            .get_mut("XUSDT")
            .unwrap()
            .exchange_time = Some(101);
        e.inner.lock().await.quotes.get_mut("XUSDT").unwrap().time = now_ms() - 1;
        e.paper_match().await.unwrap();
        assert_eq!(
            e.store.load().unwrap().unwrap().1.positions["XUSDT"].quantity,
            d("0.6")
        );
    }
    #[tokio::test]
    async fn older_exchange_quote_cannot_replace_prices_or_refresh_their_receipt_time() {
        use axum::{Json, Router, routing::get};
        let reply = Arc::new(Mutex::new(
            json!([{"symbol":"XUSDT","bidPrice":"100","askPrice":"101","bidQty":"1","askQty":"1","time":500}]),
        ));
        let body = reply.clone();
        let app = Router::new().route(
            "/fapi/v1/ticker/bookTicker",
            get(move || {
                let body = body.clone();
                async move { Json(body.lock().await.clone()) }
            }),
        );
        let (base, server) = crate::test_support::mock_server(app).await;
        let e = Engine::new(
            Mode::Paper,
            Store::open(":memory:", Mode::Paper).unwrap(),
            config(),
            Binance::mock(base),
            Jev::new().unwrap(),
        )
        .unwrap();
        seed(&e).await;
        e.inner.lock().await.book.pool.insert(
            "XUSDT".into(),
            PoolCoin {
                symbol: "XUSDT".into(),
                enabled: true,
                config: None,
            },
        );
        e.refresh_market().await.unwrap();
        let old_receipt = now_ms() - 100;
        e.inner.lock().await.quotes.get_mut("XUSDT").unwrap().time = old_receipt;
        reply.lock().await[0]["time"] = json!(499);
        reply.lock().await[0]["bidPrice"] = json!("90");
        e.refresh_market().await.unwrap();
        {
            let i = e.inner.lock().await;
            assert_eq!(i.quotes["XUSDT"].bid, d("100"));
            assert_eq!(i.quotes["XUSDT"].time, old_receipt);
        }
        reply.lock().await[0]["time"] = json!(500);
        reply.lock().await[0]["bidPrice"] = json!("100");
        e.refresh_market().await.unwrap();
        assert!(e.inner.lock().await.quotes["XUSDT"].time > old_receipt);
        reply.lock().await[0]["time"] = json!(501);
        reply.lock().await[0]["bidPrice"] = json!("101");
        e.refresh_market().await.unwrap();
        assert_eq!(
            e.inner.lock().await.quotes["XUSDT"].exchange_time,
            Some(501)
        );
        assert_eq!(e.inner.lock().await.quotes["XUSDT"].bid, d("101"));
        server.abort();
    }
    #[tokio::test]
    async fn connected_quotes_skip_rest_and_stale_symbols_are_recovered_without_erasing_other_quotes()
     {
        use axum::{Json, Router, extract::Query, routing::get};
        let calls = Arc::new(Mutex::new(Vec::new()));
        let observed = calls.clone();
        let app=Router::new().route("/fapi/v1/ticker/bookTicker",get(move |Query(query):Query<BTreeMap<String,String>>| {
            let observed=observed.clone();async move {
                let symbol=query["symbol"].clone();observed.lock().await.push(symbol.clone());
                Json(json!({"symbol":symbol,"bidPrice":"100","askPrice":"101","bidQty":"1","askQty":"1","time":101,"lastUpdateId":11}))
            }
        }));
        let (base, server) = crate::test_support::mock_server(app).await;
        let e = Engine::new(
            Mode::Paper,
            Store::open(":memory:", Mode::Paper).unwrap(),
            config(),
            Binance::mock(base),
            Jev::new().unwrap(),
        )
        .unwrap();
        seed(&e).await;
        {
            let mut i = e.inner.lock().await;
            for symbol in ["XUSDT", "YUSDC"] {
                i.book.pool.insert(
                    symbol.into(),
                    PoolCoin {
                        symbol: symbol.into(),
                        enabled: true,
                        config: None,
                    },
                );
            }
            let q = i.quotes.get_mut("XUSDT").unwrap();
            q.exchange_time = Some(100);
            q.update_id = Some(10);
            let mut q = q.clone();
            q.time = now_ms() - 16_000;
            i.quotes.insert("YUSDC".into(), q);
        }
        e.market_stream.lock().await.connected = true;
        e.refresh_market().await.unwrap();
        assert_eq!(*calls.lock().await, vec!["YUSDC"]);
        e.refresh_market().await.unwrap();
        assert_eq!(calls.lock().await.len(), 1);
        let mut quote = e.inner.lock().await.quotes["XUSDT"].clone();
        assert!(!e.apply_market_quote("ZUSDT".into(), quote.clone()).await);
        let old_receipt = quote.time;
        quote.time = now_ms();
        quote.bid = d("90");
        assert!(!e.apply_market_quote("XUSDT".into(), quote.clone()).await);
        assert_eq!(e.inner.lock().await.quotes["XUSDT"].time, old_receipt);
        quote.update_id = Some(11);
        assert!(e.apply_market_quote("XUSDT".into(), quote).await);
        assert_eq!(e.inner.lock().await.quotes["XUSDT"].bid, d("90"));
        e.inner.lock().await.quotes.get_mut("YUSDC").unwrap().time = now_ms() - 16_000;
        e.inner.lock().await.market_health(Mode::Paper);
        assert!(now_ms() - e.snapshot().await.market_at >= 15_000);
        e.market_stream.lock().await.connected = false;
        e.refresh_market().await.unwrap();
        assert_eq!(*calls.lock().await, vec!["YUSDC", "XUSDT", "YUSDC"]);
        assert_eq!(e.inner.lock().await.quotes.len(), 2);
        server.abort();
    }
    #[tokio::test]
    async fn market_budget_wait_preserves_scan_intent_and_retries_without_bypassing_freshness() {
        use axum::{Json, Router, routing::get};
        use std::sync::atomic::{AtomicUsize, Ordering};
        for mode in [Mode::Paper, Mode::Testnet] {
            let calls = Arc::new(AtomicUsize::new(0));
            let observed = calls.clone();
            let app = Router::new()
                .route(
                    "/fapi/v1/time",
                    get(|| async {
                        (
                            [("x-mbx-used-weight-1m", "2159")],
                            Json(json!({"serverTime": now_ms()})),
                        )
                    }),
                )
                .route(
                    "/fapi/v1/exchangeInfo",
                    get(|| async {
                        Json(json!({"symbols": [], "rateLimits": [{
                            "rateLimitType": "REQUEST_WEIGHT", "interval": "MINUTE",
                            "intervalNum": 1, "limit": 10000
                        }]}))
                    }),
                )
                .route("/fapi/v1/ticker/24hr", get(|| async { Json(json!([])) }))
                .route(
                    "/fapi/v1/ticker/bookTicker",
                    get(move || {
                        let observed = observed.clone();
                        async move {
                            observed.fetch_add(1, Ordering::SeqCst);
                            Json(
                                json!({"symbol": "XUSDT", "bidPrice": "100", "askPrice": "100",
                                "bidQty": "1", "askQty": "1", "time": 101, "lastUpdateId": 11}),
                            )
                        }
                    }),
                );
            let (base, server) = crate::test_support::mock_server(app).await;
            let e = Engine::new(
                mode,
                Store::open(":memory:", mode).unwrap(),
                config(),
                Binance::mock(base),
                Jev::new().unwrap(),
            )
            .unwrap();
            seed(&e).await;
            {
                let mut i = e.inner.lock().await;
                i.book.pool.insert(
                    "XUSDT".into(),
                    PoolCoin {
                        symbol: "XUSDT".into(),
                        enabled: true,
                        config: None,
                    },
                );
                i.paused = false;
                i.account_at = now_ms();
            }
            e.exchange.sync_time().await.unwrap();
            e.refresh_market().await.unwrap();
            let retry_at = {
                let i = e.inner.lock().await;
                assert!(!i.paused);
                assert_eq!(i.revision, 0);
                assert!(i.scan_allowed(mode, now_ms()));
                i.market_retry_at.unwrap()
            };
            assert!(retry_at > Instant::now());
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            let events = e.store.events(10).unwrap();
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].level, "warn");
            assert!(events[0].message.contains("自动重试"));
            assert!(events[0].detail["retry_after_ms"].as_u64().unwrap() > 0);

            // Waiting neither retries admission every tick nor renews old quotes.
            e.inner.lock().await.quotes.get_mut("XUSDT").unwrap().time = now_ms() - 16_000;
            e.refresh_market().await.unwrap();
            {
                let i = e.inner.lock().await;
                assert!(!i.paused);
                assert!(!i.scan_allowed(mode, now_ms()));
                assert!(now_ms() - i.market_at >= 15_000);
            }
            assert_eq!(e.exchange.rate_limit_snapshot().await.budget_rejections, 1);
            assert_eq!(e.store.events(10).unwrap().len(), 1);

            // Fresh WebSocket data resumes scanning even while REST awaits quota.
            let mut quote = e.inner.lock().await.quotes["XUSDT"].clone();
            quote.time = now_ms();
            quote.exchange_time = Some(100);
            quote.update_id = Some(10);
            assert!(e.apply_market_quote("XUSDT".into(), quote).await);
            e.refresh_market().await.unwrap();
            assert!(e.inner.lock().await.scan_allowed(mode, now_ms()));
            assert_eq!(calls.load(Ordering::SeqCst), 0);

            // Make quota available through a real exchangeInfo response, then
            // simulate the retry deadline passing without a minute-long sleep.
            e.exchange.symbols().await.unwrap();
            e.refresh_market().await.unwrap();
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            e.inner.lock().await.market_retry_at = Some(Instant::now());
            e.refresh_market().await.unwrap();
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            {
                let i = e.inner.lock().await;
                assert!(i.market_retry_at.is_none());
                assert!(!i.paused);
                assert_eq!(i.quotes["XUSDT"].update_id, Some(11));
                assert!(i.scan_allowed(mode, now_ms()));
            }

            // A user pause during the wait survives both pushes and REST recovery.
            e.inner.lock().await.market_retry_at = Some(retry_at);
            e.set_running(false).await.unwrap();
            e.refresh_market().await.unwrap();
            assert!(e.snapshot().await.paused);
            e.inner.lock().await.market_retry_at = Some(Instant::now());
            e.refresh_market().await.unwrap();
            assert!(e.snapshot().await.paused);
            assert_eq!(calls.load(Ordering::SeqCst), 2);
            server.abort();
        }
    }
    #[tokio::test]
    async fn market_http_failure_and_cooldown_still_require_manual_restart() {
        use axum::{Json, Router, http::StatusCode, routing::get};
        let app = Router::new().route(
            "/fapi/v1/ticker/bookTicker",
            get(|| async { (StatusCode::TOO_MANY_REQUESTS, Json(json!({"code": -1003}))) }),
        );
        let (base, server) = crate::test_support::mock_server(app).await;
        let e = Engine::new(
            Mode::Paper,
            Store::open(":memory:", Mode::Paper).unwrap(),
            config(),
            Binance::mock(base),
            Jev::new().unwrap(),
        )
        .unwrap();
        seed(&e).await;
        e.set_pool(PoolCoin {
            symbol: "XUSDT".into(),
            enabled: true,
            config: None,
        })
        .await
        .unwrap();
        for _ in 0..2 {
            e.inner.lock().await.paused = false;
            let error = e.refresh_market().await.unwrap_err();
            assert!(
                error
                    .downcast_ref::<ExchangeError>()
                    .unwrap()
                    .budget_retry_after()
                    .is_none()
            );
            e.fault("行情同步失败", error).await;
            assert!(e.snapshot().await.paused);
            assert!(e.inner.lock().await.market_retry_at.is_none());
        }
        server.abort();
    }
    #[tokio::test]
    async fn market_subscriptions_include_removed_positions_and_unknown_pending_orders() {
        let e = paper();
        seed(&e).await;
        e.paper_entry(entry(Action::Long)).await.unwrap();
        let mut pending = entry(Action::Short);
        pending.symbol = "ZUSDC".into();
        pending.status = "UNKNOWN".into();
        {
            let mut i = e.inner.lock().await;
            i.book.pool.insert(
                "YUSDT".into(),
                PoolCoin {
                    symbol: "YUSDT".into(),
                    enabled: false,
                    config: None,
                },
            );
            i.book.pending.insert(pending.client_id.clone(), pending);
        }
        assert_eq!(
            e.market_symbols().await,
            BTreeSet::from(["XUSDT".into(), "YUSDT".into(), "ZUSDC".into()])
        );
    }
    #[tokio::test]
    async fn paper_book_update_ids_allow_same_millisecond_updates_without_refilling_after_restart()
    {
        let e = paper();
        seed(&e).await;
        {
            let mut i = e.inner.lock().await;
            let q = i.quotes.get_mut("XUSDT").unwrap();
            q.exchange_time = Some(100);
            q.update_id = Some(1);
        }
        e.paper_entry(entry(Action::Long)).await.unwrap();
        e.ensure_tp("XUSDT").await.unwrap();
        prepare_partial_quote(&e).await;
        e.inner
            .lock()
            .await
            .quotes
            .get_mut("XUSDT")
            .unwrap()
            .update_id = Some(2);
        e.paper_match().await.unwrap();
        assert_eq!(
            e.inner.lock().await.book.positions["XUSDT"].quantity,
            d("0.6")
        );
        let restored = Engine::new(
            Mode::Paper,
            e.store.clone(),
            config(),
            Binance::new(Mode::Paper).unwrap(),
            Jev::new().unwrap(),
        )
        .unwrap();
        seed(&restored).await;
        prepare_partial_quote(&restored).await;
        {
            let mut i = restored.inner.lock().await;
            let q = i.quotes.get_mut("XUSDT").unwrap();
            q.exchange_time = Some(100);
            q.update_id = Some(2);
        }
        restored.paper_match().await.unwrap();
        assert_eq!(
            restored.inner.lock().await.book.positions["XUSDT"].quantity,
            d("0.6")
        );
        restored
            .inner
            .lock()
            .await
            .quotes
            .get_mut("XUSDT")
            .unwrap()
            .update_id = Some(3);
        restored.paper_match().await.unwrap();
        assert_eq!(
            restored.inner.lock().await.book.positions["XUSDT"].quantity,
            d("0.2")
        );
        restored.manual_close("XUSDT").await.unwrap();
        assert!(
            restored
                .store
                .load()
                .unwrap()
                .unwrap()
                .1
                .paper_match_update_ids
                .is_empty()
        );
    }
    #[tokio::test]
    async fn unchanged_polling_skips_writes_but_partial_and_complete_fills_persist() {
        let e = paper();
        let writes = || {
            e.store
                .with_test_connection(rusqlite::Connection::total_changes)
        };
        let before = writes();
        e.manage_positions().await.unwrap();
        e.scan().await;
        assert_eq!(writes(), before);
        let runtime = e.snapshot().await.runtime;
        assert_eq!(runtime.scan.runs, 1);
        assert_eq!(runtime.position_management.runs, 1);
        assert!(!runtime.scan.running);
        seed(&e).await;
        e.paper_entry(entry(Action::Long)).await.unwrap();
        e.ensure_tp("XUSDT").await.unwrap();
        let before = writes();
        e.manage_positions().await.unwrap();
        assert_eq!(writes(), before);
        prepare_partial_quote(&e).await;
        e.paper_match().await.unwrap();
        assert_eq!(writes(), before + 1);
        assert_eq!(
            e.store.load().unwrap().unwrap().1.positions["XUSDT"].quantity,
            d("0.6")
        );
        let before = writes();
        e.paper_match().await.unwrap();
        e.finalize_closed().await.unwrap();
        assert_eq!(writes(), before);
        {
            let mut i = e.inner.lock().await;
            let quote = i.quotes.get_mut("XUSDT").unwrap();
            quote.bid_qty = 1.into();
            quote.time = now_ms() - 1;
        }
        e.paper_match().await.unwrap();
        e.finalize_closed().await.unwrap();
        let restored = e.store.load().unwrap().unwrap().1;
        assert!(restored.positions.is_empty());
        assert!(restored.pending.is_empty());
        assert_eq!(restored.closed_trades.len(), 1);
        let before = writes();
        e.manage_positions().await.unwrap();
        assert_eq!(writes(), before);
    }
    async fn prepare_partial_quote(e: &Engine) {
        let mut i = e.inner.lock().await;
        let id = i.book.positions["XUSDT"].tp_client_id.clone().unwrap();
        let price = i.book.pending[&id].price.unwrap();
        i.book.pending.get_mut(&id).unwrap().created_at = now_ms() - 100;
        let quote = i.quotes.get_mut("XUSDT").unwrap();
        quote.bid = price + Decimal::ONE;
        quote.ask = price + Decimal::from(2);
        quote.bid_qty = d("0.4");
        quote.time = now_ms() - 10;
    }
    #[tokio::test]
    async fn unchanged_order_reports_skip_writes_but_status_changes_still_persist() {
        let e = paper();
        {
            let mut i = e.inner.lock().await;
            let order = entry(Action::Long);
            i.book.pending.insert(order.client_id.clone(), order);
            e.save(&mut i).unwrap();
        }
        let mut remote: ExchangeOrder = serde_json::from_value(json!({
            "symbol":"XUSDT", "clientOrderId":"test-entry", "orderId":42,
            "status":"NEW", "executedQty":"0", "avgPrice":"0",
            "origQty":"1", "cumQuote":"0"
        }))
        .unwrap();
        e.apply_order(remote.clone()).await.unwrap();
        assert_eq!(
            e.store.load().unwrap().unwrap().1.pending["test-entry"].status,
            "NEW"
        );
        let before = e
            .store
            .with_test_connection(rusqlite::Connection::total_changes);
        e.apply_order(remote.clone()).await.unwrap();
        assert_eq!(
            e.store
                .with_test_connection(rusqlite::Connection::total_changes),
            before
        );
        remote.status = "CANCELED".into();
        e.apply_order(remote).await.unwrap();
        assert!(e.store.load().unwrap().unwrap().1.pending.is_empty());
    }
    #[tokio::test]
    async fn failed_saves_are_retried_even_when_the_next_poll_has_no_changes() {
        let e = paper();
        seed(&e).await;
        e.paper_entry(entry(Action::Long)).await.unwrap();
        e.ensure_tp("XUSDT").await.unwrap();
        prepare_partial_quote(&e).await;
        e.store
            .with_test_connection(|c| {
                c.execute_batch(
                    "CREATE TEMP TRIGGER reject_portfolio_save BEFORE INSERT ON portfolios
             BEGIN SELECT RAISE(FAIL, 'injected write failure'); END;",
                )
            })
            .unwrap();
        assert!(e.paper_match().await.is_err());
        assert!(e.inner.lock().await.persistence_pending);
        assert_eq!(
            e.store.load().unwrap().unwrap().1.positions["XUSDT"].quantity,
            Decimal::ONE
        );
        assert!(e.manage_positions().await.is_err());
        assert_eq!(
            e.snapshot().await.runtime.position_management.failed_runs,
            1
        );
        e.store
            .with_test_connection(|c| c.execute_batch("DROP TRIGGER reject_portfolio_save"))
            .unwrap();
        // The quote was consumed in memory: this poll must only retry persistence.
        e.paper_match().await.unwrap();
        assert!(!e.inner.lock().await.persistence_pending);
        assert_eq!(
            e.store.load().unwrap().unwrap().1.positions["XUSDT"].quantity,
            d("0.6")
        );
        let before = e
            .store
            .with_test_connection(rusqlite::Connection::total_changes);
        e.paper_match().await.unwrap();
        assert_eq!(
            e.store
                .with_test_connection(rusqlite::Connection::total_changes),
            before
        );
    }
    #[tokio::test]
    async fn changing_jev_credentials_does_not_wait_for_an_in_flight_decision() {
        use crate::application::Application;
        use axum::{Json, Router, routing::post};
        use tokio::sync::Notify;
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let app = Router::new().route(
            "/v1/systemone",
            post({
                let started = started.clone();
                let release = release.clone();
                move || {
                    let started = started.clone();
                    let release = release.clone();
                    async move {
                        started.notify_one();
                        release.notified().await;
                        Json(json!({"model":"slow-test-model", "answers":{"action":{
                            "type":"choice", "choice":"LONG", "confidence":0.9,
                            "probabilities":{"LONG":0.9,"SHORT":0.05,"WAIT":0.05}
                        }}}))
                    }
                }
            }),
        );
        let (base, server) = crate::test_support::mock_server(app).await;
        let e = Engine::new(
            Mode::Paper,
            Store::open(":memory:", Mode::Paper).unwrap(),
            config(),
            Binance::new(Mode::Paper).unwrap(),
            Jev::mock(base),
        )
        .unwrap();
        let app = Application::from_engine(e.clone());
        e.inner.lock().await.paused = false;
        let decision = tokio::spawn({
            let e = e.clone();
            async move {
                let end = now_ms() / BAR_MS * BAR_MS;
                let bars: Vec<_> = (0..120)
                    .map(|index| {
                        let open_time = end - (120 - index) * BAR_MS;
                        Candle {
                            open_time,
                            close_time: open_time + BAR_MS - 1,
                            open: 100.0,
                            high: 101.0,
                            low: 99.0,
                            close: 100.0,
                            volume: 1.0,
                        }
                    })
                    .collect();
                let quote = Quote {
                    bid: 100.into(),
                    ask: 100.into(),
                    bid_qty: 1.into(),
                    ask_qty: 1.into(),
                    time: now_ms(),
                    exchange_time: None,
                    update_id: None,
                };
                e.decide("XUSDT", &bars, &quote, &config()).await.unwrap()
            }
        });
        tokio::time::timeout(Duration::from_secs(2), started.notified())
            .await
            .unwrap();
        tokio::time::timeout(
            Duration::from_secs(1),
            app.configure_connections(
                serde_json::from_value(json!({"jev_key":"replacement-test-key"})).unwrap(),
            ),
        )
        .await
        .expect("saving credentials must not wait for the old model response")
        .unwrap();
        assert!(!decision.is_finished());
        let snapshot =
            tokio::time::timeout(Duration::from_secs(1), app.engine.read().await.snapshot())
                .await
                .unwrap();
        assert!(snapshot.paused);
        assert!(snapshot.jev_available);
        assert_eq!(
            app.store.settings().unwrap().jev_key,
            "replacement-test-key"
        );
        release.notify_one();
        assert_eq!(decision.await.unwrap().action, Action::Long);
        assert!(e.snapshot().await.pending.is_empty());
        server.abort();
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
            stop_loss_usdt: Decimal::ZERO,
            maker_fee: d("0.0002"),
            taker_fee: d("0.0005"),
            status: "SUBMITTING".into(),
            accounted_quantity: Decimal::ZERO,
            settlement_issue: None,
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
                exchange_time: None,
                update_id: None,
            },
        );
        i.market_at = now_ms();
    }
    #[tokio::test]
    async fn paper_stop_loss_closes_long_short_and_both_assets_while_paused_and_removed() {
        for side in [Action::Long, Action::Short] {
            for symbol in ["XUSDT", "XUSDC"] {
                let e = paper();
                seed(&e).await;
                if symbol == "XUSDC" {
                    let mut i = e.inner.lock().await;
                    let mut rule = i.catalog.remove("XUSDT").unwrap();
                    rule.symbol = symbol.into();
                    i.catalog.insert(symbol.into(), rule);
                    let quote = i.quotes.remove("XUSDT").unwrap();
                    i.quotes.insert(symbol.into(), quote);
                }
                e.set_pool(PoolCoin {
                    symbol: symbol.into(),
                    enabled: true,
                    config: None,
                })
                .await
                .unwrap();
                let mut order = entry(side);
                order.symbol = symbol.into();
                order.stop_loss_usdt = d("0.5");
                e.paper_entry(order).await.unwrap();
                e.ensure_tp(symbol).await.unwrap();
                let mut changed = config();
                changed.stop_loss_usdt = d("10");
                changed.taker_fee = d("0.01");
                e.configure(changed).await.unwrap();
                let expected = {
                    let mut i = e.inner.lock().await;
                    let p = i.book.positions.get_mut(symbol).unwrap();
                    // The remainder after an earlier partial exit still belongs to the same trade.
                    p.quantity = d("0.4");
                    p.realized_gross = d("0.3");
                    p.exit_fee = d("0.012");
                    p.funding_cost = d("0.07");
                    assert_eq!(p.stop_loss_usdt, d("0.5"));
                    assert_eq!(p.taker_fee, d("0.0005"));
                    let price = if side == Action::Long {
                        d("98")
                    } else {
                        d("102")
                    };
                    let expected = p.estimated_exit_net(price);
                    let quote = i.quotes.get_mut(symbol).unwrap();
                    quote.bid = if side == Action::Long { price } else { d("99") };
                    quote.ask = if side == Action::Short {
                        price
                    } else {
                        d("101")
                    };
                    quote.time = now_ms();
                    expected
                };
                e.remove_coin(symbol).await.unwrap();
                e.set_running(false).await.unwrap();
                e.manage_positions().await.unwrap();
                e.manage_positions().await.unwrap();
                let i = e.inner.lock().await;
                assert!(i.book.positions.is_empty());
                assert!(i.book.pending.is_empty());
                assert_eq!(i.book.closed_trades.len(), 1);
                assert_eq!(i.book.closed_trades[0].net_profit, expected);
                assert!(i.paused);
            }
        }
    }
    #[tokio::test]
    async fn stop_loss_requires_fresh_quotes_and_triggers_at_the_exact_net_boundary() {
        let e = paper();
        seed(&e).await;
        e.paper_entry(entry(Action::Long)).await.unwrap();
        {
            let mut i = e.inner.lock().await;
            let q = i.quotes.get_mut("XUSDT").unwrap();
            // Mid-price suggests profit; the executable bid and closing fee produce a loss.
            q.bid = d("99.4");
            q.ask = d("101");
            q.time = now_ms() - 15_001;
        }
        assert!(
            !e.ensure_stop_loss("XUSDT", ExitCheck::Account)
                .await
                .unwrap()
        ); // Disabled by default.
        e.inner
            .lock()
            .await
            .book
            .positions
            .get_mut("XUSDT")
            .unwrap()
            .stop_loss_usdt = d("0.6998");
        assert!(
            e.ensure_stop_loss("XUSDT", ExitCheck::Account)
                .await
                .unwrap_err()
                .to_string()
                .contains("盘口过期")
        );
        assert!(!e.inner.lock().await.book.positions["XUSDT"].stop_loss_triggered);
        e.inner.lock().await.quotes.get_mut("XUSDT").unwrap().time = now_ms() + 30_000;
        assert!(
            e.ensure_stop_loss("XUSDT", ExitCheck::Account)
                .await
                .is_err()
        );
        e.inner.lock().await.quotes.get_mut("XUSDT").unwrap().time = now_ms();
        assert!(
            !e.ensure_stop_loss("XUSDT", ExitCheck::Account)
                .await
                .unwrap()
        );
        e.inner
            .lock()
            .await
            .book
            .positions
            .get_mut("XUSDT")
            .unwrap()
            .stop_loss_usdt = d("0.6997");
        assert!(
            e.ensure_stop_loss("XUSDT", ExitCheck::Account)
                .await
                .unwrap()
        );
        assert_eq!(
            e.inner.lock().await.book.closed_trades[0].net_profit,
            d("-0.6997")
        );
    }
    #[tokio::test]
    async fn live_stop_loss_confirms_cancellation_and_handles_partial_or_full_fill_races() {
        use axum::{Json, Router, extract::Query, http::StatusCode, routing::get};
        use std::collections::HashMap;
        use std::sync::atomic::{AtomicUsize, Ordering};
        for (status, filled) in [("NEW", "0"), ("CANCELED", "0.4"), ("FILLED", "1")] {
            let submissions = Arc::new(AtomicUsize::new(0));
            let counter = submissions.clone();
            let app = Router::new()
                .route("/fapi/v1/order", get(|| async { Json(json!({})) })
                    .delete(move || async move { Json(json!({"symbol":"XUSDT","clientOrderId":"test-tp","orderId":42,"status":status,"executedQty":filled,"avgPrice":"101","origQty":"1","cumQuote":"101"})) })
                    .post(move |body: String| {
                        let counter = counter.clone();
                        async move {
                            counter.fetch_add(1, Ordering::SeqCst);
                            let form: HashMap<_, _> = url::form_urlencoded::parse(body.as_bytes()).into_owned().collect();
                            assert_eq!(form["type"], "MARKET");
                            assert_eq!(form["side"], "SELL");
                            assert_eq!(form["positionSide"], "BOTH");
                            assert_eq!(form["reduceOnly"], "true");
                            assert_eq!(form["quantity"], "0.6");
                            assert!(form["newClientOrderId"].starts_with("gt-s-"));
                            (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"code":-1007,"msg":"unknown execution"})))
                        }
                    }))
                .route("/fapi/v1/userTrades", get(move |Query(q): Query<HashMap<String, String>>| async move {
                    let stop = q["orderId"] == "43";
                    Json(json!([{"id":if stop {2} else {1},"orderId":if stop {43} else {42},
                        "qty":if stop {"0.6"} else {filled}, "price":if stop {"98"} else {"101"},
                        "commission":if stop {"0.0294"} else if filled == "1" {"0.0202"} else {"0.00808"},
                        "commissionAsset":"USDT","realizedPnl":if stop {"-1.2"} else if filled == "1" {"1"} else {"0.4"},
                        "time":1,"side":"SELL"}]))
                }));
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
            let mut order = entry(Action::Long);
            order.stop_loss_usdt = d("0.5");
            // Seed a local position; all exchange requests in this test use the mock server.
            e.paper_entry(order.clone()).await.unwrap();
            {
                let mut i = e.inner.lock().await;
                order.client_id = "test-tp".into();
                order.role = OrderRole::TakeProfit;
                order.price = Some(d("101"));
                order.status = "NEW".into();
                i.book.pending.insert(order.client_id.clone(), order);
                let p = i.book.positions.get_mut("XUSDT").unwrap();
                p.tp_client_id = Some("test-tp".into());
                p.tp_price = Some(d("101"));
                i.account_at = now_ms();
                let q = i.quotes.get_mut("XUSDT").unwrap();
                q.bid = d("99");
                q.ask = d("99");
            }
            let result = e.ensure_stop_loss("XUSDT", ExitCheck::Account).await;
            if status == "NEW" {
                assert!(result.unwrap_err().to_string().contains("未确认撤销"));
                assert_eq!(submissions.load(Ordering::SeqCst), 0);
                assert_eq!(
                    e.inner.lock().await.book.positions["XUSDT"].quantity,
                    Decimal::ONE
                );
                assert!(e.inner.lock().await.book.positions["XUSDT"].stop_loss_triggered);
            } else if filled == "1" {
                assert!(result.unwrap());
                assert_eq!(submissions.load(Ordering::SeqCst), 0);
                assert!(e.inner.lock().await.book.positions.is_empty());
            } else {
                assert!(result.unwrap());
                let (config, book) = e.store.load().unwrap().unwrap();
                let close = book.pending.values().next().unwrap().clone();
                assert_eq!(close.role, OrderRole::StopLoss);
                assert_eq!(close.quantity, d("0.6"));
                assert_eq!(close.status, "UNKNOWN");
                assert!(book.positions["XUSDT"].stop_loss_triggered);
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
                seed(&restored).await; // Price rebounds above the stop; latch must survive.
                restored.inner.lock().await.account_at = now_ms();
                assert!(
                    restored
                        .ensure_stop_loss("XUSDT", ExitCheck::Account)
                        .await
                        .unwrap_err()
                        .to_string()
                        .contains("已有平仓委托")
                );
                assert!(restored.manual_close("XUSDT").await.is_err());
                restored.ensure_tp("XUSDT").await.unwrap();
                assert_eq!(submissions.load(Ordering::SeqCst), 1);
                let remote: ExchangeOrder = serde_json::from_value(json!({"symbol":"XUSDT","clientOrderId":close.client_id,"orderId":43,"status":"FILLED","executedQty":"0.6","avgPrice":"98","origQty":"0.6","cumQuote":"58.8"})).unwrap();
                restored.apply_order(remote.clone()).await.unwrap();
                restored.apply_order(remote).await.unwrap();
                restored.finalize_closed().await.unwrap();
                let i = restored.inner.lock().await;
                assert!(i.book.positions.is_empty());
                assert!(i.book.pending.is_empty());
                assert_eq!(i.book.closed_trades[0].net_profit, d("-0.88748"));
            }
            server.abort();
        }
    }
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum ExitScenario {
        BalanceError,
        BalanceTimeout,
        BalanceLimit,
        ConfirmationTimeout,
        ConfirmationLimit,
        WrongQuantity,
        WrongSide,
        HedgeMode,
        ExternalOrder,
        UnknownOrder,
        StaleQuote,
        PartialCancel,
        UnconfirmedCancel,
        UnsettledCancel,
        UnknownExit,
        Startup,
    }
    struct ExitFixture {
        scenario: ExitScenario,
        side: Action,
        submitted: Mutex<Vec<BTreeMap<String, String>>>,
        confirmations: std::sync::atomic::AtomicUsize,
        cancellations: std::sync::atomic::AtomicUsize,
        cancelled: std::sync::atomic::AtomicBool,
        recovered: std::sync::atomic::AtomicBool,
        store: Store,
    }
    fn exit_tp_report(status: &str, filled: &str) -> Value {
        json!({"symbol":"XUSDT","clientOrderId":"test-tp","orderId":42,
            "status":status,"executedQty":filled,"avgPrice":"101","origQty":"1",
            "cumQuote":(d(filled) * d("101")).to_string()})
    }
    async fn exit_fixture(
        scenario: ExitScenario,
        side: Action,
    ) -> (Arc<Engine>, Arc<ExitFixture>, tokio::task::JoinHandle<()>) {
        use axum::{
            Json, Router,
            extract::{Query, State},
            http::StatusCode,
            routing::get,
        };
        use std::sync::atomic::Ordering;
        let fixture = Arc::new(ExitFixture {
            scenario,
            side,
            submitted: Mutex::new(Vec::new()),
            confirmations: 0.into(),
            cancellations: 0.into(),
            cancelled: false.into(),
            recovered: false.into(),
            store: Store::open(":memory:", Mode::Testnet).unwrap(),
        });
        async fn positions(
            State(f): State<Arc<ExitFixture>>,
            Query(q): Query<BTreeMap<String, String>>,
        ) -> (StatusCode, Json<Value>) {
            if let Some(symbol) = q.get("symbol") {
                assert_eq!(symbol, "XUSDT");
                assert!(q.contains_key("signature"));
                f.confirmations.fetch_add(1, Ordering::SeqCst);
                if f.scenario == ExitScenario::ConfirmationTimeout {
                    tokio::time::sleep(Duration::from_secs(3)).await;
                }
                if f.scenario == ExitScenario::ConfirmationLimit {
                    return (StatusCode::TOO_MANY_REQUESTS, Json(json!({"code":-1003})));
                }
            }
            let quantity = if f.scenario == ExitScenario::WrongQuantity {
                d("0.5")
            } else if f.cancelled.load(Ordering::SeqCst)
                && matches!(
                    f.scenario,
                    ExitScenario::PartialCancel | ExitScenario::UnsettledCancel
                )
            {
                d("0.6")
            } else {
                Decimal::ONE
            };
            let sign = if f.scenario == ExitScenario::WrongSide {
                -f.side.sign()
            } else {
                f.side.sign()
            };
            (
                StatusCode::OK,
                Json(
                    json!([{"symbol":"XUSDT", "positionAmt":(sign * quantity).to_string(),
                "entryPrice":"100", "positionSide":if f.scenario == ExitScenario::HedgeMode {"LONG"} else {"BOTH"}}]),
                ),
            )
        }
        async fn orders(
            State(f): State<Arc<ExitFixture>>,
            Query(q): Query<BTreeMap<String, String>>,
        ) -> Json<Value> {
            if let Some(symbol) = q.get("symbol") {
                assert_eq!(symbol, "XUSDT");
                assert!(q.contains_key("signature"));
            }
            let mut orders = Vec::new();
            if !f.cancelled.load(Ordering::SeqCst)
                && matches!(
                    f.scenario,
                    ExitScenario::UnknownOrder
                        | ExitScenario::StaleQuote
                        | ExitScenario::PartialCancel
                        | ExitScenario::UnconfirmedCancel
                        | ExitScenario::UnsettledCancel
                )
            {
                orders.push(exit_tp_report("NEW", "0"));
            }
            if f.scenario == ExitScenario::ExternalOrder {
                let mut external = exit_tp_report("NEW", "0");
                external["clientOrderId"] = json!("external-order");
                orders.push(external);
            }
            Json(json!(orders))
        }
        async fn balances(State(f): State<Arc<ExitFixture>>) -> (StatusCode, Json<Value>) {
            if f.recovered.load(Ordering::SeqCst) {
                return (
                    StatusCode::OK,
                    Json(
                        json!({"multiAssetsMargin":false,"availableBalance":"1000","assets":[{"asset":"USDT","availableBalance":"1000"}]}),
                    ),
                );
            }
            if f.scenario == ExitScenario::BalanceTimeout {
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
            let limited = f.scenario == ExitScenario::BalanceLimit;
            (
                if limited {
                    StatusCode::TOO_MANY_REQUESTS
                } else {
                    StatusCode::SERVICE_UNAVAILABLE
                },
                Json(json!({"code":if limited {-1003} else {-1000}})),
            )
        }
        async fn cancel(
            State(f): State<Arc<ExitFixture>>,
            Query(q): Query<BTreeMap<String, String>>,
        ) -> Json<Value> {
            assert_eq!(q["symbol"], "XUSDT");
            assert_eq!(q["origClientOrderId"], "test-tp");
            f.cancellations.fetch_add(1, Ordering::SeqCst);
            if f.scenario == ExitScenario::UnconfirmedCancel {
                return Json(exit_tp_report("NEW", "0"));
            }
            f.cancelled.store(true, Ordering::SeqCst);
            Json(exit_tp_report("CANCELED", "0.4"))
        }
        async fn submit(
            State(f): State<Arc<ExitFixture>>,
            body: String,
        ) -> (StatusCode, Json<Value>) {
            let form: BTreeMap<_, _> = url::form_urlencoded::parse(body.as_bytes())
                .into_owned()
                .collect();
            assert_eq!(form["reduceOnly"], "true");
            assert_eq!(form["type"], "MARKET");
            assert_eq!(form["positionSide"], "BOTH");
            assert_eq!(form["side"], f.side.exit_side());
            assert!(form["newClientOrderId"].starts_with("gt-s-"));
            // The durable intent must exist before any POST reaches the exchange.
            let book = f.store.load().unwrap().unwrap().1;
            assert_eq!(book.pending[&form["newClientOrderId"]].status, "SUBMITTING");
            f.submitted.lock().await.push(form.clone());
            if f.scenario == ExitScenario::UnknownExit {
                return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"code":-1007})));
            }
            (
                StatusCode::OK,
                Json(
                    json!({"symbol":"XUSDT","clientOrderId":form["newClientOrderId"],"orderId":43,
                "status":"NEW","executedQty":"0","avgPrice":"0","origQty":form["quantity"],"cumQuote":"0"}),
                ),
            )
        }
        let app = Router::new()
            .route("/fapi/v3/positionRisk", get(positions))
            .route("/fapi/v1/openOrders", get(orders))
            .route("/fapi/v2/account", get(balances))
            .route("/fapi/v3/balance", get(|| async { (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"code":-1000}))) }))
            .route("/fapi/v1/time", get(|| async { Json(json!({"serverTime":now_ms()})) }))
            .route("/fapi/v1/positionSide/dual", get(|| async { Json(json!({"dualSidePosition":false})) }))
            .route("/fapi/v1/order", get(|| async { (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"code":-1000}))) }).delete(cancel).post(submit))
            .route("/fapi/v1/userTrades", get(move || async move { Json(json!([{"id":1,"orderId":42,"qty":"0.4","price":"101",
                "commission":"0.00808","commissionAsset":if scenario == ExitScenario::UnsettledCancel {"BNB"} else {"USDT"},"realizedPnl":(side.sign() * d("0.4")).to_string(),"time":1,"side":side.exit_side()}])) }))
            .with_state(fixture.clone());
        let (base, server) = crate::test_support::mock_server(app).await;
        let e = Engine::new(
            Mode::Testnet,
            fixture.store.clone(),
            config(),
            Binance::mock(base),
            Jev::new().unwrap(),
        )
        .unwrap();
        seed(&e).await;
        let mut order = entry(side);
        order.stop_loss_usdt = d("0.5");
        e.paper_entry(order.clone()).await.unwrap();
        {
            let mut i = e.inner.lock().await;
            i.account_at = if scenario == ExitScenario::Startup {
                0
            } else {
                now_ms() - 60_000
            };
            i.available = d("321");
            i.paused = false;
            let q = i.quotes.get_mut("XUSDT").unwrap();
            q.bid = if side == Action::Long {
                d("99")
            } else {
                d("101")
            };
            q.ask = q.bid;
            if matches!(
                scenario,
                ExitScenario::UnknownOrder
                    | ExitScenario::StaleQuote
                    | ExitScenario::PartialCancel
                    | ExitScenario::UnconfirmedCancel
                    | ExitScenario::UnsettledCancel
            ) {
                order.client_id = "test-tp".into();
                order.role = OrderRole::TakeProfit;
                order.price = Some(d("101"));
                order.status = if scenario == ExitScenario::UnknownOrder {
                    "UNKNOWN"
                } else {
                    "NEW"
                }
                .into();
                i.book.pending.insert(order.client_id.clone(), order);
                let p = i.book.positions.get_mut("XUSDT").unwrap();
                p.tp_client_id = Some("test-tp".into());
                p.tp_price = Some(d("101"));
            }
            if scenario == ExitScenario::StaleQuote {
                i.book
                    .positions
                    .get_mut("XUSDT")
                    .unwrap()
                    .stop_loss_triggered = true;
                i.quotes.get_mut("XUSDT").unwrap().time = now_ms() - 16_000;
            }
            e.save(&mut i).unwrap();
        }
        (e, fixture, server)
    }
    #[tokio::test]
    async fn stop_loss_survives_balance_errors_timeouts_and_startup_without_refreshing_account() {
        use std::sync::atomic::Ordering;
        for side in [Action::Long, Action::Short] {
            for scenario in [
                ExitScenario::BalanceError,
                ExitScenario::BalanceTimeout,
                ExitScenario::Startup,
            ] {
                let (e, fixture, server) = exit_fixture(scenario, side).await;
                let account_at = e.snapshot().await.account_at;
                e.manage_positions().await.unwrap();
                let snapshot = e.snapshot().await;
                assert_eq!(fixture.submitted.lock().await.len(), 1);
                assert_eq!(fixture.submitted.lock().await[0]["quantity"], "1");
                assert_eq!(fixture.confirmations.load(Ordering::SeqCst), 1);
                assert!(snapshot.paused && snapshot.account_sync_failed);
                assert!(snapshot.health.contains("账户同步失败"));
                assert_eq!(snapshot.account_at, account_at);
                assert_eq!(snapshot.available_usdt, d("321"));
                assert_eq!(snapshot.runtime.position_management.failed_runs, 1);
                assert_eq!(snapshot.pending[0].role, OrderRole::StopLoss);
                assert!(e.set_running(true).await.is_err());
                server.abort();
            }
        }
    }
    #[tokio::test]
    async fn degraded_stop_loss_blocks_limits_failed_confirmations_and_account_mismatches() {
        use std::sync::atomic::Ordering;
        for scenario in [
            ExitScenario::BalanceLimit,
            ExitScenario::ConfirmationLimit,
            ExitScenario::ConfirmationTimeout,
            ExitScenario::WrongQuantity,
            ExitScenario::WrongSide,
            ExitScenario::HedgeMode,
            ExitScenario::ExternalOrder,
            ExitScenario::UnknownOrder,
            ExitScenario::StaleQuote,
        ] {
            let (e, fixture, server) = exit_fixture(scenario, Action::Long).await;
            e.manage_positions().await.unwrap();
            assert!(fixture.submitted.lock().await.is_empty());
            assert_eq!(fixture.cancellations.load(Ordering::SeqCst), 0);
            let saved = e.store.load().unwrap().unwrap().1;
            assert_eq!(saved.positions["XUSDT"].quantity, Decimal::ONE);
            assert!(saved.positions["XUSDT"].stop_loss_triggered);
            assert!(
                !saved
                    .pending
                    .values()
                    .any(|o| o.role == OrderRole::StopLoss)
            );
            if scenario == ExitScenario::UnknownOrder {
                assert_eq!(saved.pending["test-tp"].status, "UNKNOWN");
            }
            if matches!(
                scenario,
                ExitScenario::BalanceLimit | ExitScenario::ConfirmationLimit
            ) {
                assert!(e.exchange.rate_limit_snapshot().await.cooldown_remaining_ms > 0);
            }
            assert!(e.snapshot().await.account_sync_failed);
            server.abort();
        }
    }
    #[tokio::test]
    async fn degraded_stop_loss_reconfirms_partial_cancellation_and_waits_for_unconfirmed_cancels()
    {
        use std::sync::atomic::Ordering;
        for scenario in [
            ExitScenario::PartialCancel,
            ExitScenario::UnconfirmedCancel,
            ExitScenario::UnsettledCancel,
        ] {
            let (e, fixture, server) = exit_fixture(scenario, Action::Long).await;
            let account_at = e.snapshot().await.account_at;
            e.manage_positions().await.unwrap();
            assert_eq!(fixture.cancellations.load(Ordering::SeqCst), 1);
            assert_eq!(fixture.confirmations.load(Ordering::SeqCst), 2);
            if scenario == ExitScenario::PartialCancel {
                assert_eq!(fixture.submitted.lock().await[0]["quantity"], "0.6");
                let book = e.store.load().unwrap().unwrap().1;
                assert_eq!(book.positions["XUSDT"].quantity, d("0.6"));
                assert!(!book.pending.contains_key("test-tp"));
                assert_eq!(
                    book.pending.values().next().unwrap().role,
                    OrderRole::StopLoss
                );
            } else if scenario == ExitScenario::UnconfirmedCancel {
                assert!(fixture.submitted.lock().await.is_empty());
                assert_eq!(
                    e.store.load().unwrap().unwrap().1.pending["test-tp"].status,
                    "NEW"
                );
            } else {
                assert!(fixture.submitted.lock().await.is_empty());
                let book = e.store.load().unwrap().unwrap().1;
                assert_eq!(book.pending["test-tp"].status, "SETTLING");
                assert_eq!(book.positions["XUSDT"].quantity, d("0.6"));
                assert!(!book.positions["XUSDT"].accounting_complete);
            }
            assert_eq!(e.snapshot().await.account_at, account_at);
            server.abort();
        }
    }
    #[tokio::test]
    async fn degraded_stop_loss_preserves_uncertain_orders_and_known_external_changes() {
        use std::sync::atomic::Ordering;
        for status in ["SUBMITTING", "SETTLING"] {
            let (e, fixture, server) = exit_fixture(ExitScenario::UnknownOrder, Action::Long).await;
            {
                let mut i = e.inner.lock().await;
                i.book.pending.get_mut("test-tp").unwrap().status = status.into();
                e.save(&mut i).unwrap();
            }
            e.manage_positions().await.unwrap();
            assert!(fixture.submitted.lock().await.is_empty());
            assert_eq!(fixture.cancellations.load(Ordering::SeqCst), 0);
            assert_eq!(
                e.store.load().unwrap().unwrap().1.pending["test-tp"].status,
                status
            );
            server.abort();
        }
        for external_position in [true, false] {
            let (e, fixture, server) = exit_fixture(ExitScenario::BalanceError, Action::Long).await;
            {
                let mut i = e.inner.lock().await;
                if external_position {
                    i.external_positions.insert("XUSDT".into(), d("0.5"));
                } else {
                    i.external_orders.push("XUSDT".into());
                }
            }
            e.manage_positions().await.unwrap();
            assert!(fixture.submitted.lock().await.is_empty());
            assert_eq!(fixture.cancellations.load(Ordering::SeqCst), 0);
            assert_eq!(fixture.confirmations.load(Ordering::SeqCst), 0);
            assert!(e.store.load().unwrap().unwrap().1.positions["XUSDT"].stop_loss_triggered);
            server.abort();
        }
    }
    #[tokio::test]
    async fn degraded_unknown_exit_is_not_resubmitted_and_survives_restart() {
        let (e, fixture, server) = exit_fixture(ExitScenario::UnknownExit, Action::Long).await;
        e.manage_positions().await.unwrap();
        assert_eq!(fixture.submitted.lock().await.len(), 1);
        let (config, book) = e.store.load().unwrap().unwrap();
        assert_eq!(book.pending.values().next().unwrap().status, "UNKNOWN");
        e.manage_positions().await.unwrap();
        let restored = Engine::new(
            Mode::Testnet,
            fixture.store.clone(),
            config,
            Binance::mock(e.exchange.base.clone()),
            Jev::new().unwrap(),
        )
        .unwrap();
        seed(&restored).await;
        restored.manage_positions().await.unwrap();
        assert_eq!(fixture.submitted.lock().await.len(), 1);
        assert_eq!(
            fixture
                .store
                .load()
                .unwrap()
                .unwrap()
                .1
                .pending
                .values()
                .next()
                .unwrap()
                .status,
            "UNKNOWN"
        );
        assert!(restored.snapshot().await.positions[0].stop_loss_triggered);
        server.abort();
    }
    #[tokio::test]
    async fn only_complete_account_recovery_clears_failure_and_does_not_resume_entries() {
        use std::sync::atomic::Ordering;
        let (e, fixture, server) = exit_fixture(ExitScenario::BalanceError, Action::Long).await;
        {
            let mut i = e.inner.lock().await;
            i.account_at = now_ms();
            let q = i.quotes.get_mut("XUSDT").unwrap();
            q.bid = d("100");
            q.ask = q.bid;
        }
        e.manage_positions().await.unwrap();
        assert!(fixture.submitted.lock().await.is_empty());
        assert!(e.snapshot().await.account_sync_failed);
        assert_eq!(
            e.set_running(true).await.unwrap_err().to_string(),
            "账户尚未对账"
        );
        assert!(e.inner.lock().await.book.pending.is_empty()); // No new TP during degradation.
        assert!(!e.snapshot().await.positions[0].stop_loss_triggered);
        e.exchange.sync_time().await.unwrap();
        assert!(e.snapshot().await.account_sync_failed);
        fixture.recovered.store(true, Ordering::SeqCst);
        e.sync_account().await.unwrap();
        let snapshot = e.snapshot().await;
        assert!(!snapshot.account_sync_failed);
        assert!(snapshot.paused);
        assert_eq!(snapshot.available_usdt, d("1000"));
        server.abort();
    }
    #[tokio::test]
    async fn user_order_events_settle_once_and_preserve_uncertain_terminal_orders() {
        let (e, fixture, server) = exit_fixture(ExitScenario::UnknownOrder, Action::Long).await;
        let push = |status: &str, filled: &str, side: &str| {
            crate::user_stream::parse_event(json!({"e":"ORDER_TRADE_UPDATE","E":2,"T":1,"o":{
            "s":"XUSDT","c":"test-tp","i":42,"q":"1","z":filled,"ap":"101","X":status,"S":side,"ps":"BOTH"}})).unwrap()
        };
        e.handle_user_event(push("PARTIALLY_FILLED", "0.4", "SELL"))
            .await
            .unwrap();
        e.handle_user_event(push("PARTIALLY_FILLED", "0.4", "SELL"))
            .await
            .unwrap();
        e.handle_user_event(push("NEW", "0", "SELL")).await.unwrap();
        {
            let i = e.inner.lock().await;
            let p = &i.book.positions["XUSDT"];
            assert_eq!(p.quantity, d("0.6"));
            assert_eq!(p.exit_fee, d("0.00808"));
            assert_eq!(p.observed_trade_ids, vec![1]);
            assert_eq!(i.book.pending["test-tp"].status, "PARTIALLY_FILLED");
            assert!(i.account_update_pending);
            assert_eq!(i.available, d("321"));
        }
        // Repricing must wait for the account read, even after partial fills
        // have been accounted from a push.
        e.inner
            .lock()
            .await
            .book
            .pending
            .get_mut("test-tp")
            .unwrap()
            .price = Some(d("99"));
        e.ensure_tp("XUSDT").await.unwrap();
        assert_eq!(
            fixture
                .cancellations
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        assert!(
            e.handle_user_event(push("PARTIALLY_FILLED", "0.4", "BUY"))
                .await
                .is_err()
        );
        e.inner
            .lock()
            .await
            .book
            .pending
            .get_mut("test-tp")
            .unwrap()
            .status = "SETTLING".into();
        e.handle_user_event(push("PARTIALLY_FILLED", "0.4", "SELL"))
            .await
            .unwrap();
        assert_eq!(
            e.inner.lock().await.book.pending["test-tp"].status,
            "SETTLING"
        );
        assert!(fixture.submitted.lock().await.is_empty());
        server.abort();
    }
    #[tokio::test]
    async fn account_push_during_rest_read_prevents_publication_of_a_stale_snapshot() {
        use axum::{Json, Router, routing::get};
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let begin = started.clone();
        let finish = release.clone();
        let app=Router::new()
            .route("/fapi/v3/positionRisk",get(move || {let begin=begin.clone();let finish=finish.clone();async move {
                begin.notify_one();finish.notified().await;Json(json!([]))
            }}))
            .route("/fapi/v1/openOrders",get(||async{Json(json!([]))}))
            .route("/fapi/v2/account",get(||async{Json(json!({"multiAssetsMargin":false,"availableBalance":"1000","assets":[{"asset":"USDT","availableBalance":"1000"}]}))}));
        let (base, server) = crate::test_support::mock_server(app).await;
        let e = Engine::new(
            Mode::Testnet,
            Store::open(":memory:", Mode::Testnet).unwrap(),
            config(),
            Binance::mock(base),
            Jev::new().unwrap(),
        )
        .unwrap();
        let original_account_at = now_ms() - 1_000;
        {
            let mut i = e.inner.lock().await;
            i.available = d("321");
            i.account_at = original_account_at;
            i.book.funding_last_poll = now_ms();
            i.paused = false;
        }
        let reader = e.clone();
        let task = tokio::spawn(async move { reader.manage_positions().await });
        tokio::time::timeout(Duration::from_secs(1), started.notified())
            .await
            .unwrap();
        let writer = e.clone();
        let push = tokio::spawn(async move { writer.handle_user_event(UserEvent::Account).await });
        tokio::time::timeout(Duration::from_secs(1), e.account_notify.notified())
            .await
            .unwrap();
        assert!(e.snapshot().await.account_update_pending);
        release.notify_one();
        task.await.unwrap().unwrap();
        push.await.unwrap().unwrap();
        let snapshot = e.snapshot().await;
        assert!(snapshot.account_update_pending);
        assert!(!snapshot.account_sync_failed);
        assert!(!snapshot.paused);
        assert_eq!(snapshot.account_at, original_account_at);
        assert_eq!(snapshot.available_usdt, d("321"));

        release.notify_one();
        e.manage_positions().await.unwrap();
        let snapshot = e.snapshot().await;
        assert!(!snapshot.account_update_pending);
        assert!(!snapshot.account_sync_failed);
        assert!(!snapshot.paused);
        assert_eq!(snapshot.available_usdt, d("1000"));

        // A push queued before a REST read must also invalidate the successful
        // read after acquiring the lock, and schedule another reconciliation.
        let guard = e.reconcile_lock.lock().await;
        e.account_notify.notified().await; // Consume the last push's wakeup.
        let writer = e.clone();
        let push = tokio::spawn(async move { writer.handle_user_event(UserEvent::Account).await });
        tokio::time::timeout(Duration::from_secs(1), e.account_notify.notified())
            .await
            .unwrap();
        release.notify_one();
        assert!(e.sync_account().await.unwrap());
        assert!(!e.snapshot().await.account_update_pending);
        drop(guard);
        push.await.unwrap().unwrap();
        assert!(e.snapshot().await.account_update_pending);
        tokio::time::timeout(Duration::from_secs(1), e.account_notify.notified())
            .await
            .unwrap();
        server.abort();
    }
    #[tokio::test]
    async fn stop_loss_remains_available_when_only_funding_sync_fails() {
        use axum::{
            Json, Router,
            http::StatusCode,
            routing::{get, post},
        };
        use std::sync::atomic::{AtomicUsize, Ordering};
        let submissions = Arc::new(AtomicUsize::new(0));
        let counter = submissions.clone();
        let app = Router::new()
            .route("/fapi/v3/positionRisk", get(|| async { Json(json!([
                {"symbol":"XUSDT","positionAmt":"1","entryPrice":"100","positionSide":"BOTH"}
            ])) }))
            .route("/fapi/v1/openOrders", get(|| async { Json(json!([])) }))
            .route("/fapi/v2/account", get(|| async { Json(json!({
                "multiAssetsMargin":false,"availableBalance":"1000","assets":[{"asset":"USDT","availableBalance":"1000"}]
            })) }))
            .route("/fapi/v1/income", get(|| async { (StatusCode::BAD_REQUEST, Json(json!({"code":-1000,"msg":"local funding outage"}))) }))
            .route("/fapi/v1/order", post(move |body: String| {
                let counter = counter.clone();
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    let form: std::collections::HashMap<_, _> = url::form_urlencoded::parse(body.as_bytes()).into_owned().collect();
                    Json(json!({"symbol":"XUSDT","clientOrderId":form["newClientOrderId"],"orderId":43,
                        "status":"NEW","executedQty":"0","avgPrice":"0","origQty":"1","cumQuote":"0"}))
                }
            }));
        let (url, server) = crate::test_support::mock_server(app).await;
        let e = Engine::new(
            Mode::Testnet,
            Store::open(":memory:", Mode::Testnet).unwrap(),
            config(),
            Binance::mock(url),
            Jev::new().unwrap(),
        )
        .unwrap();
        seed(&e).await;
        let mut order = entry(Action::Long);
        order.stop_loss_usdt = d("0.5");
        e.paper_entry(order).await.unwrap();
        {
            let mut i = e.inner.lock().await;
            i.account_at = now_ms(); // Preflight already completed.
            i.book.funding_last_poll = 0;
            let q = i.quotes.get_mut("XUSDT").unwrap();
            q.bid = d("99");
            q.ask = d("99");
        }
        e.manage_positions().await.unwrap();
        let i = e.inner.lock().await;
        assert!(i.paused);
        assert!(i.health.contains("资金费同步失败"));
        assert!(i.book.positions["XUSDT"].stop_loss_triggered);
        assert_eq!(
            i.book.pending.values().next().unwrap().role,
            OrderRole::StopLoss
        );
        assert_eq!(submissions.load(Ordering::SeqCst), 1);
        server.abort();
    }
    #[tokio::test]
    async fn stop_loss_latches_but_does_not_cancel_an_unknown_order() {
        let e = paper();
        seed(&e).await;
        let mut order = entry(Action::Long);
        order.stop_loss_usdt = d("0.5");
        e.paper_entry(order.clone()).await.unwrap();
        order.role = OrderRole::TakeProfit;
        order.client_id = "unknown-tp".into();
        order.status = "UNKNOWN".into();
        {
            let mut i = e.inner.lock().await;
            i.book.pending.insert(order.client_id.clone(), order);
            i.quotes.get_mut("XUSDT").unwrap().bid = d("99");
        }
        assert!(
            e.ensure_stop_loss("XUSDT", ExitCheck::Account)
                .await
                .unwrap_err()
                .to_string()
                .contains("尚未确认")
        );
        let (_, book) = e.store.load().unwrap().unwrap();
        assert!(book.pending.contains_key("unknown-tp"));
        assert!(book.positions["XUSDT"].stop_loss_triggered);
        e.inner.lock().await.book.pending.remove("unknown-tp");
        // A confirmed cancellation clears the obstruction; the intent remains even at a profit.
        e.inner.lock().await.quotes.get_mut("XUSDT").unwrap().bid = d("100");
        assert!(
            e.ensure_stop_loss("XUSDT", ExitCheck::Account)
                .await
                .unwrap()
        );
        assert!(e.inner.lock().await.book.positions.is_empty());
    }
    #[tokio::test]
    async fn scanning_requires_jev_credentials_even_in_paper_mode() {
        let e = paper();
        seed(&e).await;
        e.set_pool(PoolCoin {
            symbol: "XUSDT".into(),
            enabled: true,
            config: None,
        })
        .await
        .unwrap();
        let error = e.set_running(true).await.unwrap_err().to_string();
        assert!(error.contains("Jev API Key"));
        assert!(e.inner.lock().await.paused);
        *e.jev.write().await = Arc::new(Jev::with_key("local-jev-test-key").unwrap());
        e.set_running(true).await.unwrap();
        assert!(!e.inner.lock().await.paused);
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
                let mut i = e.inner.lock().await;
                let p = &i.book.positions["XUSDT"];
                let id = p.tp_client_id.clone().unwrap();
                let limit = p.tp_price.unwrap();
                // Distinct, recent timestamps without generating future quotes.
                let created = now_ms() - 100;
                i.book.pending.get_mut(&id).unwrap().created_at = created;
                (id, limit, created)
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
            e.save(&mut i).unwrap();
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
    async fn partial_fill_retries_missing_fees_without_further_execution() {
        use axum::{Json, Router, routing::get};
        use std::sync::atomic::{AtomicUsize, Ordering};
        let queries = Arc::new(AtomicUsize::new(0));
        let count = queries.clone();
        let app = Router::new().route(
            "/fapi/v1/userTrades",
            get(move || {
                let count = count.clone();
                async move {
                    Json(if count.fetch_add(1, Ordering::SeqCst) == 0 {
                        json!([])
                    } else {
                        json!([{"id":1,"orderId":42,"qty":"0.4","price":"101","commission":"0.02",
                        "commissionAsset":"USDT","realizedPnl":"0.4","time":1,"side":"SELL"}])
                    })
                }
            }),
        );
        let (base, server) = crate::test_support::mock_server(app).await;
        let e = Engine::new(
            Mode::Testnet,
            Store::open(":memory:", Mode::Testnet).unwrap(),
            config(),
            Binance::mock(base),
            Jev::new().unwrap(),
        )
        .unwrap();
        seed(&e).await;
        e.paper_entry(entry(Action::Long)).await.unwrap();
        {
            let mut i = e.inner.lock().await;
            let mut o = entry(Action::Long);
            o.client_id = "test-tp".into();
            o.role = OrderRole::TakeProfit;
            o.price = Some(d("101"));
            i.book.pending.insert(o.client_id.clone(), o);
            i.paused = false;
        }
        let remote: ExchangeOrder = serde_json::from_value(json!({"symbol":"XUSDT","clientOrderId":"test-tp",
            "orderId":42,"status":"PARTIALLY_FILLED","executedQty":"0.4","origQty":"1","avgPrice":"101","cumQuote":"40.4"})).unwrap();
        e.apply_order(remote.clone()).await.unwrap();
        assert!(!e.inner.lock().await.entry_data_ready());
        assert!(!e.snapshot().await.paused);
        let error = e.set_running(true).await.unwrap_err().to_string();
        assert!(error.contains("XUSDT"));
        assert!(error.contains("已查询 0，订单已成交 0.4"));
        // The same cumulative quantity must still retry the unconfirmed fee read.
        e.apply_order(remote.clone()).await.unwrap();
        e.apply_order(remote).await.unwrap();
        let i = e.inner.lock().await;
        assert_eq!(i.book.positions["XUSDT"].quantity, d("0.6"));
        assert_eq!(i.book.positions["XUSDT"].exit_fee, d("0.02"));
        assert_eq!(i.book.positions["XUSDT"].realized_gross, d("0.4"));
        assert!(i.book.positions["XUSDT"].accounting_complete);
        assert!(i.entry_data_ready());
        assert!(!i.paused);
        assert_eq!(queries.load(Ordering::SeqCst), 2);
        server.abort();
    }
    #[tokio::test]
    async fn legacy_partial_fee_wait_recovers_after_restart_without_new_fills() {
        use axum::{Json, Router, routing::get};
        let app = Router::new().route(
            "/fapi/v1/userTrades",
            get(|| async {
                Json(
                    json!([{"id":1,"orderId":42,"qty":"0.4","price":"101","commission":"0.02",
                "commissionAsset":"USDT","realizedPnl":"0.4","time":1,"side":"SELL"}]),
                )
            }),
        );
        let (base, server) = crate::test_support::mock_server(app).await;
        let old = paper();
        seed(&old).await;
        old.paper_entry(entry(Action::Long)).await.unwrap();
        let (config, mut book) = old.store.load().unwrap().unwrap();
        let position = book.positions.get_mut("XUSDT").unwrap();
        position.quantity = d("0.6");
        position.accounting_complete = false;
        let mut order = entry(Action::Long);
        order.client_id = "test-tp".into();
        order.role = OrderRole::TakeProfit;
        order.status = "PARTIALLY_FILLED".into();
        order.accounted_quantity = d("0.4");
        let value = serde_json::to_value(&order).unwrap();
        assert!(value.get("settlement_issue").is_none()); // Old schema remains readable.
        book.pending.insert(
            order.client_id.clone(),
            serde_json::from_value(value).unwrap(),
        );
        let store = Store::open(":memory:", Mode::Testnet).unwrap();
        store.save(Mode::Testnet, &config, &book).unwrap();
        let e = Engine::new(
            Mode::Testnet,
            store,
            config,
            Binance::mock(base),
            Jev::new().unwrap(),
        )
        .unwrap();
        assert!(e.snapshot().await.pending[0].settlement_issue.is_some());
        let remote: ExchangeOrder = serde_json::from_value(json!({"symbol":"XUSDT","clientOrderId":"test-tp",
            "orderId":42,"status":"PARTIALLY_FILLED","executedQty":"0.4","origQty":"1","avgPrice":"101","cumQuote":"40.4"})).unwrap();
        e.apply_order(remote).await.unwrap();
        let i = e.inner.lock().await;
        assert!(i.paused); // A restart does not start trading by itself.
        assert!(i.entry_data_ready());
        assert_eq!(i.book.positions["XUSDT"].quantity, d("0.6"));
        assert_eq!(i.book.positions["XUSDT"].exit_fee, d("0.02"));
        assert!(i.book.pending["test-tp"].settlement_issue.is_none());
        server.abort();
    }
    #[tokio::test]
    async fn settling_one_order_keeps_other_orders_fees_pending() {
        use axum::{Json, Router, extract::Query, routing::get};
        let app = Router::new().route("/fapi/v1/userTrades", get(|Query(query): Query<BTreeMap<String,String>>| async move {
            let first = query["orderId"] == "42";
            Json(json!([{"id":if first {1} else {2},"orderId":if first {42} else {43},
                "qty":if first {"0.2"} else {"0.3"},"price":"101","commission":if first {"0.01"} else {"0.02"},
                "commissionAsset":"USDT","realizedPnl":if first {"0.2"} else {"0.3"},"time":1,"side":"SELL"}]))
        }));
        let (base, server) = crate::test_support::mock_server(app).await;
        let e = Engine::new(
            Mode::Testnet,
            Store::open(":memory:", Mode::Testnet).unwrap(),
            config(),
            Binance::mock(base),
            Jev::new().unwrap(),
        )
        .unwrap();
        seed(&e).await;
        e.paper_entry(entry(Action::Long)).await.unwrap();
        {
            let mut i = e.inner.lock().await;
            let position = i.book.positions.get_mut("XUSDT").unwrap();
            position.quantity = d("0.5");
            position.accounting_complete = false;
            for (id, quantity) in [("tp-1", "0.2"), ("tp-2", "0.3")] {
                let mut order = entry(Action::Long);
                order.client_id = id.into();
                order.role = OrderRole::TakeProfit;
                order.quantity = d(quantity);
                order.accounted_quantity = d(quantity);
                order.status = "SETTLING".into();
                order.settlement_issue = Some("成交明细查询失败".into());
                i.book.pending.insert(order.client_id.clone(), order);
            }
        }
        for (id, order_id, quantity) in [("tp-1", 42, "0.2"), ("tp-2", 43, "0.3")] {
            let remote: ExchangeOrder = serde_json::from_value(json!({"symbol":"XUSDT","clientOrderId":id,
                "orderId":order_id,"status":"FILLED","executedQty":quantity,"origQty":quantity,"avgPrice":"101","cumQuote":"1"})).unwrap();
            e.apply_order(remote.clone()).await.unwrap();
            e.apply_order(remote).await.unwrap();
            let i = e.inner.lock().await;
            assert_eq!(i.entry_data_ready(), id == "tp-2");
            assert_eq!(i.book.positions["XUSDT"].accounting_complete, id == "tp-2");
        }
        let i = e.inner.lock().await;
        assert_eq!(i.book.positions["XUSDT"].quantity, d("0.5"));
        assert_eq!(i.book.positions["XUSDT"].exit_fee, d("0.03"));
        assert_eq!(i.book.positions["XUSDT"].realized_gross, d("0.5"));
        server.abort();
    }
    #[tokio::test]
    async fn mismatched_commission_blocks_settlement_and_new_entries() {
        use axum::{Json, Router, routing::get};
        for (symbol, commission_asset) in [
            ("XUSDT", "BNB"),
            ("XUSDT", "USDC"),
            ("XUSDC", "USDT"),
            ("XUSDC", "BNB"),
        ] {
            let app = Router::new().route("/fapi/v1/userTrades", get(move || async move { Json(json!([{"id": 1, "orderId":42,"qty":"1","price":"100","commission":"0.001","commissionAsset":commission_asset,"realizedPnl":"0","time":1,"side":"BUY"}])) }));
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
                let mut o = entry(Action::Long);
                o.symbol = symbol.into();
                i.book.pending.insert(o.client_id.clone(), o);
                i.paused = false;
            }
            let r: ExchangeOrder = serde_json::from_value(json!({"symbol":symbol,"clientOrderId":"test-entry","orderId":42,"status":"FILLED","executedQty":"1","avgPrice":"100","origQty":"1","cumQuote":"100"})).unwrap();
            e.apply_order(r).await.unwrap();
            let detail = &e.store.events(1).unwrap()[0].detail;
            assert_eq!(
                detail["commission_by_asset"][commission_asset],
                json!("0.001")
            );
            assert_eq!(detail["fills_quantity"], json!("1"));
            let error = e.set_running(true).await.unwrap_err().to_string();
            assert!(error.contains(commission_asset));
            assert!(error.contains(settlement_asset(symbol).unwrap()));
            let i = e.inner.lock().await;
            assert!(i.paused);
            assert_eq!(i.book.pending["test-entry"].status, "SETTLING");
            assert!(!i.book.positions[symbol].accounting_complete);
            assert!(i.occupied(symbol));
            server.abort();
        }
    }
    #[tokio::test]
    async fn confirmed_zero_commission_does_not_require_currency_conversion() {
        use axum::{Json, Router, routing::get};
        for symbol in ["XUSDT", "XUSDC"] {
            let app = Router::new().route(
                "/fapi/v1/userTrades",
                get(|| async {
                    Json(
                        json!([{"id":1,"orderId":42,"qty":"1","price":"100","commission":"0",
                    "commissionAsset":"BNB","realizedPnl":"0","time":1,"side":"BUY"}]),
                    )
                }),
            );
            let (base, server) = crate::test_support::mock_server(app).await;
            let e = Engine::new(
                Mode::Testnet,
                Store::open(":memory:", Mode::Testnet).unwrap(),
                config(),
                Binance::mock(base),
                Jev::new().unwrap(),
            )
            .unwrap();
            {
                let mut i = e.inner.lock().await;
                let mut order = entry(Action::Long);
                order.symbol = symbol.into();
                i.book.pending.insert(order.client_id.clone(), order);
                i.paused = false;
            }
            let remote: ExchangeOrder = serde_json::from_value(json!({"symbol":symbol,"clientOrderId":"test-entry",
                "orderId":42,"status":"FILLED","executedQty":"1","origQty":"1","avgPrice":"100","cumQuote":"100"})).unwrap();
            e.apply_order(remote.clone()).await.unwrap();
            e.apply_order(remote).await.unwrap();
            let i = e.inner.lock().await;
            assert!(!i.paused);
            assert!(i.entry_data_ready());
            assert!(i.book.pending.is_empty());
            assert_eq!(i.book.positions[symbol].entry_fee, Decimal::ZERO);
            assert_eq!(i.book.positions[symbol].quantity, Decimal::ONE);
            server.abort();
        }
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
            stop_loss_usdt: Decimal::ZERO,
            maker_fee: Decimal::ZERO,
            taker_fee: Decimal::ZERO,
            status: "UNKNOWN".into(),
            accounted_quantity: Decimal::ZERO,
            settlement_issue: None,
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
        assert_eq!(normalize_symbol(" btc/usdc ").unwrap(), "BTCUSDC");
        assert_eq!(normalize_symbol(" ethusdc ").unwrap(), "ETHUSDC");
        assert_eq!(normalize_symbol("龙虾USDT").unwrap(), "龙虾USDT");
        assert_eq!(normalize_symbol("龙虾/USDC").unwrap(), "龙虾USDC");
        for invalid in [
            "../USDT",
            "../USDC",
            "USDC",
            "/USDC",
            "BTC//USDC",
            "BTCUSD",
            "BTCBUSD",
            "BTC/USDC/USDT",
        ] {
            assert!(normalize_symbol(invalid).is_err(), "accepted {invalid}");
        }
    }
    #[tokio::test]
    async fn margin_checks_use_the_settlement_asset_or_shared_multi_asset_balance() {
        let e = paper();
        seed(&e).await;
        let mut i = e.inner.lock().await;
        let mut usdc = entry(Action::Long);
        usdc.symbol = "XUSDC".into();
        usdc.client_id = "usdc-entry".into();
        let usdt_quote = i.quotes["XUSDT"].clone();
        i.quotes.insert(
            "XUSDC".into(),
            Quote {
                bid: d("200"),
                ask: d("200"),
                ..usdt_quote
            },
        );
        let usdt = entry(Action::Long);
        i.book.pending.insert(usdt.client_id.clone(), usdt);
        i.book.pending.insert(usdc.client_id.clone(), usdc);
        i.margin_balances = Some(serde_json::from_value(json!({
            "multiAssetsMargin":false, "availableBalance":"1000",
            "assets":[{"asset":"USDT","availableBalance":"1000"}, {"asset":"USDC","availableBalance":"20"}]
        })).unwrap());
        // Each order reserves its own price and fees, without borrowing the other wallet.
        assert_eq!(
            i.available_for_entry("BTCUSDT", Mode::Live).unwrap(),
            d("949.95")
        );
        assert_eq!(
            i.available_for_entry("BTCUSDC", Mode::Live).unwrap(),
            d("-80.1")
        );
        i.margin_balances
            .as_mut()
            .unwrap()
            .assets
            .retain(|b| b.asset != "USDC");
        assert_eq!(
            i.available_for_entry("BTCUSDC", Mode::Live).unwrap(),
            d("-100.1")
        );
        i.margin_balances.as_mut().unwrap().multi_assets_margin = true;
        assert_eq!(
            i.available_for_entry("BTCUSDC", Mode::Live).unwrap(),
            d("849.85")
        );
        assert_eq!(
            i.available_for_entry("BTCUSDT", Mode::Live).unwrap(),
            d("849.85")
        );
        i.margin_balances = None;
        assert!(i.available_for_entry("BTCUSDC", Mode::Live).is_err());
    }
    #[tokio::test]
    async fn account_sync_keeps_usdc_balances_without_requiring_a_usdt_wallet() {
        use axum::{Json, Router, routing::get};
        let app = Router::new()
            .route("/fapi/v3/positionRisk", get(|| async { Json(json!([])) }))
            .route("/fapi/v1/openOrders", get(|| async { Json(json!([])) }))
            .route(
                "/fapi/v3/balance",
                get(|| async { Json(json!([{"asset":"USDC","availableBalance":"250"}])) }),
            )
            .route(
                "/fapi/v2/account",
                get(|| async {
                    Json(json!({
                        "multiAssetsMargin":false, "availableBalance":"0",
                        "assets":[{"asset":"USDC","availableBalance":"250"}]
                    }))
                }),
            );
        let (url, server) = crate::test_support::mock_server(app).await;
        let e = Engine::new(
            Mode::Testnet,
            Store::open(":memory:", Mode::Testnet).unwrap(),
            config(),
            Binance::mock(url),
            Jev::new().unwrap(),
        )
        .unwrap();
        assert_eq!(e.exchange.available_balance().await.unwrap(), d("250"));
        e.sync_account().await.unwrap();
        let i = e.inner.lock().await;
        assert!(i.account_at > 0);
        assert_eq!(
            i.available_for_entry("BTCUSDC", Mode::Testnet).unwrap(),
            d("250")
        );
        assert_eq!(
            i.available_for_entry("BTCUSDT", Mode::Testnet).unwrap(),
            Decimal::ZERO
        );
        server.abort();
    }
    #[tokio::test]
    async fn usdc_fills_funding_and_close_settle_once_in_the_contract_currency() {
        use axum::{Json, Router, extract::Query, routing::get};
        let app = Router::new()
            .route("/fapi/v1/userTrades", get(|Query(params): Query<BTreeMap<String, String>>| async move {
                assert_eq!(params["symbol"], "XUSDC");
                let exit = params["orderId"] == "43";
                Json(json!([{"id":if exit {2} else {1}, "orderId":if exit {43} else {42},
                    "qty":"1", "price":if exit {"101"} else {"100"},
                    "commission":if exit {"0.0202"} else {"0.05"}, "commissionAsset":"USDC",
                    "realizedPnl":if exit {"1"} else {"0"}, "time":now_ms(), "side":if exit {"SELL"} else {"BUY"}}]))
            }))
            .route("/fapi/v1/income", get(|| async { Json(json!([
                {"tranId":1, "symbol":"XUSDC", "income":"-0.01", "asset":"USDC", "time":now_ms()},
                {"tranId":2, "symbol":"OTHERBTC", "income":"-1", "asset":"BTC", "time":now_ms()}
            ])) }));
        let (url, server) = crate::test_support::mock_server(app).await;
        let e = Engine::new(
            Mode::Testnet,
            Store::open(":memory:", Mode::Testnet).unwrap(),
            config(),
            Binance::mock(url),
            Jev::new().unwrap(),
        )
        .unwrap();
        let mut order = entry(Action::Long);
        order.symbol = "XUSDC".into();
        {
            let mut i = e.inner.lock().await;
            i.book
                .pending
                .insert(order.client_id.clone(), order.clone());
            i.paused = false;
        }
        let remote: ExchangeOrder = serde_json::from_value(json!({"symbol":"XUSDC", "clientOrderId":order.client_id, "orderId":42, "status":"FILLED", "executedQty":"1", "avgPrice":"100", "origQty":"1", "cumQuote":"100"})).unwrap();
        e.apply_order(remote.clone()).await.unwrap();
        e.apply_order(remote).await.unwrap();
        e.sync_funding().await.unwrap();
        {
            let mut i = e.inner.lock().await;
            assert!(i.book.pending.is_empty());
            let p = &i.book.positions["XUSDC"];
            assert_eq!(p.entry_fee, d("0.05"));
            assert_eq!(p.funding_cost, d("0.01"));
            assert!(p.accounting_complete);
            assert!(!i.paused);
            i.book.funding_last_poll = 0;
        }
        e.sync_funding().await.unwrap();
        order.client_id = "usdc-exit".into();
        order.role = OrderRole::TakeProfit;
        e.inner
            .lock()
            .await
            .book
            .pending
            .insert(order.client_id.clone(), order.clone());
        let remote: ExchangeOrder = serde_json::from_value(json!({"symbol":"XUSDC", "clientOrderId":order.client_id, "orderId":43, "status":"FILLED", "executedQty":"1", "avgPrice":"101", "origQty":"1", "cumQuote":"101"})).unwrap();
        e.apply_order(remote.clone()).await.unwrap();
        e.apply_order(remote).await.unwrap();
        e.finalize_closed().await.unwrap();
        let i = e.inner.lock().await;
        assert!(i.book.pending.is_empty());
        assert!(i.book.positions.is_empty());
        assert_eq!(i.book.closed_trades.len(), 1);
        assert_eq!(i.book.closed_trades[0].net_profit, d("0.9198"));
        assert!(!i.paused);
        server.abort();
    }
}
