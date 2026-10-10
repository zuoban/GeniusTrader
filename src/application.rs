use crate::{
    auth::Auth,
    config::{Config, Mode},
    engine::{Engine, EngineTasks},
    exchange::Binance,
    jev::Jev,
    settings::{
        AppSettings, CredentialTestRequest, CredentialTestResult, Credentials,
        JevCredentialTestRequest, JevCredentialTestResult, SettingsPatch, SettingsView,
    },
    store::Store,
};
use anyhow::{Result, ensure};
use serde::Serialize;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

struct ManagedEngine {
    engine: Arc<Engine>,
    tasks: Option<EngineTasks>,
}
#[derive(Default)]
struct Runtime {
    started: bool,
    engines: Vec<ManagedEngine>,
}
#[derive(Serialize)]
pub struct BackgroundModeView {
    pub mode: Mode,
    pub managing: bool,
    pub configured: bool,
    pub positions: usize,
    pub pending_orders: usize,
    pub external_positions: usize,
    pub external_orders: usize,
    pub account_sync_failed: bool,
    pub health: String,
}

pub struct Application {
    pub auth: Auth,
    pub engine: RwLock<Arc<Engine>>,
    settings: Mutex<AppSettings>,
    pub store: Store,
    update_lock: Mutex<()>,
    runtime: Mutex<Runtime>,
}
impl Application {
    fn build(store: &Store, settings: &AppSettings) -> Result<Arc<Engine>> {
        settings.validate()?;
        Self::build_mode(store, settings, settings.mode)
    }
    fn build_mode(store: &Store, settings: &AppSettings, mode: Mode) -> Result<Arc<Engine>> {
        // Inactive modes only manage existing orders and positions. The live enable
        // flag gates selecting Live for new entries, not restoring its risk management.
        let credentials = match mode {
            Mode::Testnet => &settings.testnet,
            _ => &settings.live,
        };
        Engine::new(
            mode,
            store.for_mode(mode),
            Config::default(),
            Binance::with_credentials(mode, credentials)?,
            Jev::with_key(&settings.jev_key)?,
        )
    }
    pub fn new(store: Store) -> Result<Arc<Self>> {
        let settings = store.settings()?;
        let engine = Self::build(&store, &settings)?;
        let mut engines = vec![ManagedEngine {
            engine: engine.clone(),
            tasks: None,
        }];
        for mode in [Mode::Paper, Mode::Testnet, Mode::Live] {
            if mode != settings.mode
                && (settings.managed_modes.contains(&mode)
                    || store.for_mode(mode).load()?.is_some_and(|(_, book)| {
                        !book.positions.is_empty() || !book.pending.is_empty()
                    }))
            {
                engines.push(ManagedEngine {
                    engine: Self::build_mode(&store, &settings, mode)?,
                    tasks: None,
                });
            }
        }
        store.save_settings(&settings)?;
        Ok(Arc::new(Self {
            auth: Auth::new(store.clone()),
            engine: RwLock::new(engine),
            settings: Mutex::new(settings),
            store,
            update_lock: Mutex::new(()),
            runtime: Mutex::new(Runtime {
                started: false,
                engines,
            }),
        }))
    }
    #[cfg(test)]
    pub(crate) fn from_engine(engine: Arc<Engine>) -> Arc<Self> {
        Arc::new(Self {
            auth: Auth::new(engine.store.clone()),
            store: engine.store.clone(),
            engine: RwLock::new(engine.clone()),
            settings: Mutex::new(AppSettings::default()),
            update_lock: Mutex::new(()),
            runtime: Mutex::new(Runtime {
                started: false,
                engines: vec![ManagedEngine {
                    engine,
                    tasks: None,
                }],
            }),
        })
    }
    pub async fn start(&self) {
        let _active = self.engine.read().await;
        let mut runtime = self.runtime.lock().await;
        if !runtime.started {
            for managed in &mut runtime.engines {
                managed.tasks = Some(managed.engine.spawn());
            }
            runtime.started = true;
        }
    }
    pub async fn background_modes(&self, active_mode: Mode) -> Vec<BackgroundModeView> {
        let runtime = self.runtime.lock().await;
        let mut modes = Vec::new();
        for managed in &runtime.engines {
            if managed.engine.mode == active_mode {
                continue;
            }
            let i = managed.engine.inner.lock().await;
            if i.book.positions.is_empty()
                && i.book.pending.is_empty()
                && i.external_positions.is_empty()
                && i.external_orders.is_empty()
                && !i.account_sync_failed
            {
                continue;
            }
            modes.push(BackgroundModeView {
                mode: managed.engine.mode,
                managing: managed.tasks.is_some(),
                configured: managed.engine.mode == Mode::Paper
                    || managed.engine.exchange.configured(),
                positions: i.book.positions.len(),
                pending_orders: i.book.pending.len(),
                external_positions: i.external_positions.len(),
                external_orders: i.external_orders.len(),
                account_sync_failed: i.account_sync_failed,
                health: i.health.clone(),
            });
        }
        modes
    }
    pub async fn settings_view(&self) -> SettingsView {
        self.settings.lock().await.view()
    }
    pub async fn configure_connections(&self, patch: SettingsPatch) -> Result<SettingsView> {
        let _update = self.update_lock.lock().await;
        let mut active = self.engine.write().await;
        let mut settings = self.settings.lock().await;
        let mut candidate = settings.patched(patch)?;
        let old = active.clone();
        let switching = settings.mode != candidate.mode;
        let jev_changed = settings.jev_key != candidate.jev_key;
        let jev_client = if jev_changed {
            Some(Arc::new(Jev::with_key(&candidate.jev_key)?))
        } else {
            None
        };
        if !settings.same_account(&candidate) || jev_changed {
            old.set_running(false).await?;
        }
        let mut runtime = self.runtime.lock().await;
        let changed = runtime
            .engines
            .iter()
            .filter(|managed| match managed.engine.mode {
                Mode::Paper => false,
                Mode::Testnet => settings.testnet != candidate.testnet,
                Mode::Live => settings.live != candidate.live,
            })
            .map(|managed| managed.engine.clone())
            .collect::<Vec<_>>();
        // Hold changed accounts' reconciliation locks until their old tasks stop.
        let mut reconcile_locks = Vec::new();
        for engine in &changed {
            reconcile_locks.push(engine.reconcile_lock.lock().await);
            // An unconfigured restored mode needs its original credentials to resume
            // management. A configured account with exposure must never be replaced.
            if engine.exchange.configured() {
                let i = engine.inner.lock().await;
                ensure!(
                    i.book.positions.is_empty()
                        && i.book.pending.is_empty()
                        && i.external_positions.is_empty()
                        && i.external_orders.is_empty(),
                    "{}仍有持仓（{}）、待确认订单（{}）或外部账户差异（{}），暂不能更换该模式的账户密钥；切换交易模式时请保留原账户密钥",
                    match engine.mode {
                        Mode::Testnet => "测试网",
                        _ => "实盘",
                    },
                    i.book.positions.len(),
                    i.book.pending.len(),
                    i.external_positions.len() + i.external_orders.len()
                );
            }
        }
        let mut replacements = Vec::new();
        for engine in &changed {
            let replacement = Self::build_mode(&self.store, &candidate, engine.mode)?;
            let previous = engine.inner.lock().await;
            let mut inner = replacement.inner.lock().await;
            inner.catalog = previous.catalog.clone();
            inner.quotes = previous.quotes.clone();
            inner.market_at = previous.market_at;
            drop(inner);
            replacements.push(replacement);
        }
        let next = replacements
            .iter()
            .chain(runtime.engines.iter().map(|managed| &managed.engine))
            .find(|engine| engine.mode == candidate.mode)
            .cloned();
        let next = match next {
            Some(engine) => engine,
            None => Self::build(&self.store, &candidate)?,
        };
        {
            let i = next.inner.lock().await;
            ensure!(
                Arc::ptr_eq(&next, &old)
                    || candidate.mode == Mode::Paper
                    || next.exchange.configured()
                    || (i.book.positions.is_empty() && i.book.pending.is_empty()),
                "该模式有已保存的持仓或订单，请同时填写原账户的 API Key 与 Secret"
            );
        }
        if switching {
            next.set_running(false).await?;
            candidate.managed_modes = runtime
                .engines
                .iter()
                .map(|managed| managed.engine.mode)
                .collect();
            if !candidate.managed_modes.contains(&next.mode) {
                candidate.managed_modes.push(next.mode);
            }
        }
        self.store.save_settings(&candidate)?;
        let started = runtime.started;
        for replacement in replacements {
            let managed = runtime
                .engines
                .iter_mut()
                .find(|managed| managed.engine.mode == replacement.mode)
                .unwrap();
            managed.tasks.take();
            managed.engine = replacement;
            managed.tasks = started.then(|| managed.engine.spawn());
        }
        if !runtime
            .engines
            .iter()
            .any(|managed| managed.engine.mode == next.mode)
        {
            runtime.engines.push(ManagedEngine {
                tasks: started.then(|| next.spawn()),
                engine: next.clone(),
            });
        }
        if let Some(client) = jev_client {
            for managed in &runtime.engines {
                *managed.engine.jev.write().await = client.clone();
            }
        }
        *active = next.clone();
        *settings = candidate;
        next.event(
            "info",
            None,
            if switching {
                "已切换交易模式；原模式新增开仓暂停，持仓与订单继续后台管理；当前模式新增开仓保持暂停"
            } else {
                "账户与连接配置已保存到 SQLite"
            },
            serde_json::json!({"mode": next.mode, "previous_mode": old.mode}),
        );
        Ok(settings.view())
    }
    pub async fn check_connection(&self) -> Result<&'static str> {
        let engine = self.engine.read().await;
        if engine.mode == Mode::Paper {
            engine.exchange.sync_time().await?;
            Ok("币安公共行情连接正常；模拟模式无需币安密钥")
        } else {
            ensure!(
                engine.exchange.configured(),
                "请先保存当前模式的币安 API Key 和 Secret"
            );
            engine.exchange.preflight().await?;
            Ok("币安账户连接正常，已确认或自动设置为单向持仓；未提交交易订单")
        }
    }
    async fn credentials_for_test(&self, request: CredentialTestRequest) -> Result<Credentials> {
        let settings = self.settings.lock().await;
        let saved = match request.mode {
            Mode::Testnet => &settings.testnet,
            Mode::Live => &settings.live,
            Mode::Paper => anyhow::bail!("请选择测试网或实盘账户进行密钥测试"),
        };
        saved.for_test(request.credentials)
    }
    pub async fn test_credentials(
        &self,
        request: CredentialTestRequest,
    ) -> Result<CredentialTestResult> {
        let mode = request.mode;
        let credentials = self.credentials_for_test(request).await?;
        // Use an isolated client: testing never replaces the active engine or saves settings.
        let client = Binance::with_credentials(mode, &credentials)?.with_store(self.store.clone());
        let can_trade = client.verify_credentials().await?;
        Ok(CredentialTestResult {
            mode,
            ok: true,
            can_trade,
            message: if can_trade {
                "密钥验证成功，已读取合约账户；账户允许交易，下单权限需在币安 API 管理中确认。"
            } else {
                "密钥验证成功，已读取合约账户；账户当前不允许交易，请检查账户状态。"
            },
        })
    }
    pub async fn test_jev_credentials(
        &self,
        request: JevCredentialTestRequest,
    ) -> Result<JevCredentialTestResult> {
        let key = self.settings.lock().await.jev_key_for_test(request)?;
        let client = Jev::with_key(&key)?;
        client.verify_credentials().await?;
        Ok(JevCredentialTestResult {
            ok: true,
            message: "Jev 连接正常，密钥验证成功，已读取可用模型；未调用决策模型。",
        })
    }
    pub async fn shutdown(&self) {
        let _active = self.engine.read().await;
        let mut runtime = self.runtime.lock().await;
        for managed in &mut runtime.engines {
            let _ = managed.engine.set_running(false).await;
            managed.tasks.take();
        }
        runtime.started = false;
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::*;
    fn patch(value: serde_json::Value) -> SettingsPatch {
        serde_json::from_value(value).unwrap()
    }
    fn testnet_settings() -> AppSettings {
        AppSettings::default()
            .patched(patch(serde_json::json!({
                "mode":"testnet", "testnet":{"key":"original-key","secret":"original-secret"},
                "live":{"key":"live-key","secret":"live-secret"}, "live_enabled":true
            })))
            .unwrap()
    }
    fn pending() -> PendingOrder {
        PendingOrder {
            client_id: "unknown".into(),
            symbol: "BTCUSDT".into(),
            side: Action::Long,
            role: OrderRole::Entry,
            quantity: 1.into(),
            price: None,
            created_at: now_ms(),
            signal_time: 0,
            target_profit: 1.into(),
            stop_loss_usdt: rust_decimal::Decimal::ZERO,
            maker_fee: rust_decimal::Decimal::ZERO,
            taker_fee: rust_decimal::Decimal::ZERO,
            status: "UNKNOWN".into(),
            accounted_quantity: rust_decimal::Decimal::ZERO,
            settlement_issue: None,
            leverage: 2,
        }
    }
    fn position() -> Position {
        Position {
            symbol: "BTCUSDT".into(),
            side: Action::Long,
            quantity: 1.into(),
            entry_price: 100.into(),
            opened_at: now_ms(),
            signal_time: 0,
            target_profit: 1.into(),
            stop_loss_usdt: 1.into(),
            stop_loss_triggered: false,
            entry_fee: rust_decimal::Decimal::ZERO,
            exit_fee: rust_decimal::Decimal::ZERO,
            realized_gross: rust_decimal::Decimal::ZERO,
            funding_cost: rust_decimal::Decimal::ZERO,
            maker_fee: rust_decimal::Decimal::ZERO,
            taker_fee: rust_decimal::Decimal::ZERO,
            tp_price: None,
            tp_client_id: None,
            observed_trade_ids: vec![],
            owned_order_ids: vec![],
            accounting_complete: true,
            original_quantity: 1.into(),
            leverage: 2,
        }
    }
    async fn save_exposure(engine: &Engine) {
        let mut i = engine.inner.lock().await;
        i.book.positions.insert("BTCUSDT".into(), position());
        i.book.pending.insert("unknown".into(), pending());
        engine.store.save(engine.mode, &i.config, &i.book).unwrap();
    }
    #[tokio::test]
    async fn testnet_to_live_retains_exposure_and_switching_back_reuses_the_engine() {
        let store = Store::open(":memory:", Mode::Paper).unwrap();
        store.save_settings(&testnet_settings()).unwrap();
        let app = Application::new(store.clone()).unwrap();
        let original = app.engine.read().await.clone();
        save_exposure(&original).await;
        {
            let mut i = original.inner.lock().await;
            i.paused = false;
            i.external_positions.insert("ETHUSDT".into(), 2.into());
            i.external_orders.push("ETHUSDT".into());
        }
        // Switching selection retains this engine and must not wait for its slow
        // reconciliation. Only actually replacing an account needs that lock.
        let in_flight = original.reconcile_lock.lock().await;
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            app.configure_connections(patch(serde_json::json!({"mode":"live"}))),
        )
        .await
        .unwrap()
        .unwrap();
        drop(in_flight);
        let live = app.engine.read().await.clone();
        assert_eq!(live.mode, Mode::Live);
        assert!(live.snapshot().await.paused);
        assert!(live.inner.lock().await.book.positions.is_empty());
        assert!(original.snapshot().await.paused);
        let backgrounds = app.background_modes(Mode::Live).await;
        assert_eq!(backgrounds.len(), 1);
        assert_eq!(backgrounds[0].mode, Mode::Testnet);
        assert_eq!(backgrounds[0].positions, 1);
        assert_eq!(backgrounds[0].pending_orders, 1);
        assert_eq!(backgrounds[0].external_positions, 1);
        assert_eq!(backgrounds[0].external_orders, 1);
        assert!(!backgrounds[0].managing);
        app.configure_connections(patch(serde_json::json!({"jev_key":"updated-jev-key"})))
            .await
            .unwrap();
        assert!(original.jev.read().await.available());
        {
            let mut i = original.inner.lock().await;
            i.book.positions.get_mut("BTCUSDT").unwrap().quantity = 2.into();
            original
                .store
                .save(Mode::Testnet, &i.config, &i.book)
                .unwrap();
        }
        app.configure_connections(patch(serde_json::json!({"mode":"testnet"})))
            .await
            .unwrap();
        let returned = app.engine.read().await.clone();
        assert!(Arc::ptr_eq(&original, &returned));
        assert_eq!(
            returned.inner.lock().await.book.positions["BTCUSDT"].quantity,
            2.into()
        );
        assert!(
            returned
                .inner
                .lock()
                .await
                .book
                .pending
                .contains_key("unknown")
        );
        assert!(returned.snapshot().await.paused);
        app.configure_connections(patch(serde_json::json!({"mode":"live"})))
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&live, &*app.engine.read().await));
        assert_eq!(app.runtime.lock().await.engines.len(), 2);
        assert_eq!(store.settings().unwrap().mode, Mode::Live);
    }
    #[tokio::test]
    async fn background_accounts_with_positions_orders_or_differences_cannot_be_replaced() {
        for kind in ["position", "pending", "external_position", "external_order"] {
            let store = Store::open(":memory:", Mode::Paper).unwrap();
            store.save_settings(&testnet_settings()).unwrap();
            let app = Application::new(store.clone()).unwrap();
            let original = app.engine.read().await.clone();
            {
                let mut i = original.inner.lock().await;
                match kind {
                    "position" => {
                        i.book.positions.insert("BTCUSDT".into(), position());
                    }
                    "pending" => {
                        i.book.pending.insert("unknown".into(), pending());
                    }
                    "external_position" => {
                        i.external_positions.insert("ETHUSDT".into(), 2.into());
                    }
                    _ => i.external_orders.push("ETHUSDT".into()),
                }
                original
                    .store
                    .save(Mode::Testnet, &i.config, &i.book)
                    .unwrap();
            }
            app.configure_connections(patch(serde_json::json!({"mode":"live"})))
                .await
                .unwrap();
            let live = app.engine.read().await.clone();
            for input in [
                serde_json::json!({"testnet":{"key":"replacement-key","secret":"replacement-secret"}}),
                serde_json::json!({"testnet":{"clear":true}}),
                serde_json::json!({"mode":"testnet","testnet":{"key":"replacement-key","secret":"replacement-secret"}}),
            ] {
                let error = app
                    .configure_connections(patch(input))
                    .await
                    .err()
                    .unwrap()
                    .to_string();
                assert!(error.contains("测试网") && error.contains("账户密钥"));
                assert!(
                    !error.contains("replacement-key") && !error.contains("replacement-secret")
                );
                assert_eq!(store.settings().unwrap().testnet.key, "original-key");
                assert_eq!(store.settings().unwrap().mode, Mode::Live);
                assert!(Arc::ptr_eq(&live, &*app.engine.read().await));
                assert!(
                    app.runtime
                        .lock()
                        .await
                        .engines
                        .iter()
                        .any(|m| Arc::ptr_eq(&m.engine, &original))
                );
            }
        }
    }
    #[tokio::test]
    async fn restart_restores_inactive_positions_and_orders_from_sqlite() {
        let path = std::env::temp_dir().join(format!(
            "genius-background-modes-{}-{}.sqlite",
            std::process::id(),
            now_ms()
        ));
        {
            let store = Store::open(path.to_str().unwrap(), Mode::Paper).unwrap();
            store.save_settings(&testnet_settings()).unwrap();
            let app = Application::new(store).unwrap();
            save_exposure(&app.engine.read().await.clone()).await;
            app.configure_connections(patch(serde_json::json!({"mode":"live"})))
                .await
                .unwrap();
        }
        {
            let store = Store::open(path.to_str().unwrap(), Mode::Paper).unwrap();
            let app = Application::new(store).unwrap();
            assert_eq!(app.settings_view().await.mode, Mode::Live);
            let backgrounds = app.background_modes(Mode::Live).await;
            assert_eq!(backgrounds.len(), 1);
            assert_eq!(backgrounds[0].mode, Mode::Testnet);
            assert_eq!(backgrounds[0].positions, 1);
            assert_eq!(backgrounds[0].pending_orders, 1);
            assert!(backgrounds[0].configured);
            app.configure_connections(patch(serde_json::json!({"mode":"testnet"})))
                .await
                .unwrap();
            let snapshot = app.engine.read().await.snapshot().await;
            assert!(snapshot.paused);
            assert_eq!(snapshot.positions.len(), 1);
            assert_eq!(snapshot.pending.len(), 1);
        }
        std::fs::remove_file(path).unwrap();
    }
    #[tokio::test]
    async fn restart_keeps_an_external_only_background_account_under_observation() {
        let store = Store::open(":memory:", Mode::Paper).unwrap();
        store.save_settings(&testnet_settings()).unwrap();
        let app = Application::new(store.clone()).unwrap();
        app.engine
            .read()
            .await
            .inner
            .lock()
            .await
            .external_orders
            .push("ETHUSDT".into());
        app.configure_connections(patch(serde_json::json!({"mode":"live"})))
            .await
            .unwrap();
        assert!(
            store
                .settings()
                .unwrap()
                .managed_modes
                .contains(&Mode::Testnet)
        );
        let restarted = Application::new(store).unwrap();
        let runtime = restarted.runtime.lock().await;
        assert_eq!(runtime.engines.len(), 2);
        let testnet = runtime
            .engines
            .iter()
            .find(|m| m.engine.mode == Mode::Testnet)
            .unwrap();
        assert!(testnet.engine.exchange.configured());
        assert!(testnet.engine.snapshot().await.paused);
    }
    #[tokio::test]
    async fn missing_background_credentials_can_be_restored_without_losing_exposure() {
        let store = Store::open(":memory:", Mode::Paper).unwrap();
        let book = Book {
            positions: [("BTCUSDT".into(), position())].into(),
            pending: [("unknown".into(), pending())].into(),
            ..Book::default()
        };
        store
            .for_mode(Mode::Testnet)
            .save(Mode::Testnet, &Config::default(), &book)
            .unwrap();
        let app = Application::new(store).unwrap();
        assert!(!app.background_modes(Mode::Paper).await[0].configured);
        assert!(
            app.configure_connections(patch(serde_json::json!({"mode":"testnet"})))
                .await
                .is_err()
        );
        app.configure_connections(patch(serde_json::json!({
            "mode":"testnet", "testnet":{"key":"original-key","secret":"original-secret"}
        })))
        .await
        .unwrap();
        let snapshot = app.engine.read().await.snapshot().await;
        assert!(snapshot.paused);
        assert_eq!(snapshot.positions.len(), 1);
        assert_eq!(snapshot.pending.len(), 1);
    }
    #[tokio::test]
    async fn background_live_risk_management_is_restored_when_live_entries_are_disabled() {
        let store = Store::open(":memory:", Mode::Paper).unwrap();
        let settings = testnet_settings()
            .patched(patch(
                serde_json::json!({"mode":"paper", "live_enabled":false}),
            ))
            .unwrap();
        store.save_settings(&settings).unwrap();
        let book = Book {
            positions: [("BTCUSDT".into(), position())].into(),
            ..Book::default()
        };
        store
            .for_mode(Mode::Live)
            .save(Mode::Live, &Config::default(), &book)
            .unwrap();
        let app = Application::new(store).unwrap();
        let backgrounds = app.background_modes(Mode::Paper).await;
        assert_eq!(backgrounds[0].mode, Mode::Live);
        assert!(backgrounds[0].configured);
        assert!(
            app.configure_connections(patch(serde_json::json!({"mode":"live"})))
                .await
                .is_err()
        );
        assert_eq!(app.settings_view().await.mode, Mode::Paper);
    }
    #[tokio::test]
    async fn failed_settings_save_preserves_original_engines_and_portfolios() {
        let store = Store::open(":memory:", Mode::Paper).unwrap();
        store.save_settings(&testnet_settings()).unwrap();
        let app = Application::new(store.clone()).unwrap();
        let original = app.engine.read().await.clone();
        save_exposure(&original).await;
        store.with_test_connection(|c| c.execute_batch("CREATE TRIGGER fail_settings BEFORE UPDATE ON app_settings BEGIN SELECT RAISE(ABORT,'test failure'); END").unwrap());
        assert!(
            app.configure_connections(patch(serde_json::json!({"mode":"live"})))
                .await
                .is_err()
        );
        assert!(Arc::ptr_eq(&original, &*app.engine.read().await));
        assert_eq!(store.settings().unwrap().mode, Mode::Testnet);
        assert_eq!(app.runtime.lock().await.engines.len(), 1);
        assert!(original.snapshot().await.paused);
        assert_eq!(original.inner.lock().await.book.pending.len(), 1);
    }
    #[tokio::test]
    async fn pending_orders_continue_reconciling_in_background_and_shutdown_stops_all_tasks() {
        use axum::{Json, Router, routing::get};
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        use std::time::Duration;
        let canceled = Arc::new(AtomicBool::new(false));
        let queries = Arc::new(AtomicUsize::new(0));
        let router = Router::new().route("/fapi/v1/order", get({
            let canceled = canceled.clone();
            let queries = queries.clone();
            move || {
                let canceled = canceled.clone();
                let queries = queries.clone();
                async move {
                    queries.fetch_add(1, Ordering::SeqCst);
                    Json(serde_json::json!({"symbol":"BTCUSDT", "clientOrderId":"unknown", "orderId":42,
                        "status":if canceled.load(Ordering::SeqCst) {"CANCELED"} else {"NEW"},
                        "executedQty":"0", "avgPrice":"0", "origQty":"1", "cumQuote":"0"}))
                }
            }
        }));
        let (base, server) = crate::test_support::mock_server(router).await;
        let store = Store::open(":memory:", Mode::Paper).unwrap();
        store.save_settings(&testnet_settings()).unwrap();
        let app = Application::new(store.clone()).unwrap();
        let original = Engine::new(
            Mode::Testnet,
            store.for_mode(Mode::Testnet),
            Config::default(),
            Binance::mock(base.clone()),
            Jev::new().unwrap(),
        )
        .unwrap();
        {
            let mut i = original.inner.lock().await;
            i.book.pending.insert("unknown".into(), pending());
            original
                .store
                .save(Mode::Testnet, &i.config, &i.book)
                .unwrap();
        }
        let live = Engine::new(
            Mode::Live,
            store.for_mode(Mode::Live),
            Config::default(),
            Binance::mock(base),
            Jev::new().unwrap(),
        )
        .unwrap();
        *app.engine.write().await = original.clone();
        app.runtime.lock().await.engines = vec![
            ManagedEngine {
                engine: original.clone(),
                tasks: None,
            },
            ManagedEngine {
                engine: live,
                tasks: None,
            },
        ];
        app.start().await;
        tokio::time::timeout(Duration::from_secs(3), async {
            while queries.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        app.configure_connections(patch(serde_json::json!({"mode":"live"})))
            .await
            .unwrap();
        let backgrounds = app.background_modes(Mode::Live).await;
        assert!(backgrounds[0].managing);
        canceled.store(true, Ordering::SeqCst);
        original.account_notify.notify_one();
        tokio::time::timeout(Duration::from_secs(3), async {
            while !original.inner.lock().await.book.pending.is_empty() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(original.store.load().unwrap().unwrap().1.pending.is_empty());
        assert!(original.snapshot().await.paused);
        app.start().await;
        assert_eq!(app.runtime.lock().await.engines.len(), 2);
        app.shutdown().await;
        assert!(
            app.runtime
                .lock()
                .await
                .engines
                .iter()
                .all(|m| m.tasks.is_none())
        );
        let after_shutdown = queries.load(Ordering::SeqCst);
        original.account_notify.notify_one();
        tokio::time::sleep(Duration::from_millis(350)).await;
        assert_eq!(queries.load(Ordering::SeqCst), after_shutdown);
        server.abort();
    }
    #[tokio::test]
    async fn jev_tests_reject_invalid_input_without_saving_or_pausing_trading() {
        let app = Application::new(Store::open(":memory:", Mode::Paper).unwrap()).unwrap();
        let active = app.engine.read().await.clone();
        active.inner.lock().await.paused = false;
        for input in [
            serde_json::json!({}),
            serde_json::json!({"clear_jev":true}),
            serde_json::json!({"jev_key":"invalid key"}),
        ] {
            assert!(
                app.test_jev_credentials(serde_json::from_value(input).unwrap())
                    .await
                    .is_err()
            );
        }
        assert!(app.store.settings().unwrap() == AppSettings::default());
        assert!(app.settings.lock().await.clone() == AppSettings::default());
        assert!(Arc::ptr_eq(&active, &*app.engine.read().await));
        assert!(!active.snapshot().await.paused);
    }
    #[tokio::test]
    async fn credential_tests_merge_only_the_selected_account_without_saving_or_switching() {
        let store = Store::open(":memory:", Mode::Paper).unwrap();
        let saved = AppSettings::default()
            .patched(patch(serde_json::json!({
                "testnet":{"key":"saved-testnet-key","secret":"saved-testnet-secret"},
                "live":{"key":"saved-live-key","secret":"saved-live-secret"}
            })))
            .unwrap();
        store.save_settings(&saved).unwrap();
        let app = Application::new(store.clone()).unwrap();
        let active = app.engine.read().await.clone();
        active.inner.lock().await.paused = false;
        for (mode, credentials, expected_key, expected_secret) in [
            (
                "testnet",
                serde_json::json!({"key":"","secret":""}),
                "saved-testnet-key",
                "saved-testnet-secret",
            ),
            (
                "live",
                serde_json::json!({}),
                "saved-live-key",
                "saved-live-secret",
            ),
            (
                "testnet",
                serde_json::json!({"key":" new-key ","secret":" new-secret "}),
                "new-key",
                "new-secret",
            ),
            (
                "live",
                serde_json::json!({"secret":"replacement-secret"}),
                "saved-live-key",
                "replacement-secret",
            ),
        ] {
            let request =
                serde_json::from_value(serde_json::json!({"mode":mode,"credentials":credentials}))
                    .unwrap();
            let candidate = app.credentials_for_test(request).await.unwrap();
            assert_eq!(candidate.key, expected_key);
            assert_eq!(candidate.secret, expected_secret);
        }
        assert!(store.settings().unwrap() == saved);
        assert!(app.settings.lock().await.clone() == saved);
        assert!(Arc::ptr_eq(&active, &*app.engine.read().await));
        assert!(!active.snapshot().await.paused);
        assert_eq!(active.mode, Mode::Paper);
    }
    #[tokio::test]
    async fn credential_tests_reject_missing_partial_clear_and_paper_requests() {
        let app = Application::new(Store::open(":memory:", Mode::Paper).unwrap()).unwrap();
        for value in [
            serde_json::json!({"mode":"testnet"}),
            serde_json::json!({"mode":"live","credentials":{"key":"only-key"}}),
            serde_json::json!({"mode":"testnet","credentials":{"clear":true}}),
            serde_json::json!({"mode":"paper","credentials":{"key":"key","secret":"secret"}}),
            serde_json::json!({"mode":"live","credentials":{"key":"invalid key","secret":"secret"}}),
        ] {
            assert!(
                app.test_credentials(serde_json::from_value(value).unwrap())
                    .await
                    .is_err()
            );
        }
        assert!(app.store.settings().unwrap() == AppSettings::default());
    }
    #[tokio::test]
    async fn mode_switches_persist_and_restore_isolated_portfolios_without_keys() {
        let store = Store::open(":memory:", Mode::Paper).unwrap();
        let app = Application::new(store.clone()).unwrap();
        {
            let e = app.engine.read().await;
            let mut i = e.inner.lock().await;
            i.book.pool.insert(
                "BTCUSDT".into(),
                PoolCoin {
                    symbol: "BTCUSDT".into(),
                    enabled: true,
                    config: None,
                },
            );
            store.save(Mode::Paper, &i.config, &i.book).unwrap();
        }
        let view = app
            .configure_connections(patch(serde_json::json!({"mode":"testnet"})))
            .await
            .unwrap();
        assert_eq!(view.mode, Mode::Testnet);
        {
            let e = app.engine.read().await;
            assert!(e.inner.lock().await.book.pool.is_empty());
            assert!(e.set_running(true).await.is_err());
        }
        let restarted = Application::new(store.clone()).unwrap();
        assert_eq!(restarted.settings_view().await.mode, Mode::Testnet);
        assert!(restarted.engine.read().await.snapshot().await.paused);
        app.configure_connections(patch(serde_json::json!({"mode":"paper"})))
            .await
            .unwrap();
        assert_eq!(app.engine.read().await.snapshot().await.pool.len(), 1);
    }
    #[tokio::test]
    async fn pending_intents_block_account_replacement_and_do_not_persist_keys() {
        let store = Store::open(":memory:", Mode::Paper).unwrap();
        store.save_settings(&AppSettings::default().patched(patch(serde_json::json!({
            "mode":"testnet", "testnet":{"key":"original-key","secret":"original-secret"}
        }))).unwrap()).unwrap();
        let app = Application::new(store.clone()).unwrap();
        {
            let e = app.engine.read().await;
            let mut i = e.inner.lock().await;
            i.book.pending.insert(
                "unknown".into(),
                PendingOrder {
                    client_id: "unknown".into(),
                    symbol: "BTCUSDT".into(),
                    side: Action::Long,
                    role: OrderRole::Entry,
                    quantity: 1.into(),
                    price: None,
                    created_at: now_ms(),
                    signal_time: 0,
                    target_profit: 1.into(),
                    stop_loss_usdt: rust_decimal::Decimal::ZERO,
                    maker_fee: rust_decimal::Decimal::ZERO,
                    taker_fee: rust_decimal::Decimal::ZERO,
                    status: "UNKNOWN".into(),
                    accounted_quantity: rust_decimal::Decimal::ZERO,
                    settlement_issue: None,
                    leverage: 2,
                },
            );
        }
        assert!(app.configure_connections(patch(serde_json::json!({"mode":"testnet","testnet":{"key":"new-key","secret":"new-secret"}}))).await.is_err());
        assert_eq!(store.settings().unwrap().mode, Mode::Testnet);
        assert_eq!(store.settings().unwrap().testnet.key, "original-key");
        assert!(
            app.engine
                .read()
                .await
                .inner
                .lock()
                .await
                .book
                .pending
                .contains_key("unknown")
        );
        let before = app.engine.read().await.clone();
        app.configure_connections(patch(serde_json::json!({"jev_key":"pending-safe-jev-key"})))
            .await
            .unwrap();
        let after = app.engine.read().await.clone();
        assert!(Arc::ptr_eq(&before, &after));
        assert!(after.jev.read().await.available());
        assert!(
            after
                .inner
                .lock()
                .await
                .book
                .pending
                .contains_key("unknown")
        );
        assert_eq!(store.settings().unwrap().jev_key, "pending-safe-jev-key");
    }
}
