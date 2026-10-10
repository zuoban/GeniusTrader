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
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

pub struct Application {
    pub auth: Auth,
    pub engine: RwLock<Arc<Engine>>,
    settings: Mutex<AppSettings>,
    pub store: Store,
    update_lock: Mutex<()>,
    tasks: Mutex<Option<EngineTasks>>,
}
impl Application {
    fn build(store: &Store, settings: &AppSettings) -> Result<Arc<Engine>> {
        settings.validate()?;
        Engine::new(
            settings.mode,
            store.for_mode(settings.mode),
            Config::default(),
            Binance::with_credentials(settings.mode, settings.credentials())?,
            Jev::with_key(&settings.jev_key)?,
        )
    }
    pub fn new(store: Store) -> Result<Arc<Self>> {
        let settings = store.settings()?;
        let engine = Self::build(&store, &settings)?;
        store.save_settings(&settings)?;
        Ok(Arc::new(Self {
            auth: Auth::new(store.clone()),
            engine: RwLock::new(engine),
            settings: Mutex::new(settings),
            store,
            update_lock: Mutex::new(()),
            tasks: Mutex::new(None),
        }))
    }
    #[cfg(test)]
    pub(crate) fn from_engine(engine: Arc<Engine>) -> Arc<Self> {
        Arc::new(Self {
            auth: Auth::new(engine.store.clone()),
            store: engine.store.clone(),
            engine: RwLock::new(engine),
            settings: Mutex::new(AppSettings::default()),
            update_lock: Mutex::new(()),
            tasks: Mutex::new(None),
        })
    }
    pub async fn start(&self) {
        let engine = self.engine.read().await;
        let mut tasks = self.tasks.lock().await;
        if tasks.is_none() {
            *tasks = Some(engine.spawn());
        }
    }
    pub async fn settings_view(&self) -> SettingsView {
        self.settings.lock().await.view()
    }
    pub async fn configure_connections(&self, patch: SettingsPatch) -> Result<SettingsView> {
        let _update = self.update_lock.lock().await;
        let mut active = self.engine.write().await;
        let mut settings = self.settings.lock().await;
        let candidate = settings.patched(patch)?;
        if settings.same_account(&candidate) {
            let replacement = if settings.jev_key != candidate.jev_key {
                let client = Jev::with_key(&candidate.jev_key)?;
                active.set_running(false).await?;
                Some(client)
            } else {
                None
            };
            self.store.save_settings(&candidate)?;
            if let Some(client) = replacement {
                *active.jev.write().await = Arc::new(client);
                active.event(
                    "info",
                    None,
                    "Jev 连接配置已保存；新增开仓保持暂停",
                    serde_json::json!({}),
                );
            }
            *settings = candidate;
            return Ok(settings.view());
        }
        let old = active.clone();
        old.set_running(false).await?;
        // Stop new entry reservations, then wait for any reconciliation already in progress.
        let _reconcile = old.reconcile_lock.lock().await;
        {
            let i = old.inner.lock().await;
            ensure!(
                i.book.positions.is_empty()
                    && i.book.pending.is_empty()
                    && i.external_positions.is_empty()
                    && i.external_orders.is_empty(),
                "已暂停新增开仓；当前仍有持仓、待确认订单或外部账户差异，暂不能更换模式或当前连接"
            );
        }
        let next = Self::build(&self.store, &candidate)?;
        {
            let i = next.inner.lock().await;
            ensure!(
                candidate.mode == Mode::Paper
                    || next.exchange.configured()
                    || (i.book.positions.is_empty() && i.book.pending.is_empty()),
                "该模式有已保存的持仓或订单，请同时填写原账户的 API Key 与 Secret"
            );
        }
        if candidate.mode == old.mode {
            // Preserve public data to avoid interrupting the console for a key-only change.
            let previous = old.inner.lock().await;
            let mut inner = next.inner.lock().await;
            inner.catalog = previous.catalog.clone();
            inner.quotes = previous.quotes.clone();
            inner.market_at = previous.market_at;
        }
        self.store.save_settings(&candidate)?;
        let mut tasks = self.tasks.lock().await;
        let was_running = tasks.is_some();
        tasks.take(); // Abort the old tasks only after all order intents are confirmed absent.
        *active = next.clone();
        *settings = candidate;
        if was_running {
            *tasks = Some(next.spawn());
        }
        next.event(
            "info",
            None,
            "账户与连接配置已保存到 SQLite；新增开仓保持暂停",
            serde_json::json!({"mode": next.mode}),
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
        let client = Binance::with_credentials(mode, &credentials)?;
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
        let engine = self.engine.read().await;
        let _ = engine.set_running(false).await;
        self.tasks.lock().await.take();
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::*;
    fn patch(value: serde_json::Value) -> SettingsPatch {
        serde_json::from_value(value).unwrap()
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
    async fn pending_intents_block_connection_switch_and_do_not_persist_keys() {
        let store = Store::open(":memory:", Mode::Paper).unwrap();
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
                    leverage: 2,
                },
            );
        }
        assert!(app.configure_connections(patch(serde_json::json!({"mode":"testnet","testnet":{"key":"new-key","secret":"new-secret"}}))).await.is_err());
        assert_eq!(store.settings().unwrap().mode, Mode::Paper);
        assert!(!store.settings().unwrap().testnet.configured());
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
