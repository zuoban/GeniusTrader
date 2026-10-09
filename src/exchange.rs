use crate::{config::Mode, domain::*, settings::Credentials};
use anyhow::{Context, Result, ensure};
use hmac::{Hmac, Mac};
use reqwest::{Client, Method};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::Sha256;
use std::{
    collections::BTreeMap,
    str::FromStr,
    sync::atomic::{AtomicI64, Ordering},
    time::{Duration, Instant},
};
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct ExchangeError {
    pub code: i64,
    pub status: u16,
    pub uncertain: bool,
}
impl std::fmt::Display for ExchangeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Binance error code={}, HTTP={}, uncertain={}",
            self.code, self.status, self.uncertain
        )
    }
}
impl std::error::Error for ExchangeError {}
pub fn decimal(v: &Value) -> Result<Decimal> {
    Decimal::from_str(v.as_str().context("expected decimal string")?).context("invalid decimal")
}
fn num(v: &Value) -> Result<f64> {
    let n = v
        .as_str()
        .context("expected number string")?
        .parse::<f64>()?;
    ensure!(n.is_finite(), "non-finite market data");
    Ok(n)
}
pub fn signature(secret: &str, query: &str) -> String {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key size");
    mac.update(query.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

pub struct Binance {
    client: Client,
    pub base: String,
    key: Option<String>,
    secret: Option<String>,
    offset: AtomicI64,
    throttle: Mutex<Instant>,
    position_mode_lock: Mutex<()>,
}
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExchangeOrder {
    pub symbol: String,
    pub client_order_id: String,
    pub order_id: i64,
    pub status: String,
    pub executed_qty: Decimal,
    pub avg_price: Decimal,
    pub orig_qty: Decimal,
    pub cum_quote: Decimal,
}
impl ExchangeOrder {
    pub fn terminal(&self) -> bool {
        matches!(
            self.status.as_str(),
            "FILLED" | "CANCELED" | "EXPIRED" | "EXPIRED_IN_MATCH" | "REJECTED"
        )
    }
}
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RemotePosition {
    pub symbol: String,
    pub position_amt: Decimal,
    pub entry_price: Decimal,
    pub position_side: String,
}
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Fill {
    pub id: i64,
    pub order_id: i64,
    pub qty: Decimal,
    pub price: Decimal,
    pub commission: Decimal,
    pub commission_asset: String,
    pub realized_pnl: Decimal,
    pub time: i64,
    pub side: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all(deserialize = "camelCase"))]
pub struct AccountInfo {
    pub multi_assets_margin: bool,
    pub total_wallet_balance: Decimal,
    pub total_unrealized_profit: Decimal,
    pub total_margin_balance: Decimal,
    pub available_balance: Decimal,
    pub total_initial_margin: Decimal,
    pub assets: Vec<AccountAsset>,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all(deserialize = "camelCase"))]
pub struct AccountAsset {
    pub asset: String,
    pub wallet_balance: Decimal,
    pub unrealized_profit: Decimal,
    pub margin_balance: Decimal,
    #[serde(skip_deserializing)]
    pub wallet_value_usd: Option<Decimal>,
}
#[derive(Deserialize)]
struct AssetIndex {
    symbol: String,
    index: Decimal,
    time: i64,
}
impl AccountInfo {
    pub fn paper(wallet: Decimal, unrealized: Decimal, available: Decimal) -> Self {
        let margin = wallet + unrealized;
        Self {
            multi_assets_margin: false,
            total_wallet_balance: wallet,
            total_unrealized_profit: unrealized,
            total_margin_balance: margin,
            available_balance: available,
            total_initial_margin: (margin - available).max(Decimal::ZERO),
            assets: vec![AccountAsset {
                asset: "USDT".into(),
                wallet_balance: wallet,
                unrealized_profit: unrealized,
                margin_balance: margin,
                wallet_value_usd: Some(wallet),
            }],
        }
    }
    fn value_assets(&mut self, indexes: Vec<AssetIndex>, now: i64) {
        let prices: BTreeMap<_, _> = indexes
            .into_iter()
            .filter(|p| p.index > Decimal::ZERO && (now - 60_000..=now + 5_000).contains(&p.time))
            .map(|p| (p.symbol, p.index))
            .collect();
        for asset in &mut self.assets {
            asset.wallet_value_usd = prices
                .get(&format!("{}USD", asset.asset))
                .and_then(|price| asset.wallet_balance.checked_mul(*price));
        }
    }
}

impl Binance {
    #[cfg(test)]
    pub(crate) fn mock(base: String) -> Self {
        Self {
            client: Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(2))
                .build()
                .unwrap(),
            base,
            key: Some("local-test-key".into()),
            secret: Some("local-test-secret".into()),
            offset: AtomicI64::new(0),
            throttle: Mutex::new(Instant::now()),
            position_mode_lock: Mutex::new(()),
        }
    }
    pub fn new(mode: Mode) -> Result<Self> {
        Self::with_credentials(mode, &Credentials::default())
    }
    pub fn with_credentials(mode: Mode, credentials: &Credentials) -> Result<Self> {
        let base = if mode == Mode::Testnet {
            "https://demo-fapi.binance.com"
        } else {
            "https://fapi.binance.com"
        }
        .into();
        let key =
            (mode != Mode::Paper && !credentials.key.is_empty()).then(|| credentials.key.clone());
        let secret = (mode != Mode::Paper && !credentials.secret.is_empty())
            .then(|| credentials.secret.clone());
        Ok(Self {
            client: Client::builder()
                .timeout(Duration::from_secs(8))
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            base,
            key,
            secret,
            offset: AtomicI64::new(0),
            throttle: Mutex::new(Instant::now()),
            position_mode_lock: Mutex::new(()),
        })
    }
    pub fn configured(&self) -> bool {
        self.key.is_some() && self.secret.is_some()
    }
    async fn request(
        &self,
        method: Method,
        path: &str,
        mut params: Vec<(String, String)>,
        signed: bool,
    ) -> Result<Value> {
        {
            let mut next = self.throttle.lock().await;
            let wait = next.saturating_duration_since(Instant::now());
            if !wait.is_zero() {
                tokio::time::sleep(wait).await;
            }
            *next = Instant::now() + Duration::from_millis(100);
        }
        if signed {
            params.push((
                "timestamp".into(),
                (now_ms() + self.offset.load(Ordering::Relaxed)).to_string(),
            ));
            params.push(("recvWindow".into(), "5000".into()));
        }
        let mut query = {
            let mut serializer = url::form_urlencoded::Serializer::new(String::new());
            for (k, v) in &params {
                serializer.append_pair(k, v);
            }
            serializer.finish()
        };
        if signed {
            let sig = signature(
                self.secret.as_deref().context("missing Binance secret")?,
                &query,
            );
            query.push_str("&signature=");
            query.push_str(&sig);
        }
        let mut req = self
            .client
            .request(method.clone(), format!("{}{}", self.base, path));
        if signed {
            req = req.header(
                "X-MBX-APIKEY",
                self.key.as_deref().context("missing Binance key")?,
            );
        }
        req = if method == Method::POST {
            req.header("content-type", "application/x-www-form-urlencoded")
                .body(query)
        } else {
            req.query(&url::form_urlencoded::parse(query.as_bytes()).collect::<Vec<_>>())
        };
        let response = req.send().await.map_err(|_| ExchangeError {
            code: 0,
            status: 0,
            uncertain: method == Method::POST || method == Method::DELETE,
        })?;
        let status = response.status();
        let value: Value = response.json().await.map_err(|_| ExchangeError {
            code: 0,
            status: status.as_u16(),
            uncertain: method == Method::POST || method == Method::DELETE,
        })?;
        if !status.is_success() {
            let code = value["code"].as_i64().unwrap_or(0);
            return Err(ExchangeError {
                code,
                status: status.as_u16(),
                uncertain: (status.is_server_error() || matches!(code, -1006 | -1007))
                    && (method == Method::POST || method == Method::DELETE),
            }
            .into());
        }
        Ok(value)
    }
    pub async fn sync_time(&self) -> Result<()> {
        let start = now_ms();
        let v = self
            .request(Method::GET, "/fapi/v1/time", vec![], false)
            .await?;
        let end = now_ms();
        self.offset.store(
            v["serverTime"].as_i64().context("missing serverTime")? - (start + end) / 2,
            Ordering::Relaxed,
        );
        Ok(())
    }
    pub async fn symbols(&self) -> Result<BTreeMap<String, SymbolInfo>> {
        let info = self
            .request(Method::GET, "/fapi/v1/exchangeInfo", vec![], false)
            .await?;
        let ticks = self
            .request(Method::GET, "/fapi/v1/ticker/24hr", vec![], false)
            .await?;
        let mut volumes = BTreeMap::new();
        for t in ticks.as_array().context("invalid ticker array")? {
            if let (Some(s), Ok(v), Ok(p)) = (
                t["symbol"].as_str(),
                decimal(&t["quoteVolume"]),
                decimal(&t["lastPrice"]),
            ) {
                volumes.insert(s.to_string(), (v, p));
            }
        }
        let mut out = BTreeMap::new();
        for s in info["symbols"].as_array().context("missing symbols")? {
            if s["status"] != "TRADING"
                || s["contractType"] != "PERPETUAL"
                || s["quoteAsset"] != "USDT"
                || s["marginAsset"] != "USDT"
                || s["underlyingType"] != "COIN"
            {
                continue;
            }
            let symbol = s["symbol"].as_str().context("missing symbol")?.to_string();
            let filters = s["filters"].as_array().context("missing filters")?;
            let filter = |kind: &str| -> Result<&Value> {
                filters
                    .iter()
                    .find(|x| x["filterType"] == kind)
                    .context("missing symbol filter")
            };
            let price = filter("PRICE_FILTER")?;
            let lot = filter("LOT_SIZE")?;
            let market = filter("MARKET_LOT_SIZE")?;
            let min = filter("MIN_NOTIONAL")?;
            let (volume, last) = volumes
                .get(&symbol)
                .cloned()
                .unwrap_or((Decimal::ZERO, Decimal::ZERO));
            let r = SymbolInfo {
                symbol: symbol.clone(),
                quote_volume: volume,
                last_price: last,
                tick_size: decimal(&price["tickSize"])?,
                step_size: decimal(&lot["stepSize"])?,
                market_step: decimal(&market["stepSize"])?,
                min_qty: decimal(&lot["minQty"])?,
                market_min_qty: decimal(&market["minQty"])?,
                min_notional: decimal(&min["notional"])?,
                max_qty: decimal(&lot["maxQty"])?,
                market_max_qty: decimal(&market["maxQty"])?,
            };
            ensure!(
                r.tick_size > Decimal::ZERO
                    && r.step_size > Decimal::ZERO
                    && r.market_step > Decimal::ZERO,
                "invalid symbol steps"
            );
            out.insert(symbol, r);
        }
        Ok(out)
    }
    pub async fn quotes(&self) -> Result<BTreeMap<String, Quote>> {
        let v = self
            .request(Method::GET, "/fapi/v1/ticker/bookTicker", vec![], false)
            .await?;
        let mut out = BTreeMap::new();
        for q in v.as_array().context("invalid quotes")? {
            let quote = Quote {
                bid: decimal(&q["bidPrice"])?,
                ask: decimal(&q["askPrice"])?,
                bid_qty: decimal(&q["bidQty"])?,
                ask_qty: decimal(&q["askQty"])?,
                time: now_ms(),
            };
            if quote.validate().is_ok() {
                out.insert(
                    q["symbol"].as_str().context("missing symbol")?.into(),
                    quote,
                );
            }
        }
        Ok(out)
    }
    pub async fn candles(&self, symbol: &str, limit: usize) -> Result<Vec<Candle>> {
        let v = self
            .request(
                Method::GET,
                "/fapi/v1/klines",
                vec![
                    ("symbol".into(), symbol.into()),
                    ("interval".into(), "5m".into()),
                    ("limit".into(), limit.to_string()),
                ],
                false,
            )
            .await?;
        let now = now_ms() + self.offset.load(Ordering::Relaxed);
        let mut out = vec![];
        for row in v.as_array().context("invalid candles")? {
            let a = row.as_array().context("invalid candle")?;
            ensure!(a.len() >= 7, "short candle");
            let close_time = a[6].as_i64().context("missing closeTime")?;
            if close_time >= now {
                continue;
            }
            out.push(Candle {
                open_time: a[0].as_i64().context("missing openTime")?,
                close_time,
                open: num(&a[1])?,
                high: num(&a[2])?,
                low: num(&a[3])?,
                close: num(&a[4])?,
                volume: num(&a[5])?,
            });
        }
        Ok(out)
    }
    pub async fn preflight(&self) -> Result<Decimal> {
        self.sync_time().await?;
        self.ensure_one_way_mode().await?;
        self.available_balance().await
    }
    pub async fn verify_credentials(&self) -> Result<bool> {
        ensure!(self.configured(), "请填写 API Key 与 Secret 后再测试");
        let result = async {
            self.sync_time().await?;
            // V2 exposes canTrade; this signed GET validates the key/secret without mutations.
            let account = self
                .request(Method::GET, "/fapi/v2/account", vec![], true)
                .await?;
            account["canTrade"]
                .as_bool()
                .context("币安账户响应格式异常，请稍后重试")
        }
        .await;
        result.map_err(|error| {
            let Some(exchange) = error.downcast_ref::<ExchangeError>() else {
                return error;
            };
            let message = match exchange.code {
                -2014 => "API Key 格式无效，请检查复制内容（-2014）",
                -2015 => "验证失败：API Key 无效、IP 未获授权或权限不足；请确认密钥属于所选测试网 / 实盘环境，并检查合约权限及 IP 白名单（-2015）",
                -1022 => "签名验证失败，请检查 API Secret 是否正确、是否与 API Key 配对（-1022）",
                -1021 => "请求时间校验失败，请检查系统时间或网络延迟后重试（-1021）",
                -1002 | -1099 => "账户访问未获授权，请检查 API 权限与账户状态",
                -2017 => "该账户的 API 密钥已被锁定，请检查币安账户状态（-2017）",
                -1003 | -1015 => "币安请求频率受限，请稍后重试",
                _ => match exchange.status {
                    0 => "无法连接币安或请求超时，请检查网络后重试",
                    401 | 403 => "币安拒绝访问，请检查 API 权限、IP 白名单或访问限制",
                    418 | 429 => "币安请求频率受限，请稍后重试",
                    451 => "币安限制当前地区访问，请检查服务所在地区",
                    500..=599 => "币安服务暂时不可用，请稍后重试",
                    _ => "币安账户验证失败或响应异常，请稍后重试",
                },
            };
            anyhow::anyhow!(message)
        })
    }
    async fn hedge_mode(&self) -> Result<bool> {
        let m = self
            .request(Method::GET, "/fapi/v1/positionSide/dual", vec![], true)
            .await?;
        m["dualSidePosition"]
            .as_bool()
            .context("币安持仓模式响应缺少有效的 dualSidePosition")
    }
    async fn ensure_one_way_mode(&self) -> Result<()> {
        // Startup reconciliation and a manual connection check can run concurrently.
        let _guard = self.position_mode_lock.lock().await;
        if !self.hedge_mode().await? {
            return Ok(());
        }
        if let Err(error) = self
            .request(
                Method::POST,
                "/fapi/v1/positionSide/dual",
                vec![("dualSidePosition".into(), "false".into())],
                true,
            )
            .await
        {
            let exchange_error = error.downcast_ref::<ExchangeError>();
            let message = match exchange_error.map(|e| e.code) {
                Some(-4067) => {
                    Some("自动设置单向持仓失败：币安账户存在未成交挂单，请处理挂单后重试（-4067）")
                }
                Some(-4068) => {
                    Some("自动设置单向持仓失败：币安账户存在持仓，请处理持仓后重试（-4068）")
                }
                // Another client may have changed the mode, or the POST response was lost.
                // Query the account to confirm the result instead of resending the POST.
                Some(-4059) => None,
                _ if exchange_error.is_some_and(|e| e.uncertain) => None,
                _ => Some("自动设置单向持仓失败，请检查币安 API 权限与账户状态"),
            };
            if let Some(message) = message {
                return Err(error.context(message));
            }
        }
        ensure!(
            !self
                .hedge_mode()
                .await
                .context("无法确认账户已切换为单向持仓，请稍后重试")?,
            "自动设置单向持仓尚未生效，账户仍为双向持仓，请稍后重试"
        );
        tracing::info!("币安账户已确认切换为单向持仓模式");
        Ok(())
    }
    pub async fn available_balance(&self) -> Result<Decimal> {
        let v = self
            .request(Method::GET, "/fapi/v3/balance", vec![], true)
            .await?;
        let usdt = v
            .as_array()
            .context("invalid balance")?
            .iter()
            .find(|x| x["asset"] == "USDT")
            .context("missing USDT balance")?;
        decimal(&usdt["availableBalance"])
    }
    pub async fn account_info(&self) -> Result<AccountInfo> {
        // V2 includes the margin mode and complete asset balances. Its account totals
        // are USDT in single-asset mode and USD in multi-assets mode.
        let mut account: AccountInfo = serde_json::from_value(
            self.request(Method::GET, "/fapi/v2/account", vec![], true)
                .await?,
        )?;
        // An unavailable valuation must not hide the native account balances.
        if let Ok(value) = self
            .request(Method::GET, "/fapi/v1/assetIndex", vec![], false)
            .await
            && let Ok(indexes) = serde_json::from_value(value)
        {
            account.value_assets(indexes, now_ms() + self.offset.load(Ordering::Relaxed));
        }
        Ok(account)
    }
    pub async fn positions(&self) -> Result<Vec<RemotePosition>> {
        Ok(serde_json::from_value(
            self.request(Method::GET, "/fapi/v3/positionRisk", vec![], true)
                .await?,
        )?)
    }
    pub async fn open_orders(&self) -> Result<Vec<ExchangeOrder>> {
        Ok(serde_json::from_value(
            self.request(Method::GET, "/fapi/v1/openOrders", vec![], true)
                .await?,
        )?)
    }
    pub async fn fees(&self, symbol: &str) -> Result<(Decimal, Decimal)> {
        let v = self
            .request(
                Method::GET,
                "/fapi/v1/commissionRate",
                vec![("symbol".into(), symbol.into())],
                true,
            )
            .await?;
        Ok((
            decimal(&v["makerCommissionRate"])?,
            decimal(&v["takerCommissionRate"])?,
        ))
    }
    pub async fn leverage(&self, symbol: &str, leverage: u8) -> Result<()> {
        self.request(
            Method::POST,
            "/fapi/v1/leverage",
            vec![
                ("symbol".into(), symbol.into()),
                ("leverage".into(), leverage.to_string()),
            ],
            true,
        )
        .await?;
        Ok(())
    }
    pub async fn submit(&self, o: &PendingOrder) -> Result<ExchangeOrder> {
        let exit = o.role != OrderRole::Entry;
        let mut p = vec![
            ("symbol".into(), o.symbol.clone()),
            (
                "side".into(),
                if exit {
                    o.side.exit_side()
                } else {
                    o.side.entry_side()
                }
                .into(),
            ),
            ("positionSide".into(), "BOTH".into()),
            (
                "type".into(),
                if o.role == OrderRole::TakeProfit {
                    "LIMIT"
                } else {
                    "MARKET"
                }
                .into(),
            ),
            ("quantity".into(), o.quantity.normalize().to_string()),
            ("newClientOrderId".into(), o.client_id.clone()),
            ("newOrderRespType".into(), "RESULT".into()),
        ];
        if exit {
            p.push(("reduceOnly".into(), "true".into()));
        }
        if let Some(price) = o.price {
            p.push(("price".into(), price.normalize().to_string()));
            p.push(("timeInForce".into(), "GTX".into()));
        }
        Ok(serde_json::from_value(
            self.request(Method::POST, "/fapi/v1/order", p, true)
                .await?,
        )?)
    }
    pub async fn order(&self, symbol: &str, id: &str) -> Result<ExchangeOrder> {
        Ok(serde_json::from_value(
            self.request(
                Method::GET,
                "/fapi/v1/order",
                vec![
                    ("symbol".into(), symbol.into()),
                    ("origClientOrderId".into(), id.into()),
                ],
                true,
            )
            .await?,
        )?)
    }
    pub async fn cancel(&self, symbol: &str, id: &str) -> Result<ExchangeOrder> {
        Ok(serde_json::from_value(
            self.request(
                Method::DELETE,
                "/fapi/v1/order",
                vec![
                    ("symbol".into(), symbol.into()),
                    ("origClientOrderId".into(), id.into()),
                ],
                true,
            )
            .await?,
        )?)
    }
    pub async fn fills(&self, symbol: &str, order_id: i64) -> Result<Vec<Fill>> {
        Ok(serde_json::from_value(
            self.request(
                Method::GET,
                "/fapi/v1/userTrades",
                vec![
                    ("symbol".into(), symbol.into()),
                    ("orderId".into(), order_id.to_string()),
                    ("limit".into(), "1000".into()),
                ],
                true,
            )
            .await?,
        )?)
    }
    pub async fn funding(&self, start: i64) -> Result<Vec<Value>> {
        let v = self
            .request(
                Method::GET,
                "/fapi/v1/income",
                vec![
                    ("incomeType".into(), "FUNDING_FEE".into()),
                    ("startTime".into(), start.to_string()),
                    ("limit".into(), "1000".into()),
                ],
                true,
            )
            .await?;
        Ok(v.as_array().context("invalid income")?.clone())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Json, Router,
        extract::{RawQuery, State},
        http::{HeaderMap, StatusCode},
        routing::get,
    };
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize},
    };

    fn asset_account() -> Value {
        serde_json::json!({
            "multiAssetsMargin": false, "totalWalletBalance": "1000",
            "totalUnrealizedProfit": "-10", "totalMarginBalance": "990",
            "availableBalance": "900", "totalInitialMargin": "90",
            "assets": [
                {"asset": "USDT", "walletBalance": "1000", "unrealizedProfit": "-10", "marginBalance": "990"},
                {"asset": "USDC", "walletBalance": "500", "unrealizedProfit": "0", "marginBalance": "500"},
                {"asset": "BTC", "walletBalance": "0.001", "unrealizedProfit": "0", "marginBalance": "0.001"}
            ]
        })
    }
    async fn credential_server(
        status: StatusCode,
        response: Value,
    ) -> (Binance, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
        let requests = Arc::new(AtomicUsize::new(0));
        let observed = requests.clone();
        let router = Router::new()
            .route(
                "/fapi/v1/time",
                get(|| async { Json(serde_json::json!({"serverTime":now_ms()})) }),
            )
            .route(
                "/fapi/v2/account",
                get(move |headers: HeaderMap, query: RawQuery| {
                    let response = response.clone();
                    async move {
                        assert_signed(&headers, &query.0.unwrap());
                        (status, Json(response))
                    }
                }),
            )
            .layer(axum::middleware::from_fn(
                move |req: axum::extract::Request, next: axum::middleware::Next| {
                    let observed = observed.clone();
                    async move {
                        assert_eq!(
                            req.method(),
                            Method::GET,
                            "credential tests must be read-only"
                        );
                        assert!(matches!(
                            req.uri().path(),
                            "/fapi/v1/time" | "/fapi/v2/account"
                        ));
                        observed.fetch_add(1, Ordering::SeqCst);
                        next.run(req).await
                    }
                },
            ));
        let (url, server) = crate::test_support::mock_server(router).await;
        (Binance::mock(url), requests, server)
    }
    #[tokio::test]
    async fn credential_verification_signs_read_only_requests_and_reports_account_status() {
        for can_trade in [true, false] {
            let (client, requests, server) =
                credential_server(StatusCode::OK, serde_json::json!({"canTrade":can_trade})).await;
            assert_eq!(client.verify_credentials().await.unwrap(), can_trade);
            assert_eq!(requests.load(Ordering::SeqCst), 2);
            server.abort();
        }
    }
    #[tokio::test]
    async fn credential_verification_explains_errors_without_exposing_remote_messages_or_secrets() {
        for (status, code, expected) in [
            (StatusCode::BAD_REQUEST, -1022, "API Secret"),
            (StatusCode::UNAUTHORIZED, -2015, "IP 白名单"),
            (StatusCode::BAD_REQUEST, -2014, "格式无效"),
            (StatusCode::BAD_REQUEST, -1021, "时间校验失败"),
            (StatusCode::BAD_REQUEST, -2017, "锁定"),
            (StatusCode::FORBIDDEN, 0, "拒绝访问"),
            (StatusCode::TOO_MANY_REQUESTS, -1003, "频率受限"),
            (StatusCode::SERVICE_UNAVAILABLE, 0, "暂时不可用"),
        ] {
            let (client, requests, server) = credential_server(
                status,
                serde_json::json!({"code":code,"msg":"local-test-key local-test-secret"}),
            )
            .await;
            let error = client.verify_credentials().await.unwrap_err().to_string();
            assert!(error.contains(expected), "{error}");
            assert!(!error.contains("local-test-key") && !error.contains("local-test-secret"));
            assert_eq!(requests.load(Ordering::SeqCst), 2);
            server.abort();
        }
    }
    #[tokio::test]
    async fn credential_verification_rejects_invalid_responses_and_network_failures() {
        for response in [
            serde_json::json!({}),
            serde_json::json!({"canTrade":"true"}),
        ] {
            let (client, _, server) = credential_server(StatusCode::OK, response).await;
            assert!(
                client
                    .verify_credentials()
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("响应格式异常")
            );
            server.abort();
        }
        let (client, _, server) =
            credential_server(StatusCode::OK, serde_json::json!({"canTrade":true})).await;
        server.abort();
        let _ = server.await;
        assert!(
            client
                .verify_credentials()
                .await
                .unwrap_err()
                .to_string()
                .contains("无法连接币安")
        );
    }
    async fn asset_server(
        index_status: StatusCode,
    ) -> (Binance, Arc<AtomicBool>, tokio::task::JoinHandle<()>) {
        let failing = Arc::new(AtomicBool::new(false));
        let failure = failing.clone();
        let app = Router::new()
            .route(
                "/fapi/v2/account",
                get(move |headers: HeaderMap, RawQuery(query): RawQuery| {
                    let failure = failure.clone();
                    async move {
                        assert_signed(&headers, &query.unwrap());
                        if failure.load(Ordering::SeqCst) {
                            (
                                StatusCode::SERVICE_UNAVAILABLE,
                                Json(serde_json::json!({"code": -1000})),
                            )
                        } else {
                            (StatusCode::OK, Json(asset_account()))
                        }
                    }
                }),
            )
            .route(
                "/fapi/v1/assetIndex",
                get(move |headers: HeaderMap| async move {
                    assert!(!headers.contains_key("X-MBX-APIKEY"));
                    (
                        index_status,
                        Json(serde_json::json!([
                            {"symbol":"USDTUSD", "index":"1", "time": now_ms()},
                            {"symbol":"USDCUSD", "index":"0.999", "time": now_ms()},
                            {"symbol":"BTCUSD", "index":"80000", "time": now_ms()}
                        ])),
                    )
                }),
            );
        let (url, server) = crate::test_support::mock_server(app).await;
        (Binance::mock(url), failing, server)
    }

    #[tokio::test]
    async fn account_assets_keep_exchange_totals_and_value_native_balances() {
        let (client, _, server) = asset_server(StatusCode::OK).await;
        let account = client.account_info().await.unwrap();
        assert!(!account.multi_assets_margin);
        assert_eq!(account.total_wallet_balance, Decimal::from(1000));
        assert_eq!(account.total_margin_balance, Decimal::from(990));
        assert_eq!(account.available_balance, Decimal::from(900));
        assert_eq!(
            account.assets[1].wallet_value_usd,
            Some(Decimal::new(4995, 1))
        );
        assert_eq!(account.assets[2].wallet_balance, Decimal::new(1, 3));
        assert_eq!(account.assets[2].wallet_value_usd, Some(Decimal::from(80)));
        let json = serde_json::to_value(account).unwrap();
        assert_eq!(json["total_wallet_balance"], "1000");
        assert_eq!(json["assets"][2]["asset"], "BTC");
        server.abort();
    }
    #[tokio::test]
    async fn failed_asset_prices_preserve_native_balances_without_fake_values() {
        let (client, _, server) = asset_server(StatusCode::SERVICE_UNAVAILABLE).await;
        let account = client.account_info().await.unwrap();
        assert_eq!(account.total_margin_balance, Decimal::from(990));
        assert_eq!(account.assets.len(), 3);
        assert!(
            account
                .assets
                .iter()
                .all(|asset| asset.wallet_value_usd.is_none())
        );
        server.abort();
    }
    #[test]
    fn stale_missing_and_invalid_asset_indexes_are_not_used() {
        let mut account: AccountInfo = serde_json::from_value(asset_account()).unwrap();
        account.value_assets(
            vec![
                AssetIndex {
                    symbol: "USDTUSD".into(),
                    index: Decimal::ONE,
                    time: 100_000,
                },
                AssetIndex {
                    symbol: "BTCUSD".into(),
                    index: Decimal::from(80000),
                    time: 1,
                },
                AssetIndex {
                    symbol: "USDCUSD".into(),
                    index: Decimal::ZERO,
                    time: 100_000,
                },
            ],
            100_000,
        );
        assert_eq!(
            account.assets[0].wallet_value_usd,
            Some(Decimal::from(1000))
        );
        assert!(account.assets[1].wallet_value_usd.is_none());
        assert!(account.assets[2].wallet_value_usd.is_none());
        account.value_assets(vec![], 100_000);
        assert!(
            account
                .assets
                .iter()
                .all(|asset| asset.wallet_value_usd.is_none())
        );
    }
    #[tokio::test]
    async fn failed_asset_refresh_keeps_last_snapshot_and_does_not_pause_trading() {
        let (client, failing, server) = asset_server(StatusCode::OK).await;
        let engine = crate::engine::Engine::new(
            Mode::Testnet,
            crate::store::Store::open(":memory:", Mode::Testnet).unwrap(),
            crate::config::Config::default(),
            client,
            crate::jev::Jev::new().unwrap(),
        )
        .unwrap();
        assert!(engine.snapshot().await.account_assets.is_none());
        engine.inner.lock().await.paused = false;
        engine.refresh_account_assets().await;
        let first = engine.snapshot().await;
        assert!(first.account_assets_at > 0);
        assert!(first.account_assets_error.is_none());
        failing.store(true, Ordering::SeqCst);
        engine.refresh_account_assets().await;
        let failed = engine.snapshot().await;
        assert_eq!(failed.account_assets_at, first.account_assets_at);
        assert_eq!(
            failed.account_assets.unwrap().total_margin_balance,
            Decimal::from(990)
        );
        assert!(failed.account_assets_error.is_some());
        assert!(!failed.paused);
        assert_eq!(failed.account_at, 0); // Display data cannot mark trading reconciliation complete.
        failing.store(false, Ordering::SeqCst);
        engine.refresh_account_assets().await;
        assert!(engine.snapshot().await.account_assets_error.is_none());
        server.abort();
    }

    struct Account {
        mode: Mutex<Value>,
        mode_queries: AtomicUsize,
        changes: AtomicUsize,
        balance_queries: AtomicUsize,
        status: StatusCode,
        code: i64,
        apply_change: bool,
    }
    fn assert_signed(headers: &HeaderMap, query: &str) {
        assert_eq!(headers["X-MBX-APIKEY"], "local-test-key");
        let (payload, actual_signature) = query.rsplit_once("&signature=").unwrap();
        assert_eq!(actual_signature, signature("local-test-secret", payload));
        let params: BTreeMap<_, _> = url::form_urlencoded::parse(payload.as_bytes()).collect();
        assert!(params["timestamp"].parse::<i64>().unwrap() > 0);
        assert_eq!(params["recvWindow"], "5000");
    }
    async fn account_server(
        mode: Value,
        status: StatusCode,
        code: i64,
        apply_change: bool,
    ) -> (Binance, Arc<Account>, tokio::task::JoinHandle<()>) {
        let account = Arc::new(Account {
            mode: Mutex::new(mode),
            mode_queries: AtomicUsize::new(0),
            changes: AtomicUsize::new(0),
            balance_queries: AtomicUsize::new(0),
            status,
            code,
            apply_change,
        });
        async fn read_mode(
            State(account): State<Arc<Account>>,
            headers: HeaderMap,
            RawQuery(query): RawQuery,
        ) -> Json<Value> {
            assert_signed(&headers, &query.unwrap());
            account.mode_queries.fetch_add(1, Ordering::SeqCst);
            Json(serde_json::json!({"dualSidePosition": account.mode.lock().await.clone()}))
        }
        async fn change(
            State(account): State<Arc<Account>>,
            headers: HeaderMap,
            body: String,
        ) -> (StatusCode, Json<Value>) {
            assert_signed(&headers, &body);
            let params: BTreeMap<_, _> = url::form_urlencoded::parse(body.as_bytes()).collect();
            assert_eq!(params["dualSidePosition"], "false");
            assert!(!params.contains_key("symbol"));
            account.changes.fetch_add(1, Ordering::SeqCst);
            if account.apply_change {
                *account.mode.lock().await = Value::Bool(false);
            }
            (
                account.status,
                Json(serde_json::json!({"code": account.code})),
            )
        }
        async fn balance(
            State(account): State<Arc<Account>>,
            headers: HeaderMap,
            RawQuery(query): RawQuery,
        ) -> Json<Value> {
            assert_signed(&headers, &query.unwrap());
            account.balance_queries.fetch_add(1, Ordering::SeqCst);
            Json(serde_json::json!([{"asset": "USDT", "availableBalance": "5000"}]))
        }
        let app = Router::new()
            .route(
                "/fapi/v1/time",
                get(|| async { Json(serde_json::json!({"serverTime": now_ms()})) }),
            )
            .route("/fapi/v1/positionSide/dual", get(read_mode).post(change))
            .route("/fapi/v3/balance", get(balance))
            .with_state(account.clone());
        let (url, task) = crate::test_support::mock_server(app).await;
        (Binance::mock(url), account, task)
    }
    #[test]
    fn hmac_known_vector() {
        assert_eq!(
            signature("key", "The quick brown fox jumps over the lazy dog"),
            "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8"
        );
    }
    #[tokio::test]
    async fn preflight_leaves_existing_one_way_mode_unchanged() {
        let (client, account, server) =
            account_server(Value::Bool(false), StatusCode::OK, 200, true).await;
        assert_eq!(client.preflight().await.unwrap(), Decimal::from(5000));
        assert_eq!(account.mode_queries.load(Ordering::SeqCst), 1);
        assert_eq!(account.changes.load(Ordering::SeqCst), 0);
        server.abort();
    }
    #[tokio::test]
    async fn preflight_switches_hedge_mode_and_verifies_before_balance() {
        let (client, account, server) =
            account_server(Value::Bool(true), StatusCode::OK, 200, true).await;
        assert_eq!(client.preflight().await.unwrap(), Decimal::from(5000));
        assert_eq!(account.mode_queries.load(Ordering::SeqCst), 2);
        assert_eq!(account.changes.load(Ordering::SeqCst), 1);
        assert_eq!(*account.mode.lock().await, Value::Bool(false));
        server.abort();
    }
    #[tokio::test]
    async fn preflight_rejects_invalid_mode_without_changing_account() {
        let (client, account, server) =
            account_server(Value::String("false".into()), StatusCode::OK, 200, true).await;
        let error = client.preflight().await.unwrap_err();
        assert!(error.to_string().contains("dualSidePosition"));
        assert_eq!(account.changes.load(Ordering::SeqCst), 0);
        assert_eq!(account.balance_queries.load(Ordering::SeqCst), 0);
        server.abort();
    }
    #[tokio::test]
    async fn preflight_reports_orders_positions_and_permission_failures() {
        for (code, reason) in [(-4067, "挂单"), (-4068, "持仓"), (-2015, "API 权限")] {
            let (client, account, server) =
                account_server(Value::Bool(true), StatusCode::BAD_REQUEST, code, false).await;
            let error = client.preflight().await.unwrap_err();
            assert!(error.to_string().contains(reason));
            assert_eq!(error.downcast_ref::<ExchangeError>().unwrap().code, code);
            assert_eq!(account.mode_queries.load(Ordering::SeqCst), 1);
            assert_eq!(account.changes.load(Ordering::SeqCst), 1);
            assert_eq!(account.balance_queries.load(Ordering::SeqCst), 0);
            server.abort();
        }
    }
    #[tokio::test]
    async fn preflight_requires_confirmed_one_way_mode_after_post() {
        let (client, account, server) =
            account_server(Value::Bool(true), StatusCode::OK, 200, false).await;
        assert!(
            client
                .preflight()
                .await
                .unwrap_err()
                .to_string()
                .contains("仍为双向持仓")
        );
        assert_eq!(account.mode_queries.load(Ordering::SeqCst), 2);
        assert_eq!(account.changes.load(Ordering::SeqCst), 1);
        assert_eq!(account.balance_queries.load(Ordering::SeqCst), 0);
        server.abort();
    }
    #[tokio::test]
    async fn preflight_confirms_racing_or_uncertain_changes_without_resubmitting() {
        for (status, code) in [
            (StatusCode::BAD_REQUEST, -4059),
            (StatusCode::SERVICE_UNAVAILABLE, -1007),
        ] {
            for changed in [false, true] {
                let (client, account, server) =
                    account_server(Value::Bool(true), status, code, changed).await;
                assert_eq!(client.preflight().await.is_ok(), changed);
                assert_eq!(account.mode_queries.load(Ordering::SeqCst), 2);
                assert_eq!(account.changes.load(Ordering::SeqCst), 1);
                assert_eq!(
                    account.balance_queries.load(Ordering::SeqCst),
                    usize::from(changed)
                );
                server.abort();
            }
        }
    }
    #[tokio::test]
    async fn concurrent_preflights_only_change_mode_once() {
        let (client, account, server) =
            account_server(Value::Bool(true), StatusCode::OK, 200, true).await;
        let (first, second) = tokio::join!(client.preflight(), client.preflight());
        assert!(first.is_ok());
        assert!(second.is_ok());
        assert_eq!(account.changes.load(Ordering::SeqCst), 1);
        server.abort();
    }
    #[tokio::test]
    async fn application_connection_check_automatically_sets_one_way_mode() {
        let (client, account, server) =
            account_server(Value::Bool(true), StatusCode::OK, 200, true).await;
        let engine = crate::engine::Engine::new(
            Mode::Testnet,
            crate::store::Store::open(":memory:", Mode::Testnet).unwrap(),
            crate::config::Config::default(),
            client,
            crate::jev::Jev::new().unwrap(),
        )
        .unwrap();
        let app = crate::application::Application::from_engine(engine);
        let message = app.check_connection().await.unwrap();
        assert!(message.contains("自动设置为单向持仓"));
        assert_eq!(account.changes.load(Ordering::SeqCst), 1);
        assert!(app.engine.read().await.snapshot().await.paused);
        server.abort();
    }
}
