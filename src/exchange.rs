pub use crate::rate_limit::RateLimitSnapshot;
use crate::{
    config::Mode,
    domain::*,
    rate_limit::{AdmissionError, RequestThrottle, request_cost, retry_after, shared_throttle},
    settings::Credentials,
};
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
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct ExchangeError {
    pub code: i64,
    pub status: u16,
    pub uncertain: bool,
    path: &'static str,
    kind: ExchangeErrorKind,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExchangeErrorKind {
    RequestTimeout,
    Dns,
    Tls,
    ConnectionRefused,
    ConnectionInterrupted,
    Network,
    ResponseTimeout,
    ResponseRead,
    EmptyResponse,
    IncompleteJson,
    InvalidJson,
    Api,
    Cooldown(Duration),
    Budget(Duration),
}
impl ExchangeErrorKind {
    fn retryable(self) -> bool {
        matches!(
            self,
            Self::RequestTimeout
                | Self::Dns
                | Self::ConnectionRefused
                | Self::ConnectionInterrupted
                | Self::Network
                | Self::ResponseTimeout
                | Self::ResponseRead
                | Self::EmptyResponse
                | Self::IncompleteJson
        )
    }
    fn connection_cause(error: &(dyn std::error::Error + 'static)) -> Self {
        // The outer reqwest error includes the request URL; classify causes only.
        let mut source = error.source();
        let mut dns = false;
        let mut tls = false;
        let mut refused = false;
        let mut interrupted = false;
        while let Some(cause) = source {
            // Inspect causes only to select a fixed message. Never retain or expose
            // upstream text: URLs can include signatures and proxy credentials.
            let message = cause.to_string().to_ascii_lowercase();
            dns |= message.contains("dns") || message.contains("failed to lookup address");
            tls |= message.contains("certificate")
                || message.contains("tls")
                || message.contains("ssl");
            interrupted |= message.contains("connection closed")
                || message.contains("connection reset")
                || message.contains("unexpected end of file");
            if let Some(io) = cause.downcast_ref::<std::io::Error>() {
                refused |= io.kind() == std::io::ErrorKind::ConnectionRefused;
                interrupted |= matches!(
                    io.kind(),
                    std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::ConnectionAborted
                        | std::io::ErrorKind::BrokenPipe
                        | std::io::ErrorKind::UnexpectedEof
                );
            }
            source = cause.source();
        }
        if dns {
            Self::Dns
        } else if tls {
            Self::Tls
        } else if refused {
            Self::ConnectionRefused
        } else if interrupted {
            Self::ConnectionInterrupted
        } else {
            Self::Network
        }
    }
}
impl std::fmt::Display for ExchangeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let reason = match self.kind {
            ExchangeErrorKind::RequestTimeout => "请求超时，请检查网络或代理",
            ExchangeErrorKind::Dns => "域名解析失败，请检查服务器或容器 DNS、代理设置",
            ExchangeErrorKind::Tls => "TLS 连接失败，请检查系统时间、证书或代理设置",
            ExchangeErrorKind::ConnectionRefused => "连接被拒绝，请检查目标服务、代理地址和端口",
            ExchangeErrorKind::ConnectionInterrupted => "连接中断，请检查网络或代理后重试",
            ExchangeErrorKind::Network => "网络连接失败，请检查网络或代理",
            ExchangeErrorKind::ResponseTimeout => "响应读取超时，请检查网络或代理",
            ExchangeErrorKind::ResponseRead => "响应读取中断，请检查网络或代理",
            ExchangeErrorKind::EmptyResponse => "返回空响应，未收到有效 JSON",
            ExchangeErrorKind::IncompleteJson => "返回不完整 JSON，数据可能被截断",
            ExchangeErrorKind::InvalidJson => "返回无效 JSON，请检查网络或代理是否返回了其他内容",
            ExchangeErrorKind::Cooldown(_) => "限流退避中，暂缓发送请求",
            ExchangeErrorKind::Budget(_) => "请求权重预算不足，暂缓发送请求",
            ExchangeErrorKind::Api => match self.code {
                -1021 => "请求时间校验失败，请检查系统时间或网络延迟",
                -1003 | -1015 => "请求频率受限，请稍后重试",
                _ => match self.status {
                    401 | 403 => "访问被拒绝，请检查权限、IP 白名单或网络限制",
                    418 | 429 => "请求频率受限，请稍后重试",
                    451 => "当前地区访问受限",
                    500..=599 => "服务暂时不可用",
                    _ => "接口请求失败",
                },
            },
        };
        write!(f, "币安 {} {reason}", self.path)?;
        if let ExchangeErrorKind::Budget(remaining) = self.kind {
            return write!(
                f,
                "（约 {} 秒后可重试；本次未发送）",
                remaining.as_millis().div_ceil(1000)
            );
        }
        if let ExchangeErrorKind::Cooldown(remaining) = self.kind {
            return write!(
                f,
                "（约 {} 秒后可重试；上次 HTTP {}，code={}；本次未发送）",
                remaining.as_millis().div_ceil(1000),
                self.status,
                self.code
            );
        }
        if self.status == 0 {
            write!(f, "（未收到 HTTP 响应）")?;
        } else {
            write!(f, "（HTTP {}，code={}）", self.status, self.code)?;
        }
        if self.uncertain {
            write!(f, "；请求结果尚未确认，需查询状态")?;
        }
        Ok(())
    }
}
impl std::error::Error for ExchangeError {}
impl ExchangeError {
    fn transport(error: reqwest::Error, method: &Method, path: &'static str, status: u16) -> Self {
        let kind = match (status == 0, error.is_timeout()) {
            (true, true) => ExchangeErrorKind::RequestTimeout,
            (true, false) => ExchangeErrorKind::connection_cause(&error),
            (false, true) => ExchangeErrorKind::ResponseTimeout,
            (false, false) => ExchangeErrorKind::ResponseRead,
        };
        // Never retain the reqwest error: its URL can contain a signed query.
        Self {
            code: 0,
            status,
            uncertain: method == Method::POST || method == Method::DELETE,
            path,
            kind,
        }
    }
}
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
    throttle: Arc<Mutex<RequestThrottle>>,
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
pub struct MarginBalances {
    pub multi_assets_margin: bool,
    pub available_balance: Decimal,
    pub assets: Vec<AssetBalance>,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all(deserialize = "camelCase"))]
pub struct AssetBalance {
    pub asset: String,
    pub available_balance: Decimal,
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
        // Isolate mock servers from process-wide cooldowns when test ports are reused.
        let throttle = Arc::new(Mutex::new(RequestThrottle::new(Instant::now())));
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
            throttle,
            position_mode_lock: Mutex::new(()),
        }
    }
    pub fn new(mode: Mode) -> Result<Self> {
        Self::with_credentials(mode, &Credentials::default())
    }
    pub fn with_credentials(mode: Mode, credentials: &Credentials) -> Result<Self> {
        let base: String = if mode == Mode::Testnet {
            "https://demo-fapi.binance.com"
        } else {
            "https://fapi.binance.com"
        }
        .into();
        let key =
            (mode != Mode::Paper && !credentials.key.is_empty()).then(|| credentials.key.clone());
        let secret = (mode != Mode::Paper && !credentials.secret.is_empty())
            .then(|| credentials.secret.clone());
        let throttle = shared_throttle(&base);
        Ok(Self {
            client: Client::builder()
                .timeout(Duration::from_secs(8))
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            base,
            key,
            secret,
            offset: AtomicI64::new(0),
            throttle,
            position_mode_lock: Mutex::new(()),
        })
    }
    pub fn configured(&self) -> bool {
        self.key.is_some() && self.secret.is_some()
    }
    pub async fn rate_limit_snapshot(&self) -> RateLimitSnapshot {
        self.throttle.lock().await.snapshot(Instant::now())
    }
    async fn request_slot(
        &self,
        method: &Method,
        path: &'static str,
        params: &[(String, String)],
    ) -> Result<Instant> {
        // Keep FIFO admission while allowing responses to update cooldown state
        // immediately. This queue lock is released before sending the HTTP request.
        let dispatch = self.throttle.lock().await.dispatch.clone();
        let _queued = dispatch.lock().await;
        let cost = request_cost(method, path, params);
        loop {
            let mut gate = self.throttle.lock().await;
            let now = Instant::now();
            let slot = gate.reserve(now, cost);
            drop(gate);
            match slot {
                Ok(None) => return Ok(now),
                Ok(Some(wait)) => tokio::time::sleep(wait).await,
                Err(AdmissionError::Cooldown(cooldown)) => {
                    return Err(ExchangeError {
                        status: cooldown.status,
                        code: cooldown.code,
                        uncertain: false,
                        path,
                        kind: ExchangeErrorKind::Cooldown(cooldown.remaining),
                    }
                    .into());
                }
                Err(AdmissionError::Budget(remaining)) => {
                    return Err(ExchangeError {
                        status: 0,
                        code: 0,
                        uncertain: false,
                        path,
                        kind: ExchangeErrorKind::Budget(remaining),
                    }
                    .into());
                }
            }
        }
    }
    async fn record_limit(
        &self,
        started: Instant,
        path: &'static str,
        status: u16,
        code: i64,
        retry: Option<Duration>,
    ) {
        let delay =
            self.throttle
                .lock()
                .await
                .penalize(started, Instant::now(), status, code, retry);
        tracing::warn!(
            path,
            status,
            code,
            retry_after_ms = delay.as_millis() as u64,
            "币安限流，暂缓该域名的后续请求"
        );
    }
    async fn request(
        &self,
        method: Method,
        path: &'static str,
        params: Vec<(String, String)>,
        signed: bool,
    ) -> Result<Value> {
        // All current GET endpoints are read-only, including signed account/order
        // queries. Retrying them cannot submit or cancel an order. request_once
        // generates a fresh timestamp and signature after the retry delay.
        // Clock synchronization measures one request's round trip; including a
        // failed attempt would skew its offset and expire subsequent signatures.
        let retry_get = method == Method::GET && path != "/fapi/v1/time";
        let mut result = self
            .request_once(method.clone(), path, params.clone(), signed)
            .await;
        if retry_get
            && result.as_ref().is_err_and(|error| {
                error
                    .downcast_ref::<ExchangeError>()
                    .is_some_and(|e| e.kind.retryable())
            })
        {
            tokio::time::sleep(Duration::from_millis(250)).await;
            result = self.request_once(method, path, params, signed).await;
        }
        result
    }
    async fn request_once(
        &self,
        method: Method,
        path: &'static str,
        params: Vec<(String, String)>,
        signed: bool,
    ) -> Result<Value> {
        let started = self.request_slot(&method, path, &params).await?;
        self.request_admitted(method, path, params, signed, started)
            .await
    }
    async fn request_admitted(
        &self,
        method: Method,
        path: &'static str,
        mut params: Vec<(String, String)>,
        signed: bool,
        started: Instant,
    ) -> Result<Value> {
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
        let response = req
            .send()
            .await
            .map_err(|error| ExchangeError::transport(error, &method, path, 0))?;
        let status = response.status();
        let retry = retry_after(
            response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok()),
            SystemTime::now(),
        );
        let limited = matches!(status.as_u16(), 418 | 429);
        if limited {
            // Apply the gate as soon as headers arrive, even if the body stalls or is HTML.
            self.record_limit(started, path, status.as_u16(), 0, retry)
                .await;
        }
        // Binance explicitly documents bookTicker's weight header as inaccurate.
        // Observe other headers before reading the body, including error responses.
        if path != "/fapi/v1/ticker/bookTicker"
            && let Some(used) = response
                .headers()
                .get("x-mbx-used-weight-1m")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok())
        {
            self.throttle
                .lock()
                .await
                .observe_weight(started, Instant::now(), used);
        }
        let body = response
            .bytes()
            .await
            .map_err(|error| ExchangeError::transport(error, &method, path, status.as_u16()))?;
        let value = serde_json::from_slice::<Value>(&body);
        let code = value
            .as_ref()
            .ok()
            .and_then(|value| value.get("code"))
            .and_then(Value::as_i64)
            .unwrap_or(0);
        if !limited && matches!(code, -1003 | -1015) {
            self.record_limit(started, path, status.as_u16(), code, retry)
                .await;
        }
        // Preserve HTTP failures even when a gateway returns HTML instead of JSON.
        // A Binance error code must also be rejected if it arrives with HTTP 200.
        if !status.is_success() || code < 0 {
            return Err(ExchangeError {
                code,
                status: status.as_u16(),
                uncertain: (status.is_server_error() || matches!(code, -1006 | -1007))
                    && (method == Method::POST || method == Method::DELETE),
                path,
                kind: ExchangeErrorKind::Api,
            }
            .into());
        }
        let value = value.map_err(|error| {
            anyhow::Error::from(ExchangeError {
                code: 0,
                status: status.as_u16(),
                uncertain: method == Method::POST || method == Method::DELETE,
                path,
                kind: if body.iter().all(u8::is_ascii_whitespace) {
                    ExchangeErrorKind::EmptyResponse
                } else if error.is_eof() {
                    ExchangeErrorKind::IncompleteJson
                } else {
                    ExchangeErrorKind::InvalidJson
                },
            })
        })?;
        if method == Method::GET && path == "/fapi/v1/exchangeInfo" {
            self.throttle.lock().await.configure_weight_limit(&value);
        }
        Ok(value)
    }
    pub async fn sync_time(&self) -> Result<()> {
        let admitted = self
            .request_slot(&Method::GET, "/fapi/v1/time", &[])
            .await?;
        // Queueing is local delay, not network round-trip time. Including it would
        // move the clock offset forward and skew every later signed request.
        let start = now_ms();
        let v = self
            .request_admitted(Method::GET, "/fapi/v1/time", vec![], false, admitted)
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
                || !matches!(s["quoteAsset"].as_str(), Some("USDT" | "USDC"))
                || s["marginAsset"] != s["quoteAsset"]
                || s["underlyingType"] != "COIN"
            {
                continue;
            }
            let symbol = s["symbol"].as_str().context("missing symbol")?.to_string();
            ensure!(
                settlement_asset(&symbol) == s["quoteAsset"].as_str(),
                "symbol does not match settlement asset"
            );
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
        let received_at = now_ms();
        for q in v.as_array().context("invalid quotes")? {
            let exchange_time = q["time"]
                .as_i64()
                .context("missing quote transaction time")?;
            ensure!(exchange_time > 0, "invalid quote transaction time");
            let quote = Quote {
                bid: decimal(&q["bidPrice"])?,
                ask: decimal(&q["askPrice"])?,
                bid_qty: decimal(&q["bidQty"])?,
                ask_qty: decimal(&q["askQty"])?,
                time: received_at,
                exchange_time: Some(exchange_time),
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
            if exchange.kind != ExchangeErrorKind::Api {
                return anyhow::anyhow!("无法连接币安或未收到有效账户响应：{exchange}");
            }
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
        let balances = v.as_array().context("invalid balance")?;
        let balance = ["USDT", "USDC"]
            .into_iter()
            .find_map(|asset| balances.iter().find(|x| x["asset"] == asset))
            .context("missing USDT/USDC balance")?;
        decimal(&balance["availableBalance"])
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
    pub async fn margin_balances(&self) -> Result<MarginBalances> {
        // Single-asset balances are independent; multi-assets uses the shared USD total.
        Ok(serde_json::from_value(
            self.request(Method::GET, "/fapi/v2/account", vec![], true)
                .await?,
        )?)
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
    #[tokio::test]
    async fn budget_is_shared_across_client_replacement_and_reserves_account_and_exit_capacity() {
        use axum::{Json, Router, http::Uri};
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count = calls.clone();
        let app = Router::new().fallback(move |uri: Uri, method: axum::http::Method, body: String| {
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                if uri.path() == "/fapi/v1/exchangeInfo" {
                    Json(serde_json::json!({"rateLimits":[{"rateLimitType":"REQUEST_WEIGHT","interval":"MINUTE","intervalNum":1,"limit":100}]}))
                } else {
                    if method == Method::POST {
                        assert!(body.contains("reduceOnly=true"));
                    }
                    Json(serde_json::json!([]))
                }
            }
        });
        let (base, server) = crate::test_support::mock_server(app).await;
        let mut first = Binance::mock(base.clone());
        first.throttle = shared_throttle(&base);
        first
            .request(Method::GET, "/fapi/v1/exchangeInfo", vec![], false)
            .await
            .unwrap();
        for _ in 0..4 {
            first
                .request(Method::GET, "/fapi/v1/commissionRate", vec![], true)
                .await
                .unwrap();
        }
        assert_eq!(first.rate_limit_snapshot().await.estimated_used_weight, 81);
        drop(first);
        let mut replacement = Binance::mock(base.clone());
        replacement.throttle = shared_throttle(&base);
        let error = tokio::time::timeout(
            Duration::from_millis(500),
            replacement.request(Method::GET, "/fapi/v1/commissionRate", vec![], true),
        )
        .await
        .unwrap()
        .unwrap_err();
        let error = error.downcast_ref::<ExchangeError>().unwrap();
        assert!(matches!(error.kind, ExchangeErrorKind::Budget(_)));
        assert!(!error.uncertain);
        assert!(error.to_string().contains("本次未发送"));
        assert_eq!(calls.load(Ordering::SeqCst), 5);
        for _ in 0..2 {
            replacement
                .request(Method::GET, "/fapi/v2/account", vec![], true)
                .await
                .unwrap();
        }
        replacement
            .request(Method::DELETE, "/fapi/v1/order", vec![], true)
            .await
            .unwrap();
        // The ordinary budget is exhausted; an unsent entry is definite, while a
        // zero-IP-weight reduce-only exit can still be dispatched exactly once.
        assert!(matches!(
            replacement
                .request(Method::POST, "/fapi/v1/order", vec![], true)
                .await
                .unwrap_err()
                .downcast_ref::<ExchangeError>()
                .unwrap()
                .kind,
            ExchangeErrorKind::Budget(_)
        ));
        replacement
            .request(
                Method::POST,
                "/fapi/v1/order",
                vec![("reduceOnly".into(), "true".into())],
                true,
            )
            .await
            .unwrap();
        let snapshot = replacement.rate_limit_snapshot().await;
        assert_eq!(snapshot.weight_limit, 100);
        assert_eq!(snapshot.regular_weight_limit, 90);
        assert_eq!(snapshot.estimated_used_weight, 92);
        assert_eq!(snapshot.remaining_weight, 8);
        assert_eq!(snapshot.regular_remaining_weight, 0);
        assert_eq!(snapshot.budget_rejections, 2);
        assert_eq!(snapshot.admitted_requests, 9);
        assert_eq!(calls.load(Ordering::SeqCst), 9);
        server.abort();
    }
    #[tokio::test]
    async fn inaccurate_and_malformed_weight_headers_are_ignored() {
        use axum::{Json, Router, routing::get};
        let app = Router::new()
            .route(
                "/fapi/v1/ticker/bookTicker",
                get(|| async {
                    (
                        [("x-mbx-used-weight-1m", "999999")],
                        Json(serde_json::json!([])),
                    )
                }),
            )
            .route(
                "/fapi/v1/commissionRate",
                get(|| async {
                    (
                        [("x-mbx-used-weight-1m", "invalid")],
                        Json(serde_json::json!({})),
                    )
                }),
            );
        let (base, server) = crate::test_support::mock_server(app).await;
        let client = Binance::mock(base);
        client.quotes().await.unwrap();
        client
            .request(Method::GET, "/fapi/v1/commissionRate", vec![], true)
            .await
            .unwrap();
        let snapshot = client.rate_limit_snapshot().await;
        assert_eq!(snapshot.estimated_used_weight, 25);
        assert_eq!(snapshot.server_reported_used_weight, None);
        assert_eq!(snapshot.budget_rejections, 0);
        server.abort();
    }
    #[tokio::test]
    async fn weight_headers_admit_no_ordinary_requests_while_the_response_body_stalls() {
        use axum::{Router, body::Body, routing::get};
        use futures_util::StreamExt;
        let release = Arc::new(tokio::sync::Notify::new());
        let body_release = release.clone();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count = calls.clone();
        let app = Router::new().route(
            "/fapi/v1/order",
            get(move || {
                let release = body_release.clone();
                let count = count.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    let body = futures_util::stream::once(async {
                        Ok::<_, std::convert::Infallible>("[")
                    })
                    .chain(futures_util::stream::once(async move {
                        release.notified().await;
                        Ok("]")
                    }));
                    ([("x-mbx-used-weight-1m", "2160")], Body::from_stream(body))
                }
            }),
        );
        let (base, server) = crate::test_support::mock_server(app).await;
        let client = Arc::new(Binance::mock(base));
        let first = tokio::spawn({
            let client = client.clone();
            async move {
                client
                    .request(Method::GET, "/fapi/v1/order", vec![], true)
                    .await
            }
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while client
                .rate_limit_snapshot()
                .await
                .server_reported_used_weight
                .is_none()
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let error = tokio::time::timeout(
            Duration::from_millis(500),
            client.request(Method::GET, "/fapi/v1/commissionRate", vec![], true),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert!(matches!(
            error.downcast_ref::<ExchangeError>().unwrap().kind,
            ExchangeErrorKind::Budget(_)
        ));
        assert!(!first.is_finished());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let snapshot = client.rate_limit_snapshot().await;
        assert_eq!(snapshot.remaining_weight, 240);
        assert_eq!(snapshot.cooldown_remaining_ms, 0);
        release.notify_one();
        first.await.unwrap().unwrap();
        server.abort();
    }
    #[tokio::test]
    async fn time_sync_excludes_waiting_in_the_dispatch_queue() {
        use axum::{Json, Router, routing::get};
        let app = Router::new().route(
            "/fapi/v1/time",
            get(|| async { Json(serde_json::json!({"serverTime":now_ms()})) }),
        );
        let (base, server) = crate::test_support::mock_server(app).await;
        let client = Arc::new(Binance::mock(base));
        let dispatch = client.throttle.lock().await.dispatch.clone();
        let held = dispatch.lock().await;
        let sync = tokio::spawn({
            let client = client.clone();
            async move { client.sync_time().await }
        });
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert!(!sync.is_finished());
        drop(held);
        tokio::time::timeout(Duration::from_secs(1), sync)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(client.offset.load(Ordering::Relaxed).abs() < 100);
        server.abort();
    }
    #[tokio::test]
    async fn shared_cooldown_blocks_reads_and_mutations_without_sending_then_recovers() {
        use axum::{Json, Router, extract::Query, http::StatusCode, routing::get};
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count = calls.clone();
        let app = Router::new().route("/fapi/v1/order", get(move |Query(query): Query<BTreeMap<String, String>>| {
            let count = count.clone();
            async move {
                if count.fetch_add(1, Ordering::SeqCst) == 0 {
                    (StatusCode::TOO_MANY_REQUESTS, [("retry-after", "1")], Json(serde_json::json!({"code":-1003,"msg":"local-test-secret"})))
                } else {
                    assert!(query["timestamp"].parse::<i64>().unwrap() >= now_ms() - 200);
                    (StatusCode::OK, [("retry-after", "0")], Json(serde_json::json!({"symbol":"XUSDT","clientOrderId":"test","orderId":1,"status":"NEW","executedQty":"0","avgPrice":"0","origQty":"1","cumQuote":"0"})))
                }
            }
        }));
        let (base, server) = crate::test_support::mock_server(app).await;
        let mut first = Binance::mock(base.clone());
        first.throttle = shared_throttle(&base);
        let error = first.order("XUSDT", "test").await.unwrap_err();
        assert_eq!(error.downcast_ref::<ExchangeError>().unwrap().status, 429);
        assert!(!error.to_string().contains("local-test-secret"));
        drop(first);
        let mut replacement = Binance::mock(base.clone());
        replacement.throttle = shared_throttle(&base);
        for method in [Method::GET, Method::POST, Method::DELETE] {
            let error = replacement
                .request(method, "/fapi/v1/order", vec![], true)
                .await
                .unwrap_err();
            let error = error.downcast_ref::<ExchangeError>().unwrap();
            assert!(matches!(error.kind, ExchangeErrorKind::Cooldown(_)));
            assert!(!error.uncertain);
            assert!(error.to_string().contains("本次未发送"));
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        tokio::time::sleep(Duration::from_millis(1100)).await;
        replacement.order("XUSDT", "test").await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        server.abort();
    }
    #[tokio::test]
    async fn rate_limit_headers_block_queued_requests_before_the_body_completes() {
        use axum::{Router, body::Body, http::StatusCode, routing::get};
        use futures_util::StreamExt;
        let release = Arc::new(tokio::sync::Notify::new());
        let body_release = release.clone();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count = calls.clone();
        let app = Router::new().route(
            "/fapi/v1/order",
            get(move || {
                let release = body_release.clone();
                let count = count.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    let body = futures_util::stream::once(async {
                        Ok::<_, std::convert::Infallible>("{")
                    })
                    .chain(futures_util::stream::once(async move {
                        release.notified().await;
                        Ok("\"code\":-1003}")
                    }));
                    (
                        StatusCode::TOO_MANY_REQUESTS,
                        [("retry-after", "2")],
                        Body::from_stream(body),
                    )
                }
            }),
        );
        let (base, server) = crate::test_support::mock_server(app).await;
        let client = Arc::new(Binance::mock(base));
        let first = tokio::spawn({
            let client = client.clone();
            async move { client.order("XUSDT", "first").await }
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while calls.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let error =
            tokio::time::timeout(Duration::from_millis(500), client.order("XUSDT", "queued"))
                .await
                .unwrap()
                .unwrap_err();
        assert!(matches!(
            error.downcast_ref::<ExchangeError>().unwrap().kind,
            ExchangeErrorKind::Cooldown(_)
        ));
        assert!(!first.is_finished());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        release.notify_one();
        assert_eq!(
            first
                .await
                .unwrap()
                .unwrap_err()
                .downcast_ref::<ExchangeError>()
                .unwrap()
                .status,
            429
        );
        server.abort();
    }
    #[tokio::test]
    async fn bans_and_rate_limit_codes_apply_a_cooldown_without_retries() {
        use axum::{Json, Router, http::StatusCode};
        for (status, code) in [(418, -1003), (200, -1003), (400, -1015)] {
            let app = Router::new().fallback(move || async move {
                (
                    StatusCode::from_u16(status).unwrap(),
                    Json(serde_json::json!({"code":code})),
                )
            });
            let (base, server) = crate::test_support::mock_server(app).await;
            let client = Binance::mock(base);
            let original = client
                .request(Method::GET, "/fapi/v1/order", vec![], true)
                .await
                .unwrap_err();
            assert_eq!(original.downcast_ref::<ExchangeError>().unwrap().code, code);
            let blocked = client
                .request(Method::POST, "/fapi/v1/order", vec![], true)
                .await
                .unwrap_err();
            let error = blocked.downcast_ref::<ExchangeError>().unwrap();
            let ExchangeErrorKind::Cooldown(wait) = error.kind else {
                panic!("expected cooldown");
            };
            assert!(wait > Duration::from_secs(if status == 418 { 119 } else { 59 }));
            assert!(!error.uncertain);
            server.abort();
        }
    }
    #[tokio::test]
    async fn quote_keeps_exchange_transaction_time_separate_from_receipt_time() {
        use axum::{Json, Router, routing::get};
        let app = Router::new().route("/fapi/v1/ticker/bookTicker", get(|| async {
            Json(serde_json::json!([{"symbol":"XUSDT","bidPrice":"100","askPrice":"101","bidQty":"2","askQty":"3","time":12345}]))
        }));
        let (base, server) = crate::test_support::mock_server(app).await;
        let exchange = Binance::mock(base);
        let started = now_ms();
        let first = exchange.quotes().await.unwrap();
        let second = exchange.quotes().await.unwrap();
        for quotes in [&first, &second] {
            assert_eq!(quotes["XUSDT"].exchange_time, Some(12345));
            assert!((started..=now_ms()).contains(&quotes["XUSDT"].time));
        }
        server.abort();
    }
    #[tokio::test]
    async fn catalog_includes_usdc_perpetuals_with_their_exchange_rules() {
        use axum::{Json, Router, routing::get};
        let symbol = |name: &str, asset: &str| {
            serde_json::json!({
                "symbol": name, "status": "TRADING", "contractType": "PERPETUAL",
                "quoteAsset": asset, "marginAsset": asset, "underlyingType": "COIN",
                "filters": [
                    {"filterType":"PRICE_FILTER", "tickSize":"0.01"},
                    {"filterType":"LOT_SIZE", "stepSize":"0.001", "minQty":"0.001", "maxQty":"1000"},
                    {"filterType":"MARKET_LOT_SIZE", "stepSize":"0.002", "minQty":"0.002", "maxQty":"500"},
                    {"filterType":"MIN_NOTIONAL", "notional":"5"}
                ]
            })
        };
        let mut inactive = symbol("INACTIVEUSDC", "USDC");
        inactive["status"] = "BREAK".into();
        let mut delivery = symbol("DELIVERYUSDC", "USDC");
        delivery["contractType"] = "CURRENT_QUARTER".into();
        let mut mismatch = symbol("MISMATCHUSDC", "USDC");
        mismatch["marginAsset"] = "USDT".into();
        let mut index = symbol("INDEXUSDC", "USDC");
        index["underlyingType"] = "INDEX".into();
        let info = serde_json::json!({"symbols":[
            symbol("BTCUSDT", "USDT"), symbol("BTCUSDC", "USDC"),
            symbol("BTCBUSD", "BUSD"), inactive, delivery, mismatch, index
        ]});
        let app = Router::new()
            .route(
                "/fapi/v1/exchangeInfo",
                get(move || {
                    let info = info.clone();
                    async move { Json(info) }
                }),
            )
            .route(
                "/fapi/v1/ticker/24hr",
                get(|| async {
                    Json(serde_json::json!([
                        {"symbol":"BTCUSDT", "quoteVolume":"1000000", "lastPrice":"60000"},
                        {"symbol":"BTCUSDC", "quoteVolume":"2000000", "lastPrice":"60001"}
                    ]))
                }),
            );
        let (url, server) = crate::test_support::mock_server(app).await;
        let catalog = Binance::mock(url).symbols().await.unwrap();
        assert_eq!(
            catalog.keys().map(String::as_str).collect::<Vec<_>>(),
            ["BTCUSDC", "BTCUSDT"]
        );
        let usdc = &catalog["BTCUSDC"];
        assert_eq!(usdc.quote_volume, Decimal::from(2000000));
        assert_eq!(usdc.last_price, Decimal::from(60001));
        assert_eq!(usdc.tick_size, Decimal::from_str("0.01").unwrap());
        assert_eq!(usdc.market_step, Decimal::from_str("0.002").unwrap());
        assert_eq!(usdc.market_max_qty, Decimal::from(500));
        server.abort();
    }
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

    #[test]
    fn connection_causes_are_classified_without_exposing_signed_urls_or_upstream_text() {
        #[derive(Debug)]
        struct TransportCause(std::io::Error);
        impl std::fmt::Display for TransportCause {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                // The outer URL must not make an unrelated failure look like DNS/TLS.
                write!(
                    f,
                    "https://example.invalid/?symbol=DNSTLS&signature=private-signature"
                )
            }
        }
        impl std::error::Error for TransportCause {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                Some(&self.0)
            }
        }
        for (io_kind, cause, kind, reason) in [
            (
                std::io::ErrorKind::Other,
                "dns error: local-test-secret",
                ExchangeErrorKind::Dns,
                "域名解析失败",
            ),
            (
                std::io::ErrorKind::InvalidData,
                "invalid peer certificate: local-test-secret",
                ExchangeErrorKind::Tls,
                "TLS 连接失败",
            ),
            (
                std::io::ErrorKind::ConnectionRefused,
                "local-test-secret",
                ExchangeErrorKind::ConnectionRefused,
                "连接被拒绝",
            ),
            (
                std::io::ErrorKind::ConnectionReset,
                "local-test-secret",
                ExchangeErrorKind::ConnectionInterrupted,
                "连接中断",
            ),
            (
                std::io::ErrorKind::Other,
                "connection closed before message completed: local-test-secret",
                ExchangeErrorKind::ConnectionInterrupted,
                "连接中断",
            ),
            (
                std::io::ErrorKind::Other,
                "local-test-secret",
                ExchangeErrorKind::Network,
                "网络连接失败",
            ),
        ] {
            let kind_found = ExchangeErrorKind::connection_cause(&TransportCause(
                std::io::Error::new(io_kind, cause),
            ));
            assert_eq!(kind_found, kind);
            let error = ExchangeError {
                code: 0,
                status: 0,
                uncertain: false,
                path: "/fapi/v3/positionRisk",
                kind: kind_found,
            };
            assert!(error.to_string().contains(reason));
            assert!(error.to_string().contains("未收到 HTTP 响应"));
            let output = format!("{error} {error:?}");
            assert!(!output.contains("local-test-secret") && !output.contains("private-signature"));
        }
    }

    #[tokio::test]
    async fn refused_connections_report_the_cause_without_retaining_credentials() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);
        let error = Binance::mock(base).positions().await.unwrap_err();
        let exchange = error.downcast_ref::<ExchangeError>().unwrap();
        assert_eq!(exchange.kind, ExchangeErrorKind::ConnectionRefused);
        assert!(!exchange.uncertain);
        let output = format!("{error:#} {error:?}");
        assert!(output.contains("连接被拒绝"));
        assert!(!output.contains("signature=") && !output.contains("local-test-key"));
        assert!(!output.contains("local-test-secret"));
    }

    #[tokio::test]
    async fn signed_positions_recover_from_disconnect_with_a_fresh_signature() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = Binance::mock(format!("http://{}", listener.local_addr().unwrap()));
        let server = tokio::spawn(async move {
            let mut queries = Vec::new();
            for attempt in 0..2 {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                let mut buffer = [0; 1024];
                while !bytes.windows(4).any(|window| window == b"\r\n\r\n") {
                    let read = socket.read(&mut buffer).await.unwrap();
                    assert!(read > 0);
                    bytes.extend_from_slice(&buffer[..read]);
                    assert!(bytes.len() < 8192);
                }
                let request = String::from_utf8(bytes).unwrap();
                let target = request
                    .lines()
                    .next()
                    .unwrap()
                    .strip_prefix("GET ")
                    .unwrap();
                let target = target.strip_suffix(" HTTP/1.1").unwrap();
                let query = target.strip_prefix("/fapi/v3/positionRisk?").unwrap();
                let mut headers = HeaderMap::new();
                for header in request.lines().skip(1).filter(|line| !line.is_empty()) {
                    let (name, value) = header.split_once(':').unwrap();
                    headers.insert(
                        axum::http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                        value.trim().parse().unwrap(),
                    );
                }
                assert_signed(&headers, query);
                queries.push(query.to_owned());
                if attempt == 0 {
                    // Drop the connection before any HTTP response reaches the client.
                    continue;
                }
                let body = r#"[{"symbol":"BTCUSDT","positionAmt":"2","entryPrice":"100","positionSide":"BOTH"}]"#;
                socket
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        )
                        .as_bytes(),
                    )
                    .await
                    .unwrap();
            }
            queries
        });
        let positions = client.positions().await.unwrap();
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].symbol, "BTCUSDT");
        assert_eq!(positions[0].position_amt, Decimal::from(2));
        let queries = server.await.unwrap();
        let params: Vec<BTreeMap<_, _>> = queries
            .iter()
            .map(|query| url::form_urlencoded::parse(query.as_bytes()).collect())
            .collect();
        assert!(
            params[1]["timestamp"].parse::<i64>().unwrap()
                > params[0]["timestamp"].parse::<i64>().unwrap()
        );
        assert_ne!(params[0]["signature"], params[1]["signature"]);
    }

    #[tokio::test]
    async fn signed_positions_do_not_retry_api_rejections() {
        for (status, code) in [(401, -2015), (429, -1003), (503, -1000), (200, -1021)] {
            let calls = Arc::new(AtomicUsize::new(0));
            let observed = calls.clone();
            let app = Router::new().route(
                "/fapi/v3/positionRisk",
                get(move |headers: HeaderMap, RawQuery(query): RawQuery| {
                    let observed = observed.clone();
                    async move {
                        assert_signed(&headers, &query.unwrap());
                        observed.fetch_add(1, Ordering::SeqCst);
                        (
                            StatusCode::from_u16(status).unwrap(),
                            Json(serde_json::json!({"code":code})),
                        )
                    }
                }),
            );
            let (base, server) = crate::test_support::mock_server(app).await;
            let error = Binance::mock(base).positions().await.unwrap_err();
            assert_eq!(error.downcast_ref::<ExchangeError>().unwrap().code, code);
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            server.abort();
        }
    }

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
    #[tokio::test]
    async fn response_failures_report_the_stage_and_only_retry_transient_gets() {
        use axum::{body::Body, response::IntoResponse};
        for (status, body, kind, attempts, reason) in [
            (200, "", ExchangeErrorKind::EmptyResponse, 2, "空响应"),
            (
                200,
                "{\"bidPrice\":",
                ExchangeErrorKind::IncompleteJson,
                2,
                "不完整 JSON",
            ),
            (
                200,
                "<html>local-test-key local-test-secret</html>",
                ExchangeErrorKind::InvalidJson,
                1,
                "无效 JSON",
            ),
            (
                200,
                "{\"code\":-1021,\"msg\":\"local-test-secret\"}",
                ExchangeErrorKind::Api,
                1,
                "时间校验失败",
            ),
            (
                403,
                "<html>local-test-secret</html>",
                ExchangeErrorKind::Api,
                1,
                "访问被拒绝",
            ),
            (
                429,
                "<html>local-test-secret</html>",
                ExchangeErrorKind::Api,
                1,
                "频率受限",
            ),
            (
                503,
                "<html>local-test-secret</html>",
                ExchangeErrorKind::Api,
                1,
                "服务暂时不可用",
            ),
        ] {
            let calls = Arc::new(AtomicUsize::new(0));
            let observed = calls.clone();
            let app = Router::new().fallback(move || {
                let observed = observed.clone();
                async move {
                    observed.fetch_add(1, Ordering::SeqCst);
                    (StatusCode::from_u16(status).unwrap(), Body::from(body)).into_response()
                }
            });
            let (base, server) = crate::test_support::mock_server(app).await;
            let client = Binance::mock(base);
            let error = client.quotes().await.unwrap_err();
            let exchange = error.downcast_ref::<ExchangeError>().unwrap();
            assert_eq!(exchange.kind, kind);
            assert_eq!(exchange.status, status);
            assert!(!exchange.uncertain);
            let message = format!("{error:#}");
            assert!(message.contains("/fapi/v1/ticker/bookTicker"));
            assert!(message.contains(reason), "{message}");
            assert!(!message.contains("local-test-key") && !message.contains("local-test-secret"));
            assert_eq!(calls.load(Ordering::SeqCst), attempts);
            server.abort();
        }
    }
    #[tokio::test]
    async fn public_quotes_recover_once_and_keep_the_received_market_values() {
        use axum::body::Body;
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let app = Router::new().route(
            "/fapi/v1/ticker/bookTicker",
            get(move |headers: HeaderMap, RawQuery(query): RawQuery| {
                let observed = observed.clone();
                async move {
                    assert!(!headers.contains_key("X-MBX-APIKEY"));
                    assert!(query.is_none());
                    let first = observed.fetch_add(1, Ordering::SeqCst) == 0;
                    Body::from(if first {
                        "[{\"symbol\":"
                    } else {
                        "[{\"symbol\":\"BTCUSDT\",\"bidPrice\":\"100\",\"askPrice\":\"101\",\"bidQty\":\"2\",\"askQty\":\"3\",\"time\":12345}]"
                    })
                }
            }),
        );
        let (base, server) = crate::test_support::mock_server(app).await;
        let client = Binance::mock(base);
        let quotes = client.quotes().await.unwrap();
        assert_eq!(quotes.len(), 1);
        assert_eq!(quotes["BTCUSDT"].bid, Decimal::from(100));
        assert_eq!(quotes["BTCUSDT"].ask, Decimal::from(101));
        assert_eq!(quotes["BTCUSDT"].bid_qty, Decimal::from(2));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        server.abort();
    }
    #[tokio::test]
    async fn header_and_body_timeouts_retry_reads_but_preserve_mutation_and_clock_semantics() {
        use axum::{body::Body, response::IntoResponse};
        use std::convert::Infallible;
        for body_timeout in [false, true] {
            for (method, signed, path, attempts, uncertain) in [
                (Method::GET, false, "/fapi/v1/ticker/bookTicker", 2, false),
                (Method::GET, false, "/fapi/v1/time", 1, false),
                (Method::GET, true, "/fapi/v1/order", 2, false),
                (Method::GET, true, "/fapi/v3/positionRisk", 2, false),
                (Method::POST, true, "/fapi/v1/order", 1, true),
                (Method::DELETE, true, "/fapi/v1/order", 1, true),
            ] {
                let calls = Arc::new(AtomicUsize::new(0));
                let observed = calls.clone();
                let app = Router::new().fallback(move || {
                    let observed = observed.clone();
                    async move {
                        observed.fetch_add(1, Ordering::SeqCst);
                        if !body_timeout {
                            tokio::time::sleep(Duration::from_millis(600)).await;
                        }
                        let stream = futures_util::stream::once(async move {
                            if body_timeout {
                                tokio::time::sleep(Duration::from_millis(600)).await;
                            }
                            Ok::<_, Infallible>("{\"secret\":\"local-test-secret\"}")
                        });
                        (
                            [("content-type", "application/json")],
                            Body::from_stream(stream),
                        )
                            .into_response()
                    }
                });
                let (base, server) = crate::test_support::mock_server(app).await;
                let mut client = Binance::mock(base);
                client.client = Client::builder()
                    .no_proxy()
                    .timeout(Duration::from_millis(200))
                    .build()
                    .unwrap();
                let error = client
                    .request(method, path, vec![], signed)
                    .await
                    .unwrap_err();
                let exchange = error.downcast_ref::<ExchangeError>().unwrap();
                assert_eq!(exchange.uncertain, uncertain);
                assert_eq!(exchange.status, if body_timeout { 200 } else { 0 });
                assert_eq!(
                    exchange.kind,
                    if body_timeout {
                        ExchangeErrorKind::ResponseTimeout
                    } else {
                        ExchangeErrorKind::RequestTimeout
                    }
                );
                let message = format!("{error:#}");
                assert!(message.contains(if body_timeout {
                    "响应读取超时"
                } else {
                    "请求超时"
                }));
                assert!(
                    !message.contains("signature=")
                        && !message.contains("local-test-secret")
                        && !message.contains("local-test-key")
                );
                assert_eq!(calls.load(Ordering::SeqCst), attempts);
                server.abort();
            }
        }
    }
    #[tokio::test]
    async fn mutation_errors_preserve_definite_rejection_and_unknown_execution_without_retries() {
        use axum::body::Body;
        for method in [Method::POST, Method::DELETE] {
            for (status, body, code, uncertain) in [
                (200, "<html>local-test-secret</html>", 0, true),
                (200, "{\"code\":-1007}", -1007, true),
                (400, "{\"code\":-1021}", -1021, false),
                (403, "<html>local-test-secret</html>", 0, false),
                (503, "<html>local-test-secret</html>", 0, true),
            ] {
                let calls = Arc::new(AtomicUsize::new(0));
                let observed = calls.clone();
                let app = Router::new().fallback(move || {
                    let observed = observed.clone();
                    async move {
                        observed.fetch_add(1, Ordering::SeqCst);
                        (StatusCode::from_u16(status).unwrap(), Body::from(body))
                    }
                });
                let (base, server) = crate::test_support::mock_server(app).await;
                let client = Binance::mock(base);
                let error = client
                    .request(method.clone(), "/fapi/v1/order", vec![], true)
                    .await
                    .unwrap_err();
                let exchange = error.downcast_ref::<ExchangeError>().unwrap();
                assert_eq!(exchange.code, code);
                assert_eq!(exchange.status, status);
                assert_eq!(exchange.uncertain, uncertain);
                assert!(!format!("{error:#}").contains("local-test-secret"));
                assert_eq!(calls.load(Ordering::SeqCst), 1);
                server.abort();
            }
        }
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
        let message = client.verify_credentials().await.unwrap_err().to_string();
        assert!(message.contains("无法连接币安") && message.contains("连接被拒绝"));
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
