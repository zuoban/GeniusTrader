use crate::{config::Mode, domain::*, settings::Credentials};
use anyhow::{Context, Result, ensure};
use hmac::{Hmac, Mac};
use reqwest::{Client, Method};
use rust_decimal::Decimal;
use serde::Deserialize;
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
        let m = self
            .request(Method::GET, "/fapi/v1/positionSide/dual", vec![], true)
            .await?;
        ensure!(
            m["dualSidePosition"] == false,
            "实盘适配器只支持单向持仓模式"
        );
        self.available_balance().await
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
    #[test]
    fn hmac_known_vector() {
        assert_eq!(
            signature("key", "The quick brown fox jumps over the lazy dog"),
            "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8"
        );
    }
}
