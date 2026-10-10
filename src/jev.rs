use crate::{
    config::Config,
    domain::{Action, BAR_MS, Candle, Quote, now_ms},
    strategy::{self, Decision},
};
use anyhow::{Context, Result, anyhow, ensure};
use reqwest::Client;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{collections::BTreeMap, error::Error as _, fmt::Write as _, time::Duration};

pub struct Jev {
    client: Client,
    key: Option<String>,
    base: String,
}
#[derive(Debug, Deserialize)]
struct Response {
    model: String,
    answers: BTreeMap<String, Choice>,
    #[serde(default)]
    usage: Usage,
}
#[derive(Debug, Default, Deserialize)]
struct Usage {
    #[serde(default)]
    input_tokens: u64,
    #[serde(default)]
    output_tokens: u64,
}
#[derive(Debug, Deserialize)]
struct Choice {
    #[serde(rename = "type")]
    kind: String,
    choice: Action,
    confidence: f64,
    probabilities: BTreeMap<String, f64>,
}
#[derive(Deserialize)]
struct Models {
    models: Vec<Model>,
}
#[derive(Deserialize)]
struct Model {
    name: String,
}
impl Jev {
    pub fn new() -> Result<Self> {
        Self::with_key("")
    }
    pub fn with_key(key: &str) -> Result<Self> {
        Ok(Self {
            client: Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            key: (!key.is_empty()).then(|| key.to_string()),
            base: "https://api.typesafe.ai".into(),
        })
    }
    pub fn available(&self) -> bool {
        self.key.is_some()
    }
    pub async fn verify_credentials(&self) -> Result<()> {
        let key = self
            .key
            .as_deref()
            .context("请填写 Jev API Key 后再测试连接")?;
        // Listing models authenticates the key without submitting a billed decision.
        let response = self
            .client
            .get(format!("{}/v1/models", self.base))
            .bearer_auth(key)
            .timeout(Duration::from_secs(10))
            .send()
            .await
            .map_err(|error| transport_error(error, "连接测试", 10))?;
        let status = response.status();
        if !status.is_success() {
            match status.as_u16() {
                401 => anyhow::bail!("Jev API Key 无效或已撤销，请检查密钥"),
                403 => anyhow::bail!("Jev 访问被拒绝，请检查账户权限"),
                429 => anyhow::bail!("Jev 请求过于频繁，请稍后重试"),
                500..=599 => anyhow::bail!("Jev 服务暂时不可用，请稍后重试"),
                _ => anyhow::bail!("Jev 连接测试失败（HTTP {}）", status.as_u16()),
            }
        }
        let models: Models = response
            .json()
            .await
            .map_err(|error| transport_error(error, "模型列表读取", 10))?;
        ensure!(
            !models.models.is_empty()
                && models
                    .models
                    .iter()
                    .all(|model| !model.name.trim().is_empty()),
            "Jev 未返回可用模型，请检查账户访问权限"
        );
        Ok(())
    }
    pub async fn decide(
        &self,
        symbol: &str,
        bars: &[Candle],
        quote: &Quote,
        c: &Config,
    ) -> Result<Decision> {
        let key = self
            .key
            .as_deref()
            .context("请在页面的账户与连接中配置 Jev API Key")?;
        let indicators = strategy::indicators(bars, &c.strategy)?;
        let body = decision_request(symbol, bars, quote, c, &indicators)?;
        let payload = serde_json::to_vec(&body)?;
        let request_bytes = payload.len();
        let r = self
            .client
            .post(format!("{}/v1/systemone", self.base))
            .bearer_auth(key)
            .timeout(Duration::from_secs(c.jev.timeout_seconds))
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(payload)
            .send()
            .await
            .map_err(|error| transport_error(error, "决策请求", c.jev.timeout_seconds))?;
        let status = r.status();
        if !status.is_success() {
            match status.as_u16() {
                401 => anyhow::bail!("Jev API Key 无效或已撤销（HTTP 401），请检查密钥"),
                402 => anyhow::bail!("Jev 账户计费被拒绝（HTTP 402），请检查余额或计费状态"),
                403 => anyhow::bail!("Jev 决策访问被拒绝（HTTP 403），请检查账户权限或网络限制"),
                422 => {
                    anyhow::bail!("Jev 决策请求未通过校验（HTTP 422），请检查模型名称与输入格式")
                }
                429 => anyhow::bail!("Jev 请求过于频繁（HTTP 429），请稍后重试"),
                500..=599 => {
                    anyhow::bail!("Jev 服务暂时不可用（HTTP {}），请稍后重试", status.as_u16())
                }
                _ => anyhow::bail!("Jev 决策请求失败（HTTP {}）", status.as_u16()),
            }
        }
        let response: Response = r
            .json()
            .await
            .map_err(|error| transport_error(error, "决策结果读取", c.jev.timeout_seconds))?;
        let answer = response
            .answers
            .get("action")
            .context("Jev 未返回 action")?;
        validate_choice(answer)?;
        let mut d = Decision::wait("jev");
        d.indicators = indicators;
        d.action = if answer.confidence < c.jev.min_confidence {
            Action::Wait
        } else {
            answer.choice
        };
        d.model = Some(response.model);
        d.confidence = Some(answer.confidence);
        d.probabilities = answer.probabilities.clone();
        d.input_tokens = response.usage.input_tokens;
        d.output_tokens = response.usage.output_tokens;
        d.request_bytes = request_bytes;
        Ok(d)
    }
}
fn decision_request(
    symbol: &str,
    bars: &[Candle],
    quote: &Quote,
    c: &Config,
    indicators: &BTreeMap<String, f64>,
) -> Result<Value> {
    // Shared timestamps are lossless only for validated, contiguous closed bars.
    strategy::validate_bars(bars, now_ms())?;
    let mut csv = String::from("open,high,low,close,volume");
    for bar in bars {
        // Display uses round-trip numeric precision; do not round prices/volume.
        write!(
            csv,
            "\n{},{},{},{},{}",
            bar.open, bar.high, bar.low, bar.close, bar.volume
        )?;
    }
    Ok(json!({
        "model": c.jev.model,
        "state": {
            "symbol": symbol,
            "timeframe": "5m",
            "closed_candles": {
                "first_open_time_ms": bars[0].open_time,
                "interval_ms": BAR_MS,
                "close_offset_ms": BAR_MS - 1,
                "order": "oldest_first",
                "csv": csv
            },
            "quote": quote,
            "indicators": indicators,
            "indicator_parameters": c.strategy,
            "notional_usdt": c.notional_usdt,
            "target_net_profit_usdt": c.profit_usdt,
            "maker_fee": c.maker_fee,
            "taker_fee": c.taker_fee,
            "forecast_bars": c.jev.forecast_bars,
            "position": "flat"
        },
        "questions": {
            "action": {
                "type": "choice",
                "instructions": "Choose an entry for forecast_bars using only these closed 5m bars, indicators and quote. Account for spread and fees. No external or missing data. WAIT on weak/conflicting evidence. Confidence is not win probability.",
                "criteria": {
                    "LONG": "Upward move supports target_net_profit_usdt after spread and fees.",
                    "SHORT": "Downward move supports target_net_profit_usdt after spread and fees.",
                    "WAIT": "Insufficient, conflicting or non-profitable evidence."
                }
            }
        }
    }))
}
fn transport_error(error: reqwest::Error, operation: &str, timeout_seconds: u64) -> anyhow::Error {
    if error.is_timeout() {
        return anyhow!(
            "Jev {operation}超时（{timeout_seconds} 秒）；请检查网络，决策超时可在「策略与交易」调整请求超时"
        );
    }
    if error.is_decode() {
        return anyhow!("Jev {operation}返回格式错误");
    }
    // Inspect causes only for classification. Never expose URLs, headers or upstream text.
    let mut source = error.source();
    let mut dns = false;
    let mut tls = false;
    let mut interrupted = false;
    while let Some(cause) = source {
        let message = cause.to_string().to_ascii_lowercase();
        dns |= message.contains("dns") || message.contains("failed to lookup address");
        tls |=
            message.contains("certificate") || message.contains("tls") || message.contains("ssl");
        if let Some(io) = cause.downcast_ref::<std::io::Error>() {
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
        anyhow!("Jev {operation}域名解析失败，请检查 DNS 或代理设置")
    } else if tls {
        anyhow!("Jev {operation}TLS 连接失败，请检查系统时间、证书或代理设置")
    } else if interrupted {
        anyhow!("Jev {operation}失败：连接中断，请检查网络或代理后重试")
    } else if error.is_connect() {
        anyhow!("Jev {operation}失败：无法建立连接，请检查网络或代理设置")
    } else {
        anyhow!("Jev {operation}网络请求失败，请检查连接或代理后重试")
    }
}
fn validate_choice(a: &Choice) -> Result<()> {
    // The SDK specifies an approximate sum, and observed responses use hundredths.
    // Three independently rounded options can total 0.99 or 1.01. Keep the raw
    // values for audit; this tolerance must not loosen range or winning-choice checks.
    const SUM_TOLERANCE: f64 = 0.01;
    const FLOAT_TOLERANCE: f64 = 1e-12;
    ensure!(
        a.kind == "choice" && a.confidence.is_finite() && (0.0..=1.0).contains(&a.confidence),
        "Jev 置信度格式无效"
    );
    ensure!(
        a.probabilities.len() == 3
            && ["LONG", "SHORT", "WAIT"]
                .iter()
                .all(|k| a.probabilities.contains_key(*k)),
        "Jev 选项不匹配"
    );
    let long = a.probabilities["LONG"];
    let short = a.probabilities["SHORT"];
    let wait = a.probabilities["WAIT"];
    ensure!(
        a.probabilities
            .values()
            .all(|p| p.is_finite() && (0.0..=1.0).contains(p)),
        "Jev 概率数值无效（LONG={long:?}，SHORT={short:?}，WAIT={wait:?}）；各项须为 0–1 的有限值"
    );
    let sum = a.probabilities.values().sum::<f64>();
    ensure!(
        (sum - 1.0).abs() <= SUM_TOLERANCE + FLOAT_TOLERANCE,
        "Jev 概率总和异常（LONG={long:?}，SHORT={short:?}，WAIT={wait:?}，总和={sum:.6}）；允许 0.99–1.01"
    );
    let key = match a.choice {
        Action::Long => "LONG",
        Action::Short => "SHORT",
        Action::Wait => "WAIT",
    };
    let top = a.probabilities.values().copied().fold(0.0, f64::max);
    ensure!(
        a.probabilities[key] + FLOAT_TOLERANCE >= top,
        "Jev choice 不是最高概率选项"
    );
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    fn candles() -> Vec<Candle> {
        candles_with_count(120)
    }
    fn candles_with_count(count: i64) -> Vec<Candle> {
        let end = crate::domain::now_ms() / crate::domain::BAR_MS * crate::domain::BAR_MS;
        (0..count)
            .map(|i| {
                let open_time = end - (count - i) * crate::domain::BAR_MS;
                Candle {
                    open_time,
                    close_time: open_time + crate::domain::BAR_MS - 1,
                    open: 100.0,
                    high: 101.0,
                    low: 99.0,
                    close: 100.0,
                    volume: 10.0,
                }
            })
            .collect()
    }
    #[test]
    fn compact_request_preserves_every_bar_and_reduces_payload_size() {
        let c = Config::default();
        let quote = Quote {
            bid: 100.into(),
            ask: 100.into(),
            bid_qty: 1.into(),
            ask_qty: 1.into(),
            time: now_ms(),
        };
        for count in [120, 500] {
            for price in [0.0000123456789, 123.456789, 65000.1234567] {
                let mut bars = candles_with_count(count);
                for (i, bar) in bars.iter_mut().enumerate() {
                    bar.open = price * (1.0 + i as f64 * 0.0001);
                    bar.high = bar.open * 1.002;
                    bar.low = bar.open * 0.998;
                    bar.close = bar.open * 1.001;
                    bar.volume = if i == 0 {
                        0.0
                    } else {
                        123456.789 + i as f64 * 1.2345
                    };
                }
                let indicators = strategy::indicators(&bars, &c.strategy).unwrap();
                let body = decision_request("XUSDT", &bars, &quote, &c, &indicators).unwrap();
                let table = &body["state"]["closed_candles"];
                let mut lines = table["csv"].as_str().unwrap().lines();
                assert_eq!(lines.next(), Some("open,high,low,close,volume"));
                assert_eq!(table["order"], "oldest_first");
                let rows = lines.collect::<Vec<_>>();
                assert_eq!(rows.len(), bars.len());
                for (i, (row, bar)) in rows.iter().zip(&bars).enumerate() {
                    let values = row
                        .split(',')
                        .map(|v| v.parse::<f64>().unwrap())
                        .collect::<Vec<_>>();
                    assert_eq!(values.len(), 5);
                    for (actual, original) in values
                        .iter()
                        .zip([bar.open, bar.high, bar.low, bar.close, bar.volume])
                    {
                        assert_eq!(actual.to_bits(), original.to_bits());
                    }
                    let open = table["first_open_time_ms"].as_i64().unwrap()
                        + i as i64 * table["interval_ms"].as_i64().unwrap();
                    assert_eq!(open, bar.open_time);
                    assert_eq!(
                        open + table["close_offset_ms"].as_i64().unwrap(),
                        bar.close_time
                    );
                }
                assert_eq!(body["state"]["quote"], json!(quote));
                assert_eq!(body["state"]["indicators"], json!(indicators));
                assert_eq!(body["state"]["indicator_parameters"], json!(c.strategy));
                let mut legacy = body.clone();
                legacy["state"]["closed_candles"] = json!(bars);
                legacy["questions"]["action"] = json!({
                    "type":"choice",
                    "instructions":"Using only the supplied closed 5-minute market data and precomputed indicators, select the entry direction for the specified forecast window after accounting for spread and commission. Do not treat confidence as win probability. Select WAIT when evidence is insufficient. Do not assume live market access or infer missing data.",
                    "criteria":{
                        "LONG":"Evidence supports an upward move sufficient to cover costs and the configured profit target.",
                        "SHORT":"Evidence supports a downward move sufficient to cover costs and the configured profit target.",
                        "WAIT":"No sufficiently supported opportunity, ambiguous signals, or insufficient data."
                    }
                });
                let old_bytes = serde_json::to_vec(&legacy).unwrap().len();
                let new_bytes = serde_json::to_vec(&body).unwrap().len();
                assert!(
                    new_bytes * 100 <= old_bytes * 65,
                    "{count} bars: {new_bytes}/{old_bytes}"
                );
                println!(
                    "{count} bars, price {price}: {old_bytes} -> {new_bytes} bytes ({:.1}% reduction)",
                    100.0 * (1.0 - new_bytes as f64 / old_bytes as f64)
                );
            }
        }
    }
    #[test]
    fn compact_request_rejects_gaps_unclosed_bars_and_incomplete_history() {
        let c = Config::default();
        let quote = Quote {
            bid: 100.into(),
            ask: 100.into(),
            bid_qty: 1.into(),
            ask_qty: 1.into(),
            time: now_ms(),
        };
        let bars = candles();
        let indicators = strategy::indicators(&bars, &c.strategy).unwrap();
        assert!(decision_request("XUSDT", &bars[..99], &quote, &c, &indicators).is_err());
        let mut gap = bars.clone();
        gap[50].open_time += BAR_MS;
        gap[50].close_time += BAR_MS;
        assert!(decision_request("XUSDT", &gap, &quote, &c, &indicators).is_err());
        let mut future = bars;
        future[119].open_time += BAR_MS;
        future[119].close_time += BAR_MS;
        assert!(decision_request("XUSDT", &future, &quote, &c, &indicators).is_err());
    }
    #[tokio::test]
    async fn credential_tests_only_list_models_and_sanitize_errors() {
        use axum::{
            Json, Router,
            extract::State,
            http::{HeaderMap, StatusCode},
            routing::get,
        };
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        use tokio::sync::Mutex;
        type MockState = (Arc<Mutex<(StatusCode, Value)>>, Arc<AtomicUsize>);
        async fn respond(
            State((response, count)): State<MockState>,
            headers: HeaderMap,
        ) -> (StatusCode, Json<Value>) {
            assert_eq!(headers["authorization"], "Bearer local-jev-test-key");
            count.fetch_add(1, Ordering::SeqCst);
            let (status, body) = response.lock().await.clone();
            (status, Json(body))
        }
        let response = Arc::new(Mutex::new((
            StatusCode::OK,
            json!({"models":[{"name":"jev-latest","description":"Jev","release_date":"2026-09-15"}]}),
        )));
        let count = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route("/v1/models", get(respond))
            .with_state((response.clone(), count.clone()));
        let (base, server) = crate::test_support::mock_server(app).await;
        let mut jev = Jev {
            client: Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap(),
            key: Some("local-jev-test-key".into()),
            base,
        };
        jev.verify_credentials().await.unwrap();
        for (status, expected) in [
            (401, "密钥"),
            (403, "权限"),
            (429, "频繁"),
            (503, "暂时不可用"),
            (302, "HTTP 302"),
            (400, "HTTP 400"),
        ] {
            *response.lock().await = (
                StatusCode::from_u16(status).unwrap(),
                json!({"error":"local-jev-test-key should never be exposed"}),
            );
            let error = jev.verify_credentials().await.unwrap_err().to_string();
            assert!(error.contains(expected), "{error}");
            assert!(!error.contains("local-jev-test-key"));
        }
        for body in [
            json!({"models":[]}),
            json!({"models":[{"name":" "}]}),
            json!({"models":[{}]}),
            json!({"models":"local-jev-test-key"}),
        ] {
            *response.lock().await = (StatusCode::OK, body);
            let error = jev.verify_credentials().await.unwrap_err().to_string();
            assert!(!error.contains("local-jev-test-key"));
        }
        assert_eq!(count.load(Ordering::SeqCst), 11);
        server.abort();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        jev.base = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);
        assert_eq!(
            jev.verify_credentials().await.unwrap_err().to_string(),
            "Jev 连接测试失败：无法建立连接，请检查网络或代理设置"
        );
        assert!(Jev::new().unwrap().verify_credentials().await.is_err());
    }
    #[tokio::test]
    async fn decision_failures_distinguish_http_and_header_or_body_timeouts_without_secrets() {
        use axum::{
            Router,
            body::Body,
            extract::State,
            http::StatusCode,
            response::{IntoResponse, Response as HttpResponse},
            routing::post,
        };
        use std::{convert::Infallible, sync::Arc};
        use tokio::sync::Mutex;
        type MockState = Arc<Mutex<(StatusCode, u8, Value)>>;
        async fn respond(State(state): State<MockState>) -> HttpResponse {
            let (status, phase, body) = state.lock().await.clone();
            if phase == 1 {
                tokio::time::sleep(Duration::from_millis(1200)).await;
            }
            let stream = futures_util::stream::once(async move {
                if phase == 2 {
                    tokio::time::sleep(Duration::from_millis(1200)).await;
                }
                Ok::<_, Infallible>(body.to_string())
            });
            (
                status,
                [("content-type", "application/json")],
                Body::from_stream(stream),
            )
                .into_response()
        }
        let response = Arc::new(Mutex::new((StatusCode::OK, 0, json!({}))));
        let app = Router::new()
            .route("/v1/systemone", post(respond))
            .with_state(response.clone());
        let (base, server) = crate::test_support::mock_server(app).await;
        let jev = Jev {
            client: Client::builder().no_proxy().build().unwrap(),
            key: Some("local-jev-decision-key".into()),
            base,
        };
        let mut c = Config::default();
        c.jev.timeout_seconds = 1;
        let bars = candles();
        let quote = Quote {
            bid: 100.into(),
            ask: 100.into(),
            bid_qty: 1.into(),
            ask_qty: 1.into(),
            time: crate::domain::now_ms(),
        };
        for (status, reason) in [
            (401, "密钥"),
            (402, "计费"),
            (403, "权限"),
            (422, "输入格式"),
            (429, "频繁"),
            (503, "暂时不可用"),
        ] {
            *response.lock().await = (
                StatusCode::from_u16(status).unwrap(),
                0,
                json!({"error":"local-jev-decision-key"}),
            );
            let error = jev
                .decide("XUSDT", &bars, &quote, &c)
                .await
                .unwrap_err()
                .to_string();
            assert!(error.contains(reason), "{error}");
            assert!(error.contains(&status.to_string()));
            assert!(!error.contains("local-jev-decision-key"));
        }
        for (phase, reason) in [
            (0, "返回格式错误"),
            (1, "决策请求超时（1 秒）"),
            (2, "决策结果读取超时（1 秒）"),
        ] {
            *response.lock().await = (
                StatusCode::OK,
                phase,
                json!({"model":"local-jev-decision-key"}),
            );
            let error = jev
                .decide("XUSDT", &bars, &quote, &c)
                .await
                .unwrap_err()
                .to_string();
            assert!(error.contains(reason), "{error}");
            assert!(!error.contains("local-jev-decision-key"));
        }
        server.abort();
    }
    #[tokio::test]
    async fn jev_decides_without_local_signals_and_enforces_confidence() {
        use axum::{Json, Router, extract::State, http::HeaderMap, routing::post};
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        use tokio::sync::Mutex;
        type MockState = (Arc<Mutex<Value>>, Arc<AtomicUsize>);
        async fn respond(
            State((response, count)): State<MockState>,
            headers: HeaderMap,
            Json(body): Json<Value>,
        ) -> Json<Value> {
            assert_eq!(headers["authorization"], "Bearer local-jev-key");
            assert_eq!(headers["content-type"], "application/json");
            assert_eq!(body["questions"]["action"]["type"], "choice");
            assert_eq!(body["state"]["timeframe"], "5m");
            assert_eq!(body["state"]["position"], "flat");
            assert!(body["state"].get("builtin_signal").is_none());
            assert!(body["state"]["indicator_parameters"].get("kind").is_none());
            assert_eq!(
                body["state"]["closed_candles"]["csv"]
                    .as_str()
                    .unwrap()
                    .lines()
                    .count(),
                121
            );
            for key in [
                "ema_fast",
                "ema_slow",
                "macd",
                "signal",
                "histogram",
                "rsi",
                "lower",
                "middle",
                "upper",
            ] {
                assert!(
                    body["state"]["indicators"][key].is_number(),
                    "missing {key}"
                );
            }
            count.fetch_add(1, Ordering::SeqCst);
            Json(response.lock().await.clone())
        }
        let response = Arc::new(Mutex::new(
            json!({"model":"jev-test-version","usage":{"input_tokens":100,"output_tokens":34},"answers":{"action":{"type":"choice","choice":"LONG","confidence":0.9,"probabilities":{"LONG":0.9,"SHORT":0.05,"WAIT":0.05}}}}),
        ));
        let calls = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route("/v1/systemone", post(respond))
            .with_state((response.clone(), calls.clone()));
        let (base, server) = crate::test_support::mock_server(app).await;
        let jev = Jev {
            client: Client::builder().no_proxy().build().unwrap(),
            key: Some("local-jev-key".into()),
            base,
        };
        let c = Config::default();
        let bars = candles();
        let quote = Quote {
            bid: 100.into(),
            ask: 100.into(),
            bid_qty: 1.into(),
            ask_qty: 1.into(),
            time: crate::domain::now_ms(),
        };
        let result = jev.decide("XUSDT", &bars, &quote, &c).await.unwrap();
        assert_eq!(result.action, Action::Long);
        assert_eq!(result.source, "jev");
        assert_eq!(result.model.as_deref(), Some("jev-test-version"));
        assert_eq!(result.input_tokens, 100);
        assert_eq!(result.output_tokens, 34);
        assert_eq!(
            result.request_bytes,
            serde_json::to_vec(
                &decision_request("XUSDT", &bars, &quote, &c, &result.indicators).unwrap()
            )
            .unwrap()
            .len()
        );
        assert_eq!(result.indicators["rsi"], 50.0);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        for (choice, probabilities, action) in [
            (
                "SHORT",
                json!({"LONG":0.05,"SHORT":0.9,"WAIT":0.05}),
                Action::Short,
            ),
            (
                "WAIT",
                json!({"LONG":0.05,"SHORT":0.05,"WAIT":0.9}),
                Action::Wait,
            ),
        ] {
            response.lock().await["answers"]["action"] = json!({"type":"choice","choice":choice,"confidence":0.9,"probabilities":probabilities});
            let result = jev.decide("XUSDT", &bars, &quote, &c).await.unwrap();
            assert_eq!(result.action, action);
            assert_eq!(result.source, "jev");
        }
        response.lock().await["answers"]["action"] = json!({"type":"choice","choice":"LONG","confidence":0.9,"probabilities":{"LONG":0.9,"SHORT":0.05,"WAIT":0.05}});
        response.lock().await["answers"]["action"]["confidence"] = json!(0.6);
        assert_eq!(
            jev.decide("XUSDT", &bars, &quote, &c).await.unwrap().action,
            Action::Wait
        );
        response.lock().await["answers"]["action"]["probabilities"]["SHORT"] = json!(0.9);
        let error = jev
            .decide("XUSDT", &bars, &quote, &c)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("概率总和异常"));
        assert!(error.contains("总和=1.850000"));
        for probabilities in [
            json!({"LONG":0.84,"SHORT":0.03,"WAIT":0.12}),
            json!({"LONG":0.85,"SHORT":0.04,"WAIT":0.12}),
        ] {
            response.lock().await["answers"]["action"]["probabilities"] = probabilities.clone();
            for confidence in [0.9, 0.6] {
                response.lock().await["answers"]["action"]["confidence"] = json!(confidence);
                let result = jev.decide("XUSDT", &bars, &quote, &c).await.unwrap();
                assert_eq!(
                    result.action,
                    if confidence < c.jev.min_confidence {
                        Action::Wait
                    } else {
                        Action::Long
                    }
                );
                assert_eq!(result.confidence, Some(confidence));
                // Retain the service's actual probabilities rather than silently normalizing.
                assert_eq!(
                    serde_json::to_value(&result.probabilities).unwrap(),
                    probabilities
                );
            }
        }
        assert_eq!(calls.load(Ordering::SeqCst), 9);
        server.abort();
    }
    fn choice(probabilities: [f64; 3], action: Action) -> Choice {
        Choice {
            kind: "choice".into(),
            choice: action,
            confidence: 0.8,
            probabilities: ["LONG", "SHORT", "WAIT"]
                .into_iter()
                .zip(probabilities)
                .map(|(key, value)| (key.to_string(), value))
                .collect(),
        }
    }
    #[test]
    fn rounded_distributions_accept_both_boundaries_and_ties() {
        for (probabilities, action) in [
            ([0.84, 0.03, 0.12], Action::Long),    // sum 0.99
            ([0.85, 0.04, 0.12], Action::Long),    // sum 1.01
            ([0.333, 0.333, 0.333], Action::Wait), // sum 0.999, tie
            ([0.33, 0.33, 0.33], Action::Short),   // sum 0.99, tie
            ([0.34, 0.34, 0.33], Action::Short),   // sum 1.01, tie
            ([0.0, 0.0, 1.0], Action::Wait),
        ] {
            validate_choice(&choice(probabilities, action)).unwrap();
        }
    }
    #[test]
    fn malformed_distributions_do_not_execute() {
        for (probabilities, reason) in [
            ([-0.01, 0.9, 0.11], "概率数值无效"),
            ([1.01, 0.0, 0.0], "概率数值无效"),
            ([f64::NAN, 0.0, 0.0], "概率数值无效"),
            ([f64::INFINITY, 0.0, 0.0], "概率数值无效"),
            ([0.0, 0.0, 0.0], "概率总和异常"),
            ([0.9, 0.03, 0.05], "概率总和异常"),
            ([0.9, 0.05, 0.07], "概率总和异常"),
            ([0.9, 0.04, 0.04999], "概率总和异常"),
            ([0.9, 0.04, 0.07001], "概率总和异常"),
            ([0.3, 0.6, 0.1], "不是最高概率"),
            ([0.3331, 0.3332, 0.3337], "不是最高概率"),
        ] {
            let error = validate_choice(&choice(probabilities, Action::Long))
                .unwrap_err()
                .to_string();
            assert!(error.contains(reason), "{error}");
        }
        let error = validate_choice(&choice([0.9, 0.05, 0.07], Action::Long))
            .unwrap_err()
            .to_string();
        for detail in ["LONG=0.9", "SHORT=0.05", "WAIT=0.07", "总和=1.020000"] {
            assert!(error.contains(detail), "{error}");
        }
    }
    #[test]
    fn malformed_choice_fields_remain_rejected_without_exposing_upstream_text() {
        for confidence in [f64::NAN, f64::INFINITY, -0.1, 1.1] {
            let mut a = choice([0.9, 0.05, 0.05], Action::Long);
            a.confidence = confidence;
            assert!(validate_choice(&a).is_err());
        }
        for field in ["type", "missing", "extra", "unknown"] {
            let mut a = choice([0.9, 0.05, 0.05], Action::Long);
            match field {
                "type" => a.kind = "local-jev-key".into(),
                "missing" => {
                    a.probabilities.remove("WAIT");
                }
                "extra" => {
                    a.probabilities.insert("local-jev-key".into(), 0.0);
                }
                "unknown" => {
                    a.probabilities.remove("WAIT");
                    a.probabilities.insert("local-jev-key".into(), 0.05);
                }
                _ => unreachable!(),
            }
            let error = validate_choice(&a).unwrap_err().to_string();
            assert!(!error.contains("local-jev-key"));
        }
    }
}
