use crate::{
    config::{Config, DecisionMode},
    domain::{Action, Candle, Quote},
    strategy::Decision,
};
use anyhow::{Context, Result, ensure};
use reqwest::Client;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{collections::BTreeMap, time::Duration};

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
}
#[derive(Debug, Deserialize)]
struct Choice {
    #[serde(rename = "type")]
    kind: String,
    choice: Action,
    confidence: f64,
    probabilities: BTreeMap<String, f64>,
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
    pub async fn decide(
        &self,
        symbol: &str,
        bars: &[Candle],
        quote: &Quote,
        c: &Config,
        builtin: Decision,
    ) -> Result<Decision> {
        if c.decision_mode == DecisionMode::Builtin
            || (c.decision_mode == DecisionMode::Hybrid && builtin.action == Action::Wait)
        {
            return Ok(builtin);
        }
        let key = self
            .key
            .as_deref()
            .context("请在页面的账户与连接中配置 Jev API Key")?;
        let body = json!({"model":c.jev.model,"state":{"symbol":symbol,"timeframe":"5m","closed_candles":bars,"quote":quote,"indicators":builtin.indicators,"builtin_signal":builtin.action,"notional_usdt":c.notional_usdt,"target_net_profit_usdt":c.profit_usdt,"maker_fee":c.maker_fee,"taker_fee":c.taker_fee,"forecast_bars":c.jev.forecast_bars,"position":"flat"},"questions":{"action":{"type":"choice","instructions":"Using only the supplied closed 5-minute market data and precomputed indicators, select the entry direction for the specified forecast window after accounting for spread and commission. Do not treat confidence as win probability. Select WAIT when evidence is insufficient. Do not assume live market access or infer missing data.","criteria":{"LONG":"Evidence supports an upward move sufficient to cover costs and the configured profit target.","SHORT":"Evidence supports a downward move sufficient to cover costs and the configured profit target.","WAIT":"No sufficiently supported opportunity, ambiguous signals, or insufficient data."}}}});
        let r = self
            .client
            .post(format!("{}/v1/systemone", self.base))
            .bearer_auth(key)
            .timeout(Duration::from_secs(c.jev.timeout_seconds))
            .json(&body)
            .send()
            .await
            .map_err(|_| anyhow::anyhow!("Jev 请求超时或网络异常"))?;
        ensure!(r.status().is_success(), "Jev HTTP {}", r.status().as_u16());
        let raw: Value = r.json().await.context("Jev 返回格式错误")?;
        let response: Response = serde_json::from_value(raw)?;
        let answer = response
            .answers
            .get("action")
            .context("Jev 未返回 action")?;
        validate_choice(answer)?;
        let mut d = builtin.clone();
        d.action = if answer.confidence < c.jev.min_confidence {
            Action::Wait
        } else {
            answer.choice
        };
        if c.decision_mode == DecisionMode::Hybrid && d.action != builtin.action {
            d.action = Action::Wait;
        }
        d.source = if c.decision_mode == DecisionMode::Hybrid {
            "hybrid"
        } else {
            "jev"
        }
        .into();
        d.model = Some(response.model);
        d.confidence = Some(answer.confidence);
        d.probabilities = answer.probabilities.clone();
        d.input_tokens = response.usage.input_tokens;
        Ok(d)
    }
}
fn validate_choice(a: &Choice) -> Result<()> {
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
    ensure!(
        a.probabilities
            .values()
            .all(|p| p.is_finite() && (0.0..=1.0).contains(p))
            && (a.probabilities.values().sum::<f64>() - 1.0).abs() < 0.001,
        "Jev 概率分布无效"
    );
    let key = match a.choice {
        Action::Long => "LONG",
        Action::Short => "SHORT",
        Action::Wait => "WAIT",
    };
    let top = a.probabilities.values().copied().fold(0.0, f64::max);
    ensure!(
        (a.probabilities[key] - top).abs() < 0.001,
        "Jev choice 不是最高概率选项"
    );
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn typed_http_decisions_enforce_confidence_and_hybrid_agreement() {
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
            assert_eq!(body["questions"]["action"]["type"], "choice");
            assert_eq!(body["state"]["timeframe"], "5m");
            assert_eq!(body["state"]["position"], "flat");
            count.fetch_add(1, Ordering::SeqCst);
            Json(response.lock().await.clone())
        }
        let response = Arc::new(Mutex::new(
            json!({"model":"jev-test-version","usage":{"input_tokens":100},"answers":{"action":{"type":"choice","choice":"LONG","confidence":0.9,"probabilities":{"LONG":0.9,"SHORT":0.05,"WAIT":0.05}}}}),
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
        let mut c = Config::default();
        let quote = Quote {
            bid: 100.into(),
            ask: 100.into(),
            bid_qty: 1.into(),
            ask_qty: 1.into(),
            time: crate::domain::now_ms(),
        };
        let mut builtin = Decision::wait("ema_cross");
        builtin.action = Action::Short;
        let unchanged = jev
            .decide("XUSDT", &[], &quote, &c, builtin.clone())
            .await
            .unwrap();
        assert_eq!(unchanged.action, Action::Short);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        c.decision_mode = DecisionMode::Jev;
        let result = jev
            .decide("XUSDT", &[], &quote, &c, builtin.clone())
            .await
            .unwrap();
        assert_eq!(result.action, Action::Long);
        assert_eq!(result.model.as_deref(), Some("jev-test-version"));
        assert_eq!(result.input_tokens, 100);
        c.decision_mode = DecisionMode::Hybrid;
        assert_eq!(
            jev.decide("XUSDT", &[], &quote, &c, builtin.clone())
                .await
                .unwrap()
                .action,
            Action::Wait
        );
        response.lock().await["answers"]["action"]["confidence"] = json!(0.6);
        c.decision_mode = DecisionMode::Jev;
        assert_eq!(
            jev.decide("XUSDT", &[], &quote, &c, builtin.clone())
                .await
                .unwrap()
                .action,
            Action::Wait
        );
        response.lock().await["answers"]["action"]["probabilities"]["SHORT"] = json!(0.9);
        assert!(jev.decide("XUSDT", &[], &quote, &c, builtin).await.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 4);
        server.abort();
    }
    #[test]
    fn malformed_distributions_do_not_execute() {
        let a = Choice {
            kind: "choice".into(),
            choice: Action::Long,
            confidence: 0.8,
            probabilities: BTreeMap::from([
                ("LONG".into(), 0.3),
                ("SHORT".into(), 0.6),
                ("WAIT".into(), 0.1),
            ]),
        };
        assert!(validate_choice(&a).is_err());
    }
}
