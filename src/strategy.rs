use crate::domain::{Action, BAR_MS, Candle};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StrategyConfig {
    // Read legacy strategy selectors for compatibility, but never execute them
    // or write them back. Every indicator below is supplied to Jev.
    #[serde(default, skip_serializing)]
    pub kind: String,
    pub fast: usize,
    pub slow: usize,
    pub signal: usize,
    pub period: usize,
    pub lower: f64,
    pub upper: f64,
    pub deviations: f64,
}
impl StrategyConfig {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.fast >= 2
                && self.fast < self.slow
                && self.slow <= 150
                && (2..=100).contains(&self.signal)
                && (2..=150).contains(&self.period),
            "指标周期无效"
        );
        ensure!(
            self.lower.is_finite()
                && self.upper.is_finite()
                && self.lower >= 0.0
                && self.lower < self.upper
                && self.upper <= 100.0
                && self.deviations.is_finite()
                && self.deviations > 0.0
                && self.deviations <= 10.0,
            "指标阈值无效"
        );
        Ok(())
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Decision {
    pub action: Action,
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_action: Option<Action>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub indicators: BTreeMap<String, f64>,
    pub model: Option<String>,
    pub confidence: Option<f64>,
    pub probabilities: BTreeMap<String, f64>,
    pub input_tokens: u64,
    #[serde(default)]
    pub output_tokens: u64,
    #[serde(default)]
    pub request_bytes: usize,
}
/// A persisted decision summary; deliberately excludes candles and request inputs.
#[derive(Debug, Clone, Serialize)]
pub struct DecisionRecord {
    pub id: i64,
    pub time: i64,
    pub symbol: String,
    pub bar_close: Option<i64>,
    #[serde(flatten)]
    pub decision: Decision,
}
impl Decision {
    pub fn wait(source: &str) -> Self {
        Self {
            action: Action::Wait,
            source: source.into(),
            model_action: None,
            reason: None,
            indicators: BTreeMap::new(),
            model: None,
            confidence: None,
            probabilities: BTreeMap::new(),
            input_tokens: 0,
            output_tokens: 0,
            request_bytes: 0,
        }
    }
}
pub fn validate_bars(bars: &[Candle], now: i64) -> Result<()> {
    ensure!(bars.len() >= 100, "K 线尚未完成预热");
    for b in bars {
        ensure!(
            b.close_time < now
                && b.close.is_finite()
                && b.close > 0.0
                && b.open.is_finite()
                && b.high.is_finite()
                && b.low.is_finite()
                && b.volume.is_finite()
                && b.volume >= 0.0
                && b.high >= b.low
                && b.high >= b.close
                && b.low <= b.close
                && b.high >= b.open
                && b.low <= b.open
                && b.close_time == b.open_time + BAR_MS - 1,
            "K 线数据无效或尚未收盘"
        );
    }
    for pair in bars.windows(2) {
        ensure!(
            pair[1].open_time - pair[0].open_time == BAR_MS,
            "K 线不连续"
        );
    }
    Ok(())
}
pub fn ema(values: &[f64], period: usize) -> Vec<f64> {
    let mut out = vec![f64::NAN; values.len()];
    if values.len() < period {
        return out;
    }
    let mut e = values[..period].iter().sum::<f64>() / period as f64;
    out[period - 1] = e;
    let alpha = 2.0 / (period as f64 + 1.0);
    for i in period..values.len() {
        e = alpha * values[i] + (1.0 - alpha) * e;
        out[i] = e;
    }
    out
}
fn rsi(values: &[f64], period: usize) -> Vec<f64> {
    let mut out = vec![f64::NAN; values.len()];
    if values.len() <= period {
        return out;
    }
    let delta: Vec<f64> = values.windows(2).map(|w| w[1] - w[0]).collect();
    let mut gain = delta[..period].iter().map(|v| v.max(0.0)).sum::<f64>() / period as f64;
    let mut loss = delta[..period].iter().map(|v| (-v).max(0.0)).sum::<f64>() / period as f64;
    for i in period..values.len() {
        if i > period {
            gain = (gain * (period - 1) as f64 + delta[i - 1].max(0.0)) / period as f64;
            loss = (loss * (period - 1) as f64 + (-delta[i - 1]).max(0.0)) / period as f64;
        }
        out[i] = if loss == 0.0 {
            if gain == 0.0 { 50.0 } else { 100.0 }
        } else {
            100.0 - 100.0 / (1.0 + gain / loss)
        };
    }
    out
}
pub fn indicators(bars: &[Candle], c: &StrategyConfig) -> Result<BTreeMap<String, f64>> {
    c.validate()?;
    ensure!(
        bars.len() >= c.slow + c.signal + 2 && bars.len() >= c.period + 2,
        "指标预热不足"
    );
    let close: Vec<f64> = bars.iter().map(|b| b.close).collect();
    let n = close.len() - 1;
    let fast = ema(&close, c.fast);
    let slow = ema(&close, c.slow);
    let line: Vec<f64> = (c.slow - 1..close.len())
        .map(|i| fast[i] - slow[i])
        .collect();
    let signal = ema(&line, c.signal);
    let j = line.len() - 1;
    let rsi = rsi(&close, c.period);
    let window = &close[close.len() - c.period..];
    let mean = window.iter().sum::<f64>() / c.period as f64;
    let std = (window.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / c.period as f64).sqrt();
    let values = BTreeMap::from([
        ("ema_fast".into(), fast[n]),
        ("ema_slow".into(), slow[n]),
        ("macd".into(), line[j]),
        ("signal".into(), signal[j]),
        ("histogram".into(), line[j] - signal[j]),
        ("rsi".into(), rsi[n]),
        ("lower".into(), mean - std * c.deviations),
        ("middle".into(), mean),
        ("upper".into(), mean + std * c.deviations),
    ]);
    ensure!(values.values().all(|v| v.is_finite()), "指标包含无效值");
    Ok(values)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn legacy_decisions_without_filter_details_remain_readable() {
        let old = serde_json::json!({
            "action": "WAIT", "source": "jev", "indicators": {},
            "model": "jev-test-version", "confidence": 0.6,
            "probabilities": {"LONG": 0.8, "SHORT": 0.1, "WAIT": 0.1},
            "input_tokens": 100
        });
        let decision: Decision = serde_json::from_value(old).unwrap();
        assert_eq!(decision.action, Action::Wait);
        assert_eq!(decision.model_action, None);
        assert_eq!(decision.reason, None);
        assert_eq!(decision.confidence, Some(0.6));
        assert_eq!(decision.probabilities["LONG"], 0.8);
    }
    #[test]
    fn flat_rsi_is_neutral() {
        let r = rsi(&vec![10.0; 120], 14);
        assert_eq!(r[119], 50.0);
    }
    #[test]
    fn all_indicators_are_supplied_regardless_of_legacy_strategy_kind() {
        let bars: Vec<Candle> = (0..120)
            .map(|i| Candle {
                open_time: i * BAR_MS,
                close_time: (i + 1) * BAR_MS - 1,
                open: 10.0,
                high: 11.0,
                low: 9.0,
                close: 10.0,
                volume: 1.0,
            })
            .collect();
        let mut config = crate::config::Config::default().strategy;
        for kind in ["", "ema_cross", "macd", "bollinger", "rsi"] {
            config.kind = kind.into();
            let values = indicators(&bars, &config).unwrap();
            assert_eq!(values.len(), 9);
            for key in ["ema_fast", "ema_slow", "lower", "middle", "upper"] {
                assert_eq!(values[key], 10.0);
            }
            for key in ["macd", "signal", "histogram"] {
                assert_eq!(values[key], 0.0);
            }
            assert_eq!(values["rsi"], 50.0);
        }
        assert!(indicators(&bars[..30], &config).is_err());
        let mut malformed = bars;
        malformed[119].close = f64::NAN;
        assert!(indicators(&malformed, &config).is_err());
    }
    #[test]
    fn gaps_and_unclosed_bars_are_rejected() {
        let bars: Vec<Candle> = (0..100)
            .map(|i| Candle {
                open_time: i * BAR_MS,
                close_time: (i + 1) * BAR_MS - 1,
                open: 10.,
                high: 11.,
                low: 9.,
                close: 10.,
                volume: 1.,
            })
            .collect();
        assert!(validate_bars(&bars, 100 * BAR_MS).is_ok());
        assert!(validate_bars(&bars, 99 * BAR_MS).is_err());
        let mut gap = bars.clone();
        gap[50].open_time += 1;
        assert!(validate_bars(&gap, 100 * BAR_MS).is_err());
    }
}
