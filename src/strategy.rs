use crate::domain::{Action, BAR_MS, Candle};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StrategyConfig {
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
            matches!(
                self.kind.as_str(),
                "ema_cross" | "macd" | "bollinger" | "rsi"
            ),
            "未知策略"
        );
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
    pub indicators: BTreeMap<String, f64>,
    pub model: Option<String>,
    pub confidence: Option<f64>,
    pub probabilities: BTreeMap<String, f64>,
    pub input_tokens: u64,
}
impl Decision {
    pub fn wait(source: &str) -> Self {
        Self {
            action: Action::Wait,
            source: source.into(),
            indicators: BTreeMap::new(),
            model: None,
            confidence: None,
            probabilities: BTreeMap::new(),
            input_tokens: 0,
        }
    }
}
pub trait Strategy: Send + Sync {
    fn name(&self) -> &'static str;
    fn evaluate(&self, bars: &[Candle], config: &StrategyConfig) -> Result<Decision>;
}
pub struct IndicatorStrategy(pub &'static str);
pub fn registry() -> BTreeMap<&'static str, Box<dyn Strategy>> {
    ["ema_cross", "macd", "bollinger", "rsi"]
        .into_iter()
        .map(|name| (name, Box::new(IndicatorStrategy(name)) as Box<dyn Strategy>))
        .collect()
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
fn cross(prev: f64, now: f64) -> Action {
    if prev <= 0.0 && now > 0.0 {
        Action::Long
    } else if prev >= 0.0 && now < 0.0 {
        Action::Short
    } else {
        Action::Wait
    }
}
impl Strategy for IndicatorStrategy {
    fn name(&self) -> &'static str {
        self.0
    }
    fn evaluate(&self, bars: &[Candle], c: &StrategyConfig) -> Result<Decision> {
        c.validate()?;
        ensure!(
            bars.len() >= c.slow + c.signal + 2 && bars.len() >= c.period + 2,
            "指标预热不足"
        );
        let close: Vec<f64> = bars.iter().map(|b| b.close).collect();
        let n = close.len() - 1;
        let mut d = Decision::wait(self.name());
        match self.name() {
            "ema_cross" => {
                let a = ema(&close, c.fast);
                let b = ema(&close, c.slow);
                d.action = cross(a[n - 1] - b[n - 1], a[n] - b[n]);
                d.indicators.insert("ema_fast".into(), a[n]);
                d.indicators.insert("ema_slow".into(), b[n]);
            }
            "macd" => {
                let a = ema(&close, c.fast);
                let b = ema(&close, c.slow);
                let line: Vec<f64> = (c.slow - 1..close.len()).map(|i| a[i] - b[i]).collect();
                let sig = ema(&line, c.signal);
                let j = line.len() - 1;
                d.action = cross(line[j - 1] - sig[j - 1], line[j] - sig[j]);
                d.indicators.insert("macd".into(), line[j]);
                d.indicators.insert("signal".into(), sig[j]);
                d.indicators.insert("histogram".into(), line[j] - sig[j]);
            }
            "rsi" => {
                let r = rsi(&close, c.period);
                if r[n - 1] < c.lower && r[n] >= c.lower {
                    d.action = Action::Long;
                } else if r[n - 1] > c.upper && r[n] <= c.upper {
                    d.action = Action::Short;
                }
                d.indicators.insert("rsi".into(), r[n]);
            }
            "bollinger" => {
                let band = |i: usize| {
                    let s = &close[i + 1 - c.period..=i];
                    let mean = s.iter().sum::<f64>() / c.period as f64;
                    let std = (s.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / c.period as f64)
                        .sqrt();
                    (mean - std * c.deviations, mean, mean + std * c.deviations)
                };
                let prev = band(n - 1);
                let cur = band(n);
                if close[n - 1] < prev.0 && close[n] >= cur.0 && close[n] <= cur.2 {
                    d.action = Action::Long;
                } else if close[n - 1] > prev.2 && close[n] <= cur.2 && close[n] >= cur.0 {
                    d.action = Action::Short;
                }
                d.indicators.insert("lower".into(), cur.0);
                d.indicators.insert("middle".into(), cur.1);
                d.indicators.insert("upper".into(), cur.2);
            }
            _ => anyhow::bail!("策略未注册"),
        }
        ensure!(
            d.indicators.values().all(|v| v.is_finite()),
            "指标包含无效值"
        );
        Ok(d)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn flat_rsi_is_neutral() {
        let r = rsi(&vec![10.0; 120], 14);
        assert_eq!(r[119], 50.0);
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
    #[test]
    fn crossing_is_an_event_not_a_direction() {
        assert_eq!(cross(-1., 1.), Action::Long);
        assert_eq!(cross(1., -1.), Action::Short);
        assert_eq!(cross(1., 2.), Action::Wait);
    }
}
