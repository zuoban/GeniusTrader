use crate::strategy::StrategyConfig;
use anyhow::{Result, ensure};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    #[default]
    Paper,
    Testnet,
    Live,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionMode {
    Builtin,
    Jev,
    Hybrid,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    Both,
    Long,
    Short,
}
impl Direction {
    pub fn allows(self, action: crate::domain::Action) -> bool {
        match action {
            crate::domain::Action::Long => self != Self::Short,
            crate::domain::Action::Short => self != Self::Long,
            _ => false,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JevConfig {
    pub model: String,
    pub lookback_bars: usize,
    pub forecast_bars: usize,
    pub min_confidence: f64,
    pub timeout_seconds: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub notional_usdt: Decimal,
    pub profit_usdt: Decimal,
    pub max_positions: usize,
    pub max_exposure_usdt: Decimal,
    pub leverage: u8,
    pub cooldown_bars: u32,
    pub decision_mode: DecisionMode,
    pub allowed_direction: Direction,
    pub maker_fee: Decimal,
    pub taker_fee: Decimal,
    pub paper_balance_usdt: Decimal,
    pub max_spread_bps: Decimal,
    pub signal_max_age_seconds: u64,
    pub strategy: StrategyConfig,
    pub jev: JevConfig,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoinConfig {
    pub notional_usdt: Option<Decimal>,
    pub profit_usdt: Option<Decimal>,
    pub strategy: Option<StrategyConfig>,
    pub decision_mode: Option<DecisionMode>,
    pub allowed_direction: Option<Direction>,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            notional_usdt: Decimal::from(100),
            profit_usdt: Decimal::new(5, 1),
            max_positions: 10,
            max_exposure_usdt: Decimal::from(1000),
            leverage: 2,
            cooldown_bars: 1,
            decision_mode: DecisionMode::Builtin,
            allowed_direction: Direction::Both,
            maker_fee: Decimal::new(2, 4),
            taker_fee: Decimal::new(5, 4),
            paper_balance_usdt: Decimal::from(5000),
            max_spread_bps: Decimal::from(10),
            signal_max_age_seconds: 60,
            strategy: StrategyConfig {
                kind: "ema_cross".into(),
                fast: 9,
                slow: 21,
                signal: 9,
                period: 14,
                lower: 30.0,
                upper: 70.0,
                deviations: 2.0,
            },
            jev: JevConfig {
                model: "jev-latest".into(),
                lookback_bars: 120,
                forecast_bars: 6,
                min_confidence: 0.7,
                timeout_seconds: 5,
            },
        }
    }
}
impl Config {
    pub fn validate(&self) -> Result<()> {
        self.strategy.validate()?;
        ensure!(
            self.notional_usdt > Decimal::ZERO && self.profit_usdt > Decimal::ZERO,
            "金额和净止盈目标必须大于零"
        );
        ensure!(
            self.max_positions > 0
                && self.max_positions <= 50
                && self.max_exposure_usdt >= self.notional_usdt,
            "持仓数量或总敞口上限无效（最多 50 仓）"
        );
        ensure!((1..=20).contains(&self.leverage), "杠杆配置范围为 1–20");
        ensure!(
            self.maker_fee >= Decimal::ZERO
                && self.maker_fee < Decimal::ONE
                && self.taker_fee >= Decimal::ZERO
                && self.taker_fee < Decimal::ONE,
            "费率无效"
        );
        ensure!(
            self.paper_balance_usdt > Decimal::ZERO && self.max_spread_bps > Decimal::ZERO,
            "余额和点差上限必须大于零"
        );
        ensure!(
            (1..=120).contains(&self.signal_max_age_seconds),
            "信号时效范围为 1–120 秒"
        );
        ensure!(
            (100..=500).contains(&self.jev.lookback_bars)
                && (1..=288).contains(&self.jev.forecast_bars),
            "Jev 数据长度或判断窗口无效"
        );
        ensure!(
            self.jev.min_confidence.is_finite()
                && (0.0..=1.0).contains(&self.jev.min_confidence)
                && (1..=30).contains(&self.jev.timeout_seconds),
            "Jev 阈值或超时无效"
        );
        ensure!(!self.jev.model.trim().is_empty(), "模型名称为空");
        ensure!(self.cooldown_bars <= 288, "冷却根数最多 288");
        ensure!(
            self.jev.lookback_bars >= self.strategy.slow + self.strategy.signal + 2
                && self.jev.lookback_bars >= self.strategy.period + 2,
            "回看长度不足以预热策略"
        );
        Ok(())
    }
    pub fn for_coin(&self, coin: &crate::domain::PoolCoin) -> Result<Self> {
        let mut c = self.clone();
        if let Some(o) = &coin.config {
            if let Some(v) = o.notional_usdt {
                c.notional_usdt = v;
            }
            if let Some(v) = o.profit_usdt {
                c.profit_usdt = v;
            }
            if let Some(v) = &o.strategy {
                c.strategy = v.clone();
            }
            if let Some(v) = o.decision_mode {
                c.decision_mode = v;
            }
            if let Some(v) = o.allowed_direction {
                c.allowed_direction = v;
            }
        }
        c.validate()?;
        Ok(c)
    }
}
