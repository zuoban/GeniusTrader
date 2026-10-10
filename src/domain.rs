use anyhow::{Result, ensure};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const BAR_MS: i64 = 300_000;
pub fn settlement_asset(symbol: &str) -> Option<&'static str> {
    ["USDT", "USDC"]
        .into_iter()
        .find(|asset| symbol.len() > asset.len() && symbol.ends_with(asset))
}
pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Action {
    Long,
    Short,
    Wait,
}
impl Action {
    pub fn sign(self) -> Decimal {
        if self == Self::Long {
            Decimal::ONE
        } else {
            -Decimal::ONE
        }
    }
    pub fn entry_side(self) -> &'static str {
        if self == Self::Long { "BUY" } else { "SELL" }
    }
    pub fn exit_side(self) -> &'static str {
        if self == Self::Long { "SELL" } else { "BUY" }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Candle {
    pub open_time: i64,
    pub close_time: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolInfo {
    pub symbol: String,
    pub quote_volume: Decimal,
    pub last_price: Decimal,
    pub tick_size: Decimal,
    pub step_size: Decimal,
    pub market_step: Decimal,
    pub min_qty: Decimal,
    pub market_min_qty: Decimal,
    pub min_notional: Decimal,
    pub max_qty: Decimal,
    pub market_max_qty: Decimal,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Quote {
    pub bid: Decimal,
    pub ask: Decimal,
    pub bid_qty: Decimal,
    pub ask_qty: Decimal,
    /// Local receipt time, used to detect interrupted polling.
    pub time: i64,
    /// Exchange transaction time, used to identify repeated book snapshots.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exchange_time: Option<i64>,
}
impl Quote {
    pub fn mid(&self) -> Decimal {
        (self.bid + self.ask) / Decimal::from(2)
    }
    pub fn spread_bps(&self) -> Decimal {
        if self.mid() > Decimal::ZERO {
            (self.ask - self.bid) / self.mid() * Decimal::from(10000)
        } else {
            Decimal::MAX
        }
    }
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.bid > Decimal::ZERO && self.ask >= self.bid && self.time > 0,
            "invalid quote"
        );
        Ok(())
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolCoin {
    pub symbol: String,
    pub enabled: bool,
    pub config: Option<crate::config::CoinConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub symbol: String,
    pub side: Action,
    pub quantity: Decimal,
    pub entry_price: Decimal,
    pub opened_at: i64,
    pub signal_time: i64,
    pub target_profit: Decimal,
    #[serde(default)]
    pub stop_loss_usdt: Decimal,
    #[serde(default)]
    pub stop_loss_triggered: bool,
    pub entry_fee: Decimal,
    pub exit_fee: Decimal,
    pub realized_gross: Decimal,
    pub funding_cost: Decimal,
    pub maker_fee: Decimal,
    #[serde(default = "default_taker_fee")]
    pub taker_fee: Decimal,
    pub tp_price: Option<Decimal>,
    pub tp_client_id: Option<String>,
    pub observed_trade_ids: Vec<i64>,
    pub owned_order_ids: Vec<i64>,
    pub accounting_complete: bool,
    pub original_quantity: Decimal,
    #[serde(default = "default_leverage")]
    pub leverage: u8,
}
fn default_leverage() -> u8 {
    2
}
fn default_taker_fee() -> Decimal {
    Decimal::new(5, 4)
}
impl Position {
    /// Exact inverse of net PnL, including closing commission at the exit price.
    pub fn target_price(&self, tick: Decimal) -> Result<Decimal> {
        ensure!(
            self.quantity > Decimal::ZERO && tick > Decimal::ZERO,
            "invalid quantity/tick"
        );
        let needed = self.target_profit + self.entry_fee + self.exit_fee + self.funding_cost
            - self.realized_gross;
        let raw = match self.side {
            Action::Long => {
                (self.quantity * self.entry_price + needed)
                    / (self.quantity * (Decimal::ONE - self.maker_fee))
            }
            Action::Short => {
                (self.quantity * self.entry_price - needed)
                    / (self.quantity * (Decimal::ONE + self.maker_fee))
            }
            Action::Wait => anyhow::bail!("WAIT is not a position"),
        };
        ensure!(raw > Decimal::ZERO, "target price is not positive");
        Ok(if self.side == Action::Long {
            ceil_step(raw, tick)
        } else {
            floor_step(raw, tick)
        })
    }
    pub fn net_realized(&self) -> Decimal {
        self.realized_gross - self.entry_fee - self.exit_fee - self.funding_cost
    }
    pub fn unrealized(&self, mark: Decimal) -> Decimal {
        self.side.sign() * self.quantity * (mark - self.entry_price)
    }
    pub fn estimated_exit_net(&self, price: Decimal) -> Decimal {
        self.net_realized() + self.unrealized(price) - self.quantity * price * self.taker_fee
    }
}
pub fn floor_step(v: Decimal, step: Decimal) -> Decimal {
    (v / step).floor() * step
}
pub fn ceil_step(v: Decimal, step: Decimal) -> Decimal {
    (v / step).ceil() * step
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderRole {
    Entry,
    TakeProfit,
    StopLoss,
    ManualClose,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingOrder {
    pub client_id: String,
    pub symbol: String,
    pub side: Action,
    pub role: OrderRole,
    pub quantity: Decimal,
    pub price: Option<Decimal>,
    pub created_at: i64,
    pub signal_time: i64,
    pub target_profit: Decimal,
    #[serde(default)]
    pub stop_loss_usdt: Decimal,
    pub maker_fee: Decimal,
    pub taker_fee: Decimal,
    pub status: String,
    pub accounted_quantity: Decimal,
    #[serde(default = "default_leverage")]
    pub leverage: u8,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Book {
    pub pool: BTreeMap<String, PoolCoin>,
    pub positions: BTreeMap<String, Position>,
    pub pending: BTreeMap<String, PendingOrder>,
    pub last_bar: BTreeMap<String, i64>,
    pub cooldown_until: BTreeMap<String, i64>,
    pub closed_trades: Vec<ClosedTrade>,
    pub paper_cash: Decimal,
    pub funding_cursor: i64,
    #[serde(default)]
    pub funding_last_poll: i64,
    pub funding_ids: Vec<String>,
    pub sequence: u64,
    #[serde(default)]
    pub paper_match_times: BTreeMap<String, i64>,
    #[serde(default)]
    pub paper_match_exchange_times: BTreeMap<String, i64>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClosedTrade {
    pub symbol: String,
    pub side: Action,
    pub opened_at: i64,
    pub closed_at: i64,
    pub net_profit: Decimal,
    pub accounting_complete: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub time: i64,
    pub level: String,
    pub symbol: Option<String>,
    pub message: String,
    pub detail: serde_json::Value,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }
    fn position(side: Action) -> Position {
        Position {
            symbol: "XUSDT".into(),
            side,
            quantity: d("1"),
            entry_price: d("100"),
            opened_at: 0,
            signal_time: 0,
            target_profit: d("0.5"),
            stop_loss_usdt: Decimal::ZERO,
            stop_loss_triggered: false,
            entry_fee: d("0.05"),
            exit_fee: Decimal::ZERO,
            realized_gross: Decimal::ZERO,
            funding_cost: Decimal::ZERO,
            maker_fee: d("0.0002"),
            taker_fee: d("0.0005"),
            tp_price: None,
            tp_client_id: None,
            observed_trade_ids: vec![],
            owned_order_ids: vec![],
            accounting_complete: true,
            original_quantity: d("1"),
            leverage: 2,
        }
    }
    #[test]
    fn long_short_targets_cover_fees_after_tick_rounding() {
        for side in [Action::Long, Action::Short] {
            let p = position(side);
            let exit = p.target_price(d("0.01")).unwrap();
            let net = side.sign() * (exit - p.entry_price) * p.quantity
                - p.entry_fee
                - exit * p.quantity * p.maker_fee;
            assert!(net >= p.target_profit);
        }
    }
    #[test]
    fn partial_exit_and_paid_funding_are_included() {
        let mut p = position(Action::Long);
        p.quantity = d("0.4");
        p.realized_gross = d("0.3");
        p.exit_fee = d("0.012");
        p.funding_cost = d("0.07");
        let exit = p.target_price(d("0.01")).unwrap();
        let net = p.realized_gross + (exit - p.entry_price) * p.quantity
            - p.entry_fee
            - p.exit_fee
            - p.funding_cost
            - exit * p.quantity * p.maker_fee;
        assert!(net >= p.target_profit);
    }
    #[test]
    fn estimated_market_exit_includes_partial_realizations_and_all_costs() {
        for (side, price, expected) in [
            (Action::Long, d("98"), d("-0.6516")),
            (Action::Short, d("102"), d("-0.6524")),
        ] {
            let mut p = position(side);
            p.quantity = d("0.4");
            p.realized_gross = d("0.3");
            p.exit_fee = d("0.012");
            p.funding_cost = d("0.07");
            assert_eq!(p.estimated_exit_net(price), expected);
            p.funding_cost = d("-0.07");
            assert_eq!(p.estimated_exit_net(price), expected + d("0.14"));
        }
    }
    #[test]
    fn old_positions_restore_with_stop_loss_disabled() {
        let mut value = serde_json::to_value(position(Action::Long)).unwrap();
        for field in ["stop_loss_usdt", "stop_loss_triggered", "taker_fee"] {
            value.as_object_mut().unwrap().remove(field);
        }
        let p: Position = serde_json::from_value(value).unwrap();
        assert_eq!(p.stop_loss_usdt, Decimal::ZERO);
        assert!(!p.stop_loss_triggered);
        assert_eq!(p.taker_fee, d("0.0005"));
    }
}
