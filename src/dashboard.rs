//! Read models shared by the dashboard and watchlist. Trading rules stay in Engine.
use crate::{
    domain::{OrderRole, PendingOrder, PoolCoin, Position, Quote, now_ms},
    engine::Snapshot,
    strategy::DecisionRecord,
};
use anyhow::Result;
use rust_decimal::Decimal;
use serde::Serialize;
use std::collections::BTreeMap;

#[derive(Clone, Serialize)]
pub struct PositionView {
    #[serde(flatten)]
    pub position: Position,
    pub mark_price: Option<Decimal>,
    pub unrealized: Option<Decimal>,
    pub quote_stale: bool,
}
#[derive(Clone, Serialize)]
pub struct OrderView {
    #[serde(flatten)]
    pub order: PendingOrder,
    pub order_side: &'static str,
    pub remaining_quantity: Decimal,
}
#[derive(Serialize)]
pub struct WatchlistCard {
    #[serde(flatten)]
    pub coin: PoolCoin,
    pub quote: Option<Quote>,
    pub price: Option<Decimal>,
    pub quote_stale: bool,
    pub notional_usdt: Decimal,
    pub profit_usdt: Decimal,
    pub stop_loss_usdt: Decimal,
    pub position: Option<PositionView>,
    pub orders: Vec<OrderView>,
    pub latest_decision: Option<DecisionRecord>,
    pub external_position: Option<Decimal>,
    pub external_order: bool,
}
#[derive(Serialize)]
pub struct DashboardView {
    pub updated_at: i64,
    pub watchlist: Vec<WatchlistCard>,
    pub position_views: Vec<PositionView>,
    pub open_orders: Vec<OrderView>,
    pub recent_decisions: Vec<DecisionRecord>,
}
pub fn build(
    snapshot: &Snapshot,
    latest: Vec<DecisionRecord>,
    recent_decisions: Vec<DecisionRecord>,
) -> Result<DashboardView> {
    let updated_at = now_ms();
    let stale = |quote: Option<&Quote>| quote.is_none_or(|q| updated_at - q.time >= 15_000);
    let position_views: Vec<_> = snapshot
        .positions
        .iter()
        .map(|p| {
            let quote = snapshot.quotes.get(&p.symbol);
            let mark_price = quote.map(Quote::mid);
            PositionView {
                position: p.clone(),
                mark_price,
                unrealized: mark_price.map(|mark| p.unrealized(mark)),
                quote_stale: stale(quote),
            }
        })
        .collect();
    let mut open_orders: Vec<_> = snapshot
        .pending
        .iter()
        .map(|o| OrderView {
            order: o.clone(),
            order_side: if o.role == OrderRole::Entry {
                o.side.entry_side()
            } else {
                o.side.exit_side()
            },
            remaining_quantity: (o.quantity - o.accounted_quantity).max(Decimal::ZERO),
        })
        .collect();
    open_orders.sort_by_key(|o| {
        (
            std::cmp::Reverse(o.order.created_at),
            o.order.client_id.clone(),
        )
    });
    let latest: BTreeMap<_, _> = latest.into_iter().map(|d| (d.symbol.clone(), d)).collect();
    let watchlist = snapshot
        .pool
        .iter()
        .map(|coin| {
            let config = snapshot.config.for_coin(coin)?;
            let quote = snapshot.quotes.get(&coin.symbol);
            Ok(WatchlistCard {
                coin: coin.clone(),
                quote: quote.cloned(),
                price: quote.map(Quote::mid),
                quote_stale: stale(quote),
                notional_usdt: config.notional_usdt,
                profit_usdt: config.profit_usdt,
                stop_loss_usdt: config.stop_loss_usdt,
                position: position_views
                    .iter()
                    .find(|p| p.position.symbol == coin.symbol)
                    .cloned(),
                orders: open_orders
                    .iter()
                    .filter(|o| o.order.symbol == coin.symbol)
                    .cloned()
                    .collect(),
                latest_decision: latest.get(&coin.symbol).cloned(),
                external_position: snapshot.external_positions.get(&coin.symbol).copied(),
                external_order: snapshot.external_orders.contains(&coin.symbol),
            })
        })
        .collect::<Result<_>>()?;
    Ok(DashboardView {
        updated_at,
        watchlist,
        position_views,
        open_orders,
        recent_decisions,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::{CoinConfig, Config, Mode},
        domain::Action,
        engine::Engine,
        exchange::Binance,
        jev::Jev,
        store::Store,
    };
    use serde_json::json;

    #[tokio::test]
    async fn cards_join_partial_orders_and_positions_without_hiding_removed_symbols() {
        let engine = Engine::new(
            Mode::Paper,
            Store::open(":memory:", Mode::Paper).unwrap(),
            Config::default(),
            Binance::new(Mode::Paper).unwrap(),
            Jev::new().unwrap(),
        )
        .unwrap();
        let position:Position = serde_json::from_value(json!({
            "symbol":"BTCUSDT","side":"SHORT","quantity":"0.4","entry_price":"100","opened_at":1,"signal_time":1,"target_profit":"0.5",
            "entry_fee":"0.01","exit_fee":"0","realized_gross":"0","funding_cost":"0","maker_fee":"0.0002","tp_price":"98","tp_client_id":"tp",
            "observed_trade_ids":[],"owned_order_ids":[],"accounting_complete":true,"original_quantity":"1","leverage":3
        })).unwrap();
        let order:PendingOrder=serde_json::from_value(json!({
            "client_id":"tp","symbol":"BTCUSDT","side":"SHORT","role":"take_profit","quantity":"1","price":"98","created_at":1,"signal_time":1,
            "target_profit":"0.5","maker_fee":"0.0002","taker_fee":"0.0005","status":"PARTIALLY_FILLED","accounted_quantity":"0.6","leverage":3
        })).unwrap();
        {
            let mut inner = engine.inner.lock().await;
            inner.book.pool.insert(
                "BTCUSDT".into(),
                PoolCoin {
                    symbol: "BTCUSDT".into(),
                    enabled: false,
                    config: Some(CoinConfig {
                        notional_usdt: Some(Decimal::from(250)),
                        profit_usdt: Some(Decimal::ONE),
                        stop_loss_usdt: None,
                        strategy: None,
                        decision_mode: None,
                        allowed_direction: None,
                    }),
                },
            );
            inner.book.pool.insert(
                "ETHUSDT".into(),
                PoolCoin {
                    symbol: "ETHUSDT".into(),
                    enabled: true,
                    config: None,
                },
            );
            inner
                .book
                .positions
                .insert(position.symbol.clone(), position.clone());
            let mut removed = position.clone();
            removed.symbol = "REMOVEDUSDT".into();
            removed.side = Action::Long;
            inner.book.positions.insert(removed.symbol.clone(), removed);
            inner
                .book
                .pending
                .insert(order.client_id.clone(), order.clone());
            for (role, id) in [
                (OrderRole::Entry, "entry"),
                (OrderRole::ManualClose, "close"),
            ] {
                let mut other = order.clone();
                other.role = role;
                other.client_id = id.into();
                other.created_at = 2;
                inner.book.pending.insert(id.into(), other);
            }
            inner.quotes.insert(
                "BTCUSDT".into(),
                Quote {
                    bid: Decimal::from(95),
                    ask: Decimal::from(97),
                    bid_qty: Decimal::ONE,
                    ask_qty: Decimal::ONE,
                    time: now_ms(),
                    exchange_time: None,
                },
            );
            inner
                .external_positions
                .insert("ETHUSDT".into(), Decimal::NEGATIVE_ONE);
            inner.external_orders.push("ETHUSDT".into());
        }
        let view = build(&engine.snapshot().await, vec![], vec![]).unwrap();
        let btc = &view.watchlist[0];
        assert!(!btc.coin.enabled);
        assert_eq!(btc.notional_usdt, Decimal::from(250));
        assert_eq!(btc.profit_usdt, Decimal::ONE);
        assert_eq!(btc.price, Some(Decimal::from(96)));
        assert!(!btc.quote_stale);
        assert_eq!(
            btc.position.as_ref().unwrap().unrealized,
            Some(Decimal::new(16, 1))
        );
        assert_eq!(btc.orders.len(), 3);
        assert_eq!(view.open_orders.last().unwrap().order.client_id, "tp");
        for order in &btc.orders {
            assert_eq!(order.remaining_quantity, Decimal::new(4, 1));
            assert_eq!(
                order.order_side,
                if order.order.role == OrderRole::Entry {
                    "SELL"
                } else {
                    "BUY"
                }
            );
        }
        assert_eq!(view.position_views.len(), 2);
        let removed = view
            .position_views
            .iter()
            .find(|p| p.position.symbol == "REMOVEDUSDT")
            .unwrap();
        assert!(
            removed.mark_price.is_none() && removed.unrealized.is_none() && removed.quote_stale
        );
        assert_eq!(
            view.watchlist[1].external_position,
            Some(Decimal::NEGATIVE_ONE)
        );
        assert!(view.watchlist[1].external_order);
        engine
            .inner
            .lock()
            .await
            .quotes
            .get_mut("BTCUSDT")
            .unwrap()
            .time = now_ms() - 20_000;
        assert!(
            build(&engine.snapshot().await, vec![], vec![])
                .unwrap()
                .watchlist[0]
                .quote_stale
        );
    }
}
