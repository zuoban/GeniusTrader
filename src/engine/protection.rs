use super::*;
use crate::exchange::AlgoOrder;

impl Inner {
    pub(super) fn algo_matches(&self, remote: &AlgoOrder) -> bool {
        self.book
            .stop_orders
            .get(&remote.client_algo_id)
            .is_some_and(|s| stop_matches(s, remote))
    }
    pub(super) fn exchange_stops_ready(&self) -> bool {
        self.exchange_stop_blockers().is_empty()
    }
    pub(super) fn exchange_stop_blockers(&self) -> Vec<String> {
        let mut reasons = Vec::new();
        for s in self.book.stop_orders.values() {
            if s.order.status != "NEW" || s.cancel_requested || s.order.settlement_issue.is_some() {
                reasons.push(format!(
                    "{} 交易所止损待确认：{}",
                    s.order.symbol,
                    s.order
                        .settlement_issue
                        .as_deref()
                        .unwrap_or(&s.order.status)
                ));
            }
            if !self.book.positions.contains_key(&s.order.symbol)
                && !self
                    .book
                    .pending
                    .values()
                    .any(|o| o.symbol == s.order.symbol && o.role == OrderRole::Entry)
            {
                reasons.push(format!(
                    "{} 已无本地持仓，止损保护清理等待核对",
                    s.order.symbol
                ));
            }
        }
        for p in self.book.positions.values().filter(|p| {
            p.quantity > Decimal::ZERO && p.stop_loss_usdt > Decimal::ZERO && !p.stop_loss_triggered
        }) {
            if !self.book.stop_orders.values().any(|s| {
                s.order.symbol == p.symbol
                    && s.position_opened_at == p.opened_at
                    && s.order.side == p.side
                    && s.order.quantity == p.quantity
                    && s.order.status == "NEW"
                    && !s.cancel_requested
                    && s.order.settlement_issue.is_none()
            }) {
                reasons.push(format!("{} 交易所止损尚未确认，禁止新增开仓", p.symbol));
            }
        }
        reasons
    }
}

#[cfg(test)]
mod tests;

fn stop_matches(s: &StopOrder, r: &AlgoOrder) -> bool {
    r.algo_id > 0
        && s.algo_id.is_none_or(|id| id == r.algo_id)
        && r.client_algo_id == s.order.client_id
        && r.symbol == s.order.symbol
        && r.side == s.order.side.exit_side()
        && r.position_side == "BOTH"
        && r.order_type == "STOP_MARKET"
        && r.quantity == s.order.quantity
        && r.trigger_price == s.trigger_price
        && r.working_type == "CONTRACT_PRICE"
        && r.reduce_only
        && !r.close_position
        && !r.price_protect
        && s.actual_order_id
            .is_none_or(|id| r.actual_order_id == Some(id))
}

impl Engine {
    pub(super) async fn stop_issue(&self, id: &str, error: &anyhow::Error) -> Result<()> {
        let mut i = self.inner.lock().await;
        if let Some(s) = i.book.stop_orders.get_mut(id) {
            s.order.settlement_issue = Some(error.to_string());
            let symbol = s.order.symbol.clone();
            self.save(&mut i)?;
            drop(i);
            self.event(
                "warn",
                Some(&symbol),
                "交易所止损订单等待确认",
                json!({"client_algo_id":id,"error":error.to_string()}),
            );
        }
        Ok(())
    }

    pub(super) async fn protect_entry_fill(
        &self,
        o: &PendingOrder,
        r: &ExchangeOrder,
        previous: Option<&Position>,
    ) {
        if self.mode == Mode::Paper
            || o.role != OrderRole::Entry
            || o.stop_loss_usdt <= Decimal::ZERO
            || r.executed_qty <= Decimal::ZERO
        {
            return;
        }
        let avg = if r.avg_price > Decimal::ZERO {
            r.avg_price
        } else if r.cum_quote > Decimal::ZERO {
            r.cum_quote / r.executed_qty
        } else {
            return;
        }; // Wait for confirmed trade prices; never invent an entry price.
        let mut p = previous.cloned().unwrap_or_else(|| Position {
            symbol: o.symbol.clone(),
            side: o.side,
            quantity: Decimal::ZERO,
            original_quantity: Decimal::ZERO,
            entry_price: avg,
            opened_at: o.created_at,
            signal_time: o.signal_time,
            target_profit: o.target_profit,
            stop_loss_usdt: o.stop_loss_usdt,
            stop_loss_triggered: false,
            entry_fee: Decimal::ZERO,
            exit_fee: Decimal::ZERO,
            realized_gross: Decimal::ZERO,
            funding_cost: Decimal::ZERO,
            maker_fee: o.maker_fee,
            taker_fee: o.taker_fee,
            tp_price: None,
            tp_client_id: None,
            observed_trade_ids: vec![],
            owned_order_ids: vec![],
            accounting_complete: false,
            leverage: o.leverage,
        });
        p.quantity += r.executed_qty - o.accounted_quantity;
        p.entry_price = avg;
        p.entry_fee = r.executed_qty * avg * o.taker_fee;
        if let Err(error) = self.protect_position(&p).await {
            self.event(
                "warn",
                Some(&o.symbol),
                "开仓成交后的交易所止损保护等待",
                json!({"error":error.to_string()}),
            );
        }
    }

    pub(super) async fn ensure_exchange_stop(&self, symbol: &str) -> Result<()> {
        let p = self.inner.lock().await.book.positions.get(symbol).cloned();
        if let Some(p) = p {
            self.protect_position(&p).await?;
        }
        Ok(())
    }

    pub(super) async fn protect_position(&self, p: &Position) -> Result<()> {
        if self.mode == Mode::Paper
            || p.quantity <= Decimal::ZERO
            || p.stop_loss_usdt <= Decimal::ZERO
            || p.stop_loss_triggered
        {
            return Ok(());
        }
        let (trigger, existing) = {
            let i = self.inner.lock().await;
            ensure!(
                !i.external_positions.contains_key(&p.symbol)
                    && !i.external_orders.contains(&p.symbol),
                "存在账户差异，保留原止损保护并等待核对"
            );
            if i.book.pending.values().any(|o| {
                o.symbol == p.symbol
                    && matches!(o.role, OrderRole::StopLoss | OrderRole::ManualClose)
            }) {
                return Ok(());
            }
            let rule = i.catalog.get(&p.symbol).context("止损交易规则尚未就绪")?;
            ensure!(
                p.quantity >= rule.market_min_qty
                    && p.quantity <= rule.market_max_qty
                    && floor_step(p.quantity, rule.market_step) == p.quantity,
                "止损数量不符合市价交易规则"
            );
            let stops: Vec<_> = i
                .book
                .stop_orders
                .values()
                .filter(|s| s.order.symbol == p.symbol)
                .cloned()
                .collect();
            ensure!(
                stops.iter().all(|s| s.order.status == "NEW"
                    && s.actual_order_id.is_none()
                    && !s.cancel_requested
                    && s.order.settlement_issue.is_none()),
                "原止损委托尚未确认，禁止重复挂单"
            );
            (p.stop_price(rule.tick_size)?, stops)
        };
        let matching = existing.iter().find(|s| {
            s.order.quantity == p.quantity
                && s.position_opened_at == p.opened_at
                && s.order.side == p.side
                && s.trigger_price == trigger
        });
        let keep_id = if let Some(s) = matching {
            s.order.client_id.clone()
        } else {
            let stop = {
                let mut i = self.inner.lock().await;
                i.book.sequence += 1;
                let id = format!("gt-sl-{}-{}", now_ms(), i.book.sequence);
                let stop = StopOrder {
                    order: PendingOrder {
                        client_id: id.clone(),
                        exchange_order_id: None,
                        symbol: p.symbol.clone(),
                        side: p.side,
                        role: OrderRole::StopLoss,
                        quantity: p.quantity,
                        price: None,
                        created_at: now_ms(),
                        signal_time: p.signal_time,
                        target_profit: p.target_profit,
                        stop_loss_usdt: p.stop_loss_usdt,
                        maker_fee: p.maker_fee,
                        taker_fee: p.taker_fee,
                        status: "SUBMITTING".into(),
                        accounted_quantity: Decimal::ZERO,
                        settlement_issue: None,
                        leverage: p.leverage,
                    },
                    algo_id: None,
                    actual_order_id: None,
                    trigger_price: trigger,
                    position_opened_at: p.opened_at,
                    cancel_requested: false,
                };
                i.book.stop_orders.insert(id, stop.clone());
                self.save(&mut i)?; // The intent exists before any network side effect.
                stop
            };
            match self.exchange.submit_stop(&stop).await {
                Ok(report) => {
                    if let Err(error) = self.apply_stop_report(&stop.order.client_id, report).await
                    {
                        self.stop_issue(&stop.order.client_id, &error).await?;
                        return Err(error);
                    }
                }
                Err(error) => {
                    let definite = error
                        .downcast_ref::<ExchangeError>()
                        .is_some_and(|e| !e.uncertain);
                    let mut i = self.inner.lock().await;
                    if definite {
                        i.book.stop_orders.remove(&stop.order.client_id);
                    } else if let Some(s) = i.book.stop_orders.get_mut(&stop.order.client_id) {
                        s.order.status = "UNKNOWN".into();
                        s.order.settlement_issue = Some(error.to_string());
                    }
                    self.save(&mut i)?;
                    drop(i);
                    self.event("error", Some(&p.symbol), "交易所止损提交未确认，保留本地止损兜底",
                        json!({"client_algo_id":stop.order.client_id,"uncertain":!definite,"error":error.to_string()}));
                    return Err(error);
                }
            }
            stop.order.client_id
        };
        {
            let i = self.inner.lock().await;
            if !i
                .book
                .stop_orders
                .get(&keep_id)
                .is_some_and(|s| s.order.status == "NEW")
            {
                ensure!(
                    i.book
                        .pending
                        .values()
                        .any(|o| o.symbol == p.symbol && o.role == OrderRole::StopLoss),
                    "新止损保护尚未确认，保留原保护单"
                );
                return Ok(());
            }
        }
        // Install the replacement first, then cancel the outdated protection.
        // Reduce-only prevents either conditional order from opening a reverse position.
        for old in existing
            .into_iter()
            .filter(|s| s.order.client_id != keep_id)
        {
            self.cancel_one_stop(&old.order.client_id).await?;
        }
        Ok(())
    }

    pub(super) async fn apply_stop_report(&self, id: &str, remote: AlgoOrder) -> Result<()> {
        let s = self.inner.lock().await.book.stop_orders.get(id).cloned();
        let Some(s) = s else {
            return Ok(());
        };
        ensure!(
            stop_matches(&s, &remote),
            "交易所止损回报与持久化委托不一致"
        );
        {
            let mut i = self.inner.lock().await;
            ensure!(
                i.book
                    .positions
                    .get(&s.order.symbol)
                    .is_none_or(|p| p.opened_at == s.position_opened_at),
                "止损保护与当前持仓不属于同一笔交易"
            );
            let current = i.book.stop_orders.get_mut(id).context("止损意图不存在")?;
            current.algo_id = Some(remote.algo_id);
            current.actual_order_id = remote.actual_order_id;
            current.order.status = remote.algo_status.clone();
            current.order.settlement_issue = None;
            self.save(&mut i)?;
        }
        if let Some(actual_id) = remote.actual_order_id {
            // Persist the child link even if querying that order fails.
            let report = self
                .exchange
                .order_by_id(&s.order.symbol, actual_id)
                .await?;
            ensure!(
                report.symbol == s.order.symbol
                    && report.order_id == actual_id
                    && report.orig_qty > Decimal::ZERO
                    && report.orig_qty <= s.order.quantity
                    && !report.client_order_id.is_empty(),
                "止损触发后的成交委托与原保护单不一致"
            );
            let mut child = s.order.clone();
            child.client_id = report.client_order_id.clone();
            child.exchange_order_id = Some(actual_id);
            // A reduce-only child can be smaller after a concurrent take-profit fill.
            child.quantity = report.orig_qty;
            child.price = None;
            child.status = "SUBMITTING".into();
            child.settlement_issue = None;
            let mut i = self.inner.lock().await;
            ensure!(
                i.book
                    .pending
                    .get(&child.client_id)
                    .is_none_or(|o| o.symbol == child.symbol
                        && o.role == OrderRole::StopLoss
                        && o.exchange_order_id == Some(actual_id)),
                "止损成交订单标识冲突"
            );
            i.book
                .pending
                .entry(child.client_id.clone())
                .or_insert(child);
            if let Some(p) = i.book.positions.get_mut(&s.order.symbol) {
                p.stop_loss_triggered = true;
            }
            i.book.stop_orders.remove(id);
            self.save(&mut i)?;
            drop(i);
            self.event("risk", Some(&s.order.symbol), "交易所止损已触发，核对实际平仓成交",
                json!({"client_algo_id":id,"algo_id":remote.algo_id,"order_id":actual_id,"trigger_price":s.trigger_price}));
            return Ok(());
        }
        if matches!(
            remote.algo_status.as_str(),
            "CANCELED" | "EXPIRED" | "REJECTED"
        ) {
            let mut i = self.inner.lock().await;
            i.book.stop_orders.remove(id);
            self.save(&mut i)?;
            drop(i);
            self.event("order", Some(&s.order.symbol), "交易所止损保护单已终止",
                json!({"client_algo_id":id,"status":remote.algo_status,"cancel_requested":s.cancel_requested}));
        } else {
            ensure!(
                remote.algo_status == "NEW",
                "交易所止损已触发但实际平仓订单尚未确认"
            );
            if s.order.status != "NEW" {
                self.event("order", Some(&s.order.symbol), "交易所市价止损已确认挂单",
                    json!({"client_algo_id":id,"algo_id":remote.algo_id,"quantity":s.order.quantity,"trigger_price":s.trigger_price}));
            }
        }
        Ok(())
    }

    pub(super) async fn reconcile_exchange_stops(&self) -> Result<()> {
        let stops: Vec<_> = self
            .inner
            .lock()
            .await
            .book
            .stop_orders
            .values()
            .cloned()
            .collect();
        let reports: Vec<_> = stream::iter(stops)
            .map(|s| async move {
                let report = self.exchange.stop_order(&s).await;
                (s, report)
            })
            .buffered(4)
            .collect()
            .await;
        for (s, report) in reports {
            let result = match report {
                Ok(r) => self.apply_stop_report(&s.order.client_id, r).await,
                Err(e) => Err(e),
            };
            if let Err(error) = result {
                self.stop_issue(&s.order.client_id, &error).await?;
            } else if s.cancel_requested
                && let Err(error) = self.cancel_one_stop(&s.order.client_id).await
            {
                self.stop_issue(&s.order.client_id, &error).await?;
            }
        }
        Ok(())
    }

    pub(super) async fn cancel_one_stop(&self, id: &str) -> Result<()> {
        let stop = {
            let mut i = self.inner.lock().await;
            let Some(s) = i.book.stop_orders.get_mut(id) else {
                return Ok(());
            };
            s.cancel_requested = true;
            let s = s.clone();
            self.save(&mut i)?;
            s
        };
        // Resolve an ambiguous submission/previous cancellation before sending again.
        let remote = self.exchange.stop_order(&stop).await?;
        self.apply_stop_report(id, remote).await?;
        let current = self.inner.lock().await.book.stop_orders.get(id).cloned();
        if let Some(current) = current {
            ensure!(
                current.order.status == "NEW",
                "止损实际成交订单未确认，暂缓其他平仓"
            );
            self.exchange.cancel_stop(&current).await?;
            // A successful DELETE is an acknowledgement; only the query proves the outcome.
            self.apply_stop_report(id, self.exchange.stop_order(&current).await?)
                .await?;
            ensure!(
                !self.inner.lock().await.book.stop_orders.contains_key(id),
                "交易所止损撤销尚未确认"
            );
        }
        Ok(())
    }

    pub(super) async fn cancel_exchange_stops(&self, symbol: &str) -> Result<()> {
        let ids: Vec<_> = self
            .inner
            .lock()
            .await
            .book
            .stop_orders
            .values()
            .filter(|s| s.order.symbol == symbol)
            .map(|s| s.order.client_id.clone())
            .collect();
        for id in ids {
            if let Err(error) = self.cancel_one_stop(&id).await {
                self.stop_issue(&id, &error).await?;
                return Err(error);
            }
        }
        Ok(())
    }

    pub(super) async fn cleanup_exchange_stops(&self) -> Result<()> {
        let symbols: BTreeSet<_> = {
            let i = self.inner.lock().await;
            if i.account_sync_failed
                || i.account_update_pending
                || !(0..15_000).contains(&(now_ms() - i.account_at))
            {
                return Ok(());
            }
            i.book
                .stop_orders
                .values()
                .filter(|s| {
                    i.book
                        .positions
                        .get(&s.order.symbol)
                        .is_none_or(|p| p.quantity == Decimal::ZERO && p.accounting_complete)
                        && !i.external_positions.contains_key(&s.order.symbol)
                        && !i.external_orders.contains(&s.order.symbol)
                        && !i
                            .book
                            .pending
                            .values()
                            .any(|o| o.symbol == s.order.symbol && o.role == OrderRole::Entry)
                })
                .map(|s| s.order.symbol.clone())
                .collect()
        };
        for symbol in symbols {
            if let Err(error) = self.cancel_exchange_stops(&symbol).await {
                self.event(
                    "warn",
                    Some(&symbol),
                    "已平仓持仓的止损保护清理等待",
                    json!({"error":error.to_string()}),
                );
            }
        }
        Ok(())
    }
}
