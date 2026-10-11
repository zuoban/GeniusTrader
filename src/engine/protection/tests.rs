use super::super::tests::{config, d, entry, seed};
use super::*;
use axum::{Json, Router, extract::Query, http::StatusCode, routing::get};
use std::{collections::HashMap, sync::Mutex as StdMutex};

#[derive(Default)]
struct Mock {
    stops: BTreeMap<String, Value>,
    posts: usize,
    market_posts: usize,
    actions: Vec<String>,
    lost_submit: bool,
    reject_submit: bool,
    trigger_on_cancel: bool,
    lost_cancel: bool,
    tp_canceled: bool,
    fee_reads: usize,
    child_quantity: Option<Decimal>,
}
fn lookup(m: &Mock, q: &HashMap<String, String>) -> Option<Value> {
    m.stops
        .values()
        .find(|s| {
            q.get("clientAlgoId")
                .is_some_and(|id| id == s["clientAlgoId"].as_str().unwrap())
                || q.get("algoId")
                    .is_some_and(|id| id == &s["algoId"].as_i64().unwrap().to_string())
        })
        .cloned()
}
fn child() -> Value {
    json!({"symbol":"XUSDT","clientOrderId":"exchange-stop-child","orderId":55,
        "status":"FILLED","executedQty":"1","origQty":"1","avgPrice":"89","cumQuote":"89"})
}
fn filled_entry() -> Value {
    json!({"symbol":"XUSDT","clientOrderId":"test-entry","orderId":1,
        "status":"FILLED","executedQty":"1","origQty":"1","avgPrice":"100","cumQuote":"100"})
}
fn tp(canceled: bool) -> Value {
    json!({"symbol":"XUSDT","clientOrderId":"test-tp","orderId":42,
        "status":if canceled {"CANCELED"} else {"NEW"},"executedQty":"0","origQty":"1"})
}
async fn exchange(m: Arc<StdMutex<Mock>>) -> (String, tokio::task::JoinHandle<()>) {
    let reads = m.clone();
    let posts = m.clone();
    let cancels = m.clone();
    let open = m.clone();
    let orders = m.clone();
    let market = m.clone();
    let tp_cancel = m.clone();
    let trades = m.clone();
    let balances = m.clone();
    let router = Router::new()
        .route("/fapi/v1/algoOrder", get(move |Query(q):Query<HashMap<String,String>>| {
            let m=reads.clone(); async move {
                match lookup(&m.lock().unwrap(), &q) {
                    Some(v)=>(StatusCode::OK,Json(v)),
                    None=>(StatusCode::BAD_REQUEST,Json(json!({"code":-2013,"msg":"not found"}))),
                }
            }
        }).post(move |body:String| {
            let m=posts.clone(); async move {
                let q:HashMap<_,_>=url::form_urlencoded::parse(body.as_bytes()).into_owned().collect();
                assert_eq!(q["algoType"],"CONDITIONAL"); assert_eq!(q["type"],"STOP_MARKET");
                assert_eq!(q["positionSide"],"BOTH"); assert_eq!(q["reduceOnly"],"true");
                assert_eq!(q["workingType"],"CONTRACT_PRICE"); assert_eq!(q["priceProtect"],"false");
                assert!(!q.contains_key("price") && !q.contains_key("closePosition"));
                let mut m=m.lock().unwrap(); m.posts+=1;
                let id=m.posts as i64; m.actions.push(format!("post-{id}"));
                if m.reject_submit { return (StatusCode::BAD_REQUEST,Json(json!({"code":-2021,"msg":"would trigger"}))); }
                let v=json!({"algoId":id,"clientAlgoId":q["clientAlgoId"],"symbol":q["symbol"],
                    "side":q["side"],"positionSide":"BOTH","orderType":"STOP_MARKET",
                    "quantity":q["quantity"],"triggerPrice":q["triggerPrice"],"algoStatus":"NEW",
                    "workingType":"CONTRACT_PRICE","reduceOnly":true,"closePosition":false,
                    "priceProtect":false,"actualOrderId":""});
                m.stops.insert(q["clientAlgoId"].clone(),v.clone());
                if m.lost_submit { (StatusCode::SERVICE_UNAVAILABLE,Json(json!({"code":-1007,"msg":"unknown"}))) }
                else { (StatusCode::OK,Json(v)) }
            }
        }).delete(move |Query(q):Query<HashMap<String,String>>| {
            let m=cancels.clone(); async move {
                let mut m=m.lock().unwrap(); let mut s=lookup(&m,&q).unwrap();
                let id=s["clientAlgoId"].as_str().unwrap().to_string();
                m.actions.push(format!("delete-{}",s["algoId"]));
                if m.trigger_on_cancel { s["algoStatus"]=json!("FINISHED"); s["actualOrderId"]=json!("55"); }
                else { s["algoStatus"]=json!("CANCELED"); }
                m.stops.insert(id.clone(),s.clone());
                if m.lost_cancel { (StatusCode::SERVICE_UNAVAILABLE,Json(json!({"code":-1007,"msg":"unknown"}))) }
                else { (StatusCode::OK,Json(json!({"algoId":s["algoId"],"clientAlgoId":id,"code":"200","msg":"success"}))) }
            }
        }))
        .route("/fapi/v1/openAlgoOrders", get(move || { let m=open.clone(); async move {
            Json(m.lock().unwrap().stops.values().filter(|s| s["algoStatus"]=="NEW").cloned().collect::<Vec<_>>())
        }}))
        .route("/fapi/v1/order", get(move |Query(q):Query<HashMap<String,String>>| {let m=orders.clone();async move {
            Json(if q.get("orderId").is_some_and(|id|id=="55") {
                let mut report=child();
                if let Some(qty)=m.lock().unwrap().child_quantity {
                    report["origQty"]=json!(qty.to_string());
                    report["executedQty"]=json!(qty.to_string());
                    report["cumQuote"]=json!((qty*d("89")).to_string());
                }
                report
            }
                else if q.get("orderId").is_some_and(|id|id=="1") || q.get("origClientOrderId").is_some_and(|id|id=="test-entry") {filled_entry()}
                else {tp(m.lock().unwrap().tp_canceled)})
        }}).delete(move || {let m=tp_cancel.clone();async move {m.lock().unwrap().tp_canceled=true;Json(tp(true))}})
        .post(move || {let m=market.clone();async move {m.lock().unwrap().market_posts+=1;
            (StatusCode::SERVICE_UNAVAILABLE,Json(json!({"code":-1007,"msg":"unknown"})))
        }}))
        .route("/fapi/v1/userTrades", get(move |Query(q):Query<HashMap<String,String>>| {let m=trades.clone();async move {
            let mut m=m.lock().unwrap();m.fee_reads+=1;
            if q["orderId"]=="1" {
                assert!(m.posts>0,"stop must be submitted before fee reads");
                Json(json!([{"id":1,"orderId":1,"qty":"1","price":"100","commission":"0.05",
                    "commissionAsset":"USDT","realizedPnl":"0","side":"BUY","time":now_ms()}]))
            } else {
                let qty=m.child_quantity.unwrap_or(Decimal::ONE);
                Json(json!([{"id":2,"orderId":55,"qty":qty.to_string(),"price":"89","commission":(qty*d("0.0445")).to_string(),
                    "commissionAsset":"USDT","realizedPnl":(qty*d("-11")).to_string(),"side":"SELL","time":now_ms()}]))
            }
        }}))
        .route("/fapi/v3/positionRisk", get(|| async {Json(json!([{"symbol":"XUSDT",
            "positionAmt":"1","entryPrice":"100","positionSide":"BOTH"}]))}))
        .route("/fapi/v1/openOrders", get(|| async {Json(json!([]))}))
        .route("/fapi/v2/account", get(move || {let _m=balances.clone();async move {Json(json!({
            "multiAssetsMargin":false,"availableBalance":"1000",
            "assets":[{"asset":"USDT","availableBalance":"1000"}]}))}}));
    crate::test_support::mock_server(router).await
}
async fn engine(url: String) -> Arc<Engine> {
    let e = Engine::new(
        Mode::Testnet,
        Store::open(":memory:", Mode::Testnet).unwrap(),
        config(),
        Binance::mock(url),
        Jev::new().unwrap(),
    )
    .unwrap();
    seed(&e).await;
    e.inner.lock().await.account_at = now_ms();
    e
}
async fn positioned(e: &Engine) {
    let mut o = entry(Action::Long);
    o.stop_loss_usdt = d("10");
    e.paper_entry(o).await.unwrap();
}

#[tokio::test]
async fn filled_entry_is_protected_before_fee_reads_and_survives_restart() {
    let m = Arc::new(StdMutex::new(Mock::default()));
    let (url, server) = exchange(m.clone()).await;
    let e = engine(url.clone()).await;
    let mut o = entry(Action::Long);
    o.stop_loss_usdt = d("10");
    e.inner
        .lock()
        .await
        .book
        .pending
        .insert(o.client_id.clone(), o.clone());
    let r = serde_json::from_value(
        json!({"symbol":"XUSDT","clientOrderId":o.client_id,"orderId":1,
        "status":"FILLED","executedQty":"1","origQty":"1","avgPrice":"100","cumQuote":"100"}),
    )
    .unwrap();
    e.apply_order(r).await.unwrap();
    let (_, book) = e.store.load().unwrap().unwrap();
    assert_eq!(book.stop_orders.len(), 1);
    assert_eq!(
        book.stop_orders.values().next().unwrap().order.status,
        "NEW"
    );
    assert_eq!(
        book.stop_orders.values().next().unwrap().trigger_price,
        d("90.1")
    );
    assert!(book.positions["XUSDT"].accounting_complete);
    assert_eq!(m.lock().unwrap().posts, 1);
    let restored = Engine::new(
        Mode::Testnet,
        e.store.clone(),
        config(),
        Binance::mock(url),
        Jev::new().unwrap(),
    )
    .unwrap();
    seed(&restored).await;
    restored.reconcile_exchange_stops().await.unwrap();
    restored.ensure_exchange_stop("XUSDT").await.unwrap();
    assert_eq!(m.lock().unwrap().posts, 1);
    let dashboard = crate::dashboard::build(&restored.snapshot().await, vec![], vec![]).unwrap();
    assert_eq!(dashboard.open_orders[0].trigger_price, Some(d("90.1")));
    assert_eq!(
        dashboard.position_views[0].stop_order_status.as_deref(),
        Some("NEW")
    );
    server.abort();
}

#[tokio::test]
async fn unknown_submission_is_queried_after_restart_and_never_repeated() {
    let m = Arc::new(StdMutex::new(Mock {
        lost_submit: true,
        ..Default::default()
    }));
    let (url, server) = exchange(m.clone()).await;
    let e = engine(url.clone()).await;
    positioned(&e).await;
    assert!(e.ensure_exchange_stop("XUSDT").await.is_err());
    assert!(e.ensure_exchange_stop("XUSDT").await.is_err());
    assert_eq!(m.lock().unwrap().posts, 1);
    assert!(!e.inner.lock().await.exchange_stops_ready());
    let restored = Engine::new(
        Mode::Testnet,
        e.store.clone(),
        config(),
        Binance::mock(url),
        Jev::new().unwrap(),
    )
    .unwrap();
    seed(&restored).await;
    restored.reconcile_exchange_stops().await.unwrap();
    restored.ensure_exchange_stop("XUSDT").await.unwrap();
    assert!(restored.inner.lock().await.exchange_stops_ready());
    assert_eq!(m.lock().unwrap().posts, 1);
    server.abort();
}

#[tokio::test]
async fn replacements_are_installed_before_old_protection_is_canceled() {
    let m = Arc::new(StdMutex::new(Mock::default()));
    let (url, server) = exchange(m.clone()).await;
    let e = engine(url).await;
    positioned(&e).await;
    e.ensure_exchange_stop("XUSDT").await.unwrap();
    {
        let mut i = e.inner.lock().await;
        let p = i.book.positions.get_mut("XUSDT").unwrap();
        p.quantity = d("0.5");
        p.realized_gross = d("0.3");
        p.exit_fee = d("0.01");
    }
    e.ensure_exchange_stop("XUSDT").await.unwrap();
    assert_eq!(
        m.lock().unwrap().actions,
        vec!["post-1", "post-2", "delete-1"]
    );
    let i = e.inner.lock().await;
    assert_eq!(i.book.stop_orders.len(), 1);
    assert_eq!(
        i.book.stop_orders.values().next().unwrap().order.quantity,
        d("0.5")
    );
    assert!(i.exchange_stops_ready());
    server.abort();
}

#[tokio::test]
async fn trigger_push_links_the_actual_order_and_cleans_remaining_take_profit() {
    let m = Arc::new(StdMutex::new(Mock::default()));
    let (url, server) = exchange(m.clone()).await;
    let e = engine(url).await;
    positioned(&e).await;
    e.ensure_exchange_stop("XUSDT").await.unwrap();
    {
        let mut i = e.inner.lock().await;
        let mut o = entry(Action::Long);
        o.client_id = "test-tp".into();
        o.exchange_order_id = Some(42);
        o.role = OrderRole::TakeProfit;
        o.status = "NEW".into();
        o.price = Some(d("101"));
        i.book.pending.insert(o.client_id.clone(), o);
        let p = i.book.positions.get_mut("XUSDT").unwrap();
        p.tp_client_id = Some("test-tp".into());
        p.tp_price = Some(d("101"));
    }
    {
        let mut m = m.lock().unwrap();
        let s = m.stops.values_mut().next().unwrap();
        s["algoStatus"] = json!("FINISHED");
        s["actualOrderId"] = json!("55");
    }
    let event =
        crate::user_stream::parse_event(json!({"e":"ORDER_TRADE_UPDATE","E":now_ms(),"T":now_ms(),
        "o":{"s":"XUSDT","c":"exchange-stop-child","i":55,"q":"1","z":"1","ap":"89",
            "X":"FILLED","S":"SELL","ps":"BOTH"}}))
        .unwrap();
    e.handle_user_event(event).await.unwrap();
    {
        let mut i = e.inner.lock().await;
        assert!(i.book.positions["XUSDT"].stop_loss_triggered);
        assert_eq!(i.book.positions["XUSDT"].quantity, Decimal::ZERO);
        assert!(i.external_orders.is_empty());
        i.account_update_pending = false;
        i.account_at = now_ms();
    }
    e.cancel_closed_position_orders().await.unwrap();
    e.cleanup_exchange_stops().await.unwrap();
    e.finalize_closed().await.unwrap();
    let i = e.inner.lock().await;
    assert!(
        i.book.positions.is_empty() && i.book.pending.is_empty() && i.book.stop_orders.is_empty()
    );
    assert_eq!(i.book.closed_trades[0].net_profit, d("-11.0945"));
    assert_eq!(m.lock().unwrap().market_posts, 0);
    assert!(m.lock().unwrap().tp_canceled);
    server.abort();
}

#[tokio::test]
async fn triggering_during_manual_cancellation_never_submits_a_second_exit() {
    let m = Arc::new(StdMutex::new(Mock {
        trigger_on_cancel: true,
        ..Default::default()
    }));
    let (url, server) = exchange(m.clone()).await;
    let e = engine(url).await;
    positioned(&e).await;
    e.ensure_exchange_stop("XUSDT").await.unwrap();
    assert!(
        e.manual_close("XUSDT")
            .await
            .unwrap_err()
            .to_string()
            .contains("已触发")
    );
    assert_eq!(m.lock().unwrap().market_posts, 0);
    assert_eq!(e.inner.lock().await.book.pending.len(), 1);
    e.reconcile_pending().await.unwrap();
    e.finalize_closed().await.unwrap();
    assert!(e.inner.lock().await.book.positions.is_empty());
    server.abort();
}

#[tokio::test]
async fn ambiguous_cancellation_survives_restart_until_confirmed() {
    let m = Arc::new(StdMutex::new(Mock {
        lost_cancel: true,
        ..Default::default()
    }));
    let (url, server) = exchange(m.clone()).await;
    let e = engine(url.clone()).await;
    positioned(&e).await;
    e.ensure_exchange_stop("XUSDT").await.unwrap();
    assert!(e.cancel_exchange_stops("XUSDT").await.is_err());
    assert!(
        e.store
            .load()
            .unwrap()
            .unwrap()
            .1
            .stop_orders
            .values()
            .next()
            .unwrap()
            .cancel_requested
    );
    let restored = Engine::new(
        Mode::Testnet,
        e.store.clone(),
        config(),
        Binance::mock(url),
        Jev::new().unwrap(),
    )
    .unwrap();
    restored.reconcile_exchange_stops().await.unwrap();
    assert!(restored.inner.lock().await.book.stop_orders.is_empty());
    assert_eq!(m.lock().unwrap().actions, vec!["post-1", "delete-1"]);
    server.abort();
}

#[tokio::test]
async fn native_stop_does_not_wait_for_quotes_or_local_polling_to_trigger() {
    let m = Arc::new(StdMutex::new(Mock::default()));
    let (url, server) = exchange(m.clone()).await;
    let e = engine(url).await;
    positioned(&e).await;
    e.ensure_exchange_stop("XUSDT").await.unwrap();
    e.inner.lock().await.quotes.clear();
    assert!(
        !e.ensure_stop_loss("XUSDT", ExitCheck::Account)
            .await
            .unwrap()
    );
    {
        let mut m = m.lock().unwrap();
        let s = m.stops.values_mut().next().unwrap();
        s["algoStatus"] = json!("FINISHED");
        s["actualOrderId"] = json!(55);
    }
    e.reconcile_exchange_stops().await.unwrap();
    e.reconcile_pending().await.unwrap();
    e.finalize_closed().await.unwrap();
    assert!(e.inner.lock().await.book.positions.is_empty());
    assert_eq!(m.lock().unwrap().market_posts, 0);
    server.abort();
}

#[tokio::test]
async fn account_reconciliation_recognizes_owned_stops_but_preserves_external_algos() {
    let m = Arc::new(StdMutex::new(Mock::default()));
    let (url, server) = exchange(m.clone()).await;
    let e = engine(url).await;
    positioned(&e).await;
    e.ensure_exchange_stop("XUSDT").await.unwrap();
    assert!(e.sync_account().await.unwrap());
    assert!(e.inner.lock().await.external_orders.is_empty());
    {
        let mut m = m.lock().unwrap();
        let mut s = m.stops.values().next().unwrap().clone();
        s["clientAlgoId"] = json!("manual-protection");
        s["algoId"] = json!(999);
        m.stops.insert("manual-protection".into(), s);
    }
    assert!(e.sync_account().await.unwrap());
    assert!(
        e.inner
            .lock()
            .await
            .external_orders
            .contains(&"XUSDT".into())
    );
    assert!(e.ensure_exchange_stop("XUSDT").await.is_err());
    assert_eq!(m.lock().unwrap().posts, 1);
    assert_eq!(m.lock().unwrap().actions.len(), 1);
    server.abort();
}

#[tokio::test]
async fn rejected_or_unconfirmed_protection_blocks_new_entries() {
    let m = Arc::new(StdMutex::new(Mock {
        reject_submit: true,
        ..Default::default()
    }));
    let (url, server) = exchange(m.clone()).await;
    let e = engine(url).await;
    positioned(&e).await;
    assert!(e.ensure_exchange_stop("XUSDT").await.is_err());
    assert!(e.inner.lock().await.book.stop_orders.is_empty());
    assert!(!e.inner.lock().await.entry_data_ready());
    assert!(
        e.snapshot()
            .await
            .entry_blockers
            .iter()
            .any(|s| s.contains("止损尚未确认"))
    );
    server.abort();
}

#[tokio::test]
async fn stop_triggered_before_entry_accounting_recovers_without_losing_exit_fills() {
    let m = Arc::new(StdMutex::new(Mock::default()));
    let (url, server) = exchange(m.clone()).await;
    let e = engine(url.clone()).await;
    let mut o = entry(Action::Long);
    o.stop_loss_usdt = d("10");
    {
        let mut i = e.inner.lock().await;
        i.book.pending.insert(o.client_id.clone(), o.clone());
        e.save(&mut i).unwrap();
    }
    e.protect_entry_fill(&o, &serde_json::from_value(filled_entry()).unwrap(), None)
        .await;
    assert!(e.inner.lock().await.book.positions.is_empty());
    {
        let mut m = m.lock().unwrap();
        let s = m.stops.values_mut().next().unwrap();
        s["algoStatus"] = json!("FINISHED");
        s["actualOrderId"] = json!("55");
    }
    let restored = Engine::new(
        Mode::Testnet,
        e.store.clone(),
        config(),
        Binance::mock(url),
        Jev::new().unwrap(),
    )
    .unwrap();
    seed(&restored).await;
    restored.reconcile_exchange_stops().await.unwrap();
    // The child sorts before the entry. Retain it until the entry is accounted.
    restored.reconcile_pending().await.unwrap();
    {
        let i = restored.inner.lock().await;
        assert!(i.book.positions["XUSDT"].stop_loss_triggered);
        assert_eq!(i.book.positions["XUSDT"].quantity, Decimal::ONE);
        assert!(i.book.pending.contains_key("exchange-stop-child"));
    }
    restored.reconcile_pending().await.unwrap();
    restored.finalize_closed().await.unwrap();
    let i = restored.inner.lock().await;
    assert!(i.book.positions.is_empty() && i.book.pending.is_empty());
    assert_eq!(i.book.closed_trades[0].net_profit, d("-11.0945"));
    assert_eq!(m.lock().unwrap().posts, 1);
    assert_eq!(m.lock().unwrap().market_posts, 0);
    server.abort();
}

#[tokio::test]
async fn native_stops_use_the_exit_side_and_settlement_asset_for_long_and_short() {
    for side in [Action::Long, Action::Short] {
        for symbol in ["XUSDT", "XUSDC"] {
            let m = Arc::new(StdMutex::new(Mock::default()));
            let (url, server) = exchange(m.clone()).await;
            let e = engine(url).await;
            let mut o = entry(side);
            o.stop_loss_usdt = d("10");
            e.paper_entry(o).await.unwrap();
            if symbol == "XUSDC" {
                let mut i = e.inner.lock().await;
                let mut p = i.book.positions.remove("XUSDT").unwrap();
                p.symbol = symbol.into();
                i.book.positions.insert(symbol.into(), p);
                let mut rule = i.catalog["XUSDT"].clone();
                rule.symbol = symbol.into();
                i.catalog.insert(symbol.into(), rule);
            }
            e.ensure_exchange_stop(symbol).await.unwrap();
            let m = m.lock().unwrap();
            let s = m.stops.values().next().unwrap();
            assert_eq!(s["symbol"], symbol);
            assert_eq!(s["side"], side.exit_side());
            let trigger: Decimal = s["triggerPrice"].as_str().unwrap().parse().unwrap();
            assert_eq!(trigger < d("100"), side == Action::Long);
            assert_eq!(m.posts, 1);
            server.abort();
        }
    }
}

#[tokio::test]
async fn orphan_protection_blocks_entries_until_account_confirmation_and_cleanup() {
    let m = Arc::new(StdMutex::new(Mock::default()));
    let (url, server) = exchange(m.clone()).await;
    let e = engine(url).await;
    positioned(&e).await;
    e.ensure_exchange_stop("XUSDT").await.unwrap();
    {
        let mut i = e.inner.lock().await;
        i.book.positions.clear();
        i.account_sync_failed = true;
    }
    e.cleanup_exchange_stops().await.unwrap();
    assert!(!e.inner.lock().await.exchange_stops_ready());
    assert_eq!(m.lock().unwrap().actions, vec!["post-1"]);
    {
        let mut i = e.inner.lock().await;
        i.account_sync_failed = false;
        i.account_at = now_ms();
    }
    e.cleanup_exchange_stops().await.unwrap();
    assert!(e.inner.lock().await.book.stop_orders.is_empty());
    assert_eq!(m.lock().unwrap().actions, vec!["post-1", "delete-1"]);
    server.abort();
}

#[tokio::test]
async fn triggered_child_can_reduce_only_the_remainder_after_a_concurrent_take_profit_fill() {
    let m = Arc::new(StdMutex::new(Mock {
        child_quantity: Some(d("0.5")),
        ..Default::default()
    }));
    let (url, server) = exchange(m.clone()).await;
    let e = engine(url).await;
    positioned(&e).await;
    e.ensure_exchange_stop("XUSDT").await.unwrap();
    e.inner
        .lock()
        .await
        .book
        .positions
        .get_mut("XUSDT")
        .unwrap()
        .quantity = d("0.5");
    {
        let mut m = m.lock().unwrap();
        let s = m.stops.values_mut().next().unwrap();
        s["algoStatus"] = json!("FINISHED");
        s["actualOrderId"] = json!(55);
    }
    e.reconcile_exchange_stops().await.unwrap();
    assert_eq!(
        e.inner.lock().await.book.pending["exchange-stop-child"].quantity,
        d("0.5")
    );
    e.reconcile_pending().await.unwrap();
    assert_eq!(
        e.inner.lock().await.book.positions["XUSDT"].quantity,
        Decimal::ZERO
    );
    e.finalize_closed().await.unwrap();
    assert!(e.inner.lock().await.book.positions.is_empty());
    assert_eq!(m.lock().unwrap().market_posts, 0);
    server.abort();
}
