use crate::{
    domain::{Quote, now_ms},
    engine::{Engine, normalize_symbol},
    exchange::decimal,
};
use anyhow::{Result, bail, ensure};
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};
use tokio::time::{Instant, timeout};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};

#[derive(Default, Clone, Serialize)]
pub struct MarketStreamStatus {
    pub connected: bool,
    pub connected_at: i64,
    pub last_event_at: i64,
    pub subscriptions: usize,
    pub reconnects: u64,
    pub events: u64,
    pub last_error: Option<String>,
}

struct Pending {
    started: Instant,
    subscribe: bool,
    symbols: Vec<String>,
}

fn parse_quote(value: &Value) -> Result<Option<(String, Quote)>> {
    let data = value.get("data").unwrap_or(value);
    if data["e"] != "bookTicker" || data.get("st").is_some_and(|st| st != 1) {
        return Ok(None);
    }
    let parse = || -> Result<(String, Quote)> {
        let symbol = data["s"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("missing symbol"))?;
        ensure!(normalize_symbol(symbol)? == symbol, "invalid symbol");
        let time = data["T"]
            .as_i64()
            .filter(|t| *t > 0)
            .ok_or_else(|| anyhow::anyhow!("missing time"))?;
        let update_id = data["u"]
            .as_u64()
            .filter(|id| *id > 0)
            .ok_or_else(|| anyhow::anyhow!("missing update id"))?;
        let quote = Quote {
            bid: decimal(&data["b"])?,
            ask: decimal(&data["a"])?,
            bid_qty: decimal(&data["B"])?,
            ask_qty: decimal(&data["A"])?,
            time: now_ms(),
            exchange_time: Some(time),
            update_id: Some(update_id),
        };
        quote.validate()?;
        ensure!(
            quote.bid_qty >= Decimal::ZERO && quote.ask_qty >= Decimal::ZERO,
            "invalid depth"
        );
        Ok((symbol.into(), quote))
    };
    parse()
        .map(Some)
        .map_err(|_| anyhow::anyhow!("行情推送格式异常"))
}
fn stream_params(symbols: &[String]) -> Result<Vec<String>> {
    symbols
        .iter()
        .map(|symbol| {
            ensure!(normalize_symbol(symbol)? == *symbol, "订阅币对无效");
            Ok(format!("{}@bookTicker", symbol.to_ascii_lowercase()))
        })
        .collect()
}
pub(crate) async fn run(engine: Arc<Engine>) {
    let mut failures = 0u32;
    loop {
        if engine.market_symbols().await.is_empty() {
            let mut status = engine.market_stream.lock().await;
            status.connected = false;
            status.subscriptions = 0;
            status.last_error = None;
            drop(status);
            tokio::time::sleep(Duration::from_secs(2)).await;
            continue;
        }
        let result = session_timed(
            &engine,
            Duration::from_secs(2),
            Duration::from_secs(30),
            Duration::from_secs(90),
        )
        .await;
        let mut status = engine.market_stream.lock().await;
        if status.connected && now_ms() - status.connected_at >= 60_000 {
            failures = 0;
        }
        status.connected = false;
        status.subscriptions = 0;
        if result.is_ok() {
            status.last_error = None;
            drop(status);
            continue;
        }
        status.last_error = Some("行情推送中断，REST 继续补充报价，等待重连".into());
        status.reconnects = status.reconnects.saturating_add(1);
        drop(status);
        failures = failures.saturating_add(1);
        tokio::time::sleep(Duration::from_secs((1u64 << failures.min(6)).min(60))).await;
    }
}
async fn session_timed(
    engine: &Arc<Engine>,
    control_interval: Duration,
    heartbeat_interval: Duration,
    idle_timeout: Duration,
) -> Result<()> {
    engine.exchange.stream_admission().await?;
    let started = std::time::Instant::now();
    let connection = timeout(
        Duration::from_secs(8),
        connect_async_with_config(
            engine.exchange.market_stream_url(),
            Some(
                WebSocketConfig::default()
                    .max_message_size(Some(1 << 20))
                    .max_frame_size(Some(1 << 20)),
            ),
            false,
        ),
    )
    .await?;
    if let Err(tokio_tungstenite::tungstenite::Error::Http(response)) = &connection
        && matches!(response.status().as_u16(), 418 | 429)
    {
        engine
            .exchange
            .stream_limit(
                started,
                response.status().as_u16(),
                response
                    .headers()
                    .get("retry-after")
                    .and_then(|h| h.to_str().ok()),
            )
            .await;
    }
    let (mut socket, _) = connection.map_err(|_| anyhow::anyhow!("行情推送握手失败"))?;
    {
        let mut status = engine.market_stream.lock().await;
        status.connected = true;
        status.connected_at = now_ms();
        status.last_error = None;
    }
    let connected = Instant::now();
    let mut last_frame = connected;
    let mut active = BTreeSet::new();
    let mut pending = BTreeMap::<String, Pending>::new();
    let mut sequence = 0u64;
    let mut control = tokio::time::interval(control_interval);
    control.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut heartbeat = tokio::time::interval(heartbeat_interval);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    heartbeat.tick().await;
    loop {
        tokio::select! {
            _ = control.tick() => {
                ensure!(pending.values().all(|request| request.started.elapsed()<Duration::from_secs(8)),"行情订阅确认超时");
                let desired=engine.market_symbols().await;
                if desired.is_empty() {return Ok(());}
                ensure!(desired.len()<=1024,"行情订阅超过单连接上限，使用 REST 兜底");
                if !pending.is_empty() {continue;}
                // Unsubscribe first to stay below the connection's stream cap.
                for (subscribe,symbols) in [(false,active.difference(&desired).cloned().collect::<Vec<_>>()),(true,desired.difference(&active).cloned().collect::<Vec<_>>())] {
                    if symbols.is_empty() {continue;}
                    sequence=sequence.checked_add(1).ok_or_else(||anyhow::anyhow!("行情订阅编号耗尽"))?;
                    let id=sequence.to_string();
                    let message=json!({"method":if subscribe {"SUBSCRIBE"} else {"UNSUBSCRIBE"},"params":stream_params(&symbols)?,"id":id});
                    timeout(Duration::from_secs(8),socket.send(Message::Text(message.to_string().into()))).await??;
                    pending.insert(id,Pending {started:Instant::now(),subscribe,symbols});
                }
            }
            _ = heartbeat.tick() => {
                ensure!(last_frame.elapsed()<idle_timeout,"行情推送心跳超时");
                ensure!(connected.elapsed()<Duration::from_secs(23*3600+50*60),"行情推送定时重连");
                timeout(Duration::from_secs(8),socket.send(Message::Ping(Vec::new().into()))).await??;
            }
            frame = socket.next() => {
                let frame=frame.ok_or_else(||anyhow::anyhow!("行情推送连接关闭"))??;
                last_frame=Instant::now();
                let text=match frame {
                    Message::Text(text)=>text,
                    Message::Ping(data)=>{timeout(Duration::from_secs(8),socket.send(Message::Pong(data))).await??;continue;},
                    Message::Close(_)=>bail!("行情推送连接关闭"),
                    Message::Binary(_)=>bail!("行情推送格式异常"),
                    _=>continue,
                };
                let value:Value=serde_json::from_str(&text).map_err(|_|anyhow::anyhow!("行情推送格式异常"))?;
                ensure!(value.get("code").is_none() && value.get("error").is_none(),"行情订阅被拒绝");
                if let Some(id)=value["id"].as_str().map(str::to_owned).or_else(||value["id"].as_u64().map(|id|id.to_string())) {
                    if let Some(request)=pending.remove(&id) {
                        ensure!(value.get("result").is_some_and(Value::is_null),"行情订阅确认异常");
                        for symbol in request.symbols {
                            if request.subscribe {active.insert(symbol);} else {active.remove(&symbol);}
                        }
                        engine.market_stream.lock().await.subscriptions=active.len();
                    }
                    continue;
                }
                if let Some((symbol,quote))=parse_quote(&value)?
                    && (active.contains(&symbol) || pending.values().any(|p|p.subscribe && p.symbols.contains(&symbol)))
                    && engine.apply_market_quote(symbol,quote).await {
                    let mut status=engine.market_stream.lock().await;
                    status.last_event_at=now_ms();
                    status.events=status.events.saturating_add(1);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::{Config, Mode},
        domain::PoolCoin,
        exchange::Binance,
        jev::Jev,
        store::Store,
    };
    use tokio::{net::TcpListener, sync::mpsc};
    fn book(symbol: &str, id: u64) -> Value {
        json!({"e":"bookTicker","s":symbol,"u":id,"T":100,"b":"100","a":"101","B":"0.4","A":"0.5","st":1})
    }
    fn engine(url: String) -> Arc<Engine> {
        let mut exchange = Binance::mock("http://127.0.0.1:9".into());
        exchange.stream_base = Some(url);
        Engine::new(
            Mode::Paper,
            Store::open(":memory:", Mode::Paper).unwrap(),
            Config::default(),
            exchange,
            Jev::new().unwrap(),
        )
        .unwrap()
    }
    async fn add(engine: &Engine, symbol: &str) {
        engine.inner.lock().await.book.pool.insert(
            symbol.into(),
            PoolCoin {
                symbol: symbol.into(),
                enabled: true,
                config: None,
            },
        );
    }
    async fn wait_events(engine: &Engine, events: u64) {
        timeout(Duration::from_secs(2), async {
            loop {
                if engine.market_stream.lock().await.events >= events {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }
    #[test]
    fn parser_accepts_combined_um_quotes_and_rejects_bad_prices_without_raw_errors() {
        let raw = book("龙虾USDC", 42);
        let (symbol, quote) = parse_quote(&json!({"stream":"龙虾usdc@bookTicker","data":raw}))
            .unwrap()
            .unwrap();
        assert_eq!(symbol, "龙虾USDC");
        assert_eq!(quote.update_id, Some(42));
        assert_eq!(quote.bid_qty, Decimal::new(4, 1));
        assert_eq!(
            stream_params(&[symbol]).unwrap(),
            vec!["龙虾usdc@bookTicker"]
        );
        for (field, value) in [
            ("b", json!("private-token")),
            ("a", json!("99")),
            ("B", json!("-1")),
            ("u", json!(0)),
            ("T", json!(0)),
            ("s", json!("../USDT")),
        ] {
            let mut bad = book("XUSDT", 1);
            bad[field] = value;
            assert_eq!(
                parse_quote(&bad).unwrap_err().to_string(),
                "行情推送格式异常"
            );
        }
        let mut cm = book("XUSDT", 1);
        cm["st"] = json!(2);
        assert!(parse_quote(&cm).unwrap().is_none());
    }
    #[test]
    fn market_endpoints_follow_the_account_environment() {
        assert_eq!(
            Binance::new(Mode::Paper).unwrap().market_stream_url(),
            "wss://fstream.binance.com/public/stream"
        );
        assert_eq!(
            Binance::new(Mode::Live).unwrap().market_stream_url(),
            "wss://fstream.binance.com/public/stream"
        );
        assert_eq!(
            Binance::new(Mode::Testnet).unwrap().market_stream_url(),
            "wss://demo-fstream.binance.com/stream"
        );
    }
    #[tokio::test]
    async fn subscriptions_change_in_place_and_old_duplicate_or_unwanted_quotes_are_ignored() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let e = engine(format!("ws://{}", listener.local_addr().unwrap()));
        add(&e, "XUSDT").await;
        let (commands, mut observed) = mpsc::unbounded_channel();
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(tcp).await.unwrap();
            while let Some(Ok(frame)) = socket.next().await {
                match frame {
                    Message::Text(text) => {
                        let command: Value = serde_json::from_str(&text).unwrap();
                        socket
                            .send(Message::Text(
                                json!({"id":command["id"],"result":null}).to_string().into(),
                            ))
                            .await
                            .unwrap();
                        if command["method"] == "SUBSCRIBE" {
                            let symbol = if command["params"][0] == "xusdt@bookTicker" {
                                "XUSDT"
                            } else {
                                "YUSDC"
                            };
                            socket
                                .send(Message::Text(
                                    json!({"data":book(symbol,42)}).to_string().into(),
                                ))
                                .await
                                .unwrap();
                            let mut duplicate = book(symbol, 42);
                            duplicate["b"] = json!("90");
                            socket
                                .send(Message::Text(duplicate.to_string().into()))
                                .await
                                .unwrap();
                            socket
                                .send(Message::Text(book(symbol, 41).to_string().into()))
                                .await
                                .unwrap();
                            socket
                                .send(Message::Text(book("ZUSDT", 100).to_string().into()))
                                .await
                                .unwrap();
                        }
                        commands.send(command).unwrap();
                    }
                    Message::Ping(data) => {
                        socket.send(Message::Pong(data)).await.unwrap();
                    }
                    _ => {}
                }
            }
        });
        let worker = {
            let e = e.clone();
            tokio::spawn(async move {
                session_timed(
                    &e,
                    Duration::from_millis(40),
                    Duration::from_millis(100),
                    Duration::from_secs(1),
                )
                .await
            })
        };
        let first = timeout(Duration::from_secs(2), observed.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first["params"], json!(["xusdt@bookTicker"]));
        wait_events(&e, 1).await;
        let receipt = e.inner.lock().await.quotes["XUSDT"].time;
        add(&e, "YUSDC").await;
        e.inner.lock().await.book.pool.remove("XUSDT");
        let remove = timeout(Duration::from_secs(2), observed.recv())
            .await
            .unwrap()
            .unwrap();
        let add = timeout(Duration::from_secs(2), observed.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(remove["method"], "UNSUBSCRIBE");
        assert_eq!(remove["params"], json!(["xusdt@bookTicker"]));
        assert_eq!(add["params"], json!(["yusdc@bookTicker"]));
        wait_events(&e, 2).await;
        let i = e.inner.lock().await;
        assert_eq!(i.quotes["XUSDT"].time, receipt);
        assert_eq!(i.quotes["XUSDT"].bid, Decimal::from(100));
        assert!(!i.quotes.contains_key("ZUSDT"));
        drop(i);
        assert_eq!(e.market_stream.lock().await.subscriptions, 1);
        e.inner.lock().await.book.pool.clear();
        assert!(
            timeout(Duration::from_secs(2), worker)
                .await
                .unwrap()
                .unwrap()
                .is_ok()
        );
        server.abort();
    }
    #[tokio::test]
    async fn disconnected_stream_reconnects_and_restores_subscriptions() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let e = engine(format!("ws://{}", listener.local_addr().unwrap()));
        add(&e, "XUSDT").await;
        let server = tokio::spawn(async move {
            for index in 0..2 {
                let (tcp, _) = listener.accept().await.unwrap();
                let mut socket = tokio_tungstenite::accept_async(tcp).await.unwrap();
                let Message::Text(text) = socket.next().await.unwrap().unwrap() else {
                    panic!("subscribe expected")
                };
                let request: Value = serde_json::from_str(&text).unwrap();
                assert_eq!(request["params"], json!(["xusdt@bookTicker"]));
                socket
                    .send(Message::Text(
                        json!({"id":request["id"],"result":null}).to_string().into(),
                    ))
                    .await
                    .unwrap();
                if index == 0 {
                    socket.close(None).await.unwrap();
                } else {
                    socket
                        .send(Message::Text(book("XUSDT", 43).to_string().into()))
                        .await
                        .unwrap();
                    while socket.next().await.is_some() {}
                }
            }
        });
        let worker = tokio::spawn(run(e.clone()));
        timeout(Duration::from_secs(5), async {
            loop {
                let status = e.market_stream.lock().await.clone();
                if status.connected && status.reconnects == 1 && status.events == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(e.market_stream.lock().await.last_error.is_none());
        worker.abort();
        server.abort();
    }
    #[tokio::test]
    async fn silent_socket_cannot_stay_healthy() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let e = engine(format!("ws://{}", listener.local_addr().unwrap()));
        add(&e, "XUSDT").await;
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let _socket = tokio_tungstenite::accept_async(tcp).await.unwrap();
            tokio::time::sleep(Duration::from_secs(2)).await;
        });
        let error = timeout(
            Duration::from_secs(1),
            session_timed(
                &e,
                Duration::from_millis(20),
                Duration::from_millis(30),
                Duration::from_millis(100),
            ),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert_eq!(error.to_string(), "行情推送心跳超时");
        server.abort();
    }
    #[tokio::test]
    async fn missing_subscription_ack_times_out_even_when_pongs_keep_arriving() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let e = engine(format!("ws://{}", listener.local_addr().unwrap()));
        add(&e, "XUSDT").await;
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(tcp).await.unwrap();
            while let Some(Ok(frame)) = socket.next().await {
                if let Message::Ping(data) = frame {
                    socket.send(Message::Pong(data)).await.unwrap();
                }
            }
        });
        let error = timeout(
            Duration::from_secs(10),
            session_timed(
                &e,
                Duration::from_millis(20),
                Duration::from_millis(100),
                Duration::from_secs(1),
            ),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert_eq!(error.to_string(), "行情订阅确认超时");
        server.abort();
    }
    #[tokio::test]
    async fn rejected_subscription_uses_a_fixed_error_without_server_details() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let e = engine(format!("ws://{}", listener.local_addr().unwrap()));
        add(&e, "XUSDT").await;
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(tcp).await.unwrap();
            let _ = socket.next().await.unwrap().unwrap();
            socket
                .send(Message::Text(
                    json!({"code":2,"msg":"private-details","id":"1"})
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();
        });
        let error = session_timed(
            &e,
            Duration::from_millis(20),
            Duration::from_secs(30),
            Duration::from_secs(90),
        )
        .await
        .unwrap_err();
        assert_eq!(error.to_string(), "行情订阅被拒绝");
        server.await.unwrap();
    }
    #[tokio::test]
    async fn handshake_rate_limit_is_shared_with_rest_without_another_request() {
        use axum::{Router, http::StatusCode, routing::get};
        let app = Router::new().route(
            "/stream",
            get(|| async {
                (
                    StatusCode::TOO_MANY_REQUESTS,
                    [("retry-after", "60")],
                    "private-details",
                )
            }),
        );
        let (base, server) = crate::test_support::mock_server(app).await;
        let e = engine(format!("{}/stream", base.replacen("http://", "ws://", 1)));
        add(&e, "XUSDT").await;
        let error = session_timed(
            &e,
            Duration::from_secs(2),
            Duration::from_secs(30),
            Duration::from_secs(90),
        )
        .await
        .unwrap_err();
        assert_eq!(error.to_string(), "行情推送握手失败");
        let status = e.exchange.rate_limit_snapshot().await;
        assert_eq!(status.last_limit_status, Some(429));
        assert!(status.cooldown_remaining_ms > 50_000);
        assert!(
            e.exchange
                .quotes()
                .await
                .unwrap_err()
                .to_string()
                .contains("本次未发送")
        );
        server.abort();
    }
}
