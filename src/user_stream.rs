use crate::{domain::now_ms, engine::Engine, exchange::ExchangeOrder};
use anyhow::{Result, bail, ensure};
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::time::{Instant, timeout};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};

#[derive(Default, Clone, Serialize)]
pub struct StreamStatus {
    pub enabled: bool,
    pub connected: bool,
    pub connected_at: i64,
    pub last_event_at: i64,
    pub last_keepalive_at: i64,
    pub reconnects: u64,
    pub events: u64,
    pub last_error: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct UserOrder {
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "c")]
    pub client_id: String,
    #[serde(rename = "i")]
    pub order_id: i64,
    #[serde(rename = "q")]
    pub quantity: Decimal,
    #[serde(rename = "z")]
    pub filled: Decimal,
    #[serde(rename = "ap")]
    pub average: Decimal,
    #[serde(rename = "X")]
    pub status: String,
    #[serde(rename = "S")]
    pub side: String,
    #[serde(rename = "ps")]
    pub position_side: String,
}
impl UserOrder {
    pub(crate) fn report(&self) -> ExchangeOrder {
        ExchangeOrder {
            symbol: self.symbol.clone(),
            client_order_id: self.client_id.clone(),
            order_id: self.order_id,
            status: self.status.clone(),
            executed_qty: self.filled,
            avg_price: self.average,
            orig_qty: self.quantity,
            cum_quote: self.average * self.filled,
        }
    }
}
pub(crate) enum UserEvent {
    Order {
        event_at: i64,
        transaction_at: i64,
        order: UserOrder,
    },
    Account,
    Risk,
    Expired,
    Other,
}
pub(crate) fn parse_event(value: Value) -> Result<UserEvent> {
    match value["e"].as_str() {
        Some("listenKeyExpired") => Ok(UserEvent::Expired),
        Some("ORDER_TRADE_UPDATE") => {
            let event_at = value["E"]
                .as_i64()
                .filter(|t| *t > 0)
                .ok_or_else(|| anyhow::anyhow!("账户推送格式异常"))?;
            let transaction_at = value["T"]
                .as_i64()
                .filter(|t| *t > 0)
                .ok_or_else(|| anyhow::anyhow!("账户推送格式异常"))?;
            let order: UserOrder = serde_json::from_value(value["o"].clone())
                .map_err(|_| anyhow::anyhow!("订单推送格式异常"))?;
            ensure!(
                matches!(
                    order.status.as_str(),
                    "NEW"
                        | "PARTIALLY_FILLED"
                        | "FILLED"
                        | "CANCELED"
                        | "EXPIRED"
                        | "EXPIRED_IN_MATCH"
                        | "REJECTED"
                ) && order.quantity > Decimal::ZERO
                    && order.filled >= Decimal::ZERO
                    && order.filled <= order.quantity
                    && (order.filled == Decimal::ZERO || order.average > Decimal::ZERO),
                "订单推送格式异常"
            );
            Ok(UserEvent::Order {
                event_at,
                transaction_at,
                order,
            })
        }
        Some("ACCOUNT_UPDATE" | "ACCOUNT_CONFIG_UPDATE" | "TRADE_LITE" | "ALGO_UPDATE") => {
            Ok(UserEvent::Account)
        }
        Some("MARGIN_CALL") => Ok(UserEvent::Risk),
        _ => Ok(UserEvent::Other),
    }
}

pub(crate) async fn run(engine: Arc<Engine>) {
    let mut failures = 0u32;
    loop {
        let result = session(&engine).await;
        let message = result
            .err()
            .map(|_| "账户推送中断，REST 继续核对，等待重连".to_string());
        let needs_gap = {
            let mut status = engine.user_stream.lock().await;
            if status.connected && now_ms() - status.connected_at > 60_000 {
                failures = 0;
            }
            let needs_gap = status.last_error.is_none();
            status.connected = false;
            status.last_error = message;
            status.reconnects = status.reconnects.saturating_add(1);
            needs_gap
        };
        if needs_gap {
            engine.stream_gap().await;
        }
        failures = failures.saturating_add(1);
        // Respect the shared HTTP cooldown on subscription/renewal as well.
        let cooldown = engine
            .exchange
            .rate_limit_snapshot()
            .await
            .cooldown_remaining_ms;
        let seconds = (1u64 << failures.min(6))
            .min(60)
            .max(cooldown.div_ceil(1000).min(60));
        tokio::time::sleep(Duration::from_secs(seconds)).await;
    }
}

async fn session(engine: &Arc<Engine>) -> Result<()> {
    session_timed(
        engine,
        Duration::from_secs(30),
        Duration::from_secs(30 * 60),
        Duration::from_secs(90),
    )
    .await
}
async fn session_timed(
    engine: &Arc<Engine>,
    heartbeat_interval: Duration,
    renewal_interval: Duration,
    idle_timeout: Duration,
) -> Result<()> {
    let key = engine.exchange.start_user_stream().await?;
    engine.exchange.stream_admission().await?;
    let url = engine.exchange.user_stream_url(&key);
    let config = WebSocketConfig::default()
        .max_message_size(Some(1 << 20))
        .max_frame_size(Some(1 << 20));
    let connect_started = std::time::Instant::now();
    let connection = timeout(
        Duration::from_secs(8),
        connect_async_with_config(url, Some(config), false),
    )
    .await?;
    if let Err(tokio_tungstenite::tungstenite::Error::Http(response)) = &connection
        && matches!(response.status().as_u16(), 418 | 429)
    {
        engine
            .exchange
            .stream_limit(
                connect_started,
                response.status().as_u16(),
                response
                    .headers()
                    .get("retry-after")
                    .and_then(|h| h.to_str().ok()),
            )
            .await;
    }
    let (mut socket, _) = connection.map_err(|_| anyhow::anyhow!("账户推送握手失败"))?;
    {
        let mut status = engine.user_stream.lock().await;
        status.connected = true;
        status.connected_at = now_ms();
        status.last_error = None;
    }
    engine.handle_user_event(UserEvent::Account).await?;
    let started = Instant::now();
    let mut last_frame = started;
    let mut heartbeat = tokio::time::interval(heartbeat_interval);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    heartbeat.tick().await;
    let mut keepalive = tokio::time::interval(renewal_interval);
    keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    keepalive.tick().await;
    let mut order_times = BTreeMap::new();
    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                ensure!(last_frame.elapsed()<idle_timeout, "账户推送心跳超时");
                ensure!(started.elapsed()<Duration::from_secs(23*3600+50*60), "账户推送定时重连");
                timeout(Duration::from_secs(8),socket.send(Message::Ping(Vec::new().into()))).await??;
            }
            _ = keepalive.tick() => {
                engine.exchange.keep_user_stream().await?;
                engine.user_stream.lock().await.last_keepalive_at=now_ms();
            }
            frame = socket.next() => {
                let frame = frame.ok_or_else(|| anyhow::anyhow!("账户推送连接关闭"))??;
                last_frame=Instant::now();
                let text = match frame {
                    Message::Text(text) => text,
                    Message::Ping(data) => {timeout(Duration::from_secs(8),socket.send(Message::Pong(data))).await??;continue;},
                    Message::Close(_) => bail!("账户推送连接关闭"),
                    Message::Binary(_) => bail!("账户推送格式异常"),
                    _ => continue,
                };
                let event = parse_event(serde_json::from_str(&text).map_err(|_| anyhow::anyhow!("账户推送格式异常"))?)?;
                if matches!(event,UserEvent::Expired) {bail!("账户推送订阅已过期");}
                if let UserEvent::Order {event_at,transaction_at,order} = &event {
                    if order_times.get(&order.client_id).is_some_and(|(e,t)| event_at<e || transaction_at<t) {continue;}
                    order_times.insert(order.client_id.clone(),(*event_at,*transaction_at));
                    if order_times.len()>1024 {order_times.clear();}
                }
                engine.handle_user_event(event).await?;
                let mut status=engine.user_stream.lock().await;
                status.last_event_at=now_ms();
                status.events=status.events.saturating_add(1);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::{Config, Mode},
        exchange::Binance,
        jev::Jev,
        store::Store,
    };
    use axum::{Json, Router, http::HeaderMap, routing::post};
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    // Tungstenite's handshake callback requires this unboxed error response.
    #[allow(clippy::result_large_err)]
    fn verify_stream_request(
        req: &tokio_tungstenite::tungstenite::handshake::server::Request,
        response: tokio_tungstenite::tungstenite::handshake::server::Response,
    ) -> std::result::Result<
        tokio_tungstenite::tungstenite::handshake::server::Response,
        tokio_tungstenite::tungstenite::handshake::server::ErrorResponse,
    > {
        assert_eq!(req.uri().path(), "/ws/privateToken");
        Ok(response)
    }

    #[allow(clippy::result_large_err)]
    fn limited_stream_request(
        _: &tokio_tungstenite::tungstenite::handshake::server::Request,
        _: tokio_tungstenite::tungstenite::handshake::server::Response,
    ) -> std::result::Result<
        tokio_tungstenite::tungstenite::handshake::server::Response,
        tokio_tungstenite::tungstenite::handshake::server::ErrorResponse,
    > {
        Err(tokio_tungstenite::tungstenite::http::Response::builder()
            .status(429)
            .header("retry-after", "60")
            .body(Some("privateToken must not reach logs".into()))
            .unwrap())
    }
    #[test]
    fn user_order_payload_keeps_cumulative_fills_and_rejects_invalid_updates() {
        let value = json!({"e":"ORDER_TRADE_UPDATE","E":2,"T":1,"o":{"s":"XUSDT","c":"gt-test","i":42,"q":"1","z":"0.4","ap":"101","X":"PARTIALLY_FILLED","S":"SELL","ps":"BOTH"}});
        let UserEvent::Order { order, .. } = parse_event(value.clone()).unwrap() else {
            panic!("order expected")
        };
        assert_eq!(order.report().cum_quote, Decimal::new(404, 1));
        let mut invalid = value.clone();
        invalid["o"]["z"] = json!("2");
        assert!(parse_event(invalid).is_err());
        let mut invalid = value;
        invalid["o"]["X"] = json!("unrecognized");
        assert!(parse_event(invalid).is_err());
        assert!(matches!(
            parse_event(json!({"e":"listenKeyExpired","listenKey":"private-token"})).unwrap(),
            UserEvent::Expired
        ));
    }
    #[test]
    fn stream_endpoints_pair_with_current_account_environment() {
        assert_eq!(
            Binance::new(Mode::Testnet)
                .unwrap()
                .user_stream_url("token"),
            "wss://demo-fstream.binance.com/ws/token"
        );
        assert_eq!(
            Binance::new(Mode::Live).unwrap().user_stream_url("token"),
            "wss://fstream.binance.com/private/ws/token"
        );
    }
    async fn fixture() -> (
        Arc<Engine>,
        Arc<AtomicUsize>,
        tokio::task::JoinHandle<()>,
        tokio::net::TcpListener,
    ) {
        let renewals = Arc::new(AtomicUsize::new(0));
        let count = renewals.clone();
        let app = Router::new().route(
            "/fapi/v1/listenKey",
            post(|headers: HeaderMap| async move {
                assert_eq!(headers["x-mbx-apikey"], "local-test-key");
                Json(json!({"listenKey":"privateToken"}))
            })
            .put(move |headers: HeaderMap| {
                let count = count.clone();
                async move {
                    assert_eq!(headers["x-mbx-apikey"], "local-test-key");
                    count.fetch_add(1, Ordering::SeqCst);
                    Json(json!({"listenKey":"privateToken"}))
                }
            }),
        );
        let (base, http) = crate::test_support::mock_server(app).await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut exchange = Binance::mock(base);
        exchange.stream_base = Some(format!("ws://{}/ws", listener.local_addr().unwrap()));
        let engine = Engine::new(
            Mode::Testnet,
            Store::open(":memory:", Mode::Testnet).unwrap(),
            Config::default(),
            exchange,
            Jev::new().unwrap(),
        )
        .unwrap();
        (engine, renewals, http, listener)
    }
    #[tokio::test]
    async fn websocket_renews_subscription_answers_pings_and_detects_expiry() {
        let (engine, renewals, http, listener) = fixture().await;
        let ws = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_hdr_async(tcp, verify_stream_request)
                .await
                .unwrap();
            socket
                .send(Message::Text(
                    json!({"e":"ACCOUNT_UPDATE","E":1,"T":1,"a":{"B":[{"a":"USDT","wb":"9000"}]}})
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();
            socket
                .send(Message::Ping(b"probe".to_vec().into()))
                .await
                .unwrap();
            let mut answered = false;
            while let Some(Ok(frame)) = socket.next().await {
                match frame {
                    Message::Pong(data) if data.as_ref() == b"probe" => answered = true,
                    Message::Ping(data) => {
                        socket.send(Message::Pong(data)).await.unwrap();
                    }
                    _ => {}
                }
                if answered {
                    tokio::time::sleep(Duration::from_millis(150)).await;
                    socket
                        .send(Message::Text(
                            json!({"e":"listenKeyExpired","listenKey":"privateToken"})
                                .to_string()
                                .into(),
                        ))
                        .await
                        .unwrap();
                    return;
                }
            }
            panic!("client did not answer the server ping");
        });
        let result = timeout(
            Duration::from_secs(3),
            session_timed(
                &engine,
                Duration::from_millis(50),
                Duration::from_millis(60),
                Duration::from_millis(500),
            ),
        )
        .await
        .unwrap();
        assert!(result.is_err());
        ws.await.unwrap();
        assert!(renewals.load(Ordering::SeqCst) > 0);
        assert!(engine.snapshot().await.account_update_pending);
        assert_eq!(engine.snapshot().await.available_usdt, Decimal::ZERO);
        let serialized = serde_json::to_string(&engine.snapshot().await).unwrap();
        assert!(!serialized.contains("privateToken") && !serialized.contains("local-test-key"));
        http.abort();
    }
    #[tokio::test]
    async fn websocket_handshake_limit_blocks_following_subscription_requests() {
        let (engine, _, http, listener) = fixture().await;
        let ws = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let result = tokio_tungstenite::accept_hdr_async(tcp, limited_stream_request).await;
            assert!(result.is_err());
        });
        assert_eq!(
            session(&engine).await.unwrap_err().to_string(),
            "账户推送握手失败"
        );
        ws.await.unwrap();
        let before = engine.exchange.rate_limit_snapshot().await;
        assert!(before.cooldown_remaining_ms > 0);
        assert!(engine.exchange.start_user_stream().await.is_err());
        let after = engine.exchange.rate_limit_snapshot().await;
        assert_eq!(after.admitted_requests, before.admitted_requests);
        assert_eq!(after.cooldown_rejections, before.cooldown_rejections + 1);
        http.abort();
    }
    #[tokio::test]
    async fn websocket_silent_peer_times_out_instead_of_staying_connected() {
        let (engine, _, http, listener) = fixture().await;
        let ws = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let _socket = tokio_tungstenite::accept_async(tcp).await.unwrap();
            tokio::time::sleep(Duration::from_secs(2)).await;
        });
        let result = timeout(
            Duration::from_secs(1),
            session_timed(
                &engine,
                Duration::from_millis(50),
                Duration::from_secs(30),
                Duration::from_millis(150),
            ),
        )
        .await
        .unwrap();
        assert_eq!(result.unwrap_err().to_string(), "账户推送心跳超时");
        ws.abort();
        http.abort();
    }
    #[tokio::test]
    async fn websocket_reconnects_after_expiry_and_keeps_rest_reconciliation_pending() {
        let (engine, _, http, listener) = fixture().await;
        let connections = Arc::new(AtomicUsize::new(0));
        let count = connections.clone();
        let ws = tokio::spawn(async move {
            loop {
                let (tcp, _) = listener.accept().await.unwrap();
                let mut socket = tokio_tungstenite::accept_async(tcp).await.unwrap();
                let n = count.fetch_add(1, Ordering::SeqCst);
                let value = if n == 0 {
                    json!({"e":"listenKeyExpired","listenKey":"privateToken"})
                } else {
                    json!({"e":"ACCOUNT_UPDATE","E":2,"T":2,"a":{}})
                };
                socket
                    .send(Message::Text(value.to_string().into()))
                    .await
                    .unwrap();
                if n > 0 {
                    tokio::time::sleep(Duration::from_secs(10)).await;
                }
            }
        });
        let worker = tokio::spawn(run(engine.clone()));
        timeout(Duration::from_secs(5), async {
            loop {
                let state = engine.snapshot().await;
                if state.user_stream.connected
                    && state.user_stream.reconnects == 1
                    && state.user_stream.events == 1
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(connections.load(Ordering::SeqCst), 2);
        let snapshot = engine.snapshot().await;
        assert!(snapshot.paused && snapshot.account_update_pending);
        assert!(snapshot.user_stream.last_error.is_none());
        let events = engine.store.events(10).unwrap();
        assert!(
            !serde_json::to_string(&events)
                .unwrap()
                .contains("privateToken")
        );
        worker.abort();
        ws.abort();
        http.abort();
    }
}
