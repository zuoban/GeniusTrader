use crate::{
    application::Application,
    auth::{self, AuthError, AuthStatus, ChangePasswordInput, PasswordInput, Session, SetupInput},
    config::Config,
    dashboard::{self, DashboardView},
    domain::{Event, PoolCoin, SymbolInfo},
    engine::Snapshot,
    settings::{
        CredentialTestRequest, CredentialTestResult, JevCredentialTestRequest,
        JevCredentialTestResult, ServerSettings, SettingsPatch, SettingsView,
    },
    strategy::DecisionRecord,
};
use axum::{
    Json, Router,
    extract::{Path, Query, Request, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{net::Ipv4Addr, sync::Arc};

pub struct ApiError(anyhow::Error);
impl From<anyhow::Error> for ApiError {
    fn from(e: anyhow::Error) -> Self {
        Self(e)
    }
}
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({"error":self.0.to_string()})),
        )
            .into_response()
    }
}
pub fn router(app: Arc<Application>) -> Router {
    router_with_bind(app, Ipv4Addr::LOCALHOST)
}
pub fn router_with_bind(app: Arc<Application>, bind: Ipv4Addr) -> Router {
    Router::new()
        .route(
            "/",
            get(|| async { Html(include_str!("../web/index.html")) }),
        )
        .route(
            "/app.css",
            get(|| async {
                (
                    [("content-type", "text/css; charset=utf-8")],
                    include_str!("../web/app.css"),
                )
            }),
        )
        .route(
            "/app.js",
            get(|| async {
                (
                    [("content-type", "application/javascript; charset=utf-8")],
                    include_str!("../web/app.js"),
                )
            }),
        )
        .route("/api/state", get(state))
        .route("/api/dashboard", get(dashboard))
        .route("/api/decisions", get(decisions))
        .route("/api/auth/status", get(auth_status))
        .route("/api/auth/setup", post(auth_setup))
        .route("/api/auth/login", post(auth_login))
        .route("/api/auth/logout", post(auth_logout))
        .route("/api/auth/password", post(auth_password))
        .route("/api/server", get(server_settings).put(configure_server))
        .route("/api/markets", get(markets))
        .route("/api/events", get(events))
        .route("/api/config", axum::routing::put(configure))
        .route("/api/settings", get(settings).put(connections))
        .route("/api/settings/check", post(check_connection))
        .route("/api/settings/test", post(test_credentials))
        .route("/api/settings/jev/test", post(test_jev_credentials))
        .route("/api/pool", post(pool))
        .route("/api/pool/:symbol", axum::routing::delete(remove))
        .route("/api/control", post(control))
        .route("/api/positions/:symbol/close", post(close))
        .route("/api/markets/refresh", post(refresh))
        .layer(middleware::from_fn_with_state(app.clone(), admin_guard))
        .layer(middleware::from_fn_with_state(bind, request_guard))
        .with_state(app)
}
async fn admin_guard(State(app): State<Arc<Application>>, req: Request, next: Next) -> Response {
    if !matches!(*req.method(), Method::GET | Method::HEAD)
        && !matches!(
            req.uri().path(),
            "/api/auth/setup" | "/api/auth/login" | "/api/auth/logout"
        )
    {
        match app.auth.status(auth::cookie_token(req.headers())) {
            Ok(status) if status.authenticated => {}
            Ok(status) => {
                return AuthError(
                    StatusCode::UNAUTHORIZED,
                    if status.initialized {
                        "请输入管理员密码后操作"
                    } else {
                        "请先初始化管理员密码"
                    },
                )
                .into_response();
            }
            Err(error) => return error.into_response(),
        }
    }
    next.run(req).await
}
async fn request_guard(State(bind): State<Ipv4Addr>, req: Request, next: Next) -> Response {
    let allowed_host = req
        .headers()
        .get("host")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<axum::http::uri::Authority>().ok())
        .is_some_and(|a| {
            matches!(a.host(), "localhost" | "127.0.0.1")
                || (!bind.is_loopback() && a.host().parse::<Ipv4Addr>().is_ok())
        });
    if !allowed_host {
        return (
            StatusCode::FORBIDDEN,
            Json(
                json!({"error":"访问地址不在允许范围；远程 IPv4 访问需要使用 --bind 0.0.0.0 启动"}),
            ),
        )
            .into_response();
    }
    if !matches!(*req.method(), Method::GET | Method::HEAD) {
        if req
            .headers()
            .get("x-genius-request")
            .and_then(|v| v.to_str().ok())
            != Some("1")
        {
            return (
                StatusCode::FORBIDDEN,
                Json(json!({"error":"缺少本地操作请求标记"})),
            )
                .into_response();
        }
        if let Some(origin) = req.headers().get("origin") {
            let expected = req
                .headers()
                .get("host")
                .and_then(|v| v.to_str().ok())
                .map(|h| format!("http://{h}"));
            if origin.to_str().ok() != expected.as_deref() {
                return StatusCode::FORBIDDEN.into_response();
            }
        }
    }
    let mut response = next.run(req).await;
    response.headers_mut().insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    response
        .headers_mut()
        .insert("x-frame-options", HeaderValue::from_static("DENY"));
    response.headers_mut().insert("content-security-policy",HeaderValue::from_static("default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'"));
    response
        .headers_mut()
        .insert("cache-control", HeaderValue::from_static("no-store"));
    response
}
#[derive(Serialize)]
struct StateResponse {
    #[serde(flatten)]
    trading: Snapshot,
    settings: SettingsView,
    auth: AuthStatus,
    server: ServerSettings,
}
async fn state(
    State(app): State<Arc<Application>>,
    headers: HeaderMap,
) -> Result<Json<StateResponse>, AuthError> {
    let e = app.engine.read().await;
    Ok(Json(StateResponse {
        trading: e.snapshot().await,
        settings: app.settings_view().await,
        auth: app.auth.status(auth::cookie_token(&headers))?,
        server: app.store.server_settings()?,
    }))
}
#[derive(Serialize)]
struct DashboardResponse {
    #[serde(flatten)]
    state: StateResponse,
    #[serde(flatten)]
    view: DashboardView,
}
async fn dashboard(
    State(app): State<Arc<Application>>,
    headers: HeaderMap,
) -> Result<Json<DashboardResponse>, ApiError> {
    let engine = app.engine.read().await;
    let trading = engine.snapshot().await;
    let view = dashboard::build(
        &trading,
        engine.store.latest_decisions(
            &trading
                .pool
                .iter()
                .map(|coin| coin.symbol.as_str())
                .collect::<Vec<_>>(),
        )?,
        engine.store.recent_decisions(30, None)?,
    )?;
    Ok(Json(DashboardResponse {
        state: StateResponse {
            trading,
            settings: app.settings_view().await,
            auth: app
                .auth
                .status(auth::cookie_token(&headers))
                .map_err(|e| anyhow::anyhow!(e.1))?,
            server: app.store.server_settings()?,
        },
        view,
    }))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DecisionQuery {
    limit: Option<usize>,
    symbol: Option<String>,
}
async fn decisions(
    State(app): State<Arc<Application>>,
    Query(query): Query<DecisionQuery>,
) -> Result<Json<Vec<DecisionRecord>>, ApiError> {
    let limit = query.limit.unwrap_or(30);
    if !(1..=100).contains(&limit) {
        return Err(anyhow::anyhow!("limit 必须在 1–100 之间").into());
    }
    let symbol = query
        .symbol
        .map(|s| s.trim().to_uppercase().replace('/', ""));
    let engine = app.engine.read().await;
    Ok(Json(
        engine.store.recent_decisions(limit, symbol.as_deref())?,
    ))
}
async fn auth_status(
    State(app): State<Arc<Application>>,
    headers: HeaderMap,
) -> Result<Json<AuthStatus>, AuthError> {
    Ok(Json(app.auth.status(auth::cookie_token(&headers))?))
}
fn session_response(session: Session, app: &Application) -> Result<Response, AuthError> {
    let cookie = session.cookie();
    let status = app.auth.status(Some(&session.token))?;
    Ok(([(axum::http::header::SET_COOKIE, cookie)], Json(status)).into_response())
}
async fn auth_setup(
    State(app): State<Arc<Application>>,
    Json(input): Json<SetupInput>,
) -> Result<Response, AuthError> {
    session_response(app.auth.setup(input).await?, &app)
}
async fn auth_login(
    State(app): State<Arc<Application>>,
    Json(input): Json<PasswordInput>,
) -> Result<Response, AuthError> {
    session_response(app.auth.login(input).await?, &app)
}
async fn auth_logout(
    State(app): State<Arc<Application>>,
    headers: HeaderMap,
) -> Result<Response, AuthError> {
    app.auth.logout(auth::cookie_token(&headers))?;
    Ok((
        [(axum::http::header::SET_COOKIE, auth::clear_cookie())],
        Json(app.auth.status(None)?),
    )
        .into_response())
}
async fn auth_password(
    State(app): State<Arc<Application>>,
    Json(input): Json<ChangePasswordInput>,
) -> Result<Response, AuthError> {
    session_response(app.auth.change_password(input).await?, &app)
}
async fn server_settings(
    State(app): State<Arc<Application>>,
) -> Result<Json<ServerSettings>, ApiError> {
    Ok(Json(app.store.server_settings()?))
}
async fn configure_server(
    State(app): State<Arc<Application>>,
    Json(settings): Json<ServerSettings>,
) -> Result<Json<Value>, ApiError> {
    app.store.save_server_settings(&settings)?;
    Ok(Json(
        json!({"ok":true, "message":"已保存到 SQLite；端口在下次启动生效，会话时长用于下次登录"}),
    ))
}
async fn markets(State(app): State<Arc<Application>>) -> Json<Vec<SymbolInfo>> {
    let e = app.engine.read().await;
    let mut markets = e
        .inner
        .lock()
        .await
        .catalog
        .values()
        .cloned()
        .collect::<Vec<_>>();
    markets.sort_by_key(|a| std::cmp::Reverse(a.quote_volume));
    Json(markets)
}
#[derive(Deserialize)]
struct EventQuery {
    #[serde(default)]
    exclude_decisions: bool,
}
async fn events(
    State(app): State<Arc<Application>>,
    Query(query): Query<EventQuery>,
) -> Result<Json<Vec<Event>>, ApiError> {
    let e = app.engine.read().await;
    Ok(Json(if query.exclude_decisions {
        e.store.activity_events(100)?
    } else {
        e.store.events(100)?
    }))
}
async fn configure(
    State(app): State<Arc<Application>>,
    Json(c): Json<Config>,
) -> Result<Json<Value>, ApiError> {
    app.engine.read().await.configure(c).await?;
    Ok(Json(json!({"ok":true})))
}
async fn pool(
    State(app): State<Arc<Application>>,
    Json(c): Json<PoolCoin>,
) -> Result<Json<Value>, ApiError> {
    app.engine.read().await.set_pool(c).await?;
    Ok(Json(json!({"ok":true})))
}
async fn remove(
    State(app): State<Arc<Application>>,
    Path(s): Path<String>,
) -> Result<Json<Value>, ApiError> {
    app.engine.read().await.remove_coin(&s).await?;
    Ok(Json(json!({"ok":true})))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Control {
    running: bool,
}
async fn control(
    State(app): State<Arc<Application>>,
    Json(c): Json<Control>,
) -> Result<Json<Value>, ApiError> {
    app.engine.read().await.set_running(c.running).await?;
    Ok(Json(json!({"ok":true})))
}
async fn close(
    State(app): State<Arc<Application>>,
    Path(s): Path<String>,
) -> Result<Json<Value>, ApiError> {
    app.engine.read().await.manual_close(&s).await?;
    Ok(Json(json!({"ok":true})))
}
async fn refresh(State(app): State<Arc<Application>>) -> Result<Json<Value>, ApiError> {
    app.engine.read().await.refresh_catalog().await?;
    Ok(Json(json!({"ok":true})))
}
async fn settings(State(app): State<Arc<Application>>) -> Json<SettingsView> {
    let _active = app.engine.read().await;
    Json(app.settings_view().await)
}
async fn connections(
    State(app): State<Arc<Application>>,
    Json(patch): Json<SettingsPatch>,
) -> Result<Json<SettingsView>, ApiError> {
    Ok(Json(app.configure_connections(patch).await?))
}
async fn check_connection(State(app): State<Arc<Application>>) -> Result<Json<Value>, ApiError> {
    Ok(Json(
        json!({"ok": true, "message": app.check_connection().await?}),
    ))
}
async fn test_credentials(
    State(app): State<Arc<Application>>,
    Json(request): Json<CredentialTestRequest>,
) -> Result<Json<CredentialTestResult>, ApiError> {
    Ok(Json(app.test_credentials(request).await?))
}
async fn test_jev_credentials(
    State(app): State<Arc<Application>>,
    Json(request): Json<JevCredentialTestRequest>,
) -> Result<Json<JevCredentialTestResult>, ApiError> {
    Ok(Json(app.test_jev_credentials(request).await?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use tower::ServiceExt;
    fn app() -> Router {
        app_with_bind(Ipv4Addr::LOCALHOST)
    }
    fn app_with_bind(bind: Ipv4Addr) -> Router {
        let c = Config::default();
        let e = crate::engine::Engine::new(
            crate::config::Mode::Paper,
            crate::store::Store::open(":memory:", crate::config::Mode::Paper).unwrap(),
            c,
            crate::exchange::Binance::new(crate::config::Mode::Paper).unwrap(),
            crate::jev::Jev::new().unwrap(),
        )
        .unwrap();
        router_with_bind(Application::from_engine(e), bind)
    }
    fn request(method: &str, path: &str, body: &str, cookie: Option<&str>) -> Request {
        let mut request = Request::builder()
            .method(method)
            .uri(path)
            .header("host", "127.0.0.1:8787")
            .header("origin", "http://127.0.0.1:8787")
            .header("x-genius-request", "1")
            .header("content-type", "application/json");
        if let Some(cookie) = cookie {
            request = request.header("cookie", cookie);
        }
        request.body(Body::from(body.to_string())).unwrap()
    }
    async fn admin_cookie(router: &Router) -> String {
        let response = router.clone().oneshot(request("POST", "/api/auth/setup",
            r#"{"password":"local-api-test-password","confirm_password":"local-api-test-password"}"#, None)).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let cookie = response.headers()["set-cookie"].to_str().unwrap();
        assert!(cookie.contains("HttpOnly") && cookie.contains("SameSite=Strict"));
        cookie.split(';').next().unwrap().to_string()
    }
    #[tokio::test]
    async fn usdc_pool_routes_add_update_restore_and_remove_normalized_pairs() {
        use crate::{config::Mode, engine::Engine, exchange::Binance, jev::Jev, store::Store};
        let store = Store::open(":memory:", Mode::Paper).unwrap();
        let engine = Engine::new(
            Mode::Paper,
            store.clone(),
            Config::default(),
            Binance::new(Mode::Paper).unwrap(),
            Jev::new().unwrap(),
        )
        .unwrap();
        for (symbol, volume) in [("BTCUSDT", "1000"), ("BTCUSDC", "2000")] {
            let market: SymbolInfo = serde_json::from_value(serde_json::json!({
                "symbol":symbol, "quote_volume":volume, "last_price":"60000",
                "tick_size":"0.01", "step_size":"0.001", "market_step":"0.001",
                "min_qty":"0.001", "market_min_qty":"0.001", "min_notional":"5",
                "max_qty":"1000", "market_max_qty":"1000"
            }))
            .unwrap();
            engine
                .inner
                .lock()
                .await
                .catalog
                .insert(symbol.into(), market);
        }
        let app = router(Application::from_engine(engine.clone()));
        let cookie = admin_cookie(&app).await;
        let response = app
            .clone()
            .oneshot(request("GET", "/api/markets", "", None))
            .await
            .unwrap();
        let markets: Vec<SymbolInfo> = serde_json::from_slice(
            &axum::body::to_bytes(response.into_body(), 10000)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            markets
                .iter()
                .map(|m| m.symbol.as_str())
                .collect::<Vec<_>>(),
            ["BTCUSDC", "BTCUSDT"]
        );
        for body in [
            r#"{"symbol":" btc/usdc ","enabled":true,"config":null}"#,
            r#"{"symbol":"btcusdc","enabled":false,"config":{"notional_usdt":"150","profit_usdt":"1"}}"#,
        ] {
            let response = app
                .clone()
                .oneshot(request("POST", "/api/pool", body, Some(&cookie)))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }
        let restored = Engine::new(
            Mode::Paper,
            store,
            Config::default(),
            Binance::new(Mode::Paper).unwrap(),
            Jev::new().unwrap(),
        )
        .unwrap();
        let snapshot = restored.snapshot().await;
        assert_eq!(snapshot.pool.len(), 1);
        assert_eq!(snapshot.pool[0].symbol, "BTCUSDC");
        assert!(!snapshot.pool[0].enabled);
        assert_eq!(
            snapshot.pool[0].config.as_ref().unwrap().notional_usdt,
            Some(150.into())
        );
        for symbol in ["BTCUSD", "UNKNOWNUSDC", "../USDC"] {
            let response = app
                .clone()
                .oneshot(request(
                    "POST",
                    "/api/pool",
                    &serde_json::json!({"symbol":symbol,"enabled":true,"config":null}).to_string(),
                    Some(&cookie),
                ))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        }
        let response = app
            .clone()
            .oneshot(request("GET", "/api/decisions?symbol=btc%2Fusdc", "", None))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let response = app
            .oneshot(request("DELETE", "/api/pool/btcusdc", "", Some(&cookie)))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(engine.snapshot().await.pool.is_empty());
    }
    #[tokio::test]
    async fn jev_test_route_rejects_missing_invalid_and_clear_keys_without_echoing_secrets() {
        let store = crate::store::Store::open(":memory:", crate::config::Mode::Paper).unwrap();
        let application = Application::new(store.clone()).unwrap();
        let router = router(application);
        let cookie = admin_cookie(&router).await;
        for body in [
            "{}",
            r#"{"jev_key":"local-jev-test-key invalid"}"#,
            r#"{"clear_jev":true,"jev_key":"local-jev-test-key"}"#,
        ] {
            let response = router
                .clone()
                .oneshot(request(
                    "POST",
                    "/api/settings/jev/test",
                    body,
                    Some(&cookie),
                ))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            let bytes = axum::body::to_bytes(response.into_body(), 10_000)
                .await
                .unwrap();
            let text = std::str::from_utf8(&bytes).unwrap();
            assert!(!text.contains("local-jev-test-key"));
            assert!(serde_json::from_slice::<Value>(&bytes).unwrap()["error"].is_string());
        }
        assert!(store.settings().unwrap().jev_key.is_empty());
    }
    #[tokio::test]
    async fn cross_origin_mutation_is_rejected() {
        let req = Request::builder()
            .method("POST")
            .uri("/api/control")
            .header("host", "127.0.0.1:8787")
            .header("origin", "https://evil.example")
            .header("x-genius-request", "1")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"running":false}"#))
            .unwrap();
        assert_eq!(
            app().oneshot(req).await.unwrap().status(),
            StatusCode::FORBIDDEN
        );
    }
    #[tokio::test]
    async fn header_required_for_mutation() {
        let req = Request::builder()
            .method("POST")
            .uri("/api/control")
            .header("host", "127.0.0.1:8787")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"running":false}"#))
            .unwrap();
        assert_eq!(
            app().oneshot(req).await.unwrap().status(),
            StatusCode::FORBIDDEN
        );
    }
    #[tokio::test]
    async fn status_has_no_secrets() {
        let r = app()
            .oneshot(
                Request::builder()
                    .uri("/api/state")
                    .header("host", "127.0.0.1:8787")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(r.into_body(), 1_000_000)
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["paused"], true);
        assert_eq!(v["mode"], "paper");
        assert_eq!(v["account_sync_failed"], false);
        assert_eq!(v["account_update_pending"], false);
        assert_eq!(v["user_stream"]["enabled"], false);
        assert!(v["user_stream"].get("listenKey").is_none());
        assert_eq!(v["market_stream"]["connected"], false);
        assert_eq!(v["market_stream"]["subscriptions"], 0);
        assert!(v.get("api_key").is_none());
        let limiter = &v["exchange_rate_limit"];
        assert_eq!(limiter["weight_limit"], 2400);
        assert_eq!(limiter["cooldown_persistence_pending"], false);
        assert_eq!(limiter["regular_weight_limit"], 2160);
        assert!(limiter["remaining_weight"].is_u64());
        assert!(limiter["cooldown_remaining_ms"].is_u64());
        assert!(limiter.get("charges").is_none());
        assert!(limiter.get("dispatch").is_none());
    }
    #[tokio::test]
    async fn dashboard_is_public_and_decision_history_survives_engine_restart() {
        let store = crate::store::Store::open(":memory:", crate::config::Mode::Paper).unwrap();
        let older = crate::strategy::Decision::wait("older decision");
        for (symbol, time, decision) in [
            ("BTCUSDT", 1, older),
            (
                "ETHUSDT",
                2,
                crate::strategy::Decision::wait("eth decision"),
            ),
            (
                "BTCUSDT",
                3,
                crate::strategy::Decision::wait("latest decision"),
            ),
        ] {
            store.event(&Event {
                time, level: "decision".into(), symbol: Some(symbol.into()),
                message: "5 分钟收盘决策".into(),
                detail: json!({"decision":decision,"bar_close":time-1,"candles":[{"private_input":"not in summary"}]}),
            }).unwrap();
        }
        // Keep a decision beyond the event feed's window; the card must still recover it.
        for time in 4..110 {
            store
                .event(&Event {
                    time,
                    level: "info".into(),
                    symbol: None,
                    message: "tick".into(),
                    detail: json!({}),
                })
                .unwrap();
        }
        store
            .for_mode(crate::config::Mode::Live)
            .event(&Event {
                time: 110,
                level: "decision".into(),
                symbol: Some("BTCUSDT".into()),
                message: "live only".into(),
                detail: json!({"decision":crate::strategy::Decision::wait("live only")}),
            })
            .unwrap();
        let mut book = crate::domain::Book::default();
        book.pool.insert(
            "BTCUSDT".into(),
            PoolCoin {
                symbol: "BTCUSDT".into(),
                enabled: false,
                config: None,
            },
        );
        store
            .save(crate::config::Mode::Paper, &Config::default(), &book)
            .unwrap();
        // Application::new restores the portfolio while its in-memory decisions start empty.
        let router = router(Application::new(store).unwrap());
        let response = router
            .clone()
            .oneshot(request("GET", "/api/dashboard", "", None))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), 1_000_000)
            .await
            .unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["auth"]["authenticated"], false);
        assert_eq!(value["account_sync_failed"], false);
        assert_eq!(value["account_update_pending"], false);
        assert_eq!(value["user_stream"]["connected"], false);
        assert_eq!(value["market_stream"]["subscriptions"], 0);
        assert!(value["exchange_rate_limit"]["estimated_used_weight"].is_u64());
        assert_eq!(
            value["watchlist"][0]["latest_decision"]["source"],
            "latest decision"
        );
        assert_eq!(value["watchlist"][0]["latest_decision"]["time"], 3);
        assert!(value["watchlist"][0]["price"].is_null());
        assert_eq!(value["watchlist"][0]["quote_stale"], true);
        assert_eq!(value["recent_decisions"].as_array().unwrap().len(), 3);
        assert!(
            !value["recent_decisions"]
                .to_string()
                .contains("private_input")
        );
        assert!(
            value["recent_decisions"]
                .as_array()
                .unwrap()
                .iter()
                .all(|d| d["source"] != "live only")
        );
        assert!(value.get("api_key").is_none());
        let response = router
            .clone()
            .oneshot(request(
                "GET",
                "/api/decisions?symbol=btc%2Fusdt&limit=1",
                "",
                None,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), 100_000)
            .await
            .unwrap();
        let decisions: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decisions.as_array().unwrap().len(), 1);
        assert_eq!(decisions[0]["source"], "latest decision");
        let response = router
            .oneshot(request(
                "GET",
                "/api/events?exclude_decisions=true",
                "",
                None,
            ))
            .await
            .unwrap();
        let bytes = axum::body::to_bytes(response.into_body(), 100_000)
            .await
            .unwrap();
        let events: Vec<Event> = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(events.len(), 100);
        assert!(events.iter().all(|e| e.level != "decision"));
    }
    #[tokio::test]
    async fn decision_query_limits_are_bounded() {
        let router = app();
        for query in [
            "limit=0",
            "limit=101",
            "limit=-1",
            "limit=nope",
            "extra=true",
        ] {
            assert_eq!(
                router
                    .clone()
                    .oneshot(request("GET", &format!("/api/decisions?{query}"), "", None))
                    .await
                    .unwrap()
                    .status(),
                StatusCode::BAD_REQUEST
            );
        }
    }
    #[tokio::test]
    async fn non_local_host_is_rejected() {
        let request = Request::builder()
            .uri("/api/state")
            .header("host", "attacker.example:8787")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            app().oneshot(request).await.unwrap().status(),
            StatusCode::FORBIDDEN
        );
    }
    #[tokio::test]
    async fn remote_ip_hosts_require_a_non_loopback_bind_and_domains_remain_rejected() {
        for bind in [Ipv4Addr::LOCALHOST, Ipv4Addr::UNSPECIFIED] {
            let router = app_with_bind(bind);
            for host in [
                "203.0.113.10:8787",
                "attacker.example:8787",
                "999.1.2.3:8787",
            ] {
                let response = router
                    .clone()
                    .oneshot(
                        Request::builder()
                            .uri("/")
                            .header("host", host)
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                let expected = if !bind.is_loopback() && host == "203.0.113.10:8787" {
                    StatusCode::OK
                } else {
                    StatusCode::FORBIDDEN
                };
                assert_eq!(response.status(), expected, "bind={bind}, host={host}");
            }
        }
    }
    fn remote_request(method: &str, path: &str, body: &str, cookie: Option<&str>) -> Request {
        let mut request = request(method, path, body, cookie);
        request
            .headers_mut()
            .insert("host", HeaderValue::from_static("203.0.113.10:8787"));
        request.headers_mut().insert(
            "origin",
            HeaderValue::from_static("http://203.0.113.10:8787"),
        );
        request
    }
    #[tokio::test]
    async fn remote_admin_setup_and_writes_keep_authentication_and_origin_checks() {
        let router = app_with_bind(Ipv4Addr::UNSPECIFIED);
        let settings = r#"{"port":8877,"session_minutes":30}"#;
        assert_eq!(
            router
                .clone()
                .oneshot(remote_request("PUT", "/api/server", settings, None))
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        let setup = router.clone().oneshot(remote_request("POST", "/api/auth/setup",
            r#"{"password":"remote-api-test-password","confirm_password":"remote-api-test-password"}"#, None)).await.unwrap();
        assert_eq!(setup.status(), StatusCode::OK);
        let cookie = setup.headers()["set-cookie"]
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap();
        assert_eq!(
            router
                .clone()
                .oneshot(remote_request("PUT", "/api/server", settings, None))
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        for origin in [
            "http://attacker.example",
            "http://203.0.113.10:8877",
            "null",
        ] {
            let mut request = remote_request("PUT", "/api/server", settings, Some(cookie));
            request
                .headers_mut()
                .insert("origin", HeaderValue::from_str(origin).unwrap());
            assert_eq!(
                router.clone().oneshot(request).await.unwrap().status(),
                StatusCode::FORBIDDEN
            );
        }
        let mut missing_marker = remote_request("PUT", "/api/server", settings, Some(cookie));
        missing_marker.headers_mut().remove("x-genius-request");
        assert_eq!(
            router
                .clone()
                .oneshot(missing_marker)
                .await
                .unwrap()
                .status(),
            StatusCode::FORBIDDEN
        );
        let before = router
            .clone()
            .oneshot(remote_request("GET", "/api/server", "", None))
            .await
            .unwrap();
        let body = axum::body::to_bytes(before.into_body(), 10_000)
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap()["port"],
            8787
        );
        assert_eq!(
            router
                .clone()
                .oneshot(remote_request("PUT", "/api/server", settings, Some(cookie)))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        let after = router
            .oneshot(remote_request("GET", "/api/server", "", None))
            .await
            .unwrap();
        let body = axum::body::to_bytes(after.into_body(), 10_000)
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap()["port"],
            8877
        );
    }
    #[tokio::test]
    async fn page_settings_save_persist_and_return_only_masked_credentials() {
        let store = crate::store::Store::open(":memory:", crate::config::Mode::Paper).unwrap();
        let application = Application::new(store.clone()).unwrap();
        let router = router(application);
        let cookie = admin_cookie(&router).await;
        let request = Request::builder().method("PUT").uri("/api/settings")
            .header("host","127.0.0.1:8787").header("origin","http://127.0.0.1:8787")
            .header("x-genius-request","1").header("content-type","application/json").header("cookie", cookie)
            .body(Body::from(r#"{"testnet":{"key":"local-api-key-abcdef","secret":"local-secret-abcdef"},"jev_key":"local-jev-abcdef"}"#)).unwrap();
        let response = router.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1_000_000)
            .await
            .unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        assert!(
            !text.contains("local-api-key-abcdef")
                && !text.contains("local-secret-abcdef")
                && !text.contains("local-jev-abcdef")
        );
        assert!(store.settings().unwrap().testnet.configured());
        let restarted = Application::new(store.clone()).unwrap();
        let r = super::router(restarted)
            .oneshot(
                Request::builder()
                    .uri("/api/state")
                    .header("host", "127.0.0.1:8787")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(r.into_body(), 1_000_000)
            .await
            .unwrap();
        let value: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["settings"]["testnet"]["configured"], true);
        assert_eq!(value["jev_available"], true);
        assert!(value["settings"]["testnet"].get("secret").is_none());
        let events = serde_json::to_string(&store.events(100).unwrap()).unwrap();
        assert!(!events.contains("local-secret-abcdef") && !events.contains("local-jev-abcdef"));
    }
    #[tokio::test]
    async fn guests_read_but_every_write_requires_admin_before_and_after_setup() {
        let router = app();
        for path in [
            "/api/state",
            "/api/markets",
            "/api/events",
            "/api/settings",
            "/api/server",
            "/api/auth/status",
        ] {
            assert_eq!(
                router
                    .clone()
                    .oneshot(request("GET", path, "", None))
                    .await
                    .unwrap()
                    .status(),
                StatusCode::OK
            );
        }
        let writes = [
            ("PUT", "/api/config"),
            ("PUT", "/api/settings"),
            ("PUT", "/api/server"),
            ("POST", "/api/settings/check"),
            ("POST", "/api/settings/test"),
            ("POST", "/api/settings/jev/test"),
            ("POST", "/api/pool"),
            ("DELETE", "/api/pool/BTCUSDT"),
            ("POST", "/api/control"),
            ("POST", "/api/positions/BTCUSDT/close"),
            ("POST", "/api/markets/refresh"),
            ("POST", "/api/auth/password"),
        ];
        for (method, path) in writes {
            assert_eq!(
                router
                    .clone()
                    .oneshot(request(method, path, "{}", None))
                    .await
                    .unwrap()
                    .status(),
                StatusCode::UNAUTHORIZED,
                "{path}"
            );
        }
        let cookie = admin_cookie(&router).await;
        for (method, path) in writes {
            assert_eq!(
                router
                    .clone()
                    .oneshot(request(method, path, "{}", None))
                    .await
                    .unwrap()
                    .status(),
                StatusCode::UNAUTHORIZED,
                "{path}"
            );
            assert_eq!(
                router
                    .clone()
                    .oneshot(request(method, path, "{}", Some("genius_admin=forged")))
                    .await
                    .unwrap()
                    .status(),
                StatusCode::UNAUTHORIZED,
                "{path}"
            );
        }
        assert_eq!(
            router
                .clone()
                .oneshot(request(
                    "POST",
                    "/api/control",
                    r#"{"running":false}"#,
                    Some(&cookie)
                ))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        assert_eq!(router.clone().oneshot(request("POST","/api/auth/setup",r#"{"password":"overwrite-admin-pass","confirm_password":"overwrite-admin-pass"}"#,None)).await.unwrap().status(), StatusCode::CONFLICT);
        assert_eq!(
            router
                .clone()
                .oneshot(request("POST", "/api/auth/logout", "", Some(&cookie)))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            router
                .clone()
                .oneshot(request(
                    "POST",
                    "/api/control",
                    r#"{"running":false}"#,
                    Some(&cookie)
                ))
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            router
                .oneshot(request("GET", "/api/state", "", None))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
    }
    #[tokio::test]
    async fn credential_test_validation_does_not_save_credentials_or_change_the_engine() {
        let store = crate::store::Store::open(":memory:", crate::config::Mode::Paper).unwrap();
        let application = Application::new(store.clone()).unwrap();
        let active = application.engine.read().await.clone();
        let router = router(application.clone());
        let cookie = admin_cookie(&router).await;
        for body in [
            r#"{"mode":"testnet"}"#,
            r#"{"mode":"live","credentials":{"key":"unsaved-key"}}"#,
            r#"{"mode":"live","credentials":{"clear":true}}"#,
            r#"{"mode":"paper","credentials":{"key":"unsaved-key","secret":"unsaved-secret"}}"#,
        ] {
            let response = router
                .clone()
                .oneshot(request("POST", "/api/settings/test", body, Some(&cookie)))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            let bytes = axum::body::to_bytes(response.into_body(), 10_000)
                .await
                .unwrap();
            let text = std::str::from_utf8(&bytes).unwrap();
            assert!(!text.contains("unsaved-key") && !text.contains("unsaved-secret"));
        }
        assert!(store.settings().unwrap() == crate::settings::AppSettings::default());
        assert!(Arc::ptr_eq(&active, &*application.engine.read().await));
        assert_eq!(active.mode, crate::config::Mode::Paper);
    }
    #[tokio::test]
    async fn login_and_system_settings_persist_without_exposing_password_or_session() {
        let store = crate::store::Store::open(":memory:", crate::config::Mode::Paper).unwrap();
        let router = super::router(Application::new(store.clone()).unwrap());
        let cookie = admin_cookie(&router).await;
        assert_eq!(
            router
                .clone()
                .oneshot(request(
                    "PUT",
                    "/api/server",
                    r#"{"port":8899,"session_minutes":5}"#,
                    Some(&cookie)
                ))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        assert_eq!(store.server_settings().unwrap().port, 8899);
        assert_eq!(
            router
                .clone()
                .oneshot(request(
                    "PUT",
                    "/api/server",
                    r#"{"port":0,"session_minutes":0}"#,
                    Some(&cookie)
                ))
                .await
                .unwrap()
                .status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(store.server_settings().unwrap().port, 8899);
        let restarted = super::router(Application::new(store.clone()).unwrap());
        let response = restarted
            .clone()
            .oneshot(request(
                "POST",
                "/api/auth/login",
                r#"{"password":"local-api-test-password"}"#,
                None,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let new_cookie = response.headers()["set-cookie"]
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap()
            .to_string();
        assert!(
            response.headers()["set-cookie"]
                .to_str()
                .unwrap()
                .contains("Max-Age=300")
        );
        let response = restarted
            .clone()
            .oneshot(request("GET", "/api/state", "", Some(&new_cookie)))
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), 1_000_000)
            .await
            .unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        let value: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["auth"]["authenticated"], true);
        assert_eq!(value["server"]["port"], 8899);
        assert!(
            !text.contains("local-api-test-password")
                && !text.contains("$argon2")
                && !text.contains(new_cookie.split('=').nth(1).unwrap())
        );
        assert_eq!(
            restarted
                .oneshot(request(
                    "POST",
                    "/api/auth/login",
                    r#"{"password":"wrong-password"}"#,
                    None
                ))
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
    }
}
