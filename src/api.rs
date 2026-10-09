use crate::{
    application::Application,
    auth::{self, AuthError, AuthStatus, ChangePasswordInput, PasswordInput, Session, SetupInput},
    config::Config,
    domain::{Event, PoolCoin, SymbolInfo},
    engine::Snapshot,
    settings::{
        CredentialTestRequest, CredentialTestResult, JevCredentialTestRequest,
        JevCredentialTestResult, ServerSettings, SettingsPatch, SettingsView,
    },
};
use axum::{
    Json, Router,
    extract::{Path, Request, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;

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
        .layer(middleware::from_fn(local_request_guard))
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
async fn local_request_guard(req: Request, next: Next) -> Response {
    let local_host = req
        .headers()
        .get("host")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<axum::http::uri::Authority>().ok())
        .is_some_and(|a| matches!(a.host(), "localhost" | "127.0.0.1"));
    if !local_host {
        return StatusCode::FORBIDDEN.into_response();
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
async fn events(State(app): State<Arc<Application>>) -> Result<Json<Vec<Event>>, ApiError> {
    let e = app.engine.read().await;
    Ok(Json(e.store.events(100)?))
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
        let c = Config::default();
        let e = crate::engine::Engine::new(
            crate::config::Mode::Paper,
            crate::store::Store::open(":memory:", crate::config::Mode::Paper).unwrap(),
            c,
            crate::exchange::Binance::new(crate::config::Mode::Paper).unwrap(),
            crate::jev::Jev::new().unwrap(),
        )
        .unwrap();
        router(Application::from_engine(e))
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
        assert!(v.get("api_key").is_none());
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
