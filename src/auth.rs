use crate::{domain::now_ms, store::Store};
use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier, password_hash::SaltString};
use axum::{
    Json,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

pub const COOKIE: &str = "genius_admin";

pub struct Auth {
    store: Store,
    gate: Mutex<()>,
}
#[derive(Serialize)]
pub struct AuthStatus {
    pub initialized: bool,
    pub authenticated: bool,
    pub expires_at: Option<i64>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PasswordInput {
    pub password: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetupInput {
    pub password: String,
    pub confirm_password: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChangePasswordInput {
    pub current_password: String,
    pub password: String,
    pub confirm_password: String,
}
pub struct Session {
    pub token: String,
    pub expires_at: i64,
    pub max_age: u32,
}
#[derive(Debug)]
pub struct AuthError(pub StatusCode, pub &'static str);
impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({"error":self.1}))).into_response()
    }
}
impl From<anyhow::Error> for AuthError {
    fn from(_: anyhow::Error) -> Self {
        Self(StatusCode::INTERNAL_SERVER_ERROR, "身份验证服务不可用")
    }
}
fn random<const N: usize>() -> Result<[u8; N], AuthError> {
    let mut bytes = [0; N];
    getrandom::fill(&mut bytes)
        .map_err(|_| AuthError(StatusCode::INTERNAL_SERVER_ERROR, "无法生成安全随机数"))?;
    Ok(bytes)
}
fn token_hash(token: &str) -> String {
    hex::encode(Sha256::digest(token.as_bytes()))
}
fn validate_password(password: &str, confirm: &str) -> Result<(), AuthError> {
    if !(12..=128).contains(&password.chars().count()) || password.len() > 512 {
        return Err(AuthError(
            StatusCode::BAD_REQUEST,
            "密码长度应为 12–128 个字符",
        ));
    }
    if password.trim().is_empty() {
        return Err(AuthError(StatusCode::BAD_REQUEST, "密码不能全部为空白"));
    }
    if password != confirm {
        return Err(AuthError(StatusCode::BAD_REQUEST, "两次输入的密码不一致"));
    }
    Ok(())
}
async fn hash_password(password: String) -> Result<String, AuthError> {
    let salt = SaltString::encode_b64(&random::<16>()?)
        .map_err(|_| AuthError(StatusCode::INTERNAL_SERVER_ERROR, "无法生成密码盐"))?;
    tokio::task::spawn_blocking(move || {
        Argon2::default()
            .hash_password(password.as_bytes(), &salt)
            .map(|h| h.to_string())
            .map_err(|_| AuthError(StatusCode::INTERNAL_SERVER_ERROR, "无法保存密码"))
    })
    .await
    .map_err(|_| AuthError(StatusCode::INTERNAL_SERVER_ERROR, "密码处理失败"))?
}
async fn verify(password: String, hash: String) -> Result<bool, AuthError> {
    if password.len() > 512 {
        return Ok(false);
    }
    tokio::task::spawn_blocking(move || {
        let parsed = PasswordHash::new(&hash)
            .map_err(|_| AuthError(StatusCode::INTERNAL_SERVER_ERROR, "密码记录无效"))?;
        Ok(Argon2::default()
            .verify_password(password.as_bytes(), &parsed)
            .is_ok())
    })
    .await
    .map_err(|_| AuthError(StatusCode::INTERNAL_SERVER_ERROR, "密码处理失败"))?
}
pub fn cookie_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get_all("cookie")
        .iter()
        .filter_map(|h| h.to_str().ok())
        .flat_map(|s| s.split(';'))
        .find_map(|part| {
            let (key, value) = part.trim().split_once('=')?;
            (key == COOKIE && value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit()))
                .then_some(value)
        })
}
impl Session {
    pub fn cookie(&self) -> String {
        format!(
            "{COOKIE}={}; Path=/; HttpOnly; SameSite=Strict; Max-Age={}",
            self.token, self.max_age
        )
    }
}
pub fn clear_cookie() -> String {
    format!("{COOKIE}=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0")
}
impl Auth {
    pub fn new(store: Store) -> Self {
        Self {
            store,
            gate: Mutex::new(()),
        }
    }
    pub fn status(&self, token: Option<&str>) -> Result<AuthStatus, AuthError> {
        let initialized = self.store.admin_hash()?.is_some();
        let expires_at = if initialized {
            token
                .map(|t| self.store.session_expiry(&token_hash(t), now_ms()))
                .transpose()?
                .flatten()
        } else {
            None
        };
        Ok(AuthStatus {
            initialized,
            authenticated: expires_at.is_some(),
            expires_at,
        })
    }
    fn session(&self) -> Result<Session, AuthError> {
        let token = hex::encode(random::<32>()?);
        let max_age = self.store.server_settings()?.session_minutes * 60;
        let now = now_ms();
        let expires_at = now + i64::from(max_age) * 1000;
        self.store
            .create_session(&token_hash(&token), expires_at, now)?;
        Ok(Session {
            token,
            expires_at,
            max_age,
        })
    }
    pub async fn setup(&self, input: SetupInput) -> Result<Session, AuthError> {
        let _gate = self.gate.lock().await;
        if self.store.admin_hash()?.is_some() {
            return Err(AuthError(StatusCode::CONFLICT, "管理员已初始化，请登录"));
        }
        validate_password(&input.password, &input.confirm_password)?;
        let hash = hash_password(input.password).await?;
        if !self.store.initialize_admin(&hash, now_ms())? {
            return Err(AuthError(StatusCode::CONFLICT, "管理员已初始化，请登录"));
        }
        self.session()
    }
    fn check_throttle(&self) -> Result<(), AuthError> {
        if self.store.login_locked_until()? > now_ms() {
            return Err(AuthError(
                StatusCode::TOO_MANY_REQUESTS,
                "密码错误次数过多，请一分钟后重试",
            ));
        }
        Ok(())
    }
    async fn verify_current(&self, password: String) -> Result<(), AuthError> {
        self.check_throttle()?;
        let hash = self
            .store
            .admin_hash()?
            .ok_or(AuthError(StatusCode::CONFLICT, "请先设置管理员密码"))?;
        if !verify(password, hash).await? {
            self.store.failed_login(now_ms())?;
            return Err(AuthError(StatusCode::UNAUTHORIZED, "管理员密码不正确"));
        }
        self.store.clear_login_failures()?;
        Ok(())
    }
    pub async fn login(&self, input: PasswordInput) -> Result<Session, AuthError> {
        let _gate = self
            .gate
            .try_lock()
            .map_err(|_| AuthError(StatusCode::TOO_MANY_REQUESTS, "正在验证密码，请稍后重试"))?;
        self.verify_current(input.password).await?;
        self.session()
    }
    pub fn logout(&self, token: Option<&str>) -> Result<(), AuthError> {
        if let Some(token) = token {
            self.store.delete_session(&token_hash(token))?;
        }
        Ok(())
    }
    pub async fn change_password(&self, input: ChangePasswordInput) -> Result<Session, AuthError> {
        let _gate = self
            .gate
            .try_lock()
            .map_err(|_| AuthError(StatusCode::TOO_MANY_REQUESTS, "正在验证密码，请稍后重试"))?;
        self.verify_current(input.current_password).await?;
        validate_password(&input.password, &input.confirm_password)?;
        let hash = hash_password(input.password).await?;
        self.store.change_admin_password(&hash)?;
        self.session()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Mode;
    fn setup(password: &str) -> SetupInput {
        SetupInput {
            password: password.into(),
            confirm_password: password.into(),
        }
    }
    #[tokio::test]
    async fn password_hash_sessions_restart_logout_expiry_and_rotation() {
        let store = Store::open(":memory:", Mode::Paper).unwrap();
        let auth = Auth::new(store.clone());
        assert!(!auth.status(None).unwrap().initialized);
        assert!(auth.setup(setup("short")).await.is_err());
        let first = auth.setup(setup("local-test-password-1")).await.unwrap();
        let hash = store.admin_hash().unwrap().unwrap();
        assert!(hash.starts_with("$argon2id$") && !hash.contains("local-test-password"));
        assert_eq!(
            auth.setup(setup("another-test-password"))
                .await
                .err()
                .unwrap()
                .0,
            StatusCode::CONFLICT
        );
        let restarted = Auth::new(store.clone());
        assert!(restarted.status(Some(&first.token)).unwrap().authenticated);
        assert!(
            !restarted
                .status(Some(&"0".repeat(64)))
                .unwrap()
                .authenticated
        );
        let second = restarted
            .login(PasswordInput {
                password: "local-test-password-1".into(),
            })
            .await
            .unwrap();
        restarted.logout(Some(&first.token)).unwrap();
        assert!(!auth.status(Some(&first.token)).unwrap().authenticated);
        store
            .create_session(&token_hash(&first.token), now_ms() - 1, now_ms())
            .unwrap();
        assert!(!auth.status(Some(&first.token)).unwrap().authenticated);
        let third = auth
            .change_password(ChangePasswordInput {
                current_password: "local-test-password-1".into(),
                password: "local-test-password-2".into(),
                confirm_password: "local-test-password-2".into(),
            })
            .await
            .unwrap();
        assert!(!auth.status(Some(&second.token)).unwrap().authenticated);
        assert!(auth.status(Some(&third.token)).unwrap().authenticated);
        assert!(
            auth.login(PasswordInput {
                password: "local-test-password-1".into()
            })
            .await
            .is_err()
        );
        assert!(
            auth.login(PasswordInput {
                password: "local-test-password-2".into()
            })
            .await
            .is_ok()
        );
    }
    #[tokio::test]
    async fn administrator_and_sessions_survive_closing_and_reopening_sqlite() {
        let path = std::env::temp_dir().join(format!(
            "genius-auth-{}-{}.sqlite",
            std::process::id(),
            now_ms()
        ));
        let token = {
            let store = Store::open(path.to_str().unwrap(), Mode::Paper).unwrap();
            let auth = Auth::new(store);
            auth.setup(setup("local-file-test-password"))
                .await
                .unwrap()
                .token
        };
        {
            let store = Store::open(path.to_str().unwrap(), Mode::Paper).unwrap();
            let auth = Auth::new(store);
            assert!(auth.status(Some(&token)).unwrap().authenticated);
            assert_eq!(
                auth.setup(setup("overwrite-test-password"))
                    .await
                    .err()
                    .unwrap()
                    .0,
                StatusCode::CONFLICT
            );
        }
        std::fs::remove_file(path).unwrap();
    }
    #[tokio::test]
    async fn login_failures_are_throttled_across_restarts() {
        let store = Store::open(":memory:", Mode::Paper).unwrap();
        let auth = Auth::new(store.clone());
        auth.setup(setup("local-test-password")).await.unwrap();
        for _ in 0..5 {
            assert_eq!(
                auth.login(PasswordInput {
                    password: "wrong".into()
                })
                .await
                .err()
                .unwrap()
                .0,
                StatusCode::UNAUTHORIZED
            );
        }
        let restarted = Auth::new(store);
        assert_eq!(
            restarted
                .login(PasswordInput {
                    password: "local-test-password".into()
                })
                .await
                .err()
                .unwrap()
                .0,
            StatusCode::TOO_MANY_REQUESTS
        );
    }
}
