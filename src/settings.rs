use crate::config::Mode;
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct ServerSettings {
    pub port: u16,
    pub session_minutes: u32,
}
impl Default for ServerSettings {
    fn default() -> Self {
        Self {
            port: 8787,
            session_minutes: 30,
        }
    }
}
impl ServerSettings {
    pub fn validate(&self) -> Result<()> {
        ensure!(self.port >= 1024, "监听端口范围为 1024–65535");
        ensure!(
            (5..=1440).contains(&self.session_minutes),
            "登录会话时长范围为 5–1440 分钟"
        );
        Ok(())
    }
}

// Never derive Debug, or expose this type through an HTTP response or event.
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Credentials {
    pub key: String,
    pub secret: String,
}
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AppSettings {
    pub mode: Mode,
    pub live_enabled: bool,
    pub testnet: Credentials,
    pub live: Credentials,
    pub jev_key: String,
}
#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CredentialPatch {
    pub key: Option<String>,
    pub secret: Option<String>,
    pub clear: bool,
}
#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SettingsPatch {
    pub mode: Option<Mode>,
    pub live_enabled: Option<bool>,
    pub testnet: CredentialPatch,
    pub live: CredentialPatch,
    pub jev_key: Option<String>,
    pub clear_jev: bool,
}
#[derive(Serialize)]
pub struct CredentialView {
    pub configured: bool,
    pub key_masked: Option<String>,
    pub secret_configured: bool,
}
#[derive(Serialize)]
pub struct SettingsView {
    pub mode: Mode,
    pub live_enabled: bool,
    pub testnet: CredentialView,
    pub live: CredentialView,
    pub jev_configured: bool,
    pub jev_key_masked: Option<String>,
}
fn update_secret(current: &mut String, update: Option<String>) -> Result<()> {
    if let Some(value) = update {
        let value = value.trim();
        if !value.is_empty() {
            ensure!(
                value.len() <= 512 && value.bytes().all(|b| b.is_ascii_graphic()),
                "密钥格式无效"
            );
            *current = value.into();
        }
    }
    Ok(())
}
fn masked(value: &str) -> Option<String> {
    if value.is_empty() {
        None
    } else if value.len() <= 8 {
        Some("••••••••".into())
    } else {
        Some(format!("••••••••{}", &value[value.len() - 4..]))
    }
}
impl Credentials {
    pub fn configured(&self) -> bool {
        !self.key.is_empty() && !self.secret.is_empty()
    }
    fn apply(&mut self, patch: CredentialPatch) -> Result<()> {
        if patch.clear {
            ensure!(
                patch.key.as_ref().is_none_or(|k| k.trim().is_empty())
                    && patch.secret.as_ref().is_none_or(|k| k.trim().is_empty()),
                "清除账户时不能同时填写新密钥"
            );
            *self = Self::default();
        } else {
            update_secret(&mut self.key, patch.key)?;
            update_secret(&mut self.secret, patch.secret)?;
        }
        ensure!(
            self.key.is_empty() == self.secret.is_empty(),
            "API Key 与 Secret 必须同时配置"
        );
        Ok(())
    }
    fn view(&self) -> CredentialView {
        CredentialView {
            configured: self.configured(),
            key_masked: masked(&self.key),
            secret_configured: !self.secret.is_empty(),
        }
    }
}
impl AppSettings {
    pub fn credentials(&self) -> &Credentials {
        if self.mode == Mode::Testnet {
            &self.testnet
        } else {
            &self.live
        }
    }
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.mode != Mode::Live || self.live_enabled,
            "选择实盘前请勾选启用实盘交易"
        );
        for c in [&self.testnet, &self.live] {
            ensure!(
                c.key.is_empty() == c.secret.is_empty(),
                "API Key 与 Secret 必须同时配置"
            );
            for value in [&c.key, &c.secret] {
                ensure!(
                    value.len() <= 512 && value.bytes().all(|b| b.is_ascii_graphic()),
                    "密钥格式无效"
                );
            }
        }
        ensure!(
            self.jev_key.len() <= 512 && self.jev_key.bytes().all(|b| b.is_ascii_graphic()),
            "Jev 密钥格式无效"
        );
        Ok(())
    }
    pub fn patched(&self, patch: SettingsPatch) -> Result<Self> {
        let mut next = self.clone();
        if let Some(mode) = patch.mode {
            next.mode = mode;
        }
        if let Some(enabled) = patch.live_enabled {
            next.live_enabled = enabled;
        }
        next.testnet.apply(patch.testnet)?;
        next.live.apply(patch.live)?;
        if patch.clear_jev {
            ensure!(
                patch.jev_key.as_ref().is_none_or(|k| k.trim().is_empty()),
                "清除 Jev 时不能同时填写新密钥"
            );
            next.jev_key.clear();
        } else {
            update_secret(&mut next.jev_key, patch.jev_key)?;
        }
        next.validate()?;
        Ok(next)
    }
    pub fn same_account(&self, other: &Self) -> bool {
        self.mode == other.mode
            && (self.mode == Mode::Paper || self.credentials() == other.credentials())
    }
    pub fn view(&self) -> SettingsView {
        SettingsView {
            mode: self.mode,
            live_enabled: self.live_enabled,
            testnet: self.testnet.view(),
            live: self.live.view(),
            jev_configured: !self.jev_key.is_empty(),
            jev_key_masked: masked(&self.jev_key),
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn blank_preserves_clear_removes_and_view_never_contains_secrets() {
        let s = AppSettings::default().patched(serde_json::from_value(serde_json::json!({"testnet":{"key":"testnet-key-abcdef","secret":"testnet-secret-abcdef"},"jev_key":"jev-secret-abcdef"})).unwrap()).unwrap();
        let kept = s
            .patched(
                serde_json::from_value(
                    serde_json::json!({"testnet":{"key":"","secret":""},"jev_key":""}),
                )
                .unwrap(),
            )
            .unwrap();
        assert!(kept == s);
        let public = serde_json::to_string(&s.view()).unwrap();
        assert!(
            !public.contains("testnet-key-abcdef")
                && !public.contains("testnet-secret-abcdef")
                && !public.contains("jev-secret-abcdef")
        );
        let cleared = s
            .patched(
                serde_json::from_value(
                    serde_json::json!({"testnet":{"clear":true},"clear_jev":true}),
                )
                .unwrap(),
            )
            .unwrap();
        assert!(!cleared.testnet.configured() && cleared.jev_key.is_empty());
    }
    #[test]
    fn live_requires_explicit_enable_and_partial_keys_are_rejected() {
        assert!(
            AppSettings::default()
                .patched(serde_json::from_value(serde_json::json!({"mode":"live"})).unwrap())
                .is_err()
        );
        assert!(
            AppSettings::default()
                .patched(
                    serde_json::from_value(serde_json::json!({"testnet":{"key":"only-key"}}))
                        .unwrap()
                )
                .is_err()
        );
    }
}
