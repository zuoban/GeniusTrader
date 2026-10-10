use crate::{
    config::{Config, Mode},
    domain::{Book, Event},
    settings::{AppSettings, ServerSettings},
};
use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OptionalExtension, params};
use std::{
    path::Path,
    sync::{Arc, Mutex},
};

#[derive(Clone)]
pub struct Store {
    connection: Arc<Mutex<Connection>>,
    mode: Mode,
}
fn tag(mode: Mode) -> &'static str {
    match mode {
        Mode::Paper => "paper",
        Mode::Testnet => "testnet",
        Mode::Live => "live",
    }
}
impl Store {
    pub fn open(path: &str, mode: Mode) -> Result<Self> {
        if path != ":memory:" {
            if let Some(parent) = Path::new(path)
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
            {
                std::fs::create_dir_all(parent)?;
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
                std::fs::OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .write(true)
                    .mode(0o600)
                    .open(path)?;
                std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
            }
        }
        let mut connection = Connection::open(path)?;
        connection.busy_timeout(std::time::Duration::from_secs(5))?;
        let tx = connection.transaction()?;
        tx.execute_batch("CREATE TABLE IF NOT EXISTS portfolios (mode TEXT PRIMARY KEY, config TEXT NOT NULL, book TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS app_settings (id INTEGER PRIMARY KEY CHECK(id=1), body TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS server_settings (id INTEGER PRIMARY KEY CHECK(id=1), body TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS administrator (id INTEGER PRIMARY KEY CHECK(id=1), password_hash TEXT NOT NULL, created_at INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS admin_sessions (token_hash TEXT PRIMARY KEY, expires_at INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS auth_throttle (id INTEGER PRIMARY KEY CHECK(id=1), failures INTEGER NOT NULL, locked_until INTEGER NOT NULL, last_attempt INTEGER NOT NULL);
            INSERT OR IGNORE INTO auth_throttle VALUES(1,0,0,0);
            CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY AUTOINCREMENT, time INTEGER NOT NULL, body TEXT NOT NULL, mode TEXT NOT NULL DEFAULT 'paper');")?;
        tx.execute(
            "INSERT OR IGNORE INTO server_settings(id,body) VALUES(1,?1)",
            [serde_json::to_string(&ServerSettings::default())?],
        )?;
        let legacy: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='state')",
            [],
            |r| r.get(0),
        )?;
        let legacy_mode = if legacy {
            let row: Option<(String, String, String)> = tx
                .query_row("SELECT mode,config,book FROM state WHERE id=1", [], |r| {
                    Ok((r.get(0)?, r.get(1)?, r.get(2)?))
                })
                .optional()?;
            if let Some((mode, config, book)) = row {
                let mode: Mode = serde_json::from_str(&mode)?;
                tx.execute(
                    "INSERT OR IGNORE INTO portfolios(mode,config,book) VALUES(?1,?2,?3)",
                    params![tag(mode), config, book],
                )?;
                Some(mode)
            } else {
                None
            }
        } else {
            None
        };
        let has_mode = {
            let mut q = tx.prepare("PRAGMA table_info(events)")?;
            let cols = q
                .query_map([], |r| r.get::<_, String>(1))?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            cols.iter().any(|c| c == "mode")
        };
        if !has_mode {
            tx.execute_batch("ALTER TABLE events ADD COLUMN mode TEXT NOT NULL DEFAULT 'paper'")?;
            if let Some(mode) = legacy_mode {
                tx.execute("UPDATE events SET mode=?1", [tag(mode)])?;
            }
        }
        tx.commit()?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "FULL")?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
            mode,
        })
    }
    pub fn for_mode(&self, mode: Mode) -> Self {
        Self {
            connection: self.connection.clone(),
            mode,
        }
    }
    pub fn load(&self) -> Result<Option<(Config, Book)>> {
        let c = self.connection.lock().unwrap();
        let row: Option<(String, String)> = c
            .query_row(
                "SELECT config,book FROM portfolios WHERE mode=?1",
                [tag(self.mode)],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        row.map(|(config, book)| {
            Ok((
                serde_json::from_str(&config).context("读取配置失败")?,
                serde_json::from_str(&book).context("读取持仓失败")?,
            ))
        })
        .transpose()
    }
    pub fn save(&self, mode: Mode, config: &Config, book: &Book) -> Result<()> {
        ensure!(mode == self.mode, "禁止跨交易模式写入持仓");
        self.connection.lock().unwrap().execute("INSERT INTO portfolios(mode,config,book) VALUES(?1,?2,?3) ON CONFLICT(mode) DO UPDATE SET config=excluded.config,book=excluded.book",params![tag(mode),serde_json::to_string(config)?,serde_json::to_string(book)?])?;
        Ok(())
    }
    pub fn settings(&self) -> Result<AppSettings> {
        let body: Option<String> = self
            .connection
            .lock()
            .unwrap()
            .query_row("SELECT body FROM app_settings WHERE id=1", [], |r| r.get(0))
            .optional()?;
        let settings: AppSettings = body
            .map(|s| serde_json::from_str(&s))
            .transpose()?
            .unwrap_or_default();
        settings.validate()?;
        Ok(settings)
    }
    pub fn save_settings(&self, settings: &AppSettings) -> Result<()> {
        settings.validate()?;
        self.connection.lock().unwrap().execute("INSERT INTO app_settings(id,body) VALUES(1,?1) ON CONFLICT(id) DO UPDATE SET body=excluded.body", [serde_json::to_string(settings)?])?;
        Ok(())
    }
    pub fn server_settings(&self) -> Result<ServerSettings> {
        let body: String = self.connection.lock().unwrap().query_row(
            "SELECT body FROM server_settings WHERE id=1",
            [],
            |r| r.get(0),
        )?;
        let settings: ServerSettings = serde_json::from_str(&body)?;
        settings.validate()?;
        Ok(settings)
    }
    pub fn save_server_settings(&self, settings: &ServerSettings) -> Result<()> {
        settings.validate()?;
        self.connection.lock().unwrap().execute(
            "UPDATE server_settings SET body=?1 WHERE id=1",
            [serde_json::to_string(settings)?],
        )?;
        Ok(())
    }
    pub(crate) fn admin_hash(&self) -> Result<Option<String>> {
        Ok(self
            .connection
            .lock()
            .unwrap()
            .query_row(
                "SELECT password_hash FROM administrator WHERE id=1",
                [],
                |r| r.get(0),
            )
            .optional()?)
    }
    pub fn admin_initialized(&self) -> Result<bool> {
        Ok(self.admin_hash()?.is_some())
    }
    pub(crate) fn initialize_admin(&self, hash: &str, now: i64) -> Result<bool> {
        Ok(self.connection.lock().unwrap().execute(
            "INSERT OR IGNORE INTO administrator(id,password_hash,created_at) VALUES(1,?1,?2)",
            params![hash, now],
        )? == 1)
    }
    pub(crate) fn change_admin_password(&self, hash: &str) -> Result<()> {
        let mut c = self.connection.lock().unwrap();
        let tx = c.transaction()?;
        ensure!(
            tx.execute(
                "UPDATE administrator SET password_hash=?1 WHERE id=1",
                [hash]
            )? == 1,
            "管理员尚未初始化"
        );
        tx.execute("DELETE FROM admin_sessions", [])?;
        tx.commit()?;
        Ok(())
    }
    pub(crate) fn create_session(&self, hash: &str, expires_at: i64, now: i64) -> Result<()> {
        let mut c = self.connection.lock().unwrap();
        let tx = c.transaction()?;
        tx.execute("DELETE FROM admin_sessions WHERE expires_at<=?1", [now])?;
        // Keep a bounded number of sessions; the oldest is retired on the 21st login.
        tx.execute("DELETE FROM admin_sessions WHERE token_hash IN (SELECT token_hash FROM admin_sessions ORDER BY expires_at DESC LIMIT -1 OFFSET 19)", [])?;
        tx.execute(
            "INSERT INTO admin_sessions(token_hash,expires_at) VALUES(?1,?2)",
            params![hash, expires_at],
        )?;
        tx.commit()?;
        Ok(())
    }
    pub(crate) fn session_expiry(&self, hash: &str, now: i64) -> Result<Option<i64>> {
        Ok(self
            .connection
            .lock()
            .unwrap()
            .query_row(
                "SELECT expires_at FROM admin_sessions WHERE token_hash=?1 AND expires_at>?2",
                params![hash, now],
                |r| r.get(0),
            )
            .optional()?)
    }
    pub(crate) fn delete_session(&self, hash: &str) -> Result<()> {
        self.connection
            .lock()
            .unwrap()
            .execute("DELETE FROM admin_sessions WHERE token_hash=?1", [hash])?;
        Ok(())
    }
    pub(crate) fn login_locked_until(&self) -> Result<i64> {
        Ok(self.connection.lock().unwrap().query_row(
            "SELECT locked_until FROM auth_throttle WHERE id=1",
            [],
            |r| r.get(0),
        )?)
    }
    pub(crate) fn failed_login(&self, now: i64) -> Result<()> {
        let mut c = self.connection.lock().unwrap();
        let tx = c.transaction()?;
        let (count, last): (i64, i64) = tx.query_row(
            "SELECT failures,last_attempt FROM auth_throttle WHERE id=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        let failures = if now - last > 60_000 { 1 } else { count + 1 };
        let locked = if failures >= 5 { now + 60_000 } else { 0 };
        tx.execute(
            "UPDATE auth_throttle SET failures=?1,locked_until=?2,last_attempt=?3 WHERE id=1",
            params![failures, locked, now],
        )?;
        tx.commit()?;
        Ok(())
    }
    pub(crate) fn clear_login_failures(&self) -> Result<()> {
        self.connection.lock().unwrap().execute(
            "UPDATE auth_throttle SET failures=0,locked_until=0,last_attempt=0 WHERE id=1",
            [],
        )?;
        Ok(())
    }
    pub fn event(&self, event: &Event) -> Result<()> {
        self.connection.lock().unwrap().execute(
            "INSERT INTO events(time,body,mode) VALUES(?1,?2,?3)",
            params![event.time, serde_json::to_string(event)?, tag(self.mode)],
        )?;
        Ok(())
    }
    pub fn events(&self, limit: usize) -> Result<Vec<Event>> {
        let c = self.connection.lock().unwrap();
        let mut stmt =
            c.prepare("SELECT body FROM events WHERE mode=?1 ORDER BY id DESC LIMIT ?2")?;
        let texts = stmt.query_map(params![tag(self.mode), limit.min(500) as i64], |r| {
            r.get::<_, String>(0)
        })?;
        texts.map(|r| Ok(serde_json::from_str(&r?)?)).collect()
    }
    /// Import a previous per-mode database once; never read .env or overwrite a portfolio.
    pub fn import_legacy(&self, path: &Path) -> Result<bool> {
        if !path.exists() {
            return Ok(false);
        }
        let source = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        let legacy: bool = source.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='state')",
            [],
            |r| r.get(0),
        )?;
        if !legacy {
            return Ok(false);
        }
        let row: Option<(String, String, String)> = source
            .query_row("SELECT mode,config,book FROM state WHERE id=1", [], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?))
            })
            .optional()?;
        let Some((mode, config, book)) = row else {
            return Ok(false);
        };
        let mode: Mode = serde_json::from_str(&mode)?;
        let _: Config = serde_json::from_str(&config)?;
        let _: Book = serde_json::from_str(&book)?;
        let mut dest = self.connection.lock().unwrap();
        let tx = dest.transaction()?;
        let inserted = tx.execute(
            "INSERT OR IGNORE INTO portfolios(mode,config,book) VALUES(?1,?2,?3)",
            params![tag(mode), config, book],
        )?;
        if inserted == 1 {
            let mut events = source.prepare("SELECT time,body FROM events ORDER BY id")?;
            let rows =
                events.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))?;
            for row in rows {
                let (time, body) = row?;
                tx.execute(
                    "INSERT INTO events(time,body,mode) VALUES(?1,?2,?3)",
                    params![time, body, tag(mode)],
                )?;
            }
        }
        tx.commit()?;
        Ok(inserted == 1)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn legacy_decision_modes_and_coin_overrides_are_saved_as_jev() {
        use crate::config::DecisionMode;
        use serde_json::json;
        for mode in ["builtin", "hybrid", "jev"] {
            let store = Store::open(":memory:", Mode::Paper).unwrap();
            let mut config = serde_json::to_value(Config::default()).unwrap();
            config["decision_mode"] = json!(mode);
            config["strategy"]["kind"] = json!("macd");
            let coin = json!({
                "symbol":"BTCUSDT", "enabled":true,
                "config":{
                    "notional_usdt":"200", "profit_usdt":"1",
                    "decision_mode":mode, "strategy":config["strategy"],
                    "allowed_direction":null
                }
            });
            let mut book = serde_json::to_value(Book::default()).unwrap();
            book["pool"]["BTCUSDT"] = coin;
            book["last_bar"]["BTCUSDT"] = json!(123);
            store
                .connection
                .lock()
                .unwrap()
                .execute(
                    "INSERT INTO portfolios(mode,config,book) VALUES('paper',?1,?2)",
                    params![config.to_string(), book.to_string()],
                )
                .unwrap();
            let (loaded, book) = store.load().unwrap().unwrap();
            loaded.validate().unwrap();
            assert_eq!(loaded.decision_mode, DecisionMode::Jev);
            let effective = loaded.for_coin(&book.pool["BTCUSDT"]).unwrap();
            assert_eq!(effective.decision_mode, DecisionMode::Jev);
            assert_eq!(effective.notional_usdt, rust_decimal::Decimal::from(200));
            store.save(Mode::Paper, &loaded, &book).unwrap();
            let (config, book): (String, String) = store
                .connection
                .lock()
                .unwrap()
                .query_row(
                    "SELECT config,book FROM portfolios WHERE mode='paper'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            let config: serde_json::Value = serde_json::from_str(&config).unwrap();
            let book: serde_json::Value = serde_json::from_str(&book).unwrap();
            assert_eq!(config["decision_mode"], "jev");
            assert!(config["strategy"].get("kind").is_none());
            assert_eq!(book["pool"]["BTCUSDT"]["config"]["decision_mode"], "jev");
            assert!(
                book["pool"]["BTCUSDT"]["config"]["strategy"]
                    .get("kind")
                    .is_none()
            );
            assert_eq!(book["last_bar"]["BTCUSDT"], 123);
        }
    }
    #[test]
    fn portfolios_and_events_are_isolated_in_one_database() {
        let paper = Store::open(":memory:", Mode::Paper).unwrap();
        let mut book = Book::default();
        book.last_bar.insert("BTCUSDT".into(), 123);
        paper.save(Mode::Paper, &Config::default(), &book).unwrap();
        paper
            .event(&Event {
                time: 1,
                level: "info".into(),
                symbol: None,
                message: "paper-only".into(),
                detail: serde_json::json!({}),
            })
            .unwrap();
        let live = paper.for_mode(Mode::Live);
        assert!(live.load().unwrap().is_none());
        assert!(live.events(10).unwrap().is_empty());
        assert!(
            paper
                .save(Mode::Live, &Config::default(), &Book::default())
                .is_err()
        );
        live.save(Mode::Live, &Config::default(), &Book::default())
            .unwrap();
        assert_eq!(paper.load().unwrap().unwrap().1.last_bar["BTCUSDT"], 123);
        assert_eq!(paper.events(10).unwrap().len(), 1);
    }
    #[test]
    fn legacy_state_and_credentials_survive_reopening() {
        let dir = std::env::temp_dir().join(format!(
            "genius-store-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("legacy.sqlite");
        let old = Connection::open(&path).unwrap();
        old.execute_batch("CREATE TABLE state(id INTEGER PRIMARY KEY,mode TEXT,config TEXT,book TEXT); CREATE TABLE events(id INTEGER PRIMARY KEY,time INTEGER,body TEXT);").unwrap();
        let mut b = Book::default();
        b.last_bar.insert("BTCUSDT".into(), 123);
        old.execute(
            "INSERT INTO state VALUES(1,?1,?2,?3)",
            params![
                "\"paper\"",
                serde_json::to_string(&Config::default()).unwrap(),
                serde_json::to_string(&b).unwrap()
            ],
        )
        .unwrap();
        drop(old);
        let s = Store::open(path.to_str().unwrap(), Mode::Paper).unwrap();
        let settings = AppSettings::default()
            .patched(
                serde_json::from_value(serde_json::json!({"jev_key":"local-db-test-key"})).unwrap(),
            )
            .unwrap();
        s.save_settings(&settings).unwrap();
        drop(s);
        let s = Store::open(path.to_str().unwrap(), Mode::Paper).unwrap();
        assert_eq!(s.load().unwrap().unwrap().1.last_bar["BTCUSDT"], 123);
        assert!(s.settings().unwrap() == settings);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        drop(s);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
