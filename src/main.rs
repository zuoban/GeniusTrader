use anyhow::{Context, Result, bail};
use genius_trader::{api, application::Application, config::Mode, store::Store};
use std::{
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
};

#[derive(Debug, PartialEq)]
struct Bootstrap {
    db: String,
    bind: Ipv4Addr,
}

fn parse_bootstrap(mut args: impl Iterator<Item = String>) -> Result<Option<Bootstrap>> {
    let mut options = Bootstrap {
        db: "data/trader.sqlite".to_string(),
        bind: Ipv4Addr::LOCALHOST,
    };
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--db" => options.db = args.next().context("--db 需要数据库路径")?,
            "--bind" => {
                options.bind = args
                    .next()
                    .context("--bind 需要 IPv4 监听地址")?
                    .parse()
                    .context("--bind 需要有效的 IPv4 监听地址")?;
            }
            "--help" | "-h" => return Ok(None),
            _ => bail!("未知参数 {arg}；使用 --help 查看说明"),
        }
    }
    Ok(Some(options))
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("genius_trader=info")
        .init();
    // Bootstrap paths and the container bind address precede SQLite runtime settings.
    let Some(options) = parse_bootstrap(std::env::args().skip(1))? else {
        println!(
            "genius-trader [--db data/trader.sqlite] [--bind 127.0.0.1]\n运行配置在页面设置并保存到 SQLite；首次打开页面设置管理员密码。\n容器可指定 --bind 0.0.0.0，并仅向宿主机回环地址映射端口。"
        );
        return Ok(());
    };
    let db = options.db;
    if let Some(p) = std::path::Path::new(&db)
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
    {
        std::fs::create_dir_all(p)?;
    }
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(format!("{db}.lock"))?;
    lock.try_lock().context("同一数据库已有运行中的交易服务")?;
    let store = Store::open(&db, Mode::Paper)?;
    if db == "data/trader.sqlite" {
        for old in [
            "data/paper.sqlite",
            "data/testnet.sqlite",
            "data/live.sqlite",
            "data/preview-paper.sqlite",
        ] {
            if store.import_legacy(std::path::Path::new(old))? {
                tracing::info!("已导入旧数据库 {old}，原文件保留");
            }
        }
    }
    let port = store.server_settings()?.port;
    let initialized = store.admin_initialized()?;
    let app = Application::new(store)?;
    let addr = SocketAddr::from((options.bind, port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    app.start().await;
    tracing::info!("控制台 http://{addr}；配置保存到 SQLite，新增开仓保持暂停");
    if !initialized {
        tracing::info!("尚未初始化管理员，请打开控制台设置密码；游客可只读查看运行情况");
    }
    let shutdown = Arc::clone(&app);
    axum::serve(listener, api::router(app))
        .with_graceful_shutdown(async move {
            let _ = tokio::signal::ctrl_c().await;
            shutdown.shutdown().await;
        })
        .await?;
    drop(lock);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<Option<Bootstrap>> {
        parse_bootstrap(args.iter().map(|arg| (*arg).to_owned()))
    }

    #[test]
    fn default_bind_remains_localhost() {
        let options = parse(&[]).unwrap().unwrap();
        assert_eq!(options.db, "data/trader.sqlite");
        assert_eq!(options.bind, Ipv4Addr::LOCALHOST);
    }

    #[test]
    fn container_can_choose_bind_and_database() {
        let options = parse(&["--db", "/app/data/trader.sqlite", "--bind", "0.0.0.0"])
            .unwrap()
            .unwrap();
        assert_eq!(options.db, "/app/data/trader.sqlite");
        assert_eq!(options.bind, Ipv4Addr::UNSPECIFIED);
    }

    #[test]
    fn invalid_bootstrap_arguments_are_rejected() {
        for args in [
            vec!["--bind"],
            vec!["--bind", "example.com"],
            vec!["--bind", "::1"],
            vec!["--db"],
            vec!["--unknown"],
        ] {
            assert!(parse(&args).is_err());
        }
        assert!(parse(&["--help"]).unwrap().is_none());
    }
}
