use anyhow::{Context, Result, bail};
use genius_trader::{api, application::Application, config::Mode, store::Store};
use std::{
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("genius_trader=info")
        .init();
    // The database location is the only bootstrap argument. Runtime settings are in SQLite.
    let mut db = "data/trader.sqlite".to_string();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--db" => db = args.next().context("--db 需要数据库路径")?,
            "--help" | "-h" => {
                println!(
                    "genius-trader [--db data/trader.sqlite]\n所有运行配置在页面设置并保存到 SQLite；首次打开页面设置管理员密码。"
                );
                return Ok(());
            }
            _ => bail!("未知参数 {arg}；使用 --help 查看说明"),
        }
    }
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
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
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
