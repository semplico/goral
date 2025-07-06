use clap::Parser;
use futures::future::try_join_all;
use goral::configuration::{Configuration, APP_NAME};
use goral::google::{get_google_auth, SpreadsheetAPI, Storage};
use goral::storage::AppendableLog;
use goral::{
    collect_messengers, collect_services, setup_general_messenger_channel, welcome, Shared,
};
use std::fmt::Debug;
use std::panic;
use std::path::PathBuf;
use std::process;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::sync::broadcast;
use tracing::Level;
use tracing_subscriber::filter::{EnvFilter, LevelFilter};
use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

#[cfg(unix)]
pub async fn sigterm() -> tokio::io::Result<()> {
    signal::unix::signal(signal::unix::SignalKind::terminate())?
        .recv()
        .await;
    Ok(())
}

#[cfg(not(unix))]
pub async fn sigterm() -> tokio::io::Result<()> {
    std::future::pending::<()>().await;
    Ok(())
}

/// Goral is a modular observability toolkit for small projects.
///
/// Goral is a lightweight daemon process which
/// stores collected data in a Google Spreadsheet so no
/// additional storage required.
/// Any Goral service is easily replaced with enterprise-grade
/// solution like Prometheus, Loki or Zabbix because it scrapes
/// data in common industry formats.
/// https://github.com/maksimryndin/goral#goral
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to TOML configuration file. Environment variables starting with `GORAL_`
    /// (e.g. `GORAL__GENERAL__LOG_LEVEL`)
    /// overwrite config values.
    #[arg(short, long)]
    config: PathBuf,
    /// Host identifier (8 characters). Keep this argument persistent for this instance of Goral
    /// to properly identify your apps running at this host.
    /// It should be unique among all your hosts where Gorals observe your apps
    /// as in the case of the same spreadsheet used for several hosts the host id allows
    /// to separate sheets with data coming from different hosts.
    /// If you move your app to another machine and want to keep on using the same
    /// sheets and charts, you just reuse that host id.
    #[arg(short, long)]
    id: String,
    /// Enable JSON log format
    #[arg(short, long)]
    json: bool,
}

async fn start() -> Result<(), String> {
    let args = Args::parse();

    let config = Configuration::new(&args.config.display().to_string()).map_err(|e|
        format!(
            "Incorrect configuration (can be potentially overridden by environment variables starting with `GORAL__`): {e}"
        )
    )?;

    let level = LevelFilter::from_level(config.general.log_level);
    let filter = EnvFilter::new("")
        .add_directive(level.into())
        .add_directive(
            // to prevent logging oauth2 credentials
            "yup_oauth2=info"
                .parse()
                .expect("assert: tracing directive is properly set for yup_oauth2"),
        );

    let (json, plain) = if args.json {
        (
            Some(
                tracing_subscriber::fmt::layer()
                    .with_target(true)
                    .with_line_number(true)
                    .with_file(true)
                    .with_ansi(true)
                    .with_thread_names(true)
                    .json(),
            ),
            None,
        )
    } else if level == Level::INFO {
        (
            None,
            Some(
                tracing_subscriber::fmt::layer()
                    .with_target(false)
                    .with_line_number(false)
                    .with_file(false)
                    .with_ansi(true)
                    .with_thread_names(true),
            ),
        )
    } else {
        (
            None,
            Some(
                tracing_subscriber::fmt::layer()
                    .with_target(true)
                    .with_line_number(true)
                    .with_file(true)
                    .with_ansi(true)
                    .with_thread_names(true),
            ),
        )
    };

    tracing_subscriber::registry()
        .with(filter)
        .with(json)
        .with(plain)
        .init();

    let graceful_shutdown_timeout = config.general.graceful_timeout_secs;
    let (project_id, auth) =
        get_google_auth(&config.general.service_account_credentials_path).await;
    let (tx, rx) = setup_general_messenger_channel();
    let sheets_api = SpreadsheetAPI::new(auth, tx.clone());
    let storage = Arc::new(Storage::new(args.id.to_string(), sheets_api));
    let shared = Shared::new(tx.clone());

    let messengers = collect_messengers(&config);
    let truncation_check = config.check_truncation_limits();
    let mut services = collect_services(config, shared, messengers, rx);

    let (shutdown, _) = broadcast::channel(1);
    let mut tasks = Vec::with_capacity(services.len());
    while let Some(mut service) = services.pop() {
        let storage = storage.clone();
        let shutdown_receiver = shutdown.subscribe();
        let messenger = service.messenger();
        tasks.push(tokio::spawn(async move {
            let log = AppendableLog::new(
                storage,
                service.spreadsheet_id().to_string(),
                service.name().to_string(),
                messenger,
                service.truncate_at(),
            );
            service.run(log, shutdown_receiver).await
        }));
    }

    let goral = try_join_all(tasks);
    tokio::pin!(goral);
    welcome(tx.clone(), project_id, truncation_check).await;
    tracing::info!("{APP_NAME} started with pid {}", process::id());

    tokio::select! {
        biased;

        _ = async { signal::ctrl_c().await.expect("failed to listen for Ctrl+C event") } => {
            let msg = format!("{APP_NAME} has received `Ctrl+C` signal, will try to gracefully shutdown within {graceful_shutdown_timeout} secs");
            tracing::warn!("{}", msg.as_str());
            tx.try_warn(msg);
        }
        _ = async { sigterm().await.expect("failed to listen for Sigterm event") } => {
            let msg = format!("{APP_NAME} has received SIGTERM signal, will try to gracefully shutdown within {graceful_shutdown_timeout} secs");
            tracing::warn!("{}", msg.as_str());
            tx.try_warn(msg);
        }
        res = &mut goral => {
            tracing::error!("main thread terminated unexpectedly: {:?}", res);
            std::process::exit(1);
        }
    }
    shutdown
        .send(graceful_shutdown_timeout)
        .expect("assert: services should run when shutdown signal is sent");

    tokio::select! {
        _ = tokio::time::sleep(Duration::from_secs(graceful_shutdown_timeout.into())) => {
            tracing::error!("{} couldn't gracefully shutdown", APP_NAME);
        },
        _ = &mut goral => {
            tracing::info!("{} has gracefully shutdowned", APP_NAME);
        }
    }
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), String> {
    panic::set_hook(Box::new(|panic_info| {
        let base_message = format!("\nCould you please open an issue https://github.com/maksimryndin/goral/issues with `Bug` label? Thank you for using {APP_NAME}!");
        if let Some(s) = panic_info.payload().downcast_ref::<&str>() {
            if s.starts_with("assert:") {
                println!(
                    "key assumption about {APP_NAME} behaviour is violated: {s:?}{base_message}"
                );
            } else {
                println!("Unexpected error happened: {panic_info}");
            }
        } else {
            println!("Unexpected error happened: {panic_info}");
        }
    }));
    start().await?;
    // in case we collect logs from stdin - there is no way to cancel the underlying blocking thread
    // so after the shutdown (which ensures proper termination) we exit the whole process
    // main body function is enclosed with braces to run destructors as the `exit` aborts immediately
    std::process::exit(0);
}
