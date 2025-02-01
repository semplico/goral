pub mod configuration;
use crate::google::datavalue::{Datarow, Datavalue};
use crate::google::spreadsheet::GOOGLE_SPREADSHEET_MAXIMUM_CHARS_PER_CELL;
use crate::messenger::configuration::MessengerConfig;
use crate::notifications::{MessengerApi, Notification, Sender};
use crate::rules::{Action, Rule, RuleCondition};
use crate::services::logs::configuration::{channel_capacity, Logs};
use crate::services::{Data, Service, TaskResult};
use crate::storage::AppendableLog;
use crate::{capture_datetime, Shared};
use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, NaiveDateTime, Utc};
use lazy_static::lazy_static;
use regex::Regex;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::io::{self, AsyncBufReadExt, BufReader};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{self};
use tokio::task::JoinHandle;

pub const LOGS_SERVICE_NAME: &str = "logs";
const LEVEL_KEY: &str = "level";

pub struct LogsService {
    shared: Shared,
    spreadsheet_id: String,
    push_interval: Duration,
    channel_capacity: usize,
    filter_if_contains: Vec<String>,
    drop_if_contains: Vec<String>,
    messenger: Option<MessengerApi>,
    truncate_at: f32,
}

impl LogsService {
    pub fn new(shared: Shared, mut config: Logs) -> LogsService {
        let channel_capacity = channel_capacity(&config.push_interval_secs);
        let messenger = config
            .messenger
            .take()
            .map(|messenger_config| MessengerApi::new(messenger_config, LOGS_SERVICE_NAME));
        Self {
            shared,
            spreadsheet_id: config.spreadsheet_id,
            push_interval: Duration::from_secs(config.push_interval_secs.into()),
            filter_if_contains: config.filter_if_contains.unwrap_or_default(),
            drop_if_contains: config.drop_if_contains.unwrap_or_default(),
            channel_capacity,
            messenger,
            truncate_at: config.autotruncate_at_usage_percent,
        }
    }

    fn filter<'a>(
        text: &'a str,
        filter_if_contains: &Vec<String>,
        drop_if_contains: &Vec<String>,
    ) -> Option<&'a str> {
        match (filter_if_contains.is_empty(), drop_if_contains.is_empty()) {
            (false, _) => {
                for substring in filter_if_contains {
                    if text.contains(substring) {
                        return Some(text);
                    }
                }
                tracing::debug!(
                    "log line {} is dropped because it doesn't contain any of {:?}",
                    text,
                    filter_if_contains
                );
                None
            }
            (true, false) => {
                for substring in drop_if_contains {
                    if text.contains(substring) {
                        tracing::debug!(
                            "log line {} is dropped because it contains {:?}",
                            text,
                            substring
                        );
                        return None;
                    }
                }
                Some(text)
            }
            (true, true) => Some(text),
        }
    }

    fn guess_log_level(line: &str) -> Option<&str> {
        lazy_static! {
            static ref RE: Regex = Regex::new(
                r"(?xi)
                (?P<level>(
                    panic|panicked|fatal|critical|error|warn|alert|info|notice|debug|trace|err|crit
                ))[^\w]
                "
            )
            .expect("assert: log level regex is properly constructed");
        }
        RE.captures(line).and_then(|cap| {
            cap.name("level").map(|level| {
                let level = level.as_str();
                match level {
                    "panicked" => "panic",
                    "err" => "error",
                    "crit" => "critical",
                    _ => level,
                }
            })
        })
    }

    fn guess_datetime(line: &str) -> NaiveDateTime {
        let now = Utc::now().naive_utc();
        capture_datetime(line)
            .filter(|parsed| parsed.signed_duration_since(now).abs() < ChronoDuration::seconds(60))
            .unwrap_or(now)
    }

    fn process_line(
        line: String,
        filter_if_contains: &Vec<String>,
        drop_if_contains: &Vec<String>,
    ) -> Data {
        let text = line.trim();
        if text.is_empty() {
            return Data::Empty;
        }

        let text = if let Some(text) = Self::filter(text, filter_if_contains, drop_if_contains) {
            text
        } else {
            return Data::Empty;
        };

        let level = Self::guess_log_level(text)
            .map(|l| {
                let level = l.to_lowercase();
                match level.as_str() {
                    "panic" | "fatal" | "critical" | "error" => Datavalue::RedText(level),
                    "warn" | "alert" => Datavalue::OrangeText(level),
                    "info" | "notice" => Datavalue::GreenText(level),
                    _ => Datavalue::Text(level),
                }
            })
            .unwrap_or(Datavalue::NotAvailable);
        let datetime = Self::guess_datetime(text);
        let text = text
            .chars()
            .take(GOOGLE_SPREADSHEET_MAXIMUM_CHARS_PER_CELL)
            .collect();
        Data::Single({
            Datarow::new(
                LOGS_SERVICE_NAME.to_string(),
                datetime,
                vec![
                    (LEVEL_KEY.to_string(), level),
                    ("log_line".to_string(), Datavalue::Text(text)),
                ],
            )
        })
    }

    fn process_lines(
        is_shutdown: Arc<AtomicBool>,
        sender: mpsc::Sender<TaskResult>,
        mut request_rx: mpsc::Receiver<String>,
        filter_if_contains: Vec<String>,
        drop_if_contains: Vec<String>,
    ) {
        tracing::info!("started logs processing thread");

        while let Some(line) = request_rx.blocking_recv() {
            let data = Self::process_line(line, &filter_if_contains, &drop_if_contains);
            if sender
                .blocking_send(TaskResult {
                    id: 0,
                    result: Ok(data),
                })
                .is_err()
            {
                if is_shutdown.load(Ordering::Relaxed) {
                    return;
                }
                panic!("assert: log messages queue shouldn't be closed before shutdown signal");
            }
        }
        tracing::info!("exiting logs processing thread");
    }

    async fn logs_collector(
        is_shutdown: Arc<AtomicBool>,
        sender: mpsc::Sender<TaskResult>,
        send_notification: Sender,
        filter_if_contains: Vec<String>,
        drop_if_contains: Vec<String>,
    ) {
        tracing::info!("starting stdin logs scraping");
        let stdin = BufReader::new(io::stdin()).lines();
        tokio::pin!(stdin);
        let (tx, rx) = mpsc::channel(sender.capacity());
        let cloned_sender = sender.clone();
        let cloned_is_shutdown = is_shutdown.clone();
        std::thread::Builder::new()
            .name("logs-collector".into())
            .spawn(move || {
                Self::process_lines(
                    cloned_is_shutdown,
                    cloned_sender,
                    rx,
                    filter_if_contains,
                    drop_if_contains,
                )
            })
            .expect("assert: can spawn logs collecting thread");

        loop {
            tokio::select! {
                _ = sender.closed() => {
                    tracing::info!("finished stdin logs scraping");
                    return;
                },
                res = stdin.next_line() => {
                    match res {
                        Err(e) => {
                            let msg = format!("error reading next log line from stdin `{e}`");
                            tracing::error!("{}", msg);
                            send_notification.try_error(msg);
                        },
                        Ok(Some(line)) => {
                            match tx.try_send(line) {
                                Err(TrySendError::Full(res)) => {
                                    let msg = "log messages queue is full so decrease push interval and filter logs by substrings in `filter_if_contains`".to_string();
                                    tracing::error!("{}. Cannot send log result `{:?}`", msg, res);
                                    send_notification.try_error(msg);
                                },
                                Err(TrySendError::Closed(res)) => {
                                    if is_shutdown.load(Ordering::Relaxed) {
                                        tracing::info!("finished stdin logs scraping");
                                        return;
                                    }
                                    let msg = "log messages queue has been unexpectedly closed".to_string();
                                    tracing::error!("{}: cannot send log result {:?}", msg, res);
                                    send_notification.fatal(msg).await;
                                    panic!("assert: log messages queue shouldn't be closed before shutdown signal");
                                },
                                _ => {},
                            }
                        },
                        Ok(None) => {
                            tracing::warn!("finished collecting logs from stdin - no more lines");
                            break;
                        }
                    };

                }
            }
        }
        sender.closed().await;
    }
}

#[async_trait]
impl Service for LogsService {
    fn name(&self) -> &'static str {
        LOGS_SERVICE_NAME
    }

    fn spreadsheet_id(&self) -> &str {
        self.spreadsheet_id.as_str()
    }

    fn channel_capacity(&self) -> usize {
        self.channel_capacity
    }

    fn push_interval(&self) -> Duration {
        self.push_interval
    }

    fn get_example_rules(&self) -> Vec<Datarow> {
        vec![
            Rule {
                log_name: LOGS_SERVICE_NAME.to_string(),
                key: LEVEL_KEY.to_string(),
                condition: RuleCondition::Contains,
                value: Datavalue::Text("error".to_string()),
                action: Action::Error,
            }
            .into(),
            Rule {
                log_name: LOGS_SERVICE_NAME.to_string(),
                key: LEVEL_KEY.to_string(),
                condition: RuleCondition::Contains,
                value: Datavalue::Text("warn".to_string()),
                action: Action::Warn,
            }
            .into(),
        ]
    }

    fn shared(&self) -> &Shared {
        &self.shared
    }

    fn messenger(&self) -> Option<Sender> {
        self.messenger.as_ref().map(|m| m.message_tx.clone())
    }

    fn messenger_config(&self) -> Option<&MessengerConfig> {
        self.messenger.as_ref().map(|m| &m.config)
    }

    fn take_messenger_rx(&mut self) -> Option<mpsc::Receiver<Notification>> {
        self.messenger.as_mut().and_then(|m| m.message_rx.take())
    }

    fn truncate_at(&self) -> f32 {
        self.truncate_at
    }

    async fn process_task_result_on_shutdown(
        &mut self,
        result: TaskResult,
        log: &AppendableLog,
    ) -> Data {
        self.process_task_result(result, log).await
    }

    async fn process_task_result(&mut self, result: TaskResult, _log: &AppendableLog) -> Data {
        let TaskResult { result, .. } = result;
        match result {
            Ok(data) => data,
            Err(Data::Message(msg)) => {
                tracing::error!("{}", msg);
                self.send_error(format!("`{}` while collecting logs from stdin", msg))
                    .await;
                Data::Empty
            }
            _ => panic!("assert: system result contains either single datarow or error text"),
        }
    }

    async fn spawn_tasks(
        &mut self,
        is_shutdown: Arc<AtomicBool>,
        sender: mpsc::Sender<TaskResult>,
    ) -> Vec<JoinHandle<()>> {
        let sender = sender.clone();
        let is_shutdown = is_shutdown.clone();
        let send_notification = self.shared.send_notification.clone();
        let filter_if_contains = self.filter_if_contains.clone();
        let drop_if_contains = self.drop_if_contains.clone();
        vec![tokio::spawn(async move {
            Self::logs_collector(
                is_shutdown,
                sender,
                send_notification,
                filter_if_contains,
                drop_if_contains,
            )
            .await;
        })]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filtering() {
        assert_eq!(
            LogsService::filter("pine-apple", &vec!["apple".to_string()], &vec![]),
            Some("pine-apple")
        );
        assert_eq!(
            LogsService::filter("pine-apple", &vec![], &vec!["apple".to_string()]),
            None
        );
        assert_eq!(
            LogsService::filter("pine-apple", &vec![], &vec![]),
            Some("pine-apple")
        );
    }

    #[test]
    fn log_level() {
        assert_eq!(
            LogsService::guess_log_level("INFO:2023/02/17 14:30:15 This is an info message."),
            Some("INFO")
        );
        assert_eq!(
            LogsService::guess_log_level("ERROR:the.module.name:The log message"),
            Some("ERROR")
        );
        assert_eq!(LogsService::guess_log_level("thread 'services::logs::tests::log_level' panicked at src/services/logs/mod.rs:260:9:"), Some("panic"));
        assert_eq!(
            LogsService::guess_log_level("INFO:2023/02/17 14:30:15 err channel is closed."),
            Some("INFO")
        );
        assert_eq!(
            LogsService::guess_log_level("[2m2023-11-02T12:11:49.767270Z[0m [32m INFO[0m [2mgoral::storage[0m[2m:[0m appending to log 9 rows for service system"),
            Some("INFO")
        );
        assert_eq!(
            LogsService::guess_log_level(
                r#"\33[2m2023-11-02T12:25:20.879794Z\33[0m \33[32m INFO\33[0m \33[2mgoral::storage\33[0m\33[2m:\33[0m appended to log 10 rows for service health\n"#
            ),
            Some("INFO")
        );
        assert_eq!(
            LogsService::guess_log_level(
                r#"[2m2023-11-02T12:29:51.552906Z[0m [32m INFO[0m [2mgoral::services::healthcheck[0m[2m:[0m starting check for Http(http://127.0.0.1:9898/)"#
            ),
            Some("INFO")
        );
        assert_eq!(
            LogsService::guess_log_level(
                r#"{"timestamp":"2023-11-02T17:13:21.321597Z","level":"INFO","fields":{"message":"appending to log 9 rows for service system"},"target":"goral::storage"}"#
            ),
            Some("INFO")
        );
    }
}
