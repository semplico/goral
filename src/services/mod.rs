pub mod general;
pub mod healthcheck;
pub mod kv;
pub mod logs;
pub mod metrics;
pub mod system;

use crate::google::Storage;
use crate::google::datavalue::{Datarow, Datavalue};
use crate::messenger::configuration::MessengerConfig;
use crate::notifications::{Notification, Sender};
use crate::rules::{Action, Rule, RuleCondition, RuleOutput, Triggered};
use crate::storage::AppendableLog;
use crate::{BoxedMessenger, Shared, jitter_duration};
use async_trait::async_trait;
use futures::future::try_join_all;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::sync::mpsc::{self, error::TrySendError};
use tokio::task::JoinHandle;
use tracing::Level;

#[derive(Debug, Clone)]
pub enum Data {
    Empty,
    Single(Datarow),
    Many(Vec<Datarow>),
    Message(String),
}

impl Data {
    pub fn iterator(self) -> DataIntoIter {
        match self {
            Data::Empty | Data::Message(_) => DataIntoIter {
                next: None,
                many: vec![],
                is_single: true,
            },
            Data::Single(row) => DataIntoIter {
                next: Some(row),
                many: vec![],
                is_single: true,
            },
            Data::Many(rows) => DataIntoIter {
                next: None,
                many: rows,
                is_single: false,
            },
        }
    }
}

pub struct DataIntoIter {
    next: Option<Datarow>,
    many: Vec<Datarow>,
    is_single: bool,
}

impl Iterator for DataIntoIter {
    type Item = Datarow;

    fn next(&mut self) -> Option<Self::Item> {
        if self.is_single {
            self.next.take()
        } else {
            self.many.pop()
        }
    }
}

#[derive(Debug)]
pub struct TaskResult {
    id: usize,
    result: Result<Data, Data>,
}

fn process_rules(
    is_shutdown: Arc<AtomicBool>,
    send_notification: Sender,
    mut input_rx: mpsc::Receiver<RulePayload>,
    output_tx: mpsc::Sender<Triggered>,
    service_name: String,
) {
    tracing::info!(
        "started rules processing thread for service {}",
        service_name
    );

    let RulePayload::Rules(mut rules) = input_rx
        .try_recv()
        .expect("assert: rules are provided at service initialization")
    else {
        panic!("assert: rules are provided at service initialization as a first message");
    };
    while let Some(payload) = input_rx.blocking_recv() {
        match payload {
            RulePayload::Rules(updated_rules) => {
                rules = updated_rules;
                continue;
            }
            RulePayload::Data(data) => {
                for row in data.iterator() {
                    let applicant = row.into();
                    for rule in &rules {
                        match rule.apply(&applicant) {
                            RuleOutput::SkipFurtherRules => {
                                break;
                            }
                            RuleOutput::Process(Some(triggered)) => {
                                match output_tx.try_send(triggered) {
                                    Err(TrySendError::Full(triggered)) => {
                                        let msg = format!(
                                            "rules output messages queue is full for service {service_name}"
                                        );
                                        tracing::error!(
                                            "{}. Cannot send rules application output: {:#?}",
                                            msg,
                                            triggered
                                        );
                                        send_notification.try_error(msg);
                                    }
                                    Err(TrySendError::Closed(triggered)) => {
                                        if is_shutdown.load(Ordering::Relaxed) {
                                            return;
                                        }
                                        let msg = format!(
                                            "rules output messages queue has been unexpectedly closed for service {service_name}"
                                        );
                                        tracing::error!(
                                            "{}: cannot send rules application output: {:#?}",
                                            msg,
                                            triggered
                                        );
                                        panic!(
                                            "assert: rules output messages queue shouldn't be closed before shutdown signal"
                                        );
                                    }
                                    _ => {}
                                }
                            }
                            _ => {
                                continue;
                            }
                        }
                    }
                }
            }
        }
    }
    tracing::info!(
        "exiting rules processing thread for service {}",
        service_name
    );
}

async fn rules_notifications(
    messenger: Sender,
    mut output_receiver: mpsc::Receiver<Triggered>,
    storage: Arc<Storage>,
    spreadsheet_id: String,
) {
    while let Some(triggered) = output_receiver.recv().await {
        use Action::*;
        let level = match triggered.action {
            Info => Level::INFO,
            Warn => Level::WARN,
            Error => Level::ERROR,
            SkipFurtherRules => panic!("assert: `skip further rules` action shouldn't be sent"),
        };
        let message = format!(
            "{}\n[spreadsheet]({}) may be created a bit later",
            triggered.message,
            storage.table_row_url(&spreadsheet_id, triggered.sheet_id, triggered.row)
        );
        messenger.send(Notification::new(message, level)).await;
    }
}

async fn messenger_queue(
    messenger: Arc<BoxedMessenger>,
    messenger_config: MessengerConfig,
    send_notification: Sender,
    mut rx: mpsc::Receiver<Notification>,
    host_id: String,
    service: &'static str,
) {
    while let Some(notification) = rx.recv().await {
        let Notification { message, level } = notification;
        if messenger
            .send_by_level(&messenger_config, &format!("*{host_id}*: {message}"), level)
            .await
            .is_err()
        {
            tracing::error!(
                "failed to send notification via configured messenger: `{:?}` for service {}",
                messenger_config,
                service
            );
            send_notification.try_error(format!(
                "{message}. Sending via configured messenger failed."
            ));
        }
    }
}

pub enum RulePayload {
    Rules(Vec<Rule>),
    Data(Data),
}

#[async_trait]
pub trait Service: Send + Sync {
    fn name(&self) -> &'static str;

    fn spreadsheet_id(&self) -> &str;

    fn channel_capacity(&self) -> usize {
        // Default for General service where the channel is not actually used
        1
    }

    fn push_interval(&self) -> Duration {
        // Default for General and KV services
        Duration::from_secs(u64::MAX)
    }

    fn rules_update_interval(&self) -> Duration {
        Duration::from_secs(30) + jitter_duration()
    }

    fn get_example_rules(&self) -> Vec<Datarow> {
        let row: Datarow = Rule {
            log_name: "log_name (everything up to the first @ in a sheet title)".to_string(),
            key: "key (column header)".to_string(),
            condition: RuleCondition::Contains,
            value: Datavalue::Text("substring".to_string()),
            action: Action::SkipFurtherRules,
        }
        .into();
        vec![row]
    }

    fn shared(&self) -> &Shared;

    fn messenger(&self) -> Option<Sender> {
        None
    }

    fn messenger_config(&self) -> Option<&MessengerConfig> {
        None
    }

    fn take_messenger_rx(&mut self) -> Option<mpsc::Receiver<Notification>> {
        None
    }

    fn truncate_at(&self) -> f32;

    async fn prerun_hook(&self, log: &mut AppendableLog) {
        if let Err(e) = log.healthcheck().await {
            let msg = format!(
                "service `{}` failed to connect to Google API: `{:?}`",
                self.name(),
                e
            );
            tracing::error!("{}", msg);
            self.shared().send_notification.fatal(msg.clone()).await;
            panic!("{}", msg);
        }
    }

    async fn welcome_hook(&self, log: &AppendableLog) {
        let msg = format!(
            "service `{}` is running with [spreadsheet]({})",
            self.name(),
            log.base_url(),
        );
        tracing::info!("{}", msg);
        self.messenger()
            .unwrap_or(self.shared().send_notification.clone())
            .try_info(msg);
        tracing::debug!(
            "channel capacity `{}` for service `{}`",
            self.channel_capacity(),
            self.name()
        );
    }

    async fn send_error(&self, message: String) {
        if let Some(messenger) = self.messenger() {
            messenger
                .send(Notification::new(message, Level::ERROR))
                .await;
        } else {
            tracing::error!(
                "Messenger is not configured for {}. Error: {}",
                self.name(),
                message,
            );
            self.shared().send_notification.try_error(format!(
                "{}\nMessenger is not configured for {}",
                message,
                self.name(),
            ));
        }
    }

    async fn process_task_result_on_shutdown(
        &mut self,
        result: TaskResult,
        log: &mut AppendableLog,
    ) -> Data {
        self.process_task_result(result, log).await
    }

    async fn process_task_result(&mut self, _result: TaskResult, _log: &mut AppendableLog) -> Data {
        Data::Empty
    }

    async fn spawn_tasks(
        &mut self,
        _shutdown: Arc<AtomicBool>,
        _sender: mpsc::Sender<TaskResult>,
    ) -> Vec<JoinHandle<()>> {
        // Default for General service
        std::future::pending::<()>().await;
        vec![]
    }

    fn plan_to_append(&self, log: &mut AppendableLog, data: &mut Data) {
        match data {
            Data::Empty | Data::Message(_) => {}
            Data::Single(datarow) => {
                log.plan_to_append(datarow);
            }
            Data::Many(datarows) => {
                datarows.iter_mut().for_each(|datarow| {
                    log.plan_to_append(datarow);
                });
            }
        }
    }

    async fn send_for_rule_processing(&self, data: Data, input_tx: &mut mpsc::Sender<RulePayload>) {
        if self.shared().messenger.is_none() {
            return;
        }
        match input_tx.try_send(RulePayload::Data(data)) {
            Err(TrySendError::Full(_)) => {
                let msg = format!(
                    "rules input messages queue is full for service {}",
                    self.name()
                );
                tracing::error!("{}. Cannot send data rows for rules application", msg);
                self.shared().send_notification.try_error(msg);
            }
            Err(TrySendError::Closed(_)) => {
                let msg = format!(
                    "rules input messages queue has been unexpectedly closed for service {}",
                    self.name()
                );
                tracing::error!("{}: cannot send data rows for rules application", msg);
                self.shared().send_notification.fatal(msg).await;
                panic!(
                    "assert: rules input messages queue shouldn't be closed before shutdown signal"
                );
            }
            _ => {}
        }
    }

    async fn send_updated_rules(
        &self,
        log: &AppendableLog,
        input_tx: &mut mpsc::Sender<RulePayload>,
    ) {
        if self.shared().messenger.is_none() {
            return;
        }
        let rules = match log.get_rules().await {
            Ok(rules) => rules,
            Err(e) => {
                let msg = format!(
                    "failed to fetch rules update for service `{}`: `{}`",
                    self.name(),
                    e
                );
                tracing::warn!("{}", msg);
                if let Some(messenger_config) = &self.messenger_config()
                    && messenger_config.send_rules_update_error
                {
                    self.messenger()
                        .unwrap_or(self.shared().send_notification.clone())
                        .try_warn(msg);
                }

                return;
            }
        };

        match input_tx.try_send(RulePayload::Rules(rules)) {
            Err(TrySendError::Full(_)) => {
                let msg = format!(
                    "rules input messages queue is full for service {}",
                    self.name()
                );
                tracing::error!("{}. Cannot send rules update", msg);
                self.shared().send_notification.try_error(msg);
            }
            Err(TrySendError::Closed(_)) => {
                let msg = format!(
                    "rules input messages queue has been unexpectedly closed for service {}",
                    self.name()
                );
                tracing::error!("{}: cannot send rules update", msg);
                self.shared().send_notification.fatal(msg).await;
                panic!(
                    "assert: rules input messages queue shouldn't be closed before shutdown signal"
                );
            }
            _ => {}
        }
    }

    async fn start_rules_thread(
        &self,
        is_shutdown: Arc<AtomicBool>,
        log: &mut AppendableLog,
    ) -> (mpsc::Sender<RulePayload>, mpsc::Receiver<Triggered>) {
        if self.shared().messenger.is_none() {
            let (input_tx, _) = mpsc::channel(1);
            let (_, output_rx) = mpsc::channel(1);
            return (input_tx, output_rx);
        }
        //  channel to collect rule applicants and rules updates for rules processing
        let (input_tx, input_rx) = mpsc::channel(2 * self.channel_capacity());
        //  channel to collect rule output triggers
        let (output_tx, output_rx) = mpsc::channel(2 * self.channel_capacity());
        let example_rules = self.get_example_rules();
        example_rules
            .into_iter()
            .for_each(|mut r| log.plan_to_append(&mut r));
        let _ = log.append().await;
        let rules = log
            .get_rules()
            .await
            .expect("assert: can fetch rules at the start");
        input_tx
            .send(RulePayload::Rules(rules))
            .await
            .expect("assert: can send rules at initialization");
        let service_name = self.name().to_string();
        let send_notification = self.shared().send_notification.clone();
        std::thread::Builder::new()
            .name(format!("rules-processor-{}", self.name()))
            .spawn(move || {
                process_rules(
                    is_shutdown,
                    send_notification,
                    input_rx,
                    output_tx,
                    service_name,
                );
            })
            .expect("assert: can spawn rule processing thread");
        (input_tx, output_rx)
    }

    async fn run(&mut self, mut log: AppendableLog, mut shutdown: broadcast::Receiver<u16>) {
        self.prerun_hook(&mut log).await;
        //  channel to collect results
        let (tx, mut data_receiver) = mpsc::channel(2 * self.channel_capacity());
        let is_shutdown = Arc::new(AtomicBool::new(false));
        let (mut rules_input, rules_output) =
            self.start_rules_thread(is_shutdown.clone(), &mut log).await;
        let mut tasks = self.spawn_tasks(is_shutdown.clone(), tx).await;

        if let Some(message_rx) = self.take_messenger_rx() {
            let messenger =
                self.shared().messenger.clone().expect(
                    "assert: if messenger receiver channel is set then the messenger is set",
                );
            let messenger_config = self
                .messenger_config()
                .expect(
                    "assert: if messenger receiver channel is set then the messenger config is set",
                )
                .clone();
            let send_notification = self.shared().send_notification.clone();
            let service = self.name();
            let host_id = log.host_id().to_string();
            tasks.push(tokio::spawn(async move {
                messenger_queue(
                    messenger,
                    messenger_config,
                    send_notification,
                    message_rx,
                    host_id,
                    service,
                )
                .await
            }));
        }

        if let Some(message_tx) = self.messenger() {
            let storage = log.storage();
            let spreadsheet_id = log.spreadsheet_id();
            tasks.push(tokio::spawn(async move {
                rules_notifications(message_tx, rules_output, storage, spreadsheet_id).await
            }));
        }
        let tasks = try_join_all(tasks);
        tokio::pin!(tasks);
        let mut push_interval = tokio::time::interval(self.push_interval());
        let mut rules_update_interval = tokio::time::interval(self.rules_update_interval());
        self.welcome_hook(&log).await;
        loop {
            tokio::select! {
                result = shutdown.recv() => {
                    is_shutdown.store(true, Ordering::Release);
                    data_receiver.close(); // stop tasks
                    let graceful_shutdown_timeout = match result {
                        Err(_) => panic!("assert: shutdown signal sender should be dropped after all service listeners"),
                        Ok(graceful_shutdown_timeout) => graceful_shutdown_timeout,
                    };
                    tracing::info!("{} service has got shutdown signal", self.name());
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs(graceful_shutdown_timeout.into())) => {
                            tracing::error!("{} service has badly shutdowned. Some data is lost.", self.name());
                        },
                        _ = async {
                            // drain remaining messages
                            while let Some(task_result) = data_receiver.recv().await {
                                let mut data = self.process_task_result_on_shutdown(task_result, &mut log).await;
                                self.plan_to_append(&mut log, &mut data);
                                self.send_for_rule_processing(data, &mut rules_input).await;
                            }
                            let _ = log.append().await;
                        } => {
                            tracing::info!("{} service has successfully shutdowned", self.name());
                        }
                    }
                    return;
                },
                _ = push_interval.tick() => {
                    let rows_count = log.new_rows();
                    if rows_count == 0 {
                        continue;
                    }
                    let example_rules = self.get_example_rules();
                    example_rules.into_iter().for_each(|mut r| log.plan_to_append(&mut r));
                    let rows_count = log.new_rows();
                    tracing::info!(
                        "appending {} rows for service {}",
                        rows_count,
                        self.name()
                    );
                    if let Err(e) = log.append().await {
                        let msg = format!("`{e}` for service `{}`, failed to append {rows_count} rows", self.name());
                        tracing::error!("{}", msg);
                        if let Some(messenger_config) = &self.messenger_config() && messenger_config.send_google_append_error {
                            self.messenger().unwrap_or(self.shared().send_notification.clone()).try_error(msg);
                        }
                    } else if rows_count > 0 {
                        tracing::info!(
                            "appended {} rows for service {}",
                            rows_count,
                            self.name()
                        );
                    }
                },
                _ = rules_update_interval.tick() => {
                    self.send_updated_rules(&log, &mut rules_input).await;
                }
                Some(task_result) = data_receiver.recv() => {
                    let mut data = self.process_task_result(task_result, &mut log).await;
                    self.plan_to_append(&mut log, &mut data);
                    self.send_for_rule_processing(data, &mut rules_input).await;
                }
                res = &mut tasks => {
                    res.unwrap(); // propagate panics from spawned tasks
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configuration::ceiled_division;
    use crate::google::Storage;
    use crate::google::datavalue::Datavalue;
    use crate::google::spreadsheet::SpreadsheetAPI;
    use crate::google::spreadsheet::tests::TestState;
    use crate::notifications::Sender;
    use crate::services::general::GENERAL_SERVICE_NAME;
    use crate::tests::TEST_HOST_ID;

    use chrono::Utc;

    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio::sync::{
        broadcast,
        mpsc::{self, error::TrySendError},
    };

    struct TestService {
        counter: Arc<AtomicUsize>,
        shared: Shared,
    }

    const APPEND_DURATION_MS: u16 = 100;
    const SCRAPE_INTERVAL_MS: u16 = 20;

    #[async_trait]
    impl Service for TestService {
        fn name(&self) -> &'static str {
            GENERAL_SERVICE_NAME
        }

        fn spreadsheet_id(&self) -> &str {
            "spreadsheet1"
        }

        fn channel_capacity(&self) -> usize {
            // during appending we accumulate
            100 * usize::from(ceiled_division(APPEND_DURATION_MS, SCRAPE_INTERVAL_MS))
        }

        fn push_interval(&self) -> Duration {
            Duration::from_millis(50)
        }

        fn shared(&self) -> &Shared {
            &self.shared
        }

        fn truncate_at(&self) -> f32 {
            100.0
        }

        async fn process_task_result(
            &mut self,
            result: TaskResult,
            _log: &mut AppendableLog,
        ) -> Data {
            let TaskResult { result, .. } = result;
            result.expect("test assert: ok result is sent")
        }

        async fn spawn_tasks(
            &mut self,
            is_shutdown: Arc<AtomicBool>,
            sender: mpsc::Sender<TaskResult>,
        ) -> Vec<JoinHandle<()>> {
            let counter = self.counter.clone();
            vec![tokio::spawn(async move {
                let mut interval =
                    tokio::time::interval(Duration::from_millis(u64::from(SCRAPE_INTERVAL_MS)));
                loop {
                    tokio::select! {
                        _ = interval.tick() => {
                            let result = Ok(Data::Single(
                                Datarow::new(
                                    "log_name1".to_string(),
                                    Utc::now().naive_utc(),
                                    vec![
                                        ("key11".to_string(), Datavalue::HeatmapPercent(3_f64)),
                                        ("key12".to_string(), Datavalue::Size(400_u64)),
                                    ],
                                ),
                            ));

                            match sender.try_send(TaskResult{id: 0, result}) {
                                Err(TrySendError::Full(_)) => {
                                    panic!("test assert: messages queue shouldn't be full");
                                },
                                Err(TrySendError::Closed(_)) => {
                                    if is_shutdown.load(Ordering::Relaxed) {
                                        return;
                                    }
                                    panic!("test assert: messages queue shouldn't be closed before shutdown signal");
                                },
                                _ => {counter.fetch_add(1, Ordering::SeqCst);},
                            };
                        }
                    }
                }
            })]
        }
    }

    #[tokio::test]
    async fn basic_service_flow() {
        let (tx, mut rx) = mpsc::channel(1);
        tokio::task::spawn(async move {
            while let Some(msg) = rx.recv().await {
                println!("message {msg:?}");
            }
        });
        let tx = Sender::new(tx, GENERAL_SERVICE_NAME);
        let data_counter = Arc::new(AtomicUsize::new(0));
        let sheets_api = SpreadsheetAPI::new(
            tx.clone(),
            TestState::new(
                vec![],
                None,
                Some(u64::from(APPEND_DURATION_MS / 2)), // because append is 2 Google api calls
            ),
        );
        let storage = Arc::new(Storage::new(TEST_HOST_ID.to_string(), sheets_api));

        let (shutdown, rx) = broadcast::channel(1);

        let counter = data_counter.clone();
        let shared = Shared {
            messenger: None,
            send_notification: tx.clone(),
        };
        let mut service = TestService { counter, shared };
        let log = AppendableLog::new(
            storage.clone(),
            "spreadsheet1".to_string(),
            GENERAL_SERVICE_NAME.to_string(),
            Some(tx.clone()),
            service.truncate_at(),
        );
        let service = tokio::spawn(async move {
            service.run(log, rx).await;
        });
        tokio::spawn(async move {
            let work_duration = jitter_duration();
            tracing::info!("will work for {:?}", work_duration);
            tokio::time::sleep(work_duration).await;
            shutdown
                .send(1)
                .expect("test assert: test service should run when shutdown signal is sent");
            tracing::info!("shutdown signal is sent");
        });

        service.await.unwrap();

        let all_sheets = storage
            .tables_for_service("spreadsheet1", GENERAL_SERVICE_NAME)
            .await
            .unwrap();
        assert_eq!(
            all_sheets.len(),
            2,
            "only 2 sheets (for `log_name1` and rules) should be created"
        );
        assert!(
            all_sheets[0].name().contains("log_name1")
                || all_sheets[1].name().contains("log_name1")
        );
        assert!(all_sheets[0].name().contains("rules") || all_sheets[1].name().contains("rules"));
        let row_count = if all_sheets[0].name().contains("log_name1") {
            all_sheets[0].rows_count()
        } else {
            all_sheets[1].rows_count()
        };
        assert_eq!(
            usize::try_from(row_count).unwrap() - 1,
            data_counter.load(Ordering::SeqCst), // one row for header row
            "`log_name1` contains all data generated by the service"
        );
    }
}
