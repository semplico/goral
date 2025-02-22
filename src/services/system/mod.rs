pub mod collector;
pub mod configuration;
#[cfg(target_os = "linux")]
pub mod ssh;
use crate::google::datavalue::{Datarow, Datavalue};
use crate::messenger::configuration::MessengerConfig;
use crate::notifications::{MessengerApi, Notification, Sender};
use crate::rules::{Action, Rule, RuleCondition};
use crate::services::system::configuration::{scrape_push_rule, System};
use crate::services::{Data, Service, TaskResult};
use crate::storage::AppendableLog;
use crate::Shared;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::sync::mpsc;
#[cfg(target_os = "linux")]
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

pub const SYSTEM_SERVICE_NAME: &str = "system";
#[cfg(target_os = "linux")]
const MAX_BYTES_SSH_VERSIONS_OUTPUT: usize = 2_usize.pow(16); // ~65 KiB

#[cfg(target_os = "linux")]
async fn ssh_versions() -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let url = "http://changelogs.ubuntu.com/changelogs/pool/main/o/openssh/"
        .parse()
        .expect("assert: ssh versions url is correct");
    let client =
        crate::http::HttpClient::lousy(MAX_BYTES_SSH_VERSIONS_OUTPUT, true, Duration::from_secs(2));
    let res = client.get_text(url).await?;
    Ok(res)
}

enum SystemInfoRequest {
    Telemetry(DateTime<Utc>),
    #[cfg(target_os = "linux")]
    SshNeedUpdate(String, oneshot::Sender<Result<bool, String>>),
    #[cfg(target_os = "linux")]
    OSName(oneshot::Sender<Option<String>>),
    #[cfg(target_os = "linux")]
    IsSupported(oneshot::Sender<Result<bool, String>>),
}

pub struct SystemService {
    shared: Shared,
    spreadsheet_id: String,
    push_interval: Duration,
    scrape_interval: Duration,
    scrape_timeout: Duration,
    mounts: Vec<String>,
    process_names: Vec<String>,
    channel_capacity: usize,
    messenger: Option<MessengerApi>,
    truncate_at: f32,
}

impl SystemService {
    pub fn new(shared: Shared, mut config: System) -> SystemService {
        let channel_capacity = scrape_push_rule(
            &config.scrape_timeout_ms,
            &config.scrape_interval_secs,
            &config.push_interval_secs,
        )
        .expect("assert: push/scrate ratio is validated at configuration");
        let messenger = config
            .messenger
            .take()
            .map(|messenger_config| MessengerApi::new(messenger_config, SYSTEM_SERVICE_NAME));
        Self {
            shared,
            spreadsheet_id: config.spreadsheet_id,
            push_interval: Duration::from_secs(config.push_interval_secs.into()),
            scrape_interval: Duration::from_secs(config.scrape_interval_secs.into()),
            scrape_timeout: Duration::from_millis(config.scrape_timeout_ms.into()),
            mounts: config.mounts,
            process_names: config.process_names,
            channel_capacity,
            messenger,
            truncate_at: config.autotruncate_at_usage_percent,
        }
    }

    fn collect_sysinfo(
        is_shutdown: Arc<AtomicBool>,
        sender: mpsc::Sender<TaskResult>,
        mut request_rx: mpsc::Receiver<SystemInfoRequest>,
        mounts: Vec<String>,
        names: Vec<String>,
        messenger: Sender,
    ) {
        let mut sys = collector::initialize();
        tracing::info!("started system info scraping thread");

        while let Some(request) = request_rx.blocking_recv() {
            match request {
                #[cfg(target_os = "linux")]
                SystemInfoRequest::SshNeedUpdate(changelog, reply_to) => {
                    let response = ssh::check_ssh_needs_update(&changelog);
                    if reply_to.send(response).is_err() {
                        if is_shutdown.load(Ordering::Relaxed) {
                            tracing::info!("exiting system info scraping thread");
                            return;
                        }
                        panic!(
                            "assert: ssh update checker recipient shouldn't be closed before shutdown signal"
                        );
                    }
                }
                #[cfg(target_os = "linux")]
                SystemInfoRequest::OSName(reply_to) => {
                    let collector::SystemInfo { name, .. } = collector::system_info();
                    if reply_to.send(name).is_err() {
                        if is_shutdown.load(Ordering::Relaxed) {
                            tracing::info!("exiting system info scraping thread");
                            return;
                        }
                        panic!(
                            "assert: os name recipient shouldn't be closed before shutdown signal"
                        );
                    }
                }
                #[cfg(target_os = "linux")]
                SystemInfoRequest::IsSupported(reply_to) => {
                    let response = ssh::is_system_still_supported();
                    if reply_to.send(response).is_err() {
                        if is_shutdown.load(Ordering::Relaxed) {
                            tracing::info!("exiting system info scraping thread");
                            return;
                        }
                        panic!(
                            "assert: system support check recipient shouldn't be closed before shutdown signal"
                        );
                    }
                }
                SystemInfoRequest::Telemetry(scrape_time) => {
                    let result = collector::collect(
                        &mut sys,
                        &mounts,
                        &names,
                        scrape_time.naive_utc(),
                        &messenger,
                    )
                    .map(Data::Many)
                    .map_err(|e| Data::Message(format!("sysinfo scraping error {e}")));
                    if sender.blocking_send(TaskResult { id: 0, result }).is_err() {
                        if is_shutdown.load(Ordering::Relaxed) {
                            tracing::info!("exiting system info scraping thread");
                            return;
                        }
                        panic!("assert: sysinfo messages queue shouldn't be closed before shutdown signal");
                    }
                }
            }
        }
    }

    async fn make_system_request(
        request_tx: &mut mpsc::Sender<SystemInfoRequest>,
        timeout: Duration,
        request: SystemInfoRequest,
    ) -> Result<(), String> {
        tokio::select! {
            _ = tokio::time::sleep(timeout) => Err(format!("sysinfo request timeout {:?}", timeout)),
            res = request_tx.send(request) => res.map_err(|e| e.to_string())
        }
    }

    #[cfg(target_os = "linux")]
    async fn ssh_access_observer(
        is_shutdown: Arc<AtomicBool>,
        sender: mpsc::Sender<TaskResult>,
        send_notification: Sender,
        messenger: Sender,
    ) {
        tracing::info!("starting ssh access monitoring");
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        std::thread::Builder::new()
            .name("ssh-access-observer".into())
            .spawn(move || {
                ssh::process_sshd_log(is_shutdown, sender, send_notification, messenger, tx)
            })
            .expect("assert: can spawn ssh access monitoring thread");
        let _ = rx.await;
    }

    #[cfg(target_os = "linux")]
    async fn system_checker(
        is_shutdown: Arc<AtomicBool>,
        send_notification: Sender,
        messenger: Sender,
        mut sys_req_tx: mpsc::Sender<SystemInfoRequest>,
    ) {
        let mut ssh_interval = tokio::time::interval(Duration::from_secs(4 * 60 * 60));
        let mut system_support_interval =
            tokio::time::interval(Duration::from_secs(60 * 60 * 24 * 3));
        let request_timeout = Duration::from_secs(5);
        tracing::info!("starting system updates checking");
        loop {
            tokio::select! {
                _ = ssh_interval.tick() => {
                    let source = match ssh_versions().await {
                        Ok(source) => source,
                        Err(e) => {
                            let msg = format!("error sending ssh versions request `{}`", e);
                            tracing::error!("{}", msg);
                            messenger.error(msg).await;
                            continue;
                        }
                    };
                    let (tx, rx) = oneshot::channel();
                    if let Err(e) = Self::make_system_request(&mut sys_req_tx, request_timeout, SystemInfoRequest::SshNeedUpdate(source, tx)).await {
                        if is_shutdown.load(Ordering::Relaxed) {
                            tracing::info!("finished system updates checking");
                            return;
                        }
                        let msg = format!("error making ssh version check request `{}`", e);
                        tracing::error!("{}", msg);
                        send_notification.fatal(msg).await;
                    }
                    match rx.await {
                        Ok(Ok(true)) => {
                            let msg = "openssh patch version is outdated, update with `sudo apt update && sudo apt install openssh-server`".to_string();
                            tracing::warn!("{}", msg);
                            messenger.warn(msg).await;
                        },
                        Ok(Ok(false)) => continue,

                        Ok(Err(message)) => {
                            let msg = format!("error fetching ssh version update `{message}`");
                            tracing::error!("{}", msg);
                            send_notification.error(msg).await;
                        }
                        Err(e) => {
                            if is_shutdown.load(Ordering::Relaxed) {
                                tracing::info!("finished system updates checking");
                                return;
                            }
                            let msg = format!("error fetching ssh version update `{e}`");
                            tracing::error!("{}", msg);
                            send_notification.fatal(msg).await;
                        }
                    };
                }
                _ = system_support_interval.tick() => {
                    let (tx, rx) = oneshot::channel();
                    if let Err(e) = Self::make_system_request(&mut sys_req_tx, request_timeout, SystemInfoRequest::IsSupported(tx)).await {
                        if is_shutdown.load(Ordering::Relaxed) {
                            tracing::info!("finished system updates checking");
                            return;
                        }
                        let msg = format!("error making system support check request `{}`", e);
                        tracing::error!("{}", msg);
                        send_notification.fatal(msg).await;
                    }
                    match rx.await {
                        Ok(Ok(false)) => {
                            let msg = "the system seems to be [no longer supported](https://wiki.ubuntu.com/Releases)".to_string();
                            tracing::warn!("{}", msg);
                            messenger.warn(msg).await;
                        },
                        Ok(Ok(true)) => continue,

                        Ok(Err(message)) => {
                            let msg = format!("error fetching system support check `{message}`");
                            tracing::error!("{}", msg);
                        }
                        Err(e) => {
                            if is_shutdown.load(Ordering::Relaxed) {
                                tracing::info!("finished system updates checking");
                                return;
                            }
                            let msg = format!("error fetching system updates `{e}`");
                            tracing::error!("{}", msg);
                            send_notification.fatal(msg).await;
                        }
                    };
                }
            }
        }
    }

    #[cfg(target_os = "linux")]
    async fn fetch_os_name(
        is_shutdown: &Arc<AtomicBool>,
        sys_req_tx: &mut mpsc::Sender<SystemInfoRequest>,
        send_notification: &Sender,
    ) -> Option<String> {
        let (tx, rx) = oneshot::channel();
        if let Err(e) = Self::make_system_request(
            sys_req_tx,
            Duration::from_millis(500),
            SystemInfoRequest::OSName(tx),
        )
        .await
        {
            if !is_shutdown.load(Ordering::Relaxed) {
                let msg = format!("error requesting os name `{}`", e);
                tracing::error!("{}", msg);
                send_notification.fatal(msg).await;
            }
            None
        } else {
            rx.await
                .expect("assert: os name sender shouldn't be dropped")
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn sys_observer(
        is_shutdown: Arc<AtomicBool>,
        scrape_interval: Duration,
        scrape_timeout: Duration,
        sender: mpsc::Sender<TaskResult>,
        messenger: Sender,
        send_notification: Sender,
        mounts: Vec<String>,
        names: Vec<String>,
        sys_req_rx: mpsc::Receiver<SystemInfoRequest>,
        mut sys_req_tx: mpsc::Sender<SystemInfoRequest>,
    ) {
        let mut interval = tokio::time::interval(scrape_interval);
        tracing::info!("starting system info scraping");
        let cloned_sender = sender.clone();
        let cloned_is_shutdown = is_shutdown.clone();
        std::thread::Builder::new()
            .name("sysinfo-collector".into())
            .spawn(move || {
                Self::collect_sysinfo(
                    cloned_is_shutdown,
                    cloned_sender,
                    sys_req_rx,
                    mounts,
                    names,
                    messenger,
                )
            })
            .expect("assert: can spawn sysinfo collecting thread");

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let scrape_time = Utc::now();
                    if let Err(e) = Self::make_system_request(&mut sys_req_tx, scrape_timeout, SystemInfoRequest::Telemetry(scrape_time)).await {
                        if is_shutdown.load(Ordering::Relaxed) {
                            tracing::info!("finished sysinfo collection");
                            return;
                        }
                        let msg = format!("error sending request for sysinfo `{}`", e);
                        tracing::error!("{}", msg);
                        send_notification.fatal(msg).await;
                    }
                }
            }
        }
    }
}

#[async_trait]
impl Service for SystemService {
    fn name(&self) -> &'static str {
        SYSTEM_SERVICE_NAME
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
        let mut rows = Vec::with_capacity(3 + self.mounts.len() + 2 * self.process_names.len());
        #[cfg(target_os = "linux")]
        {
            rows.push(
                Rule {
                    log_name: ssh::SSH_LOG.to_string(),
                    key: ssh::SSH_LOG_STATUS.to_string(),
                    condition: RuleCondition::Is,
                    value: Datavalue::Text(ssh::SSH_LOG_STATUS_CONNECTED.to_string()),
                    action: Action::Warn,
                }
                .into(),
            );
        }
        rows.push(
            Rule {
                log_name: collector::BASIC_LOG.to_string(),
                key: collector::MEMORY_USE.to_string(),
                condition: RuleCondition::Greater,
                value: Datavalue::Percent(90.),
                action: Action::Warn,
            }
            .into(),
        );
        rows.push(
            Rule {
                log_name: collector::BASIC_LOG.to_string(),
                key: collector::SWAP_USE.to_string(),
                condition: RuleCondition::Greater,
                value: Datavalue::Percent(90.),
                action: Action::Warn,
            }
            .into(),
        );
        for mount in &self.mounts {
            rows.push(
                Rule {
                    log_name: mount.to_string(),
                    key: collector::DISK_USE.to_string(),
                    condition: RuleCondition::Greater,
                    value: Datavalue::Percent(90.),
                    action: Action::Warn,
                }
                .into(),
            );
        }
        for process_name in &self.process_names {
            rows.push(
                Rule {
                    log_name: process_name.to_string(),
                    key: collector::MEMORY_USE.to_string(),
                    condition: RuleCondition::Greater,
                    value: Datavalue::Percent(90.),
                    action: Action::Warn,
                }
                .into(),
            );
            rows.push(
                Rule {
                    log_name: process_name.to_string(),
                    key: collector::CPU.to_string(),
                    condition: RuleCondition::Greater,
                    value: Datavalue::Percent(90.),
                    action: Action::Warn,
                }
                .into(),
            );
        }
        rows
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
                self.send_error(format!("`{}` while observing system", msg))
                    .await;
                Data::Empty
            }
            _ => panic!("assert: system result contains either many datarows or error text"),
        }
    }

    async fn spawn_tasks(
        &mut self,
        is_shutdown: Arc<AtomicBool>,
        sender: mpsc::Sender<TaskResult>,
    ) -> Vec<JoinHandle<()>> {
        let is_shutdown = is_shutdown.clone();
        let send_notification = self.shared.send_notification.clone();
        let messenger = self
            .messenger()
            .unwrap_or(self.shared.send_notification.clone());
        let mounts = self.mounts.clone();
        let names = self.process_names.clone();
        let scrape_interval = self.scrape_interval;
        let scrape_timeout = self.scrape_timeout;
        let (sys_req_tx, sys_req_rx) = mpsc::channel::<SystemInfoRequest>(2);

        let cloned_is_shutdown = is_shutdown.clone();
        let cloned_sys_req_tx = sys_req_tx.clone();
        let cloned_sender = sender.clone();
        let mut tasks = Vec::with_capacity(3);
        // a separate push to remove cargo clippy warning
        tasks.push(tokio::spawn(async move {
            Self::sys_observer(
                cloned_is_shutdown,
                scrape_interval,
                scrape_timeout,
                cloned_sender,
                messenger,
                send_notification,
                mounts,
                names,
                sys_req_rx,
                cloned_sys_req_tx,
            )
            .await;
        }));

        #[cfg(target_os = "linux")]
        {
            let cloned_is_shutdown = is_shutdown.clone();
            let cloned_sender = sender.clone();
            let send_notification = self.shared.send_notification.clone();
            let messenger = self
                .messenger()
                .unwrap_or(self.shared.send_notification.clone());
            tasks.push(tokio::spawn(async move {
                Self::ssh_access_observer(
                    cloned_is_shutdown,
                    cloned_sender,
                    send_notification,
                    messenger,
                )
                .await;
            }));
            let send_notification = self.shared.send_notification.clone();
            let mut sys_req_tx = sys_req_tx;
            let os_name =
                Self::fetch_os_name(&is_shutdown, &mut sys_req_tx, &send_notification).await;
            if let Some(true) = os_name.map(|name| name.to_lowercase().contains("ubuntu")) {
                let messenger = self
                    .messenger()
                    .unwrap_or(self.shared.send_notification.clone());
                tasks.push(tokio::spawn(async move {
                    Self::system_checker(is_shutdown, send_notification, messenger, sys_req_tx)
                        .await;
                }));
            }
        }
        tasks
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::google::datavalue::Datavalue;
    use crate::notifications::Notification;
    use tracing::Level;

    #[tokio::test]
    async fn single_sys_scrape() {
        const NUM_OF_SCRAPES: usize = 1;
        let (send_notification, mut notifications_receiver) = mpsc::channel(1);
        let send_notification = Sender::new(send_notification, "test");
        let (data_sender, mut data_receiver) = mpsc::channel(NUM_OF_SCRAPES);
        let (req_sender, req_receiver) = mpsc::channel(2);
        let is_shutdown = Arc::new(AtomicBool::new(false));

        let notifications = tokio::spawn(async move {
            while let Some(notification) = notifications_receiver.recv().await {
                println!("Notification received: {notification:?}");
            }
        });

        let is_shutdown_clone = is_shutdown.clone();
        let scrape_handle = tokio::spawn(async move {
            SystemService::sys_observer(
                is_shutdown_clone,
                Duration::from_secs(1),
                Duration::from_secs(2),
                data_sender,
                send_notification.clone(),
                send_notification,
                vec![],
                vec![],
                req_receiver,
                req_sender,
            )
            .await;
        });

        tokio::time::sleep(Duration::from_secs(
            2 * u64::try_from(NUM_OF_SCRAPES).unwrap(),
        ))
        .await;
        is_shutdown.store(true, Ordering::Release);
        data_receiver.close();

        if let Some(TaskResult {
            result: Ok(Data::Many(datarows)),
            ..
        }) = data_receiver.recv().await
        {
            if let Some(Datavalue::HeatmapPercent(mem_use)) =
                datarows[0].keys_values().get("memory_use")
            {
                assert!(*mem_use > 0.0, "memory usage should be positive");
            } else {
                panic!("test assert: memory use should be scraped");
            }
        } else {
            panic!("test assert: at least one successful scrape should be collected");
        }

        scrape_handle.await.unwrap(); // scrape should finish as the data channel is closed
        notifications.await.unwrap();
    }

    #[tokio::test]
    async fn sys_scrape_timeout() {
        let (send_notification, mut notifications_receiver) = mpsc::channel(1);
        let send_notification = Sender::new(send_notification, SYSTEM_SERVICE_NAME);
        let (data_sender, mut data_receiver) = mpsc::channel(1);
        let (req_sender, req_receiver) = mpsc::channel(2);
        let is_shutdown = Arc::new(AtomicBool::new(false));

        let notification = tokio::spawn(async move { notifications_receiver.recv().await });

        let is_shutdown_clone = is_shutdown.clone();
        let scrape_handle = tokio::spawn(async move {
            SystemService::sys_observer(
                is_shutdown_clone,
                Duration::from_secs(1),
                Duration::from_millis(1),
                data_sender,
                send_notification.clone(),
                send_notification,
                vec![],
                vec![],
                req_receiver,
                req_sender,
            )
            .await;
        });
        if let Some(Notification { message, level }) = notification.await.unwrap() {
            assert!(
                message.contains("sysinfo request timeout"),
                "received notification: {}",
                message
            );
            assert_eq!(level, Level::ERROR);
        } else {
            panic!("test assert: at least one timeout should be happen");
        }
        is_shutdown.store(true, Ordering::Release);
        data_receiver.close();
        scrape_handle.await.unwrap(); // scrape should finish as the data channel is closed
    }
}
