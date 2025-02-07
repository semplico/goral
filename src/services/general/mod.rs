pub mod configuration;

use crate::configuration::APP_NAME;
use crate::http::HttpClient;
use crate::messenger::configuration::MessengerConfig;
use crate::messenger::BoxedMessenger;
use crate::notifications::{Notification, Sender};
use crate::services::general::configuration::General;
use crate::services::Service;
use crate::storage::AppendableLog;
use crate::Shared;
use async_trait::async_trait;
use serde::Deserialize;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::sync::mpsc::Receiver;
use tracing::Level;

pub const GENERAL_SERVICE_NAME: &str = "general";

#[derive(Deserialize, Debug)]
struct GithubRelease {
    tag_name: String,
    prerelease: bool,
}

async fn latest_release() -> Result<GithubRelease, Box<dyn std::error::Error + Send + Sync>> {
    let url = "https://api.github.com/repos/maksimryndin/goral/releases/latest"
        .parse()
        .expect("assert: latest release url is correct");
    let client = HttpClient::strict(32768, true);
    let res = client.get_text(url).await?;
    Ok(serde_json::from_str(&res)?)
}

async fn release_check(send_notification: Sender) {
    let mut release_check_interval = tokio::time::interval(Duration::from_secs(60 * 60 * 24 * 3));
    loop {
        tokio::select! {
            _ = release_check_interval.tick() => {
                let release = latest_release().await;
                match release {
                    Ok(release) => {
                        let current = env!("CARGO_PKG_VERSION");
                        let latest = release.tag_name;
                        if !release.prerelease && current != latest {
                            let msg = format!(
                                "Your `{APP_NAME}` version `{current}` is not the latest `{latest}`, \
                                 consider [upgrading](https://github.com/maksimryndin/goral/releases); \
                                 if you like `{APP_NAME}`, consider giving a star to the [repo](https://github.com/maksimryndin/goral); \
                                 thank you😊"
                            );
                            send_notification.info(msg).await;
                        }
                    },
                    Err(e) => {
                        tracing::error!("error {} when fetching the latest {} release", e, APP_NAME);
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct GeneralService {
    shared: Shared,
    messenger_config: MessengerConfig,
    log_level: Level,
    channel: Receiver<Notification>,
}

impl GeneralService {
    pub fn new(shared: Shared, config: General, channel: Receiver<Notification>) -> GeneralService {
        Self {
            shared,
            messenger_config: config.messenger,
            log_level: config.log_level,
            channel,
        }
    }

    async fn send_notification(
        &self,
        notification: Notification,
        messenger: &Arc<BoxedMessenger>,
        host_id: &str,
    ) {
        tracing::debug!(
            "{} got notification to send: {:?}",
            self.name(),
            notification
        );

        let result = messenger
            .send_by_level(
                &self.messenger_config,
                &format!("*{host_id}*: {}", &notification.message),
                notification.level,
            )
            .await;
        if result.is_err() {
            tracing::error!(
                "{} service failed to send message: {:?}",
                self.name(),
                notification
            );
        }
    }

    async fn collect_notifications(&mut self, host_id: &str) {
        let messenger = self
            .shared
            .messenger
            .clone()
            .expect("assert: messenger is always configured for general service");
        while let Some(notification) = self.channel.recv().await {
            tracing::debug!("{:?}", notification);
            self.send_notification(notification, &messenger, host_id)
                .await;
        }
    }
}

#[async_trait]
impl Service for GeneralService {
    fn name(&self) -> &'static str {
        GENERAL_SERVICE_NAME
    }

    fn spreadsheet_id(&self) -> &str {
        ""
    }

    fn shared(&self) -> &Shared {
        &self.shared
    }

    fn truncate_at(&self) -> f32 {
        100.0
    }

    async fn run(&mut self, log: AppendableLog, mut shutdown: broadcast::Receiver<u16>) {
        tracing::info!("running with log level {}", self.log_level);
        let send_notification = self.shared().send_notification.clone();
        let collect = self.collect_notifications(log.host_id());
        tokio::pin!(collect); // pin and pass by mutable ref to prevent cancelling this future by select!
        let release_checker = tokio::spawn(release_check(send_notification));
        tokio::pin!(release_checker);

        tokio::select! {
            result = shutdown.recv() => {
                let graceful_shutdown_timeout = match result {
                    Err(_) => panic!("assert: shutdown signal sender should be dropped after all service listeners"),
                    Ok(graceful_shutdown_timeout) => graceful_shutdown_timeout,
                };
                tracing::info!("{} service has got shutdown signal", GENERAL_SERVICE_NAME);
                // we read out messages from other services and components as much as we can (so we don't close the channel)
                assert!(graceful_shutdown_timeout > 0, "graceful_shutdown_timeout is validated in configuration to be positive");
                let graceful_shutdown_timeout = graceful_shutdown_timeout - 1;
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(graceful_shutdown_timeout.into())) => {},
                    _ = &mut collect => {}
                }
                tracing::info!("{} service has successfully shutdowned", GENERAL_SERVICE_NAME);
                return;
            },
            _ = &mut collect => {},
            res = &mut release_checker => {
                res.unwrap(); // propagate panics from spawned tasks
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configuration::tests::build_config;
    use crate::google::spreadsheet::tests::TestState;
    use crate::google::spreadsheet::SpreadsheetAPI;
    use crate::messenger::tests::TestMessenger;
    use crate::notifications::Sender;
    use crate::storage::Storage;
    use crate::tests::TEST_HOST_ID;
    use crate::Shared;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use tokio::sync::{broadcast, mpsc};

    #[tokio::test]
    async fn general_service_flow() {
        const NUMBER_OF_MESSAGES: usize = 10;

        let (send_notification, notifications_receiver) = mpsc::channel(NUMBER_OF_MESSAGES);
        let send_notification = Sender::new(send_notification, GENERAL_SERVICE_NAME);
        let sheets_api = SpreadsheetAPI::new(
            send_notification.clone(),
            TestState::new(vec![], None, None),
        );
        let storage = Arc::new(Storage::new(TEST_HOST_ID.to_string(), sheets_api));
        let log = AppendableLog::new(
            storage.clone(),
            "spreadsheet1".to_string(),
            GENERAL_SERVICE_NAME.to_string(),
            Some(send_notification.clone()),
            100.0,
        );

        let (shutdown, rx) = broadcast::channel(1);

        let config = r#"
        service_account_credentials_path = "/path/to/service_account.json"
        messenger.url = "https://discord.com/api/webhooks/123/456"
        "#;

        let config: General =
            build_config(config).expect("should be able to build minimum configuration");

        let shared = Shared::new(send_notification);
        let notifications_counter = Arc::new(AtomicUsize::new(0));
        let messenger =
            Arc::new(Box::new(TestMessenger::new(notifications_counter.clone())) as BoxedMessenger);
        let cloned_messenger = messenger.clone();
        let shared_clone = shared.clone();
        let service = tokio::spawn(async move {
            let mut service = GeneralService::new(
                shared_clone.clone_with_messenger(Some(cloned_messenger)),
                config,
                notifications_receiver,
            );
            service.run(log, rx).await;
        });

        for _ in 0..NUMBER_OF_MESSAGES {
            shared
                .send_notification
                .try_info(format!("some notification"));
        }

        shutdown
            .send(1)
            .expect("test assert: test service should run when shutdown signal is sent");

        service.await.unwrap();
        assert_eq!(
            notifications_counter.load(Ordering::SeqCst),
            NUMBER_OF_MESSAGES
        );
    }
}
