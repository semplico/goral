pub mod configuration;
mod discord;
mod slack;
mod telegram;
use crate::http::HttpClient;
use crate::messenger::configuration::MessengerConfig;
use crate::messenger::discord::Discord;
use crate::messenger::slack::Slack;
use crate::messenger::telegram::Telegram;
use anyhow::Result;
use anyhow::anyhow;
use async_trait::async_trait;
use tracing::Level;

pub type BoxedMessenger = Box<dyn Messenger + Sync + Send>;

#[async_trait]
pub trait Messenger {
    // when implementing info, consider suppressing mesenger's notifications for it
    // if the messenger allows
    async fn send_info(&self, config: &MessengerConfig, markdown: &str) -> Result<()>;
    async fn send_warning(&self, config: &MessengerConfig, markdown: &str) -> Result<()>;
    async fn send_error(&self, config: &MessengerConfig, markdown: &str) -> Result<()>;
    async fn send_by_level(
        &self,
        config: &MessengerConfig,
        markdown: &str,
        level: Level,
    ) -> Result<()> {
        match level {
            Level::INFO | Level::DEBUG | Level::TRACE => self.send_info(config, markdown).await,
            Level::WARN => self.send_warning(config, markdown).await,
            Level::ERROR => self.send_error(config, markdown).await,
        }
    }
}

pub fn get_messenger(host: &str) -> Result<BoxedMessenger> {
    let client = HttpClient::strict(32768, true);
    if host.contains("telegram") {
        Ok(Box::new(Telegram::new(client)))
    } else if host.contains("slack") {
        Ok(Box::new(Slack::new(client)))
    } else if host.contains("discord") {
        Ok(Box::new(Discord::new(client)))
    } else {
        Err(anyhow!(
            "Unsupported messenger. Currently available: Telegram, Slack, Discord"
        ))
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    pub struct TestMessenger {
        counter: Arc<AtomicUsize>,
    }

    impl TestMessenger {
        pub fn new(counter: Arc<AtomicUsize>) -> Self {
            Self { counter }
        }
        async fn send_message(&self, _: &MessengerConfig, _: &str) -> Result<()> {
            self.counter.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[async_trait]
    impl Messenger for TestMessenger {
        async fn send_info(&self, config: &MessengerConfig, markdown: &str) -> Result<()> {
            self.send_message(config, markdown).await
        }

        async fn send_warning(&self, config: &MessengerConfig, markdown: &str) -> Result<()> {
            self.send_message(config, markdown).await
        }

        async fn send_error(&self, config: &MessengerConfig, markdown: &str) -> Result<()> {
            self.send_message(config, markdown).await
        }
    }
}
