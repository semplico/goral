use crate::http::HttpClient;
use crate::messenger::configuration::MessengerConfig;
use crate::messenger::Messenger;
use anyhow::anyhow;
use anyhow::Result;
use async_trait::async_trait;
use serde_derive::Serialize;

pub struct Discord {
    client: HttpClient,
}

impl Discord {
    pub fn new(client: HttpClient) -> Self {
        Self { client }
    }

    async fn send_message(&self, config: &MessengerConfig, markdown: &str) -> Result<()> {
        let mut url = config.url.clone();
        url.query_pairs_mut().clear();
        let url = url.as_str().parse()?;
        let body = DiscordRequestBody::new(markdown);
        let (_, resp) = self.client.post_json(url, vec![], &body).await?;
        tracing::debug!("{:?}", resp);
        if resp.status() != 204 {
            tracing::error!("discord error response {:?}", resp);
            return Err(anyhow!("discord error"));
        }
        Ok(())
    }
}

#[derive(Debug, Serialize)]
struct DiscordRequestBody<'a> {
    content: &'a str,
}

impl<'a> DiscordRequestBody<'a> {
    fn new(content: &'a str) -> Self {
        Self { content }
    }
}

#[async_trait]
impl Messenger for Discord {
    async fn send_info(&self, config: &MessengerConfig, markdown: &str) -> Result<()> {
        self.send_message(config, format!("🟢 {markdown}").as_str())
            .await
    }

    async fn send_warning(&self, config: &MessengerConfig, markdown: &str) -> Result<()> {
        self.send_message(config, format!("🟡 {markdown}").as_str())
            .await
    }

    async fn send_error(&self, config: &MessengerConfig, markdown: &str) -> Result<()> {
        self.send_message(config, format!("🔴 {markdown}").as_str())
            .await
    }
}
