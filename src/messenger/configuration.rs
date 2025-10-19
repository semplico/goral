use crate::configuration::host_validation;

use serde_derive::Deserialize;
use serde_valid::Validate;
use std::fmt::{self, Debug};
use url::{Host, Url};

fn messenger_implementation_host_rule(
    implementation: &Option<MessengerImplementation>,
    url: &Url,
) -> Result<(), serde_valid::validation::Error> {
    host_validation(url)?;
    let host = url
        .host()
        .expect("assert: messenger url is validated to have a host")
        .to_string();
    match implementation {
        Some(MessengerImplementation::Telegram { .. }) if host.contains("telegram") => {}
        Some(MessengerImplementation::Slack { .. }) if host.contains("slack") => {}
        None if host.contains("discord") => {}
        _ => {
            return Err(serde_valid::validation::Error::Custom(format!(
                "Messenger host {host} doesn't match messenger implementation in configuration"
            )));
        }
    }
    Ok(())
}

#[derive(Deserialize, Clone, PartialEq)]
#[serde(untagged)]
pub enum MessengerImplementation {
    Slack { token: String, channel: String },
    Telegram { chat_id: String },
    Discord,
}

impl Debug for MessengerImplementation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Telegram { chat_id } => {
                write!(f, "Telegram({chat_id})")
            }
            Self::Slack { channel, .. } => {
                write!(f, "Slack({channel})")
            }
            Self::Discord => {
                write!(f, "Discord")
            }
        }
    }
}

fn send_google_append_error() -> bool {
    true
}

#[derive(Deserialize, Clone, Validate)]
#[validate(custom = |s| messenger_implementation_host_rule(&s.implementation, &s.url))]
#[serde(deny_unknown_fields)]
pub struct MessengerConfig {
    #[serde(rename(deserialize = "specific"))]
    pub implementation: Option<MessengerImplementation>,
    pub url: Url,
    #[serde(default)]
    pub send_rules_update_error: bool,
    #[serde(default = "send_google_append_error")]
    pub send_google_append_error: bool,
}

impl MessengerConfig {
    pub fn host(&self) -> Host<&str> {
        self.url
            .host()
            .expect("assert: host is validated to be nonempty in config")
    }
}

impl Debug for MessengerConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "MessengerConfig({})",
            self.url
                .host()
                .expect("assert: host for messenger is validated at configuration")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configuration::tests::build_config;
    use std::str::FromStr;
    use url::Url;

    #[test]
    fn normal_config() {
        let config = r#"
        specific.chat_id = "test_chat_id"
        url = "https://api.telegram.org/bot123/sendMessage"
        "#;

        let config: MessengerConfig =
            build_config(config).expect("should be able to build minimum configuration");
        assert_eq!(
            config.implementation,
            Some(MessengerImplementation::Telegram {
                chat_id: "test_chat_id".to_string()
            })
        );
        assert_eq!(
            config.url,
            Url::from_str("https://api.telegram.org/bot123/sendMessage").unwrap()
        );
        assert!(config.url.as_str().contains("api.telegram.org"));
        assert_eq!(
            config.url.host().map(|h| h.to_string()),
            Some("api.telegram.org".to_string()),
            "host is required"
        );
        assert!(!config.send_rules_update_error,);
        assert!(config.send_google_append_error,);
    }

    #[test]
    fn token_and_url_are_not_printed() {
        let config = r#"
        specific.chat_id = "test_chat_id"
        url = "https://api.telegram.org/bot123/sendMessage"
        "#;

        let config: MessengerConfig =
            build_config(config).expect("should be able to build minimum configuration");
        // We shouldn't output confidential token and full url in logs
        let printed = format!("{config:?}");
        assert!(
            !printed.contains("123"),
            "debug print doesn't contain secret token"
        );
        assert!(
            !printed.contains("/bot123/sendMessage"),
            "debug print doesn't contain url path as it may contain the token"
        );
    }

    #[test]
    #[should_panic(expected = "host")]
    fn host_is_required() {
        let config = r#"
        specific.chat_id = "test_chat_id"
        url = "https://"
        "#;
        let _: MessengerConfig = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "missing configuration field \"url\"")]
    fn url_is_required() {
        let config = r#"
        specific.chat_id = "test_chat_id"
        "#;

        let _: MessengerConfig = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(
        expected = "Messenger host api.telegram.org doesn't match messenger implementation in configuration"
    )]
    fn specific_is_required_for_telegram() {
        let config = r#"
        url = "https://api.telegram.org/bot123/sendMessage"
        "#;

        let _: MessengerConfig = build_config(config).unwrap();
    }
}
