use crate::configuration::case_insensitive_enum;
use crate::messenger::configuration::MessengerConfig;

use serde_derive::Deserialize;
use serde_valid::Validate;

use tracing::Level;

fn log_level() -> Level {
    Level::INFO
}

fn graceful_timeout_secs() -> u16 {
    5
}

#[derive(Debug, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
pub struct General {
    #[serde(default = "log_level")]
    #[serde(deserialize_with = "case_insensitive_enum")]
    pub log_level: Level,
    pub service_account_credentials_path: String,
    #[validate]
    pub messenger: MessengerConfig,
    #[validate(minimum = 1)]
    #[serde(default = "graceful_timeout_secs")]
    pub graceful_timeout_secs: u16,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configuration::tests::build_config;
    use std::str::FromStr;
    use url::Url;

    #[test]
    fn minimal_confg() {
        let config = r#"
        service_account_credentials_path = "/path/to/service_account.json"
        messenger.specific.chat_id = "test_chat_id"
        messenger.url = "https://api.telegram.org/bot123/sendMessage"
        "#;

        let config: General =
            build_config(config).expect("should be able to build minimum configuration");
        assert_eq!(
            config.service_account_credentials_path,
            "/path/to/service_account.json"
        );
        assert_eq!(
            config.messenger.url,
            Url::from_str("https://api.telegram.org/bot123/sendMessage").unwrap()
        );
        // Check defaults
        assert_eq!(config.log_level, Level::INFO, "wrong default value");
        assert_eq!(config.graceful_timeout_secs, 5, "wrong default value");
    }

    #[test]
    fn log_level_is_case_insensitive() {
        let config = r#"
        log_level = "dEbug"
        service_account_credentials_path = "/path/to/service_account.json"
        messenger.specific.chat_id = "test_chat_id"
        messenger.url = "https://api.telegram.org/bot123/sendMessage"
        "#;

        let config: General =
            build_config(config).expect("should be able to build minimum configuration");
        assert_eq!(config.log_level, Level::DEBUG);
    }

    #[test]
    #[should_panic(expected = "missing configuration field \"messenger\"")]
    fn messenger_is_required() {
        let config = r#"
        log_level = "dEbug"
        service_account_credentials_path = "/path/to/service_account.json"
        "#;

        let _: General = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "graceful_timeout_secs")]
    fn graceful_timeout_cannot_be_zero() {
        let config = r#"
        service_account_credentials_path = "/path/to/service_account.json"
        messenger.specific.chat_id = "test_chat_id"
        messenger.url = "https://api.telegram.org/bot123/sendMessage"
        graceful_timeout_secs = 0
        "#;

        let _: General = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "doesn't match messenger implementation")]
    fn wrong_messenger_confg_wrong_host() {
        let config = r#"
        service_account_credentials_path = "/path/to/service_account.json"
        messenger.url = "https://api.telegram.org/bot123/sendMessage"
        "#;

        let _: General = build_config(config).unwrap();
    }
}
