use crate::services::general::configuration::General;
use crate::services::healthcheck::configuration::Healthcheck;
use crate::services::kv::configuration::Kv;
use crate::services::logs::configuration::Logs;
use crate::services::metrics::configuration::Metrics;
use crate::services::system::configuration::System;
use config::{Config, ConfigError, Environment, File};
use lazy_static::lazy_static;
use regex::Regex;
use serde::de::{self, Deserialize, Deserializer};
use serde_derive::Deserialize;
use serde_valid::Validate;
use std::collections::HashMap;
use std::fmt::Write;
use std::str::FromStr;
use url::{Host, Url};

pub const APP_NAME: &str = "GORAL";
pub const GOOGLE_SHEET_BASE: &str = "https://docs.google.com/spreadsheets/d/";
pub const MAX_GOOGLE_REQUEST_DURATION_SECS: u16 = 32;

pub fn ceiled_division(divisible: u16, divisor: u16) -> u16 {
    let quotient = divisible / divisor;
    let remainder = divisible % divisor;
    if remainder == 0 {
        quotient
    } else {
        quotient + 1
    }
}

pub fn scrape_timeout_interval_rule(
    scrape_interval_secs: &u16,
    scrape_timeout_ms: &u32,
) -> Result<(), serde_valid::validation::Error> {
    let scrape_interval_ms = u32::from(*scrape_interval_secs) * 1000;
    if scrape_timeout_ms > &scrape_interval_ms {
        return Err(serde_valid::validation::Error::Custom(
            "`scrape_timeout_ms` should be less than `scrape_interval_secs`".to_string(),
        ));
    }
    Ok(())
}

pub fn scrape_interval_secs() -> u16 {
    10
}

pub fn push_interval_secs() -> u16 {
    30
}

pub fn host_validation(url: &Url) -> Result<(), serde_valid::validation::Error> {
    url.host()
        .map(|_| ())
        .ok_or(serde_valid::validation::Error::Custom(format!(
            "host for url {url} should be specified"
        )))
}

pub fn port_validation(url: &Url) -> Result<(), serde_valid::validation::Error> {
    match url.host() {
        Some(Host::Domain(domain)) if !domain.starts_with("localhost") => Ok(()),
        _ => url
            .port()
            .map(|_| ())
            .ok_or(serde_valid::validation::Error::Custom(format!(
                "port for url {url} should be specified"
            ))),
    }
}

pub fn log_name(name: &str) -> Result<(), serde_valid::validation::Error> {
    const REGEX: &str = r"^[^@]+$";
    lazy_static! {
        static ref RE: Regex =
            Regex::new(REGEX).expect("assert: log name regex is properly constructed");
    }
    if !RE.is_match(name) {
        return Err(serde_valid::validation::Error::Custom(format!(
            "name should match a regex `{REGEX}`"
        )));
    }
    Ok(())
}

pub fn log_name_opt(name: &Option<String>) -> Result<(), serde_valid::validation::Error> {
    if let Some(name) = name {
        log_name(name)
    } else {
        Ok(())
    }
}

pub fn case_insensitive_enum<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: FromStr,
    <T as FromStr>::Err: std::fmt::Display,
{
    let variant = String::deserialize(deserializer)?.to_lowercase();
    T::from_str(&variant).map_err(de::Error::custom)
}

#[derive(Debug, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
pub struct Configuration {
    #[validate]
    pub general: General,
    #[validate]
    pub healthcheck: Option<Healthcheck>,
    #[validate]
    pub kv: Option<Kv>,
    #[validate]
    pub logs: Option<Logs>,
    #[validate]
    pub metrics: Option<Metrics>,
    #[validate]
    pub system: Option<System>,
}

impl Configuration {
    pub fn new(config_path: &str) -> Result<Self, ConfigError> {
        let s = Config::builder()
            .add_source(File::with_name(config_path))
            .add_source(Environment::with_prefix(APP_NAME).separator("__"))
            .build()?;
        let deserialized = s.try_deserialize::<Self>()?;
        deserialized
            .validate()
            .map_err(|e| ConfigError::Message(e.to_string()))?;
        Ok(deserialized)
    }

    pub fn check_truncation_limits(&self) -> Result<(), String> {
        let mut limits: HashMap<String, f32> = HashMap::new();

        if let Some(healthcheck) = &self.healthcheck {
            *limits
                .entry(healthcheck.spreadsheet_id.to_string())
                .or_default() += healthcheck.autotruncate_at_usage_percent;
        }

        if let Some(metrics) = &self.metrics {
            *limits
                .entry(metrics.spreadsheet_id.to_string())
                .or_default() += metrics.autotruncate_at_usage_percent;
        }

        if let Some(logs) = &self.logs {
            *limits.entry(logs.spreadsheet_id.to_string()).or_default() +=
                logs.autotruncate_at_usage_percent;
        }

        if let Some(system) = &self.system {
            *limits.entry(system.spreadsheet_id.to_string()).or_default() +=
                system.autotruncate_at_usage_percent;
        }

        if let Some(kv) = &self.kv {
            *limits.entry(kv.spreadsheet_id.to_string()).or_default() +=
                kv.autotruncate_at_usage_percent;
        }

        let mut message = String::new();
        for (spreadsheet_id, total) in limits {
            if total > 100.0 {
                write!(
                    &mut message,
                    "current usage limits sum up to {total}% for [spreadsheet]({GOOGLE_SHEET_BASE}{spreadsheet_id}),"
                )
                .expect("assert: can write to string");
            }
        }
        if !message.is_empty() {
            write!(&mut message, " there is a risk to hit a storage quota for Google sheets, [docs](https://semplico.github.io/goral/services.html#storage-quota)").expect("assert: can write to string");
            Err(message)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use config::FileFormat;
    use std::str::FromStr;
    use url::Url;

    pub fn build_config<T: for<'a> Deserialize<'a> + Validate>(
        contents: &str,
    ) -> Result<T, ConfigError> {
        let s = Config::builder()
            .add_source(File::from_str(contents, FileFormat::Toml))
            .build()?;
        let deserialized = s.try_deserialize::<T>().map_err(|e| {
            let e = e.to_string();
            let e = if e.contains("untagged enum MessengerImplementation") {
                "messenger specific configuration is incorrect".to_string()
            } else {
                e
            };
            ConfigError::Message(e)
        })?;
        deserialized
            .validate()
            .map_err(|e| ConfigError::Message(e.to_string()))?;
        Ok(deserialized)
    }

    #[test]
    fn minimal_config() {
        let config = r#"
        [general]
        service_account_credentials_path = "/path/to/service_account.json"
        messenger.specific.chat_id = "test_chat_id"
        messenger.url = "https://api.telegram.org/bot123/sendMessage"
        "#;

        let config: Configuration =
            build_config(config).expect("should be able to build minimum configuration");
        assert_eq!(
            config.general.service_account_credentials_path,
            "/path/to/service_account.json"
        );
        assert_eq!(
            config.general.messenger.url,
            Url::from_str("https://api.telegram.org/bot123/sendMessage").unwrap()
        );
    }

    #[test]
    #[should_panic(expected = "messenger specific configuration is incorrect")]
    fn wrong_messenger_config_no_match() {
        let config = r#"
        [general]
        service_account_credentials_path = "/path/to/service_account.json"
        messenger.specific.channel = "RKSWKAHBF"
        messenger.url = "https://api.telegram.org/bot123/sendMessage"
        "#;

        let _: Configuration = build_config(config).unwrap();
    }

    #[test]
    fn division() {
        assert_eq!(ceiled_division(7, 5), 2);
        assert_eq!(ceiled_division(10, 5), 2);
        assert_eq!(ceiled_division(11, 5), 3);
    }

    #[test]
    fn log_name_validation() {
        assert!(log_name("john@mail.org").is_err());
        assert!(log_name("john_mail.org").is_ok());
        assert!(log_name("http://john_mail.org/path").is_ok());
    }

    #[test]
    fn port_check() {
        assert!(port_validation(&Url::from_str("https://www.example.com").unwrap()).is_ok());
        assert!(port_validation(&Url::from_str("https://localhost").unwrap()).is_err());
        assert!(port_validation(&Url::from_str("https://127.0.0.1").unwrap()).is_err());
        assert!(port_validation(&Url::from_str("https://127.0.0.1:8000").unwrap()).is_ok());
        assert!(port_validation(&Url::from_str("https://localhost:9000").unwrap()).is_ok());
    }
}
