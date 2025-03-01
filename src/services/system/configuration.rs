use crate::configuration::{
    ceiled_division, scrape_interval_secs, scrape_timeout_interval_rule, APP_NAME,
    MAX_GOOGLE_REQUEST_DURATION_SECS,
};
use crate::messenger::configuration::MessengerConfig;

use serde_derive::Deserialize;
use serde_valid::Validate;

pub(super) fn scrape_push_rule_validate(
    scrape_timeout_ms: &u32,
    scrape_interval_secs: &u16,
    push_interval_secs: &u16,
) -> Result<(), serde_valid::validation::Error> {
    scrape_push_rule(scrape_timeout_ms, scrape_interval_secs, push_interval_secs)?;
    Ok(())
}

pub(super) fn scrape_push_rule(
    scrape_timeout_ms: &u32,
    scrape_interval_secs: &u16,
    push_interval_secs: &u16,
) -> Result<usize, serde_valid::validation::Error> {
    if scrape_interval_secs > push_interval_secs {
        return Err(serde_valid::validation::Error::Custom(
            format!("push interval ({push_interval_secs}) shouldn't be greater than scrape interval {scrape_interval_secs}")
        ));
    }

    if *scrape_timeout_ms > u32::from(*scrape_interval_secs) * 1000 {
        return Err(serde_valid::validation::Error::Custom(
            format!("Scrape timeout ({scrape_timeout_ms}ms) shouldn't be greater than scrape interval ({scrape_interval_secs}s)")
        ));
    }

    #[cfg(target_os = "linux")]
    const AVERAGE_DATAROWS_PER_SCRAPE: u16 = 15; // see collector.rs, ssh.rs
    #[cfg(target_os = "linux")]
    const LIMIT: u16 = 90;
    #[cfg(not(target_os = "linux"))]
    const AVERAGE_DATAROWS_PER_SCRAPE: u16 = 10; // see collector.rs
    #[cfg(not(target_os = "linux"))]
    const LIMIT: u16 = 40;

    // We can wait for the previous request to finish during MAX_GOOGLE_REQUEST_DURATION_SECS
    let number_of_rows_in_batch = ceiled_division(
        MAX_GOOGLE_REQUEST_DURATION_SECS.max(*push_interval_secs),
        *scrape_interval_secs,
    ) * AVERAGE_DATAROWS_PER_SCRAPE;
    if number_of_rows_in_batch > LIMIT {
        return Err(serde_valid::validation::Error::Custom(
            format!("push interval ({push_interval_secs}) is too big (if more than {MAX_GOOGLE_REQUEST_DURATION_SECS}s) or scrape interval ({scrape_interval_secs}) is too small - too much data ({number_of_rows_in_batch} rows vs limit of {LIMIT}) would be accumulated before saving to a spreadsheet")
        ));
    }
    // appending to log is time-consuming
    // during the append we accumulate datarows in the channel
    // Estimate of append duration - 1 sec per row
    let append_duration = number_of_rows_in_batch;
    let number_of_queued_rows =
        ceiled_division(append_duration, *scrape_interval_secs) * AVERAGE_DATAROWS_PER_SCRAPE;
    if number_of_queued_rows > LIMIT {
        return Err(serde_valid::validation::Error::Custom(
            format!("push interval ({push_interval_secs}) is too big (if more than {MAX_GOOGLE_REQUEST_DURATION_SECS}s) or scrape interval ({scrape_interval_secs}) is too small - too much data (estimated {number_of_queued_rows} rows vs limit of {LIMIT}) would be accumulated before saving to a spreadsheet")
        ));
    }
    Ok(number_of_queued_rows.into())
}

fn scrape_timeout_ms() -> u32 {
    3000
}

pub fn push_interval_secs() -> u16 {
    20
}

fn mounts() -> Vec<String> {
    vec!["/".to_string()]
}

fn process_names() -> Vec<String> {
    vec![APP_NAME.to_lowercase()]
}

fn autotruncate_at_usage_percent() -> f32 {
    20.0
}

#[derive(Debug, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
#[validate(custom = |s| scrape_timeout_interval_rule(&s.scrape_interval_secs, &s.scrape_timeout_ms))]
#[validate(custom = |s| scrape_push_rule_validate(&s.scrape_timeout_ms, &s.scrape_interval_secs, &s.push_interval_secs))]
#[allow(unused)]
pub struct System {
    pub spreadsheet_id: String,
    #[validate]
    pub messenger: Option<MessengerConfig>,
    #[validate(minimum = 10)]
    #[serde(default = "push_interval_secs")]
    pub push_interval_secs: u16,
    #[validate(minimum = 1)]
    #[serde(default = "scrape_interval_secs")]
    pub scrape_interval_secs: u16,
    #[validate(minimum = 1)]
    #[serde(default = "scrape_timeout_ms")]
    pub scrape_timeout_ms: u32,
    #[serde(default = "mounts")]
    pub mounts: Vec<String>,
    #[serde(default = "process_names")]
    pub process_names: Vec<String>,
    #[serde(default = "autotruncate_at_usage_percent")]
    #[validate(minimum = 0.0)]
    #[validate(maximum = 100.0)]
    pub autotruncate_at_usage_percent: f32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configuration::tests::build_config;

    #[test]
    fn minimal_confg() {
        let config = r#"
        spreadsheet_id = "123"
        "#;

        let config: System =
            build_config(config).expect("should be able to build minimum configuration");
        assert_eq!(config.spreadsheet_id, "123");
        // Defaults
        assert_eq!(config.push_interval_secs, 20);
        assert_eq!(config.scrape_interval_secs, 10);
        assert_eq!(config.scrape_timeout_ms, 3000);
        assert_eq!(config.mounts, mounts());
        assert_eq!(config.process_names, process_names());
        assert!(
            scrape_push_rule(
                &config.scrape_timeout_ms,
                &config.scrape_interval_secs,
                &config.push_interval_secs
            )
            .expect("channel capacity should be calculated for defaults")
                > 0
        );
    }

    #[test]
    #[should_panic(expected = "timeout_ms")]
    fn scrape_timeout_cannot_be_zero() {
        let config = r#"
        spreadsheet_id = "123"
        scrape_timeout_ms = 0
        "#;

        let _: System = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "scrape_timeout_ms")]
    fn scrape_interval_cannot_be_less_than_timeout() {
        let config = r#"
        spreadsheet_id = "123"
        scrape_interval_secs = 10
        scrape_timeout_ms = 11000
        "#;

        let _: System = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "scrape_interval_secs")]
    fn scrape_interval_secs_cannot_be_zero() {
        let config = r#"
        spreadsheet_id = "123"
        scrape_interval_secs = 0
        "#;

        let _: System = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "shouldn't be greater than scrape")]
    fn scrape_interval_cannot_be_greater_than_push_interval() {
        let config = r#"
        spreadsheet_id = "123"
        push_interval_secs = 5
        scrape_interval_secs = 10
        "#;

        let _: System = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "would be accumulated before saving to a spreadsheet")]
    fn scrape_push_violation() {
        let config = r#"
        spreadsheet_id = "123"
        push_interval_secs = 60
        scrape_interval_secs = 10
        "#;

        let _: System = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "push_interval_secs")]
    fn push_interval_cannot_be_less_than_minimum() {
        let config = r#"
        spreadsheet_id = "123"
        push_interval_secs = 9
        "#;

        let _: System = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "doesn't match messenger implementation")]
    fn wrong_messenger_confg_wrong_host() {
        let config = r#"
        messenger.url = "https://api.telegram.org/bot123/sendMessage"
        spreadsheet_id = "123"
        "#;

        let _: System = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "autotruncate_at_usage_percent")]
    fn truncate_percent_cannot_be_less_than_minimum() {
        let config = r#"
        spreadsheet_id = "123"
        autotruncate_at_usage_percent = -10
        "#;

        let _: System = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "autotruncate_at_usage_percent")]
    fn truncate_percent_cannot_be_greater_than_maximum() {
        let config = r#"
        spreadsheet_id = "123"
        autotruncate_at_usage_percent = 109
        "#;

        let _: System = build_config(config).unwrap();
    }
}
