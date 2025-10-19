use crate::configuration::MAX_GOOGLE_REQUEST_DURATION_SECS;
use crate::messenger::configuration::MessengerConfig;

use serde_derive::Deserialize;
use serde_valid::Validate;

pub(super) fn channel_capacity(push_interval_secs: &u16) -> usize {
    const AVERAGE_DATAROWS_PER_SECOND: u16 = 10; // estimate of average log lines per second
    let number_of_rows_in_batch =
        AVERAGE_DATAROWS_PER_SECOND * MAX_GOOGLE_REQUEST_DURATION_SECS.max(*push_interval_secs);
    // appending to log is time-consuming
    // during the append we accumulate datarows in the channel
    // Estimate of append duration - 1 sec per row
    let append_duration = number_of_rows_in_batch;
    let number_of_queued_rows = append_duration * AVERAGE_DATAROWS_PER_SECOND;
    usize::from(number_of_queued_rows)
}

fn if_contains(
    filter_if_contains: &Option<Vec<String>>,
    drop_if_contains: &Option<Vec<String>>,
) -> Result<(), serde_valid::validation::Error> {
    match (filter_if_contains, drop_if_contains) {
        (Some(filter), Some(dropping)) if !filter.is_empty() && !dropping.is_empty() => {
            Err(serde_valid::validation::Error::Custom("either `filter_if_contains` or `drop_if_contains` should be specified but not both".to_string()))
        },
        _ => Ok(())
    }
}

fn logs_push_interval_secs() -> u16 {
    5
}

fn autotruncate_at_usage_percent() -> f32 {
    30.0
}

#[derive(Debug, Deserialize, Validate)]
#[validate(custom = |s| if_contains(&s.filter_if_contains, &s.drop_if_contains))]
#[serde(deny_unknown_fields)]
#[allow(unused)]
pub struct Logs {
    pub spreadsheet_id: String,
    #[validate]
    pub messenger: Option<MessengerConfig>,
    #[validate(maximum = 10)]
    #[validate(minimum = 3)]
    #[serde(default = "logs_push_interval_secs")]
    pub push_interval_secs: u16,
    #[validate(min_items = 1)]
    pub filter_if_contains: Option<Vec<String>>,
    #[validate(min_items = 1)]
    pub drop_if_contains: Option<Vec<String>>,
    #[validate(minimum = 0.0)]
    #[validate(maximum = 100.0)]
    #[serde(default = "autotruncate_at_usage_percent")]
    pub autotruncate_at_usage_percent: f32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configuration::tests::build_config;

    #[test]
    fn minimal_config() {
        let config = r#"
        spreadsheet_id = "123"
        "#;

        let config: Logs =
            build_config(config).expect("should be able to build minimum configuration");
        assert_eq!(config.spreadsheet_id, "123");
        // Defaults
        assert_eq!(config.push_interval_secs, 5);
        assert_eq!(config.filter_if_contains, None);
        assert!(channel_capacity(&config.push_interval_secs) > 0);
    }

    #[test]
    #[should_panic(expected = "filter_if_contains")]
    fn filter_if_contains_cannot_be_empty() {
        let config = r#"
        spreadsheet_id = "123"
        filter_if_contains = []
        "#;

        let _: Logs = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "drop_if_contains")]
    fn drop_if_contains_cannot_be_empty() {
        let config = r#"
        spreadsheet_id = "123"
        drop_if_contains = []
        "#;

        let _: Logs = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "push_interval_secs")]
    fn push_interval_cannot_be_less_than_minimum() {
        let config = r#"
        spreadsheet_id = "123"
        push_interval_secs = 2
        "#;

        let _: Logs = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "push_interval_secs")]
    fn push_interval_cannot_be_greater_than_maximum() {
        let config = r#"
        spreadsheet_id = "123"
        push_interval_secs = 11
        "#;

        let _: Logs = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "doesn't match messenger implementation")]
    fn wrong_messenger_config_wrong_host() {
        let config = r#"
        messenger.url = "https://api.telegram.org/bot123/sendMessage"
        spreadsheet_id = "123"
        "#;

        let _: Logs = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(
        expected = "either `filter_if_contains` or `drop_if_contains` should be specified but not both"
    )]
    fn filter_and_drop_cannot_coexist() {
        let config = r#"
        spreadsheet_id = "123"
        filter_if_contains = ["pine"]
        drop_if_contains = ["apple", "pine"]
        "#;

        let _: Logs = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "autotruncate_at_usage_percent")]
    fn truncate_percent_cannot_be_less_than_minimum() {
        let config = r#"
        spreadsheet_id = "123"
        autotruncate_at_usage_percent = -10
        "#;

        let _: Logs = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "autotruncate_at_usage_percent")]
    fn truncate_percent_cannot_be_greater_than_maximum() {
        let config = r#"
        spreadsheet_id = "123"
        autotruncate_at_usage_percent = 109
        "#;

        let _: Logs = build_config(config).unwrap();
    }
}
