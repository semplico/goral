use crate::configuration::{
    MAX_GOOGLE_REQUEST_DURATION_SECS, ceiled_division, host_validation, log_name_opt,
    port_validation, push_interval_secs, scrape_interval_secs, scrape_timeout_interval_rule,
};
use crate::messenger::configuration::MessengerConfig;
use serde_derive::Deserialize;
use serde_valid::Validate;
use url::Url;

pub(super) fn scrape_push_rule_validate(
    endpoints: &[Target],
    push_interval_secs: &u16,
    scrape_interval_secs: &u16,
    scrape_timeout_ms: &u32,
) -> Result<(), serde_valid::validation::Error> {
    scrape_push_rule(
        endpoints,
        push_interval_secs,
        scrape_interval_secs,
        scrape_timeout_ms,
    )?;
    Ok(())
}

pub(super) fn scrape_push_rule(
    endpoints: &[Target],
    push_interval_secs: &u16,
    scrape_interval_secs: &u16,
    scrape_timeout_ms: &u32,
) -> Result<usize, serde_valid::validation::Error> {
    if scrape_interval_secs > push_interval_secs {
        return Err(serde_valid::validation::Error::Custom(format!(
            "push interval ({push_interval_secs}) shouldn't be greater than scrape interval {scrape_interval_secs}"
        )));
    }

    if *scrape_timeout_ms > u32::from(*scrape_interval_secs) * 1000 / 2 {
        return Err(serde_valid::validation::Error::Custom(format!(
            "Scrape timeout ({scrape_timeout_ms}ms) shouldn't be greater than half of scrape interval ({scrape_interval_secs}s)"
        )));
    }

    // We can wait for the previous request to finish during MAX_GOOGLE_REQUEST_DURATION_SECS
    let number_of_metrics_endpoints_in_batch = ceiled_division(
        MAX_GOOGLE_REQUEST_DURATION_SECS.max(*push_interval_secs),
        *scrape_interval_secs,
    ) * u16::try_from(endpoints.len())
        .expect("assert: number of endpoints is less than u16::MAX");
    const ENDPOINTS_LIMIT: u16 = 4;
    if number_of_metrics_endpoints_in_batch > ENDPOINTS_LIMIT {
        return Err(serde_valid::validation::Error::Custom(format!(
            "push interval ({push_interval_secs}) is too big (if more than {MAX_GOOGLE_REQUEST_DURATION_SECS}s) or scrape interval ({scrape_interval_secs}) is too small - too much data ({number_of_metrics_endpoints_in_batch} metrics pages vs limit of {ENDPOINTS_LIMIT}) would be accumulated before saving to a spreadsheet"
        )));
    }
    // appending to log is time-consuming
    // during the append we accumulate datarows in the channel
    // Estimate of append duration - 1 sec per row
    const ESTIMATED_ROWS_PER_METRICS_ENDPOINT: usize = 10;
    let append_duration =
        usize::from(number_of_metrics_endpoints_in_batch) * ESTIMATED_ROWS_PER_METRICS_ENDPOINT;
    let number_of_queued_rows = ESTIMATED_ROWS_PER_METRICS_ENDPOINT
        * usize::from(ceiled_division(
            u16::try_from(append_duration).expect("assert: append duration cannot exceed u16::MAX"),
            *scrape_interval_secs,
        ))
        * endpoints.len();
    // we truncate output of probe to 1024 bytes - so estimated payload (without other fields) is around 20 KiB
    const ROWS_LIMIT: usize = 40;
    if number_of_queued_rows > ROWS_LIMIT {
        return Err(serde_valid::validation::Error::Custom(format!(
            "push interval ({push_interval_secs}) is too big (if more than {MAX_GOOGLE_REQUEST_DURATION_SECS}s) or scrape interval ({scrape_interval_secs}) is too small - too much data (estimated {number_of_queued_rows} rows vs limit of {ROWS_LIMIT}) would be accumulated before saving to a spreadsheet"
        )));
    }
    Ok(number_of_queued_rows)
}

pub fn host_port_validation_for_http(url: &Url) -> Result<(), serde_valid::validation::Error> {
    host_validation(url)?;
    port_validation(url)?;
    if url.scheme() != "http" && url.scheme() != "https" {
        return Err(serde_valid::validation::Error::Custom(format!(
            "Scheme for metrics endpoint {url} should be either http or https"
        )));
    }
    Ok(())
}

pub fn target_names(target: &Vec<Target>) -> Result<(), serde_valid::validation::Error> {
    for t in target {
        if target.len() > 1 && t.name.is_none() {
            return Err(serde_valid::validation::Error::Custom(
                "Target should have a name if several targets are specified".to_string(),
            ));
        }
    }
    Ok(())
}

fn scrape_timeout_ms() -> u32 {
    3000
}

#[derive(Debug, Deserialize, PartialEq, Validate)]
#[serde(deny_unknown_fields)]
pub struct Target {
    #[validate(custom(host_port_validation_for_http))]
    pub endpoint: Url,
    #[validate(custom(log_name_opt))]
    #[validate(min_length = 1)]
    pub name: Option<String>,
}

fn autotruncate_at_usage_percent() -> f32 {
    20.0
}

#[derive(Debug, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
#[validate(custom = |s| scrape_timeout_interval_rule(&s.scrape_interval_secs, &s.scrape_timeout_ms))]
#[validate(custom = |s| scrape_push_rule_validate(&s.target, &s.push_interval_secs, &s.scrape_interval_secs, &s.scrape_timeout_ms))]
#[allow(unused)]
pub struct Metrics {
    #[validate]
    pub messenger: Option<MessengerConfig>,
    pub spreadsheet_id: String,
    #[validate(minimum = 10)]
    #[serde(default = "push_interval_secs")]
    pub push_interval_secs: u16,
    #[validate(minimum = 1)]
    #[serde(default = "scrape_interval_secs")]
    pub scrape_interval_secs: u16,
    #[validate(minimum = 1)]
    #[serde(default = "scrape_timeout_ms")]
    pub scrape_timeout_ms: u32,
    #[validate]
    #[validate(custom(target_names))]
    #[validate(min_items = 1)]
    pub target: Vec<Target>,
    #[validate(minimum = 0.0)]
    #[validate(maximum = 100.0)]
    #[serde(default = "autotruncate_at_usage_percent")]
    pub autotruncate_at_usage_percent: f32,
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
        spreadsheet_id = "123"
        [[target]]
        endpoint = "http://127.0.0.1:9898/metrics"
        "#;

        let config: Metrics =
            build_config(config).expect("should be able to build minimum configuration");
        assert_eq!(config.spreadsheet_id, "123");
        assert_eq!(
            config.target[0],
            Target {
                endpoint: Url::from_str("http://127.0.0.1:9898/metrics").unwrap(),
                name: None
            }
        );
        // Defaults
        assert_eq!(config.push_interval_secs, 30);
        assert_eq!(config.scrape_interval_secs, 10);
        assert_eq!(config.scrape_timeout_ms, 3000);
        assert!(
            scrape_push_rule(
                &config.target,
                &config.push_interval_secs,
                &config.scrape_interval_secs,
                &config.scrape_timeout_ms
            )
            .expect("channel capacity should be calculated for defaults")
                > 0
        );
    }

    #[test]
    #[should_panic(expected = "target")]
    fn endpoints_cannot_be_empty() {
        let config = r#"
        spreadsheet_id = "123"
        target = []
        "#;

        let _: Metrics = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "timeout_ms")]
    fn scrape_timeout_cannot_be_zero() {
        let config = r#"
        spreadsheet_id = "123"
        scrape_timeout_ms = 0
        [[target]]
        endpoint = "http://127.0.0.1:9898/metrics"
        "#;

        let _: Metrics = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "scrape_timeout_ms")]
    fn scrape_interval_cannot_be_less_than_timeout() {
        let config = r#"
        spreadsheet_id = "123"
        scrape_interval_secs = 10
        scrape_timeout_ms = 11000
        [[target]]
        endpoint = "http://127.0.0.1:9898/metrics"
        "#;

        let _: Metrics = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "shouldn't be greater than scrape")]
    fn scrape_interval_cannot_be_greater_than_push_interval() {
        let config = r#"
        spreadsheet_id = "123"
        push_interval_secs = 5
        scrape_interval_secs = 10
        [[target]]
        endpoint = "http://127.0.0.1:9898/metrics"
        "#;

        let _: Metrics = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "would be accumulated before saving to a spreadsheet")]
    fn scrape_push_violation() {
        let config = r#"
        spreadsheet_id = "123"
        push_interval_secs = 60
        scrape_interval_secs = 10
        [[target]]
        endpoint = "http://127.0.0.1:9898/metrics"
        "#;

        let _: Metrics = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "scrape_interval_secs")]
    fn scrape_interval_secs_cannot_be_zero() {
        let config = r#"
        spreadsheet_id = "123"
        scrape_interval_secs = 0
        [[target]]
        endpoint = "http://127.0.0.1:9898/metrics"
        "#;

        let _: Metrics = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "port")]
    fn port_is_required() {
        let config = r#"
        spreadsheet_id = "123"
        [[target]]
        endpoint = "http://127.0.0.1/metrics"
        "#;

        let _: Metrics = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "push_interval_secs")]
    fn push_interval_cannot_be_less_than_minimum() {
        let config = r#"
        spreadsheet_id = "123"
        push_interval_secs = 9
        [[target]]
        endpoint = "http://127.0.0.1:9898/metrics"
        "#;

        let _: Metrics = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "doesn't match messenger implementation")]
    fn wrong_messenger_confg_wrong_host() {
        let config = r#"
        messenger.url = "https://api.telegram.org/bot123/sendMessage"
        spreadsheet_id = "123"
        [[target]]
        endpoint = "http://127.0.0.1:9898/metrics"
        "#;

        let _: Metrics = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "Target should have a name if several targets are specified")]
    fn several_targets_require_names() {
        let config = r#"
        spreadsheet_id = "123"
        [[target]]
        endpoint = "http://127.0.0.1:9898/metrics"
        [[target]]
        endpoint = "http://127.0.0.1:9899/metrics"
        "#;

        let _: Metrics = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "The length of the value must be `>= 1`")]
    fn name_cannot_be_empty() {
        let config = r#"
        spreadsheet_id = "123"
        [[target]]
        endpoint = "http://127.0.0.1:9898/metrics"
        name = ""
        "#;

        let _: Metrics = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "name should match a regex")]
    fn invalid_name() {
        let config = r#"
        spreadsheet_id = "123"
        [[target]]
        endpoint = "http://127.0.0.1:9898/metrics"
        name = "john@mail.org"
        "#;

        let _: Metrics = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "autotruncate_at_usage_percent")]
    fn truncate_percent_cannot_be_less_than_minimum() {
        let config = r#"
        spreadsheet_id = "123"
        autotruncate_at_usage_percent = -10
        [[target]]
        endpoint = "http://127.0.0.1:9898/metrics"
        "#;

        let _: Metrics = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "autotruncate_at_usage_percent")]
    fn truncate_percent_cannot_be_greater_than_maximum() {
        let config = r#"
        spreadsheet_id = "123"
        autotruncate_at_usage_percent = 109
        [[target]]
        endpoint = "http://127.0.0.1:9898/metrics"
        "#;

        let _: Metrics = build_config(config).unwrap();
    }
}
