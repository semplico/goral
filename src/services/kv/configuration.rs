use crate::messenger::configuration::MessengerConfig;
use serde_derive::Deserialize;
use serde_valid::Validate;

const PORT_RANGE_MESSAGE: &str = "consider using a port in the range 49152-65535 to avoid potential conflicts (see https://en.wikipedia.org/wiki/List_of_TCP_and_UDP_port_numbers#Dynamic,_private_or_ephemeral_ports)";

#[inline]
fn port_min_error_message(_params: &serde_valid::MinimumError) -> String {
    PORT_RANGE_MESSAGE.to_string()
}

#[inline]
fn port_max_error_message(_params: &serde_valid::MaximumError) -> String {
    PORT_RANGE_MESSAGE.to_string()
}

fn autotruncate_at_usage_percent() -> f32 {
    100.0
}

#[derive(Debug, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
#[allow(unused)]
pub struct Kv {
    pub spreadsheet_id: String,
    #[validate]
    pub messenger: Option<MessengerConfig>,
    #[validate(minimum = 49152, message_fn = port_min_error_message)]
    #[validate(maximum = 65535, message_fn = port_max_error_message)]
    pub port: u16,
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
        port = 49152
        "#;

        let config: Kv =
            build_config(config).expect("should be able to build minimum configuration");
        assert_eq!(config.spreadsheet_id, "123");
        assert_eq!(config.port, 49152);
        assert!(config.autotruncate_at_usage_percent > 99.0);
    }

    #[test]
    #[should_panic(expected = "port")]
    fn minimal_port() {
        let config = r#"
        spreadsheet_id = "123"
        port = 49151
        "#;

        let _: Kv = build_config(config).expect("should be able to build minimum configuration");
    }

    #[test]
    #[should_panic(expected = "port")]
    fn maximal_port() {
        let config = r#"
        spreadsheet_id = "123"
        port = 65536
        "#;

        let _: Kv = build_config(config).expect("should be able to build minimum configuration");
    }

    #[test]
    #[should_panic(expected = "doesn't match messenger implementation")]
    fn wrong_messenger_config_wrong_host() {
        let config = r#"
        messenger.url = "https://api.telegram.org/bot123/sendMessage"
        spreadsheet_id = "123"
        port = 65500
        "#;

        let _: Kv = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "autotruncate_at_usage_percent")]
    fn truncate_percent_cannot_be_less_than_minimum() {
        let config = r#"
        spreadsheet_id = "123"
        autotruncate_at_usage_percent = -10
        port = 65500
        "#;

        let _: Kv = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "autotruncate_at_usage_percent")]
    fn truncate_percent_cannot_be_greater_than_maximum() {
        let config = r#"
        spreadsheet_id = "123"
        autotruncate_at_usage_percent = 109
        port = 65500
        "#;

        let _: Kv = build_config(config).unwrap();
    }
}
