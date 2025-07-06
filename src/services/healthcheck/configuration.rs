use crate::configuration::{
    case_insensitive_enum, ceiled_division, host_validation, log_name_opt, port_validation,
    MAX_GOOGLE_REQUEST_DURATION_SECS,
};
use crate::messenger::configuration::MessengerConfig;

use serde_derive::Deserialize;
use serde_valid::Validate;
use std::fmt::{self, Display};
use std::net::SocketAddr;
use std::str::FromStr;
use url::Url;

fn liveness_rule(
    endpoint: &Option<String>,
    command: &Option<Vec<String>>,
    typ: &LivenessType,
) -> Result<(), serde_valid::validation::Error> {
    match (endpoint, command, typ) {
        (Some(url), None, LivenessType::Http) => {
            let url = Url::from_str(url).map_err(|e| serde_valid::validation::Error::Custom(format!("cannot parse a url from {url}: {e}")))?;
            host_validation(&url)?;
            port_validation(&url)?;
            Ok(())
        },
        (Some(addr), None, LivenessType::Grpc) => {
            if !addr.starts_with("http://") {
                return Err(serde_valid::validation::Error::Custom(format!("Address {addr} for gRPC probe should start with a scheme (only `http://` is supported)")));
            }
            tonic::transport::Endpoint::from_shared(addr.clone().into_bytes()).map_err(|e| serde_valid::validation::Error::Custom(format!("cannot parse a grpc endpoint from {addr}: {e}")))?;
            Ok(())
        },
        (Some(addr), None, LivenessType::Tcp) => {
            SocketAddr::from_str(addr.as_str()).map_err(|e| serde_valid::validation::Error::Custom(format!("cannot parse a socket address from {addr}: {e}")))?;
            Ok(())
        },
        (None, Some(_), LivenessType::Command) => Ok(()),
        _ => Err(serde_valid::validation::Error::Custom(
            "liveness probe type should be either `command` with respective field or `http/tcp/grpc` with `endpoint`".to_string(),
        )),
    }
}

fn timeout_period_rule(
    period_secs: &u16,
    timeout_ms: &u32,
) -> Result<(), serde_valid::validation::Error> {
    let period_ms = u32::from(*period_secs) * 1000;
    if timeout_ms > &period_ms {
        return Err(serde_valid::validation::Error::Custom(
            "liveness `timeout_ms` should be less than `period_secs`".to_string(),
        ));
    }
    Ok(())
}

pub(super) fn scrape_push_rule_validate(
    liveness: &Vec<Liveness>,
    push_interval_secs: &u16,
) -> Result<(), serde_valid::validation::Error> {
    scrape_push_rule(liveness, push_interval_secs)?;
    Ok(())
}

pub(super) fn scrape_push_rule(
    liveness: &Vec<Liveness>,
    push_interval_secs: &u16,
) -> Result<usize, serde_valid::validation::Error> {
    for l in liveness {
        if l.period_secs > *push_interval_secs {
            return Err(serde_valid::validation::Error::Custom(
                format!("push interval ({push_interval_secs}) should be greater or equal than liveness period {}", l.period_secs)
            ));
        }

        if l.period_secs == 0 {
            return Err(serde_valid::validation::Error::Custom(
                "liveness period cannot be zero".to_string(),
            ));
        }
    }

    // We can wait for the previous request to finish during MAX_GOOGLE_REQUEST_DURATION_SECS
    let number_of_rows_in_batch = liveness.iter().fold(0, |acc, l| {
        acc + ceiled_division(
            MAX_GOOGLE_REQUEST_DURATION_SECS.max(*push_interval_secs),
            l.period_secs,
        )
    });

    // we truncate output of probe to 1024 bytes - so estimated payload (without other fields) is around 20 KiB
    const LIMIT: u16 = 15;
    if number_of_rows_in_batch > LIMIT {
        return Err(serde_valid::validation::Error::Custom(
            format!("push interval ({push_interval_secs}) is too big (if more than {MAX_GOOGLE_REQUEST_DURATION_SECS}s) for current choices of liveness periods or liveness periods are too small - too much data ({number_of_rows_in_batch} rows vs limit of {LIMIT}) would be accumulated before saving to a spreadsheet")
        ));
    }
    Ok(number_of_rows_in_batch.into())
}

fn liveness_period_secs() -> u16 {
    5
}

fn liveness_timeout_ms() -> u32 {
    3000
}

fn push_interval_secs() -> u16 {
    20
}

#[derive(Debug, Deserialize, Clone, PartialEq, Validate)]
pub enum LivenessType {
    Http,
    Tcp,
    Command,
    Grpc,
}

impl FromStr for LivenessType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let t = match s {
            _ if s.eq_ignore_ascii_case("http") => Self::Http,
            _ if s.eq_ignore_ascii_case("tcp") => Self::Tcp,
            _ if s.eq_ignore_ascii_case("command") => Self::Command,
            _ if s.eq_ignore_ascii_case("grpc") => Self::Grpc,
            _ => return Err(format!("unsupported liveness type: {s}")),
        };
        Ok(t)
    }
}

impl Display for LivenessType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let t = match self {
            Self::Http => "http",
            Self::Tcp => "tcp",
            Self::Command => "command",
            Self::Grpc => "grpc",
        };
        write!(f, "{t}")
    }
}

fn autotruncate_at_usage_percent() -> f32 {
    10.0
}

#[derive(Debug, Deserialize, Validate)]
#[validate(custom = |s| liveness_rule(&s.endpoint, &s.command, &s.typ))]
#[validate(custom = |s| timeout_period_rule(&s.period_secs, &s.timeout_ms))]
#[serde(deny_unknown_fields)]
pub struct Liveness {
    #[validate(custom(log_name_opt))]
    #[validate(min_length = 1)]
    pub name: Option<String>,
    #[serde(default)]
    pub initial_delay_secs: u16,
    #[validate(minimum = 1)]
    #[serde(default = "liveness_period_secs")]
    pub period_secs: u16,
    #[validate(minimum = 1)]
    #[serde(default = "liveness_timeout_ms")]
    pub timeout_ms: u32,
    pub endpoint: Option<String>,
    #[validate(min_items = 1)]
    pub command: Option<Vec<String>>,
    #[serde(rename(deserialize = "type"))]
    #[serde(deserialize_with = "case_insensitive_enum")]
    pub typ: LivenessType,
}

#[derive(Debug, Deserialize, Validate)]
#[validate(custom = |s| scrape_push_rule_validate(&s.liveness, &s.push_interval_secs))]
#[serde(deny_unknown_fields)]
pub struct Healthcheck {
    #[validate]
    pub messenger: Option<MessengerConfig>,
    pub spreadsheet_id: String,
    #[validate]
    #[validate(min_items = 1)]
    pub liveness: Vec<Liveness>,
    #[validate(minimum = 10)]
    #[serde(default = "push_interval_secs")]
    pub push_interval_secs: u16,
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
    fn minimal_confg() {
        let config = r#"
        spreadsheet_id = "123"
        [[liveness]]
        type = "Http"
        endpoint = "http://127.0.0.1:9898"
        [[liveness]]
        type = "Command"
        command = ["ls", "-lha"]
        "#;

        let config: Healthcheck =
            build_config(config).expect("should be able to build minimum configuration");
        assert_eq!(config.spreadsheet_id, "123");
        assert_eq!(config.liveness[0].typ, LivenessType::Http);
        assert_eq!(
            config.liveness[0].endpoint,
            Some("http://127.0.0.1:9898".to_string())
        );
        assert_eq!(config.liveness[0].command, None);
        assert_eq!(config.liveness[1].typ, LivenessType::Command);
        assert_eq!(
            config.liveness[1].command,
            Some(vec!["ls".to_string(), "-lha".to_string()])
        );
        assert_eq!(config.liveness[1].endpoint, None);
        // Defaults
        assert_eq!(config.push_interval_secs, 20);
        assert_eq!(config.liveness[0].timeout_ms, 3000);
        assert_eq!(config.liveness[0].initial_delay_secs, 0);
        assert_eq!(config.liveness[0].period_secs, 5);
        assert_eq!(config.liveness[1].timeout_ms, 3000);
        assert_eq!(config.liveness[1].initial_delay_secs, 0);
        assert_eq!(config.liveness[1].period_secs, 5);

        assert!(
            scrape_push_rule(&config.liveness, &config.push_interval_secs)
                .expect("channel capacity should be calculated for defaults")
                > 0,
        );
    }

    #[test]
    fn case_insensitive_liveness_typ() {
        let config = r#"
        spreadsheet_id = "123"
        [[liveness]]
        type = "hTtp"
        endpoint = "http://127.0.0.1:9898"
        "#;

        let config: Healthcheck =
            build_config(config).expect("should be able to build minimum configuration");
        assert_eq!(config.liveness[0].typ, LivenessType::Http);
    }

    #[test]
    #[should_panic(expected = "liveness")]
    fn liveness_cannot_be_empty() {
        let config = r#"
        spreadsheet_id = "123"
        liveness = []
        "#;

        let _: Healthcheck = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "timeout_ms")]
    fn probe_timeout_cannot_be_zero() {
        let config = r#"
        spreadsheet_id = "123"
        [[liveness]]
        type = "Http"
        endpoint = "http://127.0.0.1:9898"
        timeout_ms = 0
        [[liveness]]
        type = "Command"
        command = ["ls", "-lha"]
        "#;

        let _: Healthcheck = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "period_secs")]
    fn period_cannot_be_less_than_timeout() {
        let config = r#"
        spreadsheet_id = "123"
        [[liveness]]
        type = "Http"
        endpoint = "http://127.0.0.1:9898"
        period_secs = 2
        timeout_ms = 3000
        "#;

        let _: Healthcheck = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "should be greater or equal than liveness period")]
    fn period_cannot_be_greater_than_push_interval() {
        let config = r#"
        spreadsheet_id = "123"
        push_interval_secs = 5
        [[liveness]]
        type = "Http"
        endpoint = "http://127.0.0.1:9898"
        period_secs = 10
        timeout_ms = 3000
        "#;

        let _: Healthcheck = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "would be accumulated before saving to a spreadsheet")]
    fn scrape_push_violation() {
        let config = r#"
        spreadsheet_id = "123"
        push_interval_secs = 201
        [[liveness]]
        type = "Http"
        endpoint = "http://127.0.0.1:9898"
        period_secs = 10
        "#;

        let _: Healthcheck = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "cannot be zero")]
    fn period_cannot_be_zero() {
        let config = r#"
        spreadsheet_id = "123"
        [[liveness]]
        type = "Http"
        endpoint = "http://127.0.0.1:9898"
        period_secs = 0
        [[liveness]]
        type = "Command"
        command = ["ls", "-lha"]
        "#;

        let _: Healthcheck = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(
        expected = "liveness probe type should be either `command` with respective field or `http/tcp/grpc` with `endpoint`"
    )]
    fn http_should_have_endpoint() {
        let config = r#"
        spreadsheet_id = "123"
        [[liveness]]
        type = "Http"
        command = ["ls", "-lha"]
        [[liveness]]
        type = "Command"
        command = ["ls", "-lha"]
        "#;

        let _: Healthcheck = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "port")]
    fn port_is_required() {
        let config = r#"
        spreadsheet_id = "123"
        [[liveness]]
        type = "Http"
        endpoint = "http://localhost"
        "#;

        let _: Healthcheck = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "push_interval_secs")]
    fn push_interval_cannot_be_less_than_minimum() {
        let config = r#"
        spreadsheet_id = "123"
        push_interval_secs = 9
        [[liveness]]
        type = "Http"
        endpoint = "http://127.0.0.1:9898"
        "#;

        let _: Healthcheck = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "doesn't match messenger implementation")]
    fn wrong_messenger_config_wrong_host() {
        let config = r#"
        messenger.url = "https://api.telegram.org/bot123/sendMessage"
        spreadsheet_id = "123"
        [[liveness]]
        type = "Http"
        endpoint = "http://127.0.0.1:9898"
        "#;

        let _: Healthcheck = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "The length of the items must be `>= 1`")]
    fn command_cannot_be_empty() {
        let config = r#"
        spreadsheet_id = "123"
        [[liveness]]
        type = "Command"
        command = []
        "#;

        let _: Healthcheck = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "The length of the value must be `>= 1`")]
    fn liveness_name_cannot_be_empty_if_specified() {
        let config = r#"
        spreadsheet_id = "123"
        [[liveness]]
        name = ""
        type = "Command"
        command = ["ls"]
        "#;

        let _: Healthcheck = build_config(config).unwrap();
    }

    #[test]
    fn tcp_socket_probe_ipv4() {
        let config = r#"
        spreadsheet_id = "123"
        [[liveness]]
        type = "Tcp"
        endpoint = "127.0.0.1:9898"
        "#;

        let _: Healthcheck = build_config(config).unwrap();
    }

    #[test]
    fn tcp_socket_probe_ipv6() {
        let config = r#"
        spreadsheet_id = "123"
        [[liveness]]
        type = "Tcp"
        endpoint = "[::1]:9898"
        "#;

        let _: Healthcheck = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "for gRPC probe should start with a scheme")]
    fn grpc_probe_requires_scheme() {
        let config = r#"
        spreadsheet_id = "123"
        [[liveness]]
        type = "Grpc"
        endpoint = "[::1]:50051"
        "#;

        let _: Healthcheck = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "only `http://` is supported")]
    fn grpc_probe_https_scheme() {
        let config = r#"
        spreadsheet_id = "123"
        [[liveness]]
        type = "Grpc"
        endpoint = "https://[::1]:50051"
        "#;

        let _: Healthcheck = build_config(config).unwrap();
    }

    #[test]
    fn grpc_probe() {
        let config = r#"
        spreadsheet_id = "123"
        [[liveness]]
        type = "Grpc"
        endpoint = "http://[::1]:50051"
        "#;

        let _: Healthcheck = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "name should match a regex")]
    fn invalid_name() {
        let config = r#"
        spreadsheet_id = "123"
        [[liveness]]
        type = "Grpc"
        endpoint = "http://[::1]:50051"
        name = "john@mail.org"
        "#;

        let _: Healthcheck = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "autotruncate_at_usage_percent")]
    fn truncate_percent_cannot_be_less_than_minimum() {
        let config = r#"
        spreadsheet_id = "123"
        autotruncate_at_usage_percent = -10
        [[liveness]]
        type = "Http"
        endpoint = "http://127.0.0.1:9898"
        "#;

        let _: Healthcheck = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "autotruncate_at_usage_percent")]
    fn truncate_percent_cannot_be_greater_than_maximum() {
        let config = r#"
        spreadsheet_id = "123"
        autotruncate_at_usage_percent = 109
        [[liveness]]
        type = "Http"
        endpoint = "http://127.0.0.1:9898"
        "#;

        let _: Healthcheck = build_config(config).unwrap();
    }
}
