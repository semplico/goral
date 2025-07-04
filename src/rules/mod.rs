use crate::google::datavalue::{Datarow, Datavalue, NOT_AVAILABLE};
use crate::google::{sheet::Dropdown, TableId};
use crate::notifications::{Notification, Sender};
use chrono::Utc;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;
use tracing::Level;

pub const RULES_LOG_NAME: &str = "rules";

pub const RULE_LOG_NAME_COLUMN_HEADER: &str = "log_name";
pub const RULE_KEY_COLUMN_HEADER: &str = "key";
pub const RULE_CONDITION_COLUMN_HEADER: &str = "condition";
pub const RULE_VALUE_COLUMN_HEADER: &str = "value";
pub const RULE_ACTION_COLUMN_HEADER: &str = "action";

pub const RULES_KEYS: [&str; 5] = [
    RULE_LOG_NAME_COLUMN_HEADER,
    RULE_KEY_COLUMN_HEADER,
    RULE_CONDITION_COLUMN_HEADER,
    RULE_VALUE_COLUMN_HEADER,
    RULE_ACTION_COLUMN_HEADER,
];
pub const LESS_CONDITION: &str = "<";
pub const GREATER_CONDITION: &str = ">";
pub const IS_CONDITION: &str = "is";
pub const IS_NOT_CONDITION: &str = "is not";
pub const CONTAINS_CONDITION: &str = "contains";
pub const NOT_CONTAINS_CONDITION: &str = "not contains";
pub const CONDITIONS: [&str; 6] = [
    LESS_CONDITION,
    GREATER_CONDITION,
    IS_CONDITION,
    IS_NOT_CONDITION,
    CONTAINS_CONDITION,
    NOT_CONTAINS_CONDITION,
];

pub const INFO_ACTION: &str = "info";
pub const WARN_ACTION: &str = "warn";
pub const ERROR_ACTION: &str = "error";
pub const SKIP_ACTION: &str = "skip further rules";
pub const ACTIONS: [&str; 4] = [INFO_ACTION, WARN_ACTION, ERROR_ACTION, SKIP_ACTION];

pub fn rules_dropdowns() -> Vec<Dropdown> {
    vec![
        Dropdown {
            column_index: 3,
            values: CONDITIONS.into_iter().map(|c| c.to_string()).collect(),
        },
        Dropdown {
            column_index: 5,
            values: ACTIONS.into_iter().map(|c| c.to_string()).collect(),
        },
    ]
}

#[derive(Debug)]
pub enum RuleCondition {
    Less,
    Greater,
    Is,
    IsNot,
    Contains,
    NotContains,
}

impl fmt::Display for RuleCondition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use RuleCondition::*;
        match self {
            Less => write!(f, "{LESS_CONDITION}"),
            Greater => write!(f, "{GREATER_CONDITION}"),
            Is => write!(f, "{IS_CONDITION}"),
            IsNot => write!(f, "{IS_NOT_CONDITION}"),
            Contains => write!(f, "{CONTAINS_CONDITION}"),
            NotContains => write!(f, "{NOT_CONTAINS_CONDITION}"),
        }
    }
}

impl<'a> TryFrom<&'a str> for RuleCondition {
    type Error = &'static str;

    fn try_from(condition: &'a str) -> Result<Self, &'static str> {
        match condition {
            LESS_CONDITION => Ok(Self::Less),
            GREATER_CONDITION => Ok(Self::Greater),
            IS_CONDITION => Ok(Self::Is),
            IS_NOT_CONDITION => Ok(Self::IsNot),
            CONTAINS_CONDITION => Ok(Self::Contains),
            NOT_CONTAINS_CONDITION => Ok(Self::NotContains),
            _ => Err("invalid condition"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    Info,
    Warn,
    Error,
    SkipFurtherRules,
}

impl fmt::Display for Action {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use Action::*;
        match self {
            Info => write!(f, "{INFO_ACTION}"),
            Warn => write!(f, "{WARN_ACTION}"),
            Error => write!(f, "{ERROR_ACTION}"),
            SkipFurtherRules => write!(f, "{SKIP_ACTION}"),
        }
    }
}

impl<'a> TryFrom<&'a str> for Action {
    type Error = &'static str;

    fn try_from(action: &'a str) -> Result<Self, &'static str> {
        match action {
            INFO_ACTION => Ok(Self::Info),
            WARN_ACTION => Ok(Self::Warn),
            ERROR_ACTION => Ok(Self::Error),
            SKIP_ACTION => Ok(Self::SkipFurtherRules),
            _ => Err("invalid action"),
        }
    }
}

#[derive(Debug)]
pub struct Rule {
    pub log_name: String,
    pub key: String,
    pub condition: RuleCondition,
    pub value: Datavalue,
    pub action: Action,
}

impl fmt::Display for Rule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use Datavalue::*;
        match &self.value {
            Number(v) => write!(
                f,
                "Rule `if {}:{} value {} {} then {}`",
                self.log_name, self.key, self.condition, v, self.action
            ),
            Integer(v) => write!(
                f,
                "Rule `if {}:{} value {} {} then {}`",
                self.log_name, self.key, self.condition, v, self.action
            ),
            Text(v) => write!(
                f,
                "Rule `if {}:{} value {} {} then {}`",
                self.log_name, self.key, self.condition, v, self.action
            ),
            Bool(v) => write!(
                f,
                "Rule `if {}:{} value {} {} then {}`",
                self.log_name, self.key, self.condition, v, self.action
            ),
            NotAvailable => write!(
                f,
                "Rule `if {}:{} value {} {} then {}`",
                self.log_name, self.key, self.condition, NOT_AVAILABLE, self.action
            ),
            _ => panic!("assert: rule can be formatted for supported datavalues"),
        }
    }
}

impl Rule {
    pub fn try_from_values(row: Vec<Value>, messenger: Option<&Sender>) -> Option<Self> {
        if row.len() != 6 {
            // Rule fields + timestamp which is first
            return None;
        }
        let log_name = row[1].as_str()?.trim().to_string();
        let key = row[2].as_str()?.trim().to_string();
        let condition = RuleCondition::try_from(row[3].as_str()?).ok()?;
        let value = &row[4];
        let action = Action::try_from(row[5].as_str()?).ok()?;

        let value = match condition {
            RuleCondition::Is if value.is_boolean() => Datavalue::Bool(value.as_bool()?),
            RuleCondition::Is if value.is_u64() => {
                Datavalue::Integer(u32::try_from(value.as_u64()?).ok()?)
            }
            RuleCondition::IsNot if value.is_boolean() => Datavalue::Bool(value.as_bool()?),
            RuleCondition::IsNot if value.is_u64() => {
                Datavalue::Integer(u32::try_from(value.as_u64()?).ok()?)
            }
            RuleCondition::Less if value.is_number() => Datavalue::Number(value.as_f64()?),
            RuleCondition::Greater if value.is_number() => Datavalue::Number(value.as_f64()?),
            RuleCondition::Is if value.is_string() && value.as_str()? == NOT_AVAILABLE => {
                Datavalue::NotAvailable
            }
            RuleCondition::IsNot if value.is_string() && value.as_str()? == NOT_AVAILABLE => {
                Datavalue::NotAvailable
            }
            RuleCondition::Is if value.is_string() && value.as_str()? != NOT_AVAILABLE => {
                Datavalue::Text(value.as_str()?.to_string())
            }
            RuleCondition::IsNot if value.is_string() && value.as_str()? != NOT_AVAILABLE => {
                Datavalue::Text(value.as_str()?.to_string())
            }
            RuleCondition::Contains if value.is_string() && value.as_str()? != NOT_AVAILABLE => {
                Datavalue::Text(value.as_str()?.to_string())
            }
            RuleCondition::NotContains if value.is_string() && value.as_str()? != NOT_AVAILABLE => {
                Datavalue::Text(value.as_str()?.to_string())
            }
            RuleCondition::Is | RuleCondition::IsNot if value.is_number() => {
                let message = format!("`{IS_CONDITION}` and `{IS_NOT_CONDITION}` conditions are not valid when the `{RULE_VALUE_COLUMN_HEADER}` is a float, the rule for `{log_name}->{key}` with `{RULE_VALUE_COLUMN_HEADER}` `{}` is skipped", value.as_f64()?);
                tracing::warn!("{}", message);
                if let Some(messenger) = messenger {
                    messenger.send_nonblock(Notification::new(message, Level::WARN));
                }
                return None;
            }
            _ => {
                return None;
            }
        };

        Some(Self {
            log_name,
            key,
            condition,
            value,
            action,
        })
    }

    pub fn apply(&self, candidate: &RuleApplicant) -> RuleOutput {
        use RuleCondition::*;

        if candidate.log_name != self.log_name {
            return RuleOutput::Process(None);
        }
        let candidate_value = match candidate.data.get(&self.key) {
            Some(cv) => cv,
            None => {
                return RuleOutput::Process(None);
            }
        };

        let message = match (candidate_value, &self.condition, &self.value) {
            (Datavalue::Number(c), Less, Datavalue::Number(v)) if c < v => {
                format!("{self} triggered for value `{c:.4}`")
            }
            (Datavalue::Number(c), Less, Datavalue::Integer(v)) if c < &(f64::from(*v)) => {
                format!("{self} triggered for value `{c:.4}`")
            }
            (Datavalue::Integer(c), Less, Datavalue::Number(v)) if &(f64::from(*c)) < v => {
                format!("{self} triggered for value `{c}`")
            }
            (Datavalue::Number(c), Greater, Datavalue::Number(v)) if c > v => {
                format!("{self} triggered for value `{c:.4}`")
            }
            (Datavalue::Number(c), Greater, Datavalue::Integer(v)) if c > &(f64::from(*v)) => {
                format!("{self} triggered for value `{c:.4}`")
            }
            (Datavalue::Integer(c), Greater, Datavalue::Number(v)) if &(f64::from(*c)) > v => {
                format!("{self} triggered for value `{c}`")
            }
            (Datavalue::Integer(c), Is, Datavalue::Integer(v)) if c == v => {
                format!("{self} triggered for value `{c}`")
            }
            (Datavalue::Integer(c), IsNot, Datavalue::Integer(v)) if c != v => {
                format!("{self} triggered for value `{c}`")
            }
            (Datavalue::Bool(c), Is, Datavalue::Bool(v)) if c == v => format!("{self} triggered"),
            (Datavalue::Bool(c), IsNot, Datavalue::Bool(v)) if c != v => {
                format!("{self} triggered")
            }
            (Datavalue::NotAvailable, Is, Datavalue::NotAvailable) => format!("{self} triggered"),
            (Datavalue::Number(c), IsNot, Datavalue::NotAvailable) => {
                format!("{self} triggered for value `{c:.4}`")
            }
            (Datavalue::Integer(c), IsNot, Datavalue::NotAvailable) => {
                format!("{self} triggered for value `{c}`")
            }
            (Datavalue::Text(c), IsNot, Datavalue::NotAvailable) => {
                format!("{self} triggered for value `{c}`")
            }
            (Datavalue::Bool(c), IsNot, Datavalue::NotAvailable) => {
                format!("{self} triggered for value `{c}`")
            }
            (Datavalue::Text(c), Is, Datavalue::Text(v)) if c == v => {
                format!("{self} triggered for value `{c}`")
            }
            (Datavalue::Text(c), IsNot, Datavalue::Text(v)) if c != v => {
                format!("{self} triggered for value `{c}`")
            }
            (Datavalue::Text(c), Contains, Datavalue::Text(v)) if c.contains(v) => {
                format!("{self} triggered for value `{c}`")
            }
            (Datavalue::Text(c), NotContains, Datavalue::Text(v)) if !c.contains(v) => {
                format!("{self} triggered for value `{c}`")
            }
            _ => {
                return RuleOutput::Process(None);
            }
        };

        let triggered = Triggered {
            message,
            action: self.action.clone(),
            sheet_id: candidate.sheet_id,
            row: candidate.row,
        };
        // rule triggered
        if self.action == Action::SkipFurtherRules {
            RuleOutput::SkipFurtherRules
        } else {
            RuleOutput::Process(Some(triggered))
        }
    }
}

// to add some example rules
impl From<Rule> for Datarow {
    fn from(val: Rule) -> Self {
        let Rule {
            log_name,
            key,
            condition,
            value,
            action,
        } = val;
        Datarow::new(
            RULES_LOG_NAME.to_string(),
            Utc::now().naive_utc(),
            vec![
                (
                    RULE_LOG_NAME_COLUMN_HEADER.to_string(),
                    Datavalue::Text(log_name),
                ),
                (RULE_KEY_COLUMN_HEADER.to_string(), Datavalue::Text(key)),
                (
                    RULE_CONDITION_COLUMN_HEADER.to_string(),
                    Datavalue::Text(condition.to_string()),
                ),
                (RULE_VALUE_COLUMN_HEADER.to_string(), value),
                (
                    RULE_ACTION_COLUMN_HEADER.to_string(),
                    Datavalue::Text(action.to_string()),
                ),
            ],
        )
    }
}

#[derive(Debug)]
pub struct RuleApplicant {
    pub log_name: String,
    pub data: HashMap<String, Datavalue>,
    pub sheet_id: TableId,
    pub row: u32,
}

#[derive(Debug)]
pub enum RuleOutput {
    Process(Option<Triggered>),
    SkipFurtherRules,
}

#[derive(Debug)]
pub struct Triggered {
    pub message: String,
    pub action: Action,
    pub sheet_id: TableId,
    pub row: u32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::TEST_HOST_ID;
    use serde_json::json;

    #[test]
    fn rules_deserialization() {
        let rule = Rule::try_from_values(
            vec![
                json!(0.0),
                json!("log_name1"),
                json!("key"),
                json!(LESS_CONDITION),
                json!("log_name1"),
                json!(INFO_ACTION),
            ],
            None,
        );
        assert!(
            rule.is_none(),
            "test assert: `less` cannot be a condition for a text value"
        );

        let rule = Rule::try_from_values(
            vec![
                json!(0.0),
                json!("log_name1"),
                json!("key"),
                json!(GREATER_CONDITION),
                json!("log_name1"),
                json!(INFO_ACTION),
            ],
            None,
        );
        assert!(
            rule.is_none(),
            "test assert: `greater` cannot be a condition for a text value"
        );

        let rule = Rule::try_from_values(
            vec![
                json!(0.0),
                json!("log_name1"),
                json!("key"),
                json!(LESS_CONDITION),
                json!(NOT_AVAILABLE),
                json!(INFO_ACTION),
            ],
            None,
        );
        assert!(
            rule.is_none(),
            "test assert: `less` cannot be a condition for a N/A value"
        );

        let rule = Rule::try_from_values(
            vec![
                json!(0.0),
                json!("log_name1"),
                json!("key"),
                json!(GREATER_CONDITION),
                json!(NOT_AVAILABLE),
                json!(INFO_ACTION),
            ],
            None,
        );
        assert!(
            rule.is_none(),
            "test assert: `greater` cannot be a condition for a N/A value"
        );

        let rule = Rule::try_from_values(
            vec![
                json!(0.0),
                json!("log_name1"),
                json!("key"),
                json!(LESS_CONDITION),
                json!(true),
                json!(INFO_ACTION),
            ],
            None,
        );
        assert!(
            rule.is_none(),
            "test assert: `less` cannot be a condition for a bool value"
        );

        let rule = Rule::try_from_values(
            vec![
                json!(0.0),
                json!("log_name1"),
                json!("key"),
                json!(GREATER_CONDITION),
                json!(true),
                json!(INFO_ACTION),
            ],
            None,
        );
        assert!(
            rule.is_none(),
            "test assert: `greater` cannot be a condition for a bool value"
        );

        let rule = Rule::try_from_values(
            vec![
                json!(0.0),
                json!("log_name1"),
                json!("key"),
                json!(IS_CONDITION),
                json!("log_name1"),
                json!(INFO_ACTION),
            ],
            None,
        );
        assert!(
            rule.is_some(),
            "test assert: `is` can be a condition for a text value"
        );

        let rule = Rule::try_from_values(
            vec![
                json!(0.0),
                json!("log_name1"),
                json!("key"),
                json!(IS_CONDITION),
                json!(1.0),
                json!(INFO_ACTION),
            ],
            None,
        );
        assert!(
            rule.is_none(),
            "test assert: `is` cannot be a condition for a float value"
        );

        let rule = Rule::try_from_values(
            vec![
                json!(0.0),
                json!("log_name1"),
                json!("key"),
                json!(IS_NOT_CONDITION),
                json!("log_name1"),
                json!(INFO_ACTION),
            ],
            None,
        );
        assert!(
            rule.is_some(),
            "test assert: `is not` can be a condition for a text value"
        );

        let rule = Rule::try_from_values(
            vec![
                json!(0.0),
                json!("log_name1"),
                json!("key"),
                json!(IS_NOT_CONDITION),
                json!(1.0),
                json!(INFO_ACTION),
            ],
            None,
        );
        assert!(
            rule.is_none(),
            "test assert: `is not` cannot be a condition for a float value"
        );

        let rule = Rule::try_from_values(
            vec![
                json!(0.0),
                json!("log_name1"),
                json!("key"),
                json!(CONTAINS_CONDITION),
                json!(1.0),
                json!(INFO_ACTION),
            ],
            None,
        );
        assert!(
            rule.is_none(),
            "test assert: `contains` cannot be a condition for a float value"
        );

        let rule = Rule::try_from_values(
            vec![
                json!(0.0),
                json!("log_name1"),
                json!("key"),
                json!(CONTAINS_CONDITION),
                json!(1),
                json!(INFO_ACTION),
            ],
            None,
        );
        assert!(
            rule.is_none(),
            "test assert: `contains` cannot be a condition for a integer value"
        );

        let rule = Rule::try_from_values(
            vec![
                json!(0.0),
                json!("log_name1"),
                json!("key"),
                json!(CONTAINS_CONDITION),
                json!(true),
                json!(INFO_ACTION),
            ],
            None,
        );
        assert!(
            rule.is_none(),
            "test assert: `contains` cannot be a condition for a bool value"
        );

        let rule = Rule::try_from_values(
            vec![
                json!(0.0),
                json!("log_name1"),
                json!("key"),
                json!(CONTAINS_CONDITION),
                json!(NOT_AVAILABLE),
                json!(INFO_ACTION),
            ],
            None,
        );
        assert!(
            rule.is_none(),
            "test assert: `contains` cannot be a condition for a N/A value"
        );

        let rule = Rule::try_from_values(
            vec![
                json!(0.0),
                json!("log_name1"),
                json!("key"),
                json!(IS_CONDITION),
                json!(-2),
                json!(INFO_ACTION),
            ],
            None,
        );
        assert!(
            rule.is_none(),
            "test assert: negative integers are not supported"
        );
    }

    #[test]
    fn rules_application() {
        let rule = Rule::try_from_values(
            vec![
                json!(0.0),
                json!("log_name1"),
                json!("key"),
                json!(IS_NOT_CONDITION),
                json!(NOT_AVAILABLE),
                json!(INFO_ACTION),
            ],
            None,
        );
        let mut datarow = Datarow::new(
            "log_name1".to_string(),
            Utc::now().naive_utc(),
            vec![("key".to_string(), Datavalue::Text("some text".to_string()))],
        );
        datarow.calculate_sheet_id(TEST_HOST_ID, "test");
        datarow.set_row(1);
        match rule.unwrap().apply(&datarow.into()) {
            RuleOutput::Process(Some(Triggered { action, .. })) => {
                assert_eq!(action, Action::Info)
            }
            _ => panic!("test assert: rule should trigger"),
        };

        let rule = Rule::try_from_values(
            vec![
                json!(0.0),
                json!("log_name1"),
                json!("key"),
                json!(LESS_CONDITION),
                json!(-9),
                json!(INFO_ACTION),
            ],
            None,
        );
        let mut datarow = Datarow::new(
            "log_name1".to_string(),
            Utc::now().naive_utc(),
            vec![("key".to_string(), Datavalue::Number(-10.0))],
        );
        datarow.calculate_sheet_id(TEST_HOST_ID, "test");
        datarow.set_row(1);
        match rule.unwrap().apply(&datarow.into()) {
            RuleOutput::Process(Some(Triggered { action, .. })) => {
                assert_eq!(action, Action::Info)
            }
            _ => panic!("test assert: rule should trigger"),
        };

        let rule = Rule::try_from_values(
            vec![
                json!(0.0),
                json!("log_name1"),
                json!("key"),
                json!(LESS_CONDITION),
                json!(9.0),
                json!(INFO_ACTION),
            ],
            None,
        );
        let mut datarow = Datarow::new(
            "log_name1".to_string(),
            Utc::now().naive_utc(),
            vec![("key".to_string(), Datavalue::Number(8.0))],
        );
        datarow.calculate_sheet_id(TEST_HOST_ID, "test");
        datarow.set_row(1);
        match rule.unwrap().apply(&datarow.into()) {
            RuleOutput::Process(Some(Triggered { action, .. })) => {
                assert_eq!(action, Action::Info)
            }
            _ => panic!("test assert: rule should trigger"),
        };

        let rule = Rule::try_from_values(
            vec![
                json!(0.0),
                json!("log_name1"),
                json!("key"),
                json!(GREATER_CONDITION),
                json!(9.0),
                json!(INFO_ACTION),
            ],
            None,
        );
        let mut datarow = Datarow::new(
            "log_name1".to_string(),
            Utc::now().naive_utc(),
            vec![("key".to_string(), Datavalue::Number(10.0))],
        );
        datarow.calculate_sheet_id(TEST_HOST_ID, "test");
        datarow.set_row(1);
        match rule.unwrap().apply(&datarow.into()) {
            RuleOutput::Process(Some(Triggered { action, .. })) => {
                assert_eq!(action, Action::Info)
            }
            _ => panic!("test assert: rule should trigger"),
        };

        let rule = Rule::try_from_values(
            vec![
                json!(0.0),
                json!("log_name1"),
                json!("key"),
                json!(LESS_CONDITION),
                json!(9),
                json!(INFO_ACTION),
            ],
            None,
        );
        let mut datarow = Datarow::new(
            "log_name1".to_string(),
            Utc::now().naive_utc(),
            vec![("key".to_string(), Datavalue::Number(8.0))],
        );
        datarow.calculate_sheet_id(TEST_HOST_ID, "test");
        datarow.set_row(1);
        match rule.unwrap().apply(&datarow.into()) {
            RuleOutput::Process(Some(Triggered { action, .. })) => {
                assert_eq!(action, Action::Info)
            }
            _ => panic!("test assert: rule should trigger"),
        };

        let rule = Rule::try_from_values(
            vec![
                json!(0.0),
                json!("log_name1"),
                json!("key"),
                json!(GREATER_CONDITION),
                json!(9),
                json!(INFO_ACTION),
            ],
            None,
        );
        let mut datarow = Datarow::new(
            "log_name1".to_string(),
            Utc::now().naive_utc(),
            vec![("key".to_string(), Datavalue::Number(10.0))],
        );
        datarow.calculate_sheet_id(TEST_HOST_ID, "test");
        datarow.set_row(1);
        match rule.unwrap().apply(&datarow.into()) {
            RuleOutput::Process(Some(Triggered { action, .. })) => {
                assert_eq!(action, Action::Info)
            }
            _ => panic!("test assert: rule should trigger"),
        };

        let rule = Rule::try_from_values(
            vec![
                json!(0.0),
                json!("log_name1"),
                json!("key"),
                json!(LESS_CONDITION),
                json!(9.0),
                json!(INFO_ACTION),
            ],
            None,
        );
        let mut datarow = Datarow::new(
            "log_name1".to_string(),
            Utc::now().naive_utc(),
            vec![("key".to_string(), Datavalue::Size(8))],
        );
        datarow.calculate_sheet_id(TEST_HOST_ID, "test");
        datarow.set_row(1);
        match rule.unwrap().apply(&datarow.into()) {
            RuleOutput::Process(Some(Triggered { action, .. })) => {
                assert_eq!(action, Action::Info)
            }
            _ => panic!("test assert: rule should trigger"),
        };

        let rule = Rule::try_from_values(
            vec![
                json!(0.0),
                json!("log_name1"),
                json!("key"),
                json!(GREATER_CONDITION),
                json!(9.0),
                json!(INFO_ACTION),
            ],
            None,
        );
        let mut datarow = Datarow::new(
            "log_name1".to_string(),
            Utc::now().naive_utc(),
            vec![("key".to_string(), Datavalue::Size(10))],
        );
        datarow.calculate_sheet_id(TEST_HOST_ID, "test");
        datarow.set_row(1);
        match rule.unwrap().apply(&datarow.into()) {
            RuleOutput::Process(Some(Triggered { action, .. })) => {
                assert_eq!(action, Action::Info)
            }
            _ => panic!("test assert: rule should trigger"),
        };

        let rule = Rule::try_from_values(
            vec![
                json!(0.0),
                json!("log_name1"),
                json!("key"),
                json!(LESS_CONDITION),
                json!(0.9),
                json!(INFO_ACTION),
            ],
            None,
        );
        let mut datarow = Datarow::new(
            "log_name1".to_string(),
            Utc::now().naive_utc(),
            vec![("key".to_string(), Datavalue::Percent(80.0))],
        );
        datarow.calculate_sheet_id(TEST_HOST_ID, "test");
        datarow.set_row(1);
        match rule.unwrap().apply(&datarow.into()) {
            RuleOutput::Process(Some(Triggered { action, .. })) => {
                assert_eq!(action, Action::Info)
            }
            _ => panic!("test assert: rule should trigger"),
        };

        let rule = Rule::try_from_values(
            vec![
                json!(0.0),
                json!("log_name1"),
                json!("key"),
                json!(GREATER_CONDITION),
                json!(0.9),
                json!(INFO_ACTION),
            ],
            None,
        );
        let mut datarow = Datarow::new(
            "log_name1".to_string(),
            Utc::now().naive_utc(),
            vec![("key".to_string(), Datavalue::Percent(100.0))],
        );
        datarow.calculate_sheet_id(TEST_HOST_ID, "test");
        datarow.set_row(1);
        match rule.unwrap().apply(&datarow.into()) {
            RuleOutput::Process(Some(Triggered { action, .. })) => {
                assert_eq!(action, Action::Info)
            }
            _ => panic!("test assert: rule should trigger"),
        };

        let rule = Rule::try_from_values(
            vec![
                json!(0.0),
                json!("log_name1"),
                json!("key"),
                json!(IS_CONDITION),
                json!(true),
                json!(INFO_ACTION),
            ],
            None,
        );
        let mut datarow = Datarow::new(
            "log_name1".to_string(),
            Utc::now().naive_utc(),
            vec![("key".to_string(), Datavalue::Bool(true))],
        );
        datarow.calculate_sheet_id(TEST_HOST_ID, "test");
        datarow.set_row(1);
        match rule.unwrap().apply(&datarow.into()) {
            RuleOutput::Process(Some(Triggered { action, .. })) => {
                assert_eq!(action, Action::Info)
            }
            _ => panic!("test assert: rule should trigger"),
        };

        let rule = Rule::try_from_values(
            vec![
                json!(0.0),
                json!("log_name1"),
                json!("key"),
                json!(IS_CONDITION),
                json!(10),
                json!(INFO_ACTION),
            ],
            None,
        );
        let mut datarow = Datarow::new(
            "log_name1".to_string(),
            Utc::now().naive_utc(),
            vec![("key".to_string(), Datavalue::IntegerID(10))],
        );
        datarow.calculate_sheet_id(TEST_HOST_ID, "test");
        datarow.set_row(1);
        match rule.unwrap().apply(&datarow.into()) {
            RuleOutput::Process(Some(Triggered { action, .. })) => {
                assert_eq!(action, Action::Info)
            }
            _ => panic!("test assert: rule should trigger"),
        };

        let rule = Rule::try_from_values(
            vec![
                json!(0.0),
                json!("log_name1"),
                json!("key"),
                json!(IS_CONDITION),
                json!(10),
                json!(INFO_ACTION),
            ],
            None,
        );
        let mut datarow = Datarow::new(
            "log_name1".to_string(),
            Utc::now().naive_utc(),
            vec![("key".to_string(), Datavalue::IntegerID(10))],
        );
        datarow.calculate_sheet_id(TEST_HOST_ID, "test");
        datarow.set_row(1);
        match rule.unwrap().apply(&datarow.into()) {
            RuleOutput::Process(Some(Triggered { action, .. })) => {
                assert_eq!(action, Action::Info)
            }
            _ => panic!("test assert: rule should trigger"),
        };

        let rule = Rule::try_from_values(
            vec![
                json!(0.0),
                json!("log_name1"),
                json!("key"),
                json!(IS_NOT_CONDITION),
                json!(10),
                json!(INFO_ACTION),
            ],
            None,
        );
        let mut datarow = Datarow::new(
            "log_name1".to_string(),
            Utc::now().naive_utc(),
            vec![("key".to_string(), Datavalue::IntegerID(11))],
        );
        datarow.calculate_sheet_id(TEST_HOST_ID, "test");
        datarow.set_row(1);
        match rule.unwrap().apply(&datarow.into()) {
            RuleOutput::Process(Some(Triggered { action, .. })) => {
                assert_eq!(action, Action::Info)
            }
            _ => panic!("test assert: rule should trigger"),
        };

        let rule = Rule::try_from_values(
            vec![
                json!(0.0),
                json!("log_name1"),
                json!("key"),
                json!(CONTAINS_CONDITION),
                json!("substring"),
                json!(INFO_ACTION),
            ],
            None,
        );
        let mut datarow = Datarow::new(
            "log_name1".to_string(),
            Utc::now().naive_utc(),
            vec![(
                "key".to_string(),
                Datavalue::Text("34substring".to_string()),
            )],
        );
        datarow.calculate_sheet_id(TEST_HOST_ID, "test");
        datarow.set_row(1);
        match rule.unwrap().apply(&datarow.into()) {
            RuleOutput::Process(Some(Triggered { action, .. })) => {
                assert_eq!(action, Action::Info)
            }
            _ => panic!("test assert: rule should trigger"),
        };

        let rule = Rule::try_from_values(
            vec![
                json!(0.0),
                json!("log_name1"),
                json!("key"),
                json!(IS_CONDITION),
                json!("substring"),
                json!(INFO_ACTION),
            ],
            None,
        );
        let mut datarow = Datarow::new(
            "log_name1".to_string(),
            Utc::now().naive_utc(),
            vec![("key".to_string(), Datavalue::Text("substring".to_string()))],
        );
        datarow.calculate_sheet_id(TEST_HOST_ID, "test");
        datarow.set_row(1);
        match rule.unwrap().apply(&datarow.into()) {
            RuleOutput::Process(Some(Triggered { action, .. })) => {
                assert_eq!(action, Action::Info)
            }
            _ => panic!("test assert: rule should trigger"),
        };

        let rule = Rule::try_from_values(
            vec![
                json!(0.0),
                json!("log_name1"),
                json!("key"),
                json!(IS_NOT_CONDITION),
                json!("first"),
                json!(INFO_ACTION),
            ],
            None,
        );
        let mut datarow = Datarow::new(
            "log_name1".to_string(),
            Utc::now().naive_utc(),
            vec![("key".to_string(), Datavalue::Text("second".to_string()))],
        );
        datarow.calculate_sheet_id(TEST_HOST_ID, "test");
        datarow.set_row(1);
        match rule.unwrap().apply(&datarow.into()) {
            RuleOutput::Process(Some(Triggered { action, .. })) => {
                assert_eq!(action, Action::Info)
            }
            _ => panic!("test assert: rule should trigger"),
        };

        let rule = Rule::try_from_values(
            vec![
                json!(0.0),
                json!("log_name1"),
                json!("key"),
                json!(NOT_CONTAINS_CONDITION),
                json!("substring"),
                json!(INFO_ACTION),
            ],
            None,
        );
        let mut datarow = Datarow::new(
            "log_name1".to_string(),
            Utc::now().naive_utc(),
            vec![(
                "key".to_string(),
                Datavalue::Text("34sub2string".to_string()),
            )],
        );
        datarow.calculate_sheet_id(TEST_HOST_ID, "test");
        datarow.set_row(1);
        match rule.unwrap().apply(&datarow.into()) {
            RuleOutput::Process(Some(Triggered { action, .. })) => {
                assert_eq!(action, Action::Info)
            }
            _ => panic!("test assert: rule should trigger"),
        };
    }
}
