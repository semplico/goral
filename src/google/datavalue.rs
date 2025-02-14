use crate::google::sheet::{str_to_id, Header, SheetId};
use crate::google::{DEFAULT_FONT, DEFAULT_FONT_TEXT};
use crate::rules::RuleApplicant;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use google_sheets4::api::{
    CellData, CellFormat, Color, ColorStyle, ExtendedValue, NumberFormat, RowData, TextFormat,
};

use std::collections::HashMap;
use std::mem;

const DATETIME_COLUMN_NAME: &str = "datetime";
const MILLIS_PER_DAY: f64 = (24 * 60 * 60 * 1000) as f64;
const THOUSAND: u64 = 10_u64.pow(3);
const MILLION: u64 = 10_u64.pow(6);
const BILLION: u64 = 10_u64.pow(9);
const TRILLION: u64 = 10_u64.pow(12);
pub const NOT_AVAILABLE: &str = "N/A";

const fn size_pattern(size: u64) -> &'static str {
    if size >= TRILLION {
        "#,,,,\"T\""
    } else if size >= BILLION {
        "#,,,\"G\""
    } else if size >= MILLION {
        "#,,\"M\""
    } else if size >= THOUSAND {
        "#,\"K\""
    } else {
        "#"
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Datavalue {
    Text(String),
    RedText(String),
    OrangeText(String),
    GreenText(String),
    Number(f64),
    // As Google accepts only f64 (https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#ExtendedValue)
    // We use u32 for lossless casts
    Integer(u32),
    IntegerID(u32),
    Percent(f64),
    HeatmapPercent(f64),
    Datetime(NaiveDateTime),
    Bool(bool),
    Size(u64),
    NotAvailable,
}

#[derive(Debug, Clone)]
pub struct Datarow {
    log_name: String,
    timestamp: NaiveDateTime,
    pub data: Vec<(String, Datavalue)>,
    sheet_id: Option<SheetId>,
}

impl Datarow {
    pub fn new(log_name: String, timestamp: NaiveDateTime, data: Vec<(String, Datavalue)>) -> Self {
        Self {
            log_name,
            timestamp,
            data,
            sheet_id: None,
        }
    }

    pub fn sheet_id(&mut self, host_id: &str, service_name: &str) -> SheetId {
        if let Some(sheet_id) = self.sheet_id {
            return sheet_id;
        }
        let keys = self.keys_sorted().join(",");
        let id_string = format!("{host_id}_{service_name}_{}_{}", self.log_name, keys);
        let sheet_id = str_to_id(&id_string);
        self.sheet_id = Some(sheet_id);
        sheet_id
    }

    pub fn keys(&self) -> Vec<&str> {
        let mut keys = Vec::with_capacity(self.data.len() + 1);
        keys.push(DATETIME_COLUMN_NAME);
        for (k, _) in self.data.iter() {
            keys.push(k);
        }
        keys
    }

    pub fn keys_sorted(&self) -> Vec<&str> {
        let mut keys = Vec::with_capacity(self.data.len() + 1);
        keys.push(DATETIME_COLUMN_NAME);
        for (k, _) in self.data.iter() {
            keys.push(k);
        }
        keys.sort_unstable();
        keys
    }

    pub fn headers(&self) -> Vec<Header> {
        let mut headers = Vec::with_capacity(self.data.len() + 1);
        headers.push(Header::new(DATETIME_COLUMN_NAME.to_string(), None));
        for (k, _) in self.data.iter() {
            headers.push(Header::new(k.to_string(), None));
        }
        headers
    }

    pub fn sort_by_keys(&mut self, keys: &[String]) {
        let cap = self.data.len();
        let mut data: HashMap<String, Datavalue> =
            mem::replace(&mut self.data, Vec::with_capacity(cap))
                .into_iter()
                .collect();
        // we skip first header which is DATETIME_COLUMN_NAME
        for h in keys.iter().skip(1) {
            self.data.push(
                data.remove_entry(h)
                    .expect("assert: datarow should be sorted by keys which it contains"),
            );
        }
    }

    pub fn values(self) -> Vec<Datavalue> {
        let mut values = Vec::with_capacity(self.data.len() + 1);
        values.push(Datavalue::Datetime(self.timestamp));
        for (_, v) in self.data.into_iter() {
            values.push(v);
        }
        values
    }

    pub fn log_name(&self) -> &String {
        &self.log_name
    }
}

// https://developers.google.com/sheets/api/guides/formats
impl From<Datarow> for RowData {
    fn from(val: Datarow) -> Self {
        let row = val
            .values()
            .into_iter()
            .map(|v| {
                let (value, format) = match v {
                    Datavalue::Text(t) => (
                        ExtendedValue {
                            string_value: Some(t),
                            ..Default::default()
                        },
                        CellFormat {
                            text_format: Some(TextFormat {
                                font_family: Some(DEFAULT_FONT_TEXT.to_string()),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                    ),
                    Datavalue::RedText(t) => (
                        ExtendedValue {
                            string_value: Some(t),
                            ..Default::default()
                        },
                        CellFormat {
                            text_format: Some(TextFormat {
                                bold: Some(true),
                                font_family: Some(DEFAULT_FONT_TEXT.to_string()),
                                foreground_color_style: Some(ColorStyle {
                                    rgb_color: Some(Color {
                                        alpha: Some(0.0),
                                        red: Some(1.0),
                                        green: Some(0.0),
                                        blue: Some(0.0),
                                    }),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                    ),
                    Datavalue::OrangeText(t) => (
                        ExtendedValue {
                            string_value: Some(t),
                            ..Default::default()
                        },
                        CellFormat {
                            text_format: Some(TextFormat {
                                bold: Some(true),
                                font_family: Some(DEFAULT_FONT_TEXT.to_string()),
                                foreground_color_style: Some(ColorStyle {
                                    rgb_color: Some(Color {
                                        alpha: Some(0.0),
                                        red: Some(1.0),
                                        green: Some(0.502),
                                        blue: Some(0.502),
                                    }),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                    ),
                    Datavalue::GreenText(t) => (
                        ExtendedValue {
                            string_value: Some(t),
                            ..Default::default()
                        },
                        CellFormat {
                            text_format: Some(TextFormat {
                                bold: Some(true),
                                font_family: Some(DEFAULT_FONT_TEXT.to_string()),
                                foreground_color_style: Some(ColorStyle {
                                    rgb_color: Some(Color {
                                        alpha: Some(0.0),
                                        red: Some(0.0),
                                        green: Some(0.5),
                                        blue: Some(0.0),
                                    }),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                    ),
                    Datavalue::Percent(p) => (
                        ExtendedValue {
                            number_value: Some(p / 100.0),
                            ..Default::default()
                        },
                        CellFormat {
                            number_format: Some(NumberFormat {
                                pattern: Some(r"#%".to_string()),
                                type_: Some("NUMBER".to_string()),
                            }),
                            text_format: Some(TextFormat {
                                font_family: Some(DEFAULT_FONT.to_string()),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                    ),
                    Datavalue::HeatmapPercent(p) => (
                        ExtendedValue {
                            number_value: Some(p / 100.0),
                            ..Default::default()
                        },
                        CellFormat {
                            number_format: Some(NumberFormat {
                                pattern: Some(
                                    r"[Red][>0.8]#%;[Color22][>0.5]#%;[Color10]#%".to_string(),
                                ),
                                type_: Some("NUMBER".to_string()),
                            }),
                            text_format: Some(TextFormat {
                                font_family: Some(DEFAULT_FONT.to_string()),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                    ),
                    Datavalue::Integer(i) => (
                        ExtendedValue {
                            number_value: Some(f64::from(i)),
                            ..Default::default()
                        },
                        CellFormat {
                            number_format: Some(NumberFormat {
                                pattern: Some(r"#,###".to_string()),
                                type_: Some("NUMBER".to_string()),
                            }),
                            text_format: Some(TextFormat {
                                font_family: Some(DEFAULT_FONT.to_string()),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                    ),
                    Datavalue::IntegerID(i) => (
                        ExtendedValue {
                            number_value: Some(f64::from(i)),
                            ..Default::default()
                        },
                        CellFormat {
                            number_format: Some(NumberFormat {
                                pattern: Some(r"#".to_string()),
                                type_: Some("NUMBER".to_string()),
                            }),
                            text_format: Some(TextFormat {
                                font_family: Some(DEFAULT_FONT.to_string()),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                    ),
                    Datavalue::Number(f) => (
                        ExtendedValue {
                            number_value: Some(f),
                            ..Default::default()
                        },
                        CellFormat {
                            number_format: Some(NumberFormat {
                                pattern: Some(r"####.##".to_string()),
                                type_: Some("NUMBER".to_string()),
                            }),
                            text_format: Some(TextFormat {
                                font_family: Some(DEFAULT_FONT.to_string()),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                    ),
                    Datavalue::Bool(b) => (
                        ExtendedValue {
                            bool_value: Some(b),
                            ..Default::default()
                        },
                        CellFormat {
                            text_format: Some(TextFormat {
                                font_family: Some(DEFAULT_FONT.to_string()),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                    ),
                    Datavalue::Datetime(d) => (
                        ExtendedValue {
                            number_value: Some(convert_datetime_to_spreadsheet_double(d)),
                            ..Default::default()
                        },
                        CellFormat {
                            number_format: Some(NumberFormat {
                                pattern: Some(r"yyy mmm d \[ddd\] h:mm:ss".to_string()),
                                type_: Some("DATE_TIME".to_string()),
                            }),
                            text_format: Some(TextFormat {
                                font_family: Some(DEFAULT_FONT.to_string()),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                    ),
                    Datavalue::Size(s) => (
                        ExtendedValue {
                            // Size type allows some round errors
                            // as it is used for system data
                            number_value: Some(s as f64),
                            ..Default::default()
                        },
                        CellFormat {
                            number_format: Some(NumberFormat {
                                pattern: Some(size_pattern(s).to_string()),
                                type_: Some("NUMBER".to_string()),
                            }),
                            text_format: Some(TextFormat {
                                font_family: Some(DEFAULT_FONT.to_string()),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                    ),
                    Datavalue::NotAvailable => (
                        ExtendedValue {
                            string_value: Some(NOT_AVAILABLE.to_string()),
                            ..Default::default()
                        },
                        CellFormat {
                            text_format: Some(TextFormat {
                                font_family: Some("Courier New".to_string()),
                                ..Default::default()
                            }),
                            horizontal_alignment: Some("CENTER".to_string()),
                            ..Default::default()
                        },
                    ),
                };
                CellData {
                    user_entered_value: Some(value),
                    user_entered_format: Some(format),
                    ..Default::default()
                }
            })
            .collect();
        RowData { values: Some(row) }
    }
}

impl From<Datarow> for RuleApplicant {
    // TODO convert only data, which is actually used in rules processing
    fn from(val: Datarow) -> Self {
        use Datavalue::*;
        let Datarow {
            log_name,
            timestamp,
            data,
            sheet_id,
        } = val;
        let sheet_id = sheet_id.expect(
            "assert: sheet id should be initialized before the rule applicant transformation",
        );
        // convert datavalues into types supported by rules with O(1) access
        let data = data
            .into_iter()
            .chain([(
                DATETIME_COLUMN_NAME.to_string(),
                Datavalue::Datetime(timestamp),
            )])
            .map(|(k, v)| {
                let v = match v {
                    Text(t) | RedText(t) | OrangeText(t) | GreenText(t) => Text(t),
                    Number(n) => Number(n),
                    Integer(i) | IntegerID(i) => Integer(i),
                    Percent(p) | HeatmapPercent(p) => Number(p / 100.0),
                    Datetime(d) => Number(convert_datetime_to_spreadsheet_double(d)),
                    Bool(b) => Bool(b),
                    Size(s) => Number(s as f64), // Rounding errors are acceptable for Size datavalues
                    NotAvailable => NotAvailable,
                };
                (k, v)
            })
            .collect();
        RuleApplicant {
            log_name,
            data,
            sheet_id,
        }
    }
}

// https://developers.google.com/sheets/api/reference/rest/v4/DateTimeRenderOption
fn convert_datetime_to_spreadsheet_double(d: NaiveDateTime) -> f64 {
    let base = NaiveDate::from_ymd_opt(1899, 12, 30)
        .expect("assert: static datetime")
        .and_hms_opt(0, 0, 0)
        .expect("assert: static datetime");
    if d < base {
        return 0.0;
    }
    let days = (d - base).num_days() as f64;
    let millis = d
        .time()
        .signed_duration_since(
            NaiveTime::from_hms_milli_opt(0, 0, 0, 0).expect("assert: static time"),
        )
        .num_milliseconds();
    let fraction_of_day = millis as f64 / MILLIS_PER_DAY;
    days + fraction_of_day
}

#[cfg(test)]
mod tests {
    use super::*;

    impl Datarow {
        pub fn keys_values(&self) -> HashMap<String, Datavalue> {
            self.data.clone().into_iter().collect()
        }
    }

    #[test]
    fn size_pattern_cases() {
        assert_eq!("#".to_string(), size_pattern(670_u64));
        assert_eq!("#,\"K\"".to_string(), size_pattern(6_701_u64));
        assert_eq!("#,,\"M\"".to_string(), size_pattern(1_020_111_u64));
        assert_eq!("#,,,\"G\"".to_string(), size_pattern(1_020_111_324_u64));
        assert_eq!(
            "#,,,,\"T\"".to_string(),
            size_pattern(1_020_111_324_428_u64)
        );
    }

    #[test]
    fn datarow_id() {
        let scrape_time = NaiveDate::from_ymd_opt(2023, 10, 19)
            .expect("test assert: static datetime")
            .and_hms_opt(0, 0, 0)
            .expect("test assert: static datetime");
        let mut datarow = Datarow::new(
            "/dev/shm".to_string(),
            scrape_time,
            vec![
                (format!("disk_use"), Datavalue::HeatmapPercent(3_f64)),
                (format!("disk_free"), Datavalue::Size(400_u64)),
            ],
        );
        assert_eq!(895475598, datarow.sheet_id("host1", "system"));
    }

    #[test]
    fn datarow_has_timestamp_as_first_header() {
        let scrape_time = NaiveDate::from_ymd_opt(2023, 10, 19)
            .expect("test assert: static datetime")
            .and_hms_opt(0, 0, 0)
            .expect("test assert: static datetime");
        let datarow = Datarow::new(
            "/dev/shm".to_string(),
            scrape_time,
            vec![
                (format!("disk_use"), Datavalue::HeatmapPercent(3_f64)),
                (format!("disk_free"), Datavalue::Size(400_u64)),
            ],
        );
        assert_eq!(
            datarow.headers()[0],
            Header::new(DATETIME_COLUMN_NAME.to_string(), None)
        );
    }

    #[test]
    fn spreadsheet_datetime() {
        let timestamp = NaiveDate::from_ymd_opt(1900, 01, 01)
            .expect("test assert: static date")
            .and_hms_opt(12, 0, 0)
            .expect("test assert: static time");
        assert_eq!(convert_datetime_to_spreadsheet_double(timestamp), 2.5);
        let timestamp = NaiveDate::from_ymd_opt(1900, 02, 01)
            .expect("test assert: static date")
            .and_hms_opt(15, 0, 0)
            .expect("test assert: static time");
        assert_eq!(convert_datetime_to_spreadsheet_double(timestamp), 33.625);
    }
}
