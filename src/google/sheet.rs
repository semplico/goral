use crate::google::{TableId, DEFAULT_FONT};
use chrono::{DateTime, Utc};
use google_sheets4::api::{CellData, CellFormat, Color, ColorStyle, ExtendedValue, TextFormat};
use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::hash::Hasher;
use std::time::Duration;

pub type TabColorRGB = (f32, f32, f32);

pub(super) const METADATA_SERVICE_KEY: &str = "service";
pub(super) const METADATA_HOST_ID_KEY: &str = "host";
pub(super) const METADATA_LOG_NAME: &str = "name";
pub(super) const METADATA_CREATED_AT: &str = "created_at";
pub(super) const METADATA_UPDATED_AT: &str = "updated_at";
pub(super) const METADATA_ROW_COUNT: &str = "rows";
pub(super) const METADATA_KEYS: &str = "keys";
pub(super) const KEYS_DELIMITER: &str = "~^~";

pub(super) fn generate_metadata_id(key: &str, sheet_id: TableId) -> i32 {
    str_to_id(&format!("{}{}", sheet_id, key))
}

#[derive(Debug, Clone, PartialEq)]
pub enum SheetType {
    Grid,
    Chart,
    Other,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Dropdown {
    pub values: Vec<String>,
    pub column_index: u16,
}

impl From<String> for SheetType {
    fn from(t: String) -> Self {
        match t.as_str() {
            "GRID" => SheetType::Grid,
            "OBJECT" => SheetType::Chart,
            _ => SheetType::Other,
        }
    }
}

impl fmt::Display for SheetType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SheetType::Grid => write!(f, "GRID"),
            SheetType::Chart => write!(f, "OBJECT"),
            SheetType::Other => write!(f, "SHEET_TYPE_UNSPECIFIED"),
        }
    }
}

#[derive(Debug, Default)]
pub struct Header {
    title: String,
    note: Option<String>,
}

impl Header {
    pub fn new(title: String, note: Option<String>) -> Self {
        Self { title, note }
    }

    pub fn title(&self) -> &str {
        &self.title
    }
}

impl PartialEq for Header {
    fn eq(&self, other: &Self) -> bool {
        self.title == other.title
    }
}
impl Eq for Header {}

impl From<Header> for CellData {
    fn from(val: Header) -> Self {
        CellData {
            user_entered_value: Some(ExtendedValue {
                string_value: Some(val.title),
                ..Default::default()
            }),
            user_entered_format: Some(CellFormat {
                background_color_style: Some(ColorStyle {
                    rgb_color: Some(Color {
                        alpha: Some(0.0),
                        red: Some(0.0),
                        green: Some(0.0),
                        blue: Some(0.0),
                    }),
                    ..Default::default()
                }),
                horizontal_alignment: Some("CENTER".to_string()),
                text_format: Some(TextFormat {
                    bold: Some(true),
                    font_family: Some(DEFAULT_FONT.to_string()),
                    foreground_color_style: Some(ColorStyle {
                        rgb_color: Some(Color {
                            alpha: Some(0.0),
                            red: Some(1.0),
                            green: Some(1.0),
                            blue: Some(1.0),
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            note: val.note,
            ..Default::default()
        }
    }
}

// This method is not collision-free
// id is required to be non-negative by
// Google sheets API
// The probability of collisions is determined
// by the birthday problem and is approximately
// q^2/i32::MAX where q is the number of objects to generate ids
// For 50 sheets to be created in the same workbook this probability
// is approximately 10^-6 which is acceptable for our purposes
// See also the test below
pub fn str_to_id(s: &str) -> i32 {
    let mut hasher = DefaultHasher::new();
    hasher.write(s.as_bytes());
    let bytes = hasher.finish().to_be_bytes();
    i32::from_be_bytes(
        bytes[4..8]
            .try_into()
            .expect("assert: i32 is created from 4 bytes"),
    )
    .abs()
}

macro_rules! sheet_name_jitter {
    ($sheet_id:expr) => {
        // we need 8 lowest significant bits
        (*$sheet_id as u8) >> 3
    };
}

#[allow(clippy::cast_possible_truncation)]
#[allow(clippy::cast_sign_loss)]
pub(super) fn prepare_sheet_title(
    host_id: &str,
    service: &str,
    log_name: &str,
    timestamp: &DateTime<Utc>,
    sheet_id: &TableId,
) -> String {
    // to prevent sheet titles conflicts we "randomize" a little bit sheet creation datetime
    let jitter = sheet_name_jitter!(sheet_id);
    let timestamp = *timestamp + Duration::from_secs(jitter.into());
    // use @ as a delimiter
    // see https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels
    format!(
        "{}@{}@{} {}",
        log_name,
        host_id,
        service,
        timestamp.format("%yy/%m/%d %H:%M:%S"),
    )
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use google_sheets4::api::{GridProperties, Sheet as GoogleSheet, SheetProperties};
    use rand::{distr::Alphanumeric, Rng};
    use std::collections::HashMap;
    use std::collections::HashSet;

    #[test]
    #[allow(clippy::cast_possible_truncation)]
    #[allow(clippy::cast_sign_loss)]
    fn jitter() {
        let jitter = sheet_name_jitter!(&137328873_i32);
        assert!(
            jitter < 2_u8.pow(5),
            "sheet_name_jitter should produce values less than 2^5"
        )
    }

    pub fn mock_ordinary_google_sheet(title: &str) -> GoogleSheet {
        GoogleSheet {
            banded_ranges: None,
            basic_filter: None,
            charts: None,
            column_groups: None,
            conditional_formats: None,
            data: None,
            developer_metadata: None,
            filter_views: None,
            merges: None,
            properties: Some(SheetProperties {
                data_source_sheet_properties: None,
                grid_properties: Some(GridProperties {
                    column_count: Some(26),
                    column_group_control_after: None,
                    frozen_column_count: None,
                    frozen_row_count: None,
                    hide_gridlines: None,
                    row_count: Some(1000),
                    row_group_control_after: None,
                }),
                hidden: None,
                index: Some(0),
                right_to_left: None,
                sheet_id: Some(0),
                sheet_type: Some("GRID".to_string()),
                tab_color: None,
                tab_color_style: Some(ColorStyle {
                    rgb_color: Some(Color {
                        alpha: None,
                        blue: None,
                        green: None,
                        red: Some(1.0),
                    }),
                    theme_color: None,
                }),
                title: Some(title.to_string()),
            }),
            protected_ranges: None,
            row_groups: None,
            slicers: None,
        }
    }

    pub fn mock_sheet_with_properties(properties: SheetProperties) -> GoogleSheet {
        GoogleSheet {
            banded_ranges: None,
            basic_filter: None,
            charts: None,
            column_groups: None,
            conditional_formats: None,
            data: None,
            developer_metadata: None,
            filter_views: None,
            merges: None,
            properties: Some(properties),
            protected_ranges: None,
            row_groups: None,
            slicers: None,
        }
    }

    #[test]
    fn id_generation() {
        let id = str_to_id("some text to generate id from");
        assert!(id > 0, "generated id should be positive");
    }

    #[test]
    #[allow(clippy::cast_lossless)]
    fn id_collision() {
        let mut counts = HashMap::new();
        let mut rng = rand::rng();
        //let total = crate::google::spreadsheet::GOOGLE_SPREADSHEET_MAXIMUM_CELLS; // a theoretical number of sheets
        let total = 50; // a reasonable number of sheets
        for _ in 0..total {
            let n: usize = rng.random_range(10..40);
            let s: String = (&mut rng)
                .sample_iter(&Alphanumeric)
                .take(n)
                .map(char::from)
                .collect();
            let id = str_to_id(&s);
            *counts.entry(id).or_insert(0) += 1;
        }
        let mut collisions = HashSet::new();
        let mut num_of_collisions = 0;
        let mut num_of_duplicates = 0; // how many objects would be rejected
        for (_, count) in counts {
            if count == 1 {
                continue;
            }
            num_of_collisions += count;
            num_of_duplicates += count - 1;
            collisions.insert(count);
        }
        // Example for 10_000_000 objects
        // num_of_collisions 46771, 0.47%, num_of_duplicates: 23402
        // collisions numbers {3, 4, 2}
        let share = 100.0 * num_of_collisions as f64 / total as f64;
        println!(
            "num_of_collisions {}, {:.2}%, num_of_duplicates: {}\n{:?}",
            num_of_collisions, share, num_of_duplicates, collisions
        );
        assert!(
            share < 0.000001,
            "collisions are highly improbable for usual cases"
        );
    }
}
