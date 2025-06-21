pub mod datavalue;
pub mod sheet;
pub mod spreadsheet;
use crate::rules::{rules_dropdowns, RULES_LOG_NAME};
use crate::HOST_ID_CHARS_LIMIT;
use chrono::{DateTime, Utc};
use google_sheets4::api::Sheet as GoogleSheet;
use google_sheets4::api::{
    AddSheetRequest, AppendCellsRequest, BasicFilter, BooleanCondition, CellData, Color,
    ColorStyle, ConditionValue, CreateDeveloperMetadataRequest, DataFilter, DataValidationRule,
    DeleteDimensionRequest, DeleteSheetRequest, DeveloperMetadata, DeveloperMetadataLocation,
    DeveloperMetadataLookup, DimensionRange, GridProperties, GridRange, Request, RowData,
    SetBasicFilterRequest, SetDataValidationRequest, SheetProperties, UpdateCellsRequest,
    UpdateDeveloperMetadataRequest,
};
use google_sheets4::FieldMask;
use serde_json::Value;
use sheet::{
    generate_metadata_id, Header, KEYS_DELIMITER, METADATA_CREATED_AT, METADATA_HOST_ID_KEY,
    METADATA_KEYS, METADATA_LOG_NAME, METADATA_ROW_COUNT, METADATA_SERVICE_KEY,
    METADATA_UPDATED_AT,
};
use sheet::{prepare_sheet_title, Dropdown, SheetType, TabColorRGB};
pub use spreadsheet::{get_google_auth, SpreadsheetAPI, GOOGLE_SPREADSHEET_MAXIMUM_CELLS};
use std::collections::HashMap;
use std::fmt;
use std::ops::AddAssign;
use std::str::FromStr;
use std::time::Duration;

pub const DEFAULT_FONT: &str = "Verdana";
pub const DEFAULT_FONT_TEXT: &str = "Courier New";

#[derive(Debug, PartialEq, Eq)]
enum Cleanup {
    Truncate(u32, u32),
    Delete,
}

pub type TableId = i32;

pub struct Table {
    columns: Vec<String>,
    id: TableId,
    used_rows: u32,            // actually used rows, even empty ones
    used_columns: u32,         // actually used rows, even empty ones
    rows_count: u32,           // rows count by goral service without rows_to_add
    rows_to_add: Vec<RowData>, // rows to be added
    rows_to_add_count: u32,    // count of rows to be added
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    host: String,
    service: String,
    name: String,
    title: String,
    tab_color_rgb: TabColorRGB,
    to_create: bool,
    to_cleanup: Option<Cleanup>,
}

impl fmt::Debug for Table {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Table")
            .field("title", &self.title)
            .field("id", &self.id)
            .field("columns", &self.columns)
            .field("created_at", &self.created_at)
            .field("updated_at", &self.updated_at)
            .field("used_rows", &self.used_rows)
            .field("used_columns", &self.used_columns)
            .field("rows_count", &self.rows_count)
            .field("rows_to_add_count", &self.rows_to_add_count)
            .field("to_create", &self.to_create)
            .field("to_cleanup", &self.to_cleanup)
            .finish()
    }
}

impl Table {
    pub fn plan_to_create(
        host_id: &str,
        service: &str,
        tab_color_rgb: TabColorRGB,
        datarow: &datavalue::Datarow,
    ) -> Self {
        let timestamp = Utc::now();
        let columns = datarow.columns();
        let header_values: Vec<CellData> = columns
            .iter()
            .map(|c| Header::new(c.clone()).into())
            .collect();
        let sheet_id = datarow.sheet_id();
        let title =
            prepare_sheet_title(host_id, service, datarow.log_name(), &timestamp, &sheet_id);
        let name = datarow.log_name().to_string();
        Self {
            columns,
            id: sheet_id,
            used_rows: 0,
            used_columns: 0,
            rows_count: 0,
            rows_to_add: vec![RowData {
                values: Some(header_values),
            }],
            rows_to_add_count: 1,
            created_at: timestamp,
            updated_at: timestamp,
            host: host_id.to_string(),
            service: service.to_string(),
            name,
            title,
            to_create: true,
            to_cleanup: None,
            tab_color_rgb,
        }
    }

    pub fn plan_to_recreate(&mut self) {
        let header_values: Vec<CellData> = self
            .columns
            .iter()
            .map(|c| Header::new(c.clone()).into())
            .collect();
        self.rows_to_add.insert(
            0,
            RowData {
                values: Some(header_values),
            },
        );
        self.rows_to_add_count += 1;
        self.to_create = true;
        self.used_rows = 0;
        self.used_columns = 0;
        self.rows_count = 0;
    }

    pub fn plan_to_delete(&mut self) {
        self.to_cleanup = Some(Cleanup::Delete);
    }

    pub fn plan_to_truncate(&mut self, start_index: u32, end_index: u32) {
        self.to_cleanup = Some(Cleanup::Truncate(start_index, end_index));
    }

    // returns a potential row to be inserted
    pub fn plan_to_append(&mut self, mut datarow: datavalue::Datarow) -> u32 {
        datarow.sort_by_keys(&self.columns);
        self.rows_to_add.push(datarow.into());
        self.rows_to_add_count += 1;
        self.rows_count + self.rows_to_add_count
    }

    // after successful execution
    pub fn post_execution(&mut self) {
        self.rows_count += self.rows_to_add_count;
        self.rows_to_add_count = 0; // the new rows have been appended
        self.to_create = false; // the table has been created
        self.to_cleanup = None; // the table has been cleaned up
        assert!(self.rows_to_add.is_empty());
    }

    pub fn rows_to_add_count(&self) -> u32 {
        self.rows_to_add_count
    }

    pub fn id(&self) -> &TableId {
        &self.id
    }

    pub fn to_be_created(&self) -> bool {
        self.to_create
    }

    pub fn to_be_cleaned(&self) -> bool {
        self.to_cleanup.is_some()
    }

    pub fn to_be_deleted(&self) -> bool {
        self.to_cleanup == Some(Cleanup::Delete)
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    #[cfg(test)]
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    pub fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }

    pub fn service(&self) -> &str {
        &self.service
    }

    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    pub fn cells_to_be_used(&self) -> u32 {
        self.rows_to_be_used() * self.columns_to_be_used()
    }

    #[cfg(test)]
    pub fn rows_count(&self) -> u32 {
        self.rows_count
    }

    pub fn rows_to_be_used(&self) -> u32 {
        self.used_rows + self.rows_to_add_count
    }

    pub fn columns_to_be_used(&self) -> u32 {
        self.used_columns.max(
            self.columns
                .len()
                .try_into()
                .expect("assert: number of table columns fits u32"),
        )
    }

    pub fn used_rows(&self) -> u32 {
        self.used_rows
    }

    pub fn used_columns(&self) -> u32 {
        self.used_columns
    }

    pub fn api_requests(&mut self) -> Vec<Request> {
        // take out accumulated rows
        let rows_to_add = std::mem::take(&mut self.rows_to_add);

        if self.name == RULES_LOG_NAME && !self.to_create {
            // we don't append for existing rules table
            // but it is important to collect rules with `plan_to_append`
            // in case the rules table is deleted to recreate it
            return vec![];
        }

        assert!(
            rows_to_add.len() == self.rows_to_add_count as usize,
            "assert: rows to add number and their counter should be equal for the table {}",
            self.title
        );
        if let Some(Cleanup::Delete) = self.to_cleanup {
            return vec![Request {
                delete_sheet: Some(DeleteSheetRequest {
                    sheet_id: Some(self.id),
                }),
                ..Default::default()
            }];
        }

        let mut requests = vec![];
        let column_count: i32 = self
            .columns
            .len()
            .try_into()
            .expect("assert: columns count fits i32");
        let rows_count: i32 = self
            .rows_count
            .try_into()
            .expect("assert: rows count fits i32");
        let new_rows_count: i32 = rows_to_add
            .len()
            .try_into()
            .expect("assert: new rows count fits i32");
        let mut total_rows_count = rows_count + new_rows_count;

        if let Some(Cleanup::Truncate(start_index, end_index)) = self.to_cleanup {
            let start_index: i32 = start_index
                .try_into()
                .expect("assert: truncate start_index fits i32");
            let end_index: i32 = end_index
                .try_into()
                .expect("assert: truncate end_index fits i32");

            let rows_to_remove_count = end_index - start_index;
            total_rows_count -= rows_to_remove_count;

            requests.push(Request {
                delete_dimension: Some(DeleteDimensionRequest {
                    range: Some(DimensionRange {
                        sheet_id: Some(self.id),
                        dimension: Some("ROWS".to_string()),
                        start_index: Some(start_index),
                        end_index: Some(end_index),
                    }),
                }),
                ..Default::default()
            });
        }

        if self.to_create {
            let grid_properties = GridProperties {
                column_count: Some(column_count),
                row_count: Some(total_rows_count),
                frozen_row_count: Some(1),
                ..Default::default()
            };

            requests.push(Request {
                add_sheet: Some(AddSheetRequest {
                    properties: Some(SheetProperties {
                        sheet_id: Some(self.id),
                        hidden: Some(false),
                        sheet_type: Some(SheetType::Grid.to_string()),
                        grid_properties: Some(grid_properties),
                        title: Some(self.title.clone()),
                        tab_color_style: Some(ColorStyle {
                            rgb_color: Some(Color {
                                alpha: Some(0.0),
                                red: Some(self.tab_color_rgb.0),
                                green: Some(self.tab_color_rgb.1),
                                blue: Some(self.tab_color_rgb.2),
                            }),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                }),
                ..Default::default()
            });
            let range = GridRange {
                sheet_id: Some(self.id),
                start_row_index: Some(0),
                end_row_index: Some(total_rows_count),
                start_column_index: Some(0),
                end_column_index: Some(column_count),
            };

            requests.push(Request {
                update_cells: Some(UpdateCellsRequest {
                    fields: Some(
                        FieldMask::from_str("userEnteredValue,userEnteredFormat")
                            .expect("assert: field mask can be constructed from static str"),
                    ),
                    range: Some(range),
                    rows: Some(rows_to_add),
                    ..Default::default()
                }),
                ..Default::default()
            });

            if self.name == RULES_LOG_NAME {
                for dropdown in rules_dropdowns() {
                    let Dropdown {
                        values,
                        column_index,
                    } = dropdown;
                    requests.push(Request {
                        set_data_validation: Some(SetDataValidationRequest {
                            rule: Some(DataValidationRule {
                                condition: Some(BooleanCondition {
                                    type_: Some("ONE_OF_LIST".to_string()),
                                    values: Some(
                                        values
                                            .into_iter()
                                            .map(|v| ConditionValue {
                                                user_entered_value: Some(v),
                                                ..Default::default()
                                            })
                                            .collect(),
                                    ),
                                }),
                                show_custom_ui: Some(true),
                                strict: Some(true),
                                ..Default::default()
                            }),
                            range: Some(GridRange {
                                sheet_id: Some(self.id),
                                start_row_index: Some(1),
                                start_column_index: Some(i32::from(column_index)),
                                end_column_index: Some(i32::from(column_index) + 1),
                                ..Default::default()
                            }),
                        }),
                        ..Default::default()
                    });
                }
            }

            let metadata = [
                (METADATA_HOST_ID_KEY, self.host.clone()),
                (METADATA_SERVICE_KEY, self.service.clone()),
                (METADATA_LOG_NAME, self.name.clone()),
                (METADATA_KEYS, self.columns.join(KEYS_DELIMITER)),
                (METADATA_ROW_COUNT, total_rows_count.to_string()),
                (METADATA_CREATED_AT, self.created_at.to_rfc3339()),
                (METADATA_UPDATED_AT, self.updated_at.to_rfc3339()),
            ]
            .into_iter()
            .map(|(k, v)| DeveloperMetadata {
                metadata_id: Some(generate_metadata_id(k, self.id)),
                location: Some(DeveloperMetadataLocation {
                    sheet_id: Some(self.id),
                    ..Default::default()
                }),
                metadata_key: Some(k.to_string()),
                metadata_value: Some(v),
                visibility: Some("PROJECT".to_string()),
            });

            for m in metadata {
                requests.push(Request {
                    create_developer_metadata: Some(CreateDeveloperMetadataRequest {
                        developer_metadata: Some(m),
                    }),
                    ..Default::default()
                })
            }
        } else if self.name != RULES_LOG_NAME {
            // We update only for append case
            // For truncation we don't update to prevent bumping old sheets up
            let take = if self.rows_to_add_count > 0 {
                self.updated_at = Utc::now();
                2
            } else {
                1
            };
            // For truncation case - only rows count update
            let metadata = [
                (METADATA_ROW_COUNT, total_rows_count.to_string()),
                (METADATA_UPDATED_AT, self.updated_at.to_rfc3339()),
            ];

            for (k, v) in metadata.into_iter().take(take) {
                requests.push(Request {
                    update_developer_metadata: Some(UpdateDeveloperMetadataRequest {
                        developer_metadata: Some(DeveloperMetadata {
                            metadata_value: Some(v),
                            ..Default::default()
                        }),
                        data_filters: Some(vec![DataFilter {
                            developer_metadata_lookup: Some(DeveloperMetadataLookup {
                                metadata_id: Some(generate_metadata_id(k, self.id)),
                                ..Default::default()
                            }),
                            ..Default::default()
                        }]),
                        fields: Some(
                            FieldMask::from_str("metadataValue")
                                .expect("assert: field mask can be constructed from static str"),
                        ),
                    }),
                    ..Default::default()
                })
            }
            if self.rows_to_add_count > 0 {
                requests.push(Request {
                    append_cells: Some(AppendCellsRequest {
                        fields: Some(
                            FieldMask::from_str("userEnteredValue,userEnteredFormat")
                                .expect("assert: field mask can be constructed from static str"),
                        ),
                        sheet_id: Some(self.id),
                        rows: Some(rows_to_add),
                    }),
                    ..Default::default()
                });
            }
        }

        // Add a per column filters
        let filter_range = GridRange {
            sheet_id: Some(self.id),
            start_row_index: Some(0),
            end_row_index: Some(total_rows_count),
            start_column_index: Some(0),
            end_column_index: None,
        };
        requests.push(Request {
            set_basic_filter: Some(SetBasicFilterRequest {
                filter: Some(BasicFilter {
                    range: Some(filter_range),
                    ..Default::default()
                }),
            }),
            ..Default::default()
        });

        if self.name != RULES_LOG_NAME && self.used_rows > self.rows_count {
            // Delete empty rows after
            requests.push(Request {
                delete_dimension: Some(DeleteDimensionRequest {
                    range: Some(DimensionRange {
                        sheet_id: Some(self.id),
                        dimension: Some("ROWS".to_string()),
                        start_index: Some(total_rows_count),
                        end_index: None,
                    }),
                }),
                ..Default::default()
            });
        }

        requests
    }
}

impl AddAssign for Table {
    // merging with another table (new version)
    fn add_assign(&mut self, mut other: Self) {
        assert!(self == &other, "assert: tables to merge are equal");
        other.rows_to_add = std::mem::take(&mut self.rows_to_add);
        other.rows_to_add_count = self.rows_to_add_count;
        *self = other;
    }
}

impl TryFrom<GoogleSheet> for Table {
    type Error = &'static str;

    fn try_from(mut sheet: GoogleSheet) -> Result<Self, Self::Error> {
        let properties = sheet
            .properties
            .expect("assert: sheet properties cannot be null");
        let sheet_type: SheetType = properties
            .sheet_type
            .expect("assert: sheet type cannot be null")
            .into();
        if sheet_type != SheetType::Grid {
            return Err("sheet is not grid");
        }
        let mut metadata: HashMap<String, String> = sheet
            .developer_metadata
            .take()
            .unwrap_or_default()
            .into_iter()
            .map(|meta| {
                (
                    meta.metadata_key
                        .expect("assert: if sheet has metadata entry, it has a key"),
                    meta.metadata_value
                        .expect("assert: if sheet has metadata entry, it has a value"),
                )
            })
            .collect();

        let sheet_id = properties
            .sheet_id
            .expect("assert: sheet sheet_id cannot be null");
        let title = properties
            .title
            .expect("assert: sheet title cannot be null");

        let columns: Vec<String> = metadata
            .remove(METADATA_KEYS)
            .ok_or("meta keys is set at sheet creation")?
            .split(KEYS_DELIMITER)
            .map(|k| k.to_string())
            .collect();
        let rows_count = metadata
            .remove(METADATA_ROW_COUNT)
            .ok_or("a managed grid sheet has rows metadata")?
            .parse()
            .expect("assert: rows metadata is a non-negative integer");
        let created_at = DateTime::parse_from_rfc3339(
            metadata
                .get(METADATA_CREATED_AT)
                .ok_or("meta created_at is set at sheet creation")?,
        )
        .expect("created_at timestamp is saved in rfc3339")
        .into();
        let updated_at = DateTime::parse_from_rfc3339(
            metadata
                .get(METADATA_UPDATED_AT)
                .ok_or("meta updated_at is set at sheet creation")?,
        )
        .expect("updated_at timestamp is saved in rfc3339")
        .into();
        let host = metadata
            .remove(METADATA_HOST_ID_KEY)
            .ok_or("meta host is set at sheet creation")?;
        let service = metadata
            .remove(METADATA_SERVICE_KEY)
            .ok_or("meta service is set at sheet creation")?;
        let name = metadata
            .remove(METADATA_LOG_NAME)
            .ok_or("meta name is set at sheet creation")?;
        let tab_color_rgb = properties
            .tab_color_style
            .and_then(|tcs| tcs.rgb_color)
            .map(|rgb_color| {
                (
                    rgb_color.red.unwrap_or(0.0),
                    rgb_color.green.unwrap_or(0.0),
                    rgb_color.blue.unwrap_or(0.0),
                )
            })
            .unwrap_or((0.0, 0.0, 0.0));
        Ok(Self {
            columns,
            id: sheet_id,
            used_rows: properties
                .grid_properties
                .as_ref()
                .and_then(|gp| gp.row_count)
                .expect("assert: grid sheet has rows")
                .try_into()
                .expect("assert: row count fits u32"),
            used_columns: properties
                .grid_properties
                .as_ref()
                .and_then(|gp| gp.column_count)
                .expect("assert: grid sheet has columns")
                .try_into()
                .expect("assert: column count fits u32"),
            rows_count,
            rows_to_add: vec![],
            rows_to_add_count: 0,
            created_at,
            updated_at,
            host,
            service,
            name,
            title,
            to_create: false,
            to_cleanup: None,
            tab_color_rgb,
        })
    }
}

impl PartialEq for Table {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for Table {}

pub struct Storage {
    google: SpreadsheetAPI,
    host_id: String,
}

impl Storage {
    pub fn new(host_id: String, google: SpreadsheetAPI) -> Self {
        assert!(
            host_id.chars().count() <= HOST_ID_CHARS_LIMIT,
            "host id should be no more than {HOST_ID_CHARS_LIMIT} characters"
        );
        Self { host_id, google }
    }

    pub fn table_row_url(&self, spreadsheet_id: &str, sheet_id: TableId, row: u32) -> String {
        self.google.sheet_row_url(spreadsheet_id, sheet_id, row)
    }

    pub fn host_id(&self) -> &str {
        &self.host_id
    }

    pub fn base_url(&self, spreadsheet_id: &str) -> String {
        self.google.spreadsheet_baseurl(spreadsheet_id)
    }

    pub async fn execute_plan(
        &self,
        spreadsheet_id: &str,
        tables: &mut HashMap<TableId, Table>,
    ) -> Result<(), StorageError> {
        let requests: Vec<Request> = tables.values_mut().flat_map(|t| t.api_requests()).collect();

        self.google.crud_sheets(spreadsheet_id, requests).await?;
        Ok(())
    }

    pub async fn get_table(
        &self,
        spreadsheet_id: &str,
        rules_table_id: TableId,
    ) -> Result<Vec<Vec<Value>>, StorageError> {
        self.google
            .get_sheet_data(spreadsheet_id, rules_table_id)
            .await
    }

    pub async fn tables_for_service(
        &self,
        spreadsheet_id: &str,
        service: &str,
    ) -> Result<Vec<Table>, StorageError> {
        let tables = self
            .google
            .sheets_filtered(spreadsheet_id, &self.host_id, service)
            .await?;

        Ok(tables)
    }
}

#[derive(Debug)]
pub enum StorageError {
    Timeout(Duration),
    RetryTimeout((Duration, usize, String)),
    Retriable(String),
    NonRetriable(String),
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use StorageError::*;
        match self {
            Timeout(duration) => write!(f, "Google API timeout {:?}", duration),
            RetryTimeout((maximum_backoff, retry, last_retry_error)) => write!(f, "Google API is unavailable ({last_retry_error}): maximum retry duration {maximum_backoff:?} is reached with {retry} retries"),
            Retriable(e) | NonRetriable(e) => write!(f, "Google API: {}", e),
        }
    }
}
