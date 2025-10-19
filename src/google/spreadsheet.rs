use crate::configuration::GOOGLE_SHEET_BASE;
#[cfg(not(test))]
use crate::google::sheet::{METADATA_HOST_ID_KEY, METADATA_SERVICE_KEY};
use crate::google::{StorageError, Table, TableId};
use crate::http::HyperConnector;
use crate::notifications::Sender;
use chrono::Utc;
use google_sheets4::api::{
    BatchUpdateSpreadsheetRequest, BatchUpdateSpreadsheetResponse, Request, Spreadsheet,
};
use google_sheets4::yup_oauth2;
use google_sheets4::{Error as SheetsError, Result as SheetsResult, hyper};
#[cfg(not(test))]
use google_sheets4::{
    api::{
        BatchGetValuesByDataFilterRequest, DataFilter, DeveloperMetadataLookup,
        GetSpreadsheetByDataFilterRequest, GridRange, Sheets,
    },
    yup_oauth2::authenticator::Authenticator,
};
use serde_json::Value;

#[cfg(test)]
use crate::google::spreadsheet::tests::TestState;

// https://support.google.com/docs/thread/181288162/whats-the-maximum-amount-of-rows-in-google-sheets?hl=en
pub const GOOGLE_SPREADSHEET_MAXIMUM_CELLS: u32 = 10_000_000;
pub const GOOGLE_SPREADSHEET_MAXIMUM_CHARS_PER_CELL: usize = 50_000;
type SheetResponse<T = BatchUpdateSpreadsheetResponse> = SheetsResult<(
    hyper::Response<http_body_util::combinators::BoxBody<hyper::body::Bytes, hyper::Error>>,
    T,
)>;

pub async fn get_google_auth(
    service_account_credentials_path: &str,
) -> (
    String,
    yup_oauth2::authenticator::Authenticator<HyperConnector>,
) {
    let key = yup_oauth2::read_service_account_key(service_account_credentials_path)
        .await
        .expect("failed to read service account credentials file");
    (
        key.project_id
            .clone()
            .expect("assert: service account has project id"),
        yup_oauth2::ServiceAccountAuthenticator::builder(key)
            .build()
            .await
            .expect("failed to create Google API authenticator"),
    )
}

async fn handle_error<T>(
    spreadsheet: &SpreadsheetAPI,
    result: SheetResponse<T>,
) -> Result<T, StorageError> {
    match result {
        Err(e) => match e {
            // fatal
            SheetsError::MissingAPIKey => {
                tracing::error!("{}", e);
                spreadsheet
                    .send_notification
                    .fatal(format!("Fatal error for Google API access\n```{e}```"))
                    .await;
                panic!("Fatal error for Google API access: `{e}`");
            }
            SheetsError::MissingToken(_) => {
                let msg = format!(
                    "`MissingToken error` for Google API\nProbably server time skewed which is now `{}`\nSync server time with NTP",
                    Utc::now()
                );
                tracing::error!("{}{}", e, msg);
                spreadsheet.send_notification.fatal(msg).await;
                panic!("{e}Probably server time skewed. Sync server time with NTP.");
            }
            SheetsError::UploadSizeLimitExceeded(actual, limit) => {
                let msg = format!("uploading to much data {actual} vs limit of {limit} bytes");
                tracing::error!("{}", msg);
                spreadsheet
                    .send_notification
                    .fatal(format!("Fatal error for Google API access\n```{msg}```"))
                    .await;
                panic!("Fatal error for Google API access: `{msg}`");
            }
            // retry
            SheetsError::Failure(v) => Err(StorageError::Retriable(format!("failure: {v:?}"))),
            SheetsError::HttpError(v) => Err(StorageError::Retriable(format!("http error: {v}"))),
            SheetsError::BadRequest(v) => {
                match v
                    .get("error")
                    .and_then(|e| e.get("code"))
                    .and_then(|code| code.as_u64())
                {
                    Some(code) if code > 499 => {
                        Err(StorageError::Retriable(format!("bad request: {v}")))
                    }
                    _ => {
                        let text = format!("bad request: {v}");
                        if text.contains("This action would increase the number of cells in the workbook above the limit of 10000000 cells") {
                            Err(StorageError::NonRetriable("The associated spreadsheet is full: either your services have incorrect [truncation limits](https://maksimryndin.github.io/goral/services.html#storage-quota) or you have other data in the spreadsheet. Until the spreadsheet is truncated manually, no new rows can be appended, no new rules updates will work.".to_string()))
                        } else {
                            Err(StorageError::NonRetriable(text))
                        }
                    }
                }
            }
            SheetsError::Io(v) => Err(StorageError::Retriable(format!("io: {v}"))),
            SheetsError::JsonDecodeError(_, v) => Err(StorageError::NonRetriable(format!(
                "json decode error: {v}"
            ))),
            SheetsError::FieldClash(v) => {
                Err(StorageError::NonRetriable(format!("field clash: {v}")))
            }
            SheetsError::Cancelled => Err(StorageError::Retriable("cancelled".to_string())),
        },
        Ok(res) => Ok(res.1),
    }
}

pub struct SpreadsheetAPI {
    send_notification: Sender,
    #[cfg(not(test))]
    hub: Sheets<HyperConnector>,
    #[cfg(test)]
    state: tokio::sync::Mutex<TestState>,
}

impl SpreadsheetAPI {
    #[cfg(not(test))]
    pub fn new(authenticator: Authenticator<HyperConnector>, send_notification: Sender) -> Self {
        let hub = Sheets::new(
            hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
                .build(
                    hyper_rustls::HttpsConnectorBuilder::new()
                        .with_native_roots()
                        .expect("assert: can build sheets client with native root certs")
                        .https_only()
                        .enable_http1()
                        .build(),
                ),
            authenticator,
        );
        Self {
            hub,
            send_notification,
        }
    }

    #[cfg(test)]
    pub fn new(send_notification: Sender, state: TestState) -> Self {
        Self {
            send_notification,
            state: tokio::sync::Mutex::new(state),
        }
    }

    #[cfg(not(test))]
    async fn get(
        &self,
        spreadsheet_id: &str,
        host: &str,
        service: &str,
    ) -> SheetResponse<Spreadsheet> {
        let filters: Vec<_> = [
            (METADATA_HOST_ID_KEY, host),
            (METADATA_SERVICE_KEY, service),
        ]
        .into_iter()
        .map(|(k, v)| DataFilter {
            developer_metadata_lookup: Some(DeveloperMetadataLookup {
                visibility: Some("PROJECT".to_string()),
                metadata_key: Some(k.to_string()),
                metadata_value: Some(v.to_string()),
                ..Default::default()
            }),
            ..Default::default()
        })
        .collect();
        let req = GetSpreadsheetByDataFilterRequest {
            data_filters: Some(filters),
            ..Default::default()
        };
        self
            .hub
            .spreadsheets()
            .get_by_data_filter(req, spreadsheet_id)
            .param("fields", "sheets.properties(sheetId,title,hidden,index,tabColorStyle,sheetType,gridProperties),sheets.developerMetadata")
            .doit()
            .await
    }

    #[cfg(not(test))]
    pub async fn get_sheet_data(
        &self,
        spreadsheet_id: &str,
        sheet_id: TableId,
    ) -> Result<Vec<Vec<Value>>, StorageError> {
        let req = BatchGetValuesByDataFilterRequest {
            data_filters: Some(vec![DataFilter {
                grid_range: Some(GridRange {
                    sheet_id: Some(sheet_id),
                    start_row_index: Some(1),
                    ..Default::default()
                }),
                ..Default::default()
            }]),
            major_dimension: Some("ROWS".to_string()),
            value_render_option: Some("UNFORMATTED_VALUE".to_string()),
            ..Default::default()
        };

        let result = self
            .hub
            .spreadsheets()
            .values_batch_get_by_data_filter(req, spreadsheet_id)
            .doit()
            .await;
        tracing::trace!("{:?}", result);
        let response = handle_error(self, result).await.map_err(|e| {
            tracing::error!("{:?}", e);
            e
        })?;
        Ok(match response.value_ranges {
            Some(r) => r
                .into_iter()
                .filter_map(|range| range.value_range.and_then(|r| r.values))
                .flatten()
                .collect::<Vec<Vec<Value>>>(),
            None => vec![vec![]],
        })
    }

    #[cfg(test)]
    pub async fn get_sheet_data(
        &self,
        spreadsheet_id: &str,
        sheet_id: TableId,
    ) -> Result<Vec<Vec<Value>>, StorageError> {
        let mut state = self.state.lock().await;
        state.get_sheet_data(spreadsheet_id, sheet_id).await
    }

    #[cfg(test)]
    async fn get(
        &self,
        spreadsheet_id: &str,
        _host: &str,
        _service: &str,
    ) -> SheetResponse<Spreadsheet> {
        let mut state = self.state.lock().await;
        state.get(spreadsheet_id).await
    }

    #[cfg(not(test))]
    async fn update(
        &self,
        req: BatchUpdateSpreadsheetRequest,
        spreadsheet_id: &str,
    ) -> SheetResponse {
        self.hub
            .spreadsheets()
            .batch_update(req, spreadsheet_id)
            .doit()
            .await
    }

    #[cfg(test)]
    async fn update(
        &self,
        req: BatchUpdateSpreadsheetRequest,
        spreadsheet_id: &str,
    ) -> SheetResponse {
        let mut state = self.state.lock().await;
        state.update(req, spreadsheet_id).await
    }

    #[cfg(test)]
    pub async fn delete_sheet(&self, table_id: TableId) {
        let mut state = self.state.lock().await;
        state.delete_sheet(&table_id);
    }

    pub fn sheet_row_url(&self, spreadsheet_id: &str, sheet_id: TableId, row: u32) -> String {
        format!("{GOOGLE_SHEET_BASE}{spreadsheet_id}#gid={sheet_id}&range={row}:{row}")
    }

    pub fn sheet_url(&self, spreadsheet_id: &str, sheet_id: TableId) -> String {
        format!("{GOOGLE_SHEET_BASE}{spreadsheet_id}#gid={sheet_id}")
    }

    pub fn spreadsheet_baseurl(&self, spreadsheet_id: &str) -> String {
        format!("{GOOGLE_SHEET_BASE}{spreadsheet_id}#gid=")
    }

    pub async fn sheets_filtered(
        &self,
        spreadsheet_id: &str,
        host: &str,
        service: &str,
    ) -> Result<Vec<Table>, StorageError> {
        let result = self.get(spreadsheet_id, host, service).await;

        tracing::trace!("{:?}", result);
        let response = handle_error(self, result).await.map_err(|e| {
            tracing::error!("{:?}", e);
            e
        })?;

        let tables: Vec<Table> = response
            .sheets
            .expect("assert: spreadsheet should contain sheets property even if no sheets")
            .into_iter()
            .filter_map(|t| t.try_into().ok())
            .filter(|t: &Table| t.host() == host && t.service() == service)
            .collect();

        Ok(tables)
    }

    async fn _crud_sheets(
        &self,
        spreadsheet_id: &str,
        requests: Vec<Request>,
    ) -> Result<BatchUpdateSpreadsheetResponse, StorageError> {
        // capacity for actual usage

        tracing::trace!("requests:\n{:?}", requests);

        let req = BatchUpdateSpreadsheetRequest {
            include_spreadsheet_in_response: Some(false),
            requests: Some(requests),
            response_ranges: None,
            response_include_grid_data: Some(false),
        };

        let result = self.update(req, spreadsheet_id).await;

        tracing::trace!("{:?}", result);
        handle_error(self, result).await
    }

    pub async fn crud_sheets(
        &self,
        spreadsheet_id: &str,
        requests: Vec<Request>,
    ) -> Result<(), StorageError> {
        self._crud_sheets(spreadsheet_id, requests)
            .await
            .map_err(|e| {
                tracing::error!("{:?}", e);
                e
            })?;
        Ok(())
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::google::sheet::tests::mock_sheet_with_properties;
    use crate::http::{Body, to_body};
    use google_sheets4::api::Sheet as GoogleSheet;
    use google_sheets4::api::{
        AddSheetRequest, AppendCellsRequest, BasicFilter, CreateDeveloperMetadataRequest,
        DeleteDimensionRequest, DeleteSheetRequest, DeveloperMetadata, DimensionRange, GridRange,
        Request, SetBasicFilterRequest, SetDataValidationRequest, UpdateCellsRequest,
        UpdateDeveloperMetadataRequest,
    };
    use hyper::{Response as HyperResponse, StatusCode, header};
    use std::collections::{HashMap, HashSet};
    use std::time::Duration;
    use tokio::time::sleep;

    pub struct TestState {
        sheets: HashMap<TableId, GoogleSheet>,
        sheet_titles: HashSet<String>,
        metadata: HashMap<i32, (TableId, usize)>,
        respond_with_error: Option<SheetsError>,
        basic_response_duration_millis: u64,
        basic_response_duration_millis_for_second_request: u64,
    }

    impl TestState {
        pub fn new(
            sheets: Vec<GoogleSheet>,
            respond_with_error: Option<SheetsError>,
            basic_response_duration_millis: Option<u64>,
        ) -> Self {
            Self::create(
                sheets,
                respond_with_error,
                basic_response_duration_millis,
                basic_response_duration_millis,
            )
        }

        pub fn with_response_durations(
            sheets: Vec<GoogleSheet>,
            basic_response_duration_millis: u64,
            basic_response_duration_millis_for_second_request: u64,
        ) -> Self {
            Self::create(
                sheets,
                None,
                Some(basic_response_duration_millis),
                Some(basic_response_duration_millis_for_second_request),
            )
        }

        fn create(
            sheets: Vec<GoogleSheet>,
            respond_with_error: Option<SheetsError>,
            basic_response_duration_millis: Option<u64>,
            basic_response_duration_millis_for_second_request: Option<u64>,
        ) -> Self {
            let mut sheet_titles = HashSet::with_capacity(sheets.len());
            let mut metadata = HashMap::with_capacity(sheets.len());
            let sheets: HashMap<TableId, GoogleSheet> = sheets
                .into_iter()
                .map(|s| {
                    sheet_titles.insert(
                        s.properties
                            .as_ref()
                            .unwrap()
                            .title
                            .as_ref()
                            .unwrap()
                            .to_string(),
                    );
                    let sheet_id = s.properties.as_ref().unwrap().sheet_id.unwrap();
                    for (i, m) in s
                        .developer_metadata
                        .as_ref()
                        .unwrap_or(&vec![])
                        .iter()
                        .enumerate()
                    {
                        metadata.insert(m.metadata_id.unwrap(), (sheet_id, i));
                    }
                    (sheet_id, s)
                })
                .collect();
            Self {
                sheets,
                sheet_titles,
                metadata: HashMap::new(),
                respond_with_error,
                basic_response_duration_millis: basic_response_duration_millis.unwrap_or(200),
                basic_response_duration_millis_for_second_request:
                    basic_response_duration_millis_for_second_request.unwrap_or(200),
            }
        }

        pub fn failure_response(text: String) -> SheetsError {
            SheetsError::Failure(
                HyperResponse::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .header(header::CONTENT_TYPE, "application/json; charset=UTF-8")
                    .body(to_body(text.into_bytes()))
                    .expect("test assert: test state mock can create responses from strings"),
            )
        }

        pub fn bad_response(text: String) -> SheetsError {
            SheetsError::BadRequest(serde_json::json!(text))
        }

        pub async fn get(&mut self, _: &str) -> SheetResponse<Spreadsheet> {
            let response_duration_millis = self.basic_response_duration_millis;
            self.basic_response_duration_millis =
                self.basic_response_duration_millis_for_second_request;
            sleep(Duration::from_millis(response_duration_millis)).await;

            if let Some(err) = self.respond_with_error.take() {
                return Err(err);
            }
            let mut sheets: Vec<GoogleSheet> = self.sheets.clone().into_values().collect();
            sheets.sort_unstable_by_key(|s| s.properties.as_ref().unwrap().index);

            Ok((
                HyperResponse::builder()
                    .status(StatusCode::OK)
                    .header(header::CONTENT_TYPE, "application/json; charset=UTF-8")
                    .body(Body::default())
                    .unwrap(),
                Spreadsheet {
                    data_source_schedules: None,
                    data_sources: None,
                    developer_metadata: None,
                    named_ranges: None,
                    properties: None,
                    sheets: Some(sheets),
                    spreadsheet_id: None,
                    spreadsheet_url: None,
                },
            ))
        }

        pub fn delete_sheet(&mut self, sheet_id: &TableId) -> bool {
            if let Some(sheet) = self.sheets.remove(sheet_id) {
                let properties = sheet
                    .properties
                    .expect("assert: sheet properties cannot be null");
                let title = properties
                    .title
                    .expect("assert: sheet title cannot be null");
                self.sheet_titles.remove(&title)
            } else {
                false
            }
        }

        pub async fn get_sheet_data(
            &mut self,
            _spreadsheet_id: &str,
            _sheet_id: TableId,
        ) -> Result<Vec<Vec<Value>>, StorageError> {
            Ok(vec![])
        }

        pub async fn update(
            &mut self,
            req: BatchUpdateSpreadsheetRequest,
            _: &str,
        ) -> SheetResponse<BatchUpdateSpreadsheetResponse> {
            let requests = req
                .requests
                .expect("test assert: batch update must have requests");
            sleep(Duration::from_millis(self.basic_response_duration_millis)).await;
            if let Some(err) = self.respond_with_error.take() {
                return Err(err);
            }

            for r in requests.into_iter() {
                match r {
                    Request {
                        add_sheet:
                            Some(AddSheetRequest {
                                properties: Some(mut properties),
                            }),
                        ..
                    } => {
                        let sheet_id = properties
                            .sheet_id
                            .expect("assert: goral creates sheets with sheet_id");
                        let title = properties
                            .title
                            .as_ref()
                            .expect("assert: goral creates sheets with title");
                        if self.sheet_titles.contains(title) {
                            return Err(Self::bad_response(format!(
                                "sheet with title {title} already exists!"
                            )));
                        }
                        self.sheet_titles.insert(title.to_string());
                        properties.index = Some(i32::try_from(self.sheets.len()).unwrap());
                        // goral creates GRID sheets
                        // we decrease row count here for correct counting
                        // for append cells requests
                        // one row is empty for first row to be frozen
                        let current_row_count = properties
                            .grid_properties
                            .as_ref()
                            .unwrap()
                            .row_count
                            .unwrap();
                        properties
                            .grid_properties
                            .as_mut()
                            .expect("assert: goral creates grid sheets")
                            .row_count = Some(current_row_count);
                        self.sheets
                            .insert(sheet_id, mock_sheet_with_properties(properties));
                    }

                    Request {
                        append_cells:
                            Some(AppendCellsRequest {
                                sheet_id: Some(sheet_id),
                                rows: Some(rows),
                                ..
                            }),
                        ..
                    } => {
                        if let Some(sheet) = self.sheets.get_mut(&sheet_id) {
                            let grid_properties = sheet
                                .properties
                                .as_mut()
                                .expect("assert: goral creates sheets with properties")
                                .grid_properties
                                .as_mut()
                                .expect("assert: goral creates grid sheets with grid_properties");
                            if let Some(row_count) = grid_properties.row_count {
                                grid_properties.row_count =
                                    Some(row_count + (i32::try_from(rows.len()).unwrap()));
                            } else {
                                return Err(Self::bad_response(
                                    "cannot append cells to a non-grid sheet!".to_string(),
                                ));
                            }
                        } else {
                            return Err(Self::bad_response(format!(
                                "sheet with id {sheet_id} not found to append cells to!"
                            )));
                        }
                    }

                    Request {
                        update_cells:
                            Some(UpdateCellsRequest {
                                range:
                                    Some(GridRange {
                                        sheet_id: Some(sheet_id),
                                        ..
                                    }),
                                rows: Some(rows_to_add),
                                ..
                            }),
                        ..
                    } => {
                        // we don't update row counters
                        // as this request follows AddSheet which
                        // already increases
                        let sheet = self
                            .sheets
                            .get(&sheet_id)
                            .expect("assert: the sheet doesn't exist");
                        let row_count: usize = sheet
                            .properties
                            .as_ref()
                            .expect("assert: goral creates sheets with properties")
                            .grid_properties
                            .as_ref()
                            .expect("assert: goral creates grid sheets with grid_properties")
                            .row_count
                            .expect("assert: grid properties has row count")
                            .try_into()
                            .expect("assert: row count is non-negative");
                        assert_eq!(rows_to_add.len(), row_count);
                        if !self.sheets.contains_key(&sheet_id) {
                            return Err(Self::bad_response(format!(
                                "sheet with id {sheet_id} not found to update cells!"
                            )));
                        }
                    }

                    Request {
                        create_developer_metadata:
                            Some(CreateDeveloperMetadataRequest {
                                developer_metadata: Some(metadata),
                                ..
                            }),
                        ..
                    } => {
                        let sheet_id = metadata
                            .location
                            .as_ref()
                            .expect("assert: goral sets location for new metadata")
                            .sheet_id
                            .unwrap();
                        if let Some(sheet) = self.sheets.get_mut(&sheet_id) {
                            if let Some(m) = sheet.developer_metadata.as_mut() {
                                self.metadata.insert(
                                    metadata
                                        .metadata_id
                                        .expect("assert: goral sets metadata_id for new metadata"),
                                    (sheet_id, m.len()),
                                );
                                m.push(metadata);
                            } else {
                                self.metadata.insert(
                                    metadata
                                        .metadata_id
                                        .expect("assert: goral sets metadata_id for new metadata"),
                                    (sheet_id, 0),
                                );
                                sheet.developer_metadata = Some(vec![metadata]);
                            }
                        } else {
                            return Err(Self::bad_response(format!(
                                "sheet with id {sheet_id} not found to create metadata for!"
                            )));
                        }
                    }

                    Request {
                        set_basic_filter:
                            Some(SetBasicFilterRequest {
                                filter:
                                    Some(BasicFilter {
                                        range:
                                            Some(GridRange {
                                                sheet_id: Some(sheet_id),
                                                ..
                                            }),
                                        ..
                                    }),
                                ..
                            }),
                        ..
                    } => {
                        if !self.sheets.contains_key(&sheet_id) {
                            return Err(Self::bad_response(format!(
                                "sheet with id {sheet_id} not found to add basic filter to!"
                            )));
                        }
                    }

                    Request {
                        update_developer_metadata:
                            Some(UpdateDeveloperMetadataRequest {
                                developer_metadata: Some(DeveloperMetadata { metadata_value, .. }),
                                data_filters: Some(data_filters),
                                ..
                            }),
                        ..
                    } => {
                        let metadata_id = data_filters[0]
                            .developer_metadata_lookup
                            .as_ref()
                            .unwrap()
                            .metadata_id
                            .unwrap();
                        if let Some((sheet_id, index)) = self.metadata.get(&metadata_id) {
                            let sheet = self.sheets.get_mut(sheet_id).unwrap();
                            let metadatas = sheet.developer_metadata.as_mut().unwrap();
                            metadatas[*index].metadata_value = metadata_value;
                        } else {
                            return Err(Self::bad_response(format!(
                                "metadata with id {metadata_id} not found!"
                            )));
                        }
                    }

                    Request {
                        set_data_validation:
                            Some(SetDataValidationRequest {
                                range:
                                    Some(GridRange {
                                        sheet_id: Some(sheet_id),
                                        ..
                                    }),
                                ..
                            }),
                        ..
                    } => {
                        if !self.sheets.contains_key(&sheet_id) {
                            return Err(Self::bad_response(format!(
                                "sheet with id {sheet_id} not found to set data validation on!"
                            )));
                        }
                    }

                    Request {
                        delete_dimension:
                            Some(DeleteDimensionRequest {
                                range:
                                    Some(DimensionRange {
                                        sheet_id: Some(sheet_id),
                                        start_index: Some(start_row_index),
                                        end_index,
                                        dimension: Some(dimension),
                                        ..
                                    }),
                            }),
                        ..
                    } => {
                        assert_eq!(dimension, "ROWS");
                        if let Some(sheet) = self.sheets.get_mut(&sheet_id) {
                            let grid_properties = sheet
                                .properties
                                .as_mut()
                                .expect("assert: goral creates sheets with properties")
                                .grid_properties
                                .as_mut()
                                .expect("assert: goral creates grid sheets with grid_properties");
                            if let Some(row_count) = grid_properties.row_count {
                                if start_row_index >= row_count {
                                    return Err(Self::bad_response(format!(
                                        "Cannot delete a row that doesn't exist. Tried to delete row index {start_row_index} but there are only {row_count} rows."
                                    )));
                                }
                                if let Some(end_index) = end_index {
                                    grid_properties.row_count =
                                        Some(row_count - end_index + start_row_index);
                                } else {
                                    grid_properties.row_count = Some(start_row_index);
                                }
                            } else {
                                return Err(Self::bad_response(
                                    "cannot delete cells from a non-grid sheet!".to_string(),
                                ));
                            }
                        } else {
                            return Err(Self::bad_response(format!(
                                "sheet with id {sheet_id} not found to delete cells from!"
                            )));
                        }
                    }

                    Request {
                        delete_sheet:
                            Some(DeleteSheetRequest {
                                sheet_id: Some(sheet_id),
                            }),
                        ..
                    } => {
                        if !self.delete_sheet(&sheet_id) {
                            return Err(Self::bad_response(format!(
                                "sheet with id {sheet_id} not found to delete!"
                            )));
                        }
                    }

                    _ => panic!("test assert: unhandled request {r:?}"),
                }
            }

            Ok((
                HyperResponse::builder()
                    .status(StatusCode::OK)
                    .header(header::CONTENT_TYPE, "application/json; charset=UTF-8")
                    .body(Body::default())
                    .unwrap(),
                BatchUpdateSpreadsheetResponse {
                    ..Default::default()
                },
            ))
        }
    }
}
