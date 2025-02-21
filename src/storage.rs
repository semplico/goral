use crate::google::datavalue::Datarow;
use crate::google::sheet::{
    CleanupSheet, Rows, Sheet, SheetId, SheetType, TabColorRGB, UpdateSheet, VirtualSheet,
};
use crate::google::spreadsheet::GOOGLE_SPREADSHEET_MAXIMUM_CELLS;
use crate::google::{Metadata, SpreadsheetAPI};
use crate::notifications::Sender;
use crate::rules::{rules_dropdowns, Rule, RULES_LOG_NAME};
use crate::{get_service_tab_color, jitter_duration, HOST_ID_CHARS_LIMIT};
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

const METADATA_SERVICE_KEY: &str = "service";
const METADATA_HOST_ID_KEY: &str = "host";
const METADATA_LOG_NAME: &str = "name";
const METADATA_CREATED_AT: &str = "created_at";
const METADATA_UPDATED_AT: &str = "updated_at";
const METADATA_ROW_COUNT: &str = "rows";
const METADATA_KEYS: &str = "keys";
const KEYS_DELIMITER: &str = "~^~";

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

pub struct AppendableLog {
    storage: Arc<Storage>,
    service: String,
    spreadsheet_id: String,
    tab_color_rgb: TabColorRGB,
    rules_sheet_id: Option<SheetId>,
    messenger: Option<Sender>,
    truncate_at: f32,
    truncate_warning_is_sent: bool,
}

impl AppendableLog {
    pub fn new(
        storage: Arc<Storage>,
        spreadsheet_id: String,
        service: String,
        messenger: Option<Sender>,
        truncate_at: f32,
    ) -> Self {
        let tab_color_rgb = get_service_tab_color(&service);
        Self {
            storage,
            spreadsheet_id,
            service,
            tab_color_rgb,
            rules_sheet_id: None,
            messenger,
            truncate_at,
            truncate_warning_is_sent: false,
        }
    }

    async fn fetch_sheets(&self) -> Result<HashMap<SheetId, (Sheet, Vec<String>)>, StorageError> {
        let basic_metadata = Metadata::new(vec![
            (METADATA_HOST_ID_KEY, self.storage.host_id.to_string()),
            (METADATA_SERVICE_KEY, self.service.to_string()),
        ]);

        let existing_service_sheets = self
            .storage
            .google
            .sheets_filtered_by_metadata(&self.spreadsheet_id, &basic_metadata)
            .await?;

        Ok(existing_service_sheets
            .into_iter()
            .map(|s| {
                let keys: Vec<String> = s
                    .meta_value(METADATA_KEYS)
                    .expect("assert: meta key is set at sheet creation")
                    .split(KEYS_DELIMITER)
                    .map(|k| k.to_string())
                    .collect();
                (s.sheet_id(), (s, keys))
            })
            .collect())
    }

    pub async fn healthcheck(&self) -> Result<(), StorageError> {
        self.fetch_sheets().await?;
        Ok(())
    }

    pub async fn append(&mut self, datarows: Vec<Datarow>) -> Result<(), StorageError> {
        self.core_append(datarows, Some(Duration::from_secs(32)))
            .await
    }

    pub async fn append_no_retry(
        &mut self,
        mut datarows: Vec<Datarow>,
    ) -> Result<Vec<String>, StorageError> {
        let sheet_ids = datarows
            .iter_mut()
            .map(|d| {
                let sheet_id = d.sheet_id(&self.storage.host_id, &self.service);
                self.sheet_url(sheet_id)
            })
            .collect();
        self.core_append(datarows, None).await?;
        Ok(sheet_ids)
    }

    // https://developers.google.com/sheets/api/limits#example-algorithm
    async fn timed_fetch_sheets(
        &self,
        maximum_backoff: Duration,
    ) -> Result<HashMap<SheetId, (Sheet, Vec<String>)>, StorageError> {
        let mut total_time = Duration::from_millis(0);
        let mut wait = Duration::from_millis(2);
        let mut retry = 0;
        let max_backoff = tokio::time::sleep(maximum_backoff);
        tokio::pin!(max_backoff);
        let mut last_retry_error = format!("timeout {maximum_backoff:?}");
        while total_time < maximum_backoff {
            tokio::select! {
                _ = &mut max_backoff => {break;}
                res = self.fetch_sheets() => {
                    if let Err(StorageError::Retriable(e)) = res {
                        tracing::error!("error {:?} for service `{}` retrying #{}", e, self.service, retry);
                        last_retry_error = e;
                    } else {
                        return res;
                    }
                }
            }
            let jittered = wait + jitter_duration() / 2u32.pow(4);
            retry += 1;
            tracing::warn!(
                "waiting {:?} for retry {} for service `{}`",
                jittered,
                retry,
                self.service
            );
            tokio::time::sleep(jittered).await;
            total_time += jittered;
            wait *= 2;
        }
        Err(StorageError::RetryTimeout((
            maximum_backoff,
            retry,
            last_retry_error,
        )))
    }

    // for newly created log sheet its headers order is determined by its first datarow. Fields for other datarows for the same sheet are sorted accordingly.
    async fn core_append(
        &mut self,
        datarows: Vec<Datarow>,
        retry_limit: Option<Duration>,
    ) -> Result<(), StorageError> {
        if datarows.is_empty() {
            return Ok(());
        }

        let existing_sheets = if let Some(retry_limit) = retry_limit {
            // We do not retry sheet `crud` as it goes after
            // `fetch_sheets` which is retriable and should
            // either fix an error or fail.
            // Retrying `crud` is not idempotent and
            // would require cloning all sheets at every retry attempt
            self.timed_fetch_sheets(retry_limit).await?
        } else {
            self.fetch_sheets().await?
        };

        tracing::trace!("existing sheets:\n{:?}", existing_sheets);

        let mut rows_count: HashMap<SheetId, i32> = existing_sheets
            .iter()
            .map(|(&sheet_id, (sheet, _))| {
                (
                    sheet_id,
                    sheet
                        .meta_value(METADATA_ROW_COUNT)
                        .expect("assert: a managed grid sheet has rows metadata")
                        .parse()
                        .expect("assert: rows metadata is a signed integer"),
                )
            })
            .collect();

        // Check for sheet type to be grid
        let mut sheets_to_create: HashMap<SheetId, (VirtualSheet, Vec<String>)> = HashMap::new();
        let mut data_to_append: HashMap<SheetId, Rows> = HashMap::new();
        let mut sheets_to_update: HashMap<SheetId, UpdateSheet> = HashMap::new();

        let timestamp = Utc::now();

        for mut datarow in datarows.into_iter() {
            let sheet_id = datarow.sheet_id(&self.storage.host_id, &self.service);

            if self.rules_sheet_id.is_none() && datarow.log_name() == RULES_LOG_NAME {
                self.rules_sheet_id = Some(sheet_id);
            }

            if let Some((sheet, keys)) = existing_sheets.get(&sheet_id) {
                if datarow.log_name() == RULES_LOG_NAME {
                    // we don't append for existing rules sheets
                    continue;
                }
                // for existing sheets we only change updated_at
                // so it is enough to have one update per sheet
                sheets_to_update
                    .entry(sheet.sheet_id())
                    .or_insert(UpdateSheet::new(
                        sheet.sheet_id(),
                        Metadata::new(vec![(METADATA_UPDATED_AT, timestamp.to_rfc3339())]),
                    ));
                datarow.sort_by_keys(keys);
                data_to_append
                    .entry(sheet.sheet_id())
                    .or_insert(Rows::new(
                        sheet.sheet_id(),
                        sheet.row_count().expect("assert: managed sheets are grid"),
                    ))
                    .push(datarow.into());
            } else if let Some((sheet, keys)) = sheets_to_create.get_mut(&sheet_id) {
                // align data according to headers of sheet to be created
                datarow.sort_by_keys(keys);
                data_to_append
                    .entry(sheet.sheet_id())
                    .or_insert(Rows::new(
                        sheet.sheet_id(),
                        sheet.row_count().expect(
                            "assert: sheets created by services are grids which have row count",
                        ),
                    ))
                    .push(datarow.into());
            } else {
                let sheet_id = datarow.sheet_id(&self.storage.host_id, &self.service);
                let title = prepare_sheet_title(
                    &self.storage.host_id,
                    &self.service,
                    datarow.log_name(),
                    &timestamp,
                    &sheet_id,
                );
                let timestamp = timestamp.to_rfc3339();
                let headers = datarow.headers();
                let metadata = vec![
                    (METADATA_HOST_ID_KEY, self.storage.host_id.to_string()),
                    (METADATA_SERVICE_KEY, self.service.to_string()),
                    (METADATA_LOG_NAME, datarow.log_name().clone()),
                    (METADATA_KEYS, datarow.keys().join(KEYS_DELIMITER)),
                    (METADATA_CREATED_AT, timestamp.clone()),
                    (METADATA_UPDATED_AT, timestamp.clone()),
                ];
                let mut sheet = VirtualSheet::new_grid(
                    sheet_id,
                    title,
                    headers,
                    Metadata::new(metadata),
                    self.tab_color_rgb,
                );
                if datarow.log_name() == RULES_LOG_NAME {
                    sheet = sheet.with_dropdowns(rules_dropdowns());
                }
                let row_count = sheet.row_count().expect("assert: grid sheet has row count");
                rows_count.insert(sheet_id, 1); // one for header row
                sheets_to_create.insert(
                    sheet_id,
                    (
                        sheet,
                        datarow.keys().iter().map(|k| k.to_string()).collect(),
                    ),
                );
                data_to_append
                    .entry(sheet_id)
                    .or_insert(Rows::new(sheet_id, row_count))
                    .push(datarow.into());
            }
        }

        let mut sheets_to_add: Vec<VirtualSheet> =
            sheets_to_create.into_iter().map(|(_, (s, _))| s).collect();
        let mut sheets_to_update: Vec<UpdateSheet> = sheets_to_update.into_values().collect();
        let truncate_requests = self.prepare_truncate_requests(
            &existing_sheets,
            &sheets_to_add,
            &data_to_append,
            self.truncate_at,
            &mut rows_count,
        );
        self.update_rows_metadata(&rows_count, &mut sheets_to_update, &mut sheets_to_add);
        let data: Vec<Rows> = data_to_append.into_values().collect();

        tracing::trace!("truncate_requests:\n{:?}", truncate_requests);
        tracing::trace!("sheets_to_update:\n{:?}", sheets_to_update);
        tracing::trace!("sheets_to_add:\n{:?}", sheets_to_add);
        tracing::trace!("data:\n{:?}", data);

        if sheets_to_add.is_empty() && sheets_to_update.is_empty() {
            return Ok(());
        }

        let truncation = !truncate_requests.is_empty();
        self.storage
            .google
            .crud_sheets(
                &self.spreadsheet_id,
                truncate_requests,
                sheets_to_update,
                sheets_to_add,
                data,
            )
            .await?;

        if truncation {
            self.truncate_warning_is_sent = false;
        }
        Ok(())
    }

    fn update_rows_metadata(
        &self,
        rows_count: &HashMap<SheetId, i32>,
        updates: &mut Vec<UpdateSheet>,
        sheets: &mut Vec<VirtualSheet>,
    ) {
        for update in updates {
            let rows = rows_count
                .get(&update.sheet_id())
                .expect("assert: rows counters initialized for existing sheets");
            update
                .metadata_mut()
                .insert(METADATA_ROW_COUNT.to_string(), rows.to_string())
        }

        for sheet in sheets {
            let rows = rows_count
                .get(&sheet.sheet_id())
                .expect("assert: rows counters initialized for new sheets");
            sheet
                .metadata_mut()
                .insert(METADATA_ROW_COUNT.to_string(), rows.to_string());
        }
    }

    #[allow(clippy::cast_precision_loss)]
    #[allow(clippy::cast_possible_truncation)]
    fn prepare_truncate_requests(
        &mut self,
        existing_service_sheets: &HashMap<SheetId, (Sheet, Vec<String>)>,
        sheets_to_create: &[VirtualSheet],
        data_to_append: &HashMap<SheetId, Rows>,
        limit: f32,
        rows_count: &mut HashMap<SheetId, i32>,
    ) -> Vec<CleanupSheet> {
        let limit = f64::from(limit); // SAFE as limit is supposed to be a % so under 100.0
        for (sheet_id, rows) in data_to_append {
            *rows_count
                .get_mut(sheet_id)
                .expect("assert: rows counters initialized for existing and new sheets") +=
                rows.new_rows_count()
        }
        tracing::debug!("rows counters:\n{:?}", rows_count);

        let cells_used_by_service: i32 = existing_service_sheets
            .values()
            .filter_map(|(s, _)| (s.sheet_type() == SheetType::Grid).then_some(s))
            .chain(
                sheets_to_create
                    .iter()
                    .filter_map(|vs| (vs.sheet_type() == SheetType::Grid).then_some(vs.sheet())),
            )
            .map(|s| {
                let used_rows = rows_count
                    .get(&s.sheet_id())
                    .expect("assert: rows counters were filled from existing sheets");
                used_rows * s.column_count().expect("assert: grid sheet has columns")
            })
            .sum();
        let usage =
            100.0 * f64::from(cells_used_by_service) / f64::from(GOOGLE_SPREADSHEET_MAXIMUM_CELLS);
        tracing::debug!(
            "cells used by service `{}`: {}, usage: {}%",
            self.service,
            cells_used_by_service,
            usage
        );

        if usage < limit {
            if usage > 0.8 * limit && !self.truncate_warning_is_sent {
                let url = self.spreadsheet_baseurl();
                let message = format!("current [spreadsheet]({url}) usage `{usage:.2}%` for service `{}` is approaching a limit `{limit}%`, the data will be truncated, copy it if needed or consider using a separate spreadsheet for this service with a higher [storage quota](https://maksimryndin.github.io/goral/services.html#storage-quota)", self.service);
                tracing::warn!("{}", message);
                if let Some(messenger) = self.messenger.as_ref() {
                    messenger.try_warn(message);
                    self.truncate_warning_is_sent = true;
                }
            }
            return vec![];
        }
        // remove surplus and 30% of the limit
        let cells_to_delete =
            (usage - 0.7 * limit) * f64::from(GOOGLE_SPREADSHEET_MAXIMUM_CELLS) / 100.0;

        let message = format!(
            "sheets managed by service `{}` with usage `{usage:.2}%` are truncated",
            self.service
        );
        tracing::info!("{}", message);
        if let Some(messenger) = self.messenger.as_ref() {
            messenger.try_info(message);
        }

        let usages = existing_service_sheets
            .values()
            .filter_map(|(s, _)| (s.sheet_type() == SheetType::Grid).then_some(s))
            .chain(
                sheets_to_create
                    .iter()
                    .filter_map(|vs| (vs.sheet_type() == SheetType::Grid).then_some(vs.sheet())),
            )
            .fold(HashMap::new(), |mut state, s| {
                let log_name = s
                    .meta_value(METADATA_LOG_NAME)
                    .expect("assert: service sheet has `name` metadata");
                // rules sheets are not included
                if log_name == RULES_LOG_NAME {
                    return state;
                }
                let used_rows = rows_count
                    .get(&s.sheet_id())
                    .expect("assert: rows counters were filled from existing sheets");
                let sheet_usage = SheetUsage {
                    sheet_id: s.sheet_id(),
                    row_count: *used_rows,
                    column_count: s.column_count().expect("assert: grid sheet has columns"),
                    updated_at: DateTime::parse_from_rfc3339(
                        s.meta_value(METADATA_UPDATED_AT)
                            .expect("assert: service sheet has `updated_at` metadata"),
                    )
                    .expect("assert: `updated_at` metadata is serialized to rfc3339")
                    .into(),
                };
                let sheet_cells = sheet_usage.row_count * sheet_usage.column_count;
                let stat = state.entry(log_name).or_insert((0, 0.0, vec![]));
                stat.0 += sheet_cells;
                stat.1 = f64::from(stat.0) / f64::from(cells_used_by_service); // share of usage by the log name
                stat.2.push(sheet_usage);
                state
            });

        tracing::debug!("usages:\n{:#?}", usages);
        let requests = usages
            .into_iter()
            .flat_map(|(_, (_, log_cells_usage, mut sheets))| {
                let cells_to_delete_for_log: f64 = log_cells_usage * cells_to_delete; // SAFE as the upper bound for cells for Sheets is within i32::MAX
                assert!(
                    cells_to_delete_for_log < f64::from(i32::MAX),
                    "assert: the upper bound for cells for Sheets is within i32::MAX"
                );
                let mut cells_to_delete_for_log = cells_to_delete_for_log as i32;
                sheets.sort_unstable_by_key(|s| s.updated_at);
                let mut requests = Vec::with_capacity(sheets.len());
                for sheet in sheets {
                    if cells_to_delete_for_log <= 0 {
                        break;
                    }
                    let cells = sheet.row_count * sheet.column_count;
                    if cells <= cells_to_delete_for_log {
                        // remove the whole sheet
                        requests.push(CleanupSheet::delete(sheet.sheet_id));
                        cells_to_delete_for_log -= cells;
                        rows_count.remove(&sheet.sheet_id);
                    } else {
                        // remove some rows
                        // TODO remove also blank rows after the data
                        let rows = sheet
                            .row_count
                            .min(cells_to_delete_for_log / sheet.column_count + 1);
                        requests.push(CleanupSheet::truncate(sheet.sheet_id, rows));
                        *rows_count
                            .get_mut(&sheet.sheet_id)
                            .expect("assert: rows counters were filled from existing sheets") -=
                            rows;
                        cells_to_delete_for_log = 0;
                    }
                }
                requests
            })
            .collect();
        tracing::debug!("rows counters after truncation:\n{:?}", rows_count);
        requests
    }

    pub fn host_id(&self) -> &str {
        &self.storage.host_id
    }

    pub fn sheet_url(&self, sheet_id: SheetId) -> String {
        self.storage
            .google
            .sheet_url(&self.spreadsheet_id, sheet_id)
    }

    pub fn spreadsheet_baseurl(&self) -> String {
        self.storage
            .google
            .spreadsheet_baseurl(&self.spreadsheet_id)
    }

    pub async fn get_rules(&self) -> Result<Vec<Rule>, StorageError> {
        let timeout = Duration::from_millis(2000);
        tokio::select! {
            _ = tokio::time::sleep(timeout) => Err(StorageError::Timeout(timeout)),
            res = self
                .storage
                .google
                .get_sheet_data(
                    &self.spreadsheet_id,
                    self.rules_sheet_id.expect("assert: rules sheet id is saved at the start of the service at the first append")
                ) => {
                    let data = res?;
                    Ok(data.into_iter()
                        .filter_map(|row| Rule::try_from_values(row, self.messenger.as_ref()))
                        .collect()
                    )
            }
        }
    }
}

#[derive(Debug)]
struct SheetUsage {
    sheet_id: SheetId,
    row_count: i32,
    column_count: i32,
    updated_at: DateTime<Utc>,
}

macro_rules! sheet_name_jitter {
    ($sheet_id:expr) => {
        // we need 8 lowest significant bits
        (*$sheet_id as u8) >> 3
    };
}

#[allow(clippy::cast_possible_truncation)]
#[allow(clippy::cast_sign_loss)]
fn prepare_sheet_title(
    host_id: &str,
    service: &str,
    log_name: &str,
    timestamp: &DateTime<Utc>,
    sheet_id: &SheetId,
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
mod tests {
    use super::*;
    use crate::google::datavalue::{Datarow, Datavalue};
    use crate::google::sheet::tests::mock_ordinary_google_sheet;
    use crate::google::spreadsheet::tests::TestState;
    use crate::notifications::{Notification, Sender};
    use crate::services::general::GENERAL_SERVICE_NAME;
    use crate::tests::TEST_HOST_ID;
    use chrono::NaiveDate;
    use google_sheets4::Error;
    use tokio::sync::mpsc;

    impl Storage {
        pub fn google(&self) -> &SpreadsheetAPI {
            &self.google
        }
    }

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

    #[tokio::test]
    async fn basic_append_flow() {
        let (tx, _) = mpsc::channel(1);
        let tx = Sender::new(tx, GENERAL_SERVICE_NAME);
        let sheets_api = SpreadsheetAPI::new(
            tx.clone(),
            TestState::new(vec![mock_ordinary_google_sheet("some sheet")], None, None),
        );
        let storage = Arc::new(Storage::new(TEST_HOST_ID.to_string(), sheets_api));
        let mut log = AppendableLog::new(
            storage,
            "spreadsheet1".to_string(),
            GENERAL_SERVICE_NAME.to_string(),
            Some(tx.clone()),
            100.0,
        );

        let timestamp = NaiveDate::from_ymd_opt(2023, 10, 19)
            .expect("test assert: static date")
            .and_hms_opt(0, 0, 0)
            .expect("test assert: static time");

        // adding two new log_name-keys rows
        let datarows = vec![
            Datarow::new(
                "log_name1".to_string(),
                timestamp,
                vec![
                    ("key11".to_string(), Datavalue::HeatmapPercent(3_f64)),
                    ("key12".to_string(), Datavalue::Size(400_u64)),
                ],
            ),
            Datarow::new(
                "log_name2".to_string(),
                timestamp,
                vec![
                    ("key21".to_string(), Datavalue::HeatmapPercent(3_f64)),
                    ("key22".to_string(), Datavalue::Size(400_u64)),
                ],
            ),
        ];

        log.append(datarows).await.unwrap();

        // for the sheet order we rely on the indexing
        let all_sheets = log
            .storage
            .google
            .sheets_filtered_by_metadata(&log.spreadsheet_id, &Metadata::new(vec![]))
            .await
            .unwrap();
        assert_eq!(
            all_sheets.len(),
            3,
            "`some sheet` already exists, `log_name1` and `log_name2` sheets have been created"
        );
        assert!(
            all_sheets[1].title().contains("log_name1")
                || all_sheets[2].title().contains("log_name1")
        );
        assert_eq!(
            all_sheets[1].row_count(),
            Some(2),
            "`log_name..` contains header row and one row of data"
        );
        assert!(
            all_sheets[1].title().contains("log_name2")
                || all_sheets[2].title().contains("log_name2")
        );
        assert_eq!(
            all_sheets[2].row_count(),
            Some(2),
            "`log_name..` contains header row and one row of data"
        );

        // adding two existing log_name-keys rows
        // but for one the order of keys is different - shouldn't make any difference
        // and new one combination
        // and new combination with the same log_name, but updated keys
        // two new sheets should be created
        let datarows = vec![
            Datarow::new(
                "log_name2".to_string(),
                timestamp,
                vec![
                    ("key21".to_string(), Datavalue::HeatmapPercent(3_f64)),
                    ("key23".to_string(), Datavalue::Size(400_u64)),
                ],
            ),
            Datarow::new(
                "log_name1".to_string(),
                timestamp,
                vec![
                    ("key12".to_string(), Datavalue::Size(400_u64)),
                    ("key11".to_string(), Datavalue::HeatmapPercent(3_f64)),
                ],
            ),
            Datarow::new(
                "log_name2".to_string(),
                timestamp,
                vec![
                    ("key21".to_string(), Datavalue::HeatmapPercent(3_f64)),
                    ("key22".to_string(), Datavalue::Size(400_u64)),
                ],
            ),
            Datarow::new(
                "log_name3".to_string(),
                timestamp,
                vec![
                    ("key31".to_string(), Datavalue::HeatmapPercent(3_f64)),
                    ("key32".to_string(), Datavalue::Size(400_u64)),
                ],
            ),
        ];

        log.append(datarows).await.unwrap();
        let all_sheets = log
            .storage
            .google
            .sheets_filtered_by_metadata(&log.spreadsheet_id, &Metadata::new(vec![]))
            .await
            .unwrap();
        assert_eq!(all_sheets.len(), 5, "`some sheet`, `log_name1` and `log_name2` already exist, `log_name2` with different keys and `log_name3` sheets have been created");

        assert!(
            all_sheets[1].title().contains("log_name1")
                || all_sheets[2].title().contains("log_name1")
        );
        assert_eq!(
            all_sheets[1].row_count(),
            Some(3),
            "`log_name1` and `log_name2` contain header row and two rows of data"
        );
        assert!(
            all_sheets[1].title().contains("log_name2")
                || all_sheets[2].title().contains("log_name2")
        );
        assert_eq!(
            all_sheets[2].row_count(),
            Some(3),
            "`log_name1` and `log_name2` contain header row and two rows of data"
        );

        assert!(
            all_sheets[3].title().contains("log_name2")
                || all_sheets[4].title().contains("log_name2")
        );
        assert_eq!(all_sheets[3].row_count(), Some(2), "`log_name2` with different keys and `log_name3` contain header row and one row of data");
        assert!(
            all_sheets[3].title().contains("log_name3")
                || all_sheets[4].title().contains("log_name3")
        );
        assert_eq!(all_sheets[4].row_count(), Some(2), "`log_name2` with different keys and `log_name3` contain header row and one row of data");

        assert_eq!(
            all_sheets[3].meta_value(METADATA_SERVICE_KEY),
            Some(&GENERAL_SERVICE_NAME.to_string())
        );
        assert_eq!(
            all_sheets[3].meta_value(METADATA_HOST_ID_KEY),
            Some(&TEST_HOST_ID.to_string())
        );

        assert!(all_sheets[3]
            .meta_value(METADATA_LOG_NAME)
            .unwrap()
            .starts_with("log_name"));
        assert_ne!(
            all_sheets[1].meta_value(METADATA_CREATED_AT),
            all_sheets[1].meta_value(METADATA_UPDATED_AT),
            "first sheets should be updated"
        );
        assert_eq!(
            all_sheets[3].meta_value(METADATA_CREATED_AT),
            all_sheets[3].meta_value(METADATA_UPDATED_AT),
            "new sheets have the same created and updated timestamps"
        );
    }

    #[tokio::test]
    async fn append_retry() {
        let (tx, _) = mpsc::channel(1);
        let tx = Sender::new(tx, GENERAL_SERVICE_NAME);
        let sheets_api = SpreadsheetAPI::new(
            tx.clone(),
            TestState::new(
                vec![mock_ordinary_google_sheet("some sheet")],
                Some(TestState::failure_response("error to retry".to_string())),
                None,
            ),
        );
        let storage = Arc::new(Storage::new(TEST_HOST_ID.to_string(), sheets_api));
        let mut log = AppendableLog::new(
            storage,
            "spreadsheet1".to_string(),
            GENERAL_SERVICE_NAME.to_string(),
            Some(tx.clone()),
            100.0,
        );

        let timestamp = NaiveDate::from_ymd_opt(2023, 10, 19)
            .expect("test assert: static date")
            .and_hms_opt(0, 0, 0)
            .expect("test assert: static time");

        // adding two new log_name-keys rows
        let datarows = vec![
            Datarow::new(
                "log_name1".to_string(),
                timestamp,
                vec![
                    ("key11".to_string(), Datavalue::HeatmapPercent(3_f64)),
                    ("key12".to_string(), Datavalue::Size(400_u64)),
                ],
            ),
            Datarow::new(
                "log_name2".to_string(),
                timestamp,
                vec![
                    ("key21".to_string(), Datavalue::HeatmapPercent(3_f64)),
                    ("key22".to_string(), Datavalue::Size(400_u64)),
                ],
            ),
        ];

        log.append(datarows).await.unwrap();
    }

    #[tokio::test]
    #[should_panic(expected = "The application's API key was not found in the configuration")]
    async fn append_fatal_error() {
        let (tx, mut rx) = mpsc::channel(1);
        let tx = Sender::new(tx, GENERAL_SERVICE_NAME);
        let sheets_api = SpreadsheetAPI::new(
            tx.clone(),
            TestState::new(
                vec![mock_ordinary_google_sheet("some sheet")],
                Some(Error::MissingAPIKey),
                None,
            ),
        );
        let storage = Arc::new(Storage::new(TEST_HOST_ID.to_string(), sheets_api));
        let mut log = AppendableLog::new(
            storage,
            "spreadsheet1".to_string(),
            GENERAL_SERVICE_NAME.to_string(),
            Some(tx.clone()),
            100.0,
        );

        let timestamp = NaiveDate::from_ymd_opt(2023, 10, 19)
            .expect("test assert: static date")
            .and_hms_opt(0, 0, 0)
            .expect("test assert: static time");

        // adding two new log_name-keys rows
        let datarows = vec![
            Datarow::new(
                "log_name1".to_string(),
                timestamp,
                vec![
                    ("key11".to_string(), Datavalue::HeatmapPercent(3_f64)),
                    ("key12".to_string(), Datavalue::Size(400_u64)),
                ],
            ),
            Datarow::new(
                "log_name2".to_string(),
                timestamp,
                vec![
                    ("key21".to_string(), Datavalue::HeatmapPercent(3_f64)),
                    ("key22".to_string(), Datavalue::Size(400_u64)),
                ],
            ),
        ];

        let handle = tokio::task::spawn(async move {
            assert!(
                rx.recv().await.is_some(),
                "notification is sent for nonrecoverable error"
            );
        });

        log.append(datarows).await.unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn append_request_timeout() {
        let (tx, _) = mpsc::channel(1);
        let tx = Sender::new(tx, GENERAL_SERVICE_NAME);
        let sheets_api = SpreadsheetAPI::new(
            tx.clone(),
            TestState::with_response_durations(
                vec![mock_ordinary_google_sheet("some sheet")],
                150,
                50,
            ),
        );
        let storage = Arc::new(Storage::new(TEST_HOST_ID.to_string(), sheets_api));
        let mut log = AppendableLog::new(
            storage,
            "spreadsheet1".to_string(),
            GENERAL_SERVICE_NAME.to_string(),
            Some(tx.clone()),
            100.0,
        );

        let timestamp = NaiveDate::from_ymd_opt(2023, 10, 19)
            .expect("test assert: static date")
            .and_hms_opt(0, 0, 0)
            .expect("test assert: static time");

        // adding two new log_name-keys rows
        let datarows = vec![
            Datarow::new(
                "log_name1".to_string(),
                timestamp,
                vec![
                    ("key11".to_string(), Datavalue::HeatmapPercent(3_f64)),
                    ("key12".to_string(), Datavalue::Size(400_u64)),
                ],
            ),
            Datarow::new(
                "log_name2".to_string(),
                timestamp,
                vec![
                    ("key21".to_string(), Datavalue::HeatmapPercent(3_f64)),
                    ("key22".to_string(), Datavalue::Size(400_u64)),
                ],
            ),
        ];

        let res = log
            .core_append(
                datarows,
                Some(Duration::from_millis(1200)), // approx 1050 maximum jitter, 150 ms for the first response
            )
            .await;
        assert!(matches!(res, Ok(())), "should fit into maximum retry limit");
    }

    #[tokio::test]
    async fn append_retry_maximum_backoff() {
        let (tx, _) = mpsc::channel(1);
        let tx = Sender::new(tx, GENERAL_SERVICE_NAME);
        let sheets_api = SpreadsheetAPI::new(
            tx.clone(),
            TestState::new(
                vec![mock_ordinary_google_sheet("some sheet")],
                Some(TestState::failure_response("error to retry".to_string())),
                Some(150),
            ),
        );
        let storage = Arc::new(Storage::new(TEST_HOST_ID.to_string(), sheets_api));
        let mut log = AppendableLog::new(
            storage,
            "spreadsheet1".to_string(),
            GENERAL_SERVICE_NAME.to_string(),
            Some(tx),
            100.0,
        );

        let timestamp = NaiveDate::from_ymd_opt(2023, 10, 19)
            .expect("test assert: static date")
            .and_hms_opt(0, 0, 0)
            .expect("test assert: static time");

        // adding two new log_name-keys rows
        let datarows = vec![
            Datarow::new(
                "log_name1".to_string(),
                timestamp,
                vec![
                    ("key11".to_string(), Datavalue::HeatmapPercent(3_f64)),
                    ("key12".to_string(), Datavalue::Size(400_u64)),
                ],
            ),
            Datarow::new(
                "log_name2".to_string(),
                timestamp,
                vec![
                    ("key21".to_string(), Datavalue::HeatmapPercent(3_f64)),
                    ("key22".to_string(), Datavalue::Size(400_u64)),
                ],
            ),
        ];

        let res = log
            .core_append(datarows, Some(Duration::from_millis(100)))
            .await;
        assert!(
            matches!(res, Err(StorageError::RetryTimeout(_))),
            "Google API request maximum retry duration should happen"
        );
    }

    #[tokio::test]
    #[should_panic(expected = "error to retry")]
    async fn append_without_retry() {
        let (tx, mut rx) = mpsc::channel(1);
        let tx = Sender::new(tx, GENERAL_SERVICE_NAME);
        let sheets_api = SpreadsheetAPI::new(
            tx.clone(),
            TestState::new(
                vec![mock_ordinary_google_sheet("some sheet")],
                Some(TestState::bad_response("error to retry".to_string())),
                None,
            ),
        );
        let storage = Arc::new(Storage::new(TEST_HOST_ID.to_string(), sheets_api));
        let mut log = AppendableLog::new(
            storage,
            "spreadsheet1".to_string(),
            GENERAL_SERVICE_NAME.to_string(),
            Some(tx.clone()),
            100.0,
        );

        let timestamp = NaiveDate::from_ymd_opt(2023, 10, 19)
            .expect("test assert: static date")
            .and_hms_opt(0, 0, 0)
            .expect("test assert: static time");

        // adding two new log_name-keys rows
        let datarows = vec![
            Datarow::new(
                "log_name1".to_string(),
                timestamp,
                vec![
                    ("key11".to_string(), Datavalue::HeatmapPercent(3_f64)),
                    ("key12".to_string(), Datavalue::Size(400_u64)),
                ],
            ),
            Datarow::new(
                "log_name2".to_string(),
                timestamp,
                vec![
                    ("key21".to_string(), Datavalue::HeatmapPercent(3_f64)),
                    ("key22".to_string(), Datavalue::Size(400_u64)),
                ],
            ),
        ];

        let handle = tokio::task::spawn(async move {
            assert!(
                rx.recv().await.is_some(),
                "notification is sent for nonrecoverable error"
            );
        });

        log.append_no_retry(datarows).await.unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn truncation_flow() {
        let (tx, mut rx) = mpsc::channel::<Notification>(1);
        let messages = tokio::spawn(async move {
            let mut warn_count = 0;
            while let Some(msg) = rx.recv().await {
                if msg.message.contains("the data will be truncated") {
                    warn_count += 1;
                }
                println!("{msg:?}");
            }
            assert_eq!(warn_count, 1, "number of warnings is 1 after being sent");
        });
        {
            let tx = Sender::new(tx, GENERAL_SERVICE_NAME);
            let sheets_api = SpreadsheetAPI::new(tx.clone(), TestState::new(vec![], None, None));
            let storage = Arc::new(Storage::new(TEST_HOST_ID.to_string(), sheets_api));
            // for simplicity we create logs with one key to easily
            // make assertions on rows count (only two columns - timestamp and key)
            let mut log = AppendableLog::new(
                storage,
                "spreadsheet1".to_string(),
                GENERAL_SERVICE_NAME.to_string(),
                Some(tx.clone()),
                0.01, // 0.01% of 10 000 000 cells means 1000 cells or 500 rows
            );

            let timestamp = NaiveDate::from_ymd_opt(2023, 10, 19)
                .expect("test assert: static date")
                .and_hms_opt(0, 0, 0)
                .expect("test assert: static time");

            let mut datarows = Vec::with_capacity(400); // going to add 400 rows or 800 cells
            for _ in 0..200 {
                datarows.push(Datarow::new(
                    "log_name1".to_string(),
                    timestamp,
                    vec![("key11".to_string(), Datavalue::Size(400_u64))],
                ));
                datarows.push(Datarow::new(
                    "log_name2".to_string(),
                    timestamp,
                    vec![("key21".to_string(), Datavalue::Size(400_u64))],
                ));
            }
            datarows.push(Datarow::new(
                RULES_LOG_NAME.to_string(),
                timestamp,
                vec![("key21".to_string(), Datavalue::Size(400_u64))],
            )); // 2 rows of rules (including header row) or 4 cells

            log.append(datarows).await.unwrap(); // 808 cells of log_name1, log_name2 and rules including headers

            let all_sheets = log
                .storage
                .google
                .sheets_filtered_by_metadata(&log.spreadsheet_id, &Metadata::new(vec![]))
                .await
                .unwrap();
            assert_eq!(
                all_sheets.len(),
                3,
                "`log_name1`, `log_name2`, `{}` sheets have been created",
                RULES_LOG_NAME
            );
            for sheet in all_sheets.iter().take(3) {
                if sheet.title().contains("log_name1") || sheet.title().contains("log_name2") {
                    assert_eq!(
                        sheet.row_count(),
                        Some(201),
                        "`log_name..` contains header row and 248 rows of data"
                    );
                } else {
                    assert_eq!(
                        sheet.row_count(),
                        Some(2),
                        "`{}` contains header row and 1 row of data",
                        RULES_LOG_NAME
                    );
                }
            }

            // we have 808 cells used out of 1000 (limit)
            // now add 200 datarows => above the limit
            // for log_name1 the key has changed - new sheet will be created

            let mut datarows = Vec::with_capacity(10);
            for _ in 0..100 {
                datarows.push(Datarow::new(
                    "log_name1".to_string(),
                    timestamp,
                    vec![("key12".to_string(), Datavalue::Size(400_u64))],
                ));
                datarows.push(Datarow::new(
                    "log_name2".to_string(),
                    timestamp,
                    vec![("key21".to_string(), Datavalue::Size(400_u64))],
                ));
            } // log_name1 - new sheet to be created with headers,
              // so old log_name1 - 201 rows, new log_name1 - 101 rows, log_name2 - 301 rows, rules - 2 rows - total 605 rows or 1210 cells
              // we remove 210 and 1000 * 30% or 510 cells
              // cells = (201+101+301) * 2 = 1206
              // (201+101)*2/1206 = 50.08% of log_name1 or 256 cells or 128 rows
              // 301*2/1206 = 49.91% of logn_name2 or 256 cells or 128 rows

            log.append(datarows).await.unwrap();

            let all_sheets = log
                .storage
                .google
                .sheets_filtered_by_metadata(&log.spreadsheet_id, &Metadata::new(vec![]))
                .await
                .unwrap();
            assert_eq!(
                all_sheets.len(),
                4,
                "`log_name1` with another key has been created"
            );

            for sheet in all_sheets.iter().take(3) {
                if sheet.title().contains("log_name2") {
                    assert_eq!(sheet.row_count(), Some(174));
                } else if sheet.title().contains("log_name1") {
                    assert!(sheet.row_count() == Some(73) || sheet.row_count() == Some(101),);
                } else {
                    assert_eq!(
                        sheet.row_count(),
                        Some(2),
                        "`{}` contains header row and 1 row of data",
                        RULES_LOG_NAME
                    );
                }
            }
        } // a scope to drop senders
        messages.await.unwrap();
    }
}
