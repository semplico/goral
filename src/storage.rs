use crate::configuration::MAX_GOOGLE_REQUEST_DURATION_SECS;

use crate::google::datavalue::Datarow;
use crate::google::sheet::TabColorRGB;
use crate::google::spreadsheet::GOOGLE_SPREADSHEET_MAXIMUM_CELLS;
use crate::google::{Storage, StorageError, Table, TableId};
use crate::notifications::Sender;
use crate::rules::{Rule, RULES_LOG_NAME};
use crate::{get_service_tab_color, jitter_duration};
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

pub struct AppendableLog {
    storage: Arc<Storage>,
    service: String,
    spreadsheet_id: String,
    tab_color_rgb: TabColorRGB,
    rules_table_id: Option<TableId>,
    messenger: Option<Sender>,
    truncate_at: f32,
    truncate_warning_is_sent: bool,
    tables: HashMap<TableId, Table>,
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
            rules_table_id: None,
            messenger,
            truncate_at,
            truncate_warning_is_sent: false,
            tables: HashMap::new(),
        }
    }

    async fn fetch_tables(&self) -> Result<HashMap<TableId, Table>, StorageError> {
        let tables: HashMap<TableId, Table> = self
            .storage
            .tables_for_service(&self.spreadsheet_id, &self.service)
            .await?
            .into_iter()
            .map(|t| (*t.id(), t))
            .collect();

        Ok(tables)
    }

    fn update_tables(&mut self, fetched_tables: HashMap<TableId, Table>) {
        // If some table isn't loaded but is considered to be updated
        // it means it was deleted between updates
        // we need to mark it for re-creation
        for (id, previous_table) in self.tables.iter_mut() {
            if fetched_tables.contains_key(id)
                || previous_table.to_be_deleted()
                || previous_table.to_be_created()
            {
                continue;
            }
            previous_table.plan_to_recreate();
        }

        for t in fetched_tables.into_values() {
            let id = t.id();
            if let Some(previous) = self.tables.get_mut(id) {
                *previous += t;
            } else {
                self.tables.insert(*id, t);
            }
        }
    }

    pub async fn healthcheck(&mut self) -> Result<(), StorageError> {
        let tables = self.fetch_tables().await?;
        self.update_tables(tables);
        Ok(())
    }

    pub async fn append(&mut self) -> Result<(), StorageError> {
        self.core_append(Some(Duration::from_secs(
            MAX_GOOGLE_REQUEST_DURATION_SECS.into(),
        )))
        .await
    }

    pub async fn append_no_retry(&mut self) -> Result<(), StorageError> {
        self.core_append(None).await?;
        Ok(())
    }

    // https://developers.google.com/sheets/api/limits#example-algorithm
    async fn exponential_backoff<T, F>(
        service: &str,
        maximum_backoff: Duration,
        f: impl Fn() -> F,
    ) -> Result<T, StorageError>
    where
        F: Future<Output = Result<T, StorageError>>,
    {
        let mut total_time = Duration::from_millis(0);
        let mut wait = Duration::from_millis(2);
        let mut retry = 0;
        let max_backoff = tokio::time::sleep(maximum_backoff);
        tokio::pin!(max_backoff);
        let mut last_retry_error = format!("timeout {maximum_backoff:?}");
        while total_time < maximum_backoff {
            tokio::select! {
                _ = &mut max_backoff => {break;}
                res = f() => {
                    if let Err(StorageError::Retriable(e)) = res {
                        tracing::error!("error {:?} for service `{}` retrying #{}", e, service, retry);
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
                service
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

    async fn timed_fetch_tables(
        &mut self,
        maximum_backoff: Duration,
    ) -> Result<HashMap<TableId, Table>, StorageError> {
        let service = self.service.clone();
        let callback = || async { self.fetch_tables().await };
        Self::exponential_backoff(&service, maximum_backoff, callback).await
    }

    // for newly created log sheet its headers order is determined by its first datarow. Fields for other datarows for the same sheet are sorted accordingly.
    async fn core_append(&mut self, retry_limit: Option<Duration>) -> Result<(), StorageError> {
        let tables = if let Some(retry_limit) = retry_limit {
            // We do not retry sheet `crud` as it goes after
            // `fetch_sheets` which is retriable and should
            // either fix an error or fail.
            // Retrying `crud` is not idempotent and
            // would require cloning all sheets at every retry attempt
            self.timed_fetch_tables(retry_limit).await?
        } else {
            self.fetch_tables().await?
        };

        self.update_tables(tables);

        tracing::debug!("existing tables:\n{:#?}", self.tables);

        self.plan_cleanup();

        self.storage
            .execute_plan(&self.spreadsheet_id, &mut self.tables)
            .await?;

        let mut ids_to_delete = vec![];
        for t in self.tables.values_mut() {
            let id = *t.id();
            if t.to_be_cleaned() {
                self.truncate_warning_is_sent = false;
            }
            if t.to_be_deleted() {
                ids_to_delete.push(id);
            }
            t.post_execution();
        }
        for id in ids_to_delete.into_iter() {
            self.tables.remove(&id);
        }

        Ok(())
    }

    #[allow(clippy::cast_precision_loss)]
    #[allow(clippy::cast_sign_loss)]
    #[allow(clippy::cast_possible_truncation)]
    fn plan_cleanup(&mut self) {
        let limit = f64::from(self.truncate_at); // SAFE as limit is supposed to be a % so under 100.0

        let cells_used_by_service: u32 = self.tables.values().map(|t| t.cells_to_be_used()).sum();

        let usage =
            100.0 * f64::from(cells_used_by_service) / f64::from(GOOGLE_SPREADSHEET_MAXIMUM_CELLS);
        tracing::debug!("service `{}` usage: {}%", self.service, usage);

        if usage < limit {
            if usage > 0.8 * limit && !self.truncate_warning_is_sent {
                let url = self.base_url();
                let message = format!("current [spreadsheet]({url}) usage `{usage:.2}%` for service `{}` is approaching a limit `{limit:.2}%`, the data will be truncated, copy it if needed or consider using a separate spreadsheet for this service with a higher [storage quota](https://maksimryndin.github.io/goral/services.html#storage-quota)", self.service);
                tracing::warn!("{}", message);
                if let Some(messenger) = self.messenger.as_ref() {
                    messenger.try_warn(message);
                    self.truncate_warning_is_sent = true;
                }
            }
            return;
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

        let usages = self.tables.values().fold(HashMap::new(), |mut state, t| {
            let log_name = t.name();
            // rules tables are not included
            if log_name == RULES_LOG_NAME {
                return state;
            }
            let table_usage = TableUsage {
                id: *t.id(),
                used_rows: t.rows_to_be_used().into(),
                used_columns: t.columns_to_be_used().into(),
                updated_at: t.updated_at(),
            };
            let table_cells = t.cells_to_be_used();
            let stat = state.entry(log_name).or_insert((0, 0.0, vec![]));
            stat.0 += table_cells;
            stat.1 = f64::from(stat.0) / f64::from(cells_used_by_service); // share of usage by the log name
            stat.2.push(table_usage);
            state
        });

        tracing::debug!("usages:\n{:#?}", usages);

        let usages: Vec<_> = usages.into_values().collect();

        for (_, log_cells_usage, mut table_usages) in usages {
            let cells_to_delete_for_log: f64 = log_cells_usage * cells_to_delete; // SAFE as the upper bound for cells for Sheets is within i32::MAX
            let mut cells_to_delete_for_log = cells_to_delete_for_log as i64;
            table_usages.sort_by_key(|s| s.updated_at);

            for usage in table_usages {
                if cells_to_delete_for_log <= 0 {
                    break;
                }
                let cells = usage.used_rows * usage.used_columns;
                let table = self
                    .tables
                    .get_mut(&usage.id)
                    .expect("assert: table is loaded for an existing sheet");
                if cells <= cells_to_delete_for_log {
                    // remove the whole sheet
                    table.plan_to_delete();
                    cells_to_delete_for_log -= cells;
                } else {
                    // remove some rows
                    let rows_to_delete: u32 = usage
                        .used_rows
                        .min(cells_to_delete_for_log / usage.used_columns + 1)
                        .try_into()
                        .expect("assert: rows_to_delete fits into u32");
                    let start_index = 1;
                    let end_index = start_index + rows_to_delete;
                    // delete first `rows` rows
                    table.plan_to_truncate(start_index, end_index);
                    cells_to_delete_for_log = 0;
                }
            }
        }
    }

    pub fn host_id(&self) -> &str {
        self.storage.host_id()
    }

    pub fn new_rows(&self) -> u32 {
        self.tables.values().map(|t| t.rows_to_add_count()).sum()
    }

    pub fn plan_to_append(&mut self, datarow: &mut Datarow) {
        // datarow has been already appended
        if datarow.row.is_some() {
            return;
        }
        let host_id = self.host_id();
        let table_id = datarow.calculate_sheet_id(host_id, &self.service);
        if self.rules_table_id.is_none() && datarow.log_name() == RULES_LOG_NAME {
            self.rules_table_id = Some(table_id);
        }
        let row = if let Some(table) = self.tables.get_mut(&table_id) {
            table.plan_to_append(datarow.clone())
        } else {
            // create table
            let mut table = Table::plan_to_create(
                self.storage.host_id(),
                &self.service,
                self.tab_color_rgb,
                datarow,
            );
            let row = table.plan_to_append(datarow.clone());
            let id = table.id();
            self.tables.insert(*id, table);
            row
        };
        datarow.set_row(row);
    }

    pub fn storage(&self) -> Arc<Storage> {
        self.storage.clone()
    }

    pub fn row_url(&self, sheet_id: TableId, row: u32) -> String {
        self.storage
            .table_row_url(&self.spreadsheet_id, sheet_id, row)
    }

    pub fn base_url(&self) -> String {
        self.storage.base_url(&self.spreadsheet_id)
    }

    pub fn spreadsheet_id(&self) -> String {
        self.spreadsheet_id.to_string()
    }

    pub async fn get_rules(&self) -> Result<Vec<Rule>, StorageError> {
        let service = self.service.clone();
        let rules_table_id = self.rules_table_id.expect(
            "assert: rules sheet id is saved at the start of the service at the first append",
        );
        let callback = || async {
            self.storage
                .get_table(&self.spreadsheet_id, rules_table_id)
                .await
        };
        let data = Self::exponential_backoff(
            &service,
            Duration::from_secs(MAX_GOOGLE_REQUEST_DURATION_SECS.into()),
            callback,
        )
        .await?;

        Ok(data
            .into_iter()
            .filter_map(|row| Rule::try_from_values(row, self.messenger.as_ref()))
            .collect())
    }
}

#[derive(Debug)]
struct TableUsage {
    id: TableId,
    used_rows: i64,
    used_columns: i64,
    updated_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::google::datavalue::{Datarow, Datavalue};
    use crate::google::sheet::tests::mock_ordinary_google_sheet;
    use crate::google::spreadsheet::{tests::TestState, SpreadsheetAPI};
    use crate::notifications::{Notification, Sender};
    use crate::services::general::GENERAL_SERVICE_NAME;
    use crate::tests::TEST_HOST_ID;
    use chrono::NaiveDate;
    use google_sheets4::Error;
    use tokio::sync::mpsc;

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
            storage.clone(),
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
        log.plan_to_append(&mut Datarow::new(
            "log_name1".to_string(),
            timestamp,
            vec![
                ("key11".to_string(), Datavalue::HeatmapPercent(3_f64)),
                ("key12".to_string(), Datavalue::Size(400_u64)),
            ],
        ));
        log.plan_to_append(&mut Datarow::new(
            "log_name2".to_string(),
            timestamp,
            vec![
                ("key21".to_string(), Datavalue::HeatmapPercent(3_f64)),
                ("key22".to_string(), Datavalue::Size(400_u64)),
            ],
        ));

        log.append().await.unwrap();

        // for the sheet order we rely on the indexing
        let all_sheets = storage
            .tables_for_service(&log.spreadsheet_id, GENERAL_SERVICE_NAME)
            .await
            .unwrap();

        assert_eq!(
            all_sheets.len(),
            2,
            "`log_name1` and `log_name2` sheets have been created"
        );
        assert!(
            all_sheets[0].name().contains("log_name1")
                || all_sheets[1].name().contains("log_name1")
        );
        assert_eq!(
            all_sheets[0].rows_count(),
            2,
            "`log_name..` contains header row and one row of data"
        );
        assert!(
            all_sheets[0].name().contains("log_name2")
                || all_sheets[1].name().contains("log_name2")
        );
        assert_eq!(
            all_sheets[1].rows_count(),
            2,
            "`log_name..` contains header row and one row of data"
        );

        // adding two existing log_name-keys rows
        // but for one the order of keys is different - shouldn't make any difference
        // and new one combination
        // and new combination with the same log_name, but updated keys
        // two new sheets should be created
        log.plan_to_append(&mut Datarow::new(
            "log_name2".to_string(),
            timestamp,
            vec![
                ("key21".to_string(), Datavalue::HeatmapPercent(3_f64)),
                ("key23".to_string(), Datavalue::Size(400_u64)),
            ],
        ));
        log.plan_to_append(&mut Datarow::new(
            "log_name1".to_string(),
            timestamp,
            vec![
                ("key12".to_string(), Datavalue::Size(400_u64)),
                ("key11".to_string(), Datavalue::HeatmapPercent(3_f64)),
            ],
        ));
        log.plan_to_append(&mut Datarow::new(
            "log_name2".to_string(),
            timestamp,
            vec![
                ("key21".to_string(), Datavalue::HeatmapPercent(3_f64)),
                ("key22".to_string(), Datavalue::Size(400_u64)),
            ],
        ));
        log.plan_to_append(&mut Datarow::new(
            "log_name3".to_string(),
            timestamp,
            vec![
                ("key31".to_string(), Datavalue::HeatmapPercent(3_f64)),
                ("key32".to_string(), Datavalue::Size(400_u64)),
            ],
        ));

        log.append().await.unwrap();
        let all_sheets = storage
            .tables_for_service(&log.spreadsheet_id, GENERAL_SERVICE_NAME)
            .await
            .unwrap();
        assert_eq!(all_sheets.len(), 4, "`some sheet`, `log_name1` and `log_name2` already exist, `log_name2` with different keys and `log_name3` sheets have been created");

        assert!(
            all_sheets[0].name().contains("log_name1")
                || all_sheets[1].name().contains("log_name1")
        );
        assert_eq!(
            all_sheets[0].rows_count(),
            3,
            "`log_name1` and `log_name2` contain header row and two rows of data"
        );
        assert!(
            all_sheets[0].name().contains("log_name2")
                || all_sheets[1].name().contains("log_name2")
        );
        assert_eq!(
            all_sheets[1].rows_count(),
            3,
            "`log_name1` and `log_name2` contain header row and two rows of data"
        );

        assert!(
            all_sheets[2].name().contains("log_name2")
                || all_sheets[3].name().contains("log_name2")
        );
        assert_eq!(all_sheets[2].rows_count(), 2, "`log_name2` with different keys and `log_name3` contain header row and one row of data");
        assert!(
            all_sheets[2].name().contains("log_name3")
                || all_sheets[3].name().contains("log_name3")
        );
        assert_eq!(all_sheets[3].rows_count(), 2, "`log_name2` with different keys and `log_name3` contain header row and one row of data");

        assert_eq!(all_sheets[2].service(), GENERAL_SERVICE_NAME);
        assert_eq!(all_sheets[2].host(), TEST_HOST_ID);

        assert!(all_sheets[2].name().starts_with("log_name"));
        assert_ne!(
            all_sheets[0].created_at(),
            all_sheets[0].updated_at(),
            "first sheets should be updated"
        );
        assert_eq!(
            all_sheets[2].created_at(),
            all_sheets[2].updated_at(),
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
        log.plan_to_append(&mut Datarow::new(
            "log_name1".to_string(),
            timestamp,
            vec![
                ("key11".to_string(), Datavalue::HeatmapPercent(3_f64)),
                ("key12".to_string(), Datavalue::Size(400_u64)),
            ],
        ));
        log.plan_to_append(&mut Datarow::new(
            "log_name2".to_string(),
            timestamp,
            vec![
                ("key21".to_string(), Datavalue::HeatmapPercent(3_f64)),
                ("key22".to_string(), Datavalue::Size(400_u64)),
            ],
        ));

        log.append().await.unwrap();
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
        log.plan_to_append(&mut Datarow::new(
            "log_name1".to_string(),
            timestamp,
            vec![
                ("key11".to_string(), Datavalue::HeatmapPercent(3_f64)),
                ("key12".to_string(), Datavalue::Size(400_u64)),
            ],
        ));
        log.plan_to_append(&mut Datarow::new(
            "log_name2".to_string(),
            timestamp,
            vec![
                ("key21".to_string(), Datavalue::HeatmapPercent(3_f64)),
                ("key22".to_string(), Datavalue::Size(400_u64)),
            ],
        ));

        let handle = tokio::task::spawn(async move {
            assert!(
                rx.recv().await.is_some(),
                "notification is sent for nonrecoverable error"
            );
        });

        log.append().await.unwrap();
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
        log.plan_to_append(&mut Datarow::new(
            "log_name1".to_string(),
            timestamp,
            vec![
                ("key11".to_string(), Datavalue::HeatmapPercent(3_f64)),
                ("key12".to_string(), Datavalue::Size(400_u64)),
            ],
        ));
        log.plan_to_append(&mut Datarow::new(
            "log_name2".to_string(),
            timestamp,
            vec![
                ("key21".to_string(), Datavalue::HeatmapPercent(3_f64)),
                ("key22".to_string(), Datavalue::Size(400_u64)),
            ],
        ));

        let res = log
            .core_append(
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
        log.plan_to_append(&mut Datarow::new(
            "log_name1".to_string(),
            timestamp,
            vec![
                ("key11".to_string(), Datavalue::HeatmapPercent(3_f64)),
                ("key12".to_string(), Datavalue::Size(400_u64)),
            ],
        ));
        log.plan_to_append(&mut Datarow::new(
            "log_name2".to_string(),
            timestamp,
            vec![
                ("key21".to_string(), Datavalue::HeatmapPercent(3_f64)),
                ("key22".to_string(), Datavalue::Size(400_u64)),
            ],
        ));

        let res = log.core_append(Some(Duration::from_millis(100))).await;
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
        log.plan_to_append(&mut Datarow::new(
            "log_name1".to_string(),
            timestamp,
            vec![
                ("key11".to_string(), Datavalue::HeatmapPercent(3_f64)),
                ("key12".to_string(), Datavalue::Size(400_u64)),
            ],
        ));
        log.plan_to_append(&mut Datarow::new(
            "log_name2".to_string(),
            timestamp,
            vec![
                ("key21".to_string(), Datavalue::HeatmapPercent(3_f64)),
                ("key22".to_string(), Datavalue::Size(400_u64)),
            ],
        ));

        let handle = tokio::task::spawn(async move {
            assert!(
                rx.recv().await.is_some(),
                "notification is sent for nonrecoverable error"
            );
        });

        log.append_no_retry().await.unwrap();
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
                storage.clone(),
                "spreadsheet1".to_string(),
                GENERAL_SERVICE_NAME.to_string(),
                Some(tx.clone()),
                0.01, // 0.01% of 10 000 000 cells means 1000 cells or 500 rows
            );

            let timestamp = NaiveDate::from_ymd_opt(2023, 10, 19)
                .expect("test assert: static date")
                .and_hms_opt(0, 0, 0)
                .expect("test assert: static time");

            // going to add 400 rows or 800 cells
            for _ in 0..200 {
                log.plan_to_append(&mut Datarow::new(
                    "log_name1".to_string(),
                    timestamp,
                    vec![("key11".to_string(), Datavalue::Size(400_u64))],
                ));
                log.plan_to_append(&mut Datarow::new(
                    "log_name2".to_string(),
                    timestamp,
                    vec![("key21".to_string(), Datavalue::Size(400_u64))],
                ));
            }
            log.plan_to_append(&mut Datarow::new(
                RULES_LOG_NAME.to_string(),
                timestamp,
                vec![("key21".to_string(), Datavalue::Size(400_u64))],
            )); // 2 rows of rules (including header row) or 4 cells

            log.append().await.unwrap(); // 808 cells of log_name1, log_name2 and rules including headers

            let all_sheets = storage
                .tables_for_service(&log.spreadsheet_id, GENERAL_SERVICE_NAME)
                .await
                .unwrap();
            assert_eq!(
                all_sheets.len(),
                3,
                "`log_name1`, `log_name2`, `{RULES_LOG_NAME}` sheets have been created",
            );

            for sheet in all_sheets.iter() {
                if sheet.name().contains("log_name1") || sheet.name().contains("log_name2") {
                    assert_eq!(
                        sheet.rows_count(),
                        201,
                        "`log_name..` contains header row and 200 rows of data"
                    );
                } else {
                    assert_eq!(
                        sheet.rows_count(),
                        2,
                        "`{RULES_LOG_NAME}` contains header row and 1 row of data",
                    );
                }
            }

            // we have 808 cells used out of 1000 (limit)
            // now add 200 datarows => above the limit
            // for log_name1 the key has changed - new sheet will be created
            for _ in 0..100 {
                log.plan_to_append(&mut Datarow::new(
                    "log_name1".to_string(),
                    timestamp,
                    vec![("key12".to_string(), Datavalue::Size(400_u64))],
                ));
                log.plan_to_append(&mut Datarow::new(
                    "log_name2".to_string(),
                    timestamp,
                    vec![("key21".to_string(), Datavalue::Size(400_u64))],
                ));
            }
            // log_name1 - new sheet to be created with headers,
            // so old log_name1 - 201 rows, new log_name1 - 101 rows, log_name2 - 301 rows, rules - 2 rows - total 605 rows or 1210 cells
            // we remove 210 (surplus) and 1000 * 30% or 510 cells total
            // cells = (201+101+301) * 2 = 1206
            // (201+101)*2/1206 = 50.08% of log_name1 or 256 cells or 128 rows
            // 301*2/1206 = 49.91% of logn_name2 or 256 cells or 128 rows

            log.append().await.unwrap();

            let all_sheets = storage
                .tables_for_service(&log.spreadsheet_id, GENERAL_SERVICE_NAME)
                .await
                .unwrap();
            assert_eq!(
                all_sheets.len(),
                4,
                "`log_name1` with another key has been created"
            );

            for sheet in all_sheets.iter() {
                if sheet.name().contains("log_name2") {
                    assert_eq!(sheet.rows_count(), 174);
                    assert_eq!(sheet.used_rows(), 174);
                } else if sheet.name().contains("log_name1") {
                    assert!(sheet.rows_count() == 73 || sheet.rows_count() == 101);
                    assert!(sheet.used_rows() == 73 || sheet.used_rows() == 101);
                } else {
                    assert_eq!(
                        sheet.rows_count(),
                        2,
                        "`{RULES_LOG_NAME}` contains header row and 1 row of data",
                    );
                    assert_eq!(sheet.used_rows(), 2)
                }
            }

            // now log_name1 old - 73 rows, log_name1 new - 101, log_name2 - 174 rows, rules - 2 rows - total 350 rows or 700 cells

            // now add more `log_name1` datarows to push to 1100 cells
            // surplus 100 cells and 1000*30% or 400 cells in total to delete
            // proportions:
            // log_name1: (73+101+400)/1100 = 52% to delete or 209 rows, so old log_name1 should be deleted
            (0..200).for_each(|_| {
                log.plan_to_append(&mut Datarow::new(
                    "log_name1".to_string(),
                    timestamp,
                    vec![("key12".to_string(), Datavalue::Size(400_u64))],
                ))
            });

            log.append().await.unwrap();

            let all_sheets = storage
                .tables_for_service(&log.spreadsheet_id, GENERAL_SERVICE_NAME)
                .await
                .unwrap();
            assert_eq!(all_sheets.len(), 3, "old `log_name1` with was deleted");
        } // a scope to drop senders
        messages.await.unwrap();
    }

    #[tokio::test]
    async fn table_recreation_flow() {
        let (tx, _) = mpsc::channel(1);
        let tx = Sender::new(tx, GENERAL_SERVICE_NAME);
        let sheets_api = SpreadsheetAPI::new(
            tx.clone(),
            TestState::new(vec![mock_ordinary_google_sheet("some sheet")], None, None),
        );
        let storage = Arc::new(Storage::new(TEST_HOST_ID.to_string(), sheets_api));
        let mut log = AppendableLog::new(
            storage.clone(),
            "spreadsheet1".to_string(),
            GENERAL_SERVICE_NAME.to_string(),
            Some(tx.clone()),
            100.0,
        );

        let timestamp = NaiveDate::from_ymd_opt(2023, 10, 19)
            .expect("test assert: static date")
            .and_hms_opt(0, 0, 0)
            .expect("test assert: static time");

        let mut datarow = Datarow::new(
            "log_name1".to_string(),
            timestamp,
            vec![
                ("key11".to_string(), Datavalue::HeatmapPercent(3_f64)),
                ("key12".to_string(), Datavalue::Size(400_u64)),
            ],
        );
        log.plan_to_append(&mut datarow);

        log.append().await.unwrap();

        let all_sheets = storage
            .tables_for_service(&log.spreadsheet_id, GENERAL_SERVICE_NAME)
            .await
            .unwrap();

        assert_eq!(all_sheets.len(), 1, "`log_name1` sheet has been created");
        assert!(all_sheets[0].name().contains("log_name1"));
        assert_eq!(all_sheets[0].rows_count(), 2);

        storage.delete_table(datarow.sheet_id()).await;

        let all_sheets = storage
            .tables_for_service(&log.spreadsheet_id, GENERAL_SERVICE_NAME)
            .await
            .unwrap();

        assert!(all_sheets.is_empty(), "`log_name1` sheet has been deleted");

        let mut datarow = Datarow::new(
            "log_name1".to_string(),
            timestamp,
            vec![
                ("key11".to_string(), Datavalue::HeatmapPercent(3_f64)),
                ("key12".to_string(), Datavalue::Size(400_u64)),
            ],
        );
        log.plan_to_append(&mut datarow);

        log.append().await.unwrap();

        let all_sheets = storage
            .tables_for_service(&log.spreadsheet_id, GENERAL_SERVICE_NAME)
            .await
            .unwrap();

        assert_eq!(all_sheets.len(), 1, "`log_name1` sheet has been created");
        assert!(all_sheets[0].name().contains("log_name1"));

        assert_eq!(all_sheets[0].rows_count(), 2);
    }

    #[tokio::test]
    async fn rules_recreation_flow() {
        let (tx, _) = mpsc::channel(1);
        let tx = Sender::new(tx, GENERAL_SERVICE_NAME);
        let sheets_api = SpreadsheetAPI::new(
            tx.clone(),
            TestState::new(vec![mock_ordinary_google_sheet("some sheet")], None, None),
        );
        let storage = Arc::new(Storage::new(TEST_HOST_ID.to_string(), sheets_api));
        let mut log = AppendableLog::new(
            storage.clone(),
            "spreadsheet1".to_string(),
            GENERAL_SERVICE_NAME.to_string(),
            Some(tx.clone()),
            100.0,
        );

        let timestamp = NaiveDate::from_ymd_opt(2023, 10, 19)
            .expect("test assert: static date")
            .and_hms_opt(0, 0, 0)
            .expect("test assert: static time");

        let mut datarow = Datarow::new(
            RULES_LOG_NAME.to_string(),
            timestamp,
            vec![
                ("key11".to_string(), Datavalue::HeatmapPercent(3_f64)),
                ("key12".to_string(), Datavalue::Size(400_u64)),
            ],
        );
        log.plan_to_append(&mut datarow);

        log.append().await.unwrap();

        let all_sheets = storage
            .tables_for_service(&log.spreadsheet_id, GENERAL_SERVICE_NAME)
            .await
            .unwrap();

        assert_eq!(all_sheets.len(), 1, "rules sheet has been created");
        assert!(all_sheets[0].name().contains(RULES_LOG_NAME));
        assert_eq!(all_sheets[0].rows_count(), 2);

        // second append - no duplicates
        log.plan_to_append(&mut datarow);

        log.append().await.unwrap();

        let all_sheets = storage
            .tables_for_service(&log.spreadsheet_id, GENERAL_SERVICE_NAME)
            .await
            .unwrap();

        assert_eq!(all_sheets.len(), 1, "rules sheet has been created");
        assert!(all_sheets[0].name().contains(RULES_LOG_NAME));
        assert_eq!(all_sheets[0].rows_count(), 2);

        storage.delete_table(datarow.sheet_id()).await;

        let all_sheets = storage
            .tables_for_service(&log.spreadsheet_id, GENERAL_SERVICE_NAME)
            .await
            .unwrap();

        assert!(all_sheets.is_empty(), "`log_name1` sheet has been deleted");

        let mut datarow = Datarow::new(
            RULES_LOG_NAME.to_string(),
            timestamp,
            vec![
                ("key11".to_string(), Datavalue::HeatmapPercent(3_f64)),
                ("key12".to_string(), Datavalue::Size(400_u64)),
            ],
        );
        log.plan_to_append(&mut datarow);

        log.append().await.unwrap();

        let all_sheets = storage
            .tables_for_service(&log.spreadsheet_id, GENERAL_SERVICE_NAME)
            .await
            .unwrap();

        assert_eq!(all_sheets.len(), 1, "rules sheet has been created");
        assert!(all_sheets[0].name().contains(RULES_LOG_NAME));

        assert_eq!(all_sheets[0].rows_count(), 2);
    }
}
