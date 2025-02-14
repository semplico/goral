pub mod configuration;
use crate::google::datavalue::{Datarow, Datavalue};
use crate::http::{run_server, to_body, Body};
use crate::messenger::configuration::MessengerConfig;
use crate::notifications::{MessengerApi, Notification, Sender};
use crate::rules::RULES_LOG_NAME;
use crate::services::kv::configuration::Kv;
use crate::services::{messenger_queue, rules_notifications, Data, Service};
use crate::storage::{AppendableLog, StorageError};
use crate::{capture_datetime, Shared};
use async_trait::async_trait;
use chrono::{DateTime, NaiveDateTime, Utc};
use futures::future::try_join_all;
use http_body_util::BodyExt;
use hyper::{
    body::Buf, header, Method, Request as HyperRequest, Response as HyperResponse, StatusCode,
};
use serde::{de::Deserializer, Deserialize, Serialize};
use serde_valid::Validate;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::sync::mpsc::{self};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

pub const KV_SERVICE_NAME: &str = "kv";

fn unique_keys(data: &[(String, Value)]) -> Result<(), serde_valid::validation::Error> {
    let keys: HashSet<&String> = data.iter().map(|(k, _)| k).collect();
    if keys.len() < data.len() {
        Err(serde_valid::validation::Error::Custom(
            "`data` should have unique keys".to_string(),
        ))
    } else {
        Ok(())
    }
}

fn is_not_reserved_name(log_name: &String) -> Result<(), serde_valid::validation::Error> {
    if log_name == RULES_LOG_NAME {
        Err(serde_valid::validation::Error::Custom(format!(
            "`log_name` cannot be a reserved name: [{}]",
            RULES_LOG_NAME
        )))
    } else {
        Ok(())
    }
}

fn deserialize_datetime<'de, D>(deserializer: D) -> Result<NaiveDateTime, D::Error>
where
    D: Deserializer<'de>,
{
    let buf = String::deserialize(deserializer)?;
    capture_datetime(&buf)
        .ok_or_else(|| serde::de::Error::custom(format!("cannot deserialize {} as datetime", buf)))
}

// for untagged enum an order is important
// https://serde.rs/enum-representations.html#untagged
#[derive(Debug, PartialEq, Deserialize)]
#[serde(untagged)]
enum Value {
    Integer(u32),
    Number(f64),
    Bool(bool),
    #[serde(deserialize_with = "deserialize_datetime")]
    Datetime(NaiveDateTime),
    Text(String),
}

impl From<Value> for Datavalue {
    fn from(val: Value) -> Self {
        use Value::*;
        match val {
            Integer(v) => Datavalue::Integer(v),
            Number(v) => Datavalue::Number(v),
            Bool(v) => Datavalue::Bool(v),
            Datetime(v) => Datavalue::Datetime(v),
            Text(v) => Datavalue::Text(v),
        }
    }
}

#[derive(Debug, Deserialize, Validate)]
struct KVRow {
    #[validate(custom(is_not_reserved_name))]
    #[validate(pattern = r"^[a-zA-Z_][a-zA-Z0-9_]*$")]
    log_name: String,
    datetime: DateTime<Utc>,
    #[validate(custom(unique_keys))]
    #[validate(min_items = 1)]
    data: Vec<(String, Value)>,
}

impl From<KVRow> for Datarow {
    fn from(val: KVRow) -> Self {
        Datarow::new(
            val.log_name,
            val.datetime.naive_utc(),
            val.data.into_iter().map(|(k, v)| (k, v.into())).collect(),
        )
    }
}

#[derive(Debug, Deserialize, Validate)]
struct Request {
    #[validate(min_items = 1)]
    #[validate]
    rows: Vec<KVRow>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Response {
    sheet_urls: Vec<String>,
}

struct AppendRequest {
    datarows: Vec<Datarow>,
    reply_to: oneshot::Sender<Result<Vec<String>, StorageError>>,
}

struct ReadyHandle(Arc<AtomicBool>);

impl Drop for ReadyHandle {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

pub struct KvService {
    shared: Shared,
    spreadsheet_id: String,
    messenger: Option<MessengerApi>,
    port: u16,
    truncate_at: f32,
}

#[derive(Clone)]
struct ServerEnvironment {
    is_ready: Arc<AtomicBool>,
    data_bus: mpsc::Sender<AppendRequest>,
}

impl KvService {
    pub fn new(shared: Shared, mut config: Kv) -> KvService {
        let messenger = config
            .messenger
            .take()
            .map(|messenger_config| MessengerApi::new(messenger_config, KV_SERVICE_NAME));
        Self {
            shared,
            spreadsheet_id: config.spreadsheet_id,
            port: config.port,
            messenger,
            truncate_at: config.autotruncate_at_usage_percent,
        }
    }

    async fn router(
        req: HyperRequest<hyper::body::Incoming>,
        send_notification: Sender,
        environment: ServerEnvironment,
    ) -> Result<HyperResponse<Body>, hyper::Error> {
        let ServerEnvironment { is_ready, data_bus } = environment;
        if is_ready
            .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Ok(HyperResponse::builder()
                .status(StatusCode::TOO_MANY_REQUESTS)
                .header(header::CONTENT_TYPE, "application/json")
                .body(to_body("no concurrent requests are allowed".as_bytes()))
                .expect("assert: should be able to construct response for static body"));
        }
        let _handle = ReadyHandle(is_ready);

        match (req.method(), req.uri().path()) {
            (&Method::POST, "/api/kv") => {
                let body = req.collect().await?.aggregate();
                let req_body: Request = match serde_json::from_reader(body.reader()) {
                    Err(e) => {
                        tracing::warn!("invalid request to KV service: {e}");
                        return Ok(HyperResponse::builder()
                        .status(StatusCode::UNPROCESSABLE_ENTITY)
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(to_body(e.to_string().into_bytes()))
                        .expect("assert: should be able to construct response for invalid kv request"));
                    }
                    Ok(b) => b,
                };
                if let Err(e) = req_body.validate() {
                    tracing::warn!("invalid request to KV service: {e}");
                    return Ok(HyperResponse::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(to_body(e.to_string().into_bytes()))
                        .expect(
                            "assert: should be able to construct response for invalid kv request",
                        ));
                }
                let (reply_to, rx) = oneshot::channel();

                let datarows = req_body
                    .rows
                    .into_iter()
                    .map(|kvrow| kvrow.into())
                    .collect();
                if let Err(e) = data_bus.send(AppendRequest { datarows, reply_to }).await {
                    let msg =
                        "kv service data messages queue has been unexpectedly closed".to_string();
                    tracing::error!("{}: {}", msg, e);
                    send_notification.fatal(msg).await;
                    panic!("assert: kv service data messages queue shouldn't be closed before shutdown signal");
                }

                let sheet_urls = match rx.await {
                    Err(e) => {
                        let msg = format!("kv service failed to get a result of append `{e}`");
                        tracing::error!("{}", msg);
                        send_notification.fatal(msg).await;
                        panic!("assert: kv service should be able to get a result of append");
                    }
                    Ok(Err(StorageError::Timeout(_))) | Ok(Err(StorageError::RetryTimeout(_))) => {
                        panic!("assert: for kv service google api timeout is not applied");
                    }
                    Ok(Err(StorageError::Retriable(text))) => {
                        return Ok(HyperResponse::builder()
                            .status(StatusCode::INTERNAL_SERVER_ERROR)
                            .header(header::CONTENT_TYPE, "application/json")
                            .body(to_body(text.into_bytes()))
                            .expect("assert: should be able to build response body from string"));
                    }
                    Ok(Err(StorageError::NonRetriable(text))) => {
                        return Ok(HyperResponse::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .header(header::CONTENT_TYPE, "application/json")
                            .body(to_body(text.into_bytes()))
                            .expect("assert: should be able to build response body from string"));
                    }
                    Ok(Ok(sheet_urls)) => sheet_urls,
                };

                let json = serde_json::to_vec(&Response { sheet_urls })
                    .expect("assert: should be able to serialize vector of sheet urls");
                let response = HyperResponse::builder()
                    .status(StatusCode::OK)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(to_body(json))
                    .expect("assert: should be able to serialize append return value");
                Ok(response)
            }
            _ => Ok(HyperResponse::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::default())
                .expect("assert: should be able to construct empty response body")),
        }
    }

    async fn start_server(
        &self,
        shutdown: broadcast::Receiver<u16>,
        data_bus: mpsc::Sender<AppendRequest>,
    ) -> JoinHandle<()> {
        let addr: SocketAddr = ([127, 0, 0, 1], self.port).into();

        let send_notification = self.shared.send_notification.clone();
        let is_ready = Arc::new(AtomicBool::new(true));
        let environment = ServerEnvironment { is_ready, data_bus };

        run_server(
            addr,
            "KV",
            send_notification,
            shutdown,
            environment,
            Self::router,
        )
        .await
    }
}

#[async_trait]
impl Service for KvService {
    fn name(&self) -> &'static str {
        KV_SERVICE_NAME
    }

    fn spreadsheet_id(&self) -> &str {
        self.spreadsheet_id.as_str()
    }

    fn channel_capacity(&self) -> usize {
        // Use3d for rules processing messages queue
        100
    }

    fn shared(&self) -> &Shared {
        &self.shared
    }

    fn messenger(&self) -> Option<Sender> {
        self.messenger.as_ref().map(|m| m.message_tx.clone())
    }

    fn messenger_config(&self) -> Option<&MessengerConfig> {
        self.messenger.as_ref().map(|m| &m.config)
    }

    fn take_messenger_rx(&mut self) -> Option<mpsc::Receiver<Notification>> {
        self.messenger.as_mut().and_then(|m| m.message_rx.take())
    }

    fn truncate_at(&self) -> f32 {
        self.truncate_at
    }

    async fn run(&mut self, mut log: AppendableLog, mut shutdown: broadcast::Receiver<u16>) {
        self.prerun_hook(&log).await;
        let (tx, mut data_receiver) = mpsc::channel(1);
        let mut tasks = vec![];
        let is_shutdown = Arc::new(AtomicBool::new(false));

        let (mut rules_input, rules_output) =
            self.start_rules_thread(is_shutdown.clone(), &mut log).await;

        if let Some(message_rx) = self.take_messenger_rx() {
            let messenger =
                self.shared().messenger.clone().expect(
                    "assert: if messenger receiver channel is set then the messenger is set",
                );
            let messenger_config = self
                .messenger_config()
                .expect(
                    "assert: if messenger receiver channel is set then the messenger config is set",
                )
                .clone();
            let send_notification = self.shared().send_notification.clone();
            let service = self.name();
            let host_id = log.host_id().to_string();
            tasks.push(tokio::spawn(async move {
                messenger_queue(
                    messenger,
                    messenger_config,
                    send_notification,
                    message_rx,
                    host_id,
                    service,
                )
                .await
            }));
        }
        if let Some(message_tx) = self.messenger() {
            let base_url = log.spreadsheet_baseurl();
            tasks.push(tokio::spawn(async move {
                rules_notifications(message_tx, rules_output, base_url).await
            }));
        }
        let mut rules_update_interval = tokio::time::interval(self.rules_update_interval());

        let server = self.start_server(shutdown.resubscribe(), tx).await;
        tasks.push(server);
        let tasks = try_join_all(tasks);
        tokio::pin!(tasks);
        self.welcome_hook(&log).await;

        loop {
            tokio::select! {
                result = shutdown.recv() => {
                    is_shutdown.store(true, Ordering::Release);
                    data_receiver.close(); // stop tasks
                    let graceful_shutdown_timeout = match result {
                        Err(_) => panic!("assert: shutdown signal sender should be dropped after all service listeners"),
                        Ok(graceful_shutdown_timeout) => graceful_shutdown_timeout,
                    };
                    tracing::info!("{} service has got shutdown signal", self.name());
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs(graceful_shutdown_timeout.into())) => {
                            tracing::error!("{} service has badly shutdowned. Some data is lost.", self.name());
                        },
                        _ = async {
                            // drain remaining messages
                            while let Some(append_request) = data_receiver.recv().await {
                                let AppendRequest{datarows, reply_to} = append_request;
                                let mut data = Data::Many(datarows);
                                self.send_for_rule_processing(&log, &mut data, &mut rules_input).await;
                                let Data::Many(datarows) = data else {panic!("assert: packing/unpacking of KV data")};
                                let res = log.append_no_retry(datarows).await;
                                if reply_to.send(res).is_err() {
                                    tracing::warn!("client of the kv server dropped connection");
                                }
                            }
                        } => {
                            tracing::info!("{} service has successfully shutdowned", self.name());
                        }
                    }
                    return;
                },
                _ = rules_update_interval.tick() => {
                    self.send_updated_rules(&log, &mut rules_input).await;
                }
                Some(append_request) = data_receiver.recv() => {
                    let AppendRequest{datarows, reply_to} = append_request;
                    let mut data = Data::Many(datarows);
                    self.send_for_rule_processing(&log, &mut data, &mut rules_input).await;
                    let Data::Many(datarows) = data else {panic!("assert: packing/unpacking of KV data")};
                    let res = log.append_no_retry(datarows).await;
                    if reply_to.send(res).is_err() {
                        tracing::warn!("client of the kv server dropped connection");
                    }
                }
                res = &mut tasks => {
                    res.unwrap(); // propagate panics from spawned tasks
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configuration::tests::build_config;
    use crate::google::spreadsheet::tests::TestState;
    use crate::google::spreadsheet::SpreadsheetAPI;
    use crate::google::Metadata;
    use crate::http::{HttpClient, Uri};
    use crate::notifications::Sender;
    use crate::storage::Storage;
    use crate::tests::TEST_HOST_ID;
    use crate::Shared;
    use serde_json::json;
    use std::sync::Arc;
    use tokio::sync::{broadcast, mpsc};

    #[tokio::test]
    async fn kv_service_flow() {
        const NUMBER_OF_MESSAGES: usize = 10;

        let (send_notification, mut rx) = mpsc::channel(NUMBER_OF_MESSAGES);
        tokio::task::spawn(async move {
            while let Some(msg) = rx.recv().await {
                println!("message {msg:?}");
            }
        });
        let send_notification = Sender::new(send_notification, KV_SERVICE_NAME);
        let sheets_api = SpreadsheetAPI::new(
            send_notification.clone(),
            TestState::new(vec![], None, None),
        );
        let storage = Arc::new(Storage::new(TEST_HOST_ID.to_string(), sheets_api));
        let log = AppendableLog::new(
            storage.clone(),
            "spreadsheet1".to_string(),
            KV_SERVICE_NAME.to_string(),
            Some(send_notification.clone()),
            100.0,
        );

        let (shutdown, rx) = broadcast::channel(1);

        let config = r#"
        spreadsheet_id = "123"
        port = 49152
        "#;

        let config: Kv =
            build_config(config).expect("should be able to build minimum configuration");

        let shared = Shared::new(send_notification);
        let service = tokio::spawn(async move {
            let mut service = KvService::new(shared, config);
            service.run(log, rx).await;
        });

        let client = HttpClient::lousy(32768, true, Duration::from_secs(5));
        let url: Uri = "http://localhost:49152/api/kv".parse().unwrap();

        let payload = json!({"rows": [
            {
                "log_name": "js_error",
                "datetime": "2023-12-09T09:50:46.136945094Z",
                "data": [("trace_id", 123), ("exception", "ERROR"), ("is_error", true), ("amount", 3.5), ("datetime", "2023-12-11 09:19:32.827321506")]
            },
            {
                "log_name": "browser_report",
                "datetime": "2023-12-09T09:50:46.136945094Z",
                "data": [("csp_violation", "cross origin resource"), ("datetime", "2023-12-11 09:19:32.827321506")]
            }
        ]});
        tokio::time::sleep(Duration::from_secs(1)).await;
        let (body, response) = client
            .post_json(url.clone(), vec![], &payload)
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            200,
            "correct KV request should be accepted"
        );
        let kv_response: Response = serde_json::from_slice(&body).unwrap();

        assert_eq!(
            kv_response.sheet_urls.len(),
            2,
            "2 sheets should be created: for `js_error` and `browser_report`"
        );

        let invalid_payload = json!({"rows": [
            {
                "log_name": "js error",
                "datetime": "2023-12-09T09:50:46.136945094Z",
                "data": [("trace_id", 123)]
            }
        ]});

        let (_, response) = client
            .post_json(url, vec![], &invalid_payload)
            .await
            .unwrap();

        assert_eq!(
            response.status(),
            400,
            "incorrect KV request should be rejected"
        );

        shutdown
            .send(1)
            .expect("test assert: kv service should run when shutdown signal is sent");

        service.await.unwrap();

        let all_sheets = storage
            .google()
            .sheets_filtered_by_metadata("spreadsheet1", &Metadata::new(vec![]))
            .await
            .unwrap();
        assert_eq!(
            all_sheets.len(),
            2,
            "2 sheets should be created: for `js_error` and `browser_report`"
        );
        assert!(
            all_sheets[0].title().contains("js_error")
                || all_sheets[1].title().contains("js_error")
        );
        assert!(
            all_sheets[0].title().contains("browser_report")
                || all_sheets[1].title().contains("browser_report")
        );
        assert_eq!(
            all_sheets[0].row_count(),
            Some(1 + 1), // one row for data and one row for headers
            "sheet contains all data from the request"
        );
    }

    #[test]
    fn kv_valid_request() {
        let data = json!({"rows": [
            {
                "log_name": "js_error",
                "datetime": "2023-12-09T09:50:46.136945094Z",
                "data": [("trace_id", 123), ("exception", "ERROR"), ("is_error", true), ("amount", 3.5), ("datetime", "2023-12-11 09:19:32.827321506")]
            },
            {
                "log_name": "browser_report",
                "datetime": "2023-12-09T09:50:46.136945094Z",
                "data": [("csp_violation", "cross origin resource"), ("datetime", "2023-12-11 09:19:32.827321506")]
            }
        ]});

        let r: Request = serde_json::from_value(data)
            .expect("test assert: can deserialize proper kv request payload");
        r.validate().unwrap();
        assert_eq!(r.rows[0].log_name, "js_error".to_string());
        assert_eq!(
            r.rows[0].datetime,
            "2023-12-09T09:50:46.136945094Z"
                .parse::<DateTime<Utc>>()
                .unwrap()
        );
        assert_eq!(r.rows[1].log_name, "browser_report".to_string());
        match r.rows[0].data[0].1 {
            Value::Integer(_) => {}
            _ => panic!("test assert: int should be parsed as integer"),
        }
        match r.rows[0].data[1].1 {
            Value::Text(_) => {}
            _ => panic!("test assert: text should be parsed as text"),
        }
        match r.rows[0].data[2].1 {
            Value::Bool(_) => {}
            _ => panic!("test assert: bool should be parsed as bool"),
        }
        match r.rows[0].data[3].1 {
            Value::Number(_) => {}
            _ => panic!("test assert: float should be parsed as number"),
        }
        match r.rows[0].data[4].1 {
            Value::Datetime(_) => {}
            _ => panic!("test assert: datetime should be parsed as naive datetime"),
        }
    }

    #[test]
    #[should_panic(expected = "min_items: 1")]
    fn kv_empty_rows() {
        let data = json!({"rows": []});
        let r: Request = serde_json::from_value(data).unwrap();
        r.validate().unwrap();
    }

    #[test]
    #[should_panic(expected = "^[a-zA-Z_][a-zA-Z0-9_]*$")]
    fn kv_invalid_log_name() {
        let data = json!({"rows": [
            {
                "log_name": "js error",
                "datetime": "2023-12-09T09:50:46.136945094Z",
                "data": [("trace_id", 123)]
            }
        ]});

        let r: Request = serde_json::from_value(data).unwrap();
        r.validate().unwrap();
    }

    #[test]
    #[should_panic(expected = "reserved name")]
    fn kv_reserved_log_name() {
        let data = json!({"rows": [
            {
                "log_name": RULES_LOG_NAME,
                "datetime": "2023-12-09T09:50:46.136945094Z",
                "data": [("trace_id", 123)]
            }
        ]});

        let r: Request = serde_json::from_value(data).unwrap();
        r.validate().unwrap();
    }

    #[test]
    #[should_panic(expected = "min_items: 1")]
    fn kv_empty_data() {
        let data = json!({"rows": [
            {
                "log_name": "js_error",
                "datetime": "2023-12-09T09:50:46.136945094Z",
                "data": []
            }
        ]});

        let r: Request = serde_json::from_value(data).unwrap();
        r.validate().unwrap();
    }

    #[test]
    #[should_panic(expected = "`data` should have unique keys")]
    fn kv_duplicate_keys() {
        let data = json!({"rows": [
            {
                "log_name": "js_error",
                "datetime": "2023-12-09T09:50:46.136945094Z",
                "data": [("trace_id", 123), ("exception", "ERROR"), ("is_error", true), ("trace_id", 3.5), ("datetime", "2023-12-11 09:19:32.827321506")]
            }
        ]});

        let r: Request = serde_json::from_value(data).unwrap();
        r.validate().unwrap();
    }
}
