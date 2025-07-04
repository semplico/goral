pub mod configuration;

use crate::configuration::APP_NAME;
use crate::google::datavalue::{Datarow, Datavalue};
use crate::http::{HttpClient, Uri};
use crate::messenger::configuration::MessengerConfig;
use crate::notifications::{MessengerApi, Notification, Sender};
use crate::services::healthcheck::configuration::{
    scrape_push_rule, Healthcheck, Liveness as LivenessConfig, LivenessType,
};
use crate::services::{Data, Service, TaskResult};
use crate::storage::AppendableLog;
use crate::Shared;
use async_trait::async_trait;
use chrono::{DateTime, Utc};

use std::fmt::{self, Debug, Display};
use std::net::SocketAddr;
use std::process::Stdio;
use std::result::Result as StdResult;
use std::str::FromStr;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};
use tokio::net::TcpSocket;
use tokio::process::Command;
use tokio::sync::mpsc::{self, error::TrySendError};
use tokio::task::JoinHandle;
use tonic::transport::Endpoint;
use tonic_health::pb::{
    health_check_response::ServingStatus, health_client::HealthClient, HealthCheckRequest,
};
use tracing::Level;

pub const HEALTHCHECK_SERVICE_NAME: &str = "health";
const MAX_BYTES_LIVENESS_OUTPUT: usize = 1024;

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug)]
enum Probe {
    Http(Uri),
    Tcp(SocketAddr),
    Grpc(Endpoint),
    Command(Vec<String>),
}

#[derive(Clone)]
struct Liveness {
    name: Option<String>,
    initial_delay: Duration,
    period: Duration,
    timeout: Duration,
    probe: Probe,
}

impl Debug for Liveness {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(name) = self.name.as_ref() {
            write!(f, "{name} ({:?})", self.probe)
        } else {
            write!(f, "{:?}", self.probe)
        }
    }
}

impl Display for Liveness {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(name) = self.name.as_ref() {
            return write!(f, "{name}");
        }
        match &self.probe {
            Probe::Http(url) => write!(f, "{url}"),
            Probe::Grpc(endpoint) => write!(f, "{}", endpoint.uri()),
            Probe::Tcp(sock) => write!(f, "{sock}"),
            Probe::Command(c) => write!(f, "{}", c.join(" ")),
        }
    }
}

impl From<LivenessConfig> for Liveness {
    fn from(mut c: LivenessConfig) -> Self {
        let timeout = Duration::from_millis(c.timeout_ms.into());
        let endpoint = c.endpoint.take();
        let probe = match c.typ {
            LivenessType::Http => Probe::Http(
                endpoint
                    .map(|s| {
                        Uri::from_maybe_shared(s.into_bytes())
                            .expect("assert: endpoint url is validated in configuration")
                    })
                    .expect("assert: endpoint is set for http probe"),
            ),
            LivenessType::Tcp => Probe::Tcp(
                SocketAddr::from_str(
                    &endpoint
                        .expect("assert: endpoint is set for tcp probe")
                        .to_string(),
                )
                .expect("assert: tcp address is validated at configuration"),
            ),
            LivenessType::Grpc => Probe::Grpc(
                endpoint
                    .map(|s| {
                        Endpoint::from_shared(s)
                            .expect("assert: grpc endpoint is validated at configuration")
                            .connect_timeout(timeout)
                            .timeout(timeout)
                            .user_agent(APP_NAME)
                            .expect("assert: can set user agent")
                    })
                    .expect("assert: endpoint is set for grpc probe"),
            ),
            LivenessType::Command => {
                Probe::Command(c.command.expect("assert: command is set for command probe"))
            }
        };

        Self {
            name: c.name,
            initial_delay: Duration::from_secs(c.initial_delay_secs.into()),
            period: Duration::from_secs(c.period_secs.into()),
            timeout,
            probe,
        }
    }
}

#[derive(Debug)]
pub struct HealthcheckService {
    shared: Shared,
    spreadsheet_id: String,
    liveness: Vec<Liveness>,
    messenger: Option<MessengerApi>,
    push_interval: Duration,
    channel_capacity: usize,
    liveness_previous_state: Vec<Option<bool>>,
    truncate_at: f32,
}

impl HealthcheckService {
    pub fn new(shared: Shared, mut config: Healthcheck) -> HealthcheckService {
        let channel_capacity = scrape_push_rule(&config.liveness, &config.push_interval_secs)
            .expect("assert: push/scrate ratio is validated at configuration");
        let liveness_previous_state = vec![None; config.liveness.len()];
        let messenger = config
            .messenger
            .take()
            .map(|messenger_config| MessengerApi::new(messenger_config, HEALTHCHECK_SERVICE_NAME));
        Self {
            shared,
            spreadsheet_id: config.spreadsheet_id,
            liveness: config.liveness.into_iter().map(|l| l.into()).collect(),
            messenger,
            push_interval: Duration::from_secs(config.push_interval_secs.into()),
            channel_capacity,
            liveness_previous_state,
            truncate_at: config.autotruncate_at_usage_percent,
        }
    }

    async fn make_probe(liveness: &Liveness) -> StdResult<String, String> {
        tracing::trace!("making probe for {:?}", liveness);
        match &liveness.probe {
            Probe::Http(url) => {
                match url.scheme_str() {
                    Some("http") | Some("https") => {
                        // we setup a new connection for the checked app each time as a new client would do
                        let client =
                            HttpClient::lousy(MAX_BYTES_LIVENESS_OUTPUT, false, liveness.timeout);
                        match client.get_text_and_status(url.clone()).await {
                            Ok((text, _)) => Ok(text),
                            Err((text, None)) => Err(text),
                            Err((text, Some(status))) => {
                                Err(format!("status code: {status}\n\n{text}"))
                            }
                        }
                    }
                    _ => Err(format!("unknown url scheme for probe {liveness:?}")),
                }
            }
            Probe::Command(command) => {
                let mut args = command.clone();
                match Command::new(&args[0])
                    .args(&mut args[1..])
                    .stdin(Stdio::null())
                    .output()
                    .await
                {
                    Ok(output) if output.status.success() => {
                        let len = output.stdout.len().min(MAX_BYTES_LIVENESS_OUTPUT);
                        Ok(String::from_utf8_lossy(&output.stdout[..len]).to_string())
                    }
                    Ok(output) => {
                        let len = output.stderr.len().min(MAX_BYTES_LIVENESS_OUTPUT);
                        Err(String::from_utf8_lossy(&output.stderr[..len]).to_string())
                    }
                    Err(e) => Err(format!("failed to execute {command:?} with error {e}")),
                }
            }
            Probe::Tcp(addr) => {
                let socket = match addr {
                    SocketAddr::V4(_) => TcpSocket::new_v4()
                        .expect("assert: should be able to create tcp IPv4 sockets"),
                    SocketAddr::V6(_) => TcpSocket::new_v6()
                        .expect("assert: should be able to create tcp IPv6 sockets"),
                };
                socket
                    .connect(*addr)
                    .await
                    .map(|stream| {
                        format!(
                            "connected to tcp socket {}",
                            stream
                                .peer_addr()
                                .expect("assert: can obtain tcp socket peer address")
                        )
                    })
                    .map_err(|e| e.to_string())
            }
            Probe::Grpc(endpoint) => {
                let conn = endpoint.connect().await.map_err(|e| e.to_string())?;
                let mut client = HealthClient::new(conn);
                match client
                    .check(HealthCheckRequest {
                        service: "".to_string(),
                    })
                    .await
                {
                    Ok(response) => match response.into_inner().status() {
                        ServingStatus::Serving => Ok("".to_string()),
                        s => Err(s.as_str_name().to_string()),
                    },
                    Err(status) => Err(status.to_string()),
                }
            }
        }
    }

    async fn is_alive(liveness: &Liveness) -> StdResult<String, String> {
        tokio::select! {
            _ = tokio::time::sleep(liveness.timeout) => Err(format!("probe timeout {:?}", liveness.timeout)),
            res = Self::make_probe(liveness) => res
        }
    }

    fn create_datarow(
        is_alive: bool,
        text: String,
        latency: Duration,
        liveness: &Liveness,
        probe_time: DateTime<Utc>,
    ) -> Datarow {
        Datarow::new(
            liveness.to_string(),
            probe_time.naive_utc(),
            vec![
                ("is_alive".to_string(), Datavalue::Bool(is_alive)),
                (
                    "latency_ms".to_string(),
                    Datavalue::Integer(
                        u32::try_from(latency.as_millis())
                            .expect("assert: latency milliseconds fit u32"),
                    ),
                ), // SAFE: cap latency at u32::MAX
                ("output".to_string(), Datavalue::Text(text)),
            ],
        )
    }

    async fn run_check(
        is_shutdown: Arc<AtomicBool>,
        index: usize,
        liveness: Liveness,
        sender: mpsc::Sender<TaskResult>,
        send_notification: Sender,
    ) {
        tokio::time::sleep(liveness.initial_delay).await;
        let mut interval = tokio::time::interval(liveness.period);
        tracing::info!("starting check for {:?}", liveness);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let probe_time = Utc::now();
                    let start = Instant::now();
                    let result = Self::is_alive(&liveness).await
                        .map(|t| Data::Single(Self::create_datarow(true, t, start.elapsed(), &liveness, probe_time)))
                        .map_err(|t| {
                            tracing::debug!("liveness check for {:?} failed with output `{}`", liveness, t);
                            Data::Single(Self::create_datarow(false, t, start.elapsed(), &liveness, probe_time))
                        });
                    match sender.try_send(TaskResult{id: index, result}) {
                        Err(TrySendError::Full(res)) => {
                            let msg = "health messages queue is full so increase scrape interval and decrease push interval".to_string();
                            tracing::error!("{}. Cannot send liveness `{:?}` result `{:?}`", msg, liveness, res);
                            send_notification.try_error(msg);
                        },
                        Err(TrySendError::Closed(res)) => {
                            if is_shutdown.load(Ordering::Relaxed) {
                                tracing::info!("finished check for {:?}", liveness);
                                return;
                            }
                            let msg = "health messages queue has been unexpectedly closed".to_string();
                            tracing::error!("{}: cannot send liveness {:?} result {:?}", msg, liveness, res);
                            send_notification.fatal(msg).await;
                            panic!("assert: health messages queue shouldn't be closed before shutdown signal");
                        },
                        _ => {},
                    }
                }
            }
        }
    }

    async fn send_message(
        &self,
        liveness: &Liveness,
        is_alive: bool,
        datarow: &Datarow,
        log: &AppendableLog,
    ) {
        let (level, message) = if is_alive {
            (
                Level::INFO,
                format!("Liveness probe for `{liveness:?}` succeeded"),
            )
        } else {
            (Level::ERROR,
            format!(
                "Liveness probe for `{:?}` failed with an output at the [spreadsheet]({}), the sheet may be created a bit later",
                liveness, log.row_url(datarow.sheet_id(), datarow.row.expect("assert: Datarow row should be set"))))
        };
        if let Some(messenger) = self.messenger() {
            messenger.send_nonblock(Notification::new(message, level));
        }
    }
}

#[async_trait]
impl Service for HealthcheckService {
    fn name(&self) -> &'static str {
        HEALTHCHECK_SERVICE_NAME
    }

    fn spreadsheet_id(&self) -> &str {
        self.spreadsheet_id.as_str()
    }

    fn channel_capacity(&self) -> usize {
        self.channel_capacity
    }

    fn push_interval(&self) -> Duration {
        self.push_interval
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

    async fn process_task_result_on_shutdown(
        &mut self,
        result: TaskResult,
        _: &mut AppendableLog,
    ) -> Data {
        let TaskResult { id: _, result } = result;
        match result {
            Ok(data) => data,
            Err(data) => data,
        }
    }

    async fn process_task_result(&mut self, result: TaskResult, log: &mut AppendableLog) -> Data {
        let TaskResult { id, result } = result;
        let (is_alive, mut datarow) = match result {
            Ok(Data::Single(datarow)) => (true, datarow),
            Err(Data::Single(datarow)) => (false, datarow),
            _ => panic!(
                "assert: healthcheck result contains single datarow both for error and for ok"
            ),
        };
        log.plan_to_append(&mut datarow);
        if self.liveness_previous_state[id].is_none()
            || self.liveness_previous_state[id] != Some(is_alive)
        {
            self.send_message(&self.liveness[id], is_alive, &datarow, log)
                .await;
            self.liveness_previous_state[id] = Some(is_alive);
        }
        Data::Single(datarow)
    }

    async fn spawn_tasks(
        &mut self,
        is_shutdown: Arc<AtomicBool>,
        sender: mpsc::Sender<TaskResult>,
    ) -> Vec<JoinHandle<()>> {
        self.liveness
            .iter()
            .enumerate()
            .map(|(i, l)| {
                let liveness = l.clone();
                let sender = sender.clone();
                let is_shutdown = is_shutdown.clone();
                let send_notification = self.shared.send_notification.clone();
                tokio::spawn(async move {
                    Self::run_check(is_shutdown, i, liveness, sender, send_notification).await;
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::tests::{run_test_server, HEALTHY_REPLY, UNHEALTHY_REPLY};
    use prost::Message;
    use std::net::TcpListener;
    use tokio::sync::mpsc;

    #[test]
    fn liveness_name_usage() {
        let liveness = Liveness {
            name: Some("check1".to_string()),
            initial_delay: Duration::from_secs(0),
            period: Duration::from_secs(1),
            timeout: Duration::from_millis(10),
            probe: Probe::Http(Uri::from_static("http://example.com/foo")),
        };
        assert!(
            liveness.to_string().contains("check1"),
            "if name is provided, it should be used for Display: {}",
            liveness
        );
    }

    #[test]
    fn liveness_default_name_for_command() {
        let liveness = Liveness {
            name: None,
            initial_delay: Duration::from_secs(0),
            period: Duration::from_secs(1),
            timeout: Duration::from_millis(10),
            probe: Probe::Command(vec!["ls".to_string(), "-lha".to_string()]),
        };
        assert!(
            liveness.to_string().contains("ls -lha"),
            "the command itself is be used for Display if no name is provided: {}",
            liveness
        );
    }

    #[test]
    fn liveness_default_name_for_tcp() {
        let liveness = Liveness {
            name: None,
            initial_delay: Duration::from_secs(0),
            period: Duration::from_secs(1),
            timeout: Duration::from_millis(10),
            probe: Probe::Tcp("127.0.0.1:53258".parse::<SocketAddr>().unwrap()),
        };
        assert!(
            liveness.to_string().contains("127.0.0.1:53258"),
            "the tcp sock addr is be used for Display if no name is provided: {}",
            liveness
        );
    }

    #[tokio::test]
    #[should_panic(expected = "probe timeout 10ms")]
    async fn probe_timeout() {
        let liveness = Liveness {
            name: None,
            initial_delay: Duration::from_secs(0),
            period: Duration::from_secs(1),
            timeout: Duration::from_millis(10),
            probe: Probe::Http(Uri::from_static("http://nonexisting.com/foo")),
        };
        HealthcheckService::is_alive(&liveness).await.unwrap();
    }

    #[tokio::test]
    async fn http_probe() {
        let _shut = run_test_server(53254).await;

        const NUM_OF_PROBES: usize = 1;
        let (send_notification, mut notifications_receiver) = mpsc::channel(1);
        let send_notification = Sender::new(send_notification, HEALTHCHECK_SERVICE_NAME);
        let (data_sender, mut data_receiver) = mpsc::channel(NUM_OF_PROBES);
        let is_shutdown = Arc::new(AtomicBool::new(false));

        let liveness = Liveness {
            name: Some("test_check".to_string()),
            initial_delay: Duration::from_secs(0),
            period: Duration::from_secs(1),
            timeout: Duration::from_millis(200),
            probe: Probe::Http(Uri::from_static("http://127.0.0.1:53254/health")),
        };

        let notifications = tokio::spawn(async move {
            while let Some(notification) = notifications_receiver.recv().await {
                println!("Notification received: {notification:?}");
            }
        });

        let is_shutdown_clone = is_shutdown.clone();
        let checker_handle = tokio::spawn(async move {
            HealthcheckService::run_check(
                is_shutdown_clone,
                0,
                liveness,
                data_sender,
                send_notification,
            )
            .await;
        });

        is_shutdown.store(true, Ordering::Release);
        tokio::time::sleep(Duration::from_secs(3)).await;
        data_receiver.close();

        if let Some(TaskResult {
            result: Ok(Data::Single(datarow)),
            ..
        }) = data_receiver.recv().await
        {
            assert_eq!(
                datarow.keys_values().get("output"),
                Some(&Datavalue::Text(HEALTHY_REPLY.to_string()))
            );
        } else {
            panic!("test assert: at least one successful probe should be collected");
        }

        checker_handle.await.unwrap(); // checker should finish as the data channel is closed
        notifications.await.unwrap();
    }

    #[tokio::test]
    async fn http_probe_failure() {
        let _shut = run_test_server(53255).await;

        const NUM_OF_PROBES: usize = 2;
        let (send_notification, mut notifications_receiver) = mpsc::channel(NUM_OF_PROBES);
        let send_notification = Sender::new(send_notification, HEALTHCHECK_SERVICE_NAME);
        let (data_sender, mut data_receiver) = mpsc::channel(NUM_OF_PROBES);
        let is_shutdown = Arc::new(AtomicBool::new(false));

        let liveness = Liveness {
            name: Some("test_check".to_string()),
            initial_delay: Duration::from_secs(0),
            period: Duration::from_secs(1),
            timeout: Duration::from_millis(300),
            probe: Probe::Http(Uri::from_static("http://127.0.0.1:53255/unhealthy")),
        };

        let notifications = tokio::spawn(async move {
            while let Some(notification) = notifications_receiver.recv().await {
                println!("Notification received: {notification:?}");
            }
        });

        let is_shutdown_clone = is_shutdown.clone();
        let checker_handle = tokio::spawn(async move {
            HealthcheckService::run_check(
                is_shutdown_clone,
                0,
                liveness,
                data_sender,
                send_notification,
            )
            .await;
        });

        is_shutdown.store(true, Ordering::Release);
        tokio::time::sleep(Duration::from_secs(3)).await;
        data_receiver.close();
        let expected = format!("status code: 500\n\n{UNHEALTHY_REPLY}");

        if let Some(TaskResult {
            result: Err(Data::Single(datarow)),
            ..
        }) = data_receiver.recv().await
        {
            assert_eq!(
                datarow.keys_values().get("output"),
                Some(&Datavalue::Text(expected.clone()))
            );
        } else {
            panic!("test assert: at least one unsuccessful probe should be collected");
        }

        if let Some(TaskResult {
            result: Err(Data::Single(datarow)),
            ..
        }) = data_receiver.recv().await
        {
            assert_eq!(
                datarow.keys_values().get("output"),
                Some(&Datavalue::Text(expected))
            );
        } else {
            panic!("test assert: second unsuccessful probe should be collected");
        }

        checker_handle.await.unwrap(); // checker should finish as the data channel is closed
        notifications.await.unwrap();
    }

    #[tokio::test]
    async fn http_probe_initial_delay() {
        let delay = Duration::from_millis(100);
        let cloned_delay = delay;
        let _shut = tokio::spawn(async move {
            tokio::time::sleep(cloned_delay).await;
            run_test_server(53256).await
        });

        const NUM_OF_PROBES: usize = 1;
        let (send_notification, mut notifications_receiver) = mpsc::channel(1);
        let send_notification = Sender::new(send_notification, HEALTHCHECK_SERVICE_NAME);
        let (data_sender, mut data_receiver) = mpsc::channel(NUM_OF_PROBES);
        let is_shutdown = Arc::new(AtomicBool::new(false));

        let liveness = Liveness {
            name: Some("test_check".to_string()),
            initial_delay: delay,
            period: Duration::from_secs(1),
            timeout: Duration::from_millis(200),
            probe: Probe::Http(Uri::from_static("http://127.0.0.1:53256/health")),
        };

        let notifications = tokio::spawn(async move {
            while let Some(notification) = notifications_receiver.recv().await {
                println!("Notification received: {notification:?}");
            }
        });

        let is_shutdown_clone = is_shutdown.clone();
        let checker_handle = tokio::spawn(async move {
            HealthcheckService::run_check(
                is_shutdown_clone,
                0,
                liveness,
                data_sender,
                send_notification,
            )
            .await;
        });

        is_shutdown.store(true, Ordering::Release);
        tokio::time::sleep(Duration::from_secs(3)).await;
        data_receiver.close();

        if let Some(TaskResult {
            result: Ok(Data::Single(datarow)),
            ..
        }) = data_receiver.recv().await
        {
            assert_eq!(
                datarow.keys_values().get("output"),
                Some(&Datavalue::Text(HEALTHY_REPLY.to_string()))
            );
            let Datavalue::Integer(latency) = *datarow.keys_values().get("latency_ms").unwrap()
            else {
                panic!("latency is an integer in ms")
            };
            assert!(latency < 200);
        } else {
            panic!("test assert: at least one successful probe should be collected");
        }

        checker_handle.await.unwrap(); // checker should finish as the data channel is closed
        notifications.await.unwrap();
    }

    #[tokio::test]
    async fn command_probe() {
        // echo command is common for Unix and Windows platforms
        let liveness = Liveness {
            name: None,
            initial_delay: Duration::from_secs(0),
            period: Duration::from_secs(1),
            timeout: Duration::from_millis(30),
            probe: Probe::Command(vec!["echo".to_string(), "goral".to_string()]),
        };
        // we trim as Windows and Unix have different line separators
        assert_eq!(
            HealthcheckService::is_alive(&liveness)
                .await
                .unwrap()
                .trim(),
            "goral".to_string()
        );
    }

    #[tokio::test]
    async fn tcp_probe() {
        let server = tokio::task::spawn_blocking(|| {
            let listener = TcpListener::bind("127.0.0.1:53257")
                .expect("test assert: should be able to create a listening tcp socket");
            //accept just one connection
            listener.incoming().next();
        });
        tokio::time::sleep(Duration::from_millis(50)).await; // some time for a thread to start
        let liveness = Liveness {
            name: None,
            initial_delay: Duration::from_millis(0),
            period: Duration::from_secs(1),
            timeout: Duration::from_millis(10),
            probe: Probe::Tcp("127.0.0.1:53257".parse::<SocketAddr>().unwrap()),
        };
        HealthcheckService::is_alive(&liveness).await.unwrap();
        server.await.unwrap();
    }

    use tonic::{transport::Server, Request, Response, Status};

    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, Message)]
    pub struct Input {}
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, Message)]
    pub struct Output {}

    #[tokio::test]
    async fn grpc_probe() {
        let service = tonic_build::manual::Service::builder()
            .name("Test")
            .package("grpc_probe_test")
            .method(
                tonic_build::manual::Method::builder()
                    .name("call")
                    .route_name("call")
                    .input_type("crate::services::healthcheck::tests::Input")
                    .output_type("crate::services::healthcheck::tests::Output")
                    .codec_path("tonic::codec::ProstCodec")
                    .build(),
            )
            .build();

        tonic_build::manual::Builder::new()
            .out_dir(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/src/services/healthcheck/"
            ))
            .build_client(false)
            .compile(&[service]);

        mod pb {
            include!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/src/services/healthcheck/grpc_probe_test.Test.rs"
            ));
        }

        #[derive(Default)]
        pub struct GrpcService {}

        #[tonic::async_trait]
        impl pb::test_server::Test for GrpcService {
            async fn call(&self, _request: Request<Input>) -> Result<Response<Output>, Status> {
                let reply = Output {};
                Ok(Response::new(reply))
            }
        }

        tokio::spawn(async {
            let (mut health_reporter, health_service) = tonic_health::server::health_reporter();

            health_reporter
                .set_serving::<pb::test_server::TestServer<GrpcService>>()
                .await;
            let addr = "[::1]:53258".parse().unwrap();
            let service = GrpcService::default();

            println!("HealthServer + TestServer listening on {}", addr);

            Server::builder()
                .add_service(health_service)
                .add_service(pb::test_server::TestServer::new(service))
                .serve(addr)
                .await
                .expect("test assert: able to run grpc service");
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        let liveness = Liveness {
            name: None,
            initial_delay: Duration::from_millis(0),
            period: Duration::from_secs(1),
            timeout: Duration::from_millis(50),
            probe: Probe::Grpc(Endpoint::from_static("http://[::1]:53258")),
        };
        HealthcheckService::is_alive(&liveness).await.unwrap();
    }
}
