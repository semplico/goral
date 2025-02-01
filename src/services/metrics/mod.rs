pub mod configuration;

use crate::google::datavalue::{Datarow, Datavalue};
use crate::http::{HttpClient, Uri};
use crate::messenger::configuration::MessengerConfig;
use crate::notifications::{MessengerApi, Notification, Sender};
use crate::services::metrics::configuration::{scrape_push_rule, Metrics};
use crate::services::{Data, Service, TaskResult};
use crate::storage::AppendableLog;
use crate::Shared;
use async_trait::async_trait;
use chrono::{DateTime, Utc};

use prometheus_parse::{Sample, Scrape, Value};
use std::cmp::Ordering as Cmp;
use std::collections::HashMap;
use std::result::Result as StdResult;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::sync::mpsc::{self, error::TrySendError};
use tokio::task::{self, JoinHandle};

pub const METRICS_SERVICE_NAME: &str = "metrics";
const MAX_BYTES_METRICS_OUTPUT: usize = 2_usize.pow(16); // ~65 KiB

#[derive(Debug, Clone)]
struct ScrapeTarget {
    index: usize,
    url: Uri,
    name: Option<String>,
    timeout: Duration,
    interval: Duration,
}

impl ScrapeTarget {
    fn name(&self) -> Option<&String> {
        self.name.as_ref()
    }
}

pub struct MetricsService {
    shared: Shared,
    messenger: Option<MessengerApi>,
    spreadsheet_id: String,
    push_interval: Duration,
    targets: Vec<ScrapeTarget>,
    channel_capacity: usize,
    truncate_at: f32,
}

impl MetricsService {
    pub fn new(shared: Shared, mut config: Metrics) -> MetricsService {
        let channel_capacity = scrape_push_rule(
            &config.target,
            &config.push_interval_secs,
            &config.scrape_interval_secs,
            &config.scrape_timeout_ms,
        )
        .expect("assert: push/scrate ratio is validated at configuration");
        let messenger = config
            .messenger
            .take()
            .map(|messenger_config| MessengerApi::new(messenger_config, METRICS_SERVICE_NAME));
        Self {
            shared,
            messenger,
            spreadsheet_id: config.spreadsheet_id,
            push_interval: Duration::from_secs(config.push_interval_secs.into()),
            targets: config
                .target
                .into_iter()
                .enumerate()
                .map(|(index, t)| {
                    let s: String = t.endpoint.into();
                    let url = Uri::from_maybe_shared(s.into_bytes())
                        .expect("assert: endpoint url is validated in configuration");
                    ScrapeTarget {
                        url,
                        index,
                        name: t.name,
                        timeout: Duration::from_millis(config.scrape_timeout_ms.into()),
                        interval: Duration::from_secs(config.scrape_interval_secs.into()),
                    }
                })
                .collect(),
            channel_capacity,
            truncate_at: config.autotruncate_at_usage_percent,
        }
    }

    // sort samples for a neat order of keys
    // histogram/summary, then count, then sum
    fn sort_samples(samples: &mut [Sample]) {
        samples.sort_unstable_by(|a, b| {
            if a.metric.ends_with("_count") && b.metric.ends_with("_sum") {
                return Cmp::Less;
            }
            if matches!(a.value, Value::Histogram(_) | Value::Summary(_))
                && (b.metric.ends_with("_sum") || b.metric.ends_with("_count"))
            {
                return Cmp::Less;
            }
            Cmp::Equal
        });
    }

    fn samples_to_datavalues(mut samples: Vec<Sample>) -> Vec<(String, Datavalue)> {
        use Value::*;
        Self::sort_samples(&mut samples);
        let first = samples
            .first()
            .expect("assert: each metric parsed should have at least one sample");
        let mut labels: Vec<(String, Datavalue)> = first
            .labels
            .iter()
            .map(|(k, v)| (k.to_string(), Datavalue::Text(v.to_string())))
            .collect();
        labels.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        let mut values: Vec<(String, Datavalue)> = samples
            .into_iter()
            .flat_map(|s| {
                let values: Vec<(String, Datavalue)> = match s.value {
                    Counter(v) | Gauge(v) | Untyped(v) => vec![(s.metric, Datavalue::Number(v))],
                    Histogram(buckets) => buckets
                        .into_iter()
                        .map(|b| (format!("le={}", b.less_than), Datavalue::Number(b.count)))
                        .collect(),
                    Summary(quantiles) => quantiles
                        .into_iter()
                        .map(|q| {
                            (
                                format!("quantile={}", q.quantile),
                                Datavalue::Number(q.count),
                            )
                        })
                        .collect(),
                };
                values
            })
            .collect();
        values.append(&mut labels);
        values
    }

    fn parse_scrape_output(
        output: String,
        scrape_time: DateTime<Utc>,
        identifier: Option<String>,
    ) -> StdResult<Vec<Datarow>, String> {
        let lines: Vec<_> = output.lines().map(|s| Ok(s.to_string())).collect();
        let Scrape { docs, samples } =
            prometheus_parse::Scrape::parse_at(lines.into_iter(), scrape_time)
                .map_err(|e| e.to_string())?;
        let mut map = HashMap::with_capacity(samples.len());

        // see https://prometheus.io/docs/practices/histograms/#count-and-sum-of-observations
        // and https://prometheus.io/docs/concepts/metric_types/#summary
        // group all samples by their base metrics,
        // labels and timestamps to produce separate rows
        samples.into_iter().for_each(|s| {
            let name = if docs.contains_key(&s.metric) {
                &s.metric
            } else if let Some((metric_basename, _)) = s.metric.rsplit_once("_count") {
                if docs.contains_key(metric_basename) {
                    metric_basename
                } else {
                    &s.metric
                }
            } else if let Some((metric_basename, _)) = s.metric.rsplit_once("_sum") {
                if docs.contains_key(metric_basename) {
                    metric_basename
                } else {
                    &s.metric
                }
            } else {
                panic!(
                    "assert: metric should either have some base metric or be a metric on its own"
                );
            };
            let mut kv: Vec<&str> = s
                .labels
                .iter()
                .flat_map(|(k, v)| [k.as_str(), v.as_str()])
                .collect();
            kv.sort_unstable();
            let key = kv.join("");
            map.entry((name.to_string(), key, s.timestamp))
                .or_insert(vec![])
                .push(s);
        });

        // Label keys are fixed in each scrape output
        // see https://prometheus.io/docs/instrumenting/writing_clientlibs/#labels
        // and https://github.com/prometheus/client_java/issues/696
        Ok(map
            .into_iter()
            .map(|((metric_name, _, timestamp), samples)| {
                let values = Self::samples_to_datavalues(samples);
                let log_name = if let Some(prefix) = identifier.as_ref() {
                    // Prmotheus metric name cannot contain `/` so we use it as a delimiter
                    // see https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels
                    format!("{prefix}/{metric_name}")
                } else {
                    metric_name
                };
                Datarow::new(log_name, timestamp.naive_utc(), values)
            })
            .collect())
    }

    async fn make_timed_scrape(
        client: &HttpClient,
        target: &ScrapeTarget,
    ) -> StdResult<String, String> {
        tokio::select! {
            _ = tokio::time::sleep(target.timeout) => Err(format!("scrape timeout {:?} for {}", target.timeout, target.url)),
            res = client.get_text(target.url.clone()) => res
        }
    }

    async fn run_scrape(
        is_shutdown: Arc<AtomicBool>,
        client: HttpClient,
        scrape_target: ScrapeTarget,
        sender: mpsc::Sender<TaskResult>,
        send_notification: Sender,
    ) {
        let mut interval = tokio::time::interval(scrape_target.interval);
        tracing::info!("starting metrics scraping for {:?}", scrape_target.url);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    tracing::trace!("metrics scrape for {:?}", scrape_target.url);
                    let scrape_time = Utc::now();
                    let scrape_result = Self::make_timed_scrape(&client, &scrape_target).await;
                    let result = match scrape_result {
                        Ok(output) => {
                            tracing::info!("starting metrics parsing for {:?}", scrape_target.url);
                            let identifier = scrape_target.name().cloned();
                            let join_result = task::spawn_blocking(move || {
                                Self::parse_scrape_output(output, scrape_time, identifier)
                            }).await;
                            tracing::info!("finished metrics parsing for {:?}", scrape_target.url);
                            match join_result {
                                Err(e) => {
                                    let msg = "failed to spawn parsing of metrics output in a separate thread".to_string();
                                    tracing::error!("{}: `{}`", msg, e);
                                    send_notification.fatal(msg).await;
                                    panic!("assert: should be able to spawn blocking tasks");
                                },
                                Ok(res) => res.map(Data::Many).map_err(|e| Data::Message(format!("error scraping metrics `{e}`"))),
                            }
                        },
                        err => {
                            tracing::debug!("metrics scrape result for {:?} is an error: {:?}", scrape_target.url, err);
                            err.map(|_| Data::Many(vec![])).map_err(Data::Message)
                        },
                    };

                    match sender.try_send(TaskResult{id: scrape_target.index, result}) {
                        Err(TrySendError::Full(res)) => {
                            let msg = "scrape messages queue is full so increase scrape interval and decrease push interval".to_string();
                            tracing::error!("{}. Cannot send scrape target `{:?}` result `{:?}`", msg, scrape_target, res);
                            send_notification.try_error(msg);
                        },
                        Err(TrySendError::Closed(res)) => {
                            if is_shutdown.load(Ordering::Relaxed) {
                                return;
                            }
                            let msg = "scrape messages queue has been unexpectedly closed".to_string();
                            tracing::error!("{}: cannot send scrape target {:?} result {:?}", msg, scrape_target, res);
                            send_notification.fatal(msg).await;
                            panic!("assert: scrape messages queue shouldn't be closed before shutdown signal");
                        },
                        _ => {},
                    }
                }
            }
        }
    }
}

#[async_trait]
impl Service for MetricsService {
    fn name(&self) -> &'static str {
        METRICS_SERVICE_NAME
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

    async fn process_task_result(&mut self, result: TaskResult, _: &AppendableLog) -> Data {
        let TaskResult { id, result } = result;
        match result {
            Ok(data) => data,
            Err(Data::Message(msg)) => {
                tracing::error!("{}", msg);
                self.send_error(format!(
                    "`{}` while scraping metrics for endpoint `{}`",
                    self.targets[id].url, msg
                ))
                .await;
                Data::Empty
            }
            _ => panic!("assert: metrics result contains either multiple datarows or error text"),
        }
    }

    async fn spawn_tasks(
        &mut self,
        is_shutdown: Arc<AtomicBool>,
        sender: mpsc::Sender<TaskResult>,
    ) -> Vec<JoinHandle<()>> {
        self.targets
            .iter()
            .map(|t| {
                let sender = sender.clone();
                let scrape_target = t.clone();
                let is_shutdown = is_shutdown.clone();
                let client = HttpClient::lousy(MAX_BYTES_METRICS_OUTPUT, true, t.interval);
                let send_notification = self.shared.send_notification.clone();
                tokio::spawn(async move {
                    Self::run_scrape(
                        is_shutdown,
                        client,
                        scrape_target,
                        sender,
                        send_notification,
                    )
                    .await;
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::tests::run_test_server;

    #[tokio::test]
    async fn single_metrics_scrape() {
        let _shut = run_test_server(53270).await;

        const NUM_OF_SCRAPES: usize = 1;
        let (send_notification, mut notifications_receiver) = mpsc::channel(1);
        let send_notification = Sender::new(send_notification, METRICS_SERVICE_NAME);
        let (data_sender, mut data_receiver) = mpsc::channel(NUM_OF_SCRAPES);
        let is_shutdown = Arc::new(AtomicBool::new(false));

        let target = ScrapeTarget {
            index: 0,
            url: Uri::from_static("http://127.0.0.1:53270/metrics"),
            name: Some("test_metrics".to_string()),
            timeout: Duration::from_millis(50),
            interval: Duration::from_secs(1),
        };

        let client = HttpClient::lousy(MAX_BYTES_METRICS_OUTPUT, true, target.interval);

        let notifications = tokio::spawn(async move {
            while let Some(notification) = notifications_receiver.recv().await {
                println!("Notification received: {notification:?}");
            }
        });

        let is_shutdown_clone = is_shutdown.clone();
        let scrape_handle = tokio::spawn(async move {
            MetricsService::run_scrape(
                is_shutdown_clone,
                client,
                target,
                data_sender,
                send_notification,
            )
            .await;
        });

        tokio::time::sleep(Duration::from_secs(u64::try_from(NUM_OF_SCRAPES).unwrap())).await;
        is_shutdown.store(true, Ordering::Release);
        data_receiver.close();

        if let Some(TaskResult {
            result: Ok(Data::Many(datarows)),
            ..
        }) = data_receiver.recv().await
        {
            assert_eq!(
                datarows[0].keys_values().get("handler"),
                Some(&Datavalue::Text("all".to_string()))
            );
        } else {
            panic!("test assert: at least one successfull scrape should be collected");
        }

        scrape_handle.await.unwrap(); // scrape should finish as the data channel is closed
        notifications.await.unwrap();
    }

    #[tokio::test]
    async fn metrics_scrape_timeout() {
        let _shut = run_test_server(53271).await;

        const NUM_OF_SCRAPES: usize = 1;
        let (send_notification, mut notifications_receiver) = mpsc::channel(1);
        let send_notification = Sender::new(send_notification, METRICS_SERVICE_NAME);
        let (data_sender, mut data_receiver) = mpsc::channel(NUM_OF_SCRAPES);
        let is_shutdown = Arc::new(AtomicBool::new(false));

        let target = ScrapeTarget {
            index: 0,
            url: Uri::from_static("http://127.0.0.1:53271/timeout"),
            name: Some("test_metrics".to_string()),
            timeout: Duration::from_millis(1),
            interval: Duration::from_secs(1),
        };

        let client = HttpClient::lousy(MAX_BYTES_METRICS_OUTPUT, true, target.interval);

        let notifications = tokio::spawn(async move {
            while let Some(notification) = notifications_receiver.recv().await {
                println!("Notification received: {notification:?}");
            }
        });

        let is_shutdown_clone = is_shutdown.clone();
        let scrape_handle = tokio::spawn(async move {
            MetricsService::run_scrape(
                is_shutdown_clone,
                client,
                target,
                data_sender,
                send_notification,
            )
            .await;
        });

        tokio::time::sleep(Duration::from_secs(u64::try_from(NUM_OF_SCRAPES).unwrap())).await;
        is_shutdown.store(true, Ordering::Release);
        data_receiver.close();

        if let Some(TaskResult {
            result: Err(Data::Message(err)),
            ..
        }) = data_receiver.recv().await
        {
            assert!(err.starts_with("scrape timeout"), "{}", err);
        } else {
            panic!("test assert: at least one timeout should be happen");
        }

        scrape_handle.await.unwrap(); // scrape should finish as the data channel is closed
        notifications.await.unwrap();
    }
}
