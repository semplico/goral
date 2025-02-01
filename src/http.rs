use crate::configuration::APP_NAME;
use crate::notifications;
use anyhow::{anyhow, Result};
use http_body_util::BodyExt;
use hyper::server::conn::http1;
use hyper::service::service_fn;
pub use hyper::Uri;
use hyper::{header, Method, Request, Response, StatusCode};
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::Client;
use serde::Serialize;
use std::future::Future;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::pin;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

pub fn to_body<T>(bytes: T) -> Body
where
    T: Into<hyper::body::Bytes>,
{
    use http_body_util::BodyExt;

    fn falliable(_: std::convert::Infallible) -> hyper::Error {
        unreachable!()
    }

    let bytes = bytes.into();
    Body::new(http_body_util::Full::from(bytes).map_err(falliable))
}

pub type HyperConnector = HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>;
pub type Body = http_body_util::combinators::BoxBody<hyper::body::Bytes, hyper::Error>;
pub type HttpsClient = hyper_util::client::legacy::Client<HyperConnector, Body>;

/// Http(s) client for purposes of services
pub struct HttpClient {
    pub client: HttpsClient,
    body_max_size: usize,
    error_on_oversize: bool,
}

impl HttpClient {
    pub fn lousy(
        body_max_size: usize,
        error_on_oversize: bool,
        pool_idle_timeout: Duration,
    ) -> Self {
        let client = Client::builder(hyper_util::rt::TokioExecutor::new())
            .http09_responses(true)
            .retry_canceled_requests(false)
            .pool_max_idle_per_host(1)
            .pool_idle_timeout(Some(pool_idle_timeout))
            .build(
                hyper_rustls::HttpsConnectorBuilder::new()
                    .with_native_roots()
                    .expect("assert: can build http client with native root certs")
                    .https_or_http()
                    .enable_http1()
                    .build(),
            );
        Self {
            client,
            body_max_size,
            error_on_oversize,
        }
    }

    pub fn strict(body_max_size: usize, error_on_oversize: bool) -> Self {
        let client: HttpsClient =
            hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
                .build(
                    hyper_rustls::HttpsConnectorBuilder::new()
                        .with_native_roots()
                        .expect("assert: can build http client with native root certs")
                        .https_only()
                        .enable_http1()
                        .build(),
                );
        Self {
            client,
            body_max_size,
            error_on_oversize,
        }
    }

    pub async fn get_text(&self, url: Uri) -> Result<String, String> {
        let (body, response) = self
            .request(url, Method::GET, vec![], Body::default())
            .await
            .map_err(|e| e.to_string())?;

        let text = String::from_utf8_lossy(&body).to_string();
        if response.status() >= StatusCode::OK && response.status() < StatusCode::BAD_REQUEST {
            Ok(text)
        } else {
            Err(text)
        }
    }

    pub async fn post_json<T>(
        &self,
        url: Uri,
        mut headers: Vec<(&'static str, String)>,
        payload: &T,
    ) -> Result<(Vec<u8>, Response<hyper::body::Incoming>)>
    where
        T: ?Sized + Serialize,
    {
        let req_body = to_body(serde_json::to_vec(&payload)?);
        headers.push(("content-type", "application/json".to_string()));
        self.request(url, Method::POST, headers, req_body).await
    }

    async fn request(
        &self,
        url: Uri,
        method: Method,
        headers: Vec<(&'static str, String)>,
        body: Body,
    ) -> Result<(Vec<u8>, Response<hyper::body::Incoming>)> {
        let mut req = Request::builder()
            .uri(url.clone())
            .method(method)
            .header(header::USER_AGENT, APP_NAME);

        for (header_name, header_value) in headers.into_iter() {
            req = req.header(header_name, header_value);
        }
        let request = req.body(body).expect("assert: can build empty body");
        tracing::debug!("{:?}", request);
        let mut res = self.client.request(request).await?;
        let mut size = 0;
        let mut body = vec![];
        while size < self.body_max_size {
            if let Some(next) = res.frame().await {
                let frame = next?;
                if let Some(chunk) = frame.data_ref() {
                    size += chunk.len();
                    body.extend_from_slice(chunk);
                }
            } else {
                break;
            }
        }
        if size >= self.body_max_size {
            if self.error_on_oversize {
                let msg = format!(
                    "response body for endpoint {} is greater than limit of {} bytes",
                    url, self.body_max_size
                );
                tracing::error!("{}", msg);
                return Err(anyhow!(msg));
            } else {
                tracing::warn!(
                    "output body for endpoint {:?} was truncated to {} bytes.",
                    url,
                    self.body_max_size
                );
            }
        }
        let size = size.min(self.body_max_size);
        body.truncate(size);
        Ok((body, res))
    }
}

pub async fn run_server<F, S, E>(
    addr: SocketAddr,
    name: &'static str,
    send_notification: notifications::Sender,
    shutdown: broadcast::Receiver<u16>,
    environment: E,
    router: F,
) -> JoinHandle<()>
where
    F: Fn(Request<hyper::body::Incoming>, notifications::Sender, E) -> S + Send + Copy + 'static,
    S: Future<Output = Result<Response<Body>, hyper::Error>> + Send,
    E: Clone + Send + 'static,
{
    let listener = match TcpListener::bind(addr).await {
        Ok(listener) => listener,
        Err(e) => {
            let msg = format!("{name} server cannot listen for connections `{}`", e);
            tracing::error!("{}", msg);
            send_notification.fatal(msg.clone()).await;
            panic!("{}", msg);
        }
    };
    tokio::spawn(async move {
        tracing::info!("{name} server is listening on http://{}", addr);

        loop {
            let send_notification_clone = send_notification.clone();
            let environment = environment.clone();
            let mut shutdown = shutdown.resubscribe();

            let (tcp, remote_address) = tokio::select! {
                _ = shutdown.recv() => {
                    tracing::info!("{name} server is shutting down");
                    return;
                }
                res = listener.accept() => {
                    match res {
                        Ok(res) => res,
                        Err(e) => {
                            let msg = format!("{name} server cannot accept a connection `{}`", e);
                            tracing::error!("{}", msg);
                            send_notification_clone.error(msg).await;
                            continue;
                        }
                    }
                }
            };
            let io = hyper_util::rt::TokioIo::new(Box::pin(tcp));
            tracing::info!(
                "{name} server accepted connection from {:?}",
                remote_address
            );

            tokio::task::spawn(async move {
                // Pin the connection object so we can use tokio::select! below.
                let send_notification_clone_clone = send_notification_clone.clone();
                let conn = http1::Builder::new().serve_connection(
                    io,
                    service_fn(move |req| {
                        router(req, send_notification_clone.clone(), environment.clone())
                    }),
                );
                pin!(conn);

                tokio::select! {
                    _ = shutdown.recv() => {
                        conn.as_mut().graceful_shutdown();
                    }
                    res = conn.as_mut() => {
                        if let Err(e) = res {
                            let msg = format!("error serving connection by {name} server {:?}", e);
                            tracing::error!("{}", msg);
                            send_notification_clone_clone.error(msg.to_string()).await;
                        }
                    }
                }
            });
        }
    })
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::http::to_body;
    use crate::Sender;
    use hyper::{Method, Request, Response, StatusCode};
    use tokio::sync::mpsc;

    pub const HEALTHY_REPLY: &str = "Test service is healthy";
    pub const UNHEALTHY_REPLY: &str = "Test service is unhealthy";
    const METRICS_REPLY: &str = r#"""
    # HELP example_http_request_duration_seconds The HTTP request latencies in seconds.
    # TYPE example_http_request_duration_seconds histogram
    example_http_request_duration_seconds_bucket{handler="all",parity="0",le="0.005"} 18
    example_http_request_duration_seconds_bucket{handler="all",parity="0",le="0.01"} 18
    example_http_request_duration_seconds_bucket{handler="all",parity="0",le="0.025"} 18
    example_http_request_duration_seconds_bucket{handler="all",parity="0",le="0.05"} 18
    example_http_request_duration_seconds_bucket{handler="all",parity="0",le="0.1"} 18
    example_http_request_duration_seconds_bucket{handler="all",parity="0",le="0.25"} 18
    example_http_request_duration_seconds_bucket{handler="all",parity="0",le="0.5"} 18
    example_http_request_duration_seconds_bucket{handler="all",parity="0",le="1"} 18
    example_http_request_duration_seconds_bucket{handler="all",parity="0",le="2.5"} 18
    example_http_request_duration_seconds_bucket{handler="all",parity="0",le="5"} 18
    example_http_request_duration_seconds_bucket{handler="all",parity="0",le="10"} 18
    example_http_request_duration_seconds_bucket{handler="all",parity="0",le="+Inf"} 18
    example_http_request_duration_seconds_sum{handler="all",parity="0"} 0.005173251
    example_http_request_duration_seconds_count{handler="all",parity="0"} 18
    example_http_request_duration_seconds_bucket{handler="all",parity="1",le="0.005"} 18
    example_http_request_duration_seconds_bucket{handler="all",parity="1",le="0.01"} 18
    example_http_request_duration_seconds_bucket{handler="all",parity="1",le="0.025"} 18
    example_http_request_duration_seconds_bucket{handler="all",parity="1",le="0.05"} 18
    example_http_request_duration_seconds_bucket{handler="all",parity="1",le="0.1"} 18
    example_http_request_duration_seconds_bucket{handler="all",parity="1",le="0.25"} 18
    example_http_request_duration_seconds_bucket{handler="all",parity="1",le="0.5"} 18
    example_http_request_duration_seconds_bucket{handler="all",parity="1",le="1"} 18
    example_http_request_duration_seconds_bucket{handler="all",parity="1",le="2.5"} 18
    example_http_request_duration_seconds_bucket{handler="all",parity="1",le="5"} 18
    example_http_request_duration_seconds_bucket{handler="all",parity="1",le="10"} 18
    example_http_request_duration_seconds_bucket{handler="all",parity="1",le="+Inf"} 18
    example_http_request_duration_seconds_sum{handler="all",parity="1"} 0.004740836999999999
    example_http_request_duration_seconds_count{handler="all",parity="1"} 18
    # HELP example_http_requests_total Number of HTTP requests made.
    # TYPE example_http_requests_total counter
    example_http_requests_total{handler="all"} 37
    # HELP example_http_response_size_bytes The HTTP response sizes in bytes.
    # TYPE example_http_response_size_bytes gauge
    example_http_response_size_bytes{handler="all"} 2779
    """#;

    pub async fn router(
        req: Request<hyper::body::Incoming>,
        _send_notification: Sender,
        _environment: (),
    ) -> Result<Response<Body>, hyper::Error> {
        assert_eq!(
            req.headers()
                .get(header::USER_AGENT)
                .map(|h| h.to_str().expect("test assert: header value is ascii")),
            Some(APP_NAME),
            "request from Goral should contain User-Agent header with the Goral name"
        );
        match (req.method(), req.uri().path()) {
            (&Method::GET, "/health") => Ok(Response::new(to_body(HEALTHY_REPLY.as_bytes()))),
            (&Method::GET, "/unhealthy") => Ok(Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(to_body(UNHEALTHY_REPLY.as_bytes()))
                .expect("test assert: should be able to construct response for static body")),
            (&Method::GET, "/metrics") => Ok(Response::new(to_body(METRICS_REPLY.as_bytes()))),
            (&Method::GET, "/timeout") => {
                let timeout = Duration::from_secs(1);
                tokio::time::sleep(timeout).await;
                Ok(Response::new(to_body(
                    format!("timeout {timeout:?}").into_bytes(),
                )))
            }
            _ => {
                let mut not_found = Response::default();
                *not_found.status_mut() = StatusCode::NOT_FOUND;
                Ok(not_found)
            }
        }
    }

    pub async fn run_test_server(port: u16) -> broadcast::Sender<u16> {
        let addr = ([127, 0, 0, 1], port).into();
        let (tx, mut rx) = mpsc::channel(1);
        tokio::task::spawn(async move {
            while let Some(msg) = rx.recv().await {
                println!("message {msg:?}");
            }
        });
        let send_notification = Sender::new(tx, "test");
        let (shutdown, rx) = broadcast::channel(1);
        run_server(addr, "test", send_notification, rx, (), router).await;
        shutdown
    }

    #[tokio::test]
    async fn http_client_ok() {
        let _shut = run_test_server(53260).await;
        tokio::time::sleep(Duration::from_millis(10)).await; // some time for server to start

        let client = HttpClient::lousy(HEALTHY_REPLY.len(), false, Duration::from_millis(10));
        assert_eq!(
            client
                .get_text(Uri::from_static("http://127.0.0.1:53260/health"))
                .await,
            Ok(HEALTHY_REPLY.to_string())
        );
    }

    #[tokio::test]
    async fn http_client_err() {
        let _shut = run_test_server(53261).await;
        tokio::time::sleep(Duration::from_millis(10)).await; // some time for server to start

        let client = HttpClient::lousy(UNHEALTHY_REPLY.len(), false, Duration::from_millis(10));
        assert_eq!(
            client
                .get_text(Uri::from_static("http://127.0.0.1:53261/unhealthy"))
                .await,
            Err(UNHEALTHY_REPLY.to_string())
        );
    }

    #[tokio::test]
    async fn http_client_body_truncation() {
        let _shut = run_test_server(53262).await;
        tokio::time::sleep(Duration::from_millis(10)).await; // some time for server to start

        let client = HttpClient::lousy(HEALTHY_REPLY.len() - 1, false, Duration::from_millis(10));
        assert_eq!(
            client
                .get_text(Uri::from_static("http://127.0.0.1:53262/health"))
                .await,
            Ok(HEALTHY_REPLY
                .chars()
                .take(HEALTHY_REPLY.len() - 1)
                .collect())
        );
    }

    #[tokio::test]
    async fn http_client_body_size_err() {
        let _shut = run_test_server(53263).await;
        tokio::time::sleep(Duration::from_millis(10)).await; // some time for server to start

        let client = HttpClient::lousy(HEALTHY_REPLY.len() - 1, true, Duration::from_millis(10));
        assert_eq!(client.get_text(Uri::from_static("http://127.0.0.1:53263/health")).await, Err(format!("response body for endpoint http://127.0.0.1:53263/health is greater than limit of {} bytes", HEALTHY_REPLY.len()-1)));
    }
}
