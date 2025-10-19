use crate::messenger::configuration::MessengerConfig;
use crate::services::general::GENERAL_SERVICE_NAME;
use std::fmt::Debug;
use std::time::Duration;
use tokio::sync::mpsc::{self, Receiver, Sender as TokioSender, error::TrySendError};
use tracing::Level;

#[derive(Debug)]
pub struct Notification {
    pub message: String,
    pub level: Level,
}

impl Notification {
    pub fn new(message: String, level: Level) -> Self {
        Self { message, level }
    }
}

#[derive(Debug, Clone)]
pub struct Sender {
    tx: TokioSender<Notification>,
    service: &'static str,
}

impl Sender {
    pub fn new(tx: TokioSender<Notification>, service: &'static str) -> Self {
        Self { tx, service }
    }

    pub async fn send(&self, notification: Notification) {
        if self.tx.send(notification).await.is_err() {
            panic!(
                "failed to send notification - `{}` service doesn't accept notifications",
                self.service
            );
        }
    }

    pub fn send_nonblock(&self, notification: Notification) {
        match self.tx.try_send(notification) {
            Err(TrySendError::Closed(_)) => {
                panic!(
                    "failed to send notification - `{}` service doesn't accept notifications",
                    self.service
                )
            }
            Err(TrySendError::Full(n)) => {
                tracing::error!(
                    "failed to send notification {:?} - `{}` service notifications queue is full",
                    n,
                    self.service
                );
            }
            Ok(_) => (),
        }
    }

    pub async fn info(&self, message: String) {
        let notification = Notification::new(message, Level::INFO);
        self.send(notification).await
    }

    pub fn try_info(&self, message: String) {
        let notification = Notification::new(message, Level::INFO);
        self.send_nonblock(notification)
    }

    pub async fn warn(&self, message: String) {
        let notification = Notification::new(message, Level::WARN);
        self.send(notification).await
    }

    pub fn try_warn(&self, message: String) {
        let notification = Notification::new(message, Level::WARN);
        self.send_nonblock(notification)
    }

    pub async fn error(&self, message: String) {
        let notification = Notification::new(message, Level::ERROR);
        self.send(notification).await
    }

    pub fn try_error(&self, message: String) {
        let notification = Notification::new(message, Level::ERROR);
        self.send_nonblock(notification)
    }

    pub async fn fatal(&self, message: String) {
        let notification = Notification::new(message, Level::ERROR);
        self.send(notification).await;
        // for fatal errors we need some time to send error
        // It is more important to notify user via messenger than to
        // restart quickly because restart doesn't help for recovery
        // user is required to fix a problem
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

pub fn setup_messenger_channel(service: &'static str) -> (Sender, Receiver<Notification>) {
    let (tx, rx) = mpsc::channel(60); // 60 simultaneous messages is enough for any rate limiting messenger
    let tx = Sender::new(tx, service);
    (tx, rx)
}

pub fn setup_general_messenger_channel() -> (Sender, Receiver<Notification>) {
    setup_messenger_channel(GENERAL_SERVICE_NAME)
}

#[derive(Debug)]
pub struct MessengerApi {
    pub config: MessengerConfig,
    pub message_tx: Sender,
    pub message_rx: Option<Receiver<Notification>>,
}

impl MessengerApi {
    pub fn new(config: MessengerConfig, service: &'static str) -> Self {
        let (message_tx, message_rx) = setup_messenger_channel(service);
        Self {
            config,
            message_tx,
            message_rx: Some(message_rx),
        }
    }
}
