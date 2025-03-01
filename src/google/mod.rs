pub mod datavalue;
pub mod sheet;
pub mod spreadsheet;
use crate::http::HyperConnector;
use google_sheets4::yup_oauth2;
pub use spreadsheet::SpreadsheetAPI;
use std::collections::{hash_map::Iter, HashMap};

#[derive(Debug)]
pub struct Metadata(HashMap<String, String>);
pub const DEFAULT_FONT: &str = "Verdana";
pub const DEFAULT_FONT_TEXT: &str = "Courier New";

impl Metadata {
    pub fn new(pairs: Vec<(&'static str, String)>) -> Self {
        let inner: HashMap<String, String> =
            pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect();
        Self(inner)
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(|v| v.as_str())
    }

    pub fn insert(&mut self, key: String, value: String) {
        self.0.insert(key, value);
    }

    pub fn contains(&self, other: &Self) -> bool {
        for k in other.0.keys() {
            if self.get(k) != other.get(k) {
                return false;
            }
        }
        true
    }

    #[cfg(not(test))]
    pub fn iter(&self) -> MetadataIter {
        MetadataIter(self.0.iter())
    }
}

pub struct MetadataIter<'a>(Iter<'a, String, String>);

impl<'a> Iterator for MetadataIter<'a> {
    type Item = (&'a String, &'a String);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

impl From<HashMap<String, String>> for Metadata {
    fn from(m: HashMap<String, String>) -> Self {
        Self(m)
    }
}

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
