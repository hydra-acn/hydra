//! Helpers for handling TLS
use super::key::Key;

/// Storing private key and certificate of a TLS server.
pub struct ServerCredentials {
    key: Key,
    cert: String,
}

impl ServerCredentials {
    pub fn new(key: Key, cert: &str) -> Self {
        ServerCredentials {
            key,
            cert: cert.to_string(),
        }
    }
    pub fn key(&self) -> &Key {
        &self.key
    }
    pub fn cert(&self) -> &str {
        &self.cert
    }
}
