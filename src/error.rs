//! Hydra errors

use std::fmt;

#[derive(Debug)]
pub enum Error {
    /// something in OpenSSL went wrong
    OpenSslError(String),
    /// some size mismatch (e.g. for keys)
    SizeMismatch(String),
    /// error due to wrong user input
    InputError(String),
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::OpenSslError(msg) => write!(f, "OpenSSL error: {}", msg),
            Error::SizeMismatch(msg) => write!(f, "Size mismatch: {}", msg),
            Error::InputError(msg) => write!(f, "Input error: {}", msg),
        }
    }
}
