//! Hydra errors

use std::fmt;
use tonic;

#[derive(Debug)]
pub enum Error {
    /// something in OpenSSL went wrong
    OpenSslError(String),
    /// IoError
    IoError(String),
    /// some size mismatch (e.g. for keys)
    SizeMismatch(String),
    /// error due to wrong user input
    InputError(String),
    /// something external went wrong
    ExternalError(String),
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::OpenSslError(msg) => write!(f, "OpenSSL error: {}", msg),
            Error::IoError(msg) => write!(f, "IO error: {}", msg),
            Error::SizeMismatch(msg) => write!(f, "Size mismatch: {}", msg),
            Error::InputError(msg) => write!(f, "Input error: {}", msg),
            Error::ExternalError(msg) => write!(f, "External error: {}", msg),
        }
    }
}

impl std::convert::From<Error> for tonic::Status {
    fn from(e: Error) -> Self {
        tonic::Status::new(tonic::Code::Internal, e.to_string())
    }
}

impl std::convert::From<openssl::error::ErrorStack> for Error {
    fn from(stack: openssl::error::ErrorStack) -> Self {
        let mut msg = "[".to_string();
        for e in stack.errors() {
            msg.push_str(&format!("{}, ", e));
        }
        msg.push_str("]");
        Error::OpenSslError(msg)
    }
}

impl std::convert::From<hkdf::InvalidLength> for Error {
    fn from(e: hkdf::InvalidLength) -> Self {
        Error::InputError(e.to_string())
    }
}

impl std::convert::From<tonic::transport::Error> for Error {
    fn from(e: tonic::transport::Error) -> Self {
        Error::ExternalError(e.to_string())
    }
}

impl std::convert::From<tokio::io::Error> for Error {
    fn from(e: tokio::io::Error) -> Self {
        Error::IoError(e.to_string())
    }
}
