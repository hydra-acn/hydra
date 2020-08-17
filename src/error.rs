//! Hydra errors

use std::fmt;
use tonic;

#[derive(Debug, PartialEq)]
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
    /// something was None that should not have been
    NoneError(String),
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
            Error::NoneError(msg) => write!(f, "None error: {}", msg),
        }
    }
}

/// Helper for returning an `Error` if condition `b` is not met, using error `type` (from `Error`
/// enum). `args` after `type` are passed to `format!(...)` to construct the error message.
#[macro_export]
macro_rules! assert_as_err {
    ($b:expr, $type:expr $(, $args:expr)*) => {
        if !$b {
            return Err($type(format!($($args, )*)));
        }
    };
}

/// Specializing assert_as_err
#[macro_export]
macro_rules! assert_as_io_err {
    ($b:expr $(, $args:expr)*) => { crate::assert_as_err!($b, Error::IoError $(, $args)*) };
}

/// Specializing assert_as_err
#[macro_export]
macro_rules! assert_as_size_err {
    ($b:expr $(, $args:expr)*) => { crate::assert_as_err!($b, Error::SizeMismatch $(, $args)*) };
}

/// Specializing assert_as_err
#[macro_export]
macro_rules! assert_as_input_err {
    ($b:expr $(, $args:expr)*) => { crate::assert_as_err!($b, Error::InputError $(, $args)*) };
}

/// Specializing assert_as_err
#[macro_export]
macro_rules! assert_as_external_err {
    ($b:expr $(, $args:expr)*) => { crate::assert_as_err!($b, Error::ExternalError $(, $args)*) };
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
        Error::IoError(e.to_string())
    }
}

impl std::convert::From<tonic::Status> for Error {
    fn from(e: tonic::Status) -> Self {
        Error::IoError(e.to_string())
    }
}

impl std::convert::From<tokio::io::Error> for Error {
    fn from(e: tokio::io::Error) -> Self {
        Error::IoError(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_assert_macros() {
        assert!(err_fun(true).unwrap_err() == Error::IoError("Reasons: specific".to_string()));
        assert!(err_fun(false).unwrap_err() == Error::InputError("Reasons: foo, 42".to_string()));
        ok_fun().expect("Should return an Ok");
    }

    fn err_fun(specific: bool) -> Result<(), Error> {
        if specific {
            assert_as_io_err!(false, "Reasons: {}", "specific");
        } else {
            assert_as_err!(false, Error::InputError, "Reasons: {}, {}", "foo", 42);
        }
        Ok(())
    }

    fn ok_fun() -> Result<(), Error> {
        assert_as_size_err!(0 == 0, "Foo {}", "bar");
        assert_as_io_err!(true, "Foobar");
        assert_as_input_err!(true, "Foobar");
        assert_as_external_err!(true, "Foobar");
        Ok(())
    }
}
