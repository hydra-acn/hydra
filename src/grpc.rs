//! shared gRPC functionality

use tonic::{Code, Status};

#[macro_export]
/// given the identifiers of the service type, its state type and the server type (from tonic),
/// this macro generates the boilerplate that
/// * defines the service struct, which in turn wraps the state within an Arc
/// * defines functions to spawn the service, given the state variable (wrapped in an Arc),
///   the socket address and optionally a signal (a future that completes) to stop the service
macro_rules! define_grpc_service {
    ($service_type:ident, $state_type:ident, $server_type:ident) => {
        pub struct $service_type {
            state: std::sync::Arc<$state_type>,
        }

        impl std::ops::Deref for Service {
            type Target = std::sync::Arc<$state_type>;

            fn deref(&self) -> &Self::Target {
                &self.state
            }
        }

        pub fn spawn_service(
            state: std::sync::Arc<State>,
            addr: std::net::SocketAddr,
        ) -> tokio::task::JoinHandle<Result<(), tonic::transport::Error>> {
            spawn_service_with_shutdown::<futures_util::future::Ready<()>>(
                state.clone(),
                addr,
                None,
            )
        }

        pub fn spawn_service_with_shutdown<F: std::future::Future<Output = ()> + Send + 'static>(
            state: std::sync::Arc<State>,
            addr: std::net::SocketAddr,
            shutdown_signal: Option<F>,
        ) -> tokio::task::JoinHandle<Result<(), tonic::transport::Error>> {
            let service = $service_type {
                state: state.clone(),
            };
            let builder =
                tonic::transport::Server::builder().add_service($server_type::new(service));

            match shutdown_signal {
                Some(s) => tokio::spawn(builder.serve_with_shutdown(addr, s)),
                None => tokio::spawn(builder.serve(addr)),
            }
        }
    };
}

#[macro_export]
/// convert Result<S, F> to Result<S, tonic::Status> using the given error code and message and
/// unwrap with "?"
macro_rules! rethrow_as {
    ($res:expr, $code:expr, $msg:expr) => {
        match $res {
            Ok(r) => Ok(r),
            Err(e) => {
                log::warn!("{}: {:?}", $msg, e);
                Err(tonic::Status::new($code, $msg))
            }
        }?
    };
}

#[macro_export]
/// convert Result<S, F> to Result<S, tonic::Status> with "internal" error code and unwrap with "?"
macro_rules! rethrow_as_internal {
    ($res:expr, $msg:expr) => {
        crate::rethrow_as!($res, tonic::Code::Internal, $msg)
    };
}

#[macro_export]
/// convert Result<S, F> to Result<S, tonic::Status> with "invalid argument" error code and unwrap
/// with "?"
macro_rules! rethrow_as_invalid {
    ($res:expr, $msg:expr) => {
        crate::rethrow_as!($res, tonic::Code::InvalidArgument, $msg)
    };
}

#[macro_export]
/// unwrap Option<T> or throw tonic::Status using the given error code and message with "?"
macro_rules! unwrap_or_throw {
    ($res:expr, $code:expr, $msg:expr) => {
        match $res {
            Some(r) => Ok(r),
            None => {
                log::warn!("Unwrap failed, throwing: {}", $msg);
                Err(tonic::Status::new($code, $msg))
            }
        }?
    };
}

#[macro_export]
/// unwrap Option<T> or throw tonic::Status with "internal" error using the given message with "?"
macro_rules! unwrap_or_throw_internal {
    ($res:expr, $msg:expr) => {
        crate::unwrap_or_throw!($res, tonic::Code::Internal, $msg)
    };
}

#[macro_export]
/// unwrap Option<T> or throw tonic::Status with "invalid argument" error using the given message
/// with "?"
macro_rules! unwrap_or_throw_invalid {
    ($res:expr, $msg:expr) => {
        crate::unwrap_or_throw!($res, tonic::Code::InvalidArgument, $msg)
    };
}

/// convert bool to Result<(), tonic::Status>, with "invalid argument" error code
pub fn valid_request_check(check: bool, msg: &str) -> Result<(), Status> {
    match check {
        true => Ok(()),
        false => {
            log::warn!("{}", msg);
            Err(Status::new(Code::InvalidArgument, msg))
        }
    }
}
