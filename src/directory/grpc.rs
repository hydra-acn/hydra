use futures_util::future;
use log::*;
use std::collections::VecDeque;
use std::future::Future;
use std::ops::Deref;
use std::sync::Arc;
use tonic::transport::Server;
use tonic::{Code, Request, Response, Status};

use super::state::{key_exchange, Mix, State};
use crate::crypto::key::Key;
use crate::crypto::x448;
use crate::tonic_directory::directory_server::DirectoryServer;
use crate::tonic_directory::*;

pub struct Service {
    state: Arc<State>,
}

impl Service {
    pub fn new(state: Arc<State>) -> Self {
        Service { state: state }
    }
}

impl Deref for Service {
    type Target = Arc<State>;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

pub fn spawn_service(
    state: Arc<State>,
    addr: std::net::SocketAddr,
) -> tokio::task::JoinHandle<Result<(), tonic::transport::Error>> {
    spawn_service_with_shutdown::<future::Ready<()>>(state.clone(), addr, None)
}

pub fn spawn_service_with_shutdown<F: Future<Output = ()> + Send + 'static>(
    state: Arc<State>,
    addr: std::net::SocketAddr,
    shutdown_signal: Option<F>,
) -> tokio::task::JoinHandle<Result<(), tonic::transport::Error>> {
    let service = Service::new(state.clone());
    let builder = Server::builder().add_service(DirectoryServer::new(service));

    match shutdown_signal {
        Some(s) => tokio::spawn(builder.serve_with_shutdown(addr, s)),
        None => tokio::spawn(builder.serve(addr)),
    }
}

macro_rules! rethrow_as {
    ($res:expr, $code:expr, $msg:expr) => {
        match $res {
            Ok(r) => Ok(r),
            Err(e) => {
                warn!("{}: {:?}", $msg, e);
                Err(Status::new($code, $msg))
            }
        }?
    };
}

macro_rules! rethrow_as_internal {
    ($res:expr, $msg:expr) => {
        rethrow_as!($res, Code::Internal, $msg)
    };
}

macro_rules! _rethrow_as_invalid {
    ($res:expr, $msg:expr) => {
        rethrow_as!($res, Code::InvalidArgument, $msg)
    };
}

fn validity_check(check: bool, msg: &str) -> Result<(), Status> {
    match check {
        true => Ok(()),
        false => {
            warn!("{}", msg);
            Err(Status::new(Code::InvalidArgument, msg))
        }
    }
}

#[tonic::async_trait]
impl directory_server::Directory for Service {
    async fn register(
        &self,
        req: Request<RegisterRequest>,
    ) -> Result<Response<RegisterReply>, Status> {
        let msg = req.into_inner();
        let pk_mix = Key::move_from_vec(msg.public_dh);
        let (pk, s) = key_exchange(&pk_mix)?;

        let fingerprint = msg.fingerprint;
        let addr = crate::net::ip_addr_from_vec(&msg.address)?;
        // TODO in nightly rust, there is a complete is_global() (routable)
        validity_check(!addr.is_loopback(), "Invalid IP address")?;
        validity_check(msg.entry_port <= std::u16::MAX as u32, "Port is not valid")?;
        validity_check(msg.relay_port <= std::u16::MAX as u32, "Port is not valid")?;

        {
            let mut mix_map = rethrow_as_internal!(self.mix_map.lock(), "Could not acquire a lock");

            // check if mix already exists
            validity_check(!mix_map.contains_key(&fingerprint), "Already registered")?;

            let mix = Mix {
                fingerprint: fingerprint.clone(),
                shared_key: s,
                addr,
                entry_port: msg.entry_port as u16, // checked range above
                relay_port: msg.relay_port as u16, // checked range above
                dh_queue: VecDeque::new(),
            };

            mix_map.insert(fingerprint.clone(), mix);
        }

        let reply = RegisterReply {
            public_dh: pk.clone_to_vec(),
        };
        info!("Registered new mix: {}", &fingerprint);
        Ok(Response::new(reply))
    }

    async fn add_static_dh(&self, req: Request<DhMessage>) -> Result<Response<DhReply>, Status> {
        let msg = req.into_inner();
        let fingerprint = msg.fingerprint.clone();

        let epoch_no;
        let pk = Key::move_from_vec(msg.public_dh);
        validity_check(pk.len() == x448::POINT_SIZE, "x448 pk has wrong size")?;

        {
            let mut mix_map = rethrow_as_internal!(self.mix_map.lock(), "Could not acquire a lock");

            validity_check(mix_map.contains_key(&fingerprint), "Not registered")?;
            let mix = mix_map
                .get_mut(&fingerprint)
                .expect("Checked existance before");

            {
                let next_free_epoch_no = rethrow_as_internal!(
                    self.next_free_epoch_no.lock(),
                    "Could not acquire a lock"
                );
                epoch_no = *next_free_epoch_no + (mix.dh_queue.len() as u32);
            }
            mix.dh_queue.push_back(pk);
        }

        let reply = DhReply {
            counter: msg.counter,
            epoch_no: epoch_no,
        };

        info!("Added new public DH key for mix {}", &fingerprint);
        Ok(Response::new(reply))
    }

    async fn query_directory(
        &self,
        req: Request<DirectoryRequest>,
    ) -> Result<Response<DirectoryReply>, Status> {
        let epoch_queue = rethrow_as_internal!(self.epochs.read(), "Acquiring a lock failed");
        let mut epoch_infos = Vec::new();
        let min_epoch_no = req.into_inner().min_epoch_no;
        for epoch in epoch_queue.iter() {
            if epoch.epoch_no >= min_epoch_no {
                epoch_infos.push(epoch.clone());
            }
        }

        let reply = DirectoryReply {
            epochs: epoch_infos,
        };
        Ok(Response::new(reply))
    }
}
