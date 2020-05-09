use log::*;
use std::collections::VecDeque;
use std::ops::Deref;
use std::sync::Arc;
use tonic::transport::Server;
use tonic::{Code, Request, Response, Status};

use super::state::{key_exchange, Mix, State};
use crate::crypto::key::Key;
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
    let service = Service::new(state.clone());
    let server = Server::builder()
        .add_service(DirectoryServer::new(service))
        .serve(addr);

    tokio::spawn(server)
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

macro_rules! rethrow_as_invalid {
    ($res:expr, $msg:expr) => {
        rethrow_as!($res, Code::InvalidArgument, $msg)
    };
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
        let socket_addr_str = format!("{}:{}", &msg.address, &msg.port);
        let socket_addr =
            rethrow_as_invalid!(socket_addr_str.parse(), "IP address or port invalid");

        {
            let mut mix_map = rethrow_as_internal!(self.mix_map.lock(), "Could not acquire a lock");

            // check if mix already exists
            let existence_result = match mix_map.contains_key(&fingerprint) {
                false => Ok(()),
                true => Err(()),
            };
            rethrow_as_invalid!(existence_result, "Already registered");

            let mix = Mix {
                fingerprint: fingerprint.clone(),
                shared_key: s,
                socket_addr: socket_addr,
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
        {
            let mut mix_map = rethrow_as_internal!(self.mix_map.lock(), "Could not acquire a lock");

            let existence_result = match mix_map.contains_key(&fingerprint) {
                true => Ok(()),
                false => Err(()),
            };
            rethrow_as_invalid!(existence_result, "Not registered");
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
        _req: Request<DirectoryRequest>,
    ) -> Result<Response<DirectoryReply>, Status> {
        let epoch_queue = rethrow_as_internal!(self.epochs.read(), "Acquiring a lock failed");
        let mut epoch_infos = Vec::new();
        for epoch in epoch_queue.iter() {
            epoch_infos.push(epoch.clone());
        }

        let reply = DirectoryReply {
            epochs: epoch_infos,
        };
        Ok(Response::new(reply))
    }
}
