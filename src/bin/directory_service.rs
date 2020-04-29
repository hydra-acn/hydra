use futures_core::stream::Stream;
use hydra::crypto::key::Key;
use hydra::crypto::x448;
use hydra::directory_grpc::*;
use log::*;
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Mutex, MutexGuard};
use tokio::sync::mpsc;
use tonic::{transport::Server, Code, Request, Response, Status};

pub struct Service {
    mix_map: Mutex<HashMap<String, MixInfo>>,
    current_epoch_no: Mutex<u32>,
    next_free_epoch_no: Mutex<u32>,
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

        // create new entry for map
        let fingerprint = msg.fingerprint;
        let socket_addr = match format!("{}:{}", msg.address, msg.port).parse() {
            Ok(a) => a,
            Err(e) => {
                warn!("Mix submitted invalid address/port: {}", e);
                return Err(invalid_req("IP address or port invalid"));
            }
        };
        {
            let mut mix_map =
                unwrap_to_internal_err(self.mix_map.lock(), "Could not acquire a lock")?;

            // insert new entry if not already existant
            if mix_map.contains_key(&fingerprint) == false {
                let mix_info = MixInfo {
                    fingerprint: fingerprint.clone(),
                    shared_key: s,
                    socket_addr: socket_addr,
                    dh_queue: VecDeque::new(),
                };

                mix_map.insert(fingerprint.clone(), mix_info);
            } else {
                warn!("Mix already registerd: {}", &fingerprint);
                return Err(invalid_req("Alreadey registered"));
            }
        }

        let reply = RegisterReply {
            public_dh: pk.clone_to_vec(),
        };
        Ok(Response::new(reply))
    }

    type AddStaticDhStream =
        Pin<Box<dyn Stream<Item = Result<DhReply, Status>> + Send + Sync + 'static>>;

    async fn add_static_dh(
        &self,
        req: Request<tonic::Streaming<DhMessage>>,
    ) -> Result<Response<Self::AddStaticDhStream>, Status> {
        unimplemented!();
    }

    type QueryDirectoryStream =
        Pin<Box<dyn Stream<Item = Result<DirectoryReply, Status>> + Send + Sync + 'static>>;

    async fn query_directory(
        &self,
        req: Request<DirectoryRequest>,
    ) -> Result<Response<Self::QueryDirectoryStream>, Status> {
        unimplemented!();
    }
}

struct MixInfo {
    fingerprint: String,
    shared_key: Key,
    socket_addr: SocketAddr,
    dh_queue: VecDeque<Key>,
}

fn key_exchange(pk_mix: &Key) -> Result<(Key, Key), Status> {
    let (pk, sk) = unwrap_to_internal_err(x448::generate_keypair(), "Failed to generate key pair")?;

    let s = unwrap_to_internal_err(
        x448::generate_shared_secret(&pk_mix, &sk),
        "Failed to derive shared secret",
    )?;
    Ok((pk, s))
}

fn internal_error(msg: &str) -> Status {
    Status::new(Code::Internal, msg)
}

fn invalid_req(msg: &str) -> Status {
    Status::new(Code::InvalidArgument, msg)
}

fn unwrap_to_internal_err<S, T>(res: Result<S, T>, msg: &str) -> Result<S, Status>
where
    T: std::fmt::Debug,
{
    match res {
        Ok(r) => Ok(r),
        Err(e) => {
            error!("{}: {:?}", msg, e);
            Err(internal_error(msg))
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // TODO initialize some logging implementation
    // TODO startup server
    Ok(())
}
