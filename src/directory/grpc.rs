use crate::crypto::key::Key;
use crate::directory_grpc::*;
use futures_core::stream::Stream;
use log::*;
use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::sync::Mutex;
use tonic::{Code, Request, Response, Status};

use super::state::{key_exchange, MixInfo};

pub struct Service {
    mix_map: Mutex<HashMap<String, MixInfo>>,
    _current_epoch_no: Mutex<u32>,
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

        let fingerprint = msg.fingerprint;
        let socket_addr_str = format!("{}:{}", &msg.address, &msg.port);
        let socket_addr =
            unwrap_to_invalid_req(socket_addr_str.parse(), "IP address or port invalid")?;

        {
            let mut mix_map =
                unwrap_to_internal_err(self.mix_map.lock(), "Could not acquire a lock")?;

            // check if mix already exists
            let existence_result = match mix_map.contains_key(&fingerprint) {
                false => Ok(()),
                true => Err(()),
            };
            unwrap_to_invalid_req(existence_result, "Alreadey registered")?;

            let mix_info = MixInfo {
                fingerprint: fingerprint.clone(),
                shared_key: s,
                socket_addr: socket_addr,
                dh_queue: VecDeque::new(),
            };

            mix_map.insert(fingerprint.clone(), mix_info);
        }

        let reply = RegisterReply {
            public_dh: pk.clone_to_vec(),
        };
        Ok(Response::new(reply))
    }

    async fn add_static_dh(&self, req: Request<DhMessage>) -> Result<Response<DhReply>, Status> {
        let msg = req.into_inner();
        let fingerprint = msg.fingerprint.clone();

        let epoch_no;
        let pk = Key::move_from_vec(msg.public_dh);
        {
            let mut mix_map =
                unwrap_to_internal_err(self.mix_map.lock(), "Could not acquire a lock")?;

            let existence_result = match mix_map.contains_key(&fingerprint) {
                true => Ok(()),
                false => Err(()),
            };
            unwrap_to_invalid_req(existence_result, "Not registered")?;
            let mix_info = mix_map
                .get_mut(&fingerprint)
                .expect("Checked existance before");

            {
                let next_free_epoch_no = unwrap_to_internal_err(
                    self.next_free_epoch_no.lock(),
                    "Could not acquire a lock",
                )?;
                epoch_no = *next_free_epoch_no + (mix_info.dh_queue.len() as u32);
            }
            mix_info.dh_queue.push_back(pk);
        }

        let reply = DhReply {
            counter: msg.counter,
            epoch_no: epoch_no,
        };

        Ok(Response::new(reply))
    }

    type QueryDirectoryStream =
        Pin<Box<dyn Stream<Item = Result<DirectoryReply, Status>> + Send + Sync + 'static>>;

    async fn query_directory(
        &self,
        _req: Request<DirectoryRequest>,
    ) -> Result<Response<Self::QueryDirectoryStream>, Status> {
        unimplemented!();
    }
}

// TODO avoid code duplication?
fn unwrap_to_invalid_req<S, T>(res: Result<S, T>, msg: &str) -> Result<S, Status>
where
    T: std::fmt::Debug,
{
    match res {
        Ok(r) => Ok(r),
        Err(e) => {
            warn!("{}: {:?}", msg, e);
            Err(Status::new(Code::InvalidArgument, msg))
        }
    }
}

fn unwrap_to_internal_err<S, T>(res: Result<S, T>, msg: &str) -> Result<S, Status>
where
    T: std::fmt::Debug,
{
    match res {
        Ok(r) => Ok(r),
        Err(e) => {
            error!("{}: {:?}", msg, e);
            Err(Status::new(Code::Internal, msg))
        }
    }
}
