use futures_util::future;
use log::*;
use std::collections::VecDeque;
use std::ops::Deref;
use tonic::transport::Server;
use tonic::{Code, Request, Response, Status};

use super::state::{key_exchange, Mix, State};
use crate::crypto::key::Key;
use crate::crypto::x448;
use crate::grpc::valid_request_check;
use crate::tonic_directory::directory_server::DirectoryServer;
use crate::tonic_directory::*;
use crate::{define_grpc_service, rethrow_as_internal};

define_grpc_service!(Service, State, DirectoryServer);

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
        valid_request_check(!addr.is_loopback(), "Invalid IP address")?;
        valid_request_check(msg.entry_port <= std::u16::MAX as u32, "Port is not valid")?;
        valid_request_check(msg.relay_port <= std::u16::MAX as u32, "Port is not valid")?;

        {
            let mut mix_map = rethrow_as_internal!(self.mix_map.lock(), "Could not acquire a lock");

            // check if mix already exists
            valid_request_check(!mix_map.contains_key(&fingerprint), "Already registered")?;

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
        valid_request_check(pk.len() == x448::POINT_SIZE, "x448 pk has wrong size")?;

        {
            let mut mix_map = rethrow_as_internal!(self.mix_map.lock(), "Could not acquire a lock");

            valid_request_check(mix_map.contains_key(&fingerprint), "Not registered")?;
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
