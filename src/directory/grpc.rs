use futures_util::StreamExt;
use hmac::{Hmac, Mac, NewMac};
use log::*;
use sha2::Sha256;
use std::collections::HashMap;
use std::collections::{BTreeMap, BTreeSet};
use tonic::{Request, Response, Status};

use crate::crypto::key::{hkdf_sha256, Key};
use crate::crypto::{x25519, x448};
use crate::defs::{DIR_AUTH_KEY_INFO, DIR_AUTH_KEY_SIZE, DIR_AUTH_UNREGISTER};
use crate::epoch::{current_epoch_no, EpochNo};
use crate::grpc::macros::valid_request_check;
use crate::tonic_directory::directory_server::DirectoryServer;
use crate::tonic_directory::*;
use crate::{
    define_grpc_service, rethrow_as_internal, rethrow_as_invalid, unwrap_or_throw_invalid,
};

use super::state::{key_exchange, Mix, State};

define_grpc_service!(Service, State, DirectoryServer);

#[tonic::async_trait]
impl directory_server::Directory for Service {
    async fn register(
        &self,
        req: Request<RegisterRequest>,
    ) -> Result<Response<RegisterReply>, Status> {
        let msg = req.into_inner();
        let pk_mix = Key::move_from_vec(msg.public_dh);
        let (pk, shared_secret) = key_exchange(&pk_mix)?;
        let auth_key = hkdf_sha256(
            &shared_secret,
            None,
            Some(DIR_AUTH_KEY_INFO),
            DIR_AUTH_KEY_SIZE,
        )?;

        let fingerprint = msg.fingerprint;
        let addr = crate::net::ip_addr_from_slice(&msg.address)?;
        // TODO security check that request is from the announced address (probably not possible in
        // the current version of Tonic)
        // TODO in nightly rust, there is a complete is_global() (routable)
        valid_request_check(!addr.is_loopback(), "Invalid IP address")?;
        valid_request_check(msg.entry_port <= std::u16::MAX as u32, "Port is not valid")?;
        valid_request_check(msg.relay_port <= std::u16::MAX as u32, "Port is not valid")?;
        valid_request_check(
            msg.rendezvous_port <= std::u16::MAX as u32,
            "Port is not valid",
        )?;

        {
            let mut mix_map = rethrow_as_internal!(self.mix_map.lock(), "Could not acquire a lock");

            // check if mix already exists
            valid_request_check(
                !mix_map.contains_key(&fingerprint),
                "Fingerprint already registered",
            )?;

            let mix = Mix {
                fingerprint: fingerprint.clone(),
                auth_key,
                addr,
                entry_port: msg.entry_port as u16, // checked range above
                relay_port: msg.relay_port as u16, // checked range above
                rendezvous_port: msg.rendezvous_port as u16, // checked range above
                dh_map: BTreeMap::new(),
                last_counter: None,
            };

            mix_map.insert(fingerprint.clone(), mix);
        }

        let reply = RegisterReply {
            public_dh: pk.clone_to_vec(),
        };
        info!("Registered new mix: {}", &fingerprint);
        Ok(Response::new(reply))
    }

    async fn unregister(
        &self,
        req: Request<UnregisterRequest>,
    ) -> Result<Response<UnregisterAck>, Status> {
        let msg = req.into_inner();
        let fingerprint = msg.fingerprint;
        let auth_tag = msg.auth_tag;
        let removed;
        {
            let mut mix_map = rethrow_as_internal!(self.mix_map.lock(), "Lock failure");
            let mix = unwrap_or_throw_invalid!(mix_map.get(&fingerprint), "Not registered?");
            let mut mac = Hmac::<Sha256>::new_varkey(&mix.auth_key.borrow_raw())
                .expect("Initialising mac failed");
            mac.update(DIR_AUTH_UNREGISTER);
            rethrow_as_invalid!(mac.verify(&auth_tag), "Wrong Mac");
            removed = mix_map.remove(&fingerprint);
        }
        match removed {
            Some(_) => {
                // update epochs to not include this mix anymore
                let mut epoch_queue = rethrow_as_internal!(self.epochs.write(), "Lock failure");
                for epoch in epoch_queue.iter_mut() {
                    epoch.mixes.retain(|v| v.fingerprint != fingerprint);
                }
                info!("Unregistered mix: {}", fingerprint);
            }
            None => valid_request_check(false, "Not registered")?,
        }
        Ok(Response::new(UnregisterAck {}))
    }

    async fn add_static_dh(&self, req: Request<DhMessage>) -> Result<Response<DhReply>, Status> {
        let msg = req.into_inner();
        let fingerprint = msg.fingerprint.clone();
        let auth_tag = msg.auth_tag;
        let counter = msg.counter;
        let next_free_epoch_no;
        let pk = Key::move_from_vec(msg.public_dh);
        valid_request_check(
            pk.len() == x25519::POINT_SIZE || pk.len() == x448::POINT_SIZE,
            "pk has wrong size",
        )?;

        {
            let epoch_queue = rethrow_as_internal!(self.epochs.read(), "Lock failure");
            next_free_epoch_no = match epoch_queue.back() {
                Some(e) => e.epoch_no + 1,
                None => current_epoch_no(self.config().phase_duration()) + 1,
            }
        }
        let mut use_epoch_no;
        {
            let mut mix_map = rethrow_as_internal!(self.mix_map.lock(), "Lock failure");
            let mix = unwrap_or_throw_invalid!(mix_map.get_mut(&fingerprint), "Not registered?");
            valid_request_check(Some(counter) > mix.last_counter, "Message not fresh")?;
            let mut mac = Hmac::<Sha256>::new_varkey(mix.auth_key.borrow_raw())
                .expect("Initialising mac failed");
            mac.update(&counter.to_le_bytes());
            mac.update(pk.borrow_raw());
            rethrow_as_invalid!(mac.verify(&auth_tag), "Wrong mac");
            mix.last_counter = Some(counter);
            let allocated_epochs: BTreeSet<EpochNo> = mix.dh_map.keys().cloned().collect();
            use_epoch_no = next_free_epoch_no;
            loop {
                if allocated_epochs.contains(&use_epoch_no) {
                    use_epoch_no += 1;
                } else {
                    break;
                }
            }
            mix.dh_map.insert(use_epoch_no, pk);
            info!(
                "Added new public DH key for mix {}, assigned it epoch {}",
                &fingerprint, &use_epoch_no
            );
        }
        let reply = DhReply {
            counter: msg.counter,
            epoch_no: use_epoch_no,
        };
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

    async fn send_statistics(
        &self,
        req: Request<tonic::Streaming<MixStatistics>>,
    ) -> Result<Response<StatisticAck>, Status> {
        let mut stream = req.into_inner();

        while let Some(m) = stream.next().await {
            let msg = match m {
                Ok(msg) => msg,
                Err(e) => {
                    warn!("Error during statistic sending: {}", e);
                    continue;
                }
            };

            let epoch_no = msg.epoch_no;
            let fingerprint = msg.fingerprint.clone();
            let mac;
            {
                //generate mac
                let mut mix_map = rethrow_as_internal!(self.mix_map.lock(), "Lock failure");
                let mix =
                    unwrap_or_throw_invalid!(mix_map.get_mut(&fingerprint), "Not registered?");
                mac = Hmac::<Sha256>::new_varkey(mix.auth_key.borrow_raw())
                    .expect("Initialising mac failed");
            }

            //check mac
            rethrow_as_invalid!(msg.verify_auth_tag(mac), "Wrong mac");

            let mut stat_map = rethrow_as_internal!(self.stat_map.write(), "Lock failure");
            match stat_map.get_mut(&fingerprint) {
                Some(mix_stats) => {
                    mix_stats.insert(epoch_no, msg);
                }
                None => {
                    let mut mix_stats = HashMap::new();
                    mix_stats.insert(epoch_no, msg);
                    stat_map.insert(fingerprint, mix_stats);
                }
            }
        }
        Ok(Response::new(StatisticAck {}))
    }
}
