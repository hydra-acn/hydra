//! gRPC service for the rendezvous part of a mix.
use futures_util::stream;
use futures_util::StreamExt;
use log::*;
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use tonic::{Request, Response, Status};

use super::subscription_map::SubscriptionMap;
use crate::epoch::EpochNo;
use crate::grpc::macros::valid_request_check;
use crate::grpc::type_extensions::CellCmd;
use crate::mix::directory_client;
use crate::tonic_mix::rendezvous_server::{Rendezvous, RendezvousServer};
use crate::tonic_mix::*;
use crate::{define_grpc_service, rethrow_as_internal, unwrap_or_throw_internal};

type EpochMap = BTreeMap<EpochNo, SubscriptionMap>;

pub struct State {
    dir_client: Arc<directory_client::Client>,
    subs_per_epoch: RwLock<EpochMap>,
}

impl State {
    pub fn new(dir_client: Arc<directory_client::Client>) -> Self {
        State {
            dir_client,
            subs_per_epoch: RwLock::new(EpochMap::new()),
        }
    }
}

define_grpc_service!(Service, State, RendezvousServer);

#[tonic::async_trait]
impl Rendezvous for Service {
    async fn subscribe(
        &self,
        req: Request<SubscriptionVector>,
    ) -> Result<Response<SubscribeAck>, Status> {
        {
            let msg = req.into_inner();
            valid_request_check(msg.is_valid(), "Address or port for injection invalid")?;
            let mut map =
                rethrow_as_internal!(self.subs_per_epoch.write(), "Could not acquire lock");
            match map.get_mut(&msg.epoch_no) {
                Some(sub_map) => {
                    sub_map.subscribe(&msg);
                }
                None => {
                    let mut sub_map = SubscriptionMap::new();
                    sub_map.subscribe(&msg);
                    map.insert(msg.epoch_no, sub_map);
                }
            };
        }
        Ok(Response::new(SubscribeAck {}))
    }

    async fn publish(
        &self,
        req: Request<tonic::Streaming<Cell>>,
    ) -> Result<Response<PublishAck>, Status> {
        let mut stream = req.into_inner();

        while let Some(c) = stream.next().await {
            let cell = match c {
                Ok(cell) => cell,
                Err(e) => {
                    warn!("Error during cell processing in publish: {}", e);
                    continue;
                }
            };
            let mut host_vec = Vec::new();
            {
                let map =
                    rethrow_as_internal!(self.subs_per_epoch.read(), "Could not acquire lock");
                let current_epoch_no = unwrap_or_throw_internal!(
                    self.dir_client.current_communication_epoch_no(),
                    "Cannot get current communication epoch no."
                );
                match map.get(&current_epoch_no) {
                    Some(sub_map) => host_vec = sub_map.get_subscribers(&cell.token()),
                    None => {
                        warn!(
                            "Publish in epoch {}, but no subscriptions?",
                            current_epoch_no
                        );
                    }
                };
            }

            for host in host_vec.iter() {
                if cell.circuit_id == host.circuit_id() {
                    // don't send cells back on the same circuit, except when asked to
                    if let Some(CellCmd::Broadcast) = cell.command() {
                    } else {
                        continue;
                    }
                }
                let sock_addr = host.addr();
                let mut channels = self.dir_client.get_mix_channels(&[*sock_addr]).await;
                let channel = match channels.get_mut(&sock_addr) {
                    Some(c) => c,
                    None => {
                        warn!("Not able to get matching channel for {}", sock_addr);
                        continue;
                    }
                };
                let mut send_cell = cell.clone();
                send_cell.circuit_id = host.circuit_id();
                match channel
                    .inject(Request::new(stream::once(async { send_cell })))
                    .await
                {
                    Ok(_) => (),
                    Err(e) => {
                        warn!("Not able to inject cell: {}", e);
                        continue;
                    }
                }
            }
        }
        Ok(Response::new(PublishAck {}))
    }
}
