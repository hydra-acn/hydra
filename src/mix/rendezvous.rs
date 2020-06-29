//! Rendezvous service of a mix
use log::*;
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use tokio::time::{delay_for, Duration};
use tonic::{Request, Response, Status};

use super::directory_client;
use crate::defs::{CircuitId, Token};
use crate::epoch::EpochNo;
use crate::net::socket_addr_from_slice;
use crate::tonic_mix::rendezvous_server::{Rendezvous, RendezvousServer};
use crate::tonic_mix::*;
use crate::{
    define_grpc_service, rethrow_as_internal, rethrow_as_invalid, unwrap_or_throw_internal,
};

#[derive(Clone, Debug, PartialEq)]
struct Endpoint {
    sock_addr: SocketAddr,
    circuit_id: CircuitId,
}

type EndpointMap = BTreeMap<Token, Vec<Endpoint>>;
type EpochMap = BTreeMap<EpochNo, EndpointMap>;

pub struct State {
    dir_client: Arc<directory_client::Client>,
    tokens_per_epoch: RwLock<EpochMap>,
}

impl State {
    pub fn new(dir_client: Arc<directory_client::Client>) -> Self {
        State {
            dir_client,
            tokens_per_epoch: RwLock::new(EpochMap::new()),
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
        let msg = req.into_inner();
        let port = rethrow_as_invalid!(u16::try_from(msg.port), "Error during port conversion");
        let sock_address = rethrow_as_invalid!(
            socket_addr_from_slice(&msg.addr, port),
            "Could not get socket address"
        );
        {
            let mut map =
                rethrow_as_internal!(self.tokens_per_epoch.write(), "Could not acquire lock");
            match map.get_mut(&msg.epoch_no) {
                Some(token_map) => {
                    for subs in msg.subs.into_iter() {
                        let endpoint = Endpoint {
                            sock_addr: sock_address,
                            circuit_id: subs.circuit_id,
                        };
                        for token in subs.tokens.into_iter() {
                            match token_map.get_mut(&token) {
                                Some(vec) => vec.push(endpoint.clone()),
                                None => {
                                    token_map.insert(token, vec![endpoint.clone()]);
                                }
                            }
                        }
                    }
                }
                None => {
                    let mut token_map = BTreeMap::new();
                    for subs in msg.subs.into_iter() {
                        let endpoint = Endpoint {
                            sock_addr: sock_address,
                            circuit_id: subs.circuit_id,
                        };
                        let vec = vec![endpoint.clone()];
                        for token in subs.tokens.into_iter() {
                            token_map.insert(token, vec.clone());
                        }
                    }
                    map.insert(msg.epoch_no, token_map);
                }
            };
        }
        Ok(Response::new(SubscribeAck {}))
    }

    async fn publish(&self, req: Request<Cell>) -> Result<Response<PublishAck>, Status> {
        let cell = req.into_inner();
        let mut host_vec = vec![];
        {
            let map = rethrow_as_internal!(self.tokens_per_epoch.read(), "Could not acquire lock");
            let current_epoch_no = unwrap_or_throw_internal!(
                self.dir_client.current_communication_epoch_no(),
                "Cannot get current communication epoch no."
            );
            match map.get(&current_epoch_no) {
                Some(token_map) => match token_map.get(&cell.token()) {
                    Some(vector) => {
                        host_vec = vector.clone();
                    }
                    None => {
                        // token in map, but noone subscribed
                    }
                },
                None => {
                    // token not in map
                }
            }
        }

        for host in host_vec.iter() {
            let sock_addr = host.sock_addr;
            let mut channels = self.dir_client.get_mix_channels(&[sock_addr]).await;
            let channel = unwrap_or_throw_internal!(
                channels.get_mut(&sock_addr),
                "Not able to get matching channel for sock_addr"
            );
            let mut send_cell = cell.clone();
            send_cell.circuit_id = host.circuit_id;
            rethrow_as_internal!(channel.inject(send_cell).await, "Not able to inject cell");
        }
        Ok(Response::new(PublishAck {}))
    }
}

pub async fn garbage_collector(state: Arc<State>) {
    loop {
        info!("Garbage collector strikes again!");
        {
            let mut map = match state.tokens_per_epoch.write() {
                Ok(m) => m,
                Err(e) => {
                    error!("Acquiring lock for cleanup failed: {}", e);
                    continue;
                }
            };
            let split_epoch = match state.dir_client.current_communication_epoch_no() {
                Some(ep) => ep,
                None => {
                    error!("Acquiring current epoch failed");
                    continue;
                }
            };
            // keep current epoch and everything above
            *map = map.split_off(&split_epoch);
        }

        // TODO better cleanup after every epoch?
        delay_for(Duration::from_secs(600)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mix::directory_client;

    #[test]
    fn test_state() {
        let ip_addr: std::net::IpAddr = ("127.0.0.1").parse().expect("failed");
        let local_addr: std::net::SocketAddr = ("127.0.0.1:9001").parse().expect("failed");
        let config = directory_client::Config {
            addr: ip_addr,
            entry_port: 12,
            relay_port: 33,
            rendezvous_port: 55,
            directory_addr: local_addr,
        };
        let state = Arc::new(State::new(Arc::new(directory_client::Client::new(config))));
        let epoch: EpochNo = 1;
        let dummy_endpoint = Endpoint {
            sock_addr: local_addr,
            circuit_id: 12,
        };
        let other_dummy_endpoint = Endpoint {
            sock_addr: local_addr,
            circuit_id: 20,
        };
        let copy_dummy_endpoint = dummy_endpoint.clone();
        let copy_other_dummy_endpoint = other_dummy_endpoint.clone();
        let token: Token = 2;
        let some_not_inserted_token: Token = 1;

        {
            let mut map = state
                .tokens_per_epoch
                .write()
                .expect("Could not acquire lock");
            match map.get_mut(&epoch) {
                Some(token_map) => match token_map.get_mut(&token) {
                    Some(vector) => vector.push(dummy_endpoint),
                    None => {
                        let vector = vec![dummy_endpoint];
                        token_map.insert(token, vector);
                    }
                },
                None => {
                    let mut token_map = BTreeMap::new();
                    let vector = vec![dummy_endpoint];
                    token_map.insert(token, vector);
                    map.insert(epoch, token_map);
                }
            };
        }
        {
            let mut map = state
                .tokens_per_epoch
                .write()
                .expect("Could not acquire lock");
            match map.get_mut(&epoch) {
                Some(token_map) => match token_map.get_mut(&token) {
                    Some(vector) => vector.push(other_dummy_endpoint),
                    None => {
                        let vector = vec![other_dummy_endpoint];
                        token_map.insert(token, vector);
                    }
                },
                None => {
                    let mut token_map = BTreeMap::new();
                    let vector = vec![other_dummy_endpoint];
                    token_map.insert(token, vector);
                    map.insert(epoch, token_map);
                }
            };
        }
        {
            let map = state
                .tokens_per_epoch
                .write()
                .expect("Could not acquire lock");
            if let Some(token_map) = map.get(&epoch) {
                assert_eq!(token_map.contains_key(&token), true);
                assert_eq!(
                    token_map.get(&token),
                    Some(&vec![copy_dummy_endpoint, copy_other_dummy_endpoint])
                );
                assert_eq!(token_map.contains_key(&some_not_inserted_token), false);
            }
        }
    }
}
