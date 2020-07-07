//! Rendezvous service of a mix

use log::*;
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use tokio::time::{delay_for, Duration};
use tonic::{Request, Response, Status};

use super::directory_client;
use crate::defs::{CellCmd, CircuitId, Token};
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
            if cell.circuit_id == host.circuit_id {
                // don't send cells back on the same circuit, except when asked to
                if let Some(CellCmd::Broadcast) = cell.command() {
                } else {
                    continue;
                }
            }
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
    use crate::mix::directory_client::mocks;
    use crate::mix::*;
    use crate::tonic_mix::mix_server::{Mix, MixServer};
    use crate::tonic_mix::rendezvous_client::RendezvousClient;
    use tokio::time::{self, Duration};

    const CURRENT_COMMUNICATION_EPOCH_NO: EpochNo = 345678;

    #[tokio::test]
    async fn test_subscribe() {
        time::delay_for(Duration::from_millis(300)).await;
        let mut client = RendezvousClient::connect("http://127.0.0.1:9101")
            .await
            .expect("failed to connect");
        let addr = vec![127, 0, 0, 1];
        let sub_vec = vec![
            Subscription {
                circuit_id: 2,
                tokens: vec![2],
            },
            Subscription {
                circuit_id: 5,
                tokens: vec![55, 12],
            },
        ];
        let request = tonic::Request::new(SubscriptionVector {
            epoch_no: CURRENT_COMMUNICATION_EPOCH_NO,
            addr: addr.clone(),
            port: 1200,
            subs: sub_vec,
        });
        let resp = client.subscribe(request).await.expect("subscribe failed");
        assert_eq!(resp.into_inner(), SubscribeAck {});

        let sub_vec = vec![Subscription {
            circuit_id: 2,
            tokens: vec![4],
        }];
        let request = tonic::Request::new(SubscriptionVector {
            epoch_no: CURRENT_COMMUNICATION_EPOCH_NO,
            addr: addr,
            port: 1200,
            subs: sub_vec,
        });
        let resp = client.subscribe(request).await.expect("subscribe failed.");
        assert_eq!(resp.into_inner(), SubscribeAck {});
    }

    #[tokio::test]
    async fn test_publish() {
        time::delay_for(Duration::from_millis(500)).await;
        let mut client = RendezvousClient::connect("http://127.0.0.1:9101")
            .await
            .expect("failed to connect");
        let mut cell_to_send = Cell::dummy(2, 3);
        cell_to_send.set_token(12);
        let request = tonic::Request::new(cell_to_send);
        let resp = client.publish(request).await.expect("publish failed");
        assert_eq!(resp.into_inner(), PublishAck {});
    }

    #[tokio::test]
    async fn rendezvous_service_with_garbage_collection() {
        let rendezvous_addr: std::net::SocketAddr = ("127.0.0.1:9101").parse().expect("failed");
        let mix_addr: std::net::SocketAddr = ("127.0.0.1:1200").parse().expect("failed");

        let mock_client = mocks::new(CURRENT_COMMUNICATION_EPOCH_NO);
        //start mix
        let mix_state = Arc::new(State::new());
        let mix_to_receive_injects = spawn_service_with_shutdown(
            mix_state.clone(),
            mix_addr,
            Some(time::delay_for(Duration::from_secs(5))),
        );
        //start rendezvous service
        let rend_dir_client = Arc::new(mock_client);
        let timeout = time::delay_for(Duration::from_secs(5));
        let state = Arc::new(rendezvous::State::new(rend_dir_client.clone()));
        let rendezvous_grpc_handle =
            rendezvous::spawn_service_with_shutdown(state.clone(), rendezvous_addr, Some(timeout));
        //initialize state for garbage collector test
        {
            let mut token_map = BTreeMap::new();
            token_map.insert(
                34,
                vec![Endpoint {
                    sock_addr: mix_addr,
                    circuit_id: 22,
                }],
            );
            let mut map = state
                .tokens_per_epoch
                .write()
                .expect("Could not acquire lock");
            map.insert(CURRENT_COMMUNICATION_EPOCH_NO - 1, token_map.clone());
            map.insert(CURRENT_COMMUNICATION_EPOCH_NO + 1, token_map.clone());
        }
        //start garbage_collector
        let garbage_handle = tokio::spawn(rendezvous::garbage_collector(state.clone()));
        if let Err(_) = tokio::time::timeout(Duration::from_secs(5), garbage_handle).await {
        } else {
            unreachable!();
        }
        match tokio::try_join!(rendezvous_grpc_handle, mix_to_receive_injects) {
            Ok(_) => (),
            Err(e) => error!("Something failed: {}", e),
        }
        //test if state contains expected host and work of garbage collector
        {
            let map = state
                .tokens_per_epoch
                .read()
                .expect("Could not acquire lock");
            let token_map = map
                .get(&CURRENT_COMMUNICATION_EPOCH_NO)
                .expect("Epoch not present in map");
            let host_vec = token_map
                .get(&55)
                .expect("CircuitId not present in token_map");
            assert_eq!(
                host_vec.contains(&Endpoint {
                    sock_addr: mix_addr,
                    circuit_id: 5
                }),
                true
            );
            let host_vec = token_map
                .get(&2)
                .expect("CircuitId not present in token_map");
            assert_eq!(
                host_vec.contains(&Endpoint {
                    sock_addr: mix_addr,
                    circuit_id: 2
                }),
                true
            );
            let host_vec = token_map
                .get(&4)
                .expect("CircuitId not present in token_map");
            assert_eq!(
                host_vec.contains(&Endpoint {
                    sock_addr: mix_addr,
                    circuit_id: 2
                }),
                true
            );
            //verify work of garbage collector
            assert_eq!(
                map.contains_key(&(CURRENT_COMMUNICATION_EPOCH_NO - 1)),
                false
            );
            assert_eq!(
                map.contains_key(&(CURRENT_COMMUNICATION_EPOCH_NO + 1)),
                true
            );
        }
        //test if mix_map contains expected cells
        {
            let map = mix_state.storage.read().expect("Could not acquire lock");
            assert_eq!(map.contains_key(&2), false);
            let v = map.get(&5).expect("CircuitId not present in token_map");
            assert_eq!(v.len(), 1);
            let cell = v.first().expect("Empty vector, no cell present");
            assert_eq!(cell.token(), 12);
        }
    }

    type CellStorage = BTreeMap<CircuitId, Vec<Cell>>;

    pub struct State {
        storage: RwLock<CellStorage>,
    }

    impl State {
        pub fn new() -> Self {
            State {
                storage: RwLock::new(CellStorage::new()),
            }
        }
    }

    define_grpc_service!(Service, State, MixServer);

    #[tonic::async_trait]
    impl Mix for Service {
        async fn setup_circuit(
            &self,
            _req: Request<SetupPacket>,
        ) -> Result<Response<SetupAck>, Status> {
            unimplemented!("Just a mock");
        }

        async fn send_and_receive(
            &self,
            _req: Request<Cell>,
        ) -> Result<Response<CellVector>, Status> {
            unimplemented!("Just a mock");
        }

        async fn late_poll(
            &self,
            _req: Request<LatePollRequest>,
        ) -> Result<Response<CellVector>, Status> {
            unimplemented!("Just a mock");
        }

        async fn relay(&self, _req: Request<Cell>) -> Result<Response<RelayAck>, Status> {
            unimplemented!("Just a mock");
        }

        async fn inject(&self, req: Request<Cell>) -> Result<Response<InjectAck>, Status> {
            let cell = req.into_inner();
            let mut map = self.storage.write().expect("Could not acquire lock");
            let mut vec;
            match map.get_mut(&cell.circuit_id) {
                Some(v) => {
                    vec = v.to_vec();
                    vec.push(cell.clone());
                }
                None => vec = vec![cell.clone()],
            };
            map.insert(cell.circuit_id, vec);
            Ok(Response::new(InjectAck {}))
        }
    }
}
