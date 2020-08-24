//! Rendezvous service of a mix

use futures_util::stream;
use futures_util::StreamExt;
use log::*;
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use tokio::time::{delay_for, Duration};
use tonic::{Request, Response, Status};

use super::subscription_map::SubscriptionMap;
use crate::defs::CellCmd;
use crate::epoch::EpochNo;
use crate::grpc::{valid_request_check, ServerTlsCredentials};
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

pub async fn garbage_collector(state: Arc<State>) {
    loop {
        info!("Garbage collector strikes again!");
        {
            let mut map = match state.subs_per_epoch.write() {
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
    use crate::defs::CircuitId;
    use crate::mix::directory_client::mocks;
    use crate::rendezvous::subscription_map::Endpoint;
    use crate::tonic_mix::mix_server::{Mix, MixServer};
    use crate::tonic_mix::rendezvous_client::RendezvousClient;
    use std::net::SocketAddr;
    use tokio::time::{self, Duration};

    const CURRENT_COMMUNICATION_EPOCH_NO: EpochNo = 345678;

    async fn test_publish_subscribe(
        rendezvous_addr: SocketAddr,
        mix_addr: SocketAddr,
        mix_state: Arc<MixState>,
    ) {
        time::delay_for(Duration::from_millis(100)).await;
        let mut client = RendezvousClient::connect(format!("http://{}", rendezvous_addr))
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
            port: mix_addr.port() as u32,
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
            port: mix_addr.port() as u32,
            subs: sub_vec,
        });
        let resp = client.subscribe(request).await.expect("subscribe failed.");
        assert_eq!(resp.into_inner(), SubscribeAck {});

        //test publish
        time::delay_for(Duration::from_millis(100)).await;
        let mut cell_to_send1 = Cell::dummy(2, 3);
        cell_to_send1.set_token(12);
        //test publish on same circuit without broadcast
        let mut cell_to_send2 = Cell::dummy(2, 3);
        cell_to_send2.set_token(2);
        //stream cells
        let resp = client
            .publish(tonic::Request::new(stream::iter(vec![
                cell_to_send1,
                cell_to_send2,
            ])))
            .await
            .expect("publish failed");
        assert_eq!(resp.into_inner(), PublishAck {});

        //test if mix_map contains expected cells
        time::delay_for(Duration::from_millis(100)).await;
        {
            //publish
            let map = mix_state.storage.read().expect("Could not acquire lock");
            assert_eq!(map.contains_key(&2), false);
            let v = map.get(&5).expect("CircuitId not present in token_map");
            assert_eq!(v.len(), 1);
            let cell = v.first().expect("Empty vector, no cell present");
            assert_eq!(cell.token(), 12);
        }
        //test publish on same circuit with broadcast
        let mut cell_to_send = Cell::dummy(2, 3);
        cell_to_send.set_token(2);
        cell_to_send.set_command(CellCmd::Broadcast);
        let resp = client
            .publish(tonic::Request::new(stream::once(async { cell_to_send })))
            .await
            .expect("publish failed");
        assert_eq!(resp.into_inner(), PublishAck {});
        //test if mix_map contains expected cell
        time::delay_for(Duration::from_millis(100)).await;

        {
            let map = mix_state.storage.read().expect("Could not acquire lock");
            assert_eq!(map.contains_key(&2), true);
        }
    }

    #[tokio::test]
    async fn rendezvous_service_with_garbage_collection() {
        let rendezvous_addr: std::net::SocketAddr = ("127.0.0.1:0").parse().expect("failed");
        let mix_addr: std::net::SocketAddr = ("127.0.0.1:0").parse().expect("failed");

        let mock_client = mocks::new(CURRENT_COMMUNICATION_EPOCH_NO);
        //start mix
        let mix_state = Arc::new(MixState::new());
        let (mix_handle, mix_addr) = tests::spawn_service_with_shutdown(
            mix_state.clone(),
            mix_addr,
            Some(time::delay_for(Duration::from_secs(5))),
            None,
        )
        .await
        .expect("Spawn failed");
        //start rendezvous service
        let rend_dir_client = Arc::new(mock_client);
        let timeout = time::delay_for(Duration::from_secs(5));
        let state = Arc::new(State::new(rend_dir_client.clone()));
        let (rendezvous_grpc_handle, rendezvous_addr) =
            super::spawn_service_with_shutdown(state.clone(), rendezvous_addr, Some(timeout), None)
                .await
                .expect("Spawn failed");
        //initialize state for garbage collector test
        {
            let mut epoch_map = state
                .subs_per_epoch
                .write()
                .expect("Could not acquire lock");
            epoch_map.insert(CURRENT_COMMUNICATION_EPOCH_NO - 1, SubscriptionMap::new());
            epoch_map.insert(CURRENT_COMMUNICATION_EPOCH_NO + 1, SubscriptionMap::new());
        }
        //start garbage_collector
        let garbage_handle = tokio::spawn(garbage_collector(state.clone()));
        if let Err(_) = tokio::time::timeout(Duration::from_secs(2), garbage_handle).await {
        } else {
            unreachable!();
        }
        //publish and subscribe
        let pubsub_handle = tokio::spawn(test_publish_subscribe(
            rendezvous_addr,
            mix_addr,
            mix_state.clone(),
        ));
        match tokio::try_join!(rendezvous_grpc_handle, mix_handle, pubsub_handle) {
            Ok(_) => (),
            Err(e) => panic!("Something failed: {}", e),
        }

        //test if state contains expected host and work of garbage collector
        {
            let epoch_map = state.subs_per_epoch.read().expect("Could not acquire lock");
            let sub_map = epoch_map
                .get(&CURRENT_COMMUNICATION_EPOCH_NO)
                .expect("Epoch not present in map");
            let host_vec = sub_map.get_subscribers(&55);
            assert!(host_vec.contains(&Endpoint::new(mix_addr, 5)));
            let host_vec = sub_map.get_subscribers(&2);
            assert!(host_vec.contains(&Endpoint::new(mix_addr, 2)));
            let host_vec = sub_map.get_subscribers(&4);
            assert!(host_vec.contains(&Endpoint::new(mix_addr, 2)));

            //verify work of garbage collector
            assert_eq!(
                epoch_map.contains_key(&(CURRENT_COMMUNICATION_EPOCH_NO - 1)),
                false
            );
            assert_eq!(
                epoch_map.contains_key(&(CURRENT_COMMUNICATION_EPOCH_NO + 1)),
                true
            );
        }
    }

    type CellStorage = BTreeMap<CircuitId, Vec<Cell>>;

    pub struct MixState {
        storage: RwLock<CellStorage>,
    }

    impl MixState {
        pub fn new() -> Self {
            MixState {
                storage: RwLock::new(CellStorage::new()),
            }
        }
    }

    define_grpc_service!(Service, MixState, MixServer);

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

        async fn relay(
            &self,
            _req: Request<tonic::Streaming<Cell>>,
        ) -> Result<Response<RelayAck>, Status> {
            unimplemented!("Just a mock");
        }

        async fn inject(
            &self,
            req: Request<tonic::Streaming<Cell>>,
        ) -> Result<Response<InjectAck>, Status> {
            let mut stream = req.into_inner();
            let mut vec;
            while let Some(c) = stream.next().await {
                let cell = c.expect("No valid cell for injecting");
                let mut map = self.storage.write().expect("Could not acquire lock");
                match map.get_mut(&cell.circuit_id) {
                    Some(v) => {
                        vec = v.to_vec();
                        vec.push(cell.clone());
                    }
                    None => vec = vec![cell.clone()],
                };
                map.insert(cell.circuit_id, vec);
            }
            Ok(Response::new(InjectAck {}))
        }
    }
}
