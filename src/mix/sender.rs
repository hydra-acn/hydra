//! Responsible for sending packets to the next mix/rendezvous
use futures_util::stream;
use log::*;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::task;

use super::directory_client;
use crate::derive_grpc_client;
use crate::error::Error;
use crate::grpc;
use crate::net::PacketWithNextHop;
use crate::tonic_mix::mix_client::MixClient;
use crate::tonic_mix::rendezvous_client::RendezvousClient;
use crate::tonic_mix::*;

pub type Batch<T> = Vec<PacketWithNextHop<T>>;
pub type SetupBatch = Batch<SetupPacket>;
pub type SubscribeBatch = Batch<SubscriptionVector>;
pub type CellBatch = Batch<Cell>;
pub type PublishBatch = Batch<Cell>;

type SetupTxQueue = spmc::Receiver<SetupBatch>;
type SubscribeTxQueue = spmc::Receiver<SubscribeBatch>;
type RelayTxQueue = spmc::Receiver<CellBatch>;
type PublishTxQueue = spmc::Receiver<CellBatch>;

type MixConnection = MixClient<tonic::transport::Channel>;
derive_grpc_client!(MixConnection);

type RendezvousConnection = RendezvousClient<tonic::transport::Channel>;
derive_grpc_client!(RendezvousConnection);

macro_rules! send_next_batch {
    ($state:expr, $queue:ident, $batch_type:ident, $channel_getter:ident, $send_fun:ident) => {
        // wait for the next batch
        let queue = $state.$queue.clone();
        let maybe_batch = task::spawn_blocking(move || queue.recv())
            .await
            .expect("Spawn failed");
        let batch: $batch_type = match maybe_batch {
            Ok(b) => b,
            Err(e) => {
                error!("Seems like the worker thread is gone: {}", e);
                // macro is expanded inside a loop
                break;
            }
        };
        debug!("Sending batch with {} packets", batch.len());

        // sort by destination and get corresponding channels
        let (batch_map, destinations) = sort_by_destination(batch);
        let channel_map = $state.dir_client.$channel_getter(&destinations).await;

        for (dst, pkts) in batch_map.into_iter() {
            match channel_map.get(&dst) {
                Some(c) => {
                    // fire and forget concurrently for each destination
                    tokio::spawn($send_fun($state.dir_client.clone(), c.clone(), pkts));
                }
                None => {
                    warn!(
                        "Expected to have a connection by now, dropping packets destined to {}",
                        dst
                    );
                    ()
                }
            }
        }
    };
}

macro_rules! define_send_task {
    ($name:ident, $queue:ident, $batch_type:ident, $channel_getter:ident, $send_fun:ident) => {
        pub async fn $name(state: Arc<State>) -> Result<(), Error> {
            loop {
                send_next_batch!(state, $queue, $batch_type, $channel_getter, $send_fun);
            }
            Ok(())
        }
    };
}

pub struct State {
    dir_client: Arc<directory_client::Client>,
    setup_tx_queue: SetupTxQueue,
    subscribe_tx_queue: SubscribeTxQueue,
    relay_tx_queue: RelayTxQueue,
    publish_tx_queue: PublishTxQueue,
}

impl State {
    pub fn new(
        dir_client: Arc<directory_client::Client>,
        setup_tx_queue: SetupTxQueue,
        subscribe_tx_queue: SubscribeTxQueue,
        relay_tx_queue: RelayTxQueue,
        publish_tx_queue: PublishTxQueue,
    ) -> Self {
        State {
            dir_client,
            setup_tx_queue,
            subscribe_tx_queue,
            relay_tx_queue,
            publish_tx_queue,
        }
    }
}

fn sort_by_destination<T>(batch: Batch<T>) -> (HashMap<SocketAddr, Vec<T>>, Vec<SocketAddr>) {
    let mut batch_map: HashMap<SocketAddr, Vec<T>> = HashMap::new();
    for pkt in batch {
        match batch_map.get_mut(pkt.next_hop()) {
            Some(vec) => vec.push(pkt.into_inner()),
            None => {
                batch_map.insert(*pkt.next_hop(), vec![pkt.into_inner()]);
            }
        }
    }
    let destinations: Vec<SocketAddr> = batch_map.keys().cloned().collect();
    (batch_map, destinations)
}

async fn send_setup_packets(
    dir_client: Arc<directory_client::Client>,
    mut c: MixConnection,
    mut pkts: Vec<SetupPacket>,
) {
    shuffle(&mut pkts);
    for pkt in pkts {
        // setup packets need the src attached as metadata
        let mut req = tonic::Request::new(pkt);
        req.metadata_mut().insert(
            "reply-to",
            dir_client
                .config()
                .setup_reply_to()
                .parse()
                .expect("Why should this fail?"),
        );
        c.setup_circuit(req)
            .await
            .map(|_| ())
            .unwrap_or_else(|e| warn!("Creating next circuit hop failed: {}", e));
    }
}

async fn send_subscriptions(
    _dir_client: Arc<directory_client::Client>,
    mut c: RendezvousConnection,
    mut pkts: Vec<SubscriptionVector>,
) {
    if pkts.len() > 1 {
        warn!("Expected only one subscription packet per rendezvous node");
        shuffle(&mut pkts);
    }
    for pkt in pkts {
        let req = tonic::Request::new(pkt);
        c.subscribe(req)
            .await
            .map(|_| ())
            .unwrap_or_else(|e| warn!("Subscription failed: {}", e));
    }
}

async fn relay_cells(
    _dir_client: Arc<directory_client::Client>,
    mut c: MixConnection,
    mut cells: Vec<Cell>,
) {
    shuffle(&mut cells);
    let req = tonic::Request::new(stream::iter(cells));
    c.relay(req)
        .await
        .map(|_| ())
        .unwrap_or_else(|e| warn!("Relaying cells failed: {}", e));
}

async fn publish_cells(
    _dir_client: Arc<directory_client::Client>,
    mut c: RendezvousConnection,
    mut cells: Vec<Cell>,
) {
    shuffle(&mut cells);
    let req = tonic::Request::new(stream::iter(cells));
    c.publish(req)
        .await
        .map(|_| ())
        .unwrap_or_else(|e| warn!("Publishing cells failed: {}", e));
}

fn shuffle<T>(_pkts: &mut Vec<T>) {
    // TODO security: shuffle for real!
}

define_send_task!(
    setup_task,
    setup_tx_queue,
    SetupBatch,
    get_mix_channels,
    send_setup_packets
);

define_send_task!(
    subscribe_task,
    subscribe_tx_queue,
    SubscribeBatch,
    get_rendezvous_channels,
    send_subscriptions
);

define_send_task!(
    relay_task,
    relay_tx_queue,
    CellBatch,
    get_mix_channels,
    relay_cells
);

define_send_task!(
    publish_task,
    publish_tx_queue,
    CellBatch,
    get_rendezvous_channels,
    publish_cells
);

pub async fn run(state: Arc<State>) {
    let setup_handle = tokio::spawn(setup_task(state.clone()));
    let subscribe_handle = tokio::spawn(subscribe_task(state.clone()));
    let relay_handle = tokio::spawn(relay_task(state.clone()));
    let publish_handle = tokio::spawn(publish_task(state.clone()));

    match tokio::try_join!(setup_handle, subscribe_handle, relay_handle, publish_handle) {
        Ok(_) => (),
        Err(e) => error!("Something panicked: {}", e),
    }
}
