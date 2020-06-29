//! Responsible for sending packets to the next mix/rendezvous
use log::*;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::task;

use super::directory_client;
use crate::derive_grpc_client;
use crate::grpc;
use crate::tonic_mix::mix_client::MixClient;
use crate::tonic_mix::rendezvous_client::RendezvousClient;
use crate::tonic_mix::*;

/// Wrapping a packet of type `T` with next hop information
pub struct PacketWithNextHop<T> {
    inner: T,
    next_hop: SocketAddr,
}

impl<T> PacketWithNextHop<T> {
    pub fn new(pkt: T, next_hop: SocketAddr) -> Self {
        PacketWithNextHop {
            inner: pkt,
            next_hop,
        }
    }

    pub fn into_inner(self) -> T {
        self.inner
    }

    pub fn next_hop(&self) -> &SocketAddr {
        &self.next_hop
    }

    pub fn set_next_hop(&mut self, next_hop: SocketAddr) {
        self.next_hop = next_hop;
    }
}

pub type Batch<T> = Vec<PacketWithNextHop<T>>;
pub type SetupBatch = Batch<SetupPacket>;
pub type CellBatch = Batch<Cell>;

type SetupTxQueue = spmc::Receiver<SetupBatch>;
type RelayTxQueue = spmc::Receiver<CellBatch>;
type MixConnection = MixClient<tonic::transport::Channel>;
derive_grpc_client!(MixConnection);

type RendezvousConnection = RendezvousClient<tonic::transport::Channel>;
derive_grpc_client!(RendezvousConnection);

macro_rules! send_next_batch {
    ($state:expr, $queue:ident, $channel_getter:ident, $send_fun:ident) => {
        // wait for the next batch
        let queue = $state.$queue.clone();
        let batch = task::spawn_blocking(move || {
            queue.recv().expect("tx queue will never be filled again")
        })
        .await
        .expect("Reading tx queue failed");

        debug!("Sending batch with {} packets", batch.len(),);

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
    ($name:ident, $queue:ident, $channel_getter:ident, $send_fun:ident) => {
        pub async fn $name(state: Arc<State>) {
            loop {
                send_next_batch!(state, $queue, $channel_getter, $send_fun);
            }
        }
    };
}

pub struct State {
    dir_client: Arc<directory_client::Client>,
    setup_tx_queue: SetupTxQueue,
    relay_tx_queue: RelayTxQueue,
}

impl State {
    pub fn new(
        dir_client: Arc<directory_client::Client>,
        setup_tx_queue: SetupTxQueue,
        relay_tx_queue: RelayTxQueue,
    ) -> Self {
        State {
            dir_client,
            setup_tx_queue,
            relay_tx_queue,
        }
    }
}

fn sort_by_destination<T>(batch: Batch<T>) -> (HashMap<SocketAddr, Vec<T>>, Vec<SocketAddr>) {
    let mut batch_map: HashMap<SocketAddr, Vec<T>> = HashMap::new();
    for pkt in batch {
        match batch_map.get_mut(&pkt.next_hop) {
            Some(vec) => vec.push(pkt.into_inner()),
            None => {
                batch_map.insert(pkt.next_hop, vec![pkt.into_inner()]);
            }
        }
    }
    let destinations: Vec<SocketAddr> = batch_map.keys().cloned().collect();
    (batch_map, destinations)
}

pub async fn send_setup_packets(
    dir_client: Arc<directory_client::Client>,
    mut c: MixConnection,
    pkts: Vec<SetupPacket>,
) {
    // TODO security: shuffle!
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
        match c.setup_circuit(req).await {
            Ok(_) => (),
            Err(e) => warn!("Creating next circuit hop failed: {}", e),
        };
    }
}

pub async fn relay_cells(
    _dir_client: Arc<directory_client::Client>,
    mut c: MixConnection,
    cells: Vec<Cell>,
) {
    // TODO security: shuffle!
    for cell in cells {
        let req = tonic::Request::new(cell);
        match c.relay(req).await {
            Ok(_) => (),
            Err(e) => warn!("Relaying cell failed: {}", e),
        }
    }
}

define_send_task!(
    setup_task,
    setup_tx_queue,
    get_mix_channels,
    send_setup_packets
);

define_send_task!(relay_task, relay_tx_queue, get_mix_channels, relay_cells);

pub async fn run(state: Arc<State>) {
    let setup_handle = tokio::spawn(setup_task(state.clone()));
    let relay_handle = tokio::spawn(relay_task(state.clone()));
    // XXX rendezvous task
    match tokio::try_join!(setup_handle, relay_handle) {
        Ok(_) => (),
        Err(e) => error!("Something panicked: {}", e),
    }
}
