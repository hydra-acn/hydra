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
use crate::tonic_mix::*;

/// Wrapping a packet of type `T` with next hop information
pub struct PacketWithNextHop<T> {
    pub inner: T,
    pub next_hop: SocketAddr,
}

impl<T> PacketWithNextHop<T> {
    pub fn into_inner(self) -> T {
        self.inner
    }
}

pub type Batch<T> = Vec<PacketWithNextHop<T>>;
pub type SetupBatch = Batch<SetupPacket>;

type SetupTxQueue = spmc::Receiver<SetupBatch>;
type MixConnection = MixClient<tonic::transport::Channel>;
derive_grpc_client!(MixConnection);

pub struct State {
    dir_client: Arc<directory_client::Client>,
    setup_tx_queue: SetupTxQueue,
}

impl State {
    pub fn new(dir_client: Arc<directory_client::Client>, setup_tx_queue: SetupTxQueue) -> Self {
        State {
            dir_client,
            setup_tx_queue: setup_tx_queue,
        }
    }

    // TODO this should be a generic "send_batch"
    pub async fn send_setup_batch(&self, batch: SetupBatch) {
        // mapping destination to corresponding packets
        let mut batch_map: HashMap<SocketAddr, Vec<SetupPacket>> = HashMap::new();
        for pkt in batch {
            // already sort packets according to destination
            match batch_map.get_mut(&pkt.next_hop) {
                Some(vec) => vec.push(pkt.into_inner()),
                None => {
                    batch_map.insert(pkt.next_hop, vec![pkt.into_inner()]);
                }
            }
        }
        let destinations: Vec<SocketAddr> = batch_map.keys().cloned().collect();
        let channel_map = self.dir_client.get_mix_channels(&destinations).await;
        for (dst, pkts) in batch_map.into_iter() {
            match channel_map.get(&dst) {
                Some(c) => {
                    // fire and forget concurrently for each destination
                    tokio::spawn(send_setup_packets(c.clone(), pkts));
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
    }
}

pub async fn send_setup_packets(mut c: MixConnection, pkts: Vec<SetupPacket>) {
    for pkt in pkts {
        match c.setup_circuit(pkt).await {
            Ok(_) => (),
            Err(e) => warn!("Creating next circuit hop failed: {}", e),
        };
    }
}

pub async fn setup_task(state: Arc<State>) {
    loop {
        // wait for the next batch
        let queue = state.setup_tx_queue.clone();
        let batch = task::spawn_blocking(move || {
            queue.recv().expect("tx queue will never be filled again")
        })
        .await
        .expect("Reading tx queue failed");
        state.send_setup_batch(batch).await;
    }
}

pub async fn run(state: Arc<State>) {
    let setup_handle = tokio::spawn(setup_task(state.clone()));
    match tokio::try_join!(setup_handle) {
        Ok(_) => (),
        Err(e) => error!("Something panicked: {}", e),
    }
}
