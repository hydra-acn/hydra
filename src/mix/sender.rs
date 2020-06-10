//! Responsible for sending packets to the next mix/rendezvous
use log::*;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::task;

use crate::derive_grpc_client;
use crate::error::Error;
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
    setup_tx_queue: SetupTxQueue,
    mix_connections: Mutex<HashMap<SocketAddr, MixConnection>>,
}

impl State {
    pub fn new(setup_tx_queue: SetupTxQueue) -> Self {
        State {
            setup_tx_queue: setup_tx_queue,
            mix_connections: Mutex::new(HashMap::new()),
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
        refresh_connections::<MixConnection>(&destinations, &self.mix_connections).await;
        let connection_map = self.mix_connections.lock().expect("Lock poisoned");
        for (dst, pkts) in batch_map.into_iter() {
            match connection_map.get(&dst) {
                Some(c) => {
                    // fire and forget in parallel
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

pub async fn refresh_connections<T: grpc::Client + Send + 'static>(
    destinations: &[SocketAddr],
    connection_map: &Mutex<HashMap<SocketAddr, T>>,
) {
    let mut to_create = Vec::new();
    {
        let connection_map = connection_map.lock().expect("Lock poisoned");
        for dst in destinations {
            if !connection_map.contains_key(dst) {
                to_create.push(*dst);
            }
        }
    }
    let new_connections = create_multiple_connections::<T>(&to_create).await;
    {
        let mut connection_map = connection_map.lock().expect("Lock poisoned");
        for (dst, c) in new_connections.into_iter() {
            connection_map.insert(dst, c);
        }
    }
}

pub async fn create_multiple_connections<T: grpc::Client + Send + 'static>(
    destinations: &[SocketAddr],
) -> HashMap<SocketAddr, T> {
    // spawn setup for each connection
    let mut connection_handles = HashMap::new();
    for dst in destinations {
        connection_handles.insert(dst, tokio::spawn(create_connection::<T>(*dst)));
    }

    // collect ready connections
    let mut connection_map = HashMap::new();
    for (dst, handle) in connection_handles {
        let result = handle.await.expect("Setting up connection panicked");
        match result {
            Ok(c) => {
                connection_map.insert(*dst, c);
                ()
            }
            Err(e) => warn!("Setting up connection to {} failed: {}", dst, e),
        }
    }
    connection_map
}

pub async fn create_connection<T: grpc::Client + Send>(addr: SocketAddr) -> Result<T, Error> {
    let endpoint = format!("http://{}:{}", addr.ip(), addr.port());
    // TODO maybe we want to decrease timeouts here (depending on tonic's defaults)?
    let connection = T::connect(endpoint).await?;
    Ok(connection)
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
