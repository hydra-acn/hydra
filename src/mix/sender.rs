//! Responsible for sending packets to the next mix/rendezvous
use futures_util::stream;
use log::*;
use rand::seq::SliceRandom;
use std::collections::HashMap;
use std::io::Write;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::task;

use crate::crypto::cprng::thread_cprng;
use crate::epoch::current_time;
use crate::error::Error;
use crate::net::cell::Cell;
use crate::net::channel_pool::{MixChannel, RendezvousChannel, TcpChannel};
use crate::net::PacketWithNextHop;
use crate::tonic_mix::{SetupPacket, Subscription};

use super::cell_processor::cell_rss_t;
use super::directory_client;
use super::setup_processor::setup_t;

pub type Batch<T> = (Vec<Vec<PacketWithNextHop<T>>>, Option<Duration>);
pub type SetupBatch = Batch<SetupPacket>;
pub type SubscribeBatch = Batch<Subscription>;
pub type CellBatch = Batch<Cell>;

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

        let deadline = batch.1;

        // sort by destination and get corresponding channels
        let (batch_map, destinations) = sort_by_destination(batch);
        let channel_map = $state.dir_client.$channel_getter(&destinations).await;

        for (dst, pkts) in batch_map.into_iter() {
            match channel_map.get(&dst) {
                Some(c) => {
                    // fire and forget concurrently for each destination
                    tokio::spawn($send_fun(
                        $state.dir_client.clone(),
                        c.clone(),
                        pkts,
                        deadline,
                    ));
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
    setup_tx_queue: setup_t::TxQueue,
    subscribe_tx_queue: setup_t::AltTxQueue,
    relay_tx_queue: cell_rss_t::TxQueue,
}

impl State {
    pub fn new(
        dir_client: Arc<directory_client::Client>,
        setup_tx_queue: setup_t::TxQueue,
        subscribe_tx_queue: setup_t::AltTxQueue,
        relay_tx_queue: cell_rss_t::TxQueue,
    ) -> Self {
        State {
            dir_client,
            setup_tx_queue,
            subscribe_tx_queue,
            relay_tx_queue,
        }
    }
}

fn sort_by_destination<T>(batch: Batch<T>) -> (HashMap<SocketAddr, Vec<T>>, Vec<SocketAddr>) {
    let mut batch_map: HashMap<SocketAddr, Vec<T>> = HashMap::new();
    for vec in batch.0.into_iter() {
        for pkt in vec.into_iter() {
            match batch_map.get_mut(pkt.next_hop()) {
                Some(vec) => vec.push(pkt.into_inner()),
                None => {
                    batch_map.insert(*pkt.next_hop(), vec![pkt.into_inner()]);
                }
            }
        }
    }
    let destinations: Vec<SocketAddr> = batch_map.keys().cloned().collect();
    (batch_map, destinations)
}

async fn send_setup_packets(
    dir_client: Arc<directory_client::Client>,
    mut c: MixChannel,
    pkts: Vec<SetupPacket>,
    deadline: Option<Duration>,
) {
    let shuffle_it = ShuffleIterator::new(pkts, deadline);
    let mut req = tonic::Request::new(stream::iter(shuffle_it));
    // attach reply address as metadata
    req.metadata_mut().insert(
        "reply-to",
        dir_client
            .config()
            .setup_reply_to()
            .parse()
            .expect("Why should this fail?"),
    );

    c.stream_setup_circuit(req)
        .await
        .map(|_| ())
        .unwrap_or_else(|e| warn!("Creating circuits failed: {}", e));
}

async fn send_subscriptions(
    _dir_client: Arc<directory_client::Client>,
    mut c: RendezvousChannel,
    pkts: Vec<Subscription>,
    _deadline: Option<Duration>,
) {
    if pkts.len() > 1 {
        warn!("Expected one subscription to each rendezvous node only");
    }

    for sub in pkts.into_iter() {
        info!("Sending subscriptions for {} circuits", sub.circuits.len());
        let req = tonic::Request::new(sub);
        c.subscribe(req)
            .await
            .map(|_| ())
            .unwrap_or_else(|e| warn!("Subscription failed: {}", e));
    }
}

async fn relay_cells(
    _dir_client: Arc<directory_client::Client>,
    c: TcpChannel,
    cells: Vec<Cell>,
    deadline: Option<Duration>,
) {
    tokio::task::spawn_blocking(move || {
        let shuffle_it = ShuffleIterator::new(cells, deadline);
        let mut stream = c.write().expect("Lock poisoned");
        for cell in shuffle_it {
            stream.write_all(cell.buf()).unwrap_or_else(|e| {
                warn!("Writing to TCP stream failed: {}", e);
                ()
            });
        }
    })
    .await
    .expect("Spawn failed");
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
    get_relay_channels,
    relay_cells
);

pub async fn run(state: Arc<State>) {
    let setup_handle = tokio::spawn(setup_task(state.clone()));
    let subscribe_handle = tokio::spawn(subscribe_task(state.clone()));
    let relay_handle = tokio::spawn(relay_task(state.clone()));

    match tokio::try_join!(setup_handle, subscribe_handle, relay_handle,) {
        Ok(_) => (),
        Err(e) => error!("Something panicked: {}", e),
    }
}

pub struct ShuffleIterator<T> {
    idx_vec: Vec<usize>,
    pkt_vec: Vec<T>,
    pos: usize,
    deadline: Option<Duration>,
}

impl<T> ShuffleIterator<T> {
    /// The iterator will return `None` when the deadline has passed.
    pub fn new(pkt_vec: Vec<T>, deadline: Option<Duration>) -> Self {
        let mut idx_vec: Vec<usize> = (0..pkt_vec.len()).collect();
        idx_vec.shuffle(&mut thread_cprng());
        ShuffleIterator {
            idx_vec,
            pkt_vec,
            pos: 0,
            deadline,
        }
    }
}

impl<T: Default> Iterator for ShuffleIterator<T> {
    type Item = T;
    fn next(&mut self) -> Option<T> {
        if let Some(deadline) = self.deadline {
            if let None = deadline.checked_sub(current_time()) {
                warn!("Sending did not finish in time");
                return None;
            }
        }
        if self.pos < self.idx_vec.len() {
            let pkt = std::mem::replace(&mut self.pkt_vec[self.idx_vec[self.pos]], T::default());
            self.pos += 1;
            Some(pkt)
        } else {
            None
        }
    }
}
