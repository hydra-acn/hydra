//! processing of one epoch
use log::*;
use std::cmp::Ordering;
use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{self, AtomicBool};
use std::sync::Arc;
use std::thread::sleep;
use tokio::time::Duration;

use super::circuit::{SetupNextHop, Circuit};
use super::directory_client;
use super::grpc::SetupPacketWithPrev;
use crate::crypto::key::Key;
use crate::defs::CircuitId;
use crate::epoch::{current_time, EpochInfo, EpochNo};
use crate::tonic_mix::*;

type SetupRxQueue = tokio::sync::mpsc::UnboundedReceiver<SetupPacketWithPrev>;
type CellRxQueue = tokio::sync::mpsc::UnboundedReceiver<Cell>;
type PendingSetupMap = BTreeMap<EpochNo, VecDeque<SetupPacketWithPrev>>;
type CircuitMap = BTreeMap<CircuitId, Circuit>;
type CircuitIdMap = BTreeMap<CircuitId, CircuitId>;

pub struct Worker {
    running: Arc<AtomicBool>,
    dir_client: Arc<directory_client::Client>,
    setup_rx_queues: Vec<SetupRxQueue>,
    _cell_rx_queues: Vec<CellRxQueue>,
    pending_setup_pkts: PendingSetupMap,
    circuits: CircuitMap,
    // mapping the upstream circuit id to the downstream id of an circuit
    circuit_id_map: CircuitIdMap,
}

impl Worker {
    pub fn new(
        running: Arc<AtomicBool>,
        dir_client: Arc<directory_client::Client>,
        setup_rx_queues: Vec<SetupRxQueue>,
        cell_rx_queues: Vec<CellRxQueue>,
    ) -> Self {
        Worker {
            running: running.clone(),
            dir_client,
            setup_rx_queues: setup_rx_queues,
            _cell_rx_queues: cell_rx_queues,
            pending_setup_pkts: PendingSetupMap::new(),
            circuits: CircuitMap::new(),
            circuit_id_map: CircuitIdMap::new(),
        }
    }

    /// endless loop for processing the epochs, starting with the upcomming epoch (setup)
    pub fn run(&mut self) {
        let first_epoch;
        // poll directory client till we get an answer
        loop {
            match self.dir_client.next_epoch_info() {
                Some(epoch) => {
                    first_epoch = epoch;
                    break;
                }
                None => (),
            }
            // don't poll too hard
            sleep(Duration::from_secs(1));
        }

        info!(
            "Mix is getting busy, starting with epoch {}",
            first_epoch.epoch_no
        );
        let mut is_first = true;
        let mut setup_epoch = first_epoch;

        // hope that timing of communication rounds did not change compared to the previous epoch
        // and take it as first reference when to process setup packets
        let mut communication_epoch = setup_epoch.clone();

        // "endless" processing of epochs; life as a mix never gets boring!
        loop {
            // note: process_epoch waits till the start of the epoch(s) automatically
            self.process_epoch(&setup_epoch, &communication_epoch, is_first);

            // one more epoch done, some more to come!
            is_first = false;
            communication_epoch = setup_epoch;
            // TODO robustness: we should try to recover from not knowing the next epoch
            setup_epoch = self
                .dir_client
                .get_epoch_info(communication_epoch.epoch_no + 1)
                .expect("Don't know the next epoch");
        }
    }

    fn process_epoch(
        &mut self,
        setup_epoch: &EpochInfo,
        communication_epoch: &EpochInfo,
        is_first: bool,
    ) {
        let round_duration = Duration::from_secs(communication_epoch.round_duration as u64);
        let round_waiting = Duration::from_secs(communication_epoch.round_waiting as u64);
        let interval = round_duration + round_waiting;
        let mut next_round_start = match is_first {
            false => Duration::from_secs(communication_epoch.communication_start_time),
            true => Duration::from_secs(setup_epoch.setup_start_time),
        };
        let mut next_round_end = next_round_start + round_duration;

        info!(
            "Next up: setup for epoch {}, and communication for epoch {}",
            setup_epoch.epoch_no, communication_epoch.epoch_no
        );
        assert!(
            communication_epoch.number_of_rounds >= setup_epoch.path_length,
            "Not enough rounds to setup next epoch"
        );

        // "cache" our private ephemeral key (if we have one)
        let maybe_sk = self
            .dir_client
            .get_private_ephemeral_key(&setup_epoch.epoch_no);

        for round_no in 0..communication_epoch.number_of_rounds {
            if self.running.load(atomic::Ordering::SeqCst) == false {
                // quick and dirty panic because we shall stop
                // TODO security this is no graceful cleanup of keys!
                panic!("You told us to panic!")
            }
            // TODO move waiting inside the process functions?
            // wait till start of next round
            let wait_time = next_round_start
                .checked_sub(current_time())
                .expect("Did not finish last setup in time?");
            sleep(wait_time);
            next_round_start += interval;

            if is_first == false {
                self.process_communication_round(&communication_epoch, round_no);
            } else {
                info!(
                    "We would process round {} now, but nothing to do in our first epoch",
                    round_no
                )
            }

            // wait till round end
            let wait_time = next_round_end
                .checked_sub(current_time())
                .expect("Did not finish round in time?");
            sleep(wait_time);
            next_round_end += interval;

            let setup_layer = round_no;
            if setup_layer < setup_epoch.path_length {
                match &maybe_sk {
                    Some(sk) => self.process_setup_layer(&setup_epoch, setup_layer, &sk),
                    None => warn!("We could setup layer {} of epoch {} now, but we don't have the matching ephemeral key", setup_layer, setup_epoch.epoch_no)
                }
            }
        }
    }

    fn process_communication_round(&self, epoch: &EpochInfo, round_no: u32) {
        info!("Processing round {} of epoch {}", round_no, epoch.epoch_no);
    }

    fn process_setup_layer(&mut self, epoch: &EpochInfo, layer: u32, sk: &Key) {
        info!(
            "Processing setup layer {} of epoch {}",
            layer, epoch.epoch_no
        );
        let current_ttl = epoch.path_length - layer - 1;

        let mut batch = Vec::new();

        // first, check for new setup packets in the rx queues
        for queue in self.setup_rx_queues.iter_mut() {
            while let Ok(pkt) = queue.try_recv() {
                handle_new_setup_pkt(&mut self.pending_setup_pkts, pkt, epoch, layer);
            }
        }

        // process pending packets for this epoch
        let setup_pkts = match self.pending_setup_pkts.remove(&epoch.epoch_no) {
            Some(pkts) => pkts,
            None => VecDeque::new(),
        };
        // TODO insert our dummy packets first?
        // TODO performance parallel iteration (rayon? deque maybe not the best for this)
        // -> one circuit map and "batch" per thread to avoid locking?
        for pkt in setup_pkts.into_iter() {
            let pkt_ttl = pkt.ttl().expect("Should have be handled by gRPC");
            if pkt_ttl != current_ttl {
                warn!(
                    "Only expecting setup packets with TTL of {}, found one with {}",
                    current_ttl, pkt_ttl,
                );
                continue;
            }
            if self.circuits.contains_key(&pkt.circuit_id()) {
                warn!("Ignoring setup pkt with already used circuit id; should be catched earlier by gRPC");
                continue;
            }
            match Circuit::new(pkt, sk) {
                Ok((circuit, next_hop_info)) => {
                    self.circuit_id_map
                        .insert(circuit.upstream_id(), circuit.downstream_id());
                    self.circuits.insert(circuit.downstream_id(), circuit);
                    match next_hop_info {
                        SetupNextHop::Extend(create) => batch.push(create),
                        SetupNextHop::Rendezvous(tokens) => {
                            // XXX
                            unimplemented!();
                        }
                    }
                }
                Err(e) => {
                    warn!("Creating circuit failed: {}", e);
                    continue;
                }
            }
        }

        // XXX shuffle and send out batch, don't forget address information in metadata
    }
}

fn handle_new_setup_pkt(
    pending_map: &mut PendingSetupMap,
    pkt: SetupPacketWithPrev,
    epoch: &EpochInfo,
    layer: u32,
) {
    let current_ttl = epoch.path_length - layer - 1;
    let pkt_ttl = pkt.ttl().expect("Expected to reject this in gRPC!?");
    match pkt.epoch_no().cmp(&epoch.epoch_no) {
        Ordering::Less => {
            debug!(
                "Dropping late (by {} epochs) setup packet",
                epoch.epoch_no - pkt.epoch_no()
            );
        }
        Ordering::Greater => {
            // early setup packet -> store for later use
            insert_pending_setup_pkt(pending_map, pkt);
        }
        Ordering::Equal => match pkt_ttl.cmp(&current_ttl) {
            Ordering::Greater => {
                debug!(
                    "Dropping late (by {} hops) setup packet",
                    pkt_ttl - current_ttl
                );
            }
            Ordering::Less => {
                // this should not happen because of sync between mixes
                warn!(
                    "Dropping early (by {} hops) setup packet",
                    current_ttl - pkt_ttl
                );
            }
            Ordering::Equal => {
                // we could setup the circuit here, but just store it for now, process it soon
                insert_pending_setup_pkt(pending_map, pkt);
            }
        },
    }
}

fn insert_pending_setup_pkt(map: &mut PendingSetupMap, pkt: SetupPacketWithPrev) {
    let epoch_no = pkt.epoch_no();
    match map.get_mut(&epoch_no) {
        Some(queue) => queue.push_back(pkt),
        None => {
            let mut queue = VecDeque::new();
            queue.push_back(pkt);
            map.insert(epoch_no, queue);
        }
    }
}
