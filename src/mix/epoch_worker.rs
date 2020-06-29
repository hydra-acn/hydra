//! processing of one epoch
use log::*;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{self, AtomicBool};
use std::sync::Arc;
use std::thread::sleep;
use tokio::time::Duration;

use super::circuit::{
    CellDirection, Circuit, ClientCircuit, ExtendInfo, NextCellStep, NextSetupStep,
};
use super::directory_client::{self, RendezvousMap};
use super::grpc::SetupPacketWithPrev;
use super::sender::{CellBatch, PacketWithNextHop, SetupBatch, SubscribeBatch};
use crate::crypto::key::Key;
use crate::defs::{CircuitId, RoundNo, Token};
use crate::epoch::{current_time, EpochInfo, EpochNo};
use crate::tonic_mix::*;

type SetupRxQueue = tokio::sync::mpsc::UnboundedReceiver<SetupPacketWithPrev>;
type SetupTxQueue = spmc::Sender<SetupBatch>;

type SubscribeTxQueue = spmc::Sender<SubscribeBatch>;

type CellRxQueue = tokio::sync::mpsc::UnboundedReceiver<Cell>;
type CellTxQueue = spmc::Sender<CellBatch>;

type PublishTxQueue = spmc::Sender<CellBatch>;

type PendingSetupMap = BTreeMap<EpochNo, VecDeque<SetupPacketWithPrev>>;
type CircuitMap = BTreeMap<CircuitId, Circuit>;
type ClientCircuitMap = BTreeMap<CircuitId, ClientCircuit>;
type CircuitIdMap = BTreeMap<CircuitId, CircuitId>;

/// Bundling the various circuit maps used during one epoch
#[derive(Default)]
struct CircuitMapBundle {
    // mapping downstream ids to circuits
    circuits: CircuitMap,
    // mapping *upstream* ids to dummy circuits (they have no downstream id)
    dummy_circuits: ClientCircuitMap,
    // mapping the upstream circuit id to the downstream id of an circuit
    circuit_id_map: CircuitIdMap,
}

pub struct Worker {
    running: Arc<AtomicBool>,
    dir_client: Arc<directory_client::Client>,
    grpc_state: Arc<super::grpc::State>,
    setup_rx_queues: Vec<SetupRxQueue>,
    setup_tx_queue: SetupTxQueue,
    subscribe_tx_queue: SubscribeTxQueue,
    cell_rx_queues: Vec<CellRxQueue>,
    cell_tx_queue: CellTxQueue,
    publish_tx_queue: PublishTxQueue,
    pending_setup_pkts: PendingSetupMap,
    setup_circuits: CircuitMapBundle,
    communication_circuits: CircuitMapBundle,
}

impl Worker {
    pub fn new(
        running: Arc<AtomicBool>,
        dir_client: Arc<directory_client::Client>,
        grpc_state: Arc<super::grpc::State>,
        setup_rx_queues: Vec<SetupRxQueue>,
        setup_tx_queue: SetupTxQueue,
        subscribe_tx_queue: SubscribeTxQueue,
        cell_rx_queues: Vec<CellRxQueue>,
        cell_tx_queue: CellTxQueue,
        publish_tx_queue: PublishTxQueue,
    ) -> Self {
        Worker {
            running: running.clone(),
            dir_client,
            grpc_state,
            setup_rx_queues,
            setup_tx_queue,
            subscribe_tx_queue,
            cell_rx_queues,
            cell_tx_queue,
            publish_tx_queue,
            pending_setup_pkts: PendingSetupMap::new(),
            communication_circuits: CircuitMapBundle::default(),
            setup_circuits: CircuitMapBundle::default(),
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
        info!("Communication of epoch {} and setup of epoch {} done, updating the circuit maps accordingly", communication_epoch.epoch_no, setup_epoch.epoch_no);
        // TODO performance: swap is most likely expensive here ...
        std::mem::swap(&mut self.communication_circuits, &mut self.setup_circuits);
        debug!(
            "We have {} circuits and {} dummy circuits for the next communication",
            self.communication_circuits.circuits.len(),
            self.communication_circuits.dummy_circuits.len()
        );
        self.setup_circuits = CircuitMapBundle::default();
    }

    fn process_communication_round(&mut self, epoch: &EpochInfo, round_no: RoundNo) {
        info!("Processing round {} of epoch {}", round_no, epoch.epoch_no);
        let round_duration = Duration::from_secs(epoch.round_duration as u64);
        let round_waiting = Duration::from_secs(epoch.round_waiting as u64);
        // round start should be now
        let round_start = Duration::from_secs(epoch.communication_start_time as u64)
            + round_no * (round_duration + round_waiting);
        let subround_interval = round_duration / (2 * epoch.path_length + 1);
        let mut subround_end = round_start + subround_interval;
        // upstream
        for layer in 0..epoch.path_length {
            self.process_subround(round_no, layer, CellDirection::Upstream, &subround_end);
            subround_end += subround_interval;
        }

        // sleep another interval to allow rendezvous service to respond
        let wait_time = subround_end
            .checked_sub(current_time())
            .expect("Did not finish last upstream subround in time?");
        sleep(wait_time);
        subround_end += subround_interval;

        // inject cells from the rendezvous service
        // TODO performance: parallel
        for queue in self.cell_rx_queues.iter_mut() {
            while let Ok(cell) = queue.try_recv() {
                inject_cell(
                    &mut self.communication_circuits.circuits,
                    &self.communication_circuits.circuit_id_map,
                    cell,
                );
            }
        }

        // downstream
        for layer in (0..epoch.path_length).rev() {
            self.process_subround(round_no, layer, CellDirection::Downstream, &subround_end);
            subround_end += subround_interval;
        }
        info!(
            "Finished processing round {} of epoch {}",
            round_no, epoch.epoch_no
        );
    }

    /// Process subround and sleep till it is over.
    fn process_subround(
        &mut self,
        round_no: RoundNo,
        layer: u32,
        direction: CellDirection,
        subround_end: &Duration,
    ) {
        info!(
            ".. processing sub-round of round {}, layer {}, direction {:?}",
            round_no, layer, direction
        );
        let mut relay_batch = CellBatch::new();
        let mut rendezvous_batch = CellBatch::new();
        let mut deliver_batch = Vec::new();
        // collect and process all cells we received
        // TODO performance: parallel
        for queue in self.cell_rx_queues.iter_mut() {
            while let Ok(cell) = queue.try_recv() {
                if cell.round_no != round_no {
                    warn!(
                        "Dropping cell with wrong round number. Expected {}, got {}.",
                        round_no, cell.round_no
                    );
                    continue;
                }
                match process_cell(
                    &mut self.communication_circuits.circuits,
                    &mut self.communication_circuits.dummy_circuits,
                    &self.communication_circuits.circuit_id_map,
                    cell,
                    layer,
                    direction,
                ) {
                    Some(step) => match step {
                        NextCellStep::Relay(c) => relay_batch.push(c),
                        NextCellStep::Rendezvous(c) => rendezvous_batch.push(c),
                        NextCellStep::Deliver(c) => deliver_batch.push(c),
                    },
                    None => (),
                }
            }
        }

        // insert dummy cells if necessary
        for (_, circuit) in self.communication_circuits.circuits.iter_mut() {
            if layer != circuit.layer() {
                // no out-of-sync dummies
                continue;
            }
            match circuit.pad(round_no, direction) {
                Some(step) => match step {
                    NextCellStep::Relay(c) => relay_batch.push(c),
                    NextCellStep::Rendezvous(c) => rendezvous_batch.push(c),
                    NextCellStep::Deliver(c) => deliver_batch.push(c),
                },
                None => (),
            }
        }

        // send on dummy circuits as well, but only upstream
        if let CellDirection::Upstream = direction {
            for (_, dummy_circuit) in self.communication_circuits.dummy_circuits.iter_mut() {
                if layer != dummy_circuit.layer() {
                    // no out-of-sync dummies
                    continue;
                }
                match dummy_circuit.pad(round_no) {
                    Some(step) => relay_batch.push(step),
                    None => (),
                }
            }
        }

        if relay_batch.len() > 0 {
            self.cell_tx_queue
                .send(relay_batch)
                .unwrap_or_else(|e| error!("Sender task is gone!? ({}))", e));
        } else if rendezvous_batch.len() > 0 {
            self.publish_tx_queue
                .send(rendezvous_batch)
                .unwrap_or_else(|e| error!("Sender task is gone!? ({}))", e));
        } else if deliver_batch.len() > 0 {
            self.grpc_state.deliver(deliver_batch);
        }

        if let CellDirection::Downstream = direction {
            if layer == 0 {
                // don't sleep in last subround
                return;
            }
        }

        // sleep till round end
        let wait_time = subround_end
            .checked_sub(current_time())
            .expect("Did not finish subround in time?");
        sleep(wait_time);
    }

    fn process_setup_layer(&mut self, epoch: &EpochInfo, layer: u32, sk: &Key) {
        info!(
            "Processing setup layer {} of epoch {}",
            layer, epoch.epoch_no
        );
        let current_ttl = epoch.path_length - layer - 1;
        // TODO performance we could create the map once per epoch instead
        let rendezvous_map = Arc::new(RendezvousMap::new(epoch));

        let mut setup_batch = SetupBatch::new();
        let mut subscription_map = HashMap::new();

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
        // TODO performance: parallel iteration (rayon? deque maybe not the best for this)
        // -> one circuit map and "batch" per thread to avoid locking?
        // TODO robustness: we could check that we don't overwrite circuits due to same upstream id
        for pkt in setup_pkts.into_iter() {
            let pkt_ttl = pkt.ttl().expect("Should have be handled by gRPC");
            if pkt_ttl != current_ttl {
                warn!(
                    "Only expecting setup packets with TTL of {}, found one with {}",
                    current_ttl, pkt_ttl,
                );
                continue;
            }
            if self.setup_circuits.circuits.contains_key(&pkt.circuit_id()) {
                warn!("Ignoring setup pkt with already used circuit id; should be catched earlier by gRPC");
                continue;
            }
            // TODO security: replay protection based on auth_tag of pkt
            match Circuit::new(
                pkt,
                sk,
                rendezvous_map.clone(),
                layer,
                epoch.number_of_rounds - 1,
            ) {
                Ok((circuit, next_hop_info)) => {
                    match next_hop_info {
                        NextSetupStep::Extend(extend) => setup_batch.push(extend),
                        NextSetupStep::Rendezvous(tokens) => {
                            add_subscriptions(
                                &mut subscription_map,
                                circuit.upstream_id(),
                                tokens,
                                &rendezvous_map,
                            );
                        }
                    }
                    self.setup_circuits
                        .circuit_id_map
                        .insert(circuit.upstream_id(), circuit.downstream_id());
                    self.setup_circuits
                        .circuits
                        .insert(circuit.downstream_id(), circuit);
                }
                Err(e) => {
                    warn!(
                        "Creating circuit failed: {}; creating dummy circuit instead",
                        e
                    );
                    if current_ttl > 0 {
                        let extend = self.create_dummy_circuit(epoch.epoch_no, layer, current_ttl);
                        setup_batch.push(extend);
                    }
                }
            }
        }
        if current_ttl > 0 {
            // create one additional dummy circuit
            let extend = self.create_dummy_circuit(epoch.epoch_no, layer, current_ttl);
            setup_batch.push(extend);
        }
        // send batch
        if setup_batch.len() > 0 {
            self.setup_tx_queue
                .send(setup_batch)
                .unwrap_or_else(|_| error!("Sender is gone!?"));
        } else {
            send_subscribe_batch(
                &mut subscription_map,
                &self.dir_client,
                epoch.epoch_no,
                &mut self.subscribe_tx_queue,
            );
        }
    }

    /// Panics on failure as dummy circuits are essential for anonymity.
    /// Returns the info necessary for circuit extension (next hop, setup packet).
    fn create_dummy_circuit(&mut self, epoch_no: EpochNo, layer: u32, ttl: u32) -> ExtendInfo {
        let path = self
            .dir_client
            .select_path(epoch_no, ttl)
            .expect("No path available");
        let (circuit, extend) =
            ClientCircuit::new(epoch_no, layer, path).expect("Creating dummy circuit failed");
        self.setup_circuits
            .dummy_circuits
            .insert(circuit.circuit_id(), circuit);
        extend
    }
}

/// Returns either the cell to forward next (might be a dummy) together with the next action or
/// `None` if the circuit id is not known or we are the downstream endpoint (dummy circuits) or the
/// cell was a duplicate
fn process_cell(
    circuits: &mut CircuitMap,
    dummy_circuits: &mut ClientCircuitMap,
    circuit_id_map: &CircuitIdMap,
    cell: Cell,
    layer: u32,
    direction: CellDirection,
) -> Option<NextCellStep> {
    match direction {
        CellDirection::Upstream => {
            if let Some(circuit) = circuits.get_mut(&cell.circuit_id) {
                return circuit.process_cell(cell, layer, direction);
            } else {
                warn!(
                    "Dropping upstream cell with unknown circuit id {}",
                    cell.circuit_id
                );
            }
        }
        CellDirection::Downstream => {
            if let Some(dummy_circuit) = dummy_circuits.get_mut(&cell.circuit_id) {
                dummy_circuit.receive_cell(cell);
                return None;
            }

            if let Some(mapped_id) = circuit_id_map.get(&cell.circuit_id) {
                if let Some(circuit) = circuits.get_mut(&mapped_id) {
                    return circuit.process_cell(cell, layer, direction);
                }
            }

            warn!(
                "Dropping downstream cell with unknown circuit id {}",
                cell.circuit_id
            );
        }
    }
    None
}

fn inject_cell(circuits: &mut CircuitMap, circuit_id_map: &CircuitIdMap, cell: Cell) {
    if let Some(downstream_id) = circuit_id_map.get(&cell.circuit_id) {
        if let Some(circuit) = circuits.get_mut(&downstream_id) {
            circuit.inject(cell);
        } else {
            warn!("Circuit for upstream id {} is gone!?", cell.circuit_id);
        }
    } else {
        warn!(
            "Don't know the upstream circuit id {} for injection",
            cell.circuit_id
        );
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

fn add_subscriptions(
    sub_map: &mut HashMap<SocketAddr, BTreeMap<CircuitId, Vec<Token>>>,
    circuit_id: CircuitId,
    tokens: Vec<Token>,
    rendezvous_map: &RendezvousMap,
) {
    if rendezvous_map.is_empty() {
        warn!("No rendezvous nodes?");
        return;
    }
    for token in tokens {
        let rendezvous_addr = rendezvous_map.rendezvous_address(&token).unwrap();
        match sub_map.get_mut(&rendezvous_addr) {
            Some(circuit_map) => match circuit_map.get_mut(&circuit_id) {
                Some(vec) => vec.push(token),
                None => {
                    circuit_map.insert(circuit_id, vec![token]);
                    ()
                }
            },
            None => {
                let mut circuit_map = BTreeMap::new();
                circuit_map.insert(circuit_id, vec![token]);
                sub_map.insert(rendezvous_addr, circuit_map);
            }
        }
    }
}

fn send_subscribe_batch(
    sub_map: &mut HashMap<SocketAddr, BTreeMap<CircuitId, Vec<Token>>>,
    dir_client: &directory_client::Client,
    epoch_no: EpochNo,
    tx_queue: &mut SubscribeTxQueue,
) {
    let inject_addr = crate::net::ip_addr_to_vec(&dir_client.config().addr);
    let inject_port = dir_client.config().relay_port as u32;
    let mut batch = SubscribeBatch::new();
    for (rendezvous_addr, circuit_map) in sub_map.into_iter() {
        let mut pkt = SubscriptionVector {
            epoch_no,
            addr: inject_addr.clone(),
            port: inject_port,
            subs: vec![],
        };
        for (circuit_id, tokens) in circuit_map.into_iter() {
            tokens.sort();
            let sub = Subscription {
                circuit_id: *circuit_id,
                tokens: tokens.clone(),
            };
            pkt.subs.push(sub);
        }
        batch.push(PacketWithNextHop::new(pkt, *rendezvous_addr));
    }
    tx_queue
        .send(batch)
        .unwrap_or_else(|e| warn!("Sender is gone!? ({})", e));
}
