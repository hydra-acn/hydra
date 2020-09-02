//! Main loop for processing epochs
use log::*;
use std::sync::atomic::{self, AtomicBool};
use std::sync::Arc;
use std::thread::sleep;
use tokio::time::Duration;

use crate::defs::RoundNo;
use crate::epoch::{current_time, EpochInfo};
use crate::rendezvous::processor::{process_publish, process_subscribe, publish_t, subscribe_t};
use crate::tonic_mix::*;

use super::circuit::{CellDirection, NextCellStep};
use super::directory_client;
use super::epoch_state::{CircuitIdMap, CircuitMap, DummyCircuitMap, EpochSetupState, EpochState};
use super::sender::CellBatch;
use super::setup_processor::{create_dummy_circuit, process_setup_pkt, setup_t};

type CellRxQueue = tokio::sync::mpsc::UnboundedReceiver<Cell>;
type CellTxQueue = spmc::Sender<CellBatch>;
type EarlyCellEnqueue = tokio::sync::mpsc::UnboundedSender<Cell>;

pub struct Worker {
    running: Arc<AtomicBool>,
    dir_client: Arc<directory_client::Client>,
    grpc_state: Arc<super::grpc::State>,
    cell_rx_queues: Vec<CellRxQueue>,
    cell_tx_queue: CellTxQueue,
    publish_tx_queue: CellTxQueue,
    early_cell_enqueue: EarlyCellEnqueue,
    early_cell_dequeue: CellRxQueue,
    setup_processor: setup_t::Processor,
    subscribe_processor: subscribe_t::Processor,
    publish_processor: publish_t::Processor,
    setup_state: EpochSetupState,
    state: EpochState,
}

impl Worker {
    pub fn new(
        running: Arc<AtomicBool>,
        dir_client: Arc<directory_client::Client>,
        grpc_state: Arc<super::grpc::State>,
        cell_rx_queues: Vec<CellRxQueue>,
        cell_tx_queue: CellTxQueue,
        publish_tx_queue: CellTxQueue,
        setup_processor: setup_t::Processor,
        subscribe_processor: subscribe_t::Processor,
        publish_processor: publish_t::Processor,
    ) -> Self {
        let (early_cell_enqueue, early_cell_dequeue) = tokio::sync::mpsc::unbounded_channel();
        Worker {
            running: running.clone(),
            dir_client,
            grpc_state,
            cell_rx_queues,
            cell_tx_queue,
            publish_tx_queue,
            early_cell_enqueue,
            early_cell_dequeue,
            setup_processor,
            subscribe_processor,
            publish_processor,
            setup_state: EpochSetupState::default(),
            state: EpochState::default(),
        }
    }

    fn shall_terminate(&self) -> bool {
        self.running.load(atomic::Ordering::SeqCst) == false
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
            if self.shall_terminate() {
                return;
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
            if self.shall_terminate() {
                return;
            }

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
        self.setup_state.init_rendezvous_map(setup_epoch);

        let round_duration = Duration::from_secs(communication_epoch.round_duration as u64);
        let round_waiting = Duration::from_secs(communication_epoch.round_waiting as u64);
        let interval = round_duration + round_waiting;
        let mut next_round_start = match is_first {
            false => Duration::from_secs(communication_epoch.communication_start_time),
            true => Duration::from_secs(setup_epoch.setup_start_time),
        };

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
            if self.shall_terminate() {
                return;
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

            // round done, time for some setup handling
            let setup_layer = round_no;
            let deadline = next_round_start - Duration::from_secs(2);
            if setup_layer < setup_epoch.path_length {
                // acting as mix: create new circuits
                match &maybe_sk {
                    Some(sk) => {
                        info!(
                            "Processing setup layer {} of epoch {}",
                            setup_layer, setup_epoch.epoch_no
                        );
                        let dir_client = &self.dir_client;
                        let rendezvous_map = self.setup_state.rendezvous_map();
                        let circuit_id_map = self.setup_state.circuit_id_map();
                        let circuit_map = self.setup_state.circuits();
                        let dummy_circuit_map = self.setup_state.dummy_circuits();
                        let f = |pkt| {
                            process_setup_pkt(pkt, dir_client.clone(), setup_epoch, sk, setup_layer, rendezvous_map.clone(), circuit_id_map.clone(), circuit_map.clone(), dummy_circuit_map.clone())
                        };
                        self.setup_processor.process_till(f, deadline);
                        if setup_layer < setup_epoch.path_length - 1 {
                            // one additional dummy circuit for all but the last layer
                            let dummy_extend = create_dummy_circuit(self.setup_state.dummy_circuits().clone(), &self.dir_client, setup_epoch.epoch_no, setup_layer, setup_epoch.path_length - setup_layer - 1);
                            self.setup_processor.pad(vec![dummy_extend]);

                            // send setup packets
                            self.setup_processor.send();
                        } else {
                            // send subscriptions
                            self.setup_processor.alt_send();
                        }
                    },
                    None => warn!("We could setup layer {} of epoch {} now, but we don't have the matching ephemeral key", setup_layer, setup_epoch.epoch_no)
                }
            } else if setup_layer == setup_epoch.path_length {
                // acting as rendezvous node: process subscriptions
                info!("Processing subscriptions of epoch {}", setup_epoch.epoch_no);
                let sub_map = self.setup_state.subscription_map();
                let f = |req| process_subscribe(req, sub_map.clone());
                self.subscribe_processor.process_till(f, deadline);
            }
        }
        info!("Communication of epoch {} and setup of epoch {} done, updating the circuit maps accordingly", communication_epoch.epoch_no, setup_epoch.epoch_no);
        self.state = EpochState::finalize_setup(&mut self.setup_state);
        info!(
            "We have {} circuits and {} dummy circuits for the next communication",
            self.state.circuits().len(),
            self.state.dummy_circuits().len()
        );
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
            let end = if layer == epoch.path_length - 1 {
                None // don't sleep in last subround
            } else {
                Some(&subround_end)
            };
            self.process_subround(round_no, layer, CellDirection::Upstream, end);
            if layer != epoch.path_length - 1 {
                subround_end += subround_interval;
            }
        }

        // process publish
        let sub_map = self.state.subscription_map();
        let f = |cell| process_publish(cell, sub_map.clone());
        self.publish_processor.process_till(f, subround_end);
        self.publish_processor.send(); // injection
        subround_end += subround_interval;

        // sleep till first downstream subround
        sleep(
            subround_end
                .checked_sub(current_time())
                .expect("Did not finish injection in time?"),
        );
        subround_end += subround_interval;
        // process injected cells from the rendezvous service
        // TODO performance: parallel -> separate pipeline
        for queue in self.cell_rx_queues.iter_mut() {
            while let Ok(cell) = queue.try_recv() {
                inject_cell(&self.state.circuits(), &self.state.circuit_id_map(), cell);
            }
        }

        // downstream
        for layer in (0..epoch.path_length).rev() {
            self.process_subround(
                round_no,
                layer,
                CellDirection::Downstream,
                Some(&subround_end),
            );
            subround_end += subround_interval;
        }
        info!(
            "Finished processing round {} of epoch {}",
            round_no, epoch.epoch_no
        );
    }

    /// Process subround and sleep till it is over (if `subround_end` is not `None`).
    fn process_subround(
        &mut self,
        round_no: RoundNo,
        layer: u32,
        direction: CellDirection,
        subround_end: Option<&Duration>,
    ) {
        info!(
            ".. processing sub-round of round {}, layer {}, direction {:?}",
            round_no, layer, direction
        );
        let mut relay_batch = Vec::new();
        let mut publish_batch = Vec::new();
        let mut deliver_batch = Vec::new();
        let mut early_cells = Vec::new();

        // collect and process all cells we received on different queues
        // TODO performance: parallel
        let early_queue_ref = &mut self.early_cell_dequeue;
        let queue_iter = self
            .cell_rx_queues
            .iter_mut()
            .chain(std::iter::once(early_queue_ref));
        for queue in queue_iter {
            while let Ok(cell) = queue.try_recv() {
                match process_cell(
                    &self.state.circuits(),
                    &self.state.dummy_circuits(),
                    &self.state.circuit_id_map(),
                    cell,
                    round_no,
                    layer,
                    direction,
                ) {
                    Some(step) => match step {
                        NextCellStep::Relay(c) => relay_batch.push(c),
                        NextCellStep::Rendezvous(c) => publish_batch.push(c),
                        NextCellStep::Deliver(c) => deliver_batch.push(c),
                        NextCellStep::Wait(c) => early_cells.push(c),
                    },
                    None => (),
                }
            }
        }

        // insert dummy cells if necessary
        for (_, circuit) in self.state.circuits().iter() {
            if layer != circuit.layer() {
                // no out-of-sync dummies
                continue;
            }
            match circuit.pad(round_no, direction) {
                Some(step) => match step {
                    NextCellStep::Relay(c) => relay_batch.push(c),
                    NextCellStep::Rendezvous(c) => publish_batch.push(c),
                    NextCellStep::Deliver(c) => deliver_batch.push(c),
                    NextCellStep::Wait(_c) => error!("Why should a dummy be out of sync?"),
                },
                None => (),
            }
        }

        // send on dummy circuits as well, but only upstream
        if let CellDirection::Upstream = direction {
            for (_, dummy_circuit) in self.state.dummy_circuits().iter() {
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
                .send(vec![relay_batch])
                .unwrap_or_else(|e| error!("Sender task is gone!? ({}))", e));
        } else if publish_batch.len() > 0 {
            self.publish_tx_queue
                .send(vec![publish_batch])
                .unwrap_or_else(|e| error!("Sender task is gone!? ({}))", e));
        } else if deliver_batch.len() > 0 {
            self.grpc_state.deliver(deliver_batch);
        }

        // put early cells back into a rx queue
        for cell in early_cells.into_iter() {
            self.early_cell_enqueue
                .send(cell)
                .expect("Early cell queue gone!?");
        }

        if let CellDirection::Downstream = direction {
            if layer == 0 {
                // don't sleep in last subround
                return;
            }
        }

        // sleep till round end
        if let Some(end) = subround_end {
            sleep(end.checked_sub(current_time()).expect("Out of sync"));
        }
    }
}

/// Returns either the cell to forward next (might be a dummy) together with the next action or
/// `None` if the circuit id is not known or we are the downstream endpoint (dummy circuits) or the
/// cell was a duplicate or the cell was dropped for other reasons.
fn process_cell(
    circuits: &CircuitMap,
    dummy_circuits: &DummyCircuitMap,
    circuit_id_map: &CircuitIdMap,
    cell: Cell,
    round_no: RoundNo,
    layer: u32,
    direction: CellDirection,
) -> Option<NextCellStep> {
    if cell.round_no != round_no {
        warn!(
            "Dropping cell with wrong round number. Expected {}, got {}.",
            round_no, cell.round_no
        );
        return None;
    }

    match direction {
        CellDirection::Upstream => {
            if let Some(circuit) = circuits.get(&cell.circuit_id) {
                return circuit.process_cell(cell, layer, direction);
            } else {
                warn!(
                    "Dropping upstream cell with unknown circuit id {}",
                    cell.circuit_id
                );
            }
        }
        CellDirection::Downstream => {
            if let Some(dummy_circuit) = dummy_circuits.get(&cell.circuit_id) {
                dummy_circuit.receive_cell(cell);
                return None;
            }

            if let Some(mapped_id) = circuit_id_map.get(&cell.circuit_id) {
                if let Some(circuit) = circuits.get(&mapped_id) {
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

fn inject_cell(circuits: &CircuitMap, circuit_id_map: &CircuitIdMap, cell: Cell) {
    if let Some(downstream_id) = circuit_id_map.get(&cell.circuit_id) {
        if let Some(circuit) = circuits.get(&downstream_id) {
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
