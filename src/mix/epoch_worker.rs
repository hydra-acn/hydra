//! Main loop for processing epochs
use log::*;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{self, AtomicBool};
use std::sync::Arc;
use std::thread::sleep;
use tokio::time::Duration;

use crate::defs::RoundNo;
use crate::epoch::{current_time, EpochInfo};
use crate::net::PacketWithNextHop;
use crate::rendezvous::processor::{process_publish, process_subscribe, publish_t, subscribe_t};

use super::cell_processor::{cell_rss_t, process_cell};
use super::circuit::{CellDirection, NextCellStep};
use super::directory_client;
use super::epoch_state::{EpochSetupState, EpochState};
use super::setup_processor::{create_dummy_circuit, process_setup_pkt, setup_t};

pub struct Worker {
    running: Arc<AtomicBool>,
    dir_client: Arc<directory_client::Client>,
    grpc_state: Arc<super::grpc::State>,
    setup_processor: setup_t::Processor,
    subscribe_processor: subscribe_t::Processor,
    cell_processor: cell_rss_t::Processor,
    publish_processor: publish_t::Processor,
    setup_state: EpochSetupState,
    state: EpochState,
}

impl Worker {
    pub fn new(
        running: Arc<AtomicBool>,
        dir_client: Arc<directory_client::Client>,
        grpc_state: Arc<super::grpc::State>,
        setup_processor: setup_t::Processor,
        subscribe_processor: subscribe_t::Processor,
        cell_processor: cell_rss_t::Processor,
        publish_processor: publish_t::Processor,
    ) -> Self {
        Worker {
            running: running.clone(),
            dir_client,
            grpc_state,
            setup_processor,
            subscribe_processor,
            cell_processor,
            publish_processor,
            setup_state: EpochSetupState::new(0),
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
        let subround_interval = round_duration / (2 * communication_epoch.path_length + 1);
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
            // TODO code: move inside separate function
            let setup_layer = round_no;
            let deadline = next_round_start - subround_interval - Duration::from_secs(1);
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
                        let bloom_bitmap = self.setup_state.bloom_bitmap();
                        let f = |pkt| {
                            process_setup_pkt(pkt, dir_client.clone(), setup_epoch, sk, setup_layer, rendezvous_map.clone(), circuit_id_map.clone(), circuit_map.clone(), dummy_circuit_map.clone(), bloom_bitmap.clone())
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
        // read/calculate important timing information
        let round_duration = Duration::from_secs(epoch.round_duration as u64);
        let round_waiting = Duration::from_secs(epoch.round_waiting as u64);
        let round_start = Duration::from_secs(epoch.communication_start_time as u64)
            + round_no * (round_duration + round_waiting);
        let subround_interval = round_duration / (2 * epoch.path_length + 1);
        let mut send_time = round_start;

        // wait till the round starts: processing begins one slot before the first send time
        let process_start = send_time - subround_interval;
        let wait_for = process_start
            .checked_sub(current_time())
            .expect("Did not finish last setup processing in time?");
        sleep(wait_for);
        info!("Processing round {} of epoch {}", round_no, epoch.epoch_no);

        // mix view: upstream
        for layer in 0..epoch.path_length {
            self.process_subround(
                round_no,
                layer,
                epoch.path_length - 1,
                CellDirection::Upstream,
                &send_time,
            );
            send_time += subround_interval;
        }

        // rendezvous node: process publish requests
        let sub_map = self.state.subscription_map();
        info!(".. process publish of round {}", round_no);
        let f = |cell| process_publish(cell, sub_map.clone());
        self.publish_processor.process_till(f, send_time);
        info!(".. send injections of round {}", round_no);
        self.publish_processor.send(); // injection
        send_time += subround_interval;

        // mix view: downstream
        for layer in (0..epoch.path_length).rev() {
            self.process_subround(
                round_no,
                layer,
                epoch.path_length - 1,
                CellDirection::Downstream,
                &send_time,
            );
            send_time += subround_interval;
        }
        info!(
            "Finished processing round {} of epoch {}",
            round_no, epoch.epoch_no
        );
    }

    fn process_subround(
        &mut self,
        round_no: RoundNo,
        layer: u32,
        max_layer: u32,
        direction: CellDirection,
        send_time: &Duration,
    ) {
        info!(
            ".. processing sub-round of round {}, layer {}, direction {:?}",
            round_no, layer, direction,
        );
        let circuit_id_map = &*self.state.circuit_id_map();
        let circuit_map = &*self.state.circuits();
        let dummy_circuit_map = &*self.state.dummy_circuits();
        let f = |cell| {
            process_cell(
                cell,
                round_no,
                layer,
                direction,
                circuit_id_map,
                circuit_map,
                dummy_circuit_map,
            )
        };
        // TODO duration should be passed by ref
        self.cell_processor.process_till(f, *send_time);

        // insert dummy cells if necessary
        let dummy_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 1337);
        for (_, circuit) in self.state.circuits().iter() {
            if layer != circuit.layer() {
                // no out-of-sync dummies
                continue;
            }
            match circuit.pad(round_no, direction) {
                Some(step) => match step {
                    NextCellStep::Relay(c) => self.cell_processor.pad(vec![c]),
                    NextCellStep::Rendezvous(c) => self.cell_processor.alt_pad(vec![c]),
                    NextCellStep::Deliver(c) => self
                        .cell_processor
                        .pad(vec![PacketWithNextHop::new(c, dummy_addr)]),
                    NextCellStep::Wait(_) => error!("Why should a dummy be out of sync?"),
                    NextCellStep::Drop => error!("Why should a dummy be dropped right away?"),
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
                    Some(c) => self.cell_processor.pad(vec![c]),
                    None => (),
                }
            }
        }

        // relay, publish or deliver
        info!(
            ".. sending sub-round of round {}, layer {}, direction {:?}",
            round_no, layer, direction,
        );
        match direction {
            CellDirection::Upstream if layer == max_layer => {
                // publish
                self.cell_processor.alt_send();
            }
            CellDirection::Downstream if layer == 0 => {
                // deliver
                let out = self.cell_processor.output();
                self.grpc_state.deliver(out);
            }
            // default: relay
            _ => self.cell_processor.send(),
        }
    }
}
