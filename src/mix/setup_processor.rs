use log::*;
use std::cmp::Ordering;
use std::sync::{Arc, RwLock};

use crate::crypto::key::Key;
use crate::epoch::EpochNo;
use crate::grpc::type_extensions::SetupPacketWithPrev;
use crate::net::PacketWithNextHop;
use crate::tonic_directory::EpochInfo;
use crate::tonic_mix::{SetupPacket, Subscription};

use super::circuit::{Circuit, NextSetupStep};
use super::directory_client;
use super::dummy_circuit::DummyCircuit;
use super::epoch_state::{CircuitIdMap, CircuitMap, DummyCircuitMap, DupFilter};
use super::rendezvous_map::RendezvousMap;
use super::sub_collector::SubCollector;

crate::define_pipeline_types!(
    setup_t,
    SetupPacketWithPrev,
    PacketWithNextHop<SetupPacket>,
    PacketWithNextHop<Subscription>
);

// TODO code: refactor epoch state (only one struct, inside Arc and optionally RwLock) to make
// parameters less ugly
pub fn process_setup_pkt(
    pkt: SetupPacketWithPrev,
    dir_client: Arc<directory_client::Client>,
    epoch: &EpochInfo,
    sk: &Key,
    layer: u32,
    rendezvous_map: Arc<RendezvousMap>,
    circuit_id_map: Arc<RwLock<CircuitIdMap>>,
    circuit_map: Arc<RwLock<CircuitMap>>,
    dummy_circuit_map: Arc<RwLock<DummyCircuitMap>>,
    bloom_bitmap: Arc<RwLock<DupFilter>>,
    sub_collector: Arc<SubCollector>,
) -> setup_t::Result {
    let current_ttl = epoch.path_length - layer - 1;
    let pkt_ttl = pkt.ttl().expect("Expected to reject this in gRPC!?");
    // first of all, check if its the right place and time for this setup packet
    match pkt.epoch_no().cmp(&epoch.epoch_no) {
        Ordering::Less => {
            warn!(
                "Dropping late (by {} epochs) setup packet",
                epoch.epoch_no - pkt.epoch_no()
            );
            return setup_t::Result::Drop;
        }
        Ordering::Greater => {
            // early setup packet -> requeue
            return setup_t::Result::Requeue(pkt);
        }
        Ordering::Equal => match pkt_ttl.cmp(&current_ttl) {
            Ordering::Greater => {
                warn!(
                    "Dropping late (by {} hops) setup packet",
                    pkt_ttl - current_ttl
                );
                return setup_t::Result::Drop;
            }
            Ordering::Less => {
                warn!(
                    "Requeing early (by {} hops) setup packet",
                    current_ttl - pkt_ttl
                );
                return setup_t::Result::Requeue(pkt);
            }
            Ordering::Equal => {
                // right place, right time -> process below
            }
        },
    };

    {
        let circuit_map_guard = circuit_map.read().expect("Lock poisoned");
        if circuit_map_guard.contains_key(&pkt.circuit_id()) {
            warn!("Dropping setup pkt with already used circuit id");
            return setup_t::Result::Drop;
        }
    }

    let mut dup_filter = bloom_bitmap.write().expect("Lock poisoned");
    if dup_filter.check_and_set(pkt.auth_tag()) {
        warn!("Dropping duplicate setup packet");
        return setup_t::Result::Drop;
    }

    match Circuit::new(pkt, sk, rendezvous_map, layer, epoch.number_of_rounds - 1) {
        Ok((circuit, next_hop_info)) => {
            // insert mappings
            let upstream_id = circuit.upstream_id();
            {
                let mut circuit_id_map_guard = circuit_id_map.write().expect("Lock poisoned");
                circuit_id_map_guard.insert(upstream_id, circuit.downstream_id());
            }
            {
                let mut circuit_map_guard = circuit_map.write().expect("Lock poisoned");
                circuit_map_guard.insert(circuit.downstream_id(), circuit);
            }
            match next_hop_info {
                NextSetupStep::Extend(extend) => setup_t::Result::Out(extend),
                NextSetupStep::Rendezvous(tokens) => {
                    sub_collector.collect_subscriptions(tokens, upstream_id);
                    setup_t::Result::Drop
                }
            }
        }
        Err(e) => {
            warn!(
                "Creating circuit failed: {}; creating dummy circuit instead",
                e
            );
            if current_ttl > 0 {
                let dummy_extend = create_dummy_circuit(
                    dummy_circuit_map,
                    &*dir_client,
                    epoch.epoch_no,
                    layer,
                    current_ttl,
                );
                setup_t::Result::Out(dummy_extend)
            } else {
                setup_t::Result::Drop
            }
        }
    }
}

/// Panics on failure as dummy circuits are essential for anonymity.
/// Returns the info necessary for circuit extension (next hop, setup packet).
pub fn create_dummy_circuit(
    dummy_circuit_map: Arc<RwLock<DummyCircuitMap>>,
    dir_client: &directory_client::Client,
    epoch_no: EpochNo,
    layer: u32,
    ttl: u32,
) -> PacketWithNextHop<SetupPacket> {
    // TODO code: move path selection inside DummyCircuit::new()?
    let path = dir_client
        .select_path_tunable(
            epoch_no,
            Some(ttl as usize),
            Some(dir_client.fingerprint()),
            None,
        )
        .expect("No path available");
    let (circuit, extend) =
        DummyCircuit::new(epoch_no, layer, &path).expect("Creating dummy circuit failed");
    let mut dummy_circuit_map_guard = dummy_circuit_map.write().expect("Lock poisoned");
    dummy_circuit_map_guard.insert(circuit.circuit_id(), circuit);
    extend
}
