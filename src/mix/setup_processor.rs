use log::*;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use crate::crypto::key::Key;
use crate::defs::{CircuitId, Token};
use crate::epoch::EpochNo;
use crate::grpc::type_extensions::SetupPacketWithPrev;
use crate::net::{ip_addr_to_vec, PacketWithNextHop};
use crate::tonic_directory::EpochInfo;
use crate::tonic_mix::{SetupPacket, Subscription, SubscriptionVector};

use super::circuit::{Circuit, NextSetupStep};
use super::directory_client;
use super::dummy_circuit::DummyCircuit;
use super::epoch_state::{CircuitIdMap, CircuitMap, DummyCircuitMap};
use super::rendezvous_map::RendezvousMap;

crate::define_pipeline_types!(
    setup_t,
    SetupPacketWithPrev,
    PacketWithNextHop<SetupPacket>,
    PacketWithNextHop<SubscriptionVector>
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
) -> setup_t::Result {
    let current_ttl = epoch.path_length - layer - 1;
    let pkt_ttl = pkt.ttl().expect("Expected to reject this in gRPC!?");
    // first of all, check if its the right place and time for this setup packet
    match pkt.epoch_no().cmp(&epoch.epoch_no) {
        Ordering::Less => {
            debug!(
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
                debug!(
                    "Dropping late (by {} hops) setup packet",
                    pkt_ttl - current_ttl
                );
                return setup_t::Result::Drop;
            }
            Ordering::Less => {
                // this should not happen because of sync between mixes
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
            warn!("Dropping setup pkt with already used circuit id; should be catched earlier by gRPC");
            return setup_t::Result::Drop;
        }
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
                    create_subscriptions(tokens, &*dir_client, upstream_id, &rendezvous_map)
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

fn create_subscriptions(
    tokens: Vec<Token>,
    dir_client: &directory_client::Client,
    circuit_id: CircuitId,
    rendezvous_map: &RendezvousMap,
) -> setup_t::Result {
    let mut map: HashMap<SocketAddr, Vec<Token>> = HashMap::new();
    for token in tokens.into_iter() {
        let rendezvous_addr = match rendezvous_map.rendezvous_address(&token) {
            Some(addr) => addr,
            None => {
                warn!("Don't know rendezvous node for token {}", token);
                continue;
            }
        };
        match map.get_mut(&rendezvous_addr) {
            Some(ts) => ts.push(token),
            None => {
                map.insert(rendezvous_addr, vec![token]);
                ()
            }
        };
    }
    let mut sub_requests = Vec::new();
    for (rendezvous_addr, tokens) in map.into_iter() {
        let req = SubscriptionVector {
            epoch_no: 42, // deprecated
            addr: ip_addr_to_vec(&dir_client.config().addr),
            port: dir_client.config().relay_port as u32,
            subs: vec![Subscription { circuit_id, tokens }],
        };
        sub_requests.push(PacketWithNextHop::new(req, rendezvous_addr));
    }
    setup_t::Result::MultipleAlt(sub_requests)
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
        .select_path_tunable(epoch_no, Some(ttl as usize), Some(dir_client.fingerprint()))
        .expect("No path available");
    let (circuit, extend) =
        DummyCircuit::new(epoch_no, layer, &path).expect("Creating dummy circuit failed");
    let mut dummy_circuit_map_guard = dummy_circuit_map.write().expect("Lock poisoned");
    dummy_circuit_map_guard.insert(circuit.circuit_id(), circuit);
    extend
}
