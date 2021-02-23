use log::*;
use std::convert::TryInto;
use std::sync::{Arc, RwLock};

use crate::defs::{CircuitId, Token};
use crate::epoch::EpochNo;
use crate::net::PacketWithNextHop;
use crate::tonic_mix::{CircuitSubscription, Subscription};

use super::directory_client;
use super::rendezvous_map::RendezvousMap;

#[derive(Default)]
pub struct SubCollector {
    rendezvous_map: Arc<RendezvousMap>,
    sub_vec: Vec<RwLock<Vec<CircuitSubscription>>>,
}

impl SubCollector {
    /// Create a new collector for the given rendezvous mixes.
    pub fn new(rendezvous_map: Arc<RendezvousMap>) -> Self {
        let mut sub_vec = Vec::new();
        for _ in 0..rendezvous_map.len() {
            sub_vec.push(RwLock::default());
        }
        SubCollector {
            rendezvous_map,
            sub_vec,
        }
    }

    /// Collect subscription to many tokens at once (setup phase).
    pub fn collect_subscriptions(&self, tokens: Vec<Token>, circuit_id: CircuitId) {
        let mut partition = Vec::new();
        for _ in 0..self.rendezvous_map.len() {
            let circuit = CircuitSubscription {
                circuit_id: circuit_id
                    .try_into()
                    .expect("This circuit id should fit in 32bit"),
                tokens: Vec::new(),
            };
            partition.push(circuit);
        }

        for token in tokens.into_iter() {
            let idx = self.rendezvous_map.index(&token);
            partition[idx].tokens.push(token);
        }

        for (idx, circuit) in partition.into_iter().enumerate() {
            let mut guard = self
                .sub_vec
                .get(idx)
                .expect("Wrong size of sub collector!?")
                .write()
                .expect("Lock poisoned");
            guard.push(circuit);
        }
    }

    /// Optimized for subscribing to only one token during communication phase.
    pub fn collect_subscription(&self, token: Token, circuit_id: CircuitId) {
        let idx = self.rendezvous_map.index(&token);
        let circuit = CircuitSubscription {
            circuit_id: circuit_id
                .try_into()
                .expect("This circuit id should fit in 32bit"),
            tokens: vec![token],
        };
        let mut guard = self
            .sub_vec
            .get(idx)
            .expect("Wrong size of sub collector!?")
            .write()
            .expect("Lock poisoned");
        guard.push(circuit);
    }

    /// Convert the collected subcsriptions to packets and remove them from the collector.
    pub fn extract_final_subscriptions(
        &self,
        epoch_no: EpochNo,
        dir_client: &directory_client::Client,
    ) -> Vec<PacketWithNextHop<Subscription>> {
        let sub_addr = crate::net::ip_addr_to_vec(&dir_client.config().addr);
        let sub_port = dir_client.config().fast_port as u32;
        let mut pkts = Vec::new();
        for (idx, sub) in self.sub_vec.iter().enumerate() {
            let mut guard = sub.write().expect("Lock poisoned");
            if guard.is_empty() {
                // no subs at this rendezvous mix -> don't send an "empty" subscription request
                continue;
            }
            // move circuit subscriptions out of RwLock
            let circuits = std::mem::replace(&mut *guard, Vec::new());
            // TODO security: shuffle circuits inside sub
            match self.rendezvous_map.rendezvous_address_for_index(idx, true) {
                Some(addr) => {
                    let sub = Subscription {
                        epoch_no,
                        addr: sub_addr.clone(),
                        port: sub_port,
                        circuits,
                    };
                    pkts.push(PacketWithNextHop::new(sub, addr));
                }
                None => warn!("Don't know the rendezvous address for index {} ", idx),
            }
        }
        pkts
    }
}
