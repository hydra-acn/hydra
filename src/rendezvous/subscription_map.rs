use log::*;
use std::collections::HashMap;
use std::convert::TryInto;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::RwLock;
use std::time::Duration;

use crate::defs::Token;
use crate::epoch::current_time;
use crate::tonic_mix::Subscription;

pub type CircuitId = u32;

#[derive(Clone, Debug, PartialEq)]
pub struct Endpoint {
    addr: SocketAddr,
    circuit_id: CircuitId,
}

impl Endpoint {
    pub fn new(addr: SocketAddr, circuit_id: CircuitId) -> Self {
        Endpoint { addr, circuit_id }
    }

    pub fn addr(&self) -> &SocketAddr {
        &self.addr
    }

    pub fn circuit_id(&self) -> CircuitId {
        self.circuit_id
    }
}

#[derive(Clone)]
struct SmallEndpoint {
    addr_idx: u16,
    circuit_id: CircuitId,
}

type MapType = HashMap<Token, Vec<SmallEndpoint>>;

/// Mapping tokens to subscribers
pub struct SubscriptionMap {
    map: Vec<RwLock<MapType>>,
    addr_map: RwLock<Vec<SocketAddr>>,
    drop_idx: AtomicUsize,
}

impl Default for SubscriptionMap {
    fn default() -> Self {
        SubscriptionMap::new()
    }
}

impl SubscriptionMap {
    pub fn new() -> Self {
        let mut map = Vec::new();
        // TODO code: don't hardcode
        for _ in 0..128 {
            map.push(RwLock::default());
        }
        SubscriptionMap {
            map,
            addr_map: RwLock::default(),
            drop_idx: 0.into(),
        }
    }

    fn idx(&self, token: Token) -> usize {
        token as usize % self.map.len()
    }

    pub fn subscribe(&self, sub: Subscription) {
        let addr = match sub.socket_addr() {
            Some(a) => a,
            None => {
                warn!("Subscription validity should have been checked by gRPC before");
                return;
            }
        };

        let addr_idx;
        {
            let mut addr_guard = self.addr_map.write().expect("Lock poisoned");
            addr_idx = match addr_guard.iter().position(|a| *a == addr) {
                Some(idx) => idx,
                None => {
                    addr_guard.push(addr);
                    addr_guard.len() - 1
                }
            };
        }

        for circuit in sub.circuits.into_iter() {
            let endpoint = SmallEndpoint {
                addr_idx: addr_idx.try_into().expect("Too many mixes?"),
                circuit_id: circuit.circuit_id,
            };

            for token in circuit.tokens.into_iter() {
                let mut map_guard = self.map[self.idx(token)].write().expect("Lock poisoned");
                match map_guard.get_mut(&token) {
                    Some(vec) => vec.push(endpoint.clone()),
                    None => {
                        map_guard.insert(token, vec![endpoint.clone()]);
                    }
                }
            }
        }
    }

    fn get_endpoint(&self, e: &SmallEndpoint) -> Endpoint {
        // TODO performance: read-only view would be better
        let guard = self.addr_map.read().expect("Lock poisoned");
        Endpoint {
            addr: *guard.get(e.addr_idx as usize).unwrap(),
            circuit_id: e.circuit_id,
        }
    }

    pub fn get_subscribers(&self, token: &Token) -> Vec<Endpoint> {
        // TODO performance: read-only view would be better
        let map_guard = self.map[self.idx(*token)].read().expect("Lock poisoned");
        match map_guard.get(token) {
            Some(vec) => vec.iter().map(|e| self.get_endpoint(e)).collect(),
            None => Vec::new(),
        }
    }

    pub fn drop_some(&self, deadline: Duration) {
        loop {
            if let None = deadline.checked_sub(current_time()) {
                return;
            }
            let drop_idx = self.drop_idx.fetch_add(1, Ordering::Relaxed);
            if drop_idx >= self.map.len() {
                return;
            }
            let mut guard = self.map[drop_idx].write().unwrap();
            *guard = HashMap::default();
        }
    }
}
