use log::*;
use std::net::SocketAddr;

use crate::defs::{CircuitId, Token};
use crate::tonic_mix::SubscriptionVector;

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

type MapType = std::collections::BTreeMap<Token, Vec<Endpoint>>;

/// Mapping tokens to subscribers
#[derive(Clone, Default)]
pub struct SubscriptionMap {
    map: MapType,
}

impl SubscriptionMap {
    pub fn new() -> Self {
        SubscriptionMap {
            map: MapType::new(),
        }
    }

    pub fn subscribe(&mut self, to: &SubscriptionVector) {
        let addr = match to.socket_addr() {
            Some(a) => a,
            None => {
                warn!("Subscription validity should have been checked by gRPC before");
                return;
            }
        };

        for sub in to.subs.iter() {
            let endpoint = Endpoint {
                addr,
                circuit_id: sub.circuit_id,
            };
            for token in sub.tokens.iter() {
                match self.map.get_mut(&token) {
                    Some(vec) => vec.push(endpoint.clone()),
                    None => {
                        self.map.insert(*token, vec![endpoint.clone()]);
                    }
                }
            }
        }
    }

    pub fn get_subscribers(&self, token: &Token) -> Vec<Endpoint> {
        match self.map.get(token) {
            Some(vec) => vec.clone(),
            None => Vec::new(),
        }
    }
}
