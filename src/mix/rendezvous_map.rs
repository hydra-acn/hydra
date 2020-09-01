use log::*;
use std::net::SocketAddr;

use crate::defs::Token;
use crate::tonic_directory::EpochInfo;

/// Map tokens to rendezvous nodes.
#[derive(Default)]
pub struct RendezvousMap {
    map: Vec<SocketAddr>,
}

impl RendezvousMap {
    pub fn new(epoch: &EpochInfo) -> Self {
        let mut rendezvous_nodes = Vec::new();
        for mix in epoch.mixes.iter() {
            match mix.rendezvous_address() {
                Some(a) => rendezvous_nodes.push((a, &mix.public_dh)),
                None => warn!("Found mix with no rendezvous address"),
            }
        }
        rendezvous_nodes.sort_by(|a, b| a.1.cmp(b.1));
        let map = rendezvous_nodes.into_iter().map(|(a, _)| a).collect();
        RendezvousMap { map }
    }

    pub fn rendezvous_address(&self, token: &Token) -> Option<SocketAddr> {
        match self.map.len() {
            0 => None,
            n => self.map.get((token % n as u64) as usize).cloned(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.map.len() == 0
    }
}
