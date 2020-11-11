use log::*;
use std::net::SocketAddr;

use crate::defs::Token;
use crate::error::Error;
use crate::tonic_directory::EpochInfo;

/// Map tokens to rendezvous nodes.
#[derive(Default)]
pub struct RendezvousMap {
    map: Vec<SocketAddr>,
}

/// Invariant: The map always has at least one rendezvous node.
impl RendezvousMap {
    pub fn new(epoch: &EpochInfo) -> Result<Self, Error> {
        if epoch.mixes.len() == 0 {
            return Err(Error::InputError(
                "No rendezvous mixes available".to_string(),
            ));
        }

        let mut rendezvous_nodes = Vec::new();
        for mix in epoch.mixes.iter() {
            match mix.rendezvous_address() {
                Some(a) => rendezvous_nodes.push((a, &mix.public_dh)),
                None => warn!("Found mix with no rendezvous address"),
            }
        }
        rendezvous_nodes.sort_by(|a, b| a.1.cmp(b.1));
        let map = rendezvous_nodes.into_iter().map(|(a, _)| a).collect();
        Ok(RendezvousMap { map })
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn index(&self, token: &Token) -> usize {
        (token % self.map.len() as u64) as usize
    }

    /// Set `subscribe` to `true` for sending subscriptions, `false` for publish.
    pub fn rendezvous_address_for_index(&self, idx: usize, subscribe: bool) -> Option<SocketAddr> {
        self.map.get(idx).map(|a| match subscribe {
            true => a.clone(),
            false => {
                let mut with_fast_port = a.clone();
                // TODO code don't hardcode +100
                with_fast_port.set_port(a.port() + 100);
                with_fast_port
            }
        })
    }

    /// Set `subscribe` to `true` for sending subscriptions, `false` for publish.
    pub fn rendezvous_address(&self, token: &Token, subscribe: bool) -> Option<SocketAddr> {
        let n = self.map.len();
        assert!(n > 0, "Checked during construction");
        let idx = (token % n as u64) as usize;
        self.rendezvous_address_for_index(idx, subscribe)
    }
}
