use log::*;
use std::net::SocketAddr;

use crate::defs::{Token, CONTACT_SERVICE_TOKEN};
use crate::error::Error;
use crate::tonic_directory::EpochInfo;

/// Map tokens to rendezvous nodes.
#[derive(Default)]
pub struct RendezvousMap {
    map: Vec<SocketAddr>,
    contact_service: Option<SocketAddr>,
}

/// Invariant: The map always has at least one rendezvous node.
impl RendezvousMap {
    pub fn new(epoch: &EpochInfo) -> Result<Self, Error> {
        if epoch.mixes.is_empty() {
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

        let contact_service = match crate::net::ip_addr_from_slice(&epoch.contact_service_addr) {
            Ok(ip) => {
                if epoch.contact_service_port <= std::u16::MAX as u32 {
                    Some(SocketAddr::new(ip, epoch.contact_service_port as u16))
                } else {
                    warn!("Contact service has an invalid port");
                    None
                }
            }
            Err(e) => {
                warn!("Contact service has an invalid IP address: {}", e);
                None
            }
        };

        Ok(RendezvousMap {
            map,
            contact_service,
        })
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
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
            true => *a,
            false => {
                let mut with_fast_port = *a;
                // TODO code don't hardcode +100
                with_fast_port.set_port(a.port() + 100);
                with_fast_port
            }
        })
    }

    /// Set `subscribe` to `true` for sending subscriptions, `false` for publish.
    pub fn rendezvous_address(&self, token: &Token, subscribe: bool) -> Option<SocketAddr> {
        if *token == CONTACT_SERVICE_TOKEN {
            self.contact_service
        } else {
            let n = self.map.len();
            assert!(n > 0, "Checked during construction");
            let idx = (token % n as u64) as usize;
            self.rendezvous_address_for_index(idx, subscribe)
        }
    }
}
