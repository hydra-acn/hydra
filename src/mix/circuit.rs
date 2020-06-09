//! Circuit abstraction
use super::grpc::SetupPacketWithPrev;
use crate::crypto::key::Key;
use crate::crypto::x448::generate_shared_secret;
use crate::defs::{CircuitId, Token};
use crate::error::Error;
use crate::tonic_mix::*;

use std::net::IpAddr;

pub struct Circuit {
    downstream_id: CircuitId,
    upstream_id: CircuitId,
}

pub struct ExtendInfo {
    addr: IpAddr,
    port: u16,
    setup_pkt: SetupPacket,
}

pub enum SetupNextHop {
    Extend(ExtendInfo),
    Rendezvous(Vec<Token>),
}

impl Circuit {
    /// Creates the circuit (if everything is ok). Furthermore, it either returns the next setup
    /// packet (with destination) or the set of tokens to subscribe to (last layer)
    pub fn new(
        pkt: SetupPacketWithPrev,
        ephemeral_sk: &Key,
    ) -> Result<(Self, SetupNextHop), Error> {
        let setup_pkt = pkt.into_inner();
        let client_pk = Key::clone_from_slice(&setup_pkt.public_dh);
        // XXX
        let master_key = generate_shared_secret(&client_pk, ephemeral_sk)?;
        let circuit = Circuit {
            downstream_id: setup_pkt.circuit_id,
            upstream_id: rand::random(),
        };
        // XXX really prepare the new pkt/the tokens
        unimplemented!();
    }

    /// circuit id used on the link towards the client (upstream rx, downstream tx)
    pub fn downstream_id(&self) -> CircuitId {
        self.downstream_id
    }

    /// circuit id used on the link towards the rendezvous node (upstream tx, downstream rx)
    pub fn upstream_id(&self) -> CircuitId {
        self.upstream_id
    }
}
