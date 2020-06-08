//! Circuit abstraction
use crate::crypto::key::Key;
use crate::defs::CircuitId;
use crate::tonic_mix::*;

pub struct Circuit {
    downstream_id: CircuitId,
    upstream_id: CircuitId,
}

impl Circuit {
    /// creates the circuit and returns the setup packet for the next hop
    pub fn new(setup_pkt: &SetupPacket, _ephemeral_sk: &Key) -> (Self, SetupPacket) {
        let circuit = Circuit {
            downstream_id: setup_pkt.circuit_id,
            upstream_id: rand::random(),
        };
        (circuit, setup_pkt.clone()) // XXX really prepare the new pkt
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
