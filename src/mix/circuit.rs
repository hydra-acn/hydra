//! Circuit abstraction
use crate::crypto::key::Key;
use crate::defs::CircuitId;
use crate::tonic_mix::*;

pub struct Circuit {
    /// circuit id used on the link towards the client (upstream rx, downstream tx)
    _downstream_id: CircuitId,
    /// circuit id used on the link towards the rendezvous node (upstream tx, downstream rx)
    _upstream_id: CircuitId,
}

impl Circuit {
    /// creates the circuit and returns the setup packet for the next hop
    pub fn new(setup_pkt: &SetupPacket, _ephemeral_sk: &Key) -> (Self, SetupPacket) {
        let circuit = Circuit {
            _downstream_id: setup_pkt.circuit_id,
            _upstream_id: rand::random(),
        };
        (circuit, setup_pkt.clone()) // XXX really prepare the new pkt
    }
}
