use log::*;
use rand::seq::IteratorRandom;
use std::net::SocketAddr;

use crate::client::circuit::Circuit;
use crate::defs::{CellCmd, CircuitId, RoundNo};
use crate::epoch::EpochNo;
use crate::error::Error;
use crate::net::PacketWithNextHop;
use crate::tonic_directory::MixInfo;
use crate::tonic_mix::{Cell, SetupPacket};

pub struct DummyCircuit {
    circuit: Circuit,
    layer: u32,
    sent_cell: Option<Cell>,
}

impl DummyCircuit {
    pub fn new(
        epoch_no: EpochNo,
        layer: u32,
        path: &[MixInfo],
    ) -> Result<(Self, PacketWithNextHop<SetupPacket>), Error> {
        let (circuit, setup_pkt) = Circuit::new(epoch_no, path, Vec::new())?;
        let dummy = DummyCircuit {
            circuit,
            layer,
            sent_cell: None,
        };
        Ok((dummy, setup_pkt))
    }

    pub fn circuit_id(&self) -> CircuitId {
        self.circuit.circuit_id()
    }

    pub fn first_hop(&self) -> &SocketAddr {
        self.circuit.first_hop()
    }

    pub fn layer(&self) -> u32 {
        self.layer
    }

    pub fn pad(&mut self, round_no: RoundNo) -> Option<PacketWithNextHop<Cell>> {
        // TODO security: for now, dummy circuits are only used to monitor correct behavior of the
        // system by forcing the system to echo them back on the same circuit; to provide security
        // benefits, cells should be send "end-to-end" between *different* dummy circuits on the
        // same mix, with random payload; nevertheless, this should only happen occassionally -
        // otherwise rendezvous nodes find out that a circuit never has dummy circuits -> return
        // `None` in this case
        let mut cell = Cell::dummy(self.circuit.circuit_id(), round_no);
        // TODO security: use secure random source?
        let rng = &mut rand::thread_rng();
        // TODO security: use some Zipf-like distribution
        let token = self
            .circuit
            .dummy_tokens()
            .iter()
            .choose(rng)
            .expect("Expected at least one token");
        cell.set_token(*token);
        // for now, we want the cell echoed back by the rendezvous service
        cell.set_command(CellCmd::Broadcast);
        if self.sent_cell.is_some() {
            warn!(
                "Seems like we did not get the last cell back on dummy circuit {}",
                self.circuit.circuit_id()
            )
        }
        self.sent_cell = Some(cell.clone());
        // as we started with a dummy, we can ignore encryption errors
        self.circuit.onion_encrypt(&mut cell).unwrap_or(());
        Some(PacketWithNextHop::new(cell, *self.circuit.first_hop()))
    }

    pub fn receive_cell(&mut self, mut cell: Cell) {
        debug!(
            "Dummy circuit {} received a cell",
            self.circuit.circuit_id()
        );
        self.circuit.onion_decrypt(&mut cell).unwrap_or(());
        match &self.sent_cell {
            Some(c) if *c != cell => {
                // in last round: bytes 1 to 7 (cmd) should be zero
                if cell.onion[1..8].iter().any(|b| *b != 0) {
                    warn!("Received cell is not as expected")
                }
            }
            None => warn!("Received a cell without having send one before"),
            _ => (), // everything ok
        }
        self.sent_cell = None;
    }
}
