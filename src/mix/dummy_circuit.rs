use log::*;
use rand::seq::IteratorRandom;
use std::net::SocketAddr;
use std::sync::RwLock;

use crate::client::circuit::Circuit;
use crate::crypto::cprng::thread_cprng;
use crate::defs::{CircuitId, RoundNo};
use crate::epoch::EpochNo;
use crate::error::Error;
use crate::net::cell::{Cell, CellCmd};
use crate::net::PacketWithNextHop;
use crate::tonic_directory::MixInfo;
use crate::tonic_mix::SetupPacket;

pub struct DummyCircuit {
    circuit: Circuit,
    first_hop: SocketAddr,
    layer: u32,
    sent_cell: RwLock<Option<Cell>>,
}

impl DummyCircuit {
    pub fn new(
        epoch_no: EpochNo,
        layer: u32,
        path: &[MixInfo],
    ) -> Result<(Self, PacketWithNextHop<SetupPacket>), Error> {
        let (circuit, setup_pkt) = Circuit::new(epoch_no, path, Vec::new())?;
        let mut first_hop = circuit.first_hop().clone();
        // TODO code: don't hardcode +200
        first_hop.set_port(first_hop.port() + 200);
        let dummy = DummyCircuit {
            circuit,
            first_hop,
            layer,
            sent_cell: RwLock::default(),
        };
        Ok((dummy, setup_pkt))
    }

    pub fn circuit_id(&self) -> CircuitId {
        self.circuit.circuit_id()
    }

    pub fn first_hop(&self) -> &SocketAddr {
        &self.first_hop
    }

    pub fn layer(&self) -> u32 {
        self.layer
    }

    pub fn pad(&self, round_no: RoundNo) -> Option<PacketWithNextHop<Cell>> {
        // TODO security: for now, dummy circuits are only used to monitor correct behavior of the
        // system by forcing the system to echo them back on the same circuit; to provide security
        // benefits, cells should be send "end-to-end" between *different* dummy circuits on the
        // same mix, with random payload; nevertheless, this should only happen occassionally -
        // otherwise rendezvous nodes find out that a circuit never has dummy circuits -> return
        // `None` in this case
        let mut cell = Cell::dummy(self.circuit.circuit_id(), round_no);
        // TODO security: use some Zipf-like distribution
        let token = self
            .circuit
            .dummy_tokens()
            .iter()
            .choose(&mut thread_cprng())
            .expect("Expected at least one token");
        cell.set_token(*token);
        // for now, we want the cell echoed back by the rendezvous service
        cell.set_command(CellCmd::Broadcast);
        let mut sent_cell_guard = self.sent_cell.write().expect("Lock poisoned");
        if sent_cell_guard.is_some() {
            warn!(
                "Seems like we did not get the last cell back on dummy circuit {}",
                self.circuit.circuit_id()
            )
        }
        *sent_cell_guard = Some(cell.clone());
        // as we started with a dummy, we can ignore encryption errors
        self.circuit
            .onion_encrypt(cell.round_no(), cell.onion_mut())
            .unwrap_or(());
        Some(PacketWithNextHop::new(cell, self.first_hop))
    }

    pub fn receive_cell(&self, mut cell: Cell) {
        debug!(
            "Dummy circuit {} received a cell",
            self.circuit.circuit_id()
        );
        self.circuit
            .onion_decrypt(cell.round_no(), cell.onion_mut())
            .unwrap_or(());
        let mut sent_cell_guard = self.sent_cell.write().expect("Lock poisoned");
        match &*sent_cell_guard {
            Some(c) if *c != cell => {
                // in last round: bytes 1 to 7 (cmd) should be zero
                if cell.onion()[1..8].iter().any(|b| *b != 0) {
                    warn!("Received cell is not as expected")
                }
            }
            None => warn!("Received a cell without having send one before"),
            _ => (), // everything ok
        }
        *sent_cell_guard = None;
    }
}
