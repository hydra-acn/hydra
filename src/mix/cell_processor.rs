use log::*;

use crate::defs::{RoundNo, PUBLISH_ROUND_NO};
use crate::net::cell::Cell;
use crate::net::PacketWithNextHop;

use super::circuit::{CellDirection, NextCellStep};
use super::epoch_state::{CircuitIdMap, CircuitMap, DummyCircuitMap};
use super::sub_collector::SubCollector;

crate::define_pipeline_types!(cell_rss_t, Cell, PacketWithNextHop<Cell>, Cell);

impl std::convert::From<NextCellStep> for cell_rss_t::Result {
    fn from(s: NextCellStep) -> Self {
        match s {
            NextCellStep::Relay(c) => cell_rss_t::Result::Out(c),
            NextCellStep::Rendezvous(c) => cell_rss_t::Result::Out(c),
            NextCellStep::Deliver(c) => cell_rss_t::Result::Alt(c),
            NextCellStep::Wait(c) => cell_rss_t::Result::Requeue(c),
            NextCellStep::Drop => cell_rss_t::Result::Drop,
        }
    }
}

pub fn process_cell(
    cell: Cell,
    round_no: RoundNo,
    incomming_round_no: RoundNo,
    layer: u32,
    max_layer: u32,
    direction: CellDirection,
    circuit_id_map: &CircuitIdMap,
    circuits: &CircuitMap,
    dummy_circuits: &DummyCircuitMap,
    sub_collector: &SubCollector,
) -> cell_rss_t::Result {
    if cell.round_no() != incomming_round_no {
        if let CellDirection::Upstream = direction {
            if cell.round_no() == PUBLISH_ROUND_NO && layer == max_layer {
                // seems like we are behind in time -> requeue cells that shall be published already
                return cell_rss_t::Result::Requeue(cell);
            }
        }

        debug!(
            "Dropping cell with wrong round number. Expected {}, got {}.",
            incomming_round_no,
            cell.round_no()
        );
        return cell_rss_t::Result::Drop;
    }

    match direction {
        CellDirection::Upstream => {
            if let Some(circuit) = circuits.get(&cell.circuit_id()) {
                return circuit
                    .process_cell(cell, round_no, layer, direction, sub_collector)
                    .into();
            } else {
                warn!(
                    "Dropping upstream cell with unknown circuit id {}",
                    cell.circuit_id()
                );
            }
        }
        CellDirection::Downstream => {
            if let Some(dummy_circuit) = dummy_circuits.get(&cell.circuit_id()) {
                dummy_circuit.receive_cell(cell);
                return cell_rss_t::Result::Drop;
            }

            if let Some(mapped_id) = circuit_id_map.get(&cell.circuit_id()) {
                if let Some(circuit) = circuits.get(&mapped_id) {
                    return circuit
                        .process_cell(cell, round_no, layer, direction, sub_collector)
                        .into();
                }
            }

            warn!(
                "Dropping downstream cell with unknown circuit id {}",
                cell.circuit_id()
            );
        }
    }
    // default: drop
    cell_rss_t::Result::Drop
}
