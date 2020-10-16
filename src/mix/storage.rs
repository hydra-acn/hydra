//! Storage for cells at entry mixes.
use log::*;
use std::collections::BTreeMap;
use std::sync::RwLock;

use crate::defs::CircuitId;
use crate::tonic_mix::Cell;

#[derive(Default)]
struct Circuit {
    cells: Vec<Cell>,
    _firebase_token: Option<String>,
}

// TODO cleanup once in a while
// TODO performance: avoid global lock on complete storage; "RSS" by circuit id instead
#[derive(Default)]
pub struct Storage {
    map: RwLock<BTreeMap<CircuitId, Circuit>>,
}

impl Storage {
    /// Create storage for `circuit_id`.
    /// Returns `true` if new storage was created and `false` if `circuit_id` existed already.
    pub fn create_circuit(&self, circuit_id: CircuitId, _firebase_token: Option<String>) -> bool {
        let mut map = self.map.write().expect("Lock poisoned");
        match map.get_mut(&circuit_id) {
            Some(_) => false,
            None => {
                map.insert(circuit_id, Circuit::default());
                true
            }
        }
    }

    pub fn insert_cell(&self, cell: Cell) {
        let mut map = self.map.write().expect("Lock poisoned");
        if let Some(circuit) = map.get_mut(&cell.circuit_id) {
            circuit.cells.push(cell);
        } else {
            warn!("Cell ready for delivery, but circuit id unknown -> dropping");
        }
    }

    /// Returns and removes all cells stored for `circuit_id`.
    /// Returns `None` if `circuit_id` is not known.
    pub fn remove_cells(&self, circuit_id: &CircuitId) -> Option<Vec<Cell>> {
        let mut map = self.map.write().expect("Lock poisoned");
        match map.get_mut(circuit_id) {
            Some(circuit) => Some(std::mem::replace(&mut circuit.cells, Vec::new())),
            None => None,
        }
    }
}
