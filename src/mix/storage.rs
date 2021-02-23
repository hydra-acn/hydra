//! Storage for cells at entry mixes.
use crossbeam_channel as xbeam;
use log::*;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};

use crate::defs::{CircuitId, RoundNo};
use crate::epoch::EpochNo;
use crate::net::cell::Cell;

use super::cfg::Config;
use super::epoch_worker::SyncBeat;

struct Circuit {
    cells: Vec<Cell>,
}

impl Circuit {
    pub fn new() -> Self {
        Circuit { cells: Vec::new() }
    }
}

// TODO cleanup once in a while
pub struct Storage {
    circuit_maps: Vec<RwLock<HashMap<CircuitId, Circuit>>>,
    firebase_auth_key: Option<String>,
    firebase_map: RwLock<BTreeMap<EpochNo, HashMap<CircuitId, String>>>,
    sync_rx: xbeam::Receiver<SyncBeat>,
}

impl Storage {
    pub fn new(cfg: &Config, sync_rx: xbeam::Receiver<SyncBeat>) -> Self {
        let mut circuit_maps = Vec::new();
        // TODO code: make number of maps configurable; should be high enough to make probability
        // that two threads access the same map at the same time small
        for _ in 0..128 {
            circuit_maps.push(RwLock::default());
        }
        Storage {
            circuit_maps,
            firebase_auth_key: cfg.firebase.server_auth_key.clone(),
            firebase_map: RwLock::new(BTreeMap::new()),
            sync_rx,
        }
    }

    fn map_idx(&self, circuit_id: CircuitId) -> usize {
        (circuit_id % self.circuit_maps.len() as u64) as usize
    }

    /// Create storage for `circuit_id`.
    /// Returns `true` if new storage was created and `false` if `circuit_id` existed already.
    pub fn create_circuit(
        &self,
        circuit_id: CircuitId,
        epoch_no: EpochNo,
        firebase_token: Option<String>,
    ) -> bool {
        if let Some(token) = firebase_token {
            self.update_firebase_token(epoch_no, circuit_id, token);
        }

        let idx = self.map_idx(circuit_id);
        let mut map = self.circuit_maps[idx].write().expect("Lock poisoned");
        match map.get_mut(&circuit_id) {
            Some(_) => false,
            None => {
                map.insert(circuit_id, Circuit::new());
                true
            }
        }
    }

    pub fn update_firebase_token(&self, epoch_no: EpochNo, circuit_id: CircuitId, token: String) {
        let mut fb_map = self.firebase_map.write().expect("Lock poisoned");
        match fb_map.get_mut(&epoch_no) {
            Some(cmap) => {
                cmap.insert(circuit_id, token);
            }
            None => {
                let mut cmap = HashMap::new();
                cmap.insert(circuit_id, token);
                fb_map.insert(epoch_no, cmap);
            }
        }
    }

    pub fn insert_cell(&self, cell: Cell) {
        let cid = cell.circuit_id();
        let idx = self.map_idx(cid);
        let mut map = self.circuit_maps[idx].write().expect("Lock poisoned");
        if let Some(circuit) = map.get_mut(&cid) {
            circuit.cells.push(cell);
        } else {
            warn!("Cell ready for delivery, but circuit id unknown -> dropping");
        }
    }

    /// Returns and removes all cells stored for `circuit_id`.
    /// Returns `None` if `circuit_id` is not known.
    pub fn remove_cells(&self, circuit_id: &CircuitId) -> Option<Vec<Cell>> {
        let idx = self.map_idx(*circuit_id);
        let mut map = self.circuit_maps[idx].write().expect("Lock poisoned");
        match map.get_mut(circuit_id) {
            Some(circuit) => Some(std::mem::replace(&mut circuit.cells, Vec::new())),
            None => None,
        }
    }

    /// Delete circuit and return any cells stored for `circuit_id`.
    /// Returns `None` if `circuit_id` is not known.
    pub fn delete_circuit(&self, circuit_id: &CircuitId) -> Option<Vec<Cell>> {
        let idx = self.map_idx(*circuit_id);
        let mut map = self.circuit_maps[idx].write().expect("Lock poisoned");
        match map.remove(circuit_id) {
            Some(circuit) => Some(circuit.cells),
            None => None,
        }
    }

    pub async fn send_firebase_notifications(&self, epoch_no: EpochNo, round_no: RoundNo) {
        // TODO performance: don't clone circuit map; need a read-only view instead
        let cmap;
        {
            let map = self.firebase_map.read().expect("Lock poisoned");
            cmap = map.get(&epoch_no).cloned().unwrap_or_default()
        }

        if self.firebase_auth_key.is_none() {
            if !cmap.is_empty() {
                warn!("At least one user requested firebase support, but we don't know the server auth key");
            }
            return;
        }

        let fcm_client = fcm::Client::new();
        for token in cmap.values() {
            let mut builder =
                fcm::MessageBuilder::new(self.firebase_auth_key.as_ref().unwrap(), &token);
            builder.priority(fcm::Priority::High).time_to_live(60);

            let mut body = BTreeMap::new();
            body.insert("epoch_no", epoch_no);
            body.insert("round_no", round_no);
            builder.data(&body).expect("This is valid json!?");

            match fcm_client.send(builder.finalize()).await {
                Ok(_) => (),
                Err(e) => warn!("Sending FCM msg failed: {}", e),
            }
        }
    }

    pub async fn handle_sync_beats(&self) {
        let rx = self.sync_rx.clone();
        let beat = tokio::task::spawn_blocking(move || rx.recv())
            .await
            .expect("Spawn failed")
            .expect("Worker thread gone!?");
        match beat {
            SyncBeat::Deliver(epoch_no, round_no) => {
                self.send_firebase_notifications(epoch_no, round_no).await;
            }
        }
    }
}

pub async fn run(storage: Arc<Storage>) {
    loop {
        storage.handle_sync_beats().await;
    }
}
