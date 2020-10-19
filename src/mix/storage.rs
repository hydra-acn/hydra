//! Storage for cells at entry mixes.
use crossbeam_channel as xbeam;
use log::*;
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use crate::defs::{CircuitId, RoundNo};
use crate::epoch::EpochNo;
use crate::tonic_mix::Cell;

use super::epoch_worker::SyncBeat;

// TODO code: don't hardcode
const FCM_AUTH_KEY: &str = "AAAA_bAyLik:APA91bGxLbubzFvUKKveyS-_61sExaXvyESqDEMni9FlR5ib2RikubU0y82ofpLbOhrf6L7tUAymNOkjeC81m6nuDHEzWldddo2Yzok4DUoDx2epkdeA4MZ_tdOSIsZ9QMRh1A9pMJLP";

struct Circuit {
    cells: Vec<Cell>,
}

impl Circuit {
    pub fn new() -> Self {
        Circuit { cells: Vec::new() }
    }
}

// TODO cleanup once in a while
// TODO performance: avoid global lock on complete storage; "RSS" by circuit id instead
pub struct Storage {
    circuit_map: RwLock<BTreeMap<CircuitId, Circuit>>,
    firebase_map: RwLock<BTreeMap<EpochNo, Vec<String>>>,
    sync_rx: xbeam::Receiver<SyncBeat>,
}

impl Storage {
    pub fn new(sync_rx: xbeam::Receiver<SyncBeat>) -> Self {
        Storage {
            circuit_map: RwLock::new(BTreeMap::new()),
            firebase_map: RwLock::new(BTreeMap::new()),
            sync_rx,
        }
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
            let mut fb_map = self.firebase_map.write().expect("Lock poisoned");
            match fb_map.get_mut(&epoch_no) {
                Some(vec) => vec.push(token),
                None => {
                    fb_map.insert(epoch_no, vec![token]);
                    ()
                }
            }
        }

        let mut map = self.circuit_map.write().expect("Lock poisoned");
        match map.get_mut(&circuit_id) {
            Some(_) => false,
            None => {
                map.insert(circuit_id, Circuit::new());
                true
            }
        }
    }

    pub fn insert_cell(&self, cell: Cell) {
        let mut map = self.circuit_map.write().expect("Lock poisoned");
        if let Some(circuit) = map.get_mut(&cell.circuit_id) {
            circuit.cells.push(cell);
        } else {
            warn!("Cell ready for delivery, but circuit id unknown -> dropping");
        }
    }

    /// Returns and removes all cells stored for `circuit_id`.
    /// Returns `None` if `circuit_id` is not known.
    pub fn remove_cells(&self, circuit_id: &CircuitId) -> Option<Vec<Cell>> {
        let mut map = self.circuit_map.write().expect("Lock poisoned");
        match map.get_mut(circuit_id) {
            Some(circuit) => Some(std::mem::replace(&mut circuit.cells, Vec::new())),
            None => None,
        }
    }

    pub async fn send_firebase_notifications(&self, epoch_no: EpochNo, round_no: RoundNo) {
        // TODO performance: don't clone the tokens; need a read-only view instead
        let tokens;
        {
            let map = self.firebase_map.read().expect("Lock poisoned");
            tokens = map.get(&epoch_no).cloned().unwrap_or_default()
        }
        let fcm_client = fcm::Client::new();
        for token in tokens {
            let mut builder = fcm::MessageBuilder::new(FCM_AUTH_KEY, &token);
            builder
                .priority(fcm::Priority::High)
                .time_to_live(60);

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
