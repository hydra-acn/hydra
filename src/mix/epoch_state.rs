use bloomfilter::Bloom;
use std::cmp::max;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::defs::{AuthTag, CircuitId};
use crate::rendezvous::subscription_map::SubscriptionMap;
use crate::tonic_directory::EpochInfo;

use super::circuit::Circuit;
use super::dummy_circuit::DummyCircuit;
use super::rendezvous_map::RendezvousMap;
use super::sub_collector::SubCollector;

pub type CircuitMap = HashMap<CircuitId, Circuit>;
pub type CircuitIdMap = HashMap<CircuitId, CircuitId>;
pub type DummyCircuitMap = BTreeMap<CircuitId, DummyCircuit>;
pub type DupFilter = Bloom<AuthTag>;

/// All the state a mix collects about an epoch during the setup phase, filled by multiple threads.
pub struct EpochSetupState {
    /// Map tokens to corresponding rendezvous nodes.
    rendezvous_map: Arc<RendezvousMap>,
    /// When acting as rendezvous node: map tokens to subscribers.
    sub_map: Arc<SubscriptionMap>,
    /// Map downstream ids to circuits.
    circuits: Arc<RwLock<CircuitMap>>,
    /// Map *upstream* ids to dummy circuits (they have no downstream id).
    dummy_circuits: Arc<RwLock<DummyCircuitMap>>,
    /// Map the upstream circuit id to the downstream id of an circuit.
    circuit_id_map: Arc<RwLock<CircuitIdMap>>,
    /// Setup packet replay protection.
    bloom: Arc<RwLock<DupFilter>>,
    /// Collecting our subscription that we send at the end of setup phase.
    sub_collector: Arc<SubCollector>,
}

impl EpochSetupState {
    /// `last_circuit_count` is the number of circuits a mix handled the last epoch.
    /// Needed to estimate the size of the bloom bitmap for the current epoch.
    pub fn new(last_circuit_count: usize) -> EpochSetupState {
        EpochSetupState {
            rendezvous_map: Arc::default(),
            sub_map: Arc::default(),
            circuits: Arc::default(),
            dummy_circuits: Arc::default(),
            circuit_id_map: Arc::default(),
            bloom: Arc::new(RwLock::new(create_bloomfilter(last_circuit_count))),
            sub_collector: Arc::default(),
        }
    }

    pub fn init_rendezvous_map(&mut self, epoch: &EpochInfo) {
        // TODO robustness: don't crash with no mixes (hmmm, first find the code that could crash
        // here)
        self.rendezvous_map = Arc::new(RendezvousMap::new(epoch).expect("No mixes available"));
        let sub_collector = SubCollector::new(self.rendezvous_map.clone());
        self.sub_collector = Arc::new(sub_collector);
    }

    pub fn rendezvous_map(&self) -> &Arc<RendezvousMap> {
        &self.rendezvous_map
    }

    pub fn subscription_collector(&self) -> &Arc<SubCollector> {
        &self.sub_collector
    }

    pub fn subscription_map(&self) -> &Arc<SubscriptionMap> {
        &self.sub_map
    }

    pub fn circuits(&self) -> &Arc<RwLock<CircuitMap>> {
        &self.circuits
    }

    pub fn dummy_circuits(&self) -> &Arc<RwLock<DummyCircuitMap>> {
        &self.dummy_circuits
    }

    pub fn circuit_id_map(&self) -> &Arc<RwLock<CircuitIdMap>> {
        &self.circuit_id_map
    }

    pub fn bloom_bitmap(&self) -> &Arc<RwLock<DupFilter>> {
        &self.bloom
    }
}

#[derive(Default)]
/// Thread safe "read only" view (interior mutability still possible) of the epoch state after setup.
pub struct EpochState {
    rendezvous_map: Arc<RendezvousMap>,
    sub_map: Arc<SubscriptionMap>,
    circuits: Arc<CircuitMap>,
    dummy_circuits: Arc<DummyCircuitMap>,
    circuit_id_map: Arc<CircuitIdMap>,
}

impl EpochState {
    /// Create a "read only" view by moving the given setup view.
    /// `state` will be prepared to handle the next setup by resetting
    /// all fields and resizing the bloomfilter
    pub fn finalize_setup(state: &mut EpochSetupState) -> EpochState {
        let mut circuits_guard = state.circuits.write().expect("Lock poisoned");
        let circuits = std::mem::replace(&mut *circuits_guard, CircuitMap::default());

        let mut circuit_id_guard = state.circuit_id_map.write().expect("Lock poisoned");
        let circuit_id_map = std::mem::replace(&mut *circuit_id_guard, CircuitIdMap::default());

        let mut dummy_circuits_guard = state.dummy_circuits.write().expect("Lock poisoned");
        let dummy_circuits =
            std::mem::replace(&mut *dummy_circuits_guard, DummyCircuitMap::default());

        let mut bloom_guard = state.bloom.write().expect("Lock poisoned");
        *bloom_guard = create_bloomfilter(circuits.len());

        // TODO move the collector to the communication state as well
        state.sub_collector = Arc::default();

        EpochState {
            rendezvous_map: std::mem::replace(&mut state.rendezvous_map, Arc::default()),
            sub_map: std::mem::replace(&mut state.sub_map, Arc::default()),
            circuits: Arc::new(circuits),
            dummy_circuits: Arc::new(dummy_circuits),
            circuit_id_map: Arc::new(circuit_id_map),
        }
    }

    pub fn rendezvous_map(&self) -> &Arc<RendezvousMap> {
        &self.rendezvous_map
    }

    pub fn subscription_map(&self) -> &Arc<SubscriptionMap> {
        &self.sub_map
    }

    pub fn circuits(&self) -> &Arc<CircuitMap> {
        &self.circuits
    }

    pub fn dummy_circuits(&self) -> &Arc<DummyCircuitMap> {
        &self.dummy_circuits
    }

    pub fn circuit_id_map(&self) -> &Arc<CircuitIdMap> {
        &self.circuit_id_map
    }

    /// Drops the circuit information (if not dropped already). If time permits, starts to drop the
    /// subscription map as well.
    pub fn drop_some(&mut self, deadline: Duration) {
        if self.circuits.len() > 0 {
            self.circuits = Arc::default();
        }
        self.sub_map.drop_some(deadline);
    }
}

fn create_bloomfilter(last_circuit_count: usize) -> DupFilter {
    Bloom::new_for_fp_rate(
        max(100_000, (last_circuit_count as f64 * 1.1) as usize),
        1e-6,
    )
}
