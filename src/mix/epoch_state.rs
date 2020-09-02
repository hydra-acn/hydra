use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use crate::defs::CircuitId;
use crate::rendezvous::subscription_map::SubscriptionMap;
use crate::tonic_directory::EpochInfo;

use super::circuit::Circuit;
use super::dummy_circuit::DummyCircuit;
use super::rendezvous_map::RendezvousMap;

pub type CircuitMap = BTreeMap<CircuitId, Circuit>;
pub type DummyCircuitMap = BTreeMap<CircuitId, DummyCircuit>;
pub type CircuitIdMap = BTreeMap<CircuitId, CircuitId>;

/// All the state a mix collects about an epoch during the setup phase, filled by multiple threads.
#[derive(Default)]
pub struct EpochSetupState {
    /// Map tokens to corresponding rendezvous nodes.
    rendezvous_map: Arc<RendezvousMap>,
    /// When acting as rendezvous node: map tokens to subscribers.
    sub_map: Arc<RwLock<SubscriptionMap>>,
    /// Map downstream ids to circuits.
    circuits: Arc<RwLock<CircuitMap>>,
    /// Map *upstream* ids to dummy circuits (they have no downstream id).
    dummy_circuits: DummyCircuitMap,
    /// Map the upstream circuit id to the downstream id of an circuit.
    circuit_id_map: Arc<RwLock<CircuitIdMap>>,
}

impl EpochSetupState {
    pub fn init_rendezvous_map(&mut self, epoch: &EpochInfo) {
        self.rendezvous_map = Arc::new(RendezvousMap::new(epoch));
    }

    pub fn rendezvous_map(&self) -> &Arc<RendezvousMap> {
        &self.rendezvous_map
    }

    pub fn subscription_map(&self) -> &Arc<RwLock<SubscriptionMap>> {
        &self.sub_map
    }

    pub fn circuits(&self) -> &Arc<RwLock<CircuitMap>> {
        &self.circuits
    }

    pub fn dummy_circuits(&mut self) -> &mut DummyCircuitMap {
        &mut self.dummy_circuits
    }

    pub fn circuit_id_map(&self) -> &Arc<RwLock<CircuitIdMap>> {
        &self.circuit_id_map
    }
}

#[derive(Default)]
/// Thread safe "read only" view (interior mutability still possible) of the epoch state after setup.
pub struct EpochState {
    rendezvous_map: Arc<RendezvousMap>,
    sub_map: Arc<SubscriptionMap>,
    circuits: Arc<CircuitMap>,
    dummy_circuits: DummyCircuitMap,
    circuit_id_map: Arc<CircuitIdMap>,
}

impl EpochState {
    /// Create a "read only" view by moving the given setup view (which will be empty afterwards)
    pub fn finalize_setup(state: &mut EpochSetupState) -> EpochState {
        let mut sub_guard = state.sub_map.write().expect("Lock poisoned");
        let sub_map = std::mem::replace(&mut *sub_guard, SubscriptionMap::default());

        let mut circuits_guard = state.circuits.write().expect("Lock poisoned");
        let circuits = std::mem::replace(&mut *circuits_guard, CircuitMap::default());

        let mut circuit_id_guard = state.circuit_id_map.write().expect("Lock poisoned");
        let circuit_id_map = std::mem::replace(&mut *circuit_id_guard, CircuitIdMap::default());

        EpochState {
            rendezvous_map: std::mem::replace(&mut state.rendezvous_map, Arc::default()),
            sub_map: Arc::new(sub_map),
            circuits: Arc::new(circuits),
            dummy_circuits: std::mem::replace(
                &mut state.dummy_circuits,
                DummyCircuitMap::default(),
            ),
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

    pub fn dummy_circuits(&self) -> &DummyCircuitMap {
        &self.dummy_circuits
    }

    pub fn circuit_id_map(&self) -> &Arc<CircuitIdMap> {
        &self.circuit_id_map
    }
}
