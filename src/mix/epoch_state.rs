use std::sync::{Arc, RwLock};

use crate::rendezvous::subscription_map::SubscriptionMap;
use crate::tonic_directory::EpochInfo;

use super::rendezvous_map::RendezvousMap;

/// All the state a mix collects about an epoch during the setup phase, filled by multiple threads.
#[derive(Default)]
pub struct EpochSetupState {
    rendezvous_map: Arc<RendezvousMap>,
    sub_map: Arc<RwLock<SubscriptionMap>>,
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
}

#[derive(Default)]
/// Thread safe "read only" view (interior mutability still possible) of the epoch state after setup.
pub struct EpochState {
    rendezvous_map: Arc<RendezvousMap>,
    sub_map: Arc<SubscriptionMap>,
}

impl EpochState {
    /// Create a "read only" view by moving the given setup view (which will be empty afterwards)
    pub fn finalize_setup(state: &mut EpochSetupState) -> EpochState {
        let mut sub_guard = state.sub_map.write().expect("Lock poisoned");
        let sub_map = std::mem::replace(&mut *sub_guard, SubscriptionMap::new());

        EpochState {
            rendezvous_map: std::mem::replace(&mut state.rendezvous_map, Arc::default()),
            sub_map: Arc::new(sub_map),
        }
    }

    pub fn rendezvous_map(&self) -> &Arc<RendezvousMap> {
        &self.rendezvous_map
    }

    pub fn subscription_map(&self) -> &Arc<SubscriptionMap> {
        &self.sub_map
    }
}
