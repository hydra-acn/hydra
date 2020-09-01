use std::sync::{Arc, RwLock};

use crate::rendezvous::subscription_map::SubscriptionMap;

/// All the state a mix collects about an epoch during the setup phase, filled by multiple threads.
#[derive(Default)]
pub struct EpochSetupState {
    sub_map: Arc<RwLock<SubscriptionMap>>,
}

impl EpochSetupState {
    pub fn subscription_map(&self) -> &Arc<RwLock<SubscriptionMap>> {
        &self.sub_map
    }
}

#[derive(Default)]
/// Thread safe "read only" view (interior mutability still possible) of the epoch state after setup.
pub struct EpochState {
    sub_map: Arc<SubscriptionMap>,
}

impl EpochState {
    /// Create a "read only" view by moving the given setup view (which will be empty afterwards)
    pub fn finalize_setup(epoch: &mut EpochSetupState) -> EpochState {
        let mut sub_guard = epoch.sub_map.write().expect("Lock poisoned");
        let sub_map = std::mem::replace(&mut *sub_guard, SubscriptionMap::new());

        EpochState {
            sub_map: Arc::new(sub_map),
        }
    }

    pub fn subscription_map(&self) -> &Arc<SubscriptionMap> {
        &self.sub_map
    }
}
