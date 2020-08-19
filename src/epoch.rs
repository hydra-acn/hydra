use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::Duration;

pub use crate::tonic_directory::EpochInfo;

pub type EpochNo = u32;

pub const MAX_EPOCH_NO: EpochNo = std::u32::MAX;

/// return current POSIX/UNIX time in seconds
pub fn current_time_in_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Failed to get UNIX time")
        .as_secs()
}

/// return current POSIX/UNIX time
pub fn current_time() -> Duration {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Failed to get UNIX time")
}

/// return the current epoch number (only the directory service should rely on this function)
pub fn current_epoch_no(phase_duration: u64) -> EpochNo {
    (current_time_in_secs() / phase_duration) as EpochNo
}

impl EpochInfo {
    /// Return the end time for the communication phase of this epoch (keep keys till then).
    pub fn communication_end_time(&self) -> u64 {
        let k = self.number_of_rounds as u64;
        let d = self.round_duration as u64;
        let w = self.round_waiting as u64;
        self.communication_start_time + k * (d + w)
    }
}
