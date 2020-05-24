use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::Duration;

use crate::tonic_directory::EpochInfo;

pub type EpochNo = u32;

pub const COMMUNICATION_DURATION: u16 = 600;
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
pub fn current_epoch_no() -> EpochNo {
    (current_time_in_secs() / COMMUNICATION_DURATION as u64) as EpochNo
}

impl EpochInfo {
    /// return the time of last receive
    pub fn communication_end_time(&self) -> u64 {
        let k = self.number_of_rounds as u64;
        let d = self.round_duration as u64;
        let w = self.round_waiting as u64;
        self.communication_start_time + k * (d + w) - w
    }

    /// if we are currently in the communication phase of this epoch, return the duration to next receive
    pub fn next_receive_in(&self) -> Option<Duration> {
        let current_time = current_time();
        let current_time_in_secs = current_time.as_secs();
        if current_time_in_secs >= self.communication_start_time
            && current_time_in_secs < self.communication_end_time()
        {
            let d = self.round_duration as u64;
            let w = self.round_waiting as u64;
            let interval_secs = d + w;
            let comm_running_secs = current_time_in_secs - self.communication_start_time;
            let mut round_no = comm_running_secs / interval_secs;
            let round_running_secs = comm_running_secs % interval_secs;
            if round_running_secs >= d {
                round_no += 1;
            }
            let next_receive = Duration::from_secs(round_no * interval_secs + d);
            match next_receive.checked_sub(current_time) {
                Some(dur) => Some(dur),
                None => panic!("Calculating time till next receive failed"),
            }
        } else {
            None
        }
    }
}
