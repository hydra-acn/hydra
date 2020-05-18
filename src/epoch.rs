use std::time::{SystemTime, UNIX_EPOCH};

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

/// return the current epoch numbe
pub fn current_epoch_no() -> EpochNo {
    (current_time_in_secs() / COMMUNICATION_DURATION as u64) as EpochNo
}
