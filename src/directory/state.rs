use crate::crypto::key::Key;
use crate::crypto::x448;
use log::*;
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

pub const EPOCH_DURATION: u64 = 600;

pub struct State {
    pub mix_map: Mutex<HashMap<String, MixInfo>>,
    pub current_epoch_no: Mutex<u32>,
    pub next_free_epoch_no: Mutex<u32>,
}

impl State {
    pub fn new() -> Self {
        let current_epoch_no = (current_time_in_secs() / EPOCH_DURATION) as u32;
        info!("Initializing directory in epoch {}", current_epoch_no);

        State {
            mix_map: Mutex::new(HashMap::new()),
            current_epoch_no: Mutex::new(current_epoch_no),
            next_free_epoch_no: Mutex::new(current_epoch_no + 1),
        }
    }
}

pub struct MixInfo {
    pub fingerprint: String,
    pub shared_key: Key,
    pub socket_addr: SocketAddr,
    pub dh_queue: VecDeque<Key>,
}

pub fn key_exchange(pk_mix: &Key) -> Result<(Key, Key), tonic::Status> {
    let (pk, sk) = x448::generate_keypair()?;
    let s = x448::generate_shared_secret(&pk_mix, &sk)?;
    Ok((pk, s))
}

pub fn current_time_in_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Failed to get UNIX time")
        .as_secs()
}
