use crate::crypto::key::Key;
use crate::crypto::x448;
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Mutex;

pub struct State {
    pub mix_map: Mutex<HashMap<String, MixInfo>>,
    pub current_epoch_no: Mutex<u32>,
    pub next_free_epoch_no: Mutex<u32>,
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
