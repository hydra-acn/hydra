use crate::crypto::key::Key;
use crate::crypto::x448;
use crate::epoch::{current_epoch_no, EpochNo, MAX_EPOCH_NO};

use log::*;
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::time::{self, Duration};

pub struct State {
    pub mix_map: Mutex<HashMap<String, MixInfo>>,
    pub next_free_epoch_no: Mutex<EpochNo>,
}

impl State {
    pub fn new() -> Self {
        let current_epoch_no = current_epoch_no();
        info!("Initializing directory in epoch {}", current_epoch_no);

        State {
            mix_map: Mutex::new(HashMap::new()),
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

pub async fn update_loop(_state: Arc<State>) -> () {
    loop {
        let timer = tokio::spawn(time::delay_for(Duration::from_secs(5)));
        timer.await.expect("Waiting for timer failed");
        let current_epoch_no = current_epoch_no();
        info!("Updating epochs, current epoch is {}", current_epoch_no);
        if current_epoch_no == MAX_EPOCH_NO {
            warn!("End of time reached, stopping ...");
            break;
        }
    }
}
