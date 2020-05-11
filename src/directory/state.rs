use crate::crypto::key::Key;
use crate::crypto::x448;
use crate::epoch::{current_epoch_no, EpochNo, COMMUNICATION_DURATION, MAX_EPOCH_NO};

use crate::tonic_directory::{EpochInfo, MixInfo};
use log::*;
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, RwLock};
use tokio::time::{self, Duration};

pub struct State {
    pub mix_map: Mutex<HashMap<String, Mix>>,
    pub next_free_epoch_no: Mutex<EpochNo>,
    config: Config,
    pub epochs: RwLock<VecDeque<EpochInfo>>,
}

impl State {
    pub fn new(config: Config) -> Self {
        let current_epoch_no = current_epoch_no();
        info!("Initializing directory in epoch {}", current_epoch_no);

        State {
            mix_map: Mutex::new(HashMap::new()),
            next_free_epoch_no: Mutex::new(current_epoch_no + 1),
            config: config,
            epochs: RwLock::new(VecDeque::new()),
        }
    }

    pub fn default() -> Self {
        State::new(Config::default())
    }

    pub fn update(self: &Self) {
        let current_epoch_no = current_epoch_no();
        if current_epoch_no == MAX_EPOCH_NO {
            panic!("End of time reached!");
        }
        info!("Updating epochs, current epoch is {}", current_epoch_no);
        let mut epoch_queue = self.epochs.write().expect("Acquiring lock failed");

        // clear old epoch information
        while let Some(front) = epoch_queue.front() {
            if front.epoch_no <= current_epoch_no {
                epoch_queue.pop_front();
            } else {
                break;
            }
        }

        // add new epoch information
        let mut epoch_no = current_epoch_no;
        let mut setup_start_time = epoch_no as u64 * COMMUNICATION_DURATION as u64;
        let mut communication_start_time = setup_start_time + COMMUNICATION_DURATION as u64;
        let cfg = &self.config;
        let number_of_rounds =
            (COMMUNICATION_DURATION as u32) / (cfg.round_duration + cfg.round_waiting) as u32;

        while epoch_queue.len() < cfg.epochs_in_advance.into() {
            let mut mixes = Vec::new();
            let mut mix_map = self.mix_map.lock().expect("Acquiring lock failed");
            for (_, mix) in mix_map.iter_mut() {
                if let Some(pk) = mix.dh_queue.pop_front() {
                    let info = MixInfo {
                        address: mix.socket_addr.ip().to_string(),
                        port: mix.socket_addr.port() as u32,
                        public_dh: pk.clone_to_vec(),
                    };
                    mixes.push(info);
                } // else: the mix has no DH keys left
            }
            let epoch_info = EpochInfo {
                epoch_no,
                setup_start_time,
                communication_start_time,
                round_duration: cfg.round_duration.into(),
                round_waiting: cfg.round_waiting.into(),
                number_of_rounds,
                path_length: cfg.path_length.into(),
                mixes,
            };
            epoch_no += 1;
            setup_start_time += COMMUNICATION_DURATION as u64;
            communication_start_time += COMMUNICATION_DURATION as u64;
            epoch_queue.push_back(epoch_info);
        }
    }
}

pub struct Config {
    pub epochs_in_advance: u8,
    pub path_length: u8,
    pub round_duration: u8,
    pub round_waiting: u8,
}

impl Config {
    pub fn default() -> Self {
        Config {
            epochs_in_advance: 10,
            path_length: 3,
            round_duration: 7,
            round_waiting: 13,
        }
    }
}

pub struct Mix {
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

pub async fn update_loop(state: Arc<State>) {
    loop {
        state.update();

        // wait till next update
        let timer = tokio::spawn(time::delay_for(Duration::from_secs(5)));
        timer.await.expect("Waiting for timer failed");
    }
}
