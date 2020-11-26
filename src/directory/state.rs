use crate::crypto::key::Key;
use crate::crypto::x448;
use crate::epoch::{current_epoch_no, EpochNo, MAX_EPOCH_NO};
use crate::error::Error;
use crate::tonic_directory::{EpochInfo, MixInfo, MixStatistics};
use derive_builder::*;

use log::*;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::net::IpAddr;
use std::sync::{Arc, Mutex, RwLock};
use tokio::time::delay_for as sleep;
use tokio::time::Duration;

type StatisticMap = HashMap<String, HashMap<EpochNo, MixStatistics>>;

pub struct State {
    pub mix_map: Mutex<HashMap<String, Mix>>,
    config: Config,
    pub epochs: RwLock<VecDeque<EpochInfo>>,
    pub stat_map: RwLock<StatisticMap>,
}

impl State {
    pub fn new(config: Config) -> Self {
        let current_epoch_no = current_epoch_no(config.phase_duration());
        info!("Initializing directory in epoch {}", current_epoch_no);

        State {
            mix_map: Mutex::new(HashMap::new()),
            config,
            epochs: RwLock::new(VecDeque::new()),
            stat_map: RwLock::new(StatisticMap::new()),
        }
    }

    pub fn default() -> Self {
        State::new(Config::default())
    }

    pub fn config(self: &Self) -> &Config {
        &self.config
    }

    pub fn update(self: &Self) {
        let cfg = &self.config;
        let phase_duration = cfg.phase_duration();

        let current_epoch_no = current_epoch_no(phase_duration);
        if current_epoch_no == MAX_EPOCH_NO {
            panic!("End of time reached!");
        }
        info!("Updating epochs, current epoch is {}", current_epoch_no);
        let mut epoch_queue = self.epochs.write().expect("Acquiring lock failed");

        // clear old epoch information (includes the current epoch, because it's too late to send
        // setup packets now)
        while let Some(front) = epoch_queue.front() {
            if front.epoch_no <= current_epoch_no {
                epoch_queue.pop_front();
            } else {
                break;
            }
        }

        // add new epoch information (starting with the next uncommited epoch)
        let mut epoch_no = match epoch_queue.back() {
            Some(e) => e.epoch_no + 1,
            None => current_epoch_no + 1,
        };
        let mut setup_start_time = phase_duration.mul_f64(epoch_no as f64);
        let mut communication_start_time = setup_start_time + phase_duration;

        while epoch_queue.len() < cfg.epochs_in_advance.into() {
            let mut mixes = Vec::new();
            let mut mix_map = self.mix_map.lock().expect("Acquiring lock failed");
            for (_, mix) in mix_map.iter_mut() {
                // note: remove because we do not need it again
                match mix.dh_map.remove(&epoch_no) {
                    Some(pk) => {
                        let info = MixInfo {
                            address: crate::net::ip_addr_to_vec(&mix.addr),
                            entry_port: mix.entry_port as u32,
                            relay_port: mix.relay_port as u32,
                            rendezvous_port: mix.rendezvous_port as u32,
                            public_dh: pk.clone_to_vec(),
                            fingerprint: mix.fingerprint.clone(),
                        };
                        mixes.push(info);
                    }
                    None => warn!(
                        "Don't have a DH key for mix {} in epoch {} (have {} non-matching)",
                        &mix.fingerprint,
                        &epoch_no,
                        mix.dh_map.len(),
                    ),
                }
            }
            let epoch_info = EpochInfo {
                epoch_no,
                setup_start_time: setup_start_time.as_secs(),
                communication_start_time: communication_start_time.as_secs(),
                round_duration: cfg.round_duration.as_secs_f64(),
                round_waiting: cfg.round_waiting.as_secs_f64(),
                number_of_rounds: cfg.number_of_rounds,
                path_length: cfg.path_length.into(),
                mixes,
            };
            epoch_queue.push_back(epoch_info);
            epoch_no += 1;
            setup_start_time += phase_duration;
            communication_start_time += phase_duration;
        }
    }
}

#[derive(Builder)]
#[builder(default)]
pub struct Config {
    number_of_rounds: u32,
    epochs_in_advance: u8,
    path_length: u8,
    round_duration: Duration,
    round_waiting: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            number_of_rounds: 8,
            epochs_in_advance: 10,
            path_length: 3,
            round_duration: Duration::from_secs(7),
            round_waiting: Duration::from_secs(13),
        }
    }
}

impl Config {
    /// TODO code: getter macro?
    pub fn phase_duration(&self) -> Duration {
        self.number_of_rounds * (self.round_duration + self.round_waiting)
    }

    pub fn epochs_in_advance(&self) -> u8 {
        self.epochs_in_advance
    }

    pub fn path_length(&self) -> u8 {
        self.path_length
    }

    pub fn round_duration(&self) -> Duration {
        self.round_duration
    }

    pub fn round_waiting(&self) -> Duration {
        self.round_waiting
    }

    fn is_valid(&self) -> bool {
        self.phase_duration().subsec_nanos() == 0
            && self.number_of_rounds % (self.path_length as u32 + 1) == 0
    }
}

impl ConfigBuilder {
    pub fn build_valid(&self) -> Result<Config, Error> {
        let cfg = self
            .build()
            .expect("This should not happen, defaults provided");
        match cfg.is_valid() {
            true => Ok(cfg),
            false => Err(Error::InputError("Invalid config".to_string())),
        }
    }
}

pub struct Mix {
    pub fingerprint: String,
    pub auth_key: Key,
    pub addr: IpAddr,
    pub entry_port: u16,
    pub relay_port: u16,
    pub rendezvous_port: u16,
    pub dh_map: BTreeMap<EpochNo, Key>,
    pub last_counter: Option<u32>,
}

pub fn key_exchange(pk_mix: &Key) -> Result<(Key, Key), Error> {
    let (pk, sk) = x448::generate_keypair();
    let s = x448::generate_shared_secret(&pk_mix, &sk)?;
    Ok((pk, s))
}

pub async fn update_loop(state: Arc<State>) {
    loop {
        // wait till next update
        sleep(Duration::from_secs(30)).await;
        state.update();
    }
}
