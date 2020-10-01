use futures_util::stream;
use hmac::{Hmac, Mac, NewMac};
use log::*;
use rand::Rng;
use sha2::Sha256;
use std::cmp;
use std::collections::{BTreeMap, HashMap};
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, RwLock};
use tokio::time::{delay_for, Duration};
use tonic::transport::Channel;

use super::channel_pool::ChannelPool;
use crate::crypto::key::{hkdf_sha256, Key};
use crate::crypto::x448;
use crate::defs::{DIR_AUTH_KEY_INFO, DIR_AUTH_KEY_SIZE, DIR_AUTH_UNREGISTER};
use crate::epoch::{current_time_in_secs, EpochNo};
use crate::error::Error;
use crate::tonic_directory::directory_client::DirectoryClient;
use crate::tonic_directory::{
    DhMessage, EpochInfo, MixInfo, MixStatistics, RegisterRequest, UnregisterRequest,
};
use crate::tonic_mix::mix_client::MixClient;
use crate::tonic_mix::rendezvous_client::RendezvousClient;

type BaseClient = crate::client::directory_client::Client;

type DirectoryChannel = DirectoryClient<Channel>;
type MixChannel = MixClient<Channel>;
type RendezvousChannel = RendezvousClient<Channel>;

pub struct Config {
    pub addr: IpAddr,
    pub entry_port: u16,
    pub relay_port: u16,
    pub rendezvous_port: u16,
    pub directory_certificate: Option<String>,
    pub directory_domain: String,
    pub directory_port: u16,
}

impl Config {
    pub fn setup_reply_to(&self) -> String {
        format!("{}:{}", self.addr, self.relay_port)
    }
}

/// Mix talking to the directory service.
pub struct Client {
    base_client: BaseClient,
    fingerprint: String,
    /// long term secret key for communication with the directory server
    sk: Key,
    /// long term public key for communication with the directory server
    pk: Key,
    config: Config,
    /// shared key for authentication at the directory server
    auth_key: RwLock<Option<Key>>,
    /// ephemeral keys
    keys: RwLock<BTreeMap<EpochNo, (Key, Key)>>,
    key_count: AtomicU32,
    /// channel pool for mix connections
    mix_channels: ChannelPool<MixChannel>,
    /// channel pool for rendezvous connections
    rendezvous_channels: ChannelPool<RendezvousChannel>,
    statistic_msg_queue: RwLock<Vec<MixStatistics>>,
}

macro_rules! delegate {
    ($fnname:ident; $($arg:ident: $type:ty),* => $ret:ty) => {
        crate::delegate_generic!(
            base_client;
            concat!("[Delegated](../../client/directory_client/struct.Client.html#method.", stringify!($fnname), ")");
            $fnname;
            $($arg: $type),* => $ret
        );
    };
}

impl Client {
    pub fn new(config: Config) -> Self {
        // TODO sync to disk
        let (pk, sk) = x448::generate_keypair();
        let dir_domain = &config.directory_domain;
        let dir_port = &config.directory_port;
        Client {
            // TODO code: avoid cloning of half the config?
            base_client: BaseClient::new(
                dir_domain.clone(),
                *dir_port,
                config.directory_certificate.clone(),
            ),
            fingerprint: format!("{}:{}", config.addr, config.entry_port),
            sk: sk,
            pk,
            config,
            auth_key: RwLock::new(None),
            keys: RwLock::new(BTreeMap::new()),
            key_count: AtomicU32::new(0),
            mix_channels: ChannelPool::new(),
            rendezvous_channels: ChannelPool::new(),
            statistic_msg_queue: RwLock::new(vec![]),
        }
    }

    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    delegate!(get_epoch_info; epoch_no: EpochNo => Option<EpochInfo>);
    delegate!(next_epoch_info; => Option<EpochInfo>);
    delegate!(next_setup_start; => Option<u64>);
    delegate!(current_communication_epoch_no; => Option<EpochNo>);
    delegate!(select_path_tunable;
        epoch_no: EpochNo,
        number_of_hops: Option<usize>,
        exclude_fingerprint: Option<&str>
     => Result<Vec<MixInfo>, Error>);

    /// note: registration also includes the first fetch
    pub async fn register(&self) {
        let mut conn = self
            .base_client
            .connect()
            .await
            .expect("Connection for registration failed");
        let addr_vec = match self.config.addr {
            IpAddr::V4(v4) => v4.octets().to_vec(),
            IpAddr::V6(v6) => v6.octets().to_vec(),
        };
        let request = RegisterRequest {
            fingerprint: self.fingerprint.clone(),
            address: addr_vec,
            entry_port: self.config.entry_port as u32,
            relay_port: self.config.relay_port as u32,
            rendezvous_port: self.config.rendezvous_port as u32,
            public_dh: self.pk.clone_to_vec(),
        };
        match conn.register(request).await {
            Ok(r) => {
                info!("Registration successful");
                let pk = Key::move_from_vec(r.into_inner().public_dh);
                let shared_secret = x448::generate_shared_secret(&pk, &self.sk)
                    .expect("Key exchange with directory failed");
                let auth_key = hkdf_sha256(
                    &shared_secret,
                    None,
                    Some(DIR_AUTH_KEY_INFO),
                    DIR_AUTH_KEY_SIZE,
                )
                .expect("Key exchange with directory failed");
                *self.auth_key.write().expect("Lock failure") = Some(auth_key);
            }
            Err(status) => match status.code() {
                tonic::Code::InvalidArgument if status.message().contains("registered") => {
                    info!("Seems like we are already registered");
                    unimplemented!("We have to load the auth key from disk here!");
                }
                _ => panic!("Register failed with unexpected reason: {}", status),
            },
        };
        let epochs_in_advance = self
            .base_client
            .fetch(&mut conn)
            .await
            .expect("Fetching directory for the first time failed");
        let goal = cmp::max(12, epochs_in_advance + 2);
        for _ in 0..goal {
            self.create_ephemeral_dh(&mut conn).await;
        }
    }

    pub async fn unregister(&self) -> Result<(), Error> {
        let mut mac = self
            .init_mac()
            .expect("Creating mac for unregistration failed");
        mac.update(DIR_AUTH_UNREGISTER);

        let mut conn = self.base_client.connect().await?;

        let request = UnregisterRequest {
            fingerprint: self.fingerprint.clone(),
            auth_tag: mac.finalize().into_bytes().to_vec(),
        };
        conn.unregister(request).await?;
        Ok(())
    }

    /// update includes fetching the directory and sending more ephemeral keys if necessary
    pub async fn update(&self) {
        let mut conn = match self.base_client.connect().await {
            Ok(client) => client,
            Err(e) => {
                warn!(
                    "Connection to directory service failed, skipping update: {}",
                    e
                );
                return;
            }
        };

        // update directory
        let epochs_in_advance = match self.base_client.fetch(&mut conn).await {
            Ok(n) => n,
            Err(e) => {
                warn!("Fetching directory for update failed: {}", e);
                return;
            }
        };

        // clear old keys and get the number of keys for future use
        let future_keys = {
            let mut key_map = self.keys.write().expect("Lock failure");
            match self.base_client.smallest_epoch_no() {
                Some(epoch_no) => {
                    *key_map = key_map.split_off(&epoch_no);
                    ()
                }
                None => key_map.clear(),
            }
            // keys for current communication phase and current setup phase are not for future use
            // -> subtract 2
            cmp::max(key_map.len() as isize - 2, 0) as usize
        };

        // send more ephemeral keys if necessary
        let goal = cmp::max(12, epochs_in_advance + 2);
        if future_keys < goal {
            for _ in 0..(goal - future_keys) {
                self.create_ephemeral_dh(&mut conn).await;
            }
        }

        // prepare channels for the upcomming epoch
        if let Some(next_epoch) = self.base_client.next_epoch_info() {
            let mut mix_endpoints = Vec::new();
            let mut rendezvous_endpoints = Vec::new();
            for mix in next_epoch.mixes {
                if let Some(addr) = mix.relay_address() {
                    mix_endpoints.push(addr);
                }
                if let Some(addr) = mix.rendezvous_address() {
                    rendezvous_endpoints.push(addr);
                }
            }
            self.mix_channels.prepare_channels(&mix_endpoints).await;
            self.rendezvous_channels
                .prepare_channels(&rendezvous_endpoints)
                .await;
        }
        //send statistics
        let msg_queue = std::mem::replace(
            &mut *self.statistic_msg_queue.write().expect("Lock poisoned"),
            Vec::new(),
        );
        conn.send_statistics(tonic::Request::new(stream::iter(msg_queue)))
            .await
            .expect("Failed to send statistics");
    }

    /// create ephemeral key pair and send the public key to the directory service
    pub async fn create_ephemeral_dh(&self, conn: &mut DirectoryChannel) {
        let (pk, sk) = x448::generate_keypair();
        //generate mac
        let mut mac = self
            .init_mac()
            .expect("Creating mac for ephemeral_dh failed");
        let counter = self.key_count.fetch_add(1, Ordering::Relaxed);
        mac.update(&counter.to_le_bytes());
        mac.update(pk.borrow_raw());
        let dh_msg = DhMessage {
            fingerprint: self.fingerprint.clone(),
            counter,
            public_dh: pk.clone_to_vec(),
            auth_tag: mac.finalize().into_bytes().to_vec(),
        };
        let epoch_no = match conn.add_static_dh(dh_msg).await {
            Ok(ack) => ack.into_inner().epoch_no,
            Err(e) => {
                warn!("Sending a ephemeral key failed: {}", e);
                return;
            }
        };
        info!(
            "Registered pk for epoch {}: 0x{}",
            epoch_no,
            hex::encode(pk.borrow_raw())
        );
        let mut key_map = self.keys.write().expect("Lock failure");
        key_map.insert(epoch_no, (pk, sk));
    }

    /// check if we own an ephemeral DH key for the given epoch
    pub fn has_ephemeral_key(&self, epoch_no: &EpochNo) -> bool {
        self.keys
            .read()
            .expect("Poisoned lock")
            .contains_key(epoch_no)
    }

    /// return our private ephemeral DH key for the given epoch if we have one
    pub fn get_private_ephemeral_key(&self, epoch_no: &EpochNo) -> Option<Key> {
        self.keys
            .read()
            .expect("Poisoned lock")
            .get(epoch_no)
            .map(|(_pk, sk)| sk)
            .cloned()
    }

    pub async fn get_mix_channels(
        &self,
        destinations: &[SocketAddr],
    ) -> HashMap<SocketAddr, MixChannel> {
        self.mix_channels.get_channels(destinations).await
    }

    pub async fn get_rendezvous_channels(
        &self,
        destinations: &[SocketAddr],
    ) -> HashMap<SocketAddr, RendezvousChannel> {
        self.rendezvous_channels.get_channels(destinations).await
    }

    pub fn init_mac(&self) -> Result<hmac::Hmac<Sha256>, Error> {
        let mac = Hmac::<Sha256>::new_varkey(
            self.auth_key
                .read()
                .expect("Lock poisoned")
                .as_ref()
                .ok_or_else(|| Error::NoneError("Don't have the auth key".to_string()))?
                .borrow_raw(),
        )
        .expect("Initialising mac failed");
        Ok(mac)
    }

    pub fn queue_statistic_msg(&self, msg: MixStatistics) {
        let mut msg_queue = self.statistic_msg_queue.write().expect("Lock poisoned");
        msg_queue.push(msg);
    }
}

pub async fn run(client: Arc<Client>) {
    client.register().await;
    loop {
        let slack = rand::thread_rng().gen_range(10, 20);
        client.base_client.sleep_till_next_setup(slack, 30).await;
        info!("Updating directory");
        client.update().await;
        // another sleep to avoid multiple updates per epoch
        delay_for(Duration::from_secs(slack + 1)).await;
    }
}

pub mod mocks {
    use super::*;
    pub fn new(current_communication_epoch_no: EpochNo) -> Client {
        let addr: std::net::IpAddr = ("127.0.0.1").parse().expect("failed");
        let config = Config {
            addr,
            entry_port: 9001,
            relay_port: 9001,
            rendezvous_port: 9101,
            directory_domain: "127.0.0.1".to_string(),
            directory_port: 9000,
            directory_certificate: Some("".to_string()),
        };
        let mock_dir_client = Client::new(config);
        *mock_dir_client.auth_key.write().expect("Lock failure") = Some(Key::new(x448::POINT_SIZE));
        let current_time = current_time_in_secs();
        let mock_epoch = EpochInfo {
            epoch_no: current_communication_epoch_no,
            path_length: 0,
            setup_start_time: current_time - 10,
            communication_start_time: current_time,
            number_of_rounds: 20,
            round_duration: 1000,
            round_waiting: 1050,
            mixes: vec![],
        };
        mock_dir_client.base_client.insert_epoch(mock_epoch);
        mock_dir_client
    }
    pub fn get_msg_queue(mock_client: &Client) -> std::vec::Vec<MixStatistics> {
        let msg_queue: Vec<MixStatistics> =
            mock_client.statistic_msg_queue.read().unwrap().to_vec();
        msg_queue
    }
}
