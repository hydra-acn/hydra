use hmac::{Hmac, Mac, NewMac};
use log::*;
use rand::seq::SliceRandom;
use sha2::Sha256;
use std::cmp;
use std::collections::{BTreeMap, HashMap};
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, RwLock};
use tokio::time::{delay_for, Duration};
use tonic::transport::{Certificate, Channel, ClientTlsConfig};
use tonic::Status;

use super::channel_pool::ChannelPool;
use crate::crypto::key::{hkdf_sha256, Key};
use crate::crypto::x448;
use crate::defs::{Token, DIR_AUTH_KEY_INFO, DIR_AUTH_KEY_SIZE, DIR_AUTH_UNREGISTER};
use crate::epoch::{current_time_in_secs, EpochNo};
use crate::error::Error;
use crate::net::ip_addr_from_slice;
use crate::tonic_directory::directory_client::DirectoryClient;
use crate::tonic_directory::{
    DhMessage, DirectoryRequest, EpochInfo, MixInfo, RegisterRequest, UnregisterRequest,
};
use crate::tonic_mix::mix_client::MixClient;
use crate::tonic_mix::rendezvous_client::RendezvousClient;
use crate::{assert_as_external_err, assert_as_input_err};

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

/// Map tokens to rendezvous nodes.
pub struct RendezvousMap {
    map: Vec<SocketAddr>,
}

impl RendezvousMap {
    pub fn new(epoch: &EpochInfo) -> Self {
        let mut rendezvous_nodes = Vec::new();
        for mix in epoch.mixes.iter() {
            match mix.rendezvous_address() {
                Some(a) => rendezvous_nodes.push((a, &mix.public_dh)),
                None => warn!("Found mix with no rendezvous address"),
            }
        }
        rendezvous_nodes.sort_by(|a, b| a.1.cmp(b.1));
        let map = rendezvous_nodes.into_iter().map(|(a, _)| a).collect();
        RendezvousMap { map }
    }

    pub fn rendezvous_address(&self, token: &Token) -> Option<SocketAddr> {
        match self.map.len() {
            0 => None,
            n => self.map.get((token % n as u64) as usize).cloned(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.map.len() == 0
    }
}

pub struct Client {
    fingerprint: String,
    /// long term secret key for communication with the directory server
    sk: Key,
    /// long term public key for communication with the directory server
    pk: Key,
    config: Config,
    endpoint: String,
    /// shared key for authentication at the directory server
    auth_key: RwLock<Option<Key>>,
    epochs: RwLock<BTreeMap<EpochNo, EpochInfo>>,
    /// ephemeral keys
    keys: RwLock<BTreeMap<EpochNo, (Key, Key)>>,
    key_count: AtomicU32,
    /// channel pool for mix connections
    mix_channels: ChannelPool<MixChannel>,
    /// channel pool for rendezvous connections
    rendezvous_channels: ChannelPool<RendezvousChannel>,
}

impl Client {
    pub fn new(config: Config) -> Self {
        // TODO sync to disk
        let (pk, sk) = x448::generate_keypair().expect("Generation of long-term key pair failed");
        let dir_domain = &config.directory_domain;
        let dir_port = &config.directory_port;
        let endpoint = format!("https://{}:{}", dir_domain, dir_port);
        Client {
            fingerprint: format!("{}:{}", config.addr, config.entry_port),
            sk: sk,
            pk,
            config,
            endpoint,
            auth_key: RwLock::new(None),
            epochs: RwLock::new(BTreeMap::new()),
            keys: RwLock::new(BTreeMap::new()),
            key_count: AtomicU32::new(0),
            mix_channels: ChannelPool::new(),
            rendezvous_channels: ChannelPool::new(),
        }
    }

    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    // TODO performance: should store infos inside Arcs to avoid copy (key material is big!)
    pub fn get_epoch_info(&self, epoch_no: EpochNo) -> Option<EpochInfo> {
        let map = self.epochs.read().expect("Lock failure");
        map.get(&epoch_no).cloned()
    }

    /// return info about the epoch that starts next (based on setup start time)
    pub fn next_epoch_info(&self) -> Option<EpochInfo> {
        let current_time = current_time_in_secs();
        let epoch_map = self.epochs.read().expect("Lock failure");
        for (_, epoch) in epoch_map.iter() {
            if epoch.setup_start_time > current_time {
                return Some(epoch.clone());
            }
        }
        None
    }

    /// return the start time of the next epoch setup (as UNIX time in seconds)
    pub fn next_setup_start(&self) -> Option<u64> {
        self.next_epoch_info().map(|epoch| epoch.setup_start_time)
    }

    /// return the epoch number for the epoch that is currently in the communication phase
    pub fn current_communication_epoch_no(&self) -> Option<EpochNo> {
        self.current_communication_epoch_info()
            .map(|epoch| epoch.epoch_no)
    }

    /// return the epoch info for the epoch that is currently in the communication phase
    pub fn current_communication_epoch_info(&self) -> Option<EpochInfo> {
        let current_time = current_time_in_secs();
        let epoch_map = self.epochs.read().expect("Lock failure");
        for (_, epoch) in epoch_map.iter() {
            if current_time >= epoch.communication_start_time
                && current_time <= epoch.communication_end_time()
            {
                return Some(epoch.clone());
            }
        }
        None
    }

    /// if we are currently in a communication phase, return the duration till next receive
    pub fn next_receive_in(&self) -> Option<Duration> {
        let epoch_map = self.epochs.read().expect("Lock failure");
        for (_, epoch) in epoch_map.iter() {
            if let Some(d) = epoch.next_receive_in() {
                return Some(d);
            }
        }
        None
    }

    /// note: registration also includes the first fetch
    pub async fn register(&self) {
        let mut conn = connect(self.endpoint.clone(), &self.config)
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
            .fetch(&mut conn)
            .await
            .expect("Fetching directory for the first time failed");
        let goal = cmp::max(12, epochs_in_advance + 2);
        for _ in 0..goal {
            self.create_ephemeral_dh(&mut conn).await;
        }
    }

    pub async fn unregister(&self) -> Result<(), Error> {
        let mut mac = Hmac::<Sha256>::new_varkey(
            self.auth_key
                .read()
                .expect("Lock poisoned")
                .as_ref()
                .ok_or_else(|| {
                    Error::NoneError("Don't have the auth key to unregister".to_string())
                })?
                .borrow_raw(),
        )
        .expect("Initialising mac failed");
        mac.update(DIR_AUTH_UNREGISTER);

        let mut conn = connect(self.endpoint.clone(), &self.config).await?;

        let request = UnregisterRequest {
            fingerprint: self.fingerprint.clone(),
            auth_tag: mac.finalize().into_bytes().to_vec(),
        };
        conn.unregister(request).await?;
        Ok(())
    }

    /// update includes fetching the directory and sending more ephemeral keys if necessary
    pub async fn update(&self) {
        let mut conn = match connect(self.endpoint.clone(), &self.config).await {
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
        let epochs_in_advance = match self.fetch(&mut conn).await {
            Ok(n) => n,
            Err(e) => {
                warn!("Fetching directory for update failed: {}", e);
                return;
            }
        };
        // send more ephemeral keys if necessary
        let sent_keys = self.keys.read().expect("Lock failure").len();
        let goal = cmp::max(12, epochs_in_advance + 2);
        if sent_keys < goal {
            for _ in 0..(goal - sent_keys) {
                self.create_ephemeral_dh(&mut conn).await;
            }
        }
        // prepare channels for the upcomming epoch
        if let Some(next_epoch) = self.next_epoch_info() {
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
    }

    /// fetch directory, merge it with our view and return the number of epochs in the reply
    pub async fn fetch(&self, conn: &mut DirectoryChannel) -> Result<usize, Status> {
        let query = DirectoryRequest { min_epoch_no: 0 };
        let directory = conn.query_directory(query).await?.into_inner();
        let mut epoch_map = self.epochs.write().expect("Lock failure");
        if let Some(next_epoch) = directory.epochs.first() {
            // delete old epochs from our maps (but keep current, which is not in the directory)
            let current_epoch_no = &(next_epoch.epoch_no - 1);
            *epoch_map = epoch_map.split_off(current_epoch_no);
            let mut key_map = self.keys.write().expect("Lock failure");
            *key_map = key_map.split_off(current_epoch_no);
        } else {
            warn!("Directory response is empty");
        }
        let number_of_epochs = directory.epochs.len();
        debug!("Fetched directory with {} epochs", number_of_epochs);
        for epoch in directory.epochs {
            debug!(
                ".. epoch {} has {} mixes",
                epoch.epoch_no,
                epoch.mixes.len()
            );
            epoch_map.insert(epoch.epoch_no, epoch);
        }
        Ok(number_of_epochs)
    }

    /// create ephemeral key pair and send the public key to the directory service
    pub async fn create_ephemeral_dh(&self, conn: &mut DirectoryChannel) {
        let (pk, sk) = x448::generate_keypair().expect("keygen failed");
        //generate mac
        let mut mac = Hmac::<Sha256>::new_varkey(
            self.auth_key
                .read()
                .expect("Lock poisoned")
                .as_ref()
                .expect("No auth_key")
                .borrow_raw(),
        )
        .expect("Initialising mac failed");
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

    /// Default client side path selection according to the Hydra protocol for epoch `epoch_no`.
    pub fn select_path(&self, epoch_no: EpochNo) -> Result<Vec<MixInfo>, Error> {
        self.select_path_tunable(epoch_no, None, None)
    }

    /// Tuneable path selection for epoch `epoch_no`.
    /// Use `number_of_hops` if you do not want to use the path length dictated by the directory.
    /// Use `exclude_fingerprint` if you want to exclude a mix from path selection.
    pub fn select_path_tunable(
        &self,
        epoch_no: EpochNo,
        number_of_hops: Option<usize>,
        exclude_fingerprint: Option<&str>,
    ) -> Result<Vec<MixInfo>, Error> {
        let epoch_map = self.epochs.read().expect("Lock poisoned");
        let epoch = epoch_map
            .get(&epoch_no)
            .ok_or_else(|| Error::NoneError(format!("Epoch {} not known", epoch_no)))?;
        let canditates: Vec<&MixInfo> = epoch
            .mixes
            .iter()
            .filter(|m| {
                exclude_fingerprint.is_none() || m.fingerprint != exclude_fingerprint.unwrap()
            })
            .collect();
        let official_len = epoch.path_length as usize;
        let (len, allow_dup) = match number_of_hops {
            Some(l) => (l, l >= official_len),
            None => (official_len, true),
        };

        assert_as_input_err!(len > 0, "Path length of 0 makes no sense");
        assert_as_external_err!(
            canditates.len() >= len,
            "Don't know enough mixes to select path of length {}",
            len
        );

        // TODO security: use secure random source
        let rng = &mut rand::thread_rng();
        let mut path: Vec<MixInfo> = canditates
            .choose_multiple(rng, len)
            .map(|&mix| mix.clone())
            .collect();

        if allow_dup {
            let new_entry_ref = canditates.choose(rng).expect("Checked above");
            path[0] = (*new_entry_ref).clone();
        }
        Ok(path)
    }
}

pub async fn connect(
    endpoint: String,
    config: &Config,
) -> Result<DirectoryChannel, tonic::transport::Error> {
    let mut tls_config = ClientTlsConfig::new().domain_name(config.directory_domain.clone());
    if let Some(cert) = &config.directory_certificate {
        tls_config = tls_config.ca_certificate(Certificate::from_pem(&cert));
    }
    let channel = Channel::from_shared(endpoint)
        .unwrap()
        .tls_config(tls_config)?
        .connect()
        .await?;

    Ok(DirectoryClient::new(channel))
}

pub async fn run(client: Arc<Client>) {
    client.register().await;
    let slack = 10;
    loop {
        // sleep till the next epoch starts
        let current_time = current_time_in_secs();
        let wait_for = match client.next_setup_start() {
            Some(t) if t > current_time + slack => t - current_time - slack,
            Some(t) if t > current_time => 0,
            Some(t) => {
                warn!(
                    "Next epoch starts in the past!? ({} seconds ago)",
                    current_time - t
                );
                30
            }
            None => {
                warn!("Don't know when the next epoch starts");
                30
            }
        };
        delay_for(Duration::from_secs(wait_for)).await;
        info!("Updating directory");
        client.update().await;
        // another sleep to avoid multiple updates per epoch
        delay_for(Duration::from_secs(slack + 1)).await;
    }
}

impl MixInfo {
    pub fn relay_address(&self) -> Option<SocketAddr> {
        ip_addr_from_slice(&self.address)
            .ok()
            .map(|ip| SocketAddr::new(ip, self.relay_port as u16))
    }

    pub fn rendezvous_address(&self) -> Option<SocketAddr> {
        ip_addr_from_slice(&self.address)
            .ok()
            .map(|ip| SocketAddr::new(ip, self.rendezvous_port as u16))
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

        let current_time = current_time_in_secs();
        let epochs_to_insert_1 = EpochInfo {
            epoch_no: current_communication_epoch_no,
            path_length: 0,
            setup_start_time: current_time - 10,
            communication_start_time: current_time,
            number_of_rounds: 20,
            round_duration: 1000,
            round_waiting: 1050,
            mixes: vec![],
        };
        {
            let mut epoch_map = mock_dir_client.epochs.write().expect("Lock failure");
            epoch_map.insert(epochs_to_insert_1.epoch_no, epochs_to_insert_1);
        }
        mock_dir_client
    }
}
