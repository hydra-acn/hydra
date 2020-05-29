use log::*;
use std::cmp;
use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, RwLock};
use tokio::time::{delay_for, Duration};
use tonic::Status;

use crate::crypto::key::Key;
use crate::crypto::x448;
use crate::epoch::{current_time_in_secs, EpochNo};
use crate::tonic_directory::directory_client::DirectoryClient;
use crate::tonic_directory::{
    DhMessage, DirectoryRequest, EpochInfo, RegisterRequest, UnregisterRequest,
};

type Connection = DirectoryClient<tonic::transport::Channel>;

pub struct Config {
    pub addr: IpAddr,
    pub entry_port: u16,
    pub relay_port: u16,
    pub rendezvous_port: u16,
    pub directory_addr: SocketAddr,
}

pub struct Client {
    fingerprint: String,
    /// long term secret key for communication with the directory server
    sk: Key,
    /// long term public key for communication with the directory server
    pk: Key,
    config: Config,
    endpoint: String,
    /// shared key with the directory server
    shared_key: RwLock<Option<Key>>,
    epochs: RwLock<BTreeMap<EpochNo, EpochInfo>>,
    /// ephemeral keys
    keys: RwLock<BTreeMap<EpochNo, (Key, Key)>>,
    key_count: AtomicU32,
}

impl Client {
    pub fn new(config: Config) -> Self {
        // TODO sync to disk
        let (pk, sk) = x448::generate_keypair().expect("Generation of long-term key pair failed");
        let dir_addr = &config.directory_addr;
        let endpoint = format!("http://{}:{}", dir_addr.ip().to_string(), dir_addr.port());
        Client {
            fingerprint: format!("{}:{}", config.addr, config.entry_port),
            sk: sk,
            pk,
            config,
            endpoint,
            shared_key: RwLock::new(None),
            epochs: RwLock::new(BTreeMap::new()),
            keys: RwLock::new(BTreeMap::new()),
            key_count: AtomicU32::new(0),
        }
    }

    /// return the start time of the next epoch setup (as UNIX time in seconds)
    pub fn next_setup_start(&self) -> Option<u64> {
        let current_time = current_time_in_secs();
        let epoch_map = self.epochs.read().expect("Lock failure");
        for (_, epoch) in epoch_map.iter() {
            if epoch.setup_start_time > current_time {
                return Some(epoch.setup_start_time);
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

    pub async fn register(&self) {
        let mut conn = DirectoryClient::connect(self.endpoint.clone())
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
                match x448::generate_shared_secret(&pk, &self.sk) {
                    Ok(s) => *self.shared_key.write().expect("Lock failure") = Some(s),
                    Err(e) => warn!(".. but key exchange with directory failed: {}", e),
                }
            }
            Err(status) => match status.code() {
                tonic::Code::InvalidArgument if status.message().contains("registered") => {
                    info!("Seems like we are already registered")
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

    pub async fn unregister(&self) {
        let mut conn = DirectoryClient::connect(self.endpoint.clone())
            .await
            .expect("Connection for unregistration failed");
        let request = UnregisterRequest {
            fingerprint: self.fingerprint.clone(),
            // TODO set auth tag appropriately
            auth_tag: Vec::new(),
        };
        conn.unregister(request)
            .await
            .expect("Unregistration failed");
    }

    /// update includes fetching the directory and sending more ephemeral keys if necessary
    pub async fn update(&self) {
        let mut conn = match DirectoryClient::connect(self.endpoint.clone()).await {
            Ok(c) => c,
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
    }

    /// fetch directory, merge it with our view and return the number of epochs in the reply
    pub async fn fetch(&self, conn: &mut Connection) -> Result<usize, Status> {
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
    pub async fn create_ephemeral_dh(&self, conn: &mut Connection) {
        let (pk, sk) = x448::generate_keypair().expect("keygen failed");
        let dh_msg = DhMessage {
            fingerprint: self.fingerprint.clone(),
            counter: self.key_count.load(Ordering::Relaxed),
            public_dh: pk.clone_to_vec(),
            auth_tag: Vec::new(), // TODO gen auth tag
        };
        let epoch_no = match conn.add_static_dh(dh_msg).await {
            Ok(ack) => ack.into_inner().epoch_no,
            Err(e) => {
                warn!("Sending a ephemeral key failed: {}", e);
                return;
            }
        };
        let mut key_map = self.keys.write().expect("Lock failure");
        key_map.insert(epoch_no, (pk, sk));
        self.key_count.fetch_add(1, Ordering::Relaxed);
    }
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
