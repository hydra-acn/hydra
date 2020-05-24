use log::*;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, RwLock};
use tokio::time::{delay_for, Duration};
use tonic::Status;

use crate::crypto::key::Key;
use crate::crypto::x448;
use crate::epoch::{current_time_in_secs, EpochNo};
use crate::tonic_directory::directory_client::DirectoryClient;
use crate::tonic_directory::{DhMessage, DirectoryRequest, EpochInfo, RegisterRequest};

type Connection = DirectoryClient<tonic::transport::Channel>;

pub struct Config {
    addr: IpAddr,
    entry_port: u16,
    relay_port: u16,
    directory_addr: SocketAddr,
}

pub struct Client {
    fingerprint: String,
    /// long term secret key for communication with the directory server
    _sk: Key,
    /// long term public key for communication with the directory server
    pk: Key,
    config: Config,
    endpoint: String,
    epochs: RwLock<BTreeMap<EpochNo, EpochInfo>>,
    /// ephemeral keys
    keys: RwLock<BTreeMap<EpochNo, (Key, Key)>>,
    key_count: RefCell<u32>,
}

impl Client {
    /// param addr of the directory server
    pub fn new(config: Config) -> Self {
        // TODO sync to disk
        let (pk, sk) = x448::generate_keypair().expect("Generation of long-term key pair failed");
        let dir_addr = &config.directory_addr;
        let endpoint = format!("{}:{}", dir_addr.ip().to_string(), dir_addr.port());
        Client {
            fingerprint: format!("{}:{}", config.addr, config.entry_port),
            _sk: sk,
            pk,
            config,
            endpoint,
            epochs: RwLock::new(BTreeMap::new()),
            keys: RwLock::new(BTreeMap::new()),
            key_count: RefCell::new(0),
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
            public_dh: self.pk.clone_to_vec(),
        };
        match conn.register(request).await {
            Ok(_) => info!("Registration successful"),
            Err(status) => match status.code() {
                tonic::Code::InvalidArgument => info!("Seems like we are already registered"),
                _ => panic!("Register failed with unexpected reason: {}", status),
            },
        };
        let epochs_in_advance = self
            .fetch(&mut conn)
            .await
            .expect("Fetching directory for the first time failed");
        for _ in 0..2 * epochs_in_advance {
            self.create_ephemeral_dh(&mut conn).await;
        }
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
        if epochs_in_advance > sent_keys {
            for _ in 0..epochs_in_advance - sent_keys {
                self.create_ephemeral_dh(&mut conn).await;
            }
        }
    }

    /// fetch directory, merge it with our view and return the number of epochs in the reply
    pub async fn fetch(&self, conn: &mut Connection) -> Result<usize, Status> {
        let query = DirectoryRequest { min_epoch_no: 0 };
        let directory = conn.query_directory(query).await?.into_inner();
        let mut epoch_map = self.epochs.write().expect("Lock failure");
        if let Some(oldest_epoch) = directory.epochs.first() {
            // delete old epochs from our maps
            *epoch_map = epoch_map.split_off(&oldest_epoch.epoch_no);
            let mut key_map = self.keys.write().expect("Lock failure");
            *key_map = key_map.split_off(&oldest_epoch.epoch_no);
        } else {
            warn!("Directory response is empty");
        }
        let number_of_epochs = directory.epochs.len();
        for epoch in directory.epochs {
            epoch_map.insert(epoch.epoch_no, epoch);
        }
        Ok(number_of_epochs)
    }

    /// create ephemeral key pair and send the public key to the directory service
    pub async fn create_ephemeral_dh(&self, conn: &mut Connection) {
        let (pk, sk) = x448::generate_keypair().expect("keygen failed");
        let dh_msg = DhMessage {
            fingerprint: self.fingerprint.clone(),
            counter: *self.key_count.borrow(),
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
        *self.key_count.borrow_mut() += 1;
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