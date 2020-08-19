//! Hydra client talking to the directory service
use log::*;
use rand::seq::SliceRandom;
use rand::Rng;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use tokio::time::{delay_for, Duration};
use tonic::transport::{Certificate, Channel, ClientTlsConfig};
use tonic::Status;

use crate::epoch::{current_time_in_secs, EpochNo};
use crate::error::Error;
use crate::net::ip_addr_from_slice;
use crate::tonic_directory::directory_client::DirectoryClient;
use crate::tonic_directory::{DirectoryRequest, EpochInfo, MixInfo};
use crate::{assert_as_external_err, assert_as_input_err};

type Cert = String;
type DirectoryChannel = DirectoryClient<Channel>;

pub struct Client {
    domain: String,
    grpc_url: String,
    tls_cert: Option<Cert>,
    // TODO performance: should store infos inside Arcs to avoid copies (key material is big!)
    epochs: RwLock<BTreeMap<EpochNo, EpochInfo>>,
}

impl Client {
    /// Use a custom CA certificate `tls_cert` if the directory service certificate is not anchored
    /// in your system.
    pub fn new(domain: String, port: u16, tls_cert: Option<Cert>) -> Self {
        let grpc_url = format!("https://{}:{}", domain, port);
        Client {
            domain,
            grpc_url,
            tls_cert,
            epochs: RwLock::new(BTreeMap::new()),
        }
    }

    /// setup connection to the directory service
    pub async fn connect(&self) -> Result<DirectoryChannel, tonic::transport::Error> {
        let mut tls_config = ClientTlsConfig::new().domain_name(self.domain.clone());
        if let Some(cert) = &self.tls_cert {
            tls_config = tls_config.ca_certificate(Certificate::from_pem(&cert));
        }
        let channel = Channel::from_shared(self.grpc_url.clone())
            .unwrap()
            .tls_config(tls_config)?
            .connect()
            .await?;

        Ok(DirectoryClient::new(channel))
    }

    /// fetch directory, merge it with our view and return the number of epochs in the reply
    pub async fn fetch(&self, conn: &mut DirectoryChannel) -> Result<usize, Status> {
        let query = DirectoryRequest { min_epoch_no: 0 };
        let directory = conn.query_directory(query).await?.into_inner();
        let number_of_epochs = directory.epochs.len();
        debug!("Fetched directory with {} epochs", number_of_epochs);
        if number_of_epochs == 0 {
            warn!("Directory response is empty");
        }

        let mut epoch_map = self.epochs.write().expect("Lock failure");
        for epoch in directory.epochs {
            debug!(
                ".. epoch {} has {} mixes",
                epoch.epoch_no,
                epoch.mixes.len()
            );
            epoch_map.insert(epoch.epoch_no, epoch);
        }

        // delete old epochs from our map (old = communication phase is over)
        // for now: search the first epoch that is not old and split the map there
        // TODO nightly: drain_filter instead
        let current_time = current_time_in_secs();
        let maybe_keep_epoch_no = epoch_map
            .iter()
            .skip_while(|(_, e)| e.communication_end_time() <= current_time)
            .next()
            .map(|(epoch_no, _)| *epoch_no);

        match maybe_keep_epoch_no {
            Some(e) => *epoch_map = epoch_map.split_off(&e),
            None => epoch_map.clear(), // all epochs too old
        }

        Ok(number_of_epochs)
    }

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

    /// Sleep till the next setup phase starts, waking up `slack` seconds before.
    /// If we are already withing `slack` seconds of the next setup start, do not sleep at all.
    /// If we do not know the next setup start time, sleep for `default` seconds.
    pub async fn sleep_till_next_setup(&self, slack: u64, default: u64) {
        let current_time = current_time_in_secs();
        let wait_for = match self.next_setup_start() {
            Some(t) if t > current_time + slack => t - current_time - slack,
            Some(t) if t > current_time => 0,
            Some(t) => {
                warn!(
                    "Next epoch starts in the past!? ({} seconds ago)",
                    current_time - t
                );
                default
            }
            None => {
                warn!("Don't know when the next epoch starts");
                default
            }
        };
        delay_for(Duration::from_secs(wait_for)).await;
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

    /// Return the smallest epoch number we know of. `None` if we don't know any epoch.
    pub fn smallest_epoch_no(&self) -> Option<EpochNo> {
        let epoch_map = self.epochs.read().expect("Lock failure");
        for (epoch_no, _) in epoch_map.iter() {
            // ordered map
            return Some(*epoch_no);
        }
        None
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

    /// Manually insert an epoch, use at own risk!
    pub fn insert_epoch(&self, epoch: EpochInfo) {
        let mut epoch_map = self.epochs.write().expect("Lock failure");
        epoch_map.insert(epoch.epoch_no, epoch);
    }
}

pub async fn run(client: Arc<Client>) {
    loop {
        let slack = rand::thread_rng().gen_range(10, 20);
        client.sleep_till_next_setup(slack, 1).await;
        info!("Updating directory");
        match client.connect().await {
            Ok(mut conn) => {
                client.fetch(&mut conn).await.unwrap_or_else(|e| {
                    warn!("Fetching directory failed: {}", e);
                    0
                });
                ()
            }
            Err(e) => {
                warn!(
                    "Connection to directory service failed, skipping update: {}",
                    e
                );
            }
        }
        // another sleep to avoid multiple updates per epoch
        delay_for(Duration::from_secs(slack + 1)).await;
    }
}

macro_rules! define_addr_getter {
    ($getter_name:ident, $port_field:ident) => {
        pub fn $getter_name(&self) -> Option<SocketAddr> {
            ip_addr_from_slice(&self.address)
                .ok()
                .map(|ip| SocketAddr::new(ip, self.$port_field as u16))
        }
    };
}

/// Implement some "complex" getters for MixInfo (gRPC message type).
impl MixInfo {
    define_addr_getter!(entry_address, entry_port);
    define_addr_getter!(relay_address, relay_port);
    define_addr_getter!(rendezvous_address, rendezvous_port);
}
