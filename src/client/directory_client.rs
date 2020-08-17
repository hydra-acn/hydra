//! Hydra client talking to the directory service
use log::*;
use rand::seq::SliceRandom;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::RwLock;
use tokio::time::Duration;
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
        let mut epoch_map = self.epochs.write().expect("Lock failure");

        if let Some(next_epoch) = directory.epochs.first() {
            // delete old epochs from our map (but keep current, which is not in the directory)
            let current_epoch_no = next_epoch.epoch_no - 1;
            *epoch_map = epoch_map.split_off(&current_epoch_no);
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

    /// Return the smallest epoch number we know of. `None` if we don't know any epoch.
    pub fn smallest_epoch_no(&self) -> Option<EpochNo> {
        let epoch_map = self.epochs.read().expect("Lock failure");
        for (epoch_no, _) in epoch_map.iter() {
            // ordered map
            return Some(*epoch_no);
        }
        None
    }

    /// if we are currently in a communication phase, return the duration till next receive
    /// TODO rename/rework: time till round end (send and receive anytime between rounds)
    pub fn next_receive_in(&self) -> Option<Duration> {
        let epoch_map = self.epochs.read().expect("Lock failure");
        for (_, epoch) in epoch_map.iter() {
            if let Some(d) = epoch.next_receive_in() {
                return Some(d);
            }
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

/// implement some "complex" getters for MixInfo (gRPC message type)
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
