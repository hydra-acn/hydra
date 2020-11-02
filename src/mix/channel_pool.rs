//! Pool for storing and managing generic gRPC channels ("clients")
use std::collections::HashMap;
use std::net::SocketAddr;
use std::net::TcpStream;
use std::sync::{Arc, Mutex, RwLock};
use tonic::async_trait;

use crate::error::Error;
use crate::tonic_mix::mix_client::MixClient;
use crate::tonic_mix::rendezvous_client::RendezvousClient;
use log::*;

#[async_trait]
pub trait Channel: Sized {
    async fn connect(dst: SocketAddr) -> Result<Self, Error>;
}

pub type TcpChannel = Arc<RwLock<TcpStream>>;

#[tonic::async_trait]
impl Channel for TcpChannel {
    async fn connect(dst: SocketAddr) -> Result<Self, Error> {
        debug!("Connecting TCP stream to {}", dst);
        let stream = TcpStream::connect(dst)?;
        stream.set_nodelay(false)?;
        debug!(
            ".. TCP connection to {} established, src port {}",
            dst,
            stream.local_addr().unwrap().port()
        );
        Ok(Arc::new(RwLock::new(stream)))
    }
}

/// Derive implementations of the `Channel` trait for gRPC clients
macro_rules! derive_grpc_channel {
    ($type:ident) => {
        #[tonic::async_trait]
        impl Channel for $type {
            async fn connect(dst: SocketAddr) -> Result<Self, Error> {
                debug!("Connecting gRPC channel to {}", dst);
                let endpoint = format!("http://{}:{}", dst.ip(), dst.port());
                // TODO maybe we want to decrease timeouts here (depending on tonic's defaults)?
                let builder = tonic::transport::Channel::from_shared(endpoint)?;
                let channel = builder.tcp_nodelay(false).connect().await?;
                debug!(".. gRPC channel to {} established", dst);
                Ok($type::new(channel))
            }
        }
    };
}

pub type MixChannel = MixClient<tonic::transport::Channel>;
pub type RendezvousChannel = RendezvousClient<tonic::transport::Channel>;
derive_grpc_channel!(MixChannel);
derive_grpc_channel!(RendezvousChannel);

/// TODO polish, i.e.
/// * refresh/keepalive (might be done by Tonic gRPC and std TCP anyway)?
/// * check for failed channels
/// * remove old channels that are not necessary anymore
pub struct ChannelPool<T: Channel + Send + Clone + 'static> {
    channels: Mutex<HashMap<SocketAddr, T>>,
}

impl<T> ChannelPool<T>
where
    T: Channel + Send + Clone + 'static,
{
    pub fn new() -> Self {
        ChannelPool::<T> {
            channels: Mutex::new(HashMap::new()),
        }
    }

    pub async fn get_channel(&self, dst: &SocketAddr) -> Option<T> {
        let as_vec = vec![*dst];
        let map = self.get_channels(&as_vec).await;
        map.get(dst).cloned()
    }

    pub async fn get_channels(&self, destinations: &[SocketAddr]) -> HashMap<SocketAddr, T> {
        self.prepare_channels(destinations).await;
        let mut requested = HashMap::new();
        let existing = self.channels.lock().expect("Lock failure");
        for dst in destinations {
            match existing.get(dst) {
                Some(c) => {
                    requested.insert(*dst, c.clone());
                    ()
                }
                None => warn!("Expected to have a connection to {} by now", dst),
            }
        }
        requested
    }

    pub async fn prepare_channels(&self, destinations: &[SocketAddr]) {
        let mut to_create = Vec::new();
        {
            let existing = self.channels.lock().expect("Lock failure");
            for dst in destinations {
                if !existing.contains_key(dst) {
                    to_create.push(*dst);
                }
            }
        }
        let new_channels: HashMap<SocketAddr, T> = create_multiple_channels(to_create.iter()).await;
        {
            let mut existing = self.channels.lock().expect("Lock failure");
            for (dst, channel) in new_channels.into_iter() {
                existing.insert(dst, channel.clone());
            }
        }
    }
}

async fn create_multiple_channels<T: Channel + Send + 'static>(
    destinations: impl Iterator<Item = &SocketAddr>,
) -> HashMap<SocketAddr, T> {
    // spawn setup for each channel
    let mut channel_handles = HashMap::new();
    for dst in destinations {
        channel_handles.insert(*dst, tokio::spawn(create_channel::<T>(*dst)));
    }

    // collect ready channels
    let mut channel_map = HashMap::new();
    for (dst, handle) in channel_handles {
        let result = handle.await.expect("Task for setting up channel panicked");
        match result {
            Ok(c) => {
                channel_map.insert(dst, c);
                ()
            }
            Err(e) => warn!("Setting up channel to {} failed: {}", dst, e),
        }
    }
    channel_map
}

async fn create_channel<T: Channel>(addr: SocketAddr) -> Result<T, Error> {
    T::connect(addr).await
}
