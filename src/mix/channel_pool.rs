//! Pool for storing and managing generic gRPC channels ("clients")
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Mutex;

use crate::error::Error;
use crate::grpc::Client;
use log::*;

/// TODO polish, i.e.
/// * refresh/keepalive (might be done by tonix anyway)?
/// * check for failed channels
/// * remove old channels that are not necessary anymore
pub struct ChannelPool<T: Client + Send + Clone + 'static> {
    channels: Mutex<HashMap<SocketAddr, T>>,
}

impl<T> ChannelPool<T>
where
    T: Client + Send + Clone + 'static,
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

async fn create_multiple_channels<T: Client + Send + 'static>(
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
        let result = handle.await.expect("Setting up channel panicked");
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

async fn create_channel<T: Client>(addr: SocketAddr) -> Result<T, Error> {
    let endpoint = format!("http://{}:{}", addr.ip(), addr.port());
    // TODO maybe we want to decrease timeouts here (depending on tonic's defaults)?
    debug!("Connecting to {}", endpoint);
    let channel = T::connect(endpoint).await?;
    Ok(channel)
}
