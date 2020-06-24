use std::net::IpAddr;
use std::net::SocketAddr;

use crate::error::Error;

pub fn ip_addr_from_slice(a: &[u8]) -> Result<IpAddr, Error> {
    match a.len() {
        4 => {
            let mut octets: [u8; 4] = [0; 4];
            octets.copy_from_slice(&a[..]);
            Ok(IpAddr::V4(octets.into()))
        }
        16 => {
            let mut octets: [u8; 16] = [0; 16];
            octets.copy_from_slice(&a[..]);
            Ok(IpAddr::V6(octets.into()))
        }
        _ => Err(Error::InputError(
            "Length of slice does not match a valid IP address length".to_string(),
        )),
    }
}

pub fn ip_addr_to_vec(a: &IpAddr) -> Vec<u8> {
    match a {
        IpAddr::V4(v4) => v4.octets().to_vec(),
        IpAddr::V6(v6) => v6.octets().to_vec(),
    }
}

pub fn socket_addr_from_slice(addr: &[u8], port: u16) -> Result<SocketAddr, Error> {
    ip_addr_from_slice(addr).map(|a| SocketAddr::new(a, port))
}
