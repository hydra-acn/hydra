//! `impl` some helper methods for gRPC message types.
use byteorder::{ByteOrder, LittleEndian};
use rand::Rng;
use std::convert::TryInto;
use std::net::SocketAddr;
use tonic::Status;

use crate::crypto::cprng::thread_cprng;
use crate::crypto::x448;
use crate::defs::{
    token_from_bytes, CircuitId, RoundNo, Token, ONION_LEN, SETUP_AUTH_LEN, SETUP_NONCE_LEN,
};
use crate::epoch::EpochNo;
use crate::grpc::macros::valid_request_check;
use crate::mix::directory_client;
use crate::mix::rss_pipeline::Scalable;
use crate::tonic_directory::MixStatistics;
use crate::tonic_mix::{Cell, SetupPacket, Subscription};
use crate::unwrap_or_throw_invalid;
use hmac::Mac;

impl SetupPacket {
    /// for a given setup packet, determine how much hops it needs to be sent
    /// (0 if the onion encrypted part only contains the tokens to subscribe to)
    /// returns `None` if the onion encrypted part has unexpected size
    pub fn ttl(&self) -> Option<u32> {
        let token_len = 256 * 8;
        if self.onion.len() < token_len {
            return None;
        }
        let nom = self.onion.len() - token_len;
        let denom = 102;
        if nom % denom != 0 {
            return None;
        }
        Some((nom / denom) as u32)
    }

    pub fn validity_check(&self, dir_client: &directory_client::Client) -> Result<(), Status> {
        unwrap_or_throw_invalid!(self.ttl(), "Your setup packet has a strange size");
        valid_request_check(
            dir_client.has_ephemeral_key(&self.epoch_no),
            "Seems like we are not part of the given epoch",
        )?;
        valid_request_check(
            self.public_dh.len() == x448::POINT_SIZE,
            "Public key has not the expected size",
        )?;
        valid_request_check(
            self.nonce.len() == SETUP_NONCE_LEN,
            "Nonce has not the expected size",
        )?;
        valid_request_check(
            self.auth_tag.len() == SETUP_AUTH_LEN,
            "Authentication has not the expected size",
        )?;
        Ok(())
    }
}

impl Scalable for SetupPacket {
    fn thread_id(&self, size: usize) -> usize {
        self.circuit_id as usize % size
    }
}

/// The `previous_hop` will be used to forward cells in downstream direction. It is `None` for the
/// first layer.
#[derive(Debug)]
pub struct SetupPacketWithPrev {
    inner: SetupPacket,
    previous_hop: Option<SocketAddr>,
}

impl SetupPacketWithPrev {
    pub fn new(pkt: SetupPacket, previous_hop: Option<SocketAddr>) -> Self {
        SetupPacketWithPrev {
            inner: pkt,
            previous_hop,
        }
    }

    pub fn epoch_no(&self) -> EpochNo {
        self.inner.epoch_no
    }

    pub fn circuit_id(&self) -> CircuitId {
        self.inner.circuit_id
    }

    pub fn ttl(&self) -> Option<u32> {
        self.inner.ttl()
    }

    pub fn previous_hop(&self) -> Option<SocketAddr> {
        self.previous_hop
    }

    pub fn into_inner(self) -> SetupPacket {
        self.inner
    }
}

impl Scalable for SetupPacketWithPrev {
    fn thread_id(&self, size: usize) -> usize {
        self.inner.thread_id(size)
    }
}

impl Subscription {
    /// Check if address and port for injection are valid.
    // TODO security: check for IP addr should be better (e.g. localhost); or use the real src
    // address instead
    pub fn is_valid(&self) -> bool {
        let addr_check = match self.addr.len() {
            4 | 16 => true,
            _ => false,
        };
        let port_check = self.port <= std::u16::MAX as u32;
        addr_check && port_check
    }

    /// Return the socket address to inject to if it is valid.
    pub fn socket_addr(&self) -> Option<std::net::SocketAddr> {
        match self.is_valid() {
            true => Some(
                crate::net::socket_addr_from_slice(&self.addr, self.port as u16)
                    .expect("Checked before"),
            ),
            false => None,
        }
    }
}

impl Scalable for Subscription {
    fn thread_id(&self, size: usize) -> usize {
        self.circuit_id as usize % size
    }
}

pub enum CellCmd {
    Delay(u8),
    Broadcast,
}

impl Cell {
    /// creates new dummy cell
    pub fn dummy(cid: CircuitId, r: RoundNo) -> Self {
        let mut cell = Cell {
            circuit_id: cid,
            round_no: r,
            onion: vec![0; ONION_LEN],
        };
        cell.randomize();
        cell
    }

    pub fn token(&self) -> Token {
        token_from_bytes(self.onion[8..16].try_into().expect("Failed")).unwrap()
    }

    pub fn set_token(&mut self, token: Token) {
        LittleEndian::write_u64(&mut self.onion[8..16], token)
    }

    pub fn command(&self) -> Option<CellCmd> {
        let cmd_slice = &self.onion[1..8];
        if cmd_slice.iter().all(|b| *b == 0) {
            Some(CellCmd::Delay(self.onion[0]))
        } else if self.onion[0] == 255 && cmd_slice.iter().all(|b| *b == 255) {
            Some(CellCmd::Broadcast)
        } else {
            None
        }
    }

    pub fn set_command(&mut self, cmd: CellCmd) {
        let args_cmd_slice = &mut self.onion[0..8];
        match cmd {
            CellCmd::Delay(rounds) => {
                for b in args_cmd_slice.iter_mut() {
                    *b = 0;
                }
                args_cmd_slice[0] = rounds;
            }
            CellCmd::Broadcast => {
                for b in args_cmd_slice.iter_mut() {
                    *b = 255;
                }
            }
        }
    }

    /// Turn existing cell into dummy by randomizing the onion encrypted part.
    pub fn randomize(&mut self) {
        thread_cprng().fill(self.onion.as_mut_slice());
    }
}

impl Scalable for Cell {
    fn thread_id(&self, size: usize) -> usize {
        self.circuit_id as usize % size
    }
}

impl MixStatistics {
    fn generate_mac(&self, mac: &mut hmac::Hmac<sha2::Sha256>) {
        mac.update(&self.epoch_no.to_le_bytes());
        mac.update(&self.fingerprint.as_bytes());
        for val in self.no_circuits_per_layer.iter() {
            mac.update(&val.to_le_bytes());
        }
        for val in self.setup_time_per_layer.iter() {
            mac.update(&val.to_le_bytes());
        }
        for val in self.avg_processing_time_per_layer.iter() {
            mac.update(&val.to_le_bytes());
        }
    }

    pub fn set_auth_tag(&mut self, dir_client: &directory_client::Client) {
        let mut mac = dir_client
            .init_mac()
            .expect("Mac creation failed at directory client.");
        self.generate_mac(&mut mac);
        self.auth_tag = mac.finalize().into_bytes().to_vec();
    }

    pub fn verify_auth_tag(
        &self,
        mut mac: hmac::Hmac<sha2::Sha256>,
    ) -> std::result::Result<(), hmac::crypto_mac::MacError> {
        self.generate_mac(&mut mac);
        mac.verify(&self.auth_tag)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_token() {
        let mut my_cell: Cell = Cell::dummy(1, 2);
        my_cell.set_token(1000);
        let token: Token = my_cell.token();
        assert_eq!(token, 1000);
    }
}
