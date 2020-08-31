//! `impl` some helper methods for gRPC message types.
use byteorder::{ByteOrder, LittleEndian};
use rand::Rng;
use std::convert::TryInto;

use crate::crypto::cprng::thread_cprng;
use crate::defs::{token_from_bytes, CircuitId, RoundNo, Token, ONION_LEN};
use crate::mix::rss_pipeline::Scalable;
use crate::tonic_mix::{Cell, SetupPacket, SubscriptionVector};

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
}

impl Scalable for SetupPacket {
    fn thread_id(&self, size: usize) -> usize {
        self.circuit_id as usize % size
    }
}

impl SubscriptionVector {
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

impl Scalable for SubscriptionVector {}

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
