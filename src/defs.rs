//! Various definitions and helper functions

use byteorder::{ByteOrder, LittleEndian, ReadBytesExt};
use ctrlc;
use rand::Rng;
use std::convert::TryInto;
use std::mem::size_of;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::time::{delay_for, Duration};

use crate::crypto::cprng::thread_cprng;
use crate::tonic_mix::{Cell, SetupPacket};

pub type Token = u64;
pub type CircuitId = u64;
pub type CircuitIdSet = std::collections::BTreeSet<CircuitId>;
pub type RoundNo = u32;

pub const DIR_AUTH_KEY_SIZE: usize = 32;
pub const DIR_AUTH_KEY_INFO: &[u8; 4] = b"auth";

pub const DIR_AUTH_UNREGISTER: &[u8; 10] = b"unregister";

/// Number of tokens in a setup packet
pub const SETUP_TOKENS: usize = 256;
pub const ONION_LEN: usize = 256;

// TODO use these more often instead of magic numbers :)
pub const SETUP_NONCE_LEN: usize = 12;
pub const SETUP_AUTH_LEN: usize = 16;

pub fn hydra_version() -> &'static str {
    option_env!("CARGO_PKG_VERSION").unwrap_or("Unknown")
}

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

/// Usage: create an `AtomicBool` with value `true` and spawn the handler on a separate thread. As
/// soon as `SIGINT` is catched, two things will happen (both may be helpful for cleanup):
/// 1. the thread the handler ran own panics -> catch and cleanup
/// 2. the `AtomicBool` is set to false -> poll and cleanup
pub async fn sigint_handler(running: Arc<AtomicBool>) {
    let r = running.clone();
    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
    })
    .expect("Setting Ctrl-C handler failed");
    while running.load(Ordering::SeqCst) {
        delay_for(Duration::from_millis(500)).await;
    }
    log::info!("Caught SIGINT");
    panic!("Interrupted");
}

/// Decode bytes as little-endian u64
///
/// # Examples
/// ```
/// # use hydra::defs::token_from_bytes;
/// let raw = vec![42, 0, 0, 0, 0, 0, 0, 128];
/// let token = token_from_bytes(&raw);
/// assert_eq!(token.unwrap(), (1u64 << 63) + 42);
/// ```
pub fn token_from_bytes(raw: &[u8]) -> Option<Token> {
    if raw.len() != 8 {
        return None;
    }
    let mut rdr = std::io::Cursor::new(raw);
    Some(
        rdr.read_u64::<LittleEndian>()
            .expect("Why should this fail?"),
    )
}

pub fn tokens_from_bytes(raw: &[u8]) -> Vec<Token> {
    let mut tokens: Vec<Token> = Vec::new();
    for i in (0..raw.len()).step_by(8) {
        match raw.get(i..i + 8) {
            Some(token) => tokens.push(
                token_from_bytes(&token).expect("Something went wrong during the conversion"),
            ),
            None => {
                log::warn!("Size of Vector is not a multiple of eight.");
            }
        };
    }
    tokens
}

pub fn tokens_to_byte_vec(tokens: &[Token]) -> Vec<u8> {
    let mut vec = vec![0; size_of::<Token>() * tokens.len()];
    let mut i = 0;
    for t in tokens.iter() {
        LittleEndian::write_u64(&mut vec[i..i + 8], *t);
        i += 8;
    }
    vec
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

#[macro_export]
macro_rules! delegate_generic {
    ($to:ident; $doc:expr; $fnname:ident; $($arg:ident: $type:ty),* => $ret:ty) => {
        #[doc = $doc]
        pub fn $fnname(&self, $($arg: $type),*) -> $ret {
            self.$to.$fnname($($arg),*)
        }
    };
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

    #[test]
    fn test_bytes_to_token() {
        //initialise test: perfect sized vector to tokens
        let bytes: Vec<u8> = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 255, 255, 255, 255, 255, 255, 255, 255, 11, 11, 11, 11, 11, 11,
            11,
        ];
        let expected_vec_bytes: Vec<_> = vec![578437695752307201, 18446744073709551615];
        //initialise test: wrong sized Vector to tokens
        let too_short: Vec<u8> = vec![255, 255, 255, 255, 255, 255, 255];
        let expected_vec_too_short: Vec<_> = Vec::new();
        //Call function and evaluate result for the perfect sized vector
        let tokens1 = tokens_from_bytes(&bytes);
        assert_eq!(tokens1, expected_vec_bytes);
        //Call function and evaluate result for the wrong sized vector
        let tokens2 = tokens_from_bytes(&too_short);
        assert_eq!(tokens2, expected_vec_too_short);
    }
}
