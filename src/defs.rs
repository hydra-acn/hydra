//! Various definitions and helper functions
use byteorder::{ByteOrder, LittleEndian, ReadBytesExt};
use std::mem::size_of;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::time::delay_for as sleep;
use tokio::time::Duration;

pub fn hydra_version() -> &'static str {
    option_env!("CARGO_PKG_VERSION").unwrap_or("Unknown")
}

pub type AuthTag = Vec<u8>;
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
pub const CELL_LEN: usize =
    ONION_LEN + std::mem::size_of::<CircuitId>() + std::mem::size_of::<RoundNo>();

pub const SETUP_ADDR_LEN: usize = 18; // v6 addr and port
pub const SETUP_NONCE_LEN: usize = 12;
pub const SETUP_AUTH_LEN: usize = 16;

pub const PUBLISH_ROUND_NO: RoundNo = std::u32::MAX;
pub const INJECT_ROUND_NO: RoundNo = std::u32::MAX - 1;

pub const CONTACT_SERVICE_TOKEN: Token = 0;

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
        sleep(Duration::from_millis(500)).await;
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
    fn test_bytes_to_token() {
        // initialize test: perfect sized vector to tokens
        let bytes: Vec<u8> = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 255, 255, 255, 255, 255, 255, 255, 255, 11, 11, 11, 11, 11, 11,
            11,
        ];
        let expected_tokens: Vec<_> = vec![578437695752307201, 18446744073709551615];
        // initialize test: wrong sized Vector to tokens
        let too_short: Vec<u8> = vec![255, 255, 255, 255, 255, 255, 255];
        let expected_tokens_too_short: Vec<Token> = Vec::new();
        let tokens1 = tokens_from_bytes(&bytes);
        assert_eq!(tokens1, expected_tokens);
        let tokens2 = tokens_from_bytes(&too_short);
        assert_eq!(tokens2, expected_tokens_too_short);
    }
}
