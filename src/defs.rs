//! Various definitions and helper functions

use byteorder::{LittleEndian, ReadBytesExt};

pub type Token = u64;

/// Decode bytes as little-endian u64
///
/// # Examples
/// ```
/// # use hydra::defs::token_from_bytes;
/// let raw = [42, 0, 0, 0, 0, 0, 0, 128];
/// let token = token_from_bytes(&raw);
/// assert_eq!(token, (1u64 << 63) + 42);
/// ```
pub fn token_from_bytes(raw: &[u8; 8]) -> Token {
    let mut rdr = std::io::Cursor::new(raw);
    rdr.read_u64::<LittleEndian>()
        .expect("Why should this fail?")
}

pub fn hydra_version() -> &'static str {
    option_env!("CARGO_PKG_VERSION").unwrap_or("Unknown")
}
