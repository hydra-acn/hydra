//! Various definitions and helper functions

use ctrlc;
use openssl::rand::rand_bytes;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::time::{delay_for, Duration};

use crate::tonic_mix::Cell;
use byteorder::{LittleEndian, ReadBytesExt};

pub type Token = u64;
pub type CircuitId = u64;
pub type RoundNo = u32;
pub const ONION_SIZE: usize = 256;

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

pub fn dummy_cell(cid: CircuitId, r: RoundNo) -> Cell {
    let mut dummy = Cell {
        circuit_id: cid,
        round_no: r,
        onion: vec![0; ONION_SIZE],
    };
    rand_bytes(&mut dummy.onion).expect("Could not randomize dummy cell, better crash now");
    dummy
}

/// usage: spawn the handler on a separate thread and catch the panic it throws after catching SIGINT
pub async fn sigint_handler() {
    let running = Arc::new(AtomicBool::new(true));
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
