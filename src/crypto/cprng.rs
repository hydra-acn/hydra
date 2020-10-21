//! Wrapping `ThreadRng` from `rand` library, which actually is a CPRNG in version 0.7.
use rand::{thread_rng, CryptoRng, Rng};

pub fn thread_cprng() -> impl Rng + CryptoRng {
    thread_rng()
}
