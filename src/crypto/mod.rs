//! Crypto stuff for Hydra
pub mod aes;
pub mod cprng;
pub mod key;
pub mod threefish;
pub mod threefish_bindings;
pub mod tls;
pub mod x25519;
pub mod x25519_bindings;
pub mod x448;
pub mod x448_bindings;

use std::os::raw::c_uint;

pub enum KeyExchangeAlgorithm {
    X25519,
    X448,
}

extern "C" {
    fn activate_fakerand(seed: c_uint);
}

/// Attention: Use in tests only!!!
pub fn activate_fake_rand(seed: u32) {
    unsafe {
        activate_fakerand(seed as c_uint);
    }
}
