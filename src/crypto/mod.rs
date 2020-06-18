//! Crypto stuff for Hydra
pub mod aes;
pub mod key;
pub mod threefish;
pub mod x448;
pub mod x448_bindings;

use std::os::raw::c_uint;

extern "C" {
    fn activate_fakerand(seed: c_uint);
}

/// Attention: Use in tests only!!!
pub fn activate_fake_rand(seed: u32) {
    unsafe {
        activate_fakerand(seed as c_uint);
    }
}
