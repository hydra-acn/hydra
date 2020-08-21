//! Wrapping OpenSSL random number generation for convenient use
use rand::Rng;
use rand_core::{impls, CryptoRng, Error, RngCore};

struct Cprng {}

impl CryptoRng for Cprng {}

impl RngCore for Cprng {
    fn fill_bytes(&mut self, dest: &mut [u8]) {
        openssl::rand::rand_bytes(dest).expect("openssl could not seed CPRNG");
    }

    fn try_fill_bytes(&mut self, dest: &mut [u8]) -> Result<(), Error> {
        Ok(self.fill_bytes(dest))
    }

    fn next_u32(&mut self) -> u32 {
        impls::next_u32_via_fill(self)
    }

    fn next_u64(&mut self) -> u64 {
        impls::next_u64_via_fill(self)
    }
}

pub fn thread_cprng() -> impl Rng + CryptoRng {
    Cprng {}
}
