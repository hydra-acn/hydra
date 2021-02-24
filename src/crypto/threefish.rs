use crate::error::Error;

use super::key::Key;
use super::threefish_bindings::{threefish1024_encrypt, threefish1024_set_key, Threefish1024Key};

/// Double the block size of Threefish1024, using a 12-round Feistel network.
pub struct Threefish2048 {
    key: Threefish1024Key,
}

enum Mode {
    Enc,
    Dec,
}

impl Threefish2048 {
    pub fn new(key: Key) -> Result<Self, Error> {
        if key.len() != 128 {
            return Err(Error::SizeMismatch(format!(
                "Wrong key len for Threefish2048: {}",
                key.len()
            )));
        }
        let mut tf_key = Threefish1024Key { key: [0; 17] };
        unsafe {
            threefish1024_set_key(&mut tf_key, key.head_ptr() as *const u64);
        }
        Ok(Threefish2048 { key: tf_key })
    }

    /// Encrypt `data` in place.
    pub fn encrypt(&self, tweak_src: u64, data: &mut [u8]) -> Result<(), Error> {
        self.feistel(tweak_src, data, Mode::Enc)
    }

    /// Dencrypt `data` in place.
    pub fn decrypt(&self, tweak_src: u64, data: &mut [u8]) -> Result<(), Error> {
        self.feistel(tweak_src, data, Mode::Dec)
    }

    fn feistel(&self, tweak_src: u64, data: &mut [u8], mode: Mode) -> Result<(), Error> {
        if data.len() != 256 {
            return Err(Error::SizeMismatch(format!(
                "Wrong data len for Threefish2048: {}",
                data.len()
            )));
        }

        let (mut left, mut right) = data.split_at_mut(128);

        let tweak_ctrs: Vec<u64>;
        match mode {
            Mode::Enc => tweak_ctrs = (tweak_src..tweak_src + 12).collect(),
            Mode::Dec => {
                tweak_ctrs = (tweak_src..tweak_src + 12).rev().collect();
                // swap once beforehand for decryption
                std::mem::swap(&mut left, &mut right);
            }
        }

        for tweak_ctr in tweak_ctrs {
            // step 0: set the tweak
            // TODO will not work on big endian?
            let tweak: [u64; 2] = [tweak_ctr, 0];

            // step 1: encrypt right (not in place!)
            let mut enc = [0u8; 128];
            enc.copy_from_slice(right);

            unsafe {
                threefish1024_encrypt(
                    &self.key,
                    &tweak[0] as *const u64,
                    (&enc[0] as *const u8).cast(),
                    (&mut enc[0] as *mut u8).cast(),
                );
            }

            // step 2: xor the ciphertext with left in place
            for i in 0..left.len() {
                left[i] ^= enc[i];
            }

            // step 3: swap left and right for next round
            std::mem::swap(&mut left, &mut right);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity() {
        let key = Key::from_hex_str("952f0afbc1df668aaba30c8f6cc55e947c9ead343588a34c31eb42dc3b8c55df72faab848acbd8b3f697546c1b413e094f52b887a62326b3b564b3c3bca41186a20fdbae720f6b7ce3596f0d88ba9fe69bc38a69bf43ff2ffbdb5d1039e627565b5a99f3e9529bec49b43510bdd7612e529ca1a6e5ad10ff3f05af86446fb9fe").unwrap();
        let first_tweak = 24;
        let tf = Threefish2048::new(key).unwrap();
        let mut data = hex::decode("59acd2c416777218da491ac6c23d887d62248dd163c9c8730f6a64b83a02010399f720f2ad591c654d12ead75251f9a9e99db8ce02094d9d4e0f5bc7aa07585e2945abfee4c21c511f8cd8518b5263449cc6401e297418013d0be551933e5d5b9960e6ae0b9f70196c9f6495f43d85f02f30a620f49b2976c8427ac2aff77f7606c2a9adc71cc01a11a7a34ac57c724224f7da237e30e389992e9e849edd5be4dc88c7fc4a8a41253a50a44ca8fb36771ca6ccc54277722965ee7e540f5c43dbb48803a5651a71bac2b9996380aab5bea9a8cbdd03872fd298bd0d738c4daab4b178867a5e0fb873544a5565e74184c0354a69d762d11b8453f06d8bf984d4d8").unwrap();

        let plaintext = data.clone();
        let ciphertext = hex::decode("644d078cf0e47b8c1b2a1aa7b1c53ec3d0a50c5f9cb720985dad18d321a26482abb0988cea30bb5ff0a9c367f4bb4e186fc25f55235e715f0e5bcfa9163b838d8886e5f42ce47e1ce842cfc8468e476b71fbc9427a0fc2af7256ea728e02f66789ce339ca5414b8f66ecd849e1dc3d1749ecf8832604ad8c827ef792973ef1b9322ef3f74332ed0bd3babac7990774878c7772a59b38440052202cbfa80197c413cd441637880af4ee63a93f8e6c7584710127a90b099d7634f44fe3f66b5d63fe5613d4d8a21fccd744237e31b49da0702bcdd4ed80c0169bde3f23ff1dc7397a4984d94f0f0fda5a8c21ee9e2fe0b6bf39f8e52ed43e2141e13a79925a1d00").unwrap();

        tf.encrypt(first_tweak, &mut data).unwrap();
        assert_eq!(data, ciphertext);
        tf.decrypt(first_tweak, &mut data).unwrap();
        assert_eq!(data, plaintext);
    }
}
