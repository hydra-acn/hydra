//! Safe abstraction (overwrite mem with zero on drop) for raw bytes that are used as keys

use crate::error::Error;

use hkdf::Hkdf;
use openssl::rand::rand_bytes;

/// the Key datatype
/// TODO type parameter to indicate public/private keys?
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Key {
    key: Vec<u8>,
}

impl Key {
    /// generate a random key (cryptographically secure) with a given size
    pub fn new(size: usize) -> Result<Self, Error> {
        let mut key = Key { key: vec![0; size] };
        match rand_bytes(&mut key.key) {
            Ok(_) => Ok(key),
            Err(e) => Err(Error::OpenSslError(e.to_string())),
        }
    }

    /// move from byte vector
    pub fn move_from_vec(bytes: Vec<u8>) -> Self {
        Key { key: bytes }
    }

    /// clone from byte slice
    pub fn clone_from_slice(bytes: &[u8]) -> Self {
        Key {
            key: bytes.to_vec(),
        }
    }

    /// generate "key" with zero bytes
    pub fn zero(size: usize) -> Self {
        Key { key: vec![0; size] }
    }

    /// generate key from hex string
    /// TODO not safe yet (the string itself will not be overriden)
    pub fn from_hex_str(hex: &str) -> Result<Self, Error> {
        match hex::decode(&hex) {
            Ok(vec) => Ok(Key { key: vec }),
            Err(e) => return Err(Error::InputError(e.to_string())),
        }
    }

    /// const pointer to first byte, use at own risk
    pub fn head_ptr(&self) -> *const u8 {
        &(self.key[0]) as *const u8
    }

    /// return size in bytes
    pub fn len(&self) -> usize {
        self.key.len()
    }

    /// TODO only for public keys
    pub fn clone_to_vec(&self) -> Vec<u8> {
        self.key.clone()
    }

    pub fn borrow_raw(&self) -> &[u8] {
        &self.key
    }
}

impl Drop for Key {
    fn drop(&mut self) {
        // zero out the key
        for b in &mut self.key {
            *b = 0;
        }
    }
}

/// Generate multiple keys by expanding a "master" `key` using the HKDF key derivation function
/// (RFC 5869), instantiated with SHA256 as hash algorithm. The number and size of generated keys
/// is dictated by the `sizes` slice.
pub fn hkdf_sha256(
    key: &Key,
    salt: Option<&[u8]>,
    info: Option<&[u8]>,
    sizes: &[usize],
) -> Result<Vec<Key>, Error> {
    let hkdf = Hkdf::<sha2::Sha256>::new(salt, key.borrow_raw());
    let total_size = sizes.iter().sum();
    let mut okm = vec![0u8; total_size];
    let info_slice = info.unwrap_or(&[0u8; 0]);
    hkdf.expand(info_slice, &mut okm)?;
    let mut keys = Vec::new();
    for size in sizes {
        // TODO security: split_off potentially does copies but no overwrites?
        let remaining_okm = okm.split_off(*size);
        let key_vec = okm;
        okm = remaining_okm;
        keys.push(Key::move_from_vec(key_vec));
    }
    Ok(keys)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn generate_random_key() {
        let size = 1337;
        let key = Key::new(size).expect("Key gen failed");
        assert_eq!(key.len(), size);
    }

    #[test]
    // (Some) HKDF test vectors from RFC 5869
    fn hkdf() {
        // test case 2
        let ikm = Key::from_hex_str("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f").unwrap();
        let salt = hex::decode("606162636465666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f808182838485868788898a8b8c8d8e8f909192939495969798999a9b9c9d9e9fa0a1a2a3a4a5a6a7a8a9aaabacadaeaf").unwrap();

        let info = hex::decode("b0b1b2b3b4b5b6b7b8b9babbbcbdbebfc0c1c2c3c4c5c6c7c8c9cacbcccdcecfd0d1d2d3d4d5d6d7d8d9dadbdcdddedfe0e1e2e3e4e5e6e7e8e9eaebecedeeeff0f1f2f3f4f5f6f7f8f9fafbfcfdfeff").unwrap();

        let sizes = [42, 40];

        let expected_first = Key::from_hex_str(
            "b11e398dc80327a1c8e7f78c596a49344f012eda2d4efad8a050cc4c19afa97c59045a99cac7827271cb",
        )
        .unwrap();

        let expected_second = Key::from_hex_str(
            "41c65e590e09da3275600c2f09b8367793a9aca3db71cc30c58179ec3e87c14c01d5c1f3434f1d87",
        )
        .unwrap();

        let keys = hkdf_sha256(&ikm, Some(&salt), Some(&info), &sizes).unwrap();
        assert_eq!(keys.get(0).unwrap(), &expected_first);
        assert_eq!(keys.get(1).unwrap(), &expected_second);
    }
}
