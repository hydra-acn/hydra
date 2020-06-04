//! Safe abstraction (overwrite mem with zero on drop) for raw bytes that are used as keys

use crate::error::Error;
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

    /// move from u8 vector
    pub fn move_from_vec(bytes: Vec<u8>) -> Self {
        Key { key: bytes }
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn generate_random_key() {
        let size = 1337;
        let key = Key::new(size).expect("Key gen failed");
        assert_eq!(key.len(), size);
    }
}
