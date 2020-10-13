//! x25519 key exchange (Diffie Hellman with Curve25519)

use super::key::Key;
pub use super::x25519_bindings::POINT_SIZE;
use super::x25519_bindings::{X25519_public_from_private, SCALAR_SIZE, X25519};
use crate::error::Error;

/// return (pk, sk)
pub fn generate_keypair() -> (Key, Key) {
    let sk = Key::new(SCALAR_SIZE);
    let mut pk_vec = vec![0u8; POINT_SIZE];
    unsafe {
        X25519_public_from_private(&mut (pk_vec[0]) as *mut u8, sk.head_ptr());
    }
    let pk = Key::move_from_vec(pk_vec);
    (pk, sk)
}

/// generate shared secret
pub fn generate_shared_secret(pk: &Key, sk: &Key) -> Result<Key, Error> {
    if pk.len() != POINT_SIZE {
        return Err(Error::SizeMismatch(format!(
            "Public key has wrong size: {}",
            pk.len()
        )));
    }
    if sk.len() != POINT_SIZE {
        return Err(Error::SizeMismatch(format!(
            "Secret key has wrong size: {}",
            sk.len()
        )));
    }
    let mut s_vec = vec![0u8; POINT_SIZE];
    unsafe {
        let res = X25519(&mut (s_vec[0]) as *mut u8, sk.head_ptr(), pk.head_ptr());
        if res == 0 {
            return Err(Error::ExternalError(format!("x25519 C call failed")));
        }
    }
    let s = Key::move_from_vec(s_vec);
    Ok(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    // test vector from RFC 7748, section 6.1
    fn rfc_test_vector() {
        // constants from RFC
        let sk_alice =
            Key::from_hex_str("77076d0a7318a57d3c16c17251b26645df4c2f87ebc0992ab177fba51db92c2a")
                .unwrap();
        let pk_alice =
            Key::from_hex_str("8520f0098930a754748b7ddcb43ef75a0dbf3a0d26381af4eba4a98eaa9b4e6a")
                .unwrap();
        let sk_bob =
            Key::from_hex_str("5dab087e624a8a4b79e17f8b83800ee66f3bb1292618b6fd1c2f8b27ff88e0eb")
                .unwrap();
        let pk_bob =
            Key::from_hex_str("de9edb7d7b7dc1b4d35b61c2ece435373f8343c85b78674dadfc7e146f882b4f")
                .unwrap();
        let s =
            Key::from_hex_str("4a5d9d5ba4ce2de1728e3bf480350f25e07e21c947d19e3376f09b3c1e161742")
                .unwrap();

        // test Alice' view
        let s_alice = generate_shared_secret(&pk_bob, &sk_alice).unwrap();
        assert_eq!(s_alice, s);

        // test Bob's view
        let s_bob = generate_shared_secret(&pk_alice, &sk_bob).unwrap();
        assert_eq!(s_bob, s);
    }
}
