//! x448 key exchange (Diffie Hellman with Curve448)

pub use super::x448_bindings::{POINT_SIZE};
use super::key::Key;
use super::x448_bindings::{self, x448_derive_public_key, x448_int, SCALAR_SIZE};
use crate::error::Error;

/// return Ok((pk, sk)) on success
pub fn generate_keypair() -> Result<(Key, Key), Error> {
    let sk = Key::new(SCALAR_SIZE)?;
    let mut pk_vec = vec![0u8; POINT_SIZE];
    unsafe {
        x448_derive_public_key(&mut (pk_vec[0]) as *mut u8, sk.head_ptr());
    }
    let pk = Key::move_from_vec(pk_vec);
    Ok((pk, sk))
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
        let res = x448_int(&mut (s_vec[0]) as *mut u8, pk.head_ptr(), sk.head_ptr());
        if res == x448_bindings::FAILURE {
            return Err(Error::OpenSslError(format!("x448 C call failed")));
        }
    }
    let s = Key::move_from_vec(s_vec);
    Ok(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    // test vector from RFC 7748, section 6.2
    fn rfc_test_vector() {
        // constants from RFC
        let sk_alice = Key::from_hex_str(
            "9a8f4925d1519f5775cf46b04b5800d4ee9ee8bae8bc5565d498c28dd9c9baf574a9419744897391006382a6f127ab1d9ac2d8c0a598726b"
        )
        .unwrap();
        let pk_alice = Key::from_hex_str(
            "9b08f7cc31b7e3e67d22d5aea121074a273bd2b83de09c63faa73d2c22c5d9bbc836647241d953d40c5b12da88120d53177f80e532c41fa0"
        )
        .unwrap();
        let sk_bob = Key::from_hex_str(
            "1c306a7ac2a0e2e0990b294470cba339e6453772b075811d8fad0d1d6927c120bb5ee8972b0d3e21374c9c921b09d1b0366f10b65173992d"
        )
        .unwrap();
        let pk_bob = Key::from_hex_str(
            "3eb7a829b0cd20f5bcfc0b599b6feccf6da4627107bdb0d4f345b43027d8b972fc3e34fb4232a13ca706dcb57aec3dae07bdc1c67bf33609"
        )
        .unwrap();
        let s = Key::from_hex_str(
            "07fff4181ac6cc95ec1c16a94a0f74d12da232ce40a77552281d282bb60c0b56fd2464c335543936521c24403085d59a449a5037514a879d"
        )
        .unwrap();

        // test Alice' view
        let s_alice = generate_shared_secret(&pk_bob, &sk_alice).unwrap();
        assert_eq!(s_alice, s);

        // test Bob's view
        let s_bob = generate_shared_secret(&pk_alice, &sk_bob).unwrap();
        assert_eq!(s_bob, s);
    }
}
