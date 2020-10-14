pub const KEY_LEN: usize = 32;

extern "C" {
    pub fn X25519(out: *mut u8, sk: *const u8, pk: *const u8) -> i32;
}

extern "C" {
    pub fn X25519_public_from_private(out: *mut u8, scalar: *const u8);
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    // test vector from RFC 7748, section 6.1
    fn rfc_test_vector() {
        // constants from RFC
        let sk_alice =
            hex::decode("77076d0a7318a57d3c16c17251b26645df4c2f87ebc0992ab177fba51db92c2a")
                .unwrap();
        let pk_alice =
            hex::decode("8520f0098930a754748b7ddcb43ef75a0dbf3a0d26381af4eba4a98eaa9b4e6a")
                .unwrap();
        let sk_bob =
            hex::decode("5dab087e624a8a4b79e17f8b83800ee66f3bb1292618b6fd1c2f8b27ff88e0eb")
                .unwrap();
        let pk_bob =
            hex::decode("de9edb7d7b7dc1b4d35b61c2ece435373f8343c85b78674dadfc7e146f882b4f")
                .unwrap();
        let shared =
            hex::decode("4a5d9d5ba4ce2de1728e3bf480350f25e07e21c947d19e3376f09b3c1e161742")
                .unwrap();

        // alloc memory for generated keys
        let mut gen_pk_alice = vec![0u8; KEY_LEN];
        let mut gen_pk_bob = vec![0u8; KEY_LEN];
        let mut gen_shared_alice = vec![0u8; KEY_LEN];
        let mut gen_shared_bob = vec![0u8; KEY_LEN];

        // sanity size checks
        assert_eq!(pk_alice.len(), gen_pk_alice.len());
        assert_eq!(pk_bob.len(), gen_pk_bob.len());
        assert_eq!(shared.len(), gen_shared_alice.len());
        assert_eq!(shared.len(), gen_shared_bob.len());

        // call C code
        unsafe {
            X25519_public_from_private(
                &mut (gen_pk_alice[0]) as *mut u8,
                &(sk_alice[0]) as *const u8,
            );
            X25519_public_from_private(&mut (gen_pk_bob[0]) as *mut u8, &(sk_bob[0]) as *const u8);
            let ret_alice = X25519(
                &mut (gen_shared_alice[0]) as *mut u8,
                &(sk_alice[0]) as *const u8,
                &(gen_pk_bob[0]) as *const u8,
            );
            assert_eq!(ret_alice, 1);
            let ret_bob = X25519(
                &mut (gen_shared_bob[0]) as *mut u8,
                &(sk_bob[0]) as *const u8,
                &(gen_pk_alice[0]) as *const u8,
            );
            assert_eq!(ret_bob, 1);
        }

        // test Alice public key
        assert_eq!(pk_alice, gen_pk_alice);

        // test Bob public key
        assert_eq!(pk_bob, gen_pk_bob);

        // test shared, Alice view
        assert_eq!(shared, gen_shared_alice);

        // test shared, Bob view
        assert_eq!(shared, gen_shared_bob);
    }
}
