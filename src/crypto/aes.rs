//! AES specific crypto
use openssl::symm;

use crate::crypto::key::Key;
use crate::error::Error;

/// AES-256-GCM context (static key)
pub struct Aes256Gcm {
    key: Key,
}

impl Aes256Gcm {
    pub fn new(key: Key) -> Self {
        Aes256Gcm { key }
    }

    pub fn encrypt(
        &self,
        iv: &[u8],
        plaintext: &[u8],
        ciphertext: &mut [u8],
        auth_data: Option<&[u8]>,
        tag: &mut [u8],
    ) -> Result<(), Error> {
        let aad = match auth_data {
            Some(data) => data,
            None => &[],
        };
        // TODO this is by far not the most efficient implementation
        let ct = symm::encrypt_aead(
            symm::Cipher::aes_256_gcm(),
            self.key.borrow_raw(),
            Some(iv),
            aad,
            plaintext,
            tag,
        )?;
        if ciphertext.len() != ct.len() {
            return Err(Error::SizeMismatch(
                "Ciphertext does not have the expected size".to_string(),
            ));
        }
        ciphertext.clone_from_slice(&ct);
        Ok(())
    }

    pub fn decrypt(
        &self,
        iv: &[u8],
        ciphertext: &[u8],
        plaintext: &mut [u8],
        auth_data: Option<&[u8]>,
        tag: &[u8],
    ) -> Result<(), Error> {
        let aad = match auth_data {
            Some(data) => data,
            None => &[],
        };
        // TODO this is by far not the most efficient implementation
        let pt = symm::decrypt_aead(
            symm::Cipher::aes_256_gcm(),
            self.key.borrow_raw(),
            Some(iv),
            aad,
            ciphertext,
            tag,
        )?;
        if plaintext.len() != pt.len() {
            return Err(Error::SizeMismatch(
                "Plaintext does not have the expected size".to_string(),
            ));
        }
        plaintext.clone_from_slice(&pt);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    // arbitrary test from wycheproof project
    fn test_vector() {
        let key =
            Key::from_hex_str("92ace3e348cd821092cd921aa3546374299ab46209691bc28b8752d17f123c20")
                .unwrap();
        let iv = hex::decode("00112233445566778899aabb").unwrap();
        let aad = hex::decode("00000000ffffffff").unwrap();
        let data = hex::decode("00010203040506070809").unwrap();
        let ct_expected = hex::decode("e27abdd2d2a53d2f136b").unwrap();
        let tag_expected = hex::decode("9a4a2579529301bcfb71c78d4060f52c").unwrap();

        let mut ct_buf = vec![0; data.len()];
        let mut pt_buf = vec![0; data.len()];
        let mut tag_buf = vec![0; tag_expected.len()];

        let ctx = Aes256Gcm::new(key);

        // test encrypt
        ctx.encrypt(&iv, &data, &mut ct_buf, Some(&aad), &mut tag_buf)
            .unwrap();
        assert_eq!(ct_expected, ct_buf);

        // test decrypt
        ctx.decrypt(&iv, &ct_buf, &mut pt_buf, Some(&aad), &tag_buf)
            .unwrap();
        assert_eq!(data, pt_buf);

        // test failing authentication (wrong tag)
        tag_buf[0] += 1;
        ctx.decrypt(&iv, &ct_buf, &mut pt_buf, Some(&aad), &tag_buf).unwrap_err();

        // test failing authentication (wrong ciphertext)
        tag_buf[0] -= 1;
        ct_buf[5] -= 1;
        ctx.decrypt(&iv, &ct_buf, &mut pt_buf, Some(&aad), &tag_buf).unwrap_err();
    }
}
