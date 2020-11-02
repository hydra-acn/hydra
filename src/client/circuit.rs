use log::*;
use rand::seq::SliceRandom;
use rand::Rng;

use std::net::{IpAddr, SocketAddr};

use crate::assert_as_size_err;
use crate::crypto::aes::Aes256Gcm;
use crate::crypto::cprng::thread_cprng;
use crate::crypto::key::{hkdf_sha256, Key};
use crate::crypto::threefish::Threefish2048;
use crate::crypto::{x25519, x448};
use crate::defs::{
    tokens_to_byte_vec, CircuitId, RoundNo, Token, SETUP_AUTH_LEN, SETUP_NONCE_LEN, SETUP_TOKENS,
};
use crate::epoch::EpochNo;
use crate::error::Error;
use crate::net::PacketWithNextHop;
use crate::tonic_directory::MixInfo;
use crate::tonic_mix::SetupPacket;

/// Derive AES and Threefish key from shared secret.
pub fn derive_keys(master_key: &Key, nonce: &[u8]) -> Result<(Key, Key), Error> {
    // 32 byte AES key for the onion-encrypted part of the setup packet
    let aes_info = [42u8];
    let aes_key = hkdf_sha256(&master_key, Some(&nonce), Some(&aes_info), 32)?;
    // 128 byte Threefish-1024 key for circuit cells
    let onion_info = [43u8];
    let onion_key = hkdf_sha256(&master_key, Some(&nonce), Some(&onion_info), 128)?;
    Ok((aes_key, onion_key))
}

/// Client view on circuits.
pub struct Circuit {
    circuit_id: CircuitId,
    first_hop: SocketAddr,
    threefishies: Vec<Threefish2048>,
    tokens: Vec<Token>,
    dummy_tokens: Vec<Token>,
}

impl Circuit {
    /// Create a new circuit for epoch `epoch_no`, using the given `path` and subscribe to the
    /// given `tokens` (filled up by random dummy tokens if necessary).
    /// Returns the circuit context and the setup packet.
    pub fn new(
        epoch_no: EpochNo,
        path: &[MixInfo],
        tokens: Vec<Token>,
    ) -> Result<(Circuit, PacketWithNextHop<SetupPacket>), Error> {
        let first_mix = path
            .first()
            .ok_or_else(|| Error::SizeMismatch("Expected path with length >= 1".to_string()))?;
        let first_hop = first_mix.relay_address().ok_or_else(|| {
            Error::InputError("First mix does not have a valid relay address".to_string())
        })?;

        assert_as_size_err!(
            tokens.len() <= SETUP_TOKENS,
            "Cannot subscribe to {} tokens at once",
            tokens.len()
        );

        let mut rng = thread_cprng();
        let circuit_id = rng.gen();

        struct Hop {
            nonce: Vec<u8>,
            aes: Aes256Gcm,
            pk: Vec<u8>,
            ip: Vec<u8>,
            port: Vec<u8>,
        }

        let mut hops = Vec::new();
        let mut threefishies = Vec::new();

        for mix in path {
            let mix_pk = Key::clone_from_slice(&mix.public_dh);
            let (pk, shared_key) = match mix_pk.len() {
                x25519::KEY_LEN => {
                    let (pk, sk) = x25519::generate_keypair();
                    (pk, x25519::generate_shared_secret(&mix_pk, &sk)?)
                }
                x448::KEY_LEN => {
                    let (pk, sk) = x448::generate_keypair();
                    (pk, x448::generate_shared_secret(&mix_pk, &sk)?)
                }
                _ => {
                    return Err(Error::InputError(
                        "Mix does not have public key with valid length".to_string(),
                    ))
                }
            };
            let mut nonce = vec![0u8; SETUP_NONCE_LEN];
            let mut rng = thread_cprng();
            rng.fill(nonce.as_mut_slice());
            let (aes_key, onion_key) = derive_keys(&shared_key, &nonce)?;
            let aes = Aes256Gcm::new(aes_key);
            let sock_addr = mix.relay_address().ok_or_else(|| {
                Error::InputError("Mix does not have a valid relay address".to_string())
            })?;
            let ip = match sock_addr.ip() {
                IpAddr::V4(v4) => v4.to_ipv6_mapped().octets().to_vec(),
                IpAddr::V6(v6) => v6.octets().to_vec(),
            };
            let port = vec![
                (sock_addr.port() % 256) as u8,
                (sock_addr.port() / 256) as u8,
            ];
            let hop_info = Hop {
                nonce,
                aes,
                pk: pk.clone_to_vec(),
                ip,
                port,
            };
            hops.push(hop_info);
            threefishies.push(Threefish2048::new(onion_key)?);
        }

        // fill with random tokens
        let mut subscribe_to = tokens.clone();
        let mut dummy_tokens = Vec::new();
        while subscribe_to.len() < SETUP_TOKENS {
            let t = rng.gen();
            subscribe_to.push(t);
            dummy_tokens.push(t);
        }

        // random order of tokens
        subscribe_to.shuffle(&mut rng);
        let mut plaintext = tokens_to_byte_vec(&subscribe_to);

        // onion encryption of all but the first layer
        let (first_hop_info, tail_hops) = hops.split_first().expect("Already checked");
        for hop in tail_hops.iter().rev() {
            let mut ciphertext = plaintext.clone();
            let mut auth_tag = vec![0u8; SETUP_AUTH_LEN];
            hop.aes
                .encrypt(&hop.nonce, &plaintext, &mut ciphertext, None, &mut auth_tag)?;
            let layer_combined = vec![
                hop.ip.clone(),
                hop.port.clone(),
                hop.pk.clone(),
                hop.nonce.clone(),
                auth_tag,
                ciphertext,
            ];
            plaintext = layer_combined.into_iter().flatten().collect();
        }

        // last encryption (first hop) can be done in place
        let mut setup_pkt = SetupPacket {
            epoch_no,
            circuit_id,
            public_dh: first_hop_info.pk.clone(),
            nonce: first_hop_info.nonce.clone(),
            auth_tag: vec![0u8; 16],
            onion: plaintext.clone(),
        };

        first_hop_info.aes.encrypt(
            &setup_pkt.nonce,
            &plaintext,
            &mut setup_pkt.onion,
            None,
            &mut setup_pkt.auth_tag,
        )?;

        let circuit = Circuit {
            circuit_id,
            first_hop,
            threefishies,
            tokens,
            dummy_tokens,
        };

        debug!(
            "Created client circuit with id {} and next hop {}",
            circuit_id, circuit.first_hop
        );
        let extend = PacketWithNextHop::new(setup_pkt, circuit.first_hop);
        Ok((circuit, extend))
    }

    /// Client circuit only has an upstream id
    pub fn circuit_id(&self) -> CircuitId {
        self.circuit_id
    }

    pub fn first_hop(&self) -> &SocketAddr {
        &self.first_hop
    }

    pub fn tokens(&self) -> &[Token] {
        &self.tokens
    }

    pub fn dummy_tokens(&self) -> &[Token] {
        &self.dummy_tokens
    }

    pub fn onion_encrypt(&self, round_no: RoundNo, onion: &mut [u8]) -> Result<(), Error> {
        let tweak_src = 24 * round_no as u64;
        for tf in self.threefishies.iter().rev() {
            tf.encrypt(tweak_src, onion)?;
        }
        Ok(())
    }

    pub fn onion_decrypt(&self, round_no: RoundNo, onion: &mut [u8]) -> Result<(), Error> {
        let tweak_src = 24 * round_no as u64 + 12;
        for tf in self.threefishies.iter() {
            tf.decrypt(tweak_src, onion)?;
        }
        Ok(())
    }
}
