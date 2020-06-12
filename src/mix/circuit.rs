//! Circuit abstraction
use super::grpc::SetupPacketWithPrev;
use super::sender::PacketWithNextHop;
use crate::crypto::aes::Aes256Gcm;
use crate::crypto::key::{hkdf_sha256, Key};
use crate::crypto::x448::generate_shared_secret;
use crate::defs::{CircuitId, Token};
use crate::error::Error;
use crate::net::ip_addr_from_slice;
use crate::tonic_mix::*;

use std::net::{IpAddr, SocketAddr};

pub struct Circuit {
    layer: u32,
    downstream_id: CircuitId,
    upstream_id: CircuitId,
    // None for first layer
    downstream_hop: Option<SocketAddr>,
    // None for last layer
    upstream_hop: Option<SocketAddr>,
    onion_key: Key,
}

type ExtendInfo = PacketWithNextHop<SetupPacket>;

pub enum NextSetupStep {
    Extend(ExtendInfo),
    Rendezvous(Vec<Token>),
}

impl Circuit {
    /// Creates the circuit (if everything is ok). Furthermore, it either returns the next setup
    /// packet (with destination) or the set of tokens to subscribe to (last layer)
    pub fn new(
        pkt: SetupPacketWithPrev,
        ephemeral_sk: &Key,
        layer: u32,
    ) -> Result<(Self, NextSetupStep), Error> {
        let downstream_hop = pkt.previous_hop();
        if downstream_hop.is_none() && layer > 0 {
            return Err(Error::InputError(
                "Expected downstream hop information".to_string(),
            ));
        }
        let setup_pkt = pkt.into_inner();
        let client_pk = Key::clone_from_slice(&setup_pkt.public_dh);
        let master_key = generate_shared_secret(&client_pk, ephemeral_sk)?;
        let nonce = setup_pkt.nonce.clone();
        // 32 byte AES key for the onion-encrypted part of the setup packet
        let aes_info = [42u8];
        let aes_key = hkdf_sha256(&master_key, Some(&nonce), Some(&aes_info), 32)?;
        // 128 byte Threefish-1024 key for circuit cells
        let onion_info = [43u8];
        let onion_key = hkdf_sha256(&master_key, Some(&nonce), Some(&onion_info), 128)?;

        // decrypt onion part
        let mut decrypted = vec![0u8; setup_pkt.onion.len()];
        Aes256Gcm::new(aes_key).decrypt(
            &nonce,
            &setup_pkt.onion,
            &mut decrypted,
            None,
            &setup_pkt.auth_tag,
        )?;

        let ttl = setup_pkt
            .ttl()
            .ok_or_else(|| Error::InputError("Should have been filtered by gRPC".to_string()))?;

        let mut circuit = Circuit {
            layer,
            downstream_id: setup_pkt.circuit_id,
            upstream_id: rand::random(),
            downstream_hop,
            upstream_hop: None,
            onion_key,
        };

        if ttl == 0 {
            // time for rendezvous
            // XXX use function by jbloss to extract the real tokens
            let tokens = Vec::new();
            Ok((circuit, NextSetupStep::Rendezvous(tokens)))
        } else {
            // show must go on
            let v6 = match ip_addr_from_slice(&setup_pkt.onion[0..16])? {
                IpAddr::V6(v6) => v6,
                _ => panic!("Why should this not be an v6 address?"),
            };
            let port = setup_pkt.onion[16] as u16 + 16 * setup_pkt.onion[17] as u16;
            let upstream_hop = match v6.to_ipv4() {
                Some(v4) => SocketAddr::new(IpAddr::V4(v4), port),
                None => SocketAddr::new(IpAddr::V6(v6), port),
            };
            circuit.upstream_hop = Some(upstream_hop);
            let next_setup_pkt = SetupPacket {
                epoch_no: setup_pkt.epoch_no,
                circuit_id: circuit.upstream_id,
                public_dh: decrypted[18..74].to_vec(),
                nonce: decrypted[74..86].to_vec(),
                auth_tag: decrypted[86..102].to_vec(),
                onion: decrypted[102..].to_vec(),
            };
            let extend_info = ExtendInfo {
                inner: next_setup_pkt,
                next_hop: upstream_hop,
            };
            Ok((circuit, NextSetupStep::Extend(extend_info)))
        }
    }

    /// circuit id used on the link towards the client (upstream rx, downstream tx)
    pub fn downstream_id(&self) -> CircuitId {
        self.downstream_id
    }

    /// circuit id used on the link towards the rendezvous node (upstream tx, downstream rx)
    pub fn upstream_id(&self) -> CircuitId {
        self.upstream_id
    }
}
