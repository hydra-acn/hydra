//! Circuit abstraction
use super::directory_client::RendezvousMap;
use super::grpc::SetupPacketWithPrev;
use super::sender::PacketWithNextHop;
use crate::crypto::aes::Aes256Gcm;
use crate::crypto::key::{hkdf_sha256, Key};
use crate::crypto::threefish::Threefish2048;
use crate::crypto::x448;
use crate::defs::{tokens_from_bytes, CellCmd, CircuitId, RoundNo, Token, ONION_LEN};
use crate::epoch::EpochNo;
use crate::error::Error;
use crate::net::ip_addr_from_slice;
use crate::tonic_directory::MixInfo;
use crate::tonic_mix::*;

use byteorder::{ByteOrder, LittleEndian};
use log::*;
use openssl::rand::rand_bytes;
use rand::seq::IteratorRandom;
use std::cmp;
use std::collections::{BTreeMap, VecDeque};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

pub type ExtendInfo = PacketWithNextHop<SetupPacket>;

pub enum NextSetupStep {
    Extend(ExtendInfo),
    Rendezvous(Vec<Token>),
}

pub enum NextCellStep {
    Relay(PacketWithNextHop<Cell>),
    Rendezvous(PacketWithNextHop<Cell>),
    Deliver(Cell),
}

#[derive(Copy, Clone, Debug)]
pub enum CellDirection {
    Upstream,
    Downstream,
}

pub struct Circuit {
    rendezvous_map: Arc<RendezvousMap>,
    layer: u32,
    downstream_id: CircuitId,
    upstream_id: CircuitId,
    // None for first layer
    downstream_hop: Option<SocketAddr>,
    // None for last layer
    upstream_hop: Option<SocketAddr>,
    threefish: Threefish2048,
    delayed_cells: BTreeMap<RoundNo, Cell>,
    inject_cells: VecDeque<Cell>,
    max_round_no: RoundNo,
    last_upstream_round_no: Option<RoundNo>,
    last_downstream_round_no: Option<RoundNo>,
}

impl Circuit {
    /// Creates the circuit (if everything is ok). Furthermore, it either returns the next setup
    /// packet (with destination) or the set of tokens to subscribe to (last layer)
    pub fn new(
        pkt: SetupPacketWithPrev,
        ephemeral_sk: &Key,
        rendezvous_map: Arc<RendezvousMap>,
        layer: u32,
        max_round_no: RoundNo,
    ) -> Result<(Self, NextSetupStep), Error> {
        if rendezvous_map.is_empty() {
            return Err(Error::InputError(
                "We need rendezvous nodes to work correctly".to_string(),
            ));
        }
        let downstream_hop = pkt.previous_hop();
        if downstream_hop.is_none() && layer > 0 {
            return Err(Error::InputError(
                "Expected downstream hop information".to_string(),
            ));
        }
        let setup_pkt = pkt.into_inner();
        let client_pk = Key::clone_from_slice(&setup_pkt.public_dh);
        let master_key = x448::generate_shared_secret(&client_pk, ephemeral_sk)?;
        let nonce = &setup_pkt.nonce;
        let (aes_key, onion_key) = derive_keys(&master_key, &nonce)?;
        let threefish = Threefish2048::new(onion_key)?;

        // decrypt onion part
        let mut decrypted = vec![0u8; setup_pkt.onion.len()];
        let aes = Aes256Gcm::new(aes_key);
        match aes.decrypt(
            nonce,
            &setup_pkt.onion,
            &mut decrypted,
            None,
            &setup_pkt.auth_tag,
        ) {
            Ok(_) => (),
            Err(e) => {
                warn!("Decryption of setup packet failed: {}", e);
                warn!(".. client_pk = 0x{}", hex::encode(client_pk.borrow_raw()));
                warn!(".. x448_shared_secret = 0x{}", hex::encode(master_key.borrow_raw()));
                warn!(".. aes_key = 0x{}", hex::encode(aes.key().borrow_raw()));
                warn!(".. nonce = 0x{}", hex::encode(nonce));
                warn!(".. auth_tag = 0x{}", hex::encode(&setup_pkt.auth_tag));
                return Err(e);
            }
        }

        let ttl = setup_pkt
            .ttl()
            .ok_or_else(|| Error::InputError("Should have been filtered by gRPC".to_string()))?;

        let mut circuit = Circuit {
            rendezvous_map,
            layer,
            downstream_id: setup_pkt.circuit_id,
            // TODO security: better use cPRNG?
            upstream_id: rand::random(),
            downstream_hop,
            upstream_hop: None,
            threefish,
            delayed_cells: BTreeMap::new(),
            inject_cells: VecDeque::new(),
            max_round_no,
            last_upstream_round_no: None,
            last_downstream_round_no: None,
        };

        if ttl == 0 {
            // time for rendezvous
            let tokens = tokens_from_bytes(&decrypted);
            // TODO security: this should not be logged in production ...
            debug!(
                "Created relay circuit with downstream id {} and upstream id {} in layer {} (last layer)",
                circuit.downstream_id, circuit.upstream_id, circuit.layer
            );
            Ok((circuit, NextSetupStep::Rendezvous(tokens)))
        } else {
            // show must go on
            let v6 = match ip_addr_from_slice(&decrypted[0..16])? {
                IpAddr::V6(v6) => v6,
                _ => panic!("Why should this not be an v6 address?"),
            };
            let port = decrypted[16] as u16 + 256 * decrypted[17] as u16;
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
            let extend_info = ExtendInfo::new(next_setup_pkt, upstream_hop);

            // TODO security: this should not be logged in production ...
            debug!(
                "Created relay circuit with downstream id {} and upstream id {} in layer {}",
                circuit.downstream_id, circuit.upstream_id, circuit.layer
            );
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

    pub fn layer(&self) -> u32 {
        self.layer
    }

    /// Returns `None` for duplicates and out-of-sync cells.
    pub fn process_cell(
        &mut self,
        mut cell: Cell,
        layer: u32,
        direction: CellDirection,
    ) -> Option<NextCellStep> {
        if layer != self.layer {
            warn!(
                "Layer mismatch in circuit with downstream id {}: expected {}, got {} -> most likely out-of-sync sending",
                self.downstream_id, self.layer, layer
            );
            return None;
        }

        // duplicate detection
        let maybe_last_round_no: &mut Option<RoundNo> = match direction {
            CellDirection::Upstream => &mut self.last_upstream_round_no,
            CellDirection::Downstream => &mut self.last_downstream_round_no,
        };

        if let Some(last_round_no) = maybe_last_round_no {
            if cell.round_no <= *last_round_no {
                warn!("Dropping duplicate");
                return None;
            }
        }

        *maybe_last_round_no = Some(cell.round_no);

        // re-write circuit id
        let next_circuit_id = match direction {
            CellDirection::Upstream => self.upstream_id,
            CellDirection::Downstream => self.downstream_id,
        };
        cell.circuit_id = next_circuit_id;

        // onion!
        self.handle_onion(&mut cell, direction);

        // command handling and forwarding
        match direction {
            CellDirection::Upstream => {
                // check if we have a delayed cell
                if let Some(delayed_cell) = self.delayed_cells.remove(&cell.round_no) {
                    cell = delayed_cell;
                }

                // TODO we could skip the check if we use a delayed cell
                // read command
                if let Some(CellCmd::Delay(rounds)) = cell.command() {
                    // we shall delay the cell -> forward dummy instead
                    let dummy = Cell::dummy(cell.circuit_id, cell.round_no);
                    // randomize cmd of original cell
                    rand_bytes(&mut cell.onion[0..8]).expect("Cell randomization failed");
                    self.delayed_cells
                        .insert(cell.round_no + rounds as u32, cell);
                    cell = dummy;
                }

                match self.upstream_hop {
                    Some(hop) => Some(NextCellStep::Relay(PacketWithNextHop::new(cell, hop))),
                    None => {
                        let rendezvous_addr = self
                            .rendezvous_map
                            .rendezvous_address(&cell.token())
                            .expect("Checked this at circuit creation");
                        Some(NextCellStep::Rendezvous(PacketWithNextHop::new(
                            cell,
                            rendezvous_addr,
                        )))
                    }
                }
            }
            CellDirection::Downstream => match self.downstream_hop {
                Some(hop) => Some(NextCellStep::Relay(PacketWithNextHop::new(cell, hop))),
                None => Some(NextCellStep::Deliver(cell)),
            },
        }
    }

    /// Use an injected cell or create a dummy cell to pad the circuit.
    pub fn pad(&mut self, round_no: RoundNo, direction: CellDirection) -> Option<NextCellStep> {
        let maybe_last_round_no: &mut Option<RoundNo> = match direction {
            CellDirection::Upstream => &mut self.last_upstream_round_no,
            CellDirection::Downstream => &mut self.last_downstream_round_no,
        };

        let need_dummy = match *maybe_last_round_no {
            Some(last_round_no) => round_no != last_round_no,
            None => true,
        };

        if need_dummy == false {
            return None;
        }

        *maybe_last_round_no = Some(round_no);

        let circuit_id = match direction {
            CellDirection::Upstream => self.upstream_id,
            CellDirection::Downstream => self.downstream_id,
        };

        // inject a waiting cell or create a dummy
        let mut cell = match direction {
            CellDirection::Upstream => {
                // no upstream injection -> always dummy
                debug!("Creating dummy cell for circuit with upstream id {}", self.upstream_id);
                Cell::dummy(circuit_id, round_no)
            }
            CellDirection::Downstream => match round_no == self.max_round_no {
                true => self.create_nack(),
                false => match self.inject_cells.pop_front() {
                    Some(c) => c,
                    None => {
                        debug!("Creating dummy cell for circuit with downstream id {}", self.downstream_id);
                        Cell::dummy(circuit_id, round_no)
                    }
                },
            },
        };

        // onion handling
        cell.round_no = round_no;
        self.handle_onion(&mut cell, direction);

        // forwarding
        match direction {
            CellDirection::Upstream => match self.upstream_hop {
                Some(hop) => Some(NextCellStep::Relay(PacketWithNextHop::new(cell, hop))),
                None => {
                    let rendezvous_addr = self
                        .rendezvous_map
                        .rendezvous_address(&cell.token())
                        .expect("Checked at circuit setup");
                    Some(NextCellStep::Rendezvous(PacketWithNextHop::new(
                        cell,
                        rendezvous_addr,
                    )))
                }
            },
            CellDirection::Downstream => match self.downstream_hop {
                Some(hop) => Some(NextCellStep::Relay(PacketWithNextHop::new(cell, hop))),
                None => Some(NextCellStep::Deliver(cell)),
            },
        }
    }

    /// Onion encryption/decryption.
    fn handle_onion(&self, cell: &mut Cell, direction: CellDirection) {
        match direction {
            CellDirection::Upstream => {
                let tweak_src = 24 * cell.round_no as u64;
                match self.threefish.decrypt(tweak_src, &mut cell.onion) {
                    Ok(()) => (),
                    Err(e) => {
                        warn!("Onion decryption failed: {}", e);
                        rand_bytes(&mut cell.onion).expect("Failed to generate dummy");
                    }
                }
            }
            CellDirection::Downstream => {
                let tweak_src = 24 * cell.round_no as u64 + 12;
                match self.threefish.encrypt(tweak_src, &mut cell.onion) {
                    Ok(()) => (),
                    Err(e) => {
                        warn!("Onion encryption failed: {}", e);
                        rand_bytes(&mut cell.onion).expect("Failed to generate dummy");
                    }
                }
            }
        }
    }

    pub fn inject(&mut self, mut cell: Cell) {
        cell.circuit_id = self.downstream_id;
        self.inject_cells.push_back(cell);
    }

    fn create_nack(&self) -> Cell {
        let mut cell = Cell {
            circuit_id: self.downstream_id,
            round_no: self.max_round_no,
            onion: vec![0u8; ONION_LEN],
        };
        let n = cmp::min((ONION_LEN - 8) / 8, self.inject_cells.len()) as u8;
        cell.onion[0] = n;
        let mut i = 8;
        for dropped_cell in self.inject_cells.iter() {
            match cell.onion.get_mut(i..i + 8) {
                Some(buf) => LittleEndian::write_u64(buf, dropped_cell.token()),
                None => warn!("Nack is not big enough"),
            }
            i += 8;
        }
        cell
    }
}

/// When mixes act as clients for additional cover traffic.
pub struct ClientCircuit {
    layer: u32,
    circuit_id: CircuitId,
    first_hop: SocketAddr,
    threefishies: Vec<Threefish2048>,
    tokens: Vec<Token>,
    sent_cell: Option<Cell>,
}

impl ClientCircuit {
    pub fn new(
        epoch_no: EpochNo,
        layer: u32,
        path: Vec<MixInfo>,
    ) -> Result<(ClientCircuit, PacketWithNextHop<SetupPacket>), Error> {
        let first_mix = path
            .first()
            .ok_or_else(|| Error::SizeMismatch("Expected path with length >= 1".to_string()))?;
        let first_hop = first_mix.relay_address().ok_or_else(|| {
            Error::InputError("First mix does not have a valid relay address".to_string())
        })?;

        // TODO security: better use cPRNG?
        let circuit_id = rand::random();

        struct Hop {
            nonce: Vec<u8>,
            aes: Aes256Gcm,
            pk: Vec<u8>,
            ip: Vec<u8>,
            port: Vec<u8>,
        }

        let mut hops = Vec::new();
        let mut threefishies = Vec::new();

        for mix in &path {
            let mix_pk = Key::clone_from_slice(&mix.public_dh);
            let (pk, sk) = x448::generate_keypair()?;
            let shared_key = x448::generate_shared_secret(&mix_pk, &sk)?;
            let mut nonce = vec![0u8; 12];
            rand_bytes(&mut nonce)?;
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

        let tokens_size = 256 * 8;
        let onion_size = 102 * (path.len() - 1) + tokens_size;

        // subscribe to random tokens
        let mut plaintext = vec![0u8; tokens_size];
        rand_bytes(&mut plaintext)?;
        let tokens = tokens_from_bytes(&plaintext);

        // onion encryption of all but the first layer
        let (first_hop_info, tail_hops) = hops.split_first().expect("Already checked");
        for hop in tail_hops.iter().rev() {
            let mut ciphertext = plaintext.clone();
            let mut auth_tag = vec![0u8; 16];
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

        assert_eq!(plaintext.len(), onion_size);

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

        let circuit = ClientCircuit {
            layer,
            circuit_id,
            first_hop,
            threefishies,
            tokens,
            sent_cell: None,
        };

        debug!(
            "Created client circuit with id {} and next hop {}",
            circuit_id, circuit.first_hop
        );
        let extend = ExtendInfo::new(setup_pkt, circuit.first_hop);
        Ok((circuit, extend))
    }

    /// Client circuit only has an upstream id
    pub fn circuit_id(&self) -> CircuitId {
        self.circuit_id
    }

    pub fn layer(&self) -> u32 {
        self.layer
    }

    pub fn pad(&mut self, round_no: RoundNo) -> Option<PacketWithNextHop<Cell>> {
        // TODO security: for now, client circuits are only used to monitor correct behavior of the
        // system by forcing the system to echo them back on the same circuit; to provide security
        // benefits, cells should be send "end-to-end" between *different* client circuits on the
        // same mix, with random payload; nevertheless, this should only happen occassionally -
        // otherwise rendezvous nodes find out that a circuit never has dummy circuits -> return
        // `None` in this case
        let mut cell = Cell::dummy(self.circuit_id, round_no);
        // TODO security: use secure random source?
        let rng = &mut rand::thread_rng();
        // TODO security: use some Zipf-like distribution
        let token = self
            .tokens
            .iter()
            .choose(rng)
            .expect("Expected at least one token");
        cell.set_token(*token);
        // for now, we want the cell echoed back by the rendezvous service
        cell.set_command(CellCmd::Broadcast);
        if self.sent_cell.is_some() {
            warn!(
                "Seems like we did not get the last cell back on client circuit {}",
                self.circuit_id
            )
        }
        self.sent_cell = Some(cell.clone());
        // onion encrypt
        let tweak_src = 24 * round_no as u64;
        for tf in self.threefishies.iter().rev() {
            // as we started with a dummy, we can ignore encryption errors
            tf.encrypt(tweak_src, &mut cell.onion).unwrap_or(());
        }
        Some(PacketWithNextHop::new(cell, self.first_hop))
    }

    pub fn receive_cell(&mut self, mut cell: Cell) {
        debug!("Client circuit {} received a cell", self.circuit_id);
        // onion decrypt
        let tweak_src = 24 * cell.round_no as u64 + 12;
        for tf in self.threefishies.iter() {
            tf.decrypt(tweak_src, &mut cell.onion).unwrap_or(());
        }
        match &self.sent_cell {
            Some(c) if *c != cell => {
                // in last round: bytes 1 to 7 (cmd) should be zero
                if cell.onion[1..8].iter().any(|b| *b != 0) {
                    warn!("Received cell is not as expected")
                }
            }
            None => warn!("Received a cell without having send one before"),
            _ => (), // everything ok
        }
        self.sent_cell = None;
    }
}

/// Derive AES and Threefish keys from shared secret.
fn derive_keys(master_key: &Key, nonce: &[u8]) -> Result<(Key, Key), Error> {
    // 32 byte AES key for the onion-encrypted part of the setup packet
    let aes_info = [42u8];
    let aes_key = hkdf_sha256(&master_key, Some(&nonce), Some(&aes_info), 32)?;
    // 128 byte Threefish-1024 key for circuit cells
    let onion_info = [43u8];
    let onion_key = hkdf_sha256(&master_key, Some(&nonce), Some(&onion_info), 128)?;
    Ok((aes_key, onion_key))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tonic_directory::EpochInfo;

    #[test]
    fn key_derivation() {
        let master_key =
            Key::from_hex_str("775f84edb8bf10bb747765f2582c87f4a1e4463f275f38ce447a5885").unwrap();
        let nonce = hex::decode("903a73a912df").unwrap();
        let (aes_key, onion_key) = derive_keys(&master_key, &nonce).unwrap();
        let aes_expected =
            Key::from_hex_str("ac5e5ae356e4f943574ee7cefadb091b17eec79d642fcafcd8679f8c110cc51f")
                .unwrap();
        let onion_expected = Key::from_hex_str("30755ecd02757afb390d28ae2eb5bc4e6015e95c835a998cc74551e8a8a183fac722852edf51c1a82b0b9f068c085ad6c17233ef20730e710e862232cb8675696c140a5ee306a816df06f99b1cf639baa93a7d15fbe0be7e4c10afaeea26d77f6b656808d756df0c0f978610faa8c35597e49e04f4f1b85225bff654b69ee06e").unwrap();
        assert_eq!(aes_key, aes_expected);
        assert_eq!(onion_key, onion_expected);
    }

    #[test]
    fn setup_onion() {
        // deterministic test (only when not run in parallel with other tests)
        crate::crypto::activate_fake_rand(1337);

        let mixes: Vec<(MixInfo, Key)> = [1, 2, 3].iter().map(|i| create_mix_info(*i)).collect();
        let path: Vec<MixInfo> = mixes.iter().map(|(info, _)| info.clone()).collect();
        let endpoints: Vec<SocketAddr> = path
            .iter()
            .map(|info| {
                SocketAddr::new(
                    ip_addr_from_slice(&info.address).unwrap(),
                    info.relay_port as u16,
                )
            })
            .collect();
        let mut epoch_info = EpochInfo::default();
        epoch_info.mixes = path.clone();
        let rendezvous_map = Arc::new(RendezvousMap::new(&epoch_info));
        let (client_circuit, extend) = ClientCircuit::new(42, 0, path.clone()).unwrap();
        assert_eq!(*extend.next_hop(), endpoints[0]);
        let setup_pkt = extend.into_inner();

        // first mix
        assert_eq!(setup_pkt.ttl().unwrap(), 2);
        let previous_hop = Some("8.8.8.8:42".parse().unwrap()); // previous hop is client
        let pkt_with_prev = SetupPacketWithPrev::new(setup_pkt, previous_hop);
        let (circuit, next_step) =
            Circuit::new(pkt_with_prev, &mixes[0].1, rendezvous_map.clone(), 0, 41).unwrap();
        assert_eq!(
            *client_circuit.threefishies[0].key(),
            *circuit.threefish.key()
        );
        let extend = match next_step {
            NextSetupStep::Extend(e) => e,
            _ => unreachable!(),
        };
        assert_eq!(*extend.next_hop(), endpoints[1]);
        let setup_pkt = extend.into_inner();

        // second mix
        assert_eq!(setup_pkt.ttl().unwrap(), 1);
        let pkt_with_prev = SetupPacketWithPrev::new(setup_pkt, Some(endpoints[0].clone()));
        let (circuit, next_step) =
            Circuit::new(pkt_with_prev, &mixes[1].1, rendezvous_map.clone(), 1, 41).unwrap();
        assert_eq!(
            *client_circuit.threefishies[1].key(),
            *circuit.threefish.key()
        );
        let extend = match next_step {
            NextSetupStep::Extend(e) => e,
            _ => unreachable!(),
        };
        assert_eq!(*extend.next_hop(), endpoints[2]);
        let setup_pkt = extend.into_inner();

        // third mix (last one)
        assert_eq!(setup_pkt.ttl().unwrap(), 0);
        let pkt_with_prev = SetupPacketWithPrev::new(setup_pkt, Some(endpoints[1].clone()));
        let (circuit, next_step) =
            Circuit::new(pkt_with_prev, &mixes[2].1, rendezvous_map.clone(), 2, 41).unwrap();
        assert_eq!(
            *client_circuit.threefishies[2].key(),
            *circuit.threefish.key()
        );
        let tokens = match next_step {
            NextSetupStep::Rendezvous(ts) => ts,
            _ => unreachable!(),
        };
        assert_eq!(tokens, client_circuit.tokens);
    }

    fn create_mix_info(index: u8) -> (MixInfo, Key) {
        let (pk, sk) = x448::generate_keypair().unwrap();
        let info = MixInfo {
            address: vec![127, 0, 0, index],
            entry_port: 9001,
            relay_port: 9002,
            rendezvous_port: 9003,
            fingerprint: format!("mix-{}", index),
            public_dh: pk.clone_to_vec(),
        };
        (info, sk)
    }
}
