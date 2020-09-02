//! Circuit abstraction
use super::grpc::SetupPacketWithPrev;
use super::rendezvous_map::RendezvousMap;
use crate::client::circuit::derive_keys;
use crate::crypto::aes::Aes256Gcm;
use crate::crypto::cprng::thread_cprng;
use crate::crypto::key::Key;
use crate::crypto::threefish::Threefish2048;
use crate::crypto::x448;
use crate::defs::{tokens_from_bytes, CircuitId, RoundNo, Token, ONION_LEN};
use crate::error::Error;
use crate::grpc::type_extensions::CellCmd;
use crate::net::{ip_addr_from_slice, PacketWithNextHop};
use crate::tonic_mix::{Cell, SetupPacket};

use byteorder::{ByteOrder, LittleEndian};
use log::*;
use rand::Rng;
use std::cmp::{self, Ordering};
use std::collections::{BTreeMap, VecDeque};
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, RwLock};

pub type ExtendInfo = PacketWithNextHop<SetupPacket>;

pub enum NextSetupStep {
    Extend(ExtendInfo),
    Rendezvous(Vec<Token>),
}

pub enum NextCellStep {
    Relay(PacketWithNextHop<Cell>),
    Rendezvous(PacketWithNextHop<Cell>),
    Deliver(Cell),
    Wait(Cell),
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
    delayed_cells: RwLock<BTreeMap<RoundNo, Cell>>,
    inject_cells: RwLock<VecDeque<Cell>>,
    max_round_no: RoundNo,
    last_upstream_round_no: RwLock<Option<RoundNo>>,
    last_downstream_round_no: RwLock<Option<RoundNo>>,
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
                warn!(
                    ".. x448_shared_secret = 0x{}",
                    hex::encode(master_key.borrow_raw())
                );
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
            upstream_id: thread_cprng().gen(),
            downstream_hop,
            upstream_hop: None,
            threefish,
            delayed_cells: RwLock::default(),
            inject_cells: RwLock::default(),
            max_round_no,
            last_upstream_round_no: RwLock::default(),
            last_downstream_round_no: RwLock::default(),
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

    /// Returns `None` for duplicates and out-of-sync (too late) cells.
    pub fn process_cell(
        &self,
        mut cell: Cell,
        layer: u32,
        direction: CellDirection,
    ) -> Option<NextCellStep> {
        // out-of-sync detection
        match layer.cmp(&self.layer) {
            Ordering::Equal => (), // in-sync -> continue processing
            Ordering::Less => {
                match direction {
                    CellDirection::Upstream => return Some(NextCellStep::Wait(cell)), // too early
                    CellDirection::Downstream => {
                        warn!("Dropping cell that's too late");
                        return None;
                    }
                }
            }
            Ordering::Greater => {
                match direction {
                    CellDirection::Upstream => {
                        warn!("Dropping cell that's too late");
                        return None;
                    }
                    CellDirection::Downstream => return Some(NextCellStep::Wait(cell)), // too early
                }
            }
        }

        // duplicate detection
        let mut last_round_no_guard = match direction {
            CellDirection::Upstream => self.last_upstream_round_no.write().expect("Lock poisoned"),
            CellDirection::Downstream => self
                .last_downstream_round_no
                .write()
                .expect("Lock poisoned"),
        };

        if let Some(last_round_no) = *last_round_no_guard {
            if cell.round_no <= last_round_no {
                warn!("Dropping duplicate");
                return None;
            }
        }

        *last_round_no_guard = Some(cell.round_no);

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
                if let Some(delayed_cell) = self
                    .delayed_cells
                    .write()
                    .expect("Lock poisoned")
                    .remove(&cell.round_no)
                {
                    cell = delayed_cell;
                }

                // TODO we could skip the check if we use a delayed cell
                // read command
                if let Some(CellCmd::Delay(rounds)) = cell.command() {
                    // we shall delay the cell -> forward dummy instead
                    let dummy = Cell::dummy(cell.circuit_id, cell.round_no);
                    // randomize cmd of original cell
                    thread_cprng().fill(&mut cell.onion[0..8]);
                    self.delayed_cells
                        .write()
                        .expect("Lock poisoned")
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

    /// Use an injected cell or create a dummy cell to pad the circuit if necessary.
    pub fn pad(&self, round_no: RoundNo, direction: CellDirection) -> Option<NextCellStep> {
        let mut last_round_no_guard = match direction {
            CellDirection::Upstream => self.last_upstream_round_no.write().expect("Lock poisoned"),
            CellDirection::Downstream => self
                .last_downstream_round_no
                .write()
                .expect("Lock poisoned"),
        };

        let need_dummy = match *last_round_no_guard {
            Some(last_round_no) => round_no != last_round_no,
            None => true,
        };

        if need_dummy == false {
            return None;
        }

        *last_round_no_guard = Some(round_no);

        let circuit_id = match direction {
            CellDirection::Upstream => self.upstream_id,
            CellDirection::Downstream => self.downstream_id,
        };

        // inject a waiting cell or create a dummy
        let mut cell = match direction {
            CellDirection::Upstream => {
                // no upstream injection -> always dummy
                debug!(
                    "Creating dummy cell for circuit with upstream id {}",
                    self.upstream_id
                );
                Cell::dummy(circuit_id, round_no)
            }
            CellDirection::Downstream => match round_no == self.max_round_no {
                true => self.create_nack(),
                false => match self
                    .inject_cells
                    .write()
                    .expect("Lock poisoned")
                    .pop_front()
                {
                    Some(c) => c,
                    None => {
                        debug!(
                            "Creating dummy cell for circuit with downstream id {}",
                            self.downstream_id
                        );
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
                        warn!("Onion decryption failed, randomizing instead: {}", e);
                        cell.randomize();
                    }
                }
            }
            CellDirection::Downstream => {
                let tweak_src = 24 * cell.round_no as u64 + 12;
                match self.threefish.encrypt(tweak_src, &mut cell.onion) {
                    Ok(()) => (),
                    Err(e) => {
                        warn!("Onion encryption failed, randomizing instead: {}", e);
                        cell.randomize();
                    }
                }
            }
        }
    }

    pub fn inject(&self, mut cell: Cell) {
        cell.circuit_id = self.downstream_id;
        self.inject_cells
            .write()
            .expect("Lock poisoned")
            .push_back(cell);
    }

    fn create_nack(&self) -> Cell {
        let mut cell = Cell {
            circuit_id: self.downstream_id,
            round_no: self.max_round_no,
            onion: vec![0u8; ONION_LEN],
        };
        let inject_cells_guard = self.inject_cells.read().expect("Lock poisoned");
        let n = cmp::min((ONION_LEN - 8) / 8, inject_cells_guard.len()) as u8;
        cell.onion[0] = n;
        let mut i = 8;
        for dropped_cell in inject_cells_guard.iter() {
            match cell.onion.get_mut(i..i + 8) {
                Some(buf) => LittleEndian::write_u64(buf, dropped_cell.token()),
                None => {
                    warn!(
                        "Have to drop {} cells -> NACK not big enough",
                        inject_cells_guard.len()
                    );
                    break;
                }
            }
            i += 8;
        }
        cell
    }
}
