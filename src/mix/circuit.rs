//! Circuit abstraction
use crate::client::circuit::derive_keys;
use crate::crypto::aes::Aes256Gcm;
use crate::crypto::cprng::thread_cprng;
use crate::crypto::key::Key;
use crate::crypto::threefish::Threefish2048;
use crate::crypto::{x25519, x448};
use crate::defs::{
    tokens_from_bytes, CircuitId, RoundNo, Token, CELL_LEN, INJECT_ROUND_NO, ONION_LEN,
    PUBLISH_ROUND_NO, SETUP_ADDR_LEN, SETUP_AUTH_LEN, SETUP_NONCE_LEN,
};
use crate::error::Error;
use crate::grpc::type_extensions::SetupPacketWithPrev;
use crate::net::cell::{Cell, CellCmd};
use crate::net::{ip_addr_from_slice, PacketWithNextHop};
use crate::tonic_mix::SetupPacket;

use super::rendezvous_map::RendezvousMap;
use super::sub_collector::SubCollector;

use byteorder::{ByteOrder, LittleEndian};
use log::*;
use rand::Rng;
use std::cmp::{self, Ordering};
use std::collections::{BTreeMap, VecDeque};
use std::convert::TryInto;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
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
    Drop,
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
    inject_queue: RwLock<VecDeque<Cell>>,
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
        let downstream_hop = pkt.previous_hop();
        if downstream_hop.is_none() && layer > 0 {
            return Err(Error::InputError(
                "Expected downstream hop information".to_string(),
            ));
        }
        let setup_pkt = pkt.into_inner();
        let client_pk = Key::clone_from_slice(&setup_pkt.public_dh);
        let master_key = match ephemeral_sk.len() {
            x25519::KEY_LEN => x25519::generate_shared_secret(&client_pk, ephemeral_sk)?,
            x448::KEY_LEN => x448::generate_shared_secret(&client_pk, ephemeral_sk)?,
            _ => {
                return Err(Error::InputError(
                    "Our ephemeral sk has a strange size".to_string(),
                ))
            }
        };
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
                    ".. shared_dh_secret = 0x{}",
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

        let upstream_id = match ttl {
            0 => {
                // rendezvous only uses 32bit of the circuit id
                let small_id: u32 = thread_cprng().gen();
                small_id.into()
            }
            _ => thread_cprng().gen(),
        };

        let mut circuit = Circuit {
            rendezvous_map,
            layer,
            downstream_id: setup_pkt.circuit_id,
            upstream_id,
            downstream_hop,
            upstream_hop: None,
            threefish,
            delayed_cells: RwLock::default(),
            inject_queue: RwLock::default(),
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
            let mut port = decrypted[16] as u16 + 256 * decrypted[17] as u16;
            let next_setup_hop = match v6.to_ipv4() {
                Some(mut v4) => {
                    // we have to undo NAT for forwarding inside the testbed
                    let nat_addr = Ipv4Addr::new(141, 24, 207, 69);
                    if v4 == nat_addr {
                        let mix_idx = (port - 9000) as u8;
                        v4 = Ipv4Addr::new(10, 0, 0, mix_idx);
                        port = 9000;
                    }
                    SocketAddr::new(IpAddr::V4(v4), port)
                }
                None => SocketAddr::new(IpAddr::V6(v6), port),
            };
            let mut upstream_hop = next_setup_hop.clone();
            // TODO code don't hardcode +200
            upstream_hop.set_port(port + 200);
            circuit.upstream_hop = Some(upstream_hop);
            let mut offset = SETUP_ADDR_LEN;
            let next_pk = &decrypted[offset..offset + ephemeral_sk.len()];
            offset += ephemeral_sk.len();
            let next_nonce = &decrypted[offset..offset + SETUP_NONCE_LEN];
            offset += SETUP_NONCE_LEN;
            let next_tag = &decrypted[offset..offset + SETUP_AUTH_LEN];
            offset += SETUP_AUTH_LEN;
            let next_onion = &decrypted[offset..];
            let next_setup_pkt = SetupPacket {
                epoch_no: setup_pkt.epoch_no,
                circuit_id: circuit.upstream_id,
                public_dh: next_pk.to_vec(),
                nonce: next_nonce.to_vec(),
                auth_tag: next_tag.to_vec(),
                onion: next_onion.to_vec(),
            };
            let extend_info = ExtendInfo::new(next_setup_pkt, next_setup_hop);

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

    pub fn is_exit(&self) -> bool {
        match self.upstream_hop {
            Some(_) => false,
            None => true,
        }
    }

    pub fn process_cell(
        &self,
        mut cell: Cell,
        round_no: RoundNo,
        layer: u32,
        direction: CellDirection,
        sub_collector: &SubCollector,
    ) -> NextCellStep {
        // out-of-sync detection
        match layer.cmp(&self.layer) {
            Ordering::Equal => (), // in-sync -> continue processing
            Ordering::Less => {
                match direction {
                    CellDirection::Upstream => return NextCellStep::Wait(cell), // too early
                    CellDirection::Downstream => {
                        debug!("Dropping cell that's too late");
                        return NextCellStep::Drop;
                    }
                }
            }
            Ordering::Greater => {
                match direction {
                    CellDirection::Upstream => {
                        debug!("Dropping cell that's too late");
                        return NextCellStep::Drop;
                    }
                    CellDirection::Downstream => return NextCellStep::Wait(cell), // too early
                }
            }
        }

        // re-write circuit id
        let next_circuit_id = match direction {
            CellDirection::Upstream => self.upstream_id,
            CellDirection::Downstream => self.downstream_id,
        };
        cell.set_circuit_id(next_circuit_id);

        // duplicate detection
        let mut last_round_no_guard = match direction {
            CellDirection::Upstream => self.last_upstream_round_no.write().expect("Lock poisoned"),
            CellDirection::Downstream => self
                .last_downstream_round_no
                .write()
                .expect("Lock poisoned"),
        };

        let is_duplicate = if let Some(last_round_no) = *last_round_no_guard {
            round_no <= last_round_no
        } else {
            false
        };

        if is_duplicate && cell.round_no() != INJECT_ROUND_NO {
            warn!("Dropping duplicate");
            return NextCellStep::Drop;
        }

        // inject handling
        if cell.round_no() == INJECT_ROUND_NO {
            if let CellDirection::Downstream = direction {
                if self.is_exit() {
                    let queue_len = self.inject_queue.read().expect("Lock poisoned").len();
                    if is_duplicate || queue_len > 0 {
                        // nothing to forward yet -> solved via padding later
                        let mut inject_queue_guard =
                            self.inject_queue.write().expect("Lock poisoned");
                        inject_queue_guard.push_back(cell);
                        return NextCellStep::Drop;
                    } else {
                        // inject right now -> set correct round number and fall through after the
                        // "inject handling"
                        cell.set_round_no(round_no);
                        // special case: nack in last round
                        if round_no == self.max_round_no {
                            let token = cell.token();
                            cell = self.create_nack(Some(token));
                        }
                    }
                } else {
                    warn!("Dropping cell that has the injection round no, but we are not exit mix");
                    return NextCellStep::Drop;
                }
            } else {
                warn!("Dropping cell that has the injection round no, but we didn't expect it yet");
                return NextCellStep::Drop;
            }
        }

        // we are sending a cell now, so update last round no
        *last_round_no_guard = Some(cell.round_no());

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
                    .remove(&cell.round_no())
                {
                    cell = delayed_cell;
                }

                // read command
                match cell.command() {
                    // TODO design: delaying does not work as expected -> can only delay a *different* cell?
                    Some(CellCmd::Delay(_)) => {
                        warn!("Somebody used the delay cmd, but it is not implemented yet")
                    }
                    Some(CellCmd::Subscribe(n_tokens)) if self.is_exit() => {
                        let slice_upper = (n_tokens as usize) * std::mem::size_of::<Token>();
                        let tokens = crate::defs::tokens_from_bytes(&cell.onion()[0..slice_upper]);
                        match tokens.len() {
                            0 => (),
                            1 => sub_collector.collect_subscription(tokens[0], self.upstream_id),
                            _ => sub_collector.collect_subscriptions(tokens, self.upstream_id),
                        }
                        // this cell has no end-to-end payload -> drop
                        return NextCellStep::Drop;
                    }
                    // remaining commands are not for upstream mixes
                    _ => (),
                }

                match self.upstream_hop {
                    Some(hop) => NextCellStep::Relay(PacketWithNextHop::new(cell, hop)),
                    None => {
                        cell.set_round_no(PUBLISH_ROUND_NO);
                        let rendezvous_addr = self
                            .rendezvous_map
                            .rendezvous_address(&cell.token(), false)
                            .expect("Checked this at circuit creation");
                        NextCellStep::Rendezvous(PacketWithNextHop::new(cell, rendezvous_addr))
                    }
                }
            }
            CellDirection::Downstream => match self.downstream_hop {
                Some(hop) => NextCellStep::Relay(PacketWithNextHop::new(cell, hop)),
                None => NextCellStep::Deliver(cell),
            },
        }
    }

    /// Use an injected cell or create a dummy cell to pad the circuit if necessary.
    /// Returns `None` if no padding is necessary.
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

        if !need_dummy {
            return None;
        }

        *last_round_no_guard = Some(round_no);

        let circuit_id = match direction {
            CellDirection::Upstream => self.upstream_id,
            CellDirection::Downstream => self.downstream_id,
        };

        // inject a waiting cell or create a dummy
        let (mut cell, need_enc) = match direction {
            CellDirection::Upstream => {
                // no upstream injection -> always dummy
                debug!("Creating upstream dummy cell in layer {}", self.layer);
                let mut dummy = Cell::dummy(circuit_id, round_no);
                if self.is_exit() {
                    dummy.set_round_no(PUBLISH_ROUND_NO);
                }
                (dummy, false)
            }
            CellDirection::Downstream => match round_no == self.max_round_no {
                true => (self.create_nack(None), true),
                false => match self
                    .inject_queue
                    .write()
                    .expect("Lock poisoned")
                    .pop_front()
                {
                    Some(c) => (c, true),
                    None => {
                        debug!("Creating downstream dummy cell in layer {}", self.layer);
                        (Cell::dummy(circuit_id, round_no), false)
                    }
                },
            },
        };

        // onion handling if it is no random dummy
        if need_enc {
            cell.set_round_no(round_no);
            self.handle_onion(&mut cell, direction);
        }

        // forwarding
        match direction {
            CellDirection::Upstream => match self.upstream_hop {
                Some(hop) => Some(NextCellStep::Relay(PacketWithNextHop::new(cell, hop))),
                None => {
                    cell.set_round_no(PUBLISH_ROUND_NO);
                    let rendezvous_addr = self
                        .rendezvous_map
                        .rendezvous_address(&cell.token(), false)
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
                let tweak_src = 24 * cell.round_no() as u64;
                match self.threefish.decrypt(tweak_src, cell.onion_mut()) {
                    Ok(()) => (),
                    Err(e) => {
                        warn!("Onion decryption failed, randomizing instead: {}", e);
                        cell.randomize();
                    }
                }
            }
            CellDirection::Downstream => {
                let tweak_src = 24 * cell.round_no() as u64 + 12;
                match self.threefish.encrypt(tweak_src, cell.onion_mut()) {
                    Ok(()) => (),
                    Err(e) => {
                        warn!("Onion encryption failed, randomizing instead: {}", e);
                        cell.randomize();
                    }
                }
            }
        }
    }

    /// Use `additional_token` if the NACK replaces a cell that is not inserted into the queue
    /// before.
    fn create_nack(&self, additional_token: Option<Token>) -> Cell {
        let mut cell: Cell = vec![0u8; CELL_LEN].try_into().unwrap();
        cell.set_circuit_id(self.downstream_id);
        cell.set_round_no(self.max_round_no);
        let inject_queue_guard = self.inject_queue.read().expect("Lock poisoned");
        let dropped = inject_queue_guard.len()
            + match additional_token {
                Some(_) => 1,
                None => 0,
            };
        let n = cmp::min((ONION_LEN - 8) / 8, dropped) as u8;
        cell.onion_mut()[0] = n;
        if (n as usize) < dropped {
            warn!("Have to drop {} cells -> NACK not big enough", dropped);
        }

        let mut dropped_tokens: Vec<Token> = inject_queue_guard.iter().map(|c| c.token()).collect();
        if let Some(t) = additional_token {
            dropped_tokens.push(t);
        }
        let mut i = 8;
        for token in dropped_tokens {
            match cell.onion_mut().get_mut(i..i + 8) {
                Some(buf) => LittleEndian::write_u64(buf, token),
                None => {
                    break;
                }
            }
            i += 8;
        }
        cell
    }
}
