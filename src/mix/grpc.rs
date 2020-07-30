use futures_util::StreamExt;
use log::*;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tonic::{Code, Request, Response, Status};

use crate::crypto::x448;
use crate::defs::{CircuitId, SETUP_AUTH_LEN, SETUP_NONCE_LEN};
use crate::epoch::EpochNo;
use crate::grpc::valid_request_check;
use crate::mix::directory_client;
use crate::tonic_mix::mix_server::{Mix, MixServer};
use crate::tonic_mix::*;
use crate::{
    define_grpc_service, rethrow_as_internal, rethrow_as_invalid, unwrap_or_throw_internal,
    unwrap_or_throw_invalid,
};

/// The `previous_hop` will be used to forward dummy cells in downstream direction. It is `None`
/// for the first layer.
#[derive(Debug)]
pub struct SetupPacketWithPrev {
    inner: SetupPacket,
    previous_hop: Option<SocketAddr>,
}

impl SetupPacketWithPrev {
    pub fn new(pkt: SetupPacket, previous_hop: Option<SocketAddr>) -> Self {
        SetupPacketWithPrev {
            inner: pkt,
            previous_hop,
        }
    }

    pub fn epoch_no(&self) -> EpochNo {
        self.inner.epoch_no
    }

    pub fn circuit_id(&self) -> CircuitId {
        self.inner.circuit_id
    }

    pub fn ttl(&self) -> Option<u32> {
        self.inner.ttl()
    }

    pub fn previous_hop(&self) -> Option<SocketAddr> {
        self.previous_hop
    }

    pub fn into_inner(self) -> SetupPacket {
        self.inner
    }
}

type SetupRxQueue = tokio::sync::mpsc::UnboundedSender<SetupPacketWithPrev>;
type CellRxQueue = tokio::sync::mpsc::UnboundedSender<Cell>;
type CellStorage = BTreeMap<CircuitId, Vec<Cell>>;

pub struct State {
    dir_client: Arc<directory_client::Client>,
    setup_rx_queues: Vec<SetupRxQueue>,
    cell_rx_queues: Vec<CellRxQueue>,
    // TODO cleanup once in a while (see garbage collector of simple relay)
    // TODO performance: avoid global lock on complete storage; "RSS" by circuit id instead
    storage: Mutex<CellStorage>,
}

impl State {
    pub fn new(
        dir_client: Arc<directory_client::Client>,
        setup_rx_queues: Vec<SetupRxQueue>,
        cell_rx_queues: Vec<CellRxQueue>,
    ) -> Self {
        State {
            dir_client,
            setup_rx_queues,
            cell_rx_queues,
            storage: Mutex::new(CellStorage::new()),
        }
    }

    pub fn deliver(&self, cells: Vec<Cell>) {
        let mut storage = self.storage.lock().expect("Lock poisoned");
        for cell in cells {
            if let Some(cell_vec) = storage.get_mut(&cell.circuit_id) {
                cell_vec.push(cell);
            } else {
                warn!("Cell ready for delivery, but circuit id unknown -> dropping");
            }
        }
    }

    async fn relay_impl(&self, req: Request<tonic::Streaming<Cell>>) -> Result<(), Status> {
        let mut stream = req.into_inner();
        while let Some(c) = stream.next().await {
            let cell = match c {
                Ok(cell) => cell,
                Err(e) => {
                    warn!(
                        "Error during stream processing in function relay_impl: {}",
                        e
                    );
                    continue;
                }
            };
            let i = cell.circuit_id as usize % self.cell_rx_queues.len();
            let queue =
                unwrap_or_throw_internal!(self.cell_rx_queues.get(i), "Logical index error");
            rethrow_as_internal!(queue.send(cell), "Sync error");
        }
        Ok(())
    }
}

define_grpc_service!(Service, State, MixServer);

#[tonic::async_trait]
impl Mix for Service {
    async fn setup_circuit(&self, req: Request<SetupPacket>) -> Result<Response<SetupAck>, Status> {
        let previous_hop = match req.metadata().get("reply-to") {
            Some(val) => {
                let as_str = rethrow_as_invalid!(val.to_str(), "reply-to is not valid");
                let prev = rethrow_as_invalid!(as_str.to_string().parse(), "reply-to is not valid");
                Some(prev)
            }
            None => None,
        };
        let pkt = req.into_inner();
        unwrap_or_throw_invalid!(pkt.ttl(), "Your setup packet has a strange size");
        valid_request_check(
            self.dir_client.has_ephemeral_key(&pkt.epoch_no),
            "Seems like we are not part of the given epoch",
        )?;
        valid_request_check(
            pkt.public_dh.len() == x448::POINT_SIZE,
            "Public key has not the expected size",
        )?;
        valid_request_check(
            pkt.nonce.len() == SETUP_NONCE_LEN,
            "Nonce has not the expected size",
        )?;
        valid_request_check(
            pkt.auth_tag.len() == SETUP_AUTH_LEN,
            "Authentication has not the expected size",
        )?;
        {
            let mut storage = rethrow_as_internal!(self.storage.lock(), "Lock failure");
            let already_in_use = match storage.get_mut(&pkt.circuit_id) {
                Some(_) => true,
                None => {
                    storage.insert(pkt.circuit_id, Vec::new());
                    false
                }
            };
            if already_in_use == true {
                return Err(Status::new(
                    Code::AlreadyExists,
                    format!("Circuit Id {} already in use", &pkt.circuit_id),
                ));
            }
        }
        let i = pkt.circuit_id as usize % self.setup_rx_queues.len();
        let queue = unwrap_or_throw_internal!(self.setup_rx_queues.get(i), "Logical index error");
        let pkt_with_prev = SetupPacketWithPrev {
            inner: pkt,
            previous_hop,
        };
        rethrow_as_internal!(queue.send(pkt_with_prev), "Sync error");
        Ok(Response::new(SetupAck {}))
    }

    async fn send_and_receive(&self, req: Request<Cell>) -> Result<Response<CellVector>, Status> {
        // TODO security: circuit ids should be encrypted to avoid easy DoS (query for other users)
        let cell = req.into_inner();
        // first collect all missed cells
        let mut storage = rethrow_as_internal!(self.storage.lock(), "Lock failure");
        let missed_cells =
            unwrap_or_throw_invalid!(storage.get_mut(&cell.circuit_id), "Unknown circuit");
        let mut cell_vec = CellVector {
            cells: missed_cells.clone(),
        };
        if cell_vec.cells.is_empty() {
            cell_vec
                .cells
                .push(Cell::dummy(cell.circuit_id, cell.round_no - 1));
        }
        // deliver cells only once
        missed_cells.clear();

        // forward new cell
        let i = cell.circuit_id as usize % self.cell_rx_queues.len();
        let queue = unwrap_or_throw_internal!(self.cell_rx_queues.get(i), "Logical index error");
        rethrow_as_internal!(queue.send(cell), "Sync error");

        // send response
        Ok(Response::new(cell_vec))
    }

    async fn late_poll(
        &self,
        req: Request<LatePollRequest>,
    ) -> Result<Response<CellVector>, Status> {
        let mut storage = rethrow_as_internal!(self.storage.lock(), "Lock failure");
        let ids = req.into_inner().circuit_ids;
        let mut cell_vec = CellVector { cells: Vec::new() };
        for circuit_id in ids {
            match storage.remove(&circuit_id) {
                Some(mut vec) => cell_vec.cells.append(&mut vec),
                None => (),
            }
        }
        Ok(Response::new(cell_vec))
    }

    async fn relay(
        &self,
        req: Request<tonic::Streaming<Cell>>,
    ) -> Result<Response<RelayAck>, Status> {
        self.relay_impl(req).await?;
        Ok(Response::new(RelayAck {}))
    }

    async fn inject(
        &self,
        req: Request<tonic::Streaming<Cell>>,
    ) -> Result<Response<InjectAck>, Status> {
        self.relay_impl(req).await?;
        Ok(Response::new(InjectAck {}))
    }
}
