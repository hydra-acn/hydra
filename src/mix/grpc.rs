use futures_util::StreamExt;
use log::*;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tonic::{Code, Request, Response, Status};

use crate::defs::CircuitId;
use crate::grpc::type_extensions::SetupPacketWithPrev;
use crate::net::PacketWithNextHop;
use crate::tonic_mix::mix_server::{Mix, MixServer};
use crate::tonic_mix::*;
use crate::{
    define_grpc_service, rethrow_as_internal, rethrow_as_invalid, unwrap_or_throw_invalid,
};

use super::cell_processor::cell_rss_t;
use super::directory_client;
use super::setup_processor::setup_t;

type CellStorage = BTreeMap<CircuitId, Vec<Cell>>;

pub struct State {
    dir_client: Arc<directory_client::Client>,
    setup_rx_queue: setup_t::RxQueue,
    cell_rx_queue: cell_rss_t::RxQueue,
    // TODO cleanup once in a while (see garbage collector of simple relay)
    // TODO performance: avoid global lock on complete storage; "RSS" by circuit id instead
    storage: Mutex<CellStorage>,
}

impl State {
    pub fn new(
        dir_client: Arc<directory_client::Client>,
        setup_rx_queue: setup_t::RxQueue,
        cell_rx_queue: cell_rss_t::RxQueue,
    ) -> Self {
        State {
            dir_client,
            setup_rx_queue,
            cell_rx_queue,
            storage: Mutex::new(CellStorage::new()),
        }
    }

    fn create_storage(&self, circuit_id: CircuitId) -> Result<(), Status> {
        let mut storage = rethrow_as_internal!(self.storage.lock(), "Lock failure");
        match storage.get_mut(&circuit_id) {
            Some(_) => Err(Status::new(
                Code::AlreadyExists,
                format!("Circuit Id {} already in use", &circuit_id),
            )),
            None => {
                storage.insert(circuit_id, Vec::new());
                Ok(())
            }
        }
    }

    pub fn deliver(&self, cells: Vec<Vec<PacketWithNextHop<Cell>>>) {
        let mut storage = self.storage.lock().expect("Lock poisoned");
        for vec in cells.into_iter() {
            for pkt in vec.into_iter() {
                let cell = pkt.into_inner();
                if let Some(cell_vec) = storage.get_mut(&cell.circuit_id) {
                    debug!("New cell ready for delivery on circuit {}", cell.circuit_id);
                    cell_vec.push(cell);
                } else {
                    warn!("Cell ready for delivery, but circuit id unknown -> dropping");
                }
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
            self.cell_rx_queue.enqueue(cell);
        }
        Ok(())
    }
}

define_grpc_service!(Service, State, MixServer);

#[tonic::async_trait]
impl Mix for Service {
    async fn setup_circuit(&self, req: Request<SetupPacket>) -> Result<Response<SetupAck>, Status> {
        let previous_hop = get_previous_hop(&req)?;
        let pkt = req.into_inner();
        pkt.validity_check(&*self.dir_client)?;
        self.create_storage(pkt.circuit_id)?;
        self.setup_rx_queue
            .enqueue(SetupPacketWithPrev::new(pkt, previous_hop));
        Ok(Response::new(SetupAck {}))
    }

    async fn stream_setup_circuit(
        &self,
        req: Request<tonic::Streaming<SetupPacket>>,
    ) -> Result<Response<SetupAck>, Status> {
        let previous_hop = get_previous_hop(&req)?;
        let is_client = previous_hop.is_none();
        let mut stream = req.into_inner();
        while let Some(p) = stream.next().await {
            let pkt = match p {
                Ok(pp) => pp,
                Err(e) => {
                    warn!("Error during stream processing in setup: {}", e);
                    continue;
                }
            };
            pkt.validity_check(&*self.dir_client)?;
            if is_client {
                self.create_storage(pkt.circuit_id)?;
            }
            self.setup_rx_queue
                .enqueue(SetupPacketWithPrev::new(pkt, previous_hop));
        }
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
        if cell_vec.cells.is_empty() && cell.round_no > 0 {
            cell_vec
                .cells
                .push(Cell::dummy(cell.circuit_id, cell.round_no - 1));
        }
        // deliver cells only once
        missed_cells.clear();

        // handle new cell
        self.cell_rx_queue.enqueue(cell);

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

fn get_previous_hop<T>(req: &Request<T>) -> Result<Option<SocketAddr>, Status> {
    match req.metadata().get("reply-to") {
        Some(val) => {
            let as_str = rethrow_as_invalid!(val.to_str(), "reply-to is not valid");
            let prev = rethrow_as_invalid!(as_str.to_string().parse(), "reply-to is not valid");
            Ok(Some(prev))
        }
        None => Ok(None),
    }
}
