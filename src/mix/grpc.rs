use futures_util::StreamExt;
use log::*;
use std::net::SocketAddr;
use std::sync::Arc;
use tonic::{Code, Request, Response, Status};

use crate::grpc::type_extensions::SetupPacketWithPrev;
use crate::net::cell::Cell as FlatCell;
use crate::tonic_mix::mix_server::{Mix, MixServer};
use crate::tonic_mix::*;
use crate::{define_grpc_service, rethrow_as_invalid, unwrap_or_throw_invalid};

use super::cell_processor::cell_rss_t;
use super::directory_client;
use super::setup_processor::setup_t;
use super::storage::Storage;

pub struct State {
    dir_client: Arc<directory_client::Client>,
    setup_rx_queue: setup_t::RxQueue,
    cell_rx_queue: cell_rss_t::RxQueue,
    storage: Arc<Storage>,
}

impl State {
    pub fn new(
        dir_client: Arc<directory_client::Client>,
        setup_rx_queue: setup_t::RxQueue,
        cell_rx_queue: cell_rss_t::RxQueue,
        storage: Arc<Storage>,
    ) -> Self {
        State {
            dir_client,
            setup_rx_queue,
            cell_rx_queue,
            storage,
        }
    }

    pub fn deliver(&self, cells: Vec<Vec<FlatCell>>) {
        for vec in cells.into_iter() {
            for cell in vec.into_iter() {
                self.storage.insert_cell(cell);
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
            self.cell_rx_queue.enqueue(cell.into());
        }
        Ok(())
    }
}

define_grpc_service!(Service, State, MixServer);

#[tonic::async_trait]
impl Mix for Service {
    async fn setup_circuit(&self, req: Request<SetupPacket>) -> Result<Response<SetupAck>, Status> {
        let previous_hop = get_previous_hop(&req)?;
        let firebase_token = get_firebase_token(&req);
        let pkt = req.into_inner();
        pkt.validity_check(&*self.dir_client)?;
        if self
            .storage
            .create_circuit(pkt.circuit_id, pkt.epoch_no, firebase_token)
            == false
        {
            return Err(Status::new(
                Code::AlreadyExists,
                "Circuit id exists already",
            ));
        }
        self.setup_rx_queue
            .enqueue(SetupPacketWithPrev::new(pkt, previous_hop));
        Ok(Response::new(SetupAck {}))
    }

    async fn stream_setup_circuit(
        &self,
        req: Request<tonic::Streaming<SetupPacket>>,
    ) -> Result<Response<SetupAck>, Status> {
        let previous_hop = get_previous_hop(&req)?;
        let firebase_token = get_firebase_token(&req);
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
                if self
                    .storage
                    .create_circuit(pkt.circuit_id, pkt.epoch_no, firebase_token.clone())
                    == false
                {
                    return Err(Status::new(
                        Code::AlreadyExists,
                        "Circuit id exists already",
                    ));
                }
            }
            self.setup_rx_queue
                .enqueue(SetupPacketWithPrev::new(pkt, previous_hop));
        }
        Ok(Response::new(SetupAck {}))
    }

    async fn send_and_receive(&self, req: Request<Cell>) -> Result<Response<CellVector>, Status> {
        // TODO security: circuit ids should be encrypted to avoid easy DoS (query for other users)
        let cell = req.into_inner();
        let cid = cell.circuit_id;
        let r = cell.round_no;

        // first, enqueue new cell
        self.cell_rx_queue.enqueue(cell.into());

        if r > 0 {
            // collect all missed cells
            let mut cell_vec =
                unwrap_or_throw_invalid!(self.storage.remove_cells(&cid), "Unknown circuit");
            if cell_vec.is_empty() {
                cell_vec.push(FlatCell::dummy(cid, r - 1));
            }
            // convert missed cells to grpc format
            let grpc_cells = cell_vec.into_iter().map(|c| c.into()).collect();

            Ok(Response::new(CellVector { cells: grpc_cells }))
        } else {
            // nothing to receive in first round -> empty response
            Ok(Response::new(CellVector { cells: Vec::new() }))
        }
    }

    async fn late_poll(
        &self,
        req: Request<LatePollRequest>,
    ) -> Result<Response<CellVector>, Status> {
        let ids = req.into_inner().circuit_ids;
        let mut cell_vec = CellVector::default();
        for circuit_id in ids {
            // TODO performance: completely delete the circuit from storage (keep map small!)
            match self.storage.remove_cells(&circuit_id) {
                Some(vec) => {
                    for c in vec.into_iter() {
                        cell_vec.cells.push(c.into());
                    }
                }
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

fn get_firebase_token<T>(req: &Request<T>) -> Option<String> {
    match req.metadata().get("firebase") {
        Some(val) => match val.to_str() {
            Ok(token) => Some(token.to_string()),
            Err(_) => {
                warn!("Firebase token is not valid");
                None
            }
        },
        None => None,
    }
}
