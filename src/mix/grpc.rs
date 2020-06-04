use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tonic::{Code, Request, Response, Status};

use crate::defs::CircuitIdSet;
use crate::epoch::EpochNo;
use crate::grpc::valid_request_check;
use crate::mix::directory_client;
use crate::tonic_mix::mix_server::{Mix, MixServer};
use crate::tonic_mix::*;
use crate::{
    define_grpc_service, rethrow_as_internal, unwrap_or_throw_internal, unwrap_or_throw_invalid,
};

type SetupRxQueue = tokio::sync::mpsc::UnboundedSender<SetupPacket>;
type CellRxQueue = tokio::sync::mpsc::UnboundedSender<Cell>;

pub struct State {
    dir_client: Arc<directory_client::Client>,
    setup_rx_queues: Vec<SetupRxQueue>,
    _cell_rx_queues: Vec<CellRxQueue>,
    used_circuit_ids: Mutex<BTreeMap<EpochNo, CircuitIdSet>>,
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
            _cell_rx_queues: cell_rx_queues,
            used_circuit_ids: Mutex::new(BTreeMap::new()),
        }
    }
}

define_grpc_service!(Service, State, MixServer);

#[tonic::async_trait]
impl Mix for Service {
    async fn setup_circuit(&self, req: Request<SetupPacket>) -> Result<Response<SetupAck>, Status> {
        let pkt = req.into_inner();
        unwrap_or_throw_invalid!(pkt.ttl(), "Your setup packet has a strange size");
        valid_request_check(
            self.dir_client.has_ephemeral_key(&pkt.epoch_no),
            "Seems like we are not part of the given epoch",
        )?;

        {
            let mut map = rethrow_as_internal!(self.used_circuit_ids.lock(), "Lock failure");
            let already_in_use = match map.get_mut(&pkt.epoch_no) {
                Some(set) => set.contains(&pkt.circuit_id),
                None => {
                    let mut new_set = CircuitIdSet::new();
                    new_set.insert(pkt.circuit_id);
                    map.insert(pkt.epoch_no, new_set);
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
        rethrow_as_internal!(queue.send(pkt), "Sync error");
        Ok(Response::new(SetupAck {}))
    }

    async fn send_and_receive(&self, _req: Request<Cell>) -> Result<Response<CellVector>, Status> {
        unimplemented!();
    }

    async fn late_poll(
        &self,
        _req: Request<LatePollRequest>,
    ) -> Result<Response<CellVector>, Status> {
        unimplemented!();
    }

    async fn relay(&self, _req: Request<Cell>) -> Result<Response<RelayAck>, Status> {
        unimplemented!();
    }
}
