use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tonic::{Code, Request, Response, Status};

use crate::defs::{CircuitId, CircuitIdSet};
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

pub struct State {
    dir_client: Arc<directory_client::Client>,
    setup_rx_queues: Vec<SetupRxQueue>,
    _cell_rx_queues: Vec<CellRxQueue>,
    // TODO cleanup once in a while (as soon as we don't have any more cells to deliver for a
    // circuit)
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
        let pkt_with_prev = SetupPacketWithPrev {
            inner: pkt,
            previous_hop,
        };
        rethrow_as_internal!(queue.send(pkt_with_prev), "Sync error");
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
