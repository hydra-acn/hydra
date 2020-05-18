use std::collections::BTreeMap;
use std::convert::TryInto;
use std::sync::Mutex;
use tonic::{Request, Response, Status};

use crate::defs::{token_from_bytes, Token};
use crate::epoch::current_time_in_secs;
use crate::grpc::valid_request_check;
use crate::tonic_mix::simple_relay_server::{SimpleRelay, SimpleRelayServer};
use crate::tonic_mix::{Cell, CellVector, LongPoll, SendAck, SimplePoll};
use crate::{define_grpc_service, rethrow_as_internal};

struct TimestampedCell {
    timestamp: u64,
    cell: Cell,
}

pub struct State {
    cells: Mutex<BTreeMap<Token, Vec<TimestampedCell>>>,
}

define_grpc_service!(Service, State, SimpleRelayServer);

#[tonic::async_trait]
impl SimpleRelay for Service {
    async fn send(&self, req: Request<Cell>) -> Result<Response<SendAck>, Status> {
        let cell = req.into_inner();
        let timestamp = current_time_in_secs();
        let onion = &cell.onion;
        valid_request_check(onion.len() == 256, "Cell has wrong size")?;
        let token = token_from_bytes(onion[8..16].try_into().expect("Why should this fail?"));

        let ts_cell = TimestampedCell { timestamp, cell };

        {
            let mut map = rethrow_as_internal!(self.cells.lock(), "Could not acquire lock");
            match map.get_mut(&token) {
                Some(cell_vec) => cell_vec.push(ts_cell),
                None => {
                    map.insert(token, vec![ts_cell]);
                    ()
                }
            };
        }
        Ok(Response::new(SendAck {}))
    }

    async fn receive(&self, req: Request<SimplePoll>) -> Result<Response<CellVector>, Status> {
        unimplemented!();
    }

    async fn send_and_long_poll(
        &self,
        req: Request<LongPoll>,
    ) -> Result<Response<CellVector>, Status> {
        unimplemented!();
    }
}
