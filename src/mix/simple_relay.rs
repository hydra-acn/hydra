use std::collections::BTreeMap;
use std::convert::TryInto;
use std::sync::{Arc, RwLock};
use tokio::time::{delay_for, Duration};
use tonic::{Request, Response, Status};

use crate::defs::{token_from_bytes, CircuitId, Token};
use crate::epoch::current_time_in_secs;
use crate::grpc::valid_request_check;
use crate::tonic_mix::simple_relay_server::{SimpleRelay, SimpleRelayServer};
use crate::tonic_mix::{Cell, CellVector, LongPoll, SendAck, SimplePoll};
use crate::{define_grpc_service, rethrow_as_internal};

struct TimestampedCell {
    timestamp: u64,
    cell: Cell,
}

type CellMap = BTreeMap<Token, Vec<TimestampedCell>>;

pub struct State {
    cells: Arc<RwLock<CellMap>>,
}

impl State {
    pub fn new() -> Self {
        State {
            cells: Arc::new(RwLock::new(CellMap::new())),
        }
    }
}

define_grpc_service!(Service, State, SimpleRelayServer);

#[tonic::async_trait]
impl SimpleRelay for Service {
    async fn send(&self, req: Request<Cell>) -> Result<Response<SendAck>, Status> {
        let cell = req.into_inner();
        self.insert_cell(cell)?;
        Ok(Response::new(SendAck {}))
    }

    async fn receive(&self, req: Request<SimplePoll>) -> Result<Response<CellVector>, Status> {
        let msg = req.into_inner();
        let reply = CellVector {
            cells: self.get_matching_cells(&msg.tokens, msg.circuit_id)?,
        };
        Ok(Response::new(reply))
    }

    async fn send_and_long_poll(
        &self,
        req: Request<LongPoll>,
    ) -> Result<Response<CellVector>, Status> {
        let poll = req.into_inner();
        let circuit_id;
        if let Some(cell) = poll.cell {
            circuit_id = cell.circuit_id;
            self.insert_cell(cell)?;
        } else {
            circuit_id = 0; // silence compiler
            valid_request_check(false, "You have to send a cell for long polling")?;
        }

        // TODO calculate correct waiting time till end of round
        let wait_ms = 10 * 1000u64;
        delay_for(Duration::from_millis(wait_ms)).await;
        let reply = CellVector {
            cells: self.get_matching_cells(&poll.tokens, circuit_id)?,
        };
        Ok(Response::new(reply))
    }
}

impl Service {
    fn insert_cell(&self, cell: Cell) -> Result<(), Status> {
        let timestamp = current_time_in_secs();
        let onion = &cell.onion;
        valid_request_check(onion.len() == 256, "Cell has wrong size")?;
        let token = token_from_bytes(onion[8..16].try_into().expect("Why should this fail?"));

        let ts_cell = TimestampedCell { timestamp, cell };

        let mut map = rethrow_as_internal!(self.cells.write(), "Could not acquire lock");
        match map.get_mut(&token) {
            Some(cell_vec) => cell_vec.push(ts_cell),
            None => {
                map.insert(token, vec![ts_cell]);
                ()
            }
        };
        Ok(())
    }

    /// return all cells we know that match one of the requsted tokens but have a different circuit
    /// id than cid
    fn get_matching_cells(&self, tokens: &[Token], cid: CircuitId) -> Result<Vec<Cell>, Status> {
        let mut cells = Vec::new();
        let map = rethrow_as_internal!(self.cells.read(), "Could not acquire lock");
        for t in tokens {
            if let Some(cells_for_t) = map.get(&t) {
                for ts_cell in cells_for_t {
                    let cell = &ts_cell.cell;
                    if cell.circuit_id != cid {
                        cells.push(cell.clone());
                    }
                }
            }
        }
        Ok(cells)
    }
}
