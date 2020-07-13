use log::*;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, RwLock};
use tokio::time::{delay_for, Duration};
use tonic::{Request, Response, Status};

use crate::defs::{CircuitId, Token};
use crate::epoch::current_time_in_secs;
use crate::grpc::valid_request_check;
use crate::tonic_mix::simple_relay_server::{SimpleRelay, SimpleRelayServer};
use crate::tonic_mix::*;
use crate::{define_grpc_service, rethrow_as_internal, unwrap_or_throw_invalid};

#[derive(Clone)]
struct TimestampedCell {
    timestamp: u64,
    cell: Cell,
}

impl Ord for TimestampedCell {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.timestamp.cmp(&other.timestamp) {
            Ordering::Less => Ordering::Less,
            Ordering::Greater => Ordering::Greater,
            Ordering::Equal => self.cell.circuit_id.cmp(&other.cell.circuit_id),
        }
    }
}

impl PartialOrd for TimestampedCell {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for TimestampedCell {}

impl PartialEq for TimestampedCell {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp == other.timestamp && self.cell.circuit_id == other.cell.circuit_id
    }
}

type CellMap = BTreeMap<Token, BTreeSet<TimestampedCell>>;

pub struct State {
    cells: RwLock<CellMap>,
}

impl State {
    pub fn new() -> Self {
        State {
            cells: RwLock::new(CellMap::new()),
        }
    }
}

define_grpc_service!(Service, State, SimpleRelayServer);

#[tonic::async_trait]
impl SimpleRelay for Service {
    async fn send_and_receive(
        &self,
        req: Request<RelayRequest>,
    ) -> Result<Response<CellVector>, Status> {
        let msg = req.into_inner();
        let cell = unwrap_or_throw_invalid!(msg.cell, "You have to send a cell each round");
        let reply = CellVector {
            cells: self.extract_matching_cells(&msg.tokens, cell.circuit_id)?,
        };
        self.insert_cell(cell)?;
        Ok(Response::new(reply))
    }
}

impl Service {
    fn insert_cell(&self, cell: Cell) -> Result<(), Status> {
        let timestamp = current_time_in_secs();
        let onion = &cell.onion;
        valid_request_check(onion.len() == 256, "Cell has wrong size")?;
        let token = cell.token();

        let ts_cell = TimestampedCell { timestamp, cell };

        let mut map = rethrow_as_internal!(self.cells.write(), "Could not acquire lock");
        match map.get_mut(&token) {
            Some(cell_set) => cell_set.insert(ts_cell),
            None => {
                let mut set = BTreeSet::new();
                set.insert(ts_cell);
                map.insert(token, set);
                true
            }
        };
        Ok(())
    }

    /// delete and return all cells we know that match one of the requested tokens but have a
    /// different circuit id than cid; if no cells match, return one dummy cell
    fn extract_matching_cells(
        &self,
        tokens: &[Token],
        cid: CircuitId,
    ) -> Result<Vec<Cell>, Status> {
        let mut cells = Vec::new();
        let mut map = rethrow_as_internal!(self.cells.write(), "Could not acquire lock");
        for t in tokens {
            if let Some(cells_for_t) = map.get_mut(&t) {
                let mut to_remove = BTreeSet::new();
                for ts_cell in cells_for_t.iter() {
                    let cell = &ts_cell.cell;
                    if cell.circuit_id != cid {
                        to_remove.insert(ts_cell.clone());
                        cells.push(cell.clone());
                    }
                }
                for c in to_remove {
                    cells_for_t.remove(&c);
                }
            }
        }
        if cells.len() == 0 {
            cells.push(Cell::dummy(cid, 42)); // note: round_no (42) is wayne here
        }
        Ok(cells)
    }
}

/// endless-loop that deletes old cells (older than 24 hours)
pub async fn garbage_collector(state: Arc<State>) {
    loop {
        info!("Garbage collector strikes again!");
        {
            let mut map = match state.cells.write() {
                Ok(m) => m,
                Err(e) => {
                    error!("Acquiring lock for cleanup failed: {}", e);
                    continue;
                }
            };
            for (_, cell_set) in map.iter_mut() {
                let split_cell = TimestampedCell {
                    timestamp: current_time_in_secs() - 24 * 3600,
                    cell: Cell {
                        circuit_id: 0,
                        round_no: 0,
                        onion: vec![],
                    },
                };
                *cell_set = cell_set.split_off(&split_cell);
            }
        }
        // cleanup every hour
        delay_for(Duration::from_secs(3600)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::mix::simple_relay;
    use crate::tonic_mix::simple_relay_client::SimpleRelayClient;
    use crate::tonic_mix::RelayRequest;
    use std::sync::Arc;
    use tokio::time::{self, Duration};

    #[test]
    fn test_order() {
        let my_dummy_cell1 = Cell::dummy(21, 3);
        let my_dummy_cell2 = Cell::dummy(3, 3);
        let my_dummy_cell3 = Cell::dummy(2, 3);
        let smaller = TimestampedCell {
            timestamp: 12,
            cell: my_dummy_cell1,
        };
        let larger = TimestampedCell {
            timestamp: 22,
            cell: my_dummy_cell2,
        };
        let equal_larger_with_smaller_cid = TimestampedCell {
            timestamp: 22,
            cell: my_dummy_cell3,
        };

        assert_eq!(Ordering::Less, smaller.cmp(&larger));
        assert_eq!(Ordering::Greater, larger.cmp(&smaller));
        assert_eq!(
            Ordering::Greater,
            larger.cmp(&equal_larger_with_smaller_cid)
        );
    }

    #[test]
    fn test_partial_order() {
        let my_dummy_cell1 = Cell::dummy(21, 3);
        let smaller = TimestampedCell {
            timestamp: 12,
            cell: my_dummy_cell1,
        };

        assert_eq!(Some(Ordering::Equal), smaller.partial_cmp(&smaller));
    }

    #[test]
    fn test_eq() {
        let my_dummy_cell1 = Cell::dummy(21, 3);
        let same_as_my_dummy_cell1 = Cell::dummy(21, 3);
        let my_dummy_cell2 = Cell::dummy(2, 4);

        let cell = TimestampedCell {
            timestamp: 12,
            cell: my_dummy_cell1,
        };
        let same_cell = TimestampedCell {
            timestamp: 12,
            cell: same_as_my_dummy_cell1,
        };
        let other_cell = TimestampedCell {
            timestamp: 12,
            cell: my_dummy_cell2,
        };
        assert_eq!(true, cell.eq(&same_cell));
        assert_eq!(false, cell.eq(&other_cell));
    }

    async fn test_send_and_receive(addr: std::net::SocketAddr) {
        time::delay_for(Duration::from_millis(100)).await;
        //initialize client
        let mut client = SimpleRelayClient::connect(format!("http://{}", addr))
            .await
            .expect("failed to connect");
        //initialize dummy cell
        let mut my_dummy_cell = Cell::dummy(21, 3);
        let working_cell = my_dummy_cell.clone();
        let sent_onion = working_cell.onion;
        let token = my_dummy_cell.token();
        //send valid request
        let request = tonic::Request::new(RelayRequest {
            cell: Some(my_dummy_cell),
            tokens: Vec::new(),
        });
        let response = client
            .send_and_receive(request)
            .await
            .expect("Send Receive failed");
        let empty_resp_cell: Cell = std::option::Option::unwrap(response.into_inner().cells.pop());
        assert_eq!(empty_resp_cell.circuit_id, 21);
        //send another dummy cell to get first cell back
        my_dummy_cell = Cell::dummy(1, 3);
        let request = tonic::Request::new(RelayRequest {
            cell: Some(my_dummy_cell),
            tokens: vec![token],
        });
        let response = client
            .send_and_receive(request)
            .await
            .expect("Send Receive failed");
        let resp_cell = std::option::Option::unwrap(response.into_inner().cells.pop());
        assert_eq!(resp_cell.onion, sent_onion);
        //send a too short cell
        let my_short_cell = Cell {
            circuit_id: 1,
            round_no: 1,
            onion: vec![3],
        };
        let request = tonic::Request::new(RelayRequest {
            cell: Some(my_short_cell),
            tokens: vec![2],
        });
        expect_specific_fail(
            client.send_and_receive(request).await,
            "Cell has wrong size",
        );
        //send request without cell
        let request = tonic::Request::new(RelayRequest {
            cell: None,
            tokens: vec![1],
        });
        expect_specific_fail(
            client.send_and_receive(request).await,
            "You have to send a cell each round",
        );
    }

    #[tokio::test]
    async fn test_simple_relay_and_garbage_collector() {
        //insert timestamped cell that should be deleted by the garbage collector
        let timestamp = current_time_in_secs() - (24 * 3600 + 33);
        let cell_to_discard = Cell::dummy(2, 3);
        let state = Arc::new(simple_relay::State::new());
        let token1 = cell_to_discard.token();
        let ts_cell_to_discard = TimestampedCell {
            timestamp: timestamp,
            cell: cell_to_discard,
        };
        {
            let mut map = state.cells.write().expect("Could not acquire lock");
            match map.get_mut(&token1) {
                Some(cell_set) => cell_set.insert(ts_cell_to_discard.clone()),
                None => {
                    let mut set = BTreeSet::new();
                    set.insert(ts_cell_to_discard.clone());
                    map.insert(token1, set);
                    true
                }
            };
        }
        //insert second timestamped cell that should not be deleted by the garbage collector
        let timestamp_fresh = current_time_in_secs();
        let cell_to_keep = Cell::dummy(2, 3);
        let token2 = cell_to_keep.token();
        let ts_cell_to_keep = TimestampedCell {
            timestamp: timestamp_fresh,
            cell: cell_to_keep,
        };
        {
            let mut map = state.cells.write().expect("Could not acquire lock");
            match map.get_mut(&token2) {
                Some(cell_set) => cell_set.insert(ts_cell_to_keep.clone()),
                None => {
                    let mut set = BTreeSet::new();
                    set.insert(ts_cell_to_keep.clone());
                    map.insert(token2, set);
                    true
                }
            };
        }
        //start server and garbage collector
        let local_addr: std::net::SocketAddr = ("127.0.0.1:0").parse().expect("failed to parse");
        let timeout = time::delay_for(Duration::from_secs(2));
        let (grpc_handle, local_addr) =
            spawn_service_with_shutdown(state.clone(), local_addr, Some(timeout))
                .await
                .expect("Spawn failed");
        let garbage_handle = tokio::spawn(simple_relay::garbage_collector(state.clone()));
        if let Err(_) = tokio::time::timeout(Duration::from_secs(1), garbage_handle).await {
        } else {
            unreachable!();
        }
        //send test cells
        let client_handle = tokio::spawn(test_send_and_receive(local_addr));
        match tokio::try_join!(grpc_handle, client_handle) {
            Ok(_) => (),
            Err(e) => panic!("Something failed: {}", e),
        }
        //verify work of garbage collector
        {
            let mut cell_to_discard_in_tree = true;
            let mut cell_to_keep_in_tree = false;
            let map = state.cells.write().expect("Could not acquire lock").clone();
            if let Some(bt_from_map) = map.get(&token1) {
                cell_to_discard_in_tree = bt_from_map.contains(&ts_cell_to_discard);
            }
            if let Some(bt_from_map) = map.get(&token2) {
                cell_to_keep_in_tree = bt_from_map.contains(&ts_cell_to_keep);
            }
            assert_eq!(cell_to_discard_in_tree, false);
            assert_eq!(cell_to_keep_in_tree, true);
        }
    }
    fn expect_specific_fail<T>(reply: Result<T, tonic::Status>, msg: &str) {
        match reply {
            Ok(_) => panic!("Expected fail did not occure"),
            Err(e) => assert_eq!(format!("{}", e.message()), msg),
        }
    }
}
