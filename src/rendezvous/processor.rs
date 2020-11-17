use log::*;
use std::cmp::Ordering;
use std::convert::TryInto;
use std::sync::Arc;

use crate::defs::{INJECT_ROUND_NO, PUBLISH_ROUND_NO};
use crate::epoch::EpochNo;
use crate::mix::cell_processor::cell_rss_t;
use crate::mix::rss_pipeline::ProcessResult;
use crate::net::cell::{Cell, CellCmd};
use crate::net::PacketWithNextHop;
use crate::tonic_mix::Subscription;

use super::subscription_map::{CircuitId, SubscriptionMap};

crate::define_pipeline_types!(subscribe_t, Subscription, (), ());

pub fn process_subscribe(
    epoch_no: EpochNo,
    req: Subscription,
    map: Arc<SubscriptionMap>,
) -> subscribe_t::Result {
    match epoch_no.cmp(&req.epoch_no) {
        Ordering::Less => {
            warn!(
                "Dropping late subscription; expected epoch {}, got {}",
                epoch_no, req.epoch_no
            );
            ProcessResult::Drop
        }
        Ordering::Greater => ProcessResult::Requeue(req),
        Ordering::Equal => {
            map.subscribe(req);
            ProcessResult::Drop
        }
    }
}

pub fn process_publish(cell: Cell, map: Arc<SubscriptionMap>) -> cell_rss_t::Result {
    if cell.round_no() != PUBLISH_ROUND_NO {
        if cell.round_no() == INJECT_ROUND_NO {
            // seems like we are behind in time -> requeue for inject
            return cell_rss_t::Result::Requeue(cell);
        } else {
            debug!(
                "Dropping cell with wrong round number. Expected {}, got {}.",
                PUBLISH_ROUND_NO,
                cell.round_no()
            );

            return cell_rss_t::Result::Drop;
        }
    }

    let circuit_id: CircuitId = match cell.circuit_id().try_into() {
        Ok(cid) => cid,
        Err(_) => {
            warn!("Rendezvous circuit id too large");
            return ProcessResult::Drop;
        }
    };

    let subscribers = map.get_subscribers(&cell.token());
    if subscribers.is_empty() {
        debug!("No subscribers!");
        return ProcessResult::Drop;
    }
    let mut out = Vec::new();
    for sub in subscribers {
        if circuit_id == sub.circuit_id() {
            // don't send cells back on the same circuit, except when asked to
            if let Some(CellCmd::Broadcast) = cell.command() {
            } else {
                continue;
            }
        }
        let mut inject_cell = cell.clone();
        inject_cell.set_circuit_id(sub.circuit_id().into());
        inject_cell.set_round_no(INJECT_ROUND_NO);
        out.push(PacketWithNextHop::new(inject_cell, *sub.addr()));
    }
    ProcessResult::Multiple(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::epoch::current_time;
    use crate::mix::rss_pipeline::new_pipeline;
    use crate::tonic_mix::{CircuitSubscription, Subscription};

    use std::net::SocketAddr;
    use std::time::Duration;

    #[test]
    fn test_pubsub() {
        rayon::ThreadPoolBuilder::new()
            .num_threads(8)
            .build_global()
            .unwrap_or(());
        let map = Arc::new(SubscriptionMap::new());
        let (sub_rx, mut sub_processor, _, _): subscribe_t::Pipeline = new_pipeline(1);
        let (pub_rx, mut pub_processor, inject_tx, _): cell_rss_t::Pipeline = new_pipeline(2);

        let circuit_1 = CircuitSubscription {
            circuit_id: 1337,
            tokens: vec![1, 2, 3],
        };
        let circuit_2 = CircuitSubscription {
            circuit_id: 99,
            tokens: vec![4, 5, 6],
        };
        let circuit_3 = CircuitSubscription {
            circuit_id: 12,
            tokens: vec![1, 4, 7],
        };

        let sub_1 = Subscription {
            epoch_no: 1337,
            addr: vec![112, 13, 12, 1],
            port: 9001,
            circuits: vec![circuit_1, circuit_2],
        };
        let sub_2 = Subscription {
            epoch_no: 1337,
            addr: vec![112, 13, 13, 1],
            port: 9001,
            circuits: vec![circuit_3],
        };

        sub_rx.enqueue(sub_1);
        sub_rx.enqueue(sub_2);
        sub_processor.process_till(
            |req| process_subscribe(1337, req, map.clone()),
            current_time() + Duration::from_millis(100),
        );

        let mut cell_1 = Cell::dummy(1337, PUBLISH_ROUND_NO);
        cell_1.set_token(1);
        let mut cell_2 = Cell::dummy(12, PUBLISH_ROUND_NO);
        cell_2.set_token(4);
        cell_2.set_command(CellCmd::Broadcast);
        // last cell should be dropped because of wrong round number
        let cell_3 = Cell::dummy(1337, 0);

        pub_rx.enqueue(cell_1.clone());
        pub_rx.enqueue(cell_2.clone());
        pub_rx.enqueue(cell_3.clone());
        pub_processor.process_till(
            |cell| process_publish(cell, map.clone()),
            current_time() + Duration::from_millis(50),
        );
        pub_processor.send(None);

        let mut out = inject_tx.try_recv().unwrap().0;
        assert_eq!(out.len(), 2);
        let out_0 = &out[0];
        let out_1 = &out[1];
        assert_eq!(out_0.len(), 2); // with broadcast
        assert_eq!(out_1.len(), 1);

        let addr_1: SocketAddr = "112.13.12.1:9001".parse().unwrap();
        let addr_2: SocketAddr = "112.13.13.1:9001".parse().unwrap();
        assert_eq!(*out_0[0].next_hop(), addr_1);
        assert_eq!(*out_0[1].next_hop(), addr_2);
        assert_eq!(*out_1[0].next_hop(), addr_2);

        let out_cell_1 = out[0].pop().unwrap().into_inner();
        let out_cell_2 = out[0].pop().unwrap().into_inner();
        let out_cell_3 = out[1].pop().unwrap().into_inner();
        assert_eq!(out_cell_1.circuit_id(), 12);
        assert_eq!(out_cell_1.round_no(), INJECT_ROUND_NO);
        assert_eq!(out_cell_1.onion(), cell_2.onion());
        assert_eq!(out_cell_2.circuit_id(), 99);
        assert_eq!(out_cell_2.onion(), cell_2.onion());
        assert_eq!(out_cell_3.circuit_id(), 12);
        assert_eq!(out_cell_3.onion(), cell_1.onion());
    }
}
