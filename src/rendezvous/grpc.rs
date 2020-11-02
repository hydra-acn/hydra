//! gRPC service for the rendezvous part of a mix.
use futures_util::StreamExt;
use log::*;
use tonic::{Request, Response, Status};

use crate::grpc::macros::valid_request_check;
use crate::tonic_mix::rendezvous_server::{Rendezvous, RendezvousServer};
use crate::tonic_mix::{Cell, PublishAck, SubscribeAck, Subscription};

use super::processor::subscribe_t;

pub struct State {
    subscribe_rx_queue: subscribe_t::RxQueue,
}

impl State {
    pub fn new(subscribe_rx_queue: subscribe_t::RxQueue) -> Self {
        State { subscribe_rx_queue }
    }
}

crate::define_grpc_service!(Service, State, RendezvousServer);

#[tonic::async_trait]
impl Rendezvous for Service {
    async fn subscribe(
        &self,
        req: Request<tonic::Streaming<Subscription>>,
    ) -> Result<Response<SubscribeAck>, Status> {
        {
            let mut stream = req.into_inner();
            while let Some(s) = stream.next().await {
                let sub = match s {
                    Ok(ss) => ss,
                    Err(e) => {
                        warn!("Reading sub stream failed: {}", e);
                        continue;
                    }
                };
                valid_request_check(sub.is_valid(), "Address or port for injection invalid")?;
                self.subscribe_rx_queue.enqueue(sub);
            }
        }
        Ok(Response::new(SubscribeAck {}))
    }

    async fn publish(
        &self,
        _req: Request<tonic::Streaming<Cell>>,
    ) -> Result<Response<PublishAck>, Status> {
        // TODO code: remove publish from proto def
        unimplemented!();
    }
}
