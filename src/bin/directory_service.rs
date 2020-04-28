use hydra::crypto::key::Key;
use hydra::crypto::x448;
use hydra::directory_grpc::*;
use log::*;
use std::sync::Mutex;
use tonic::{transport::Server, Code, Request, Response, Status};

pub struct Service {
    test_key_vec: Mutex<Vec<Key>>,
}

#[tonic::async_trait]
impl directory_server::Directory for Service {
    async fn register(
        &self,
        req: Request<RegisterRequest>,
    ) -> Result<Response<RegisterReply>, Status> {
        let msg = req.into_inner();
        // XXX convert?
        let mix_pk = Key::move_from_vec(msg.public_dh);
        let (pk, sk) = match x448::generate_keypair() {
            Ok((pk, sk)) => (pk, sk),
            Err(e) => {
                error!("Failed to generate x448 key pair: {}", e);
                return Err(internal_error(
                    "Directory service faild to generate a key pair",
                ));
            }
        };

        let s = match x448::generate_shared_secret(&mix_pk, &sk) {
            Ok(s) => s,
            Err(e) => {
                error!("Failed to execute x448 exchange: {}", e);
                return Err(internal_error(
                    "Directory service failed to generate shared secret",
                ));
            }
        };
        {
            // XXX store shared secret for real
            let mut key_vec = match self.test_key_vec.lock() {
                Ok(v) => v,
                Err(e) => {
                    error!("Failed to acquire lock: {}", e);
                    return Err(internal_error("Directory service could not acquire a lock"));
                }
            };
            key_vec.push(s);
        }

        let reply = RegisterReply {
            public_dh: pk.clone_to_vec(),
        };
        Ok(Response::new(reply))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // TODO initialize some logging implementation
    Ok(())
}

fn internal_error(msg: &str) -> Status {
    Status::new(Code::Internal, msg)
}
