use log::*;
use std::sync::Arc;

use hydra::directory::grpc;
use hydra::directory::state::{self, State};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    hydra::log::init();
    info!("Starting directory service");

    let state = Arc::new(State::default());

    // TODO use real address
    let local_addr = "127.0.0.1:4242".parse()?;
    let grpc_handle = grpc::spawn_service(state.clone(), local_addr);

    let update_handle = tokio::spawn(state::update_loop(state.clone()));

    match tokio::try_join!(update_handle, grpc_handle) {
        Ok(_) => (),
        Err(e) => error!("Something failed: {}", e),
    }

    info!("Stopping gracefully");
    Ok(())
}
