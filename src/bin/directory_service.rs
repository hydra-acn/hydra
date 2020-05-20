use clap::clap_app;
use log::*;
use std::sync::Arc;

use hydra::directory::grpc;
use hydra::directory::state::{self, State};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    hydra::log::init();
    info!("Starting directory service");

    let args = clap_app!(directory_service =>
        (version: hydra::defs::hydra_version())
        (about: "Simple, non distributed, implementation of the Hydra directory service")
        (@arg addr: +required "IP address to listen on")
    )
    .get_matches();

    let state = Arc::new(State::default());

    let local_addr = format!("{}:9000", args.value_of("addr").unwrap()).parse()?;
    let grpc_handle = grpc::spawn_service(state.clone(), local_addr);

    let update_handle = tokio::spawn(state::update_loop(state.clone()));

    match tokio::try_join!(update_handle, grpc_handle) {
        Ok(_) => (),
        Err(e) => error!("Something failed: {}", e),
    }

    info!("Stopping gracefully");
    Ok(())
}
