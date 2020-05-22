use clap::clap_app;
use log::*;
use std::sync::Arc;

use hydra::mix::simple_relay::{State, spawn_service, garbage_collector};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    hydra::log::init();
    info!("Starting directory service");

    let args = clap_app!(simple_relay =>
        (version: hydra::defs::hydra_version())
        (about: "Simple relay for cells without onion encryption")
        (@arg sockAddr: +required "Socket address to listen on, e.g. 127.0.0.1:9001")
    )
    .get_matches();

    let state = Arc::new(State::new());
    let local_addr = args.value_of("sockAddr").unwrap().parse()?;

    let grpc_handle = spawn_service(state.clone(), local_addr);
    let garbage_handle = tokio::spawn(garbage_collector(state.clone()));

    match tokio::try_join!(grpc_handle, garbage_handle) {
        Ok(_) => (),
        Err(e) => error!("Something failed: {}", e),
    }

    info!("Stopping gracefully");
    Ok(())
}
