use clap::clap_app;
use log::*;
use std::sync::Arc;

use hydra::defs::sigint_handler;
use hydra::mix::directory_client::{self, Client};
use hydra::mix::simple_relay::{garbage_collector, spawn_service, State};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    hydra::log::init();
    info!("Starting simple relay");

    let args = clap_app!(simple_relay =>
        (version: hydra::defs::hydra_version())
        (about: "Simple relay for cells without onion encryption")
        (@arg sockAddr: +required "Socket address to listen on, e.g. 127.0.0.1:9001")
    )
    .get_matches();

    // TODO don't hardcode address
    let directory_addr = "141.24.207.69:9000".parse()?;
    let local_addr: std::net::SocketAddr = args.value_of("sockAddr").unwrap().parse()?;

    let dir_cfg = directory_client::Config {
        addr: local_addr.ip(),
        entry_port: local_addr.port(),
        relay_port: local_addr.port(),
        directory_addr,
    };

    let dir_client = Arc::new(Client::new(dir_cfg));
    let state = Arc::new(State::new());

    let grpc_handle = spawn_service(state.clone(), local_addr);
    let garbage_handle = tokio::spawn(garbage_collector(state.clone()));
    let dir_client_handle = tokio::spawn(directory_client::run(dir_client.clone()));
    let sigint_handle = tokio::spawn(sigint_handler());

    match tokio::try_join!(grpc_handle, garbage_handle, dir_client_handle, sigint_handle) {
        Ok(_) => (),
        Err(e) => error!("Something failed: {}", e),
    }

    info!("Stopping gracefully by unregistering at the directory service");
    dir_client.unregister().await;
    Ok(())
}
