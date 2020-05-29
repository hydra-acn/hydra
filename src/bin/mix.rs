use clap::clap_app;
use log::*;
use std::sync::Arc;

use hydra::defs::sigint_handler;
use hydra::mix::directory_client::{self, Client};
use hydra::mix::simple_relay;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    hydra::log::init();
    info!("Starting mix");

    let args = clap_app!(simple_relay =>
        (version: hydra::defs::hydra_version())
        (about: "Mix for the Hydra system")
        (@arg sockAddr: +required "Socket address to listen on, e.g. 127.0.0.1:9001")
        (@arg directory: -d --directory +takes_value default_value("141.24.207.69:9000") "Address and port of directory service")
        (@arg simple: --simple "Start a simple relay instead of a real mix")
    )
    .get_matches();

    let sigint_handle = tokio::spawn(sigint_handler());

    // directory client config
    let local_addr: std::net::SocketAddr = args.value_of("sockAddr").unwrap().parse()?;
    let directory_addr = args.value_of("directory").unwrap().parse()?;

    let dir_cfg = directory_client::Config {
        addr: local_addr.ip(),
        entry_port: local_addr.port(),
        relay_port: local_addr.port(),
        rendezvous_port: local_addr.port() + 100,
        directory_addr,
    };

    let dir_client = Arc::new(Client::new(dir_cfg));
    let dir_client_handle = tokio::spawn(directory_client::run(dir_client.clone()));

    if args.is_present("simple") {
        // simple relay only
        let state = Arc::new(simple_relay::State::new());
        let grpc_handle = simple_relay::spawn_service(state.clone(), local_addr);
        let garbage_handle = tokio::spawn(simple_relay::garbage_collector(state.clone()));

        match tokio::try_join!(
            sigint_handle,
            grpc_handle,
            garbage_handle,
            dir_client_handle,
        ) {
            Ok(_) => (),
            Err(e) => error!("Something failed: {}", e),
        }
    } else {
        // real mix
        unimplemented!();
    }

    info!("Stopping gracefully by unregistering at the directory service");
    dir_client.unregister().await;
    Ok(())
}
