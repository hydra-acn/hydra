use clap::{clap_app, value_t};
use log::*;
use std::sync::Arc;

use hydra::directory::grpc;
use hydra::directory::state::{self, Config, State};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    hydra::log::init();
    info!("Starting directory service");

    let args = clap_app!(directory_service =>
        (version: hydra::defs::hydra_version())
        (about: "Simple, non distributed, implementation of the Hydra directory service")
        (@arg addr: +required "IP address to listen on")
        (@arg phaseDuration: -d --duration +takes_value default_value("600") "Duration of one phase (setup/communication have the same duration")
    )
    .get_matches();

    let phase_duration = value_t!(args, "phaseDuration", u64).unwrap();
    let mut config = Config::default();
    config.phase_duration = phase_duration;
    let state = Arc::new(State::new(config));

    let local_addr = format!("{}:9000", args.value_of("addr").unwrap()).parse()?;
    let (grpc_handle, _) = grpc::spawn_service(state.clone(), local_addr).await?;

    let update_handle = tokio::spawn(state::update_loop(state.clone()));

    match tokio::try_join!(update_handle, grpc_handle) {
        Ok(_) => (),
        Err(e) => error!("Something failed: {}", e),
    }

    info!("Stopping gracefully");
    Ok(())
}
