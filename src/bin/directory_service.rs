use log::*;
use simplelog::{LevelFilter, TermLogger, TerminalMode};
use std::sync::Arc;

use hydra::directory::grpc;
use hydra::directory::state::{self, Config, State};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    TermLogger::init(
        LevelFilter::Debug,
        simplelog::Config::default(),
        TerminalMode::Mixed,
    )?;
    info!("Starting directory service");

    let cfg = Config::default();
    let state = Arc::new(State::new(cfg));

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
