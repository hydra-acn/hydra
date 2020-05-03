use log::*;
use simplelog::{LevelFilter, TermLogger, TerminalMode};
use std::sync::Arc;
use tonic::transport::Server;

use hydra::directory::grpc::Service;
use hydra::directory::state::State;
use hydra::tonic_directory::directory_server::DirectoryServer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    TermLogger::init(
        LevelFilter::Debug,
        simplelog::Config::default(),
        TerminalMode::Mixed,
    )?;
    info!("Starting directory service");

    let state = Arc::new(State::new());

    let service = Service::new(state.clone());
    let local_addr = "127.0.0.1:4242".parse()?;
    let server = Server::builder()
        .add_service(DirectoryServer::new(service))
        .serve(local_addr);

    let server_handle = tokio::spawn(server);
    let _ = server_handle.await?;

    info!("Stopping gracefully");
    Ok(())
}
