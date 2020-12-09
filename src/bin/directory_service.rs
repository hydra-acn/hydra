use clap::{clap_app, value_t};
use log::*;
use std::sync::Arc;
use std::time::Duration;

use hydra::crypto::key::Key;
use hydra::crypto::tls::ServerCredentials;
use hydra::directory::grpc;
use hydra::directory::state::{self, ConfigBuilder, State};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = clap_app!(directory_service =>
        (version: hydra::defs::hydra_version())
        (about: "Simple, non distributed, implementation of the Hydra directory service")
        (@arg addr: +required "IP address to listen on")
        (@arg port: -p --port +takes_value default_value("9000") "Port to listen on (with TLS)")
        (@arg plain_port: --("plain-port") +takes_value default_value("8999") "Port to listen on without TLS. Clients should use this for debugging only")
        (@arg key_path: +required "Path to key file")
        (@arg cert_path: +required "Path to certificate file")
        (@arg path_len: -l --("path-len") +takes_value default_value("3") "Number of mixes for client circuits")
        (@arg number_of_rounds: -k --("comm-rounds") +takes_value default_value("8") "Number of communication rounds per epoch")
        (@arg round_dur: -d --("round-duration") +takes_value default_value("7.0") "Duration of one communication round in seconds")
        (@arg round_wait: -w --("round-wait") +takes_value default_value("13.0") "Duration between communication rounds in seconds")
        (@arg verbose: -v --verbose ... "Also show log of dependencies")
    )
    .get_matches();

    hydra::log_cfg::init(args.occurrences_of("verbose") > 0);
    info!("Starting directory service");

    let l = value_t!(args, "path_len", u8).unwrap();
    let k = value_t!(args, "number_of_rounds", u32).unwrap();
    let round_dur_secs = value_t!(args, "round_dur", f64).unwrap();
    let round_wait_secs = value_t!(args, "round_wait", f64).unwrap();
    let cfg = ConfigBuilder::default()
        .path_len(l)
        .number_of_rounds(k)
        .round_duration(Duration::from_secs_f64(round_dur_secs))
        .round_waiting(Duration::from_secs_f64(round_wait_secs))
        .build_valid()?;
    let state = Arc::new(State::new(cfg));

    let key = Key::read_from_file(args.value_of("key_path").unwrap()).expect("Failed to read File");
    let cert = std::fs::read_to_string(args.value_of("cert_path").unwrap())?;
    let tls_cred = ServerCredentials::new(key, &cert);

    let addr = args.value_of("addr").unwrap();
    let port = value_t!(args, "port", u16).unwrap();
    let local_endpoint = format!("{}:{}", addr, port).parse()?;
    let (grpc_handle, _) =
        grpc::spawn_service(state.clone(), local_endpoint, Some(tls_cred)).await?;

    let plain_port = value_t!(args, "plain_port", u16).unwrap();
    let plain_local_endpoint = format!("{}:{}", addr, plain_port).parse()?;
    let (plain_grpc_handle, _) =
        grpc::spawn_service(state.clone(), plain_local_endpoint, None).await?;

    let update_handle = tokio::spawn(state::update_loop(state.clone()));

    match tokio::try_join!(update_handle, grpc_handle, plain_grpc_handle) {
        Ok(_) => (),
        Err(e) => error!("Something failed: {}", e),
    }

    info!("Stopping gracefully");
    Ok(())
}
