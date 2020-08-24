use clap::{clap_app, value_t};
use log::*;
use std::sync::atomic::{self, AtomicBool};
use std::sync::Arc;
use tokio::sync::mpsc::unbounded_channel;

use hydra::defs::sigint_handler;
use hydra::mix::directory_client::{self, Client};
use hydra::mix::epoch_worker::Worker;
use hydra::mix::{self, sender, simple_relay};
use hydra::rendezvous;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = clap_app!(hydra_mix =>
        (version: hydra::defs::hydra_version())
        (about: "Mix for the Hydra system")
        (@arg sockAddr: +required "Socket address to listen on, e.g. 127.0.0.1:9001")
        (@arg dirDom: -d --("directory-dom") +takes_value default_value("hydra-swp.prakinf.tu-ilmenau.de") "Address of directory service")
        (@arg dirPort: -p --("directory-port") +takes_value default_value("9000") "Port of directory service")
        (@arg certPath: -c --("directory-certificate") +takes_value "Path to directory server certificate (only necessary if trust is not anchored in system")
        (@arg simple: --simple "Start a simple relay instead of a real mix")
        (@arg verbose: -v --verbose ... "Also show log of dependencies")
    )
    .get_matches();

    hydra::log_cfg::init(args.occurrences_of("verbose") > 0);
    info!("Starting mix");

    let running = Arc::new(AtomicBool::new(true));
    let sigint_handle = tokio::spawn(sigint_handler(running.clone()));

    // directory client config
    let mix_addr: std::net::SocketAddr = args.value_of("sockAddr").unwrap().parse()?;
    let directory_domain = args.value_of("dirDom").unwrap().parse()?;
    let directory_port = value_t!(args, "dirPort", u16).unwrap();

    let directory_certificate = match args.value_of("certPath") {
        Some(path) => Some(std::fs::read_to_string(&path)?),
        None => None,
    };

    let dir_cfg = directory_client::Config {
        addr: mix_addr.ip(),
        entry_port: mix_addr.port(),
        relay_port: mix_addr.port(),
        rendezvous_port: mix_addr.port() + 100,
        directory_certificate,
        directory_domain,
        directory_port,
    };
    let rendezvous_addr: std::net::SocketAddr =
        format!("{}:{}", dir_cfg.addr, dir_cfg.rendezvous_port).parse()?;

    let dir_client = Arc::new(Client::new(dir_cfg));
    let dir_client_handle = tokio::spawn(directory_client::run(dir_client.clone()));

    if args.is_present("simple") {
        // simple relay only
        let state = Arc::new(simple_relay::State::new());
        let (grpc_handle, _) = simple_relay::spawn_service(state.clone(), mix_addr, None).await?;
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
        // setup channels for communication between gRPC service and main worker
        let mut setup_rx_queue_senders = Vec::new();
        let mut setup_rx_queue_receivers = Vec::new();
        let mut cell_rx_queue_senders = Vec::new();
        let mut cell_rx_queue_receivers = Vec::new();
        let no_of_worker_threads = 2u8;
        for _ in 0..no_of_worker_threads {
            let (sender, receiver) = unbounded_channel();
            setup_rx_queue_senders.push(sender);
            setup_rx_queue_receivers.push(receiver);

            let (sender, receiver) = unbounded_channel();
            cell_rx_queue_senders.push(sender);
            cell_rx_queue_receivers.push(receiver);
        }

        // setup mix gRPC
        let mix_grpc_state = Arc::new(mix::grpc::State::new(
            dir_client.clone(),
            setup_rx_queue_senders,
            cell_rx_queue_senders,
        ));
        let (mix_grpc_handle, _) =
            mix::grpc::spawn_service(mix_grpc_state.clone(), mix_addr, None).await?;

        // setup rendezvous gRPC
        let rendezvous_grpc_state = Arc::new(rendezvous::grpc::State::new(dir_client.clone()));
        let (rendezvous_grpc_handle, _) =
            rendezvous::grpc::spawn_service(rendezvous_grpc_state.clone(), rendezvous_addr, None)
                .await?;

        // setup channels for communication between main worker and sender
        let (setup_sender, setup_receiver) = spmc::channel();
        let (subscribe_sender, subscribe_receiver) = spmc::channel();
        let (relay_sender, relay_receiver) = spmc::channel();
        let (publish_sender, publish_receiver) = spmc::channel();

        // setup sender
        let sender = Arc::new(sender::State::new(
            dir_client.clone(),
            setup_receiver,
            subscribe_receiver,
            relay_receiver,
            publish_receiver,
        ));
        let sender_handle = tokio::spawn(sender::run(sender));

        // setup main worker
        let mut worker = Worker::new(
            running.clone(),
            dir_client.clone(),
            mix_grpc_state.clone(),
            setup_rx_queue_receivers,
            setup_sender,
            subscribe_sender,
            cell_rx_queue_receivers,
            relay_sender,
            publish_sender,
        );
        let main_handle = tokio::task::spawn_blocking(move || worker.run());

        match tokio::try_join!(
            sigint_handle,
            dir_client_handle,
            mix_grpc_handle,
            rendezvous_grpc_handle,
            sender_handle,
            main_handle
        ) {
            Ok(_) => (),
            Err(e) => error!("Something failed: {}", e),
        }
    }

    info!("Stopping gracefully by unregistering at the directory service");
    dir_client
        .unregister()
        .await
        .unwrap_or_else(|e| warn!("Unregister failed: {}", e));

    // inform threads that we are not supposed to run anymore
    running.store(false, atomic::Ordering::SeqCst);
    Ok(())
}
