use clap::clap_app;
use log::*;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tokio::sync::mpsc::unbounded_channel;

use hydra::defs::sigint_handler;
use hydra::mix::directory_client::{self, Client};
use hydra::mix::epoch_worker::Worker;
use hydra::mix::sender;
use hydra::mix::{self, simple_relay};

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

    let running = Arc::new(AtomicBool::new(true));
    let sigint_handle = tokio::spawn(sigint_handler(running.clone()));

    // directory client config
    let mix_addr: std::net::SocketAddr = args.value_of("sockAddr").unwrap().parse()?;
    let directory_addr = args.value_of("directory").unwrap().parse()?;

    let dir_cfg = directory_client::Config {
        addr: mix_addr.ip(),
        entry_port: mix_addr.port(),
        relay_port: mix_addr.port(),
        rendezvous_port: mix_addr.port() + 100,
        directory_addr,
    };

    let dir_client = Arc::new(Client::new(dir_cfg));
    let dir_client_handle = tokio::spawn(directory_client::run(dir_client.clone()));

    if args.is_present("simple") {
        // simple relay only
        let state = Arc::new(simple_relay::State::new());
        let grpc_handle = simple_relay::spawn_service(state.clone(), mix_addr);
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

        // setup gRPC
        let grpc_state = Arc::new(mix::grpc::State::new(
            dir_client.clone(),
            setup_rx_queue_senders,
            cell_rx_queue_senders,
        ));
        let grpc_handle = mix::grpc::spawn_service(grpc_state.clone(), mix_addr);

        // setup channels for communication between main worker and sender
        let (setup_sender, setup_receiver) = spmc::channel();
        let (relay_sender, relay_receiver) = spmc::channel();

        // setup sender
        let sender = Arc::new(sender::State::new(
            dir_client.clone(),
            setup_receiver,
            relay_receiver,
        ));
        let sender_handle = tokio::spawn(sender::run(sender));

        // setup main worker
        let mut worker = Worker::new(
            running.clone(),
            dir_client.clone(),
            grpc_state.clone(),
            setup_rx_queue_receivers,
            setup_sender,
            cell_rx_queue_receivers,
            relay_sender,
        );
        let main_handle = tokio::task::spawn_blocking(move || worker.run());

        match tokio::try_join!(
            sigint_handle,
            dir_client_handle,
            grpc_handle,
            sender_handle,
            main_handle
        ) {
            Ok(_) => (),
            Err(e) => error!("Something failed: {}", e),
        }
    }

    info!("Stopping gracefully by unregistering at the directory service");
    dir_client.unregister().await;
    info!("Unregistration successful");
    // TODO security this most likely does not terminate the worker gracefully, keys might be
    // leaked -> better use a channel to signal shutdown
    Ok(())
}
