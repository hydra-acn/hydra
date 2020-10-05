use clap::{clap_app, value_t};
use futures_util::stream;
use log::*;
use rand::seq::IteratorRandom;
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, RwLock};
use std::thread::sleep;
use tokio::time::{delay_for, Duration};

use hydra::client::directory_client;
use hydra::defs::{CircuitId, RoundNo};
use hydra::epoch::{current_time, current_time_in_secs, EpochNo};
use hydra::grpc::type_extensions::CellCmd;
use hydra::net::PacketWithNextHop;
use hydra::tonic_directory::{EpochInfo, MixInfo};
use hydra::tonic_mix::{Cell, LatePollRequest, SetupPacket};

type DirClient = hydra::client::directory_client::Client;
type MixChannel = hydra::tonic_mix::mix_client::MixClient<tonic::transport::Channel>;
type MixChannelPool = hydra::mix::channel_pool::ChannelPool<MixChannel>;
type ClientCircuit = hydra::client::circuit::Circuit;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = clap_app!(load_gen =>
        (version: hydra::defs::hydra_version())
        (about: "Load generator (simulating clients) for the Hydra system")
        (@arg n: +required "How many circuits to build each epoch")
        (@arg runs: -r --runs +takes_value default_value("32") "How many epochs to run")
        (@arg dirDom: -d --("directory-dom") +takes_value default_value("hydra-swp.prakinf.tu-ilmenau.de") "Address of directory service")
        (@arg dirPort: -p --("directory-port") +takes_value default_value("9000") "Port of directory service")
        (@arg certPath: -c --("directory-certificate") +takes_value "Path to directory server certificate (only necessary if trust is not anchored in system")
        (@arg verbose: -v --verbose ... "Also show log of dependencies")
    )
    .get_matches();

    hydra::log_cfg::init(args.occurrences_of("verbose") > 0);
    info!("Starting load generator");

    let n = value_t!(args, "n", u64).expect("n has to an unsigned integer");
    let runs = value_t!(args, "runs", u32).expect("runs has to an unsigned integer");

    // directory client config
    let dir_domain = args.value_of("dirDom").unwrap().parse()?;
    let dir_port = value_t!(args, "dirPort", u16).unwrap();
    let dir_certificate = match args.value_of("certPath") {
        Some(path) => Some(std::fs::read_to_string(&path)?),
        None => None,
    };

    let dir_client = Arc::new(DirClient::new(dir_domain, dir_port, dir_certificate));
    let dir_client_handle = tokio::spawn(directory_client::run(dir_client.clone()));
    let setup_handle = tokio::task::spawn(setup_loop(dir_client.clone(), n, runs));
    match tokio::try_join!(dir_client_handle, setup_handle) {
        Ok(_) => (),
        Err(e) => error!("Something panicked: {}", e),
    }
    Ok(())
}

async fn setup_loop(dir_client: Arc<DirClient>, n: u64, runs: u32) {
    for r in 0..runs {
        let mut maybe_epoch = dir_client.next_epoch_info();
        while let None = maybe_epoch {
            warn!("Don't know the next epoch for setup, retrying in 5 seconds");
            sleep(Duration::from_secs(5));
            maybe_epoch = dir_client.next_epoch_info();
        }
        let epoch = maybe_epoch.expect("Checked before");
        let epoch_no = epoch.epoch_no;

        let channels = prepare_entry_channels(&epoch).await;
        let circuits: RwLock<Vec<Arc<Circuit>>> = RwLock::default();
        let pkt_by_dst: RwLock<HashMap<SocketAddr, Vec<SetupPacket>>> = RwLock::default();

        info!("Starting to create setup packets for epoch {}", epoch_no);
        // first, create all packets and circuit state
        (0..n).into_par_iter().for_each(|_| {
            if current_time_in_secs() > epoch.setup_start_time - 1 {
                return;
            }

            let path = dir_client
                .select_path(epoch_no)
                .expect("Path selection failed");
            let (circuit, setup_pkt) = Circuit::new(epoch_no, path);
            circuits.write().unwrap().push(Arc::new(circuit));

            let mut map = pkt_by_dst.write().unwrap();
            match map.get_mut(setup_pkt.next_hop()) {
                Some(vec) => vec.push(setup_pkt.into_inner()),
                None => {
                    map.insert(*setup_pkt.next_hop(), vec![setup_pkt.into_inner()]);
                    ()
                }
            };
        });

        // move packets out of RwLock
        let pkt_map;
        {
            let mut pkt_map_guard = pkt_by_dst.write().unwrap();
            pkt_map = std::mem::replace(&mut *pkt_map_guard, HashMap::new());
        }

        info!("Starting to send setup packets for epoch {}", epoch_no);
        for (dst, pkts) in pkt_map.into_iter() {
            let mut channel = channels
                .get_channel(&dst)
                .await
                .expect(&format!("No connection to {}", dst));
            channel
                .stream_setup_circuit(tonic::Request::new(stream::iter(pkts.into_iter())))
                .await
                .expect("Sending setup packets failed");
        }
        info!("Sending setup packets for epoch {} done", epoch_no);

        // move circuits out of the RwLock
        let final_circuits;
        {
            let mut circuit_guard = circuits.write().unwrap();
            final_circuits = std::mem::replace(&mut *circuit_guard, Vec::new());
        }
        if final_circuits.len() != n as usize {
            warn!(
                "Did not create all circuits in time; {} missing",
                n as usize - final_circuits.len()
            );
        }

        // spawn a task to handle the communication phase and wait till next epoch
        let wait_for = Duration::from_secs(epoch.setup_start_time - current_time_in_secs() + 1);
        tokio::spawn(run_epoch_communication(epoch, final_circuits, n, r));
        delay_for(wait_for).await;
    }
    // wait for the communication phase of the last epoch to end (ends when communication of next
    // epoch start)
    let next_epoch = dir_client.next_epoch_info().unwrap();
    let wait_for =
        Duration::from_secs(next_epoch.communication_start_time - current_time_in_secs() + 1);
    delay_for(wait_for).await;
    info!("All communication should be done now, feel free to exit with Ctrl-C :)");
}

async fn run_epoch_communication(epoch: EpochInfo, circuits: Vec<Arc<Circuit>>, n: u64, r: u32) {
    let epoch_no = epoch.epoch_no;
    let round_duration = Duration::from_secs(epoch.round_duration as u64);
    let round_waiting = Duration::from_secs(epoch.round_waiting as u64);
    let round_interval = round_duration + round_waiting;
    let mut round_start = Duration::from_secs(epoch.communication_start_time) - round_waiting
        + Duration::from_secs(1);

    let channels = Arc::new(prepare_entry_channels(&epoch).await);
    for round_no in 0..epoch.number_of_rounds {
        // wait till round starts
        delay_till(round_start).await;
        info!(
            "Send/receiving for round {} in epoch {}",
            round_no, epoch_no
        );

        let mut tasks = Vec::new();
        for circ in circuits.iter() {
            tasks.push(tokio::spawn(run_send_and_receive(
                circ.clone(),
                round_no,
                channels.clone(),
            )));
        }
        // wait for all tasks to finish
        for t in tasks.iter_mut() {
            t.await.expect("Task failed");
        }
        info!(".. Send/receiving done");
        round_start += round_interval;
    }

    // wait and get the nacks (last round)
    delay_till(round_start).await;
    let mut tasks = Vec::new();
    for circ in circuits.iter() {
        tasks.push(tokio::spawn(run_receive_nack(
            circ.clone(),
            channels.clone(),
        )));
    }
    // wait for all tasks to finish
    for t in tasks.iter_mut() {
        t.await.expect("Task failed");
    }

    // collect stats
    let mut avg_loss_rate = 0f64;
    for circ in circuits.iter() {
        let loss_rate = circ.lost_cells() as f64 / epoch.number_of_rounds as f64;
        avg_loss_rate += loss_rate / circuits.len() as f64;
    }
    let mut result = format!("{}, {}, {}, {}", n, circuits.len(), avg_loss_rate, r);

    info!("Communication for epoch {} done", epoch_no);
    info!("Results: {}", result);

    let result_path = "load-gen-results.dat";
    let (file, is_new) = match Path::new(result_path).exists() {
        false => (File::create(result_path), true),
        true => (OpenOptions::new().append(true).open(result_path), false),
    };

    match file {
        Ok(mut f) => {
            if is_new {
                f.write_all(b"n, n_dash, avg_loss, run\n")
                    .unwrap_or_else(|e| warn!("Writing header failed: {}", e));
            };
            result += "\n";
            f.write_all(result.as_bytes())
                .unwrap_or_else(|e| warn!("Writing results failed: {}", e));
        }
        Err(e) => warn!("Opening result file failed: {}", e),
    }
}

async fn run_send_and_receive(
    circ: Arc<Circuit>,
    round_no: RoundNo,
    channels: Arc<MixChannelPool>,
) {
    circ.send_and_receive(round_no, channels).await;
}

async fn run_receive_nack(circ: Arc<Circuit>, channels: Arc<MixChannelPool>) {
    circ.receive_nack(channels).await;
}

async fn delay_till(time: Duration) {
    let duration = time.checked_sub(current_time()).expect("Out of sync");
    delay_for(duration).await;
}

async fn prepare_entry_channels(epoch: &EpochInfo) -> MixChannelPool {
    let mix_addr_vec = epoch
        .mixes
        .iter()
        .map(|m| m.entry_address().expect("Getting mix address failed"))
        .collect::<Vec<_>>();
    let channels: MixChannelPool = MixChannelPool::new();
    channels.prepare_channels(&mix_addr_vec).await;
    channels
}

struct Circuit {
    circuit: ClientCircuit,
    path: Vec<MixInfo>,
    entry_addr: SocketAddr,
    expected_cell: RwLock<Option<Cell>>,
    lost_cells: AtomicU32,
}

impl std::fmt::Debug for Circuit {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let path_str = self
            .path
            .iter()
            .map(|m| m.entry_port.to_string())
            .collect::<Vec<_>>()
            .join(" -> ");
        write!(f, "[{}: {}]", self.circuit_id(), path_str)
    }
}

impl Circuit {
    pub fn new(epoch_no: EpochNo, path: Vec<MixInfo>) -> (Self, PacketWithNextHop<SetupPacket>) {
        let (client_circuit, setup_pkt) =
            ClientCircuit::new(epoch_no, &path, Vec::new()).expect("Creating setup packet failed");
        let entry_addr = path[0]
            .entry_address()
            .expect("Getting entry address failed");
        let circuit = Circuit {
            circuit: client_circuit,
            path,
            entry_addr,
            expected_cell: RwLock::new(None),
            lost_cells: AtomicU32::new(0),
        };
        (circuit, setup_pkt)
    }

    pub fn circuit_id(&self) -> CircuitId {
        self.circuit.circuit_id()
    }

    pub fn create_plaintext_cell(&self, round_no: RoundNo) -> Cell {
        let mut cell = Cell::dummy(self.circuit.circuit_id(), round_no);
        let rng = &mut rand::thread_rng();
        let token = self
            .circuit
            .dummy_tokens()
            .iter()
            .choose(rng)
            .expect("Expected at least one token");
        cell.set_token(*token);
        // we want the cell echoed back by the rendezvous service
        cell.set_command(CellCmd::Broadcast);
        cell
    }

    pub async fn send_and_receive(&self, round_no: RoundNo, channels: Arc<MixChannelPool>) {
        let plain_cell = self.create_plaintext_cell(round_no);
        let mut cell = plain_cell.clone();
        self.circuit
            .onion_encrypt(&mut cell)
            .expect("Onion encryption failed");
        let mut channel = channels
            .get_channel(&self.entry_addr)
            .await
            .expect(&format!("No connection to {}", self.entry_addr));
        let mut received = channel
            .send_and_receive(tonic::Request::new(cell))
            .await
            .expect("Send and receive failed")
            .into_inner()
            .cells;
        self.check_received(&mut received);
        *self.expected_cell.write().unwrap() = Some(plain_cell);
    }

    pub fn check_received(&self, cells: &mut [Cell]) {
        if let Some(expected) = &*self.expected_cell.read().unwrap() {
            if cells.len() > 1 {
                warn!("{:?} Received {} cells instead of 1", self, cells.len());
            }
            if let Some(cell) = cells.last_mut() {
                self.circuit
                    .onion_decrypt(cell)
                    .expect("Onion decryption failed");
                if expected.onion != cell.onion {
                    warn!("{:?} Received cell is not as expected", self);
                    self.lost_cells.fetch_add(1, Ordering::Relaxed);
                }
            } else {
                warn!("{:?} Did not receive a cell back", self);
                self.lost_cells.fetch_add(1, Ordering::Relaxed);
            }
        } else {
            if !cells.is_empty() {
                warn!("{:?} Received {} cells instead of 0", self, cells.len());
            }
        }
    }

    pub async fn receive_nack(&self, channels: Arc<MixChannelPool>) {
        let mut channel = channels
            .get_channel(&self.entry_addr)
            .await
            .expect(&format!("No connection to {}", self.entry_addr));
        let poll_req = LatePollRequest {
            circuit_ids: vec![self.circuit_id()],
        };
        let mut cells = channel
            .late_poll(tonic::Request::new(poll_req))
            .await
            .expect("Late poll failed")
            .into_inner()
            .cells;

        if cells.is_empty() {
            warn!("{:?} Late polled nothing", self);
            self.lost_cells.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if cells.len() > 1 {
            warn!("{:?} Late polled {} cells instead of 1", self, cells.len());
        }
        let nack = cells.last_mut().expect("Checked before");
        self.circuit
            .onion_decrypt(nack)
            .expect("Onion decryption failed");
        for b in &nack.onion[1..8] {
            if *b != 0 {
                warn!("{:?} Nack is broken", self);
                self.lost_cells.fetch_add(1, Ordering::Relaxed);
                return;
            }
        }
    }

    pub fn lost_cells(&self) -> u32 {
        self.lost_cells.load(Ordering::Relaxed)
    }
}
