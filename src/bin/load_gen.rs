use clap::{clap_app, value_t};
use futures_util::stream;
use log::*;
use rand::seq::IteratorRandom;
use rand::thread_rng;
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, RwLock};
use std::thread;
use tokio::time::delay_for as sleep;
use tokio::time::Duration;

use hydra::client::directory_client;
use hydra::defs::RoundNo;
use hydra::epoch::{current_time, current_time_in_secs, EpochNo};
use hydra::net::cell::CellCmd;
use hydra::net::channel_pool::{ChannelPool, MixChannel};
use hydra::net::PacketWithNextHop;
use hydra::tonic_directory::EpochInfo;
use hydra::tonic_mix::{Cell, LatePollRequest, SendAndLatePollRequest, SetupPacket};

type DirClient = hydra::client::directory_client::Client;
type MixChannelPool = ChannelPool<MixChannel>;
type ClientCircuit = hydra::client::circuit::Circuit;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = clap_app!(load_gen =>
        (version: hydra::defs::hydra_version())
        (about: "Load generator (simulating clients) for the Hydra system")
        (@arg n: +required "How many circuits to build each epoch")
        (@arg runs: -r --runs +takes_value default_value("3") "How many epochs to run")
        (@arg dirDom: -d --("directory-dom") +takes_value default_value("hydra-swp.prakinf.tu-ilmenau.de") "Address of directory service")
        (@arg dirPort: -p --("directory-port") +takes_value default_value("9000") "Port of directory service")
        (@arg certPath: -c --("directory-certificate") +takes_value "Path to directory server certificate (only necessary if trust is not anchored in system")
        (@arg nat: --nat "Query the testbed NAT addresses -> mandatory for external load generation")
        (@arg noWait: --("no-wait") "Don't wait for the next epoch start")
        (@arg verbose: -v --verbose ... "Also show log of dependencies")
    )
    .get_matches();

    hydra::log_cfg::init(args.occurrences_of("verbose") > 0);
    info!("Starting load generator");

    let n = value_t!(args, "n", usize).expect("n has to an unsigned integer");
    let runs = value_t!(args, "runs", usize).expect("runs has to an unsigned integer");

    // directory client config
    let dir_domain = args.value_of("dirDom").unwrap().parse()?;
    let dir_port = value_t!(args, "dirPort", u16).unwrap();
    let dir_certificate = match args.value_of("certPath") {
        Some(path) => Some(std::fs::read_to_string(&path)?),
        None => None,
    };
    let nat = args.is_present("nat");
    let wait = !args.is_present("noWait");

    let dir_client = Arc::new(DirClient::new(dir_domain, dir_port, dir_certificate, nat));
    let dir_client_handle = tokio::spawn(directory_client::run(dir_client.clone()));
    let setup_handle = tokio::task::spawn(setup_loop(dir_client.clone(), n, runs, wait));
    match tokio::try_join!(dir_client_handle, setup_handle) {
        Ok(_) => (),
        Err(e) => error!("Something panicked: {}", e),
    }
    Ok(())
}

async fn setup_loop(dir_client: Arc<DirClient>, n: usize, runs: usize, wait: bool) {
    let mut maybe_epoch = dir_client.next_epoch_info();
    while let None = maybe_epoch {
        warn!("Don't know the next epoch for setup, retrying in 5 seconds");
        thread::sleep(Duration::from_secs(5));
        maybe_epoch = dir_client.next_epoch_info();
    }

    let next_epoch = maybe_epoch.expect("Checked before");
    let mut epoch_no = next_epoch.epoch_no;
    let epoch_duration =
        Duration::from_secs(next_epoch.communication_start_time - next_epoch.setup_start_time);

    if wait {
        // if we don't have plenty of time left (setup duration - 10s), skip this epoch
        let wait_for =
            Duration::from_secs(next_epoch.setup_start_time - current_time_in_secs() + 1);
        if wait_for < epoch_duration - Duration::from_secs(10) {
            sleep(wait_for).await;
            epoch_no += 1;
        }
    }

    let mut clients: Vec<Arc<Client>> = Vec::new();
    for i in 0..n {
        clients.push(Arc::new(Client::new(i, dir_client.clone(), epoch_no)));
    }

    for r in 0..runs {
        let epoch = dir_client
            .get_epoch_info(epoch_no)
            .expect("Not enough epochs in advance?");

        let channels = prepare_entry_channels(&epoch).await;
        let pkt_by_dst: RwLock<HashMap<SocketAddr, Vec<SetupPacket>>> = RwLock::default();

        info!("Starting to create setup packets for epoch {}", epoch_no);
        // first, create all packets and circuit state
        (0..n).into_par_iter().for_each(|i| {
            if current_time_in_secs() > epoch.setup_start_time - 1 {
                return;
            }

            let setup_pkt = clients[i].new_circuit(epoch_no, dir_client.clone());

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

        // spawn a task to handle the communication phase
        tokio::spawn(run_epoch_communication(epoch, clients.clone(), r, runs));
        epoch_no += 1;
    }

    // wait for the communication phase of the last epoch to end (ends when communication of next
    // epoch start)
    let not_our_epoch = dir_client
        .get_epoch_info(epoch_no)
        .expect("Not enough epochs in advance?");
    let wait_for =
        Duration::from_secs(not_our_epoch.communication_start_time - current_time_in_secs() + 1);
    sleep(wait_for).await;
    // TODO code: graceful shutdown?
    panic!("Panic intended :)");
}

async fn run_epoch_communication(
    epoch: EpochInfo,
    clients: Vec<Arc<Client>>,
    run: usize,
    runs: usize,
) {
    let epoch_no = epoch.epoch_no;
    let round_duration = Duration::from_secs_f64(epoch.round_duration);
    let round_waiting = Duration::from_secs_f64(epoch.round_waiting);
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
        for client in clients.iter() {
            tasks.push(tokio::spawn(run_communication_round(
                client.clone(),
                run,
                round_no,
                channels.clone(),
            )));
        }
        // wait for all tasks to finish
        for t in tasks.iter_mut() {
            t.await.expect("Task failed");
        }
        let mut cum_lost_cells = 0;
        let mut circuits_without_loss = 0;
        for client in clients.iter() {
            let lost = client.lost_cells();
            cum_lost_cells += lost;
            if lost == 0 {
                circuits_without_loss += 1;
            }
        }
        info!(".. Send/receiving done");
        info!(".. Cumulative total cell loss counter: {}", cum_lost_cells);
        info!(".. Clients without loss: {}", circuits_without_loss);
        round_start += round_interval;
        if run > 0 && round_no == 0 {
            epoch_stats(&epoch, run, &clients);
        }
    }

    // wait and do a plain late poll for the last run
    if run == runs - 1 {
        delay_till(round_start).await;
        info!("Late polling for epoch {}", epoch_no);
        let mut tasks = Vec::new();
        for client in clients.iter() {
            tasks.push(tokio::spawn(run_late_poll(
                client.clone(),
                run,
                channels.clone(),
            )));
        }
        // wait for all tasks to finish
        for t in tasks.iter_mut() {
            t.await.expect("Task failed");
        }
        info!(".. late polling done");
        epoch_stats(&epoch, run, &clients);
    }

    info!("Communication for epoch {} done", epoch_no);
}

fn epoch_stats(epoch: &EpochInfo, run: usize, clients: &Vec<Arc<Client>>) {
    let mut avg_loss_rate = 0f64;
    let result_path = "load-gen-results.dat";
    let (file, is_new) = match Path::new(result_path).exists() {
        false => (File::create(result_path), true),
        true => (OpenOptions::new().append(true).open(result_path), false),
    };

    match file {
        Ok(mut f) => {
            if is_new {
                f.write_all(b"n, loss, run, epoch\n")
                    .unwrap_or_else(|e| warn!("Writing header failed: {}", e));
            };
            for client in clients.iter() {
                let loss_rate = client.lost_cells() as f64 / epoch.number_of_rounds as f64;
                client.clear_lost_cells();
                avg_loss_rate += loss_rate / clients.len() as f64;
                let result_line = format!(
                    "{}, {}, {}, {}\n",
                    clients.len(),
                    loss_rate,
                    run,
                    epoch.epoch_no
                );
                f.write_all(result_line.as_bytes())
                    .unwrap_or_else(|e| warn!("Writing results failed: {}", e));
            }
        }
        Err(e) => warn!("Opening result file failed: {}", e),
    }
    info!(".. average loss rate: {}", avg_loss_rate);
}

async fn run_communication_round(
    client: Arc<Client>,
    run: usize,
    round_no: RoundNo,
    channels: Arc<MixChannelPool>,
) {
    client
        .run_communication_round(run, round_no, channels)
        .await;
}

async fn run_late_poll(client: Arc<Client>, run: usize, channels: Arc<MixChannelPool>) {
    client.late_poll(run, channels).await;
}

async fn delay_till(time: Duration) {
    match time.checked_sub(current_time()) {
        Some(d) => sleep(d).await,
        None => warn!("Out of sync!"),
    }
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

struct Client {
    client_id: usize,
    circuits: RwLock<Vec<ClientCircuit>>,
    entry_fingerprint: String,
    entry_addr: SocketAddr,
    expected_cell: RwLock<Option<Cell>>,
    lost_cells: AtomicU32,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "[Client {}]", self.client_id)
    }
}

impl Client {
    pub fn new(client_id: usize, dir_client: Arc<DirClient>, first_epoch: EpochNo) -> Self {
        let entry_guard = dir_client.select_entry_guard(first_epoch).unwrap();
        Client {
            client_id,
            circuits: RwLock::default(),
            entry_fingerprint: entry_guard.fingerprint.clone(),
            entry_addr: entry_guard.entry_address().unwrap(),
            expected_cell: RwLock::default(),
            lost_cells: AtomicU32::new(0),
        }
    }

    pub fn new_circuit(
        &self,
        epoch_no: EpochNo,
        dir_client: Arc<DirClient>,
    ) -> PacketWithNextHop<SetupPacket> {
        let path = dir_client
            .select_path_tunable(epoch_no, None, None, Some(self.entry_fingerprint.as_str()))
            .unwrap();
        let (circuit, setup_pkt) =
            ClientCircuit::new(epoch_no, &path, Vec::new(), None).expect("Creating circuit failed");
        self.circuits.write().unwrap().push(circuit);
        setup_pkt
    }

    pub async fn run_communication_round(
        &self,
        run: usize,
        round_no: RoundNo,
        channels: Arc<MixChannelPool>,
    ) {
        if round_no == 0 {
            self.send_and_late_poll(run, channels).await;
        } else {
            self.send_and_receive(run, round_no, channels).await;
        }
    }

    pub async fn send_and_late_poll(&self, run: usize, channels: Arc<MixChannelPool>) {
        let poll_req;
        let plain_cell;
        {
            let circuits_guard = self.circuits.read().unwrap();
            let circuit = circuits_guard.get(run).unwrap();
            let prev_circuit = match run {
                0 => None,
                _ => Some(circuits_guard.get(run - 1).unwrap()),
            };
            plain_cell = create_plaintext_cell(circuit, 0);
            let mut cell = plain_cell.clone();
            circuit
                .onion_encrypt(cell.round_no, &mut cell.onion)
                .expect("Onion encryption failed");

            let late_poll_ids = match prev_circuit {
                None => vec![],
                Some(circ) => vec![circ.circuit_id()],
            };
            poll_req = SendAndLatePollRequest {
                cell: Some(cell),
                circuit_ids: late_poll_ids,
            };
        }
        let mut channel = channels
            .get_channel(&self.entry_addr)
            .await
            .expect(&format!("No connection to {}", self.entry_addr));
        let mut received = channel
            .send_and_late_poll(tonic::Request::new(poll_req))
            .await
            .expect("Send and late poll failed")
            .into_inner()
            .cells;
        *self.expected_cell.write().unwrap() = Some(plain_cell);
        if run > 0 {
            self.check_nack(&mut received, run - 1);
        } else if received.len() > 0 {
            warn!("Late polled something, but did not expect it");
        }
    }

    pub async fn send_and_receive(
        &self,
        run: usize,
        round_no: RoundNo,
        channels: Arc<MixChannelPool>,
    ) {
        let mut cell;
        let plain_cell;
        {
            let circuits_guard = self.circuits.read().unwrap();
            let circuit = circuits_guard.get(run).unwrap();
            plain_cell = create_plaintext_cell(circuit, round_no);
            cell = plain_cell.clone();
            circuit
                .onion_encrypt(cell.round_no, &mut cell.onion)
                .expect("Onion encryption failed");
        }
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
        self.check_received(&mut received, run);
        *self.expected_cell.write().unwrap() = Some(plain_cell);
    }

    pub async fn late_poll(&self, run: usize, channels: Arc<MixChannelPool>) {
        let req;
        {
            let circuits_guard = self.circuits.read().unwrap();
            let circuit = circuits_guard.get(run).unwrap();
            req = LatePollRequest {
                circuit_ids: vec![circuit.circuit_id()],
            };
        }
        let mut channel = channels
            .get_channel(&self.entry_addr)
            .await
            .expect(&format!("No connection to {}", self.entry_addr));
        let mut received = channel
            .late_poll(tonic::Request::new(req))
            .await
            .expect("Late poll failed")
            .into_inner()
            .cells;
        self.check_nack(&mut received, run);
    }

    pub fn check_received(&self, cells: &mut [Cell], run: usize) {
        let circuits_guard = self.circuits.read().unwrap();
        let circuit = circuits_guard.get(run).unwrap();
        if let Some(expected) = &*self.expected_cell.read().unwrap() {
            if cells.len() > 1 {
                debug!("{:?} Received {} cells instead of 1", self, cells.len());
            }
            if let Some(cell) = cells.last_mut() {
                circuit
                    .onion_decrypt(cell.round_no, &mut cell.onion)
                    .expect("Onion decryption failed");
                if expected.onion != cell.onion {
                    debug!("{:?} Received cell is not as expected", self);
                    self.lost_cells.fetch_add(1, Ordering::Relaxed);
                }
            } else {
                debug!("{:?} Did not receive a cell back", self);
                self.lost_cells.fetch_add(1, Ordering::Relaxed);
            }
        } else {
            if !cells.is_empty() {
                debug!("{:?} Received {} cells instead of 0", self, cells.len());
            }
        }
    }

    pub fn check_nack(&self, cells: &mut [Cell], run: usize) {
        let circuits_guard = self.circuits.read().unwrap();
        let circuit = circuits_guard.get(run).unwrap();
        if cells.is_empty() {
            debug!("{:?} Late polled nothing", self);
            self.lost_cells.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if cells.len() > 1 {
            debug!("{:?} Late polled {} cells instead of 1", self, cells.len());
        }
        let nack = cells.last_mut().expect("Checked before");

        circuit
            .onion_decrypt(nack.round_no, &mut nack.onion)
            .expect("Onion decryption failed");
        for b in &nack.onion[1..8] {
            if *b != 0 {
                debug!("{:?} Nack is broken", self);
                self.lost_cells.fetch_add(1, Ordering::Relaxed);
                return;
            }
        }
    }

    pub fn lost_cells(&self) -> u32 {
        self.lost_cells.load(Ordering::Relaxed)
    }

    pub fn clear_lost_cells(&self) {
        self.lost_cells.store(0, Ordering::Relaxed);
    }
}

fn create_plaintext_cell(circuit: &ClientCircuit, round_no: RoundNo) -> Cell {
    let mut cell = Cell::dummy(circuit.circuit_id(), round_no);
    let token = circuit
        .dummy_tokens()
        .iter()
        .choose(&mut thread_rng())
        .expect("Expected at least one dummy token");
    cell.set_token(*token);
    // we want the cell echoed back by the rendezvous service
    cell.set_command(CellCmd::Broadcast);
    cell
}
