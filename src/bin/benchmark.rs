use clap::{clap_app, value_t};
use log::*;
use std::time::Duration;

use hydra::crypto::key::Key;
use hydra::crypto::threefish::Threefish2048;
use hydra::crypto::{x25519, x448};
use hydra::defs::ONION_LEN;
use hydra::epoch::current_time;
use hydra::tonic_mix::Cell;

pub fn main() {
    hydra::log_cfg::init(true);
    let args = clap_app!(benchmark =>
        (version: hydra::defs::hydra_version())
        (about: "Hydra benchmark tool")
        (@arg tf: --tf +takes_value default_value("0") "Number of repetitions for Threefish2048")
        (@arg x25519: --x25519 +takes_value default_value("0") "Number of repetitions for x25519")
        (@arg x448: --x448 +takes_value default_value("0") "Number of repetitions for x448")
        (@arg time: --time +takes_value default_value("0") "Number of repetitions for getting the current time")
        (@arg shuffle: --shuffle +takes_value default_value("0") "Number of elements to shuffle")
    )
    .get_matches();

    let tf_n = value_t!(args, "tf", u32).unwrap();
    let x25519_n = value_t!(args, "x25519", u32).unwrap();
    let x448_n = value_t!(args, "x448", u32).unwrap();
    let time_n = value_t!(args, "time", u32).unwrap();
    let shuffle_n = value_t!(args, "shuffle", u32).unwrap();

    if tf_n > 0 {
        threefish(tf_n);
    }
    if x25519_n > 0 {
        x25519(x25519_n);
    }
    if x448_n > 0 {
        x448(x448_n);
    }
    if time_n > 0 {
        time(time_n);
    }
    if shuffle_n > 0 {
        shuffle(shuffle_n);
    }
}

fn threefish(n: u32) {
    info!("Starting Threefish-2048 benchmark");
    let key = Key::new(128);
    let tf = Threefish2048::new(key).unwrap();
    let start = current_time();
    for i in 0..n {
        let mut cell = Cell {
            circuit_id: 337,
            round_no: 42,
            onion: vec![i as u8; ONION_LEN],
        };
        let tweak_src = 24 * i as u64;
        tf.encrypt(tweak_src, &mut cell.onion).unwrap();
    }
    info!("Threefish-2048 benchmark (enc): {:.2} pps", pps(n, &start));
}

fn x25519(n: u32) {
    info!("Preparing x25519 benchmark");
    let (_, sk) = x25519::generate_keypair();
    let mut pks = Vec::new();
    for _ in 0..n {
        pks.push(x25519::generate_keypair().0);
    }

    info!("Starting x25519 benchmark");
    let start = current_time();
    for pk in pks {
        x25519::generate_shared_secret(&pk, &sk).unwrap();
    }
    info!("x25519 benchmark: {:.2} pps", pps(n, &start));
}

fn x448(n: u32) {
    info!("Preparing x448 benchmark");
    let (_, sk) = x448::generate_keypair();
    let mut pks = Vec::new();
    for _ in 0..n {
        pks.push(x448::generate_keypair().0);
    }

    info!("Starting x448 benchmark");
    let start = current_time();
    for pk in pks {
        x448::generate_shared_secret(&pk, &sk).unwrap();
    }
    info!("x448 benchmark: {:.2} pps", pps(n, &start));
}

fn time(n: u32) {
    info!("Preparing time benchmark");
    let mut sum = Duration::from_secs(0);
    info!("Starting time benchmark");
    let start = current_time();
    for _ in 0..n {
        sum += current_time().checked_sub(start).unwrap();
    }
    info!("Anti-optimization output: {}", sum.as_millis());
    info!("Time benchmark: {:.2} cps", pps(n, &start));
}

fn shuffle(n: u32) {
    info!("Preparing shuffle benchmark");
    let mut cells = vec![];
    for i in 0..n {
        let c = Cell {
            circuit_id: 337,
            round_no: 42,
            onion: vec![i as u8; ONION_LEN],
        };
        cells.push(c);
    }
    info!("Starting shuffle benchmark");
    let start = current_time();
    let shuffle_it = hydra::mix::sender::ShuffleIterator::new(cells);
    info!("Shuffle only benchmark: {:.2} pps", pps(n, &start));
    let shuffled_cells: Vec<Cell> = shuffle_it.collect();
    info!("Shuffle and collect benchmark: {:.2} pps", pps(n, &start));
    info!("Anti-optimization output: {}", shuffled_cells[0].onion[0]);
}

fn pps(n: u32, start_time: &Duration) -> f64 {
    let duration = current_time().checked_sub(*start_time).unwrap();
    n as f64 / duration.as_secs_f64()
}
