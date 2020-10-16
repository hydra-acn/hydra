use clap::{clap_app, value_t};
use log::*;

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
    )
    .get_matches();

    let tf_n = value_t!(args, "tf", u32).unwrap();
    let x25519_n = value_t!(args, "x25519", u32).unwrap();
    let x448_n = value_t!(args, "x448", u32).unwrap();

    if tf_n > 0 {
        threefish(tf_n);
    }
    if x25519_n > 0 {
        x25519(x25519_n);
    }
    if x448_n > 0 {
        x448(x448_n);
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
    let duration = current_time().checked_sub(start).unwrap();
    let pps = n as f64 / duration.as_secs_f64();
    info!("Threefish-2048 benchmark (enc): {:.2} pps", pps);
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
    let duration = current_time().checked_sub(start).unwrap();
    let pps = n as f64 / duration.as_secs_f64();
    info!("x25519 benchmark: {:.2} pps", pps);
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
    let duration = current_time().checked_sub(start).unwrap();
    let pps = n as f64 / duration.as_secs_f64();
    info!("x448 benchmark: {:.2} pps", pps);
}
