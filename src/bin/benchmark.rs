use clap::{clap_app, value_t};
use log::*;

use hydra::crypto::key::Key;
use hydra::crypto::threefish::Threefish2048;
use hydra::defs::ONION_LEN;
use hydra::epoch::current_time;
use hydra::tonic_mix::Cell;

pub fn main() {
    hydra::log_cfg::init(true);
    let args = clap_app!(benchmark =>
        (version: hydra::defs::hydra_version())
        (about: "Hydra benchmark tool")
        (@arg n: +required "Number of repetitions")
    )
    .get_matches();

    let n = value_t!(args, "n", u32).unwrap();
    threefish(n);
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
