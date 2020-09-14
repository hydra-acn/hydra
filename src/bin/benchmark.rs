use log::*;

use hydra::crypto::key::Key;
use hydra::crypto::threefish::Threefish2048;
use hydra::epoch::current_time;
use hydra::tonic_mix::Cell;

pub fn main() {
    hydra::log_cfg::init(true);
    threefish();
}

fn threefish() {
    info!("Starting Threefish-2048 benchmark");
    let key = Key::new(128);
    let tf = Threefish2048::new(key).unwrap();
    let mut cell = Cell::dummy(42, 0);
    let start = current_time();
    let n = 100_000;
    for i in 0..n {
        let tweak_src = 24 * i as u64;
        tf.encrypt(tweak_src, &mut cell.onion).unwrap();
    }
    let duration = current_time().checked_sub(start).unwrap();
    let pps = n as f64 / duration.as_secs_f64();
    info!("Threefish-2048 benchmark (enc): {:.2} pps", pps);
}
