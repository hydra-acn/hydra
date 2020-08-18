use std::net::SocketAddr;
use std::sync::Arc;

use hydra::client::circuit::derive_keys;
use hydra::crypto::key::Key;
use hydra::crypto::x448;
use hydra::defs::SETUP_TOKENS;
use hydra::mix::circuit::NextSetupStep;
use hydra::mix::grpc::SetupPacketWithPrev;
use hydra::mix::rendezvous_map::RendezvousMap;
use hydra::net::ip_addr_from_slice;
use hydra::tonic_directory::{EpochInfo, MixInfo};

type ClientCircuit = hydra::client::circuit::Circuit;
type MixCircuit = hydra::mix::circuit::Circuit;

#[test]
fn key_derivation() {
    let master_key =
        Key::from_hex_str("775f84edb8bf10bb747765f2582c87f4a1e4463f275f38ce447a5885").unwrap();
    let nonce = hex::decode("903a73a912df").unwrap();
    let (aes_key, onion_key) = derive_keys(&master_key, &nonce).unwrap();
    let aes_expected =
        Key::from_hex_str("ac5e5ae356e4f943574ee7cefadb091b17eec79d642fcafcd8679f8c110cc51f")
            .unwrap();
    let onion_expected = Key::from_hex_str("30755ecd02757afb390d28ae2eb5bc4e6015e95c835a998cc74551e8a8a183fac722852edf51c1a82b0b9f068c085ad6c17233ef20730e710e862232cb8675696c140a5ee306a816df06f99b1cf639baa93a7d15fbe0be7e4c10afaeea26d77f6b656808d756df0c0f978610faa8c35597e49e04f4f1b85225bff654b69ee06e").unwrap();
    assert_eq!(aes_key, aes_expected);
    assert_eq!(onion_key, onion_expected);
}

#[test]
fn setup_onion() {
    // deterministic test (only when not run in parallel with other tests)
    hydra::crypto::activate_fake_rand(1337);

    let mixes: Vec<(MixInfo, Key)> = [1, 2, 3].iter().map(|i| create_mix_info(*i)).collect();
    let path: Vec<MixInfo> = mixes.iter().map(|(info, _)| info.clone()).collect();
    let endpoints: Vec<SocketAddr> = path
        .iter()
        .map(|info| {
            SocketAddr::new(
                ip_addr_from_slice(&info.address).unwrap(),
                info.relay_port as u16,
            )
        })
        .collect();
    let mut epoch_info = EpochInfo::default();
    epoch_info.mixes = path.clone();
    let subscribe_to = vec![13, 37];
    let rendezvous_map = Arc::new(RendezvousMap::new(&epoch_info));
    let (client_circuit, extend) =
        ClientCircuit::new(42, path.clone(), subscribe_to.clone()).unwrap();
    assert_eq!(*extend.next_hop(), endpoints[0]);
    let setup_pkt = extend.into_inner();

    // first mix
    assert_eq!(setup_pkt.ttl().unwrap(), 2);
    let previous_hop = Some("8.8.8.8:42".parse().unwrap()); // previous hop is client
    let pkt_with_prev = SetupPacketWithPrev::new(setup_pkt, previous_hop);
    let (_mix_circuit, next_step) =
        MixCircuit::new(pkt_with_prev, &mixes[0].1, rendezvous_map.clone(), 0, 41).unwrap();
    let extend = match next_step {
        NextSetupStep::Extend(e) => e,
        _ => unreachable!(),
    };
    assert_eq!(*extend.next_hop(), endpoints[1]);
    let setup_pkt = extend.into_inner();

    // second mix
    assert_eq!(setup_pkt.ttl().unwrap(), 1);
    let pkt_with_prev = SetupPacketWithPrev::new(setup_pkt, Some(endpoints[0].clone()));
    let (_mix_circuit, next_step) =
        MixCircuit::new(pkt_with_prev, &mixes[1].1, rendezvous_map.clone(), 1, 41).unwrap();
    let extend = match next_step {
        NextSetupStep::Extend(e) => e,
        _ => unreachable!(),
    };
    assert_eq!(*extend.next_hop(), endpoints[2]);
    let setup_pkt = extend.into_inner();

    // third mix (last one)
    assert_eq!(setup_pkt.ttl().unwrap(), 0);
    let pkt_with_prev = SetupPacketWithPrev::new(setup_pkt, Some(endpoints[1].clone()));
    let (_mix_circuit, next_step) =
        MixCircuit::new(pkt_with_prev, &mixes[2].1, rendezvous_map.clone(), 2, 41).unwrap();

    let rendezvous_tokens = match next_step {
        NextSetupStep::Rendezvous(ts) => ts,
        _ => unreachable!(),
    };
    let expected_tokens_it = subscribe_to
        .iter()
        .chain(client_circuit.dummy_tokens().iter());
    assert_eq!(
        subscribe_to.len() + client_circuit.dummy_tokens().len(),
        SETUP_TOKENS
    );
    assert_eq!(rendezvous_tokens.len(), SETUP_TOKENS);
    for t in expected_tokens_it {
        assert!(rendezvous_tokens.contains(t));
    }
}

fn create_mix_info(index: u8) -> (MixInfo, Key) {
    let (pk, sk) = x448::generate_keypair().unwrap();
    let info = MixInfo {
        address: vec![127, 0, 0, index],
        entry_port: 9001,
        relay_port: 9002,
        rendezvous_port: 9003,
        fingerprint: format!("mix-{}", index),
        public_dh: pk.clone_to_vec(),
    };
    (info, sk)
}
