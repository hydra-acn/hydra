use hmac::{Hmac, Mac, NewMac};
use hydra::crypto::key::{hkdf_sha256, Key};
use hydra::crypto::x448;
use hydra::defs::{DIR_AUTH_KEY_INFO, DIR_AUTH_KEY_SIZE, DIR_AUTH_UNREGISTER};
use hydra::directory::grpc;
use hydra::directory::state::State;
use hydra::epoch::MAX_EPOCH_NO;
use hydra::tonic_directory::directory_client::DirectoryClient;
use hydra::tonic_directory::{
    DhMessage, DirectoryRequest, RegisterRequest, UnregisterAck, UnregisterRequest,
};
use sha2::Sha256;

use hydra::crypto::tls::ServerCredentials;
use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::runtime::Builder;
use tokio::time::{self, Duration};
use tonic::Request;

#[test]
fn integration() {
    let mut rt = Builder::new()
        .threaded_scheduler()
        .enable_all()
        .build()
        .expect("Failed to init tokio runtime");

    rt.block_on(async {
        let state = Arc::new(State::default());

        let local_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let timeout = time::delay_for(Duration::from_secs(2));
        let key =
            Key::read_from_file("tests/data/tls-test.key").expect("Failed to read key from file");
        let cert = std::fs::read_to_string("tests/data/tls-test.pem").unwrap();
        let tls_cred = ServerCredentials::new(key, &cert);
        let (grpc_handle, local_addr) = grpc::spawn_service_with_shutdown(
            state.clone(),
            local_addr,
            Some(timeout),
            Some(tls_cred),
        )
        .await
        .expect("Spawning failed");

        let client_handle = tokio::spawn(client_task(state.clone(), local_addr.port()));

        let _ = tokio::try_join!(grpc_handle, client_handle).expect("Something failed");
    })
}

async fn client_task(state: Arc<State>, port: u16) {
    let config = state.config();
    let cert = std::fs::read_to_string("tests/data/tls-test-ca.pem").unwrap();
    let endpoint = format!("https://localhost:{}", port);

    // wait to avoid race condition client/server
    time::delay_for(Duration::from_millis(100)).await;
    let channel = tonic::transport::Channel::from_shared(endpoint)
        .unwrap()
        .tls_config(
            tonic::transport::ClientTlsConfig::new()
                .ca_certificate(tonic::transport::Certificate::from_pem(&cert))
                .domain_name("localhost".to_string()),
        )
        .unwrap()
        .connect()
        .await
        .expect("Connecting failed");

    let mut client = DirectoryClient::new(channel);

    let dummy_key = Key::new(x448::POINT_SIZE);

    // test some successful registers
    let m = 4;
    let mut mix_key_vec = vec![];
    for i in 1..=m {
        mix_key_vec.push(register_mix(&mut client, i).await);
    }

    // mapping (mix index, counter) to their sent public keys
    let mut pk_map: BTreeMap<(u8, u32), Key> = BTreeMap::new();

    // test send of public DH key with wrong auth_tag
    let counter: u32 = 0;
    let mut bad_mac = Hmac::<Sha256>::new_varkey(mix_key_vec[(1) as usize].borrow_raw())
        .expect("Initialising bad_mac failed");
    bad_mac.update(&counter.to_le_bytes());
    bad_mac.update(dummy_key.borrow_raw());
    let bad_msg = create_dh_msg(1, counter, &dummy_key, &bad_mac.finalize().into_bytes());
    expect_fail(&client.add_static_dh(Request::new(bad_msg)).await);

    // test some successful sends of public DH keys
    for i in 1..=m {
        for ctr in 0..config.epochs_in_advance() - 1 {
            send_pk(
                &mut client,
                i,
                ctr.into(),
                &mut pk_map,
                mix_key_vec[(i - 1) as usize].clone(),
            )
            .await;
        }
    }

    // test double register
    let bad_info = create_register_request(1, &dummy_key);
    expect_fail(&client.register(Request::new(bad_info)).await);

    // test bad address
    let mut bad_info = create_register_request(m + 1, &dummy_key);
    bad_info.address = vec![127, 0, 0, 1];
    expect_fail(&client.register(Request::new(bad_info)).await);

    // test bad port
    let mut bad_info = create_register_request(m + 1, &dummy_key);
    bad_info.entry_port = (std::u16::MAX as u32) + 1;
    expect_fail(&client.register(Request::new(bad_info)).await);

    // test another bad port
    let mut bad_info = create_register_request(m + 1, &dummy_key);
    bad_info.relay_port = (std::u16::MAX as u32) + 1;
    expect_fail(&client.register(Request::new(bad_info)).await);

    // test bad key len during registration
    let bad_key = Key::new(x448::POINT_SIZE - 1);
    let bad_info = create_register_request(m + 1, &bad_key);
    expect_fail(&client.register(Request::new(bad_info)).await);

    // test bad fingerprint for adding public DH key
    let bad_msg = create_dh_msg(m + 1, 0, &dummy_key, &vec![]);
    expect_fail(&client.add_static_dh(Request::new(bad_msg)).await);

    // test bad key len for adding public DH key
    let bad_msg = create_dh_msg(1, 0, &bad_key, &vec![]);
    expect_fail(&client.add_static_dh(Request::new(bad_msg)).await);

    // test send of public DH key with old counter
    let counter: u32 = 0;
    let mut bad_mac = Hmac::<Sha256>::new_varkey(mix_key_vec[(0) as usize].borrow_raw())
        .expect("Initialising bad_mac failed");
    bad_mac.update(&counter.to_le_bytes());
    bad_mac.update(dummy_key.borrow_raw());
    let bad_msg = create_dh_msg(1, counter, &dummy_key, &bad_mac.finalize().into_bytes());
    expect_fail(&client.add_static_dh(Request::new(bad_msg)).await);

    // explicitly run epoch update
    state.update();

    // test expected empty response by explicitly querying >= MAX_EPOCH_NO
    let req = Request::new(DirectoryRequest {
        min_epoch_no: MAX_EPOCH_NO,
    });
    let epochs = client
        .query_directory(req)
        .await
        .expect("Query failed")
        .into_inner()
        .epochs;
    assert_eq!(epochs.len(), 0, "Did not expect epochs >= MAX_EPOCH_NO");

    // test normal query
    let req = Request::new(DirectoryRequest { min_epoch_no: 0 });
    let epochs = client
        .query_directory(req)
        .await
        .expect("Query failed")
        .into_inner()
        .epochs;

    assert_eq!(
        epochs.len(),
        config.epochs_in_advance() as usize,
        "Number of epochs unexpected"
    );
    let first_epoch = epochs.first().unwrap();
    let mut last_epoch_no = first_epoch.epoch_no;
    let mut last_setup_start = first_epoch.setup_start_time;
    let mut last_comm_start = first_epoch.communication_start_time;
    for (i, epoch) in epochs.iter().enumerate() {
        if i < config.epochs_in_advance() as usize - 1 {
            assert_eq!(epoch.mixes.len(), m as usize, "Mismatch in number of mixes");
        } else {
            assert_eq!(
                epoch.mixes.len(),
                0,
                "Did not expect any mixes in last epoch"
            );
        }

        assert_eq!(epoch.round_duration, config.round_duration() as u32);
        assert_eq!(epoch.round_waiting, config.round_waiting() as u32);
        assert_eq!(
            epoch.number_of_rounds,
            config.phase_duration() as u32 / (epoch.round_duration + epoch.round_waiting)
        );

        for mix in epoch.mixes.iter() {
            assert_eq!(mix.entry_port, 4242);
            assert_eq!(mix.relay_port, 1337);
            assert_eq!(mix.address.len(), 4);
            assert_eq!(mix.address[..3], [10, 0, 0]);
            assert_eq!(
                mix.public_dh,
                pk_map
                    .get(&(mix.address[3], i as u32))
                    .expect("pk map broken?")
                    .borrow_raw(),
                "Wrong pk"
            );
        }
        if i > 0 {
            assert_eq!(
                epoch.epoch_no,
                last_epoch_no + 1,
                "Epoch numbers not ascending"
            );
            assert_eq!(
                epoch.setup_start_time + config.phase_duration(),
                epoch.communication_start_time,
                "Duration mismatch"
            );
            assert_eq!(
                epoch.setup_start_time, last_comm_start,
                "Setup and communication should be in sync"
            );
            assert_eq!(
                epoch.setup_start_time,
                last_setup_start + config.phase_duration(),
                "Duration mismatch"
            );
            assert_eq!(
                epoch.communication_start_time,
                last_comm_start + config.phase_duration(),
                "Duration mismatch"
            );
        }
        last_epoch_no = epoch.epoch_no;
        last_setup_start = epoch.setup_start_time;
        last_comm_start = epoch.communication_start_time;
    }

    // test unregister with bad mac
    let mut bad_mac = Hmac::<Sha256>::new_varkey(mix_key_vec[(1) as usize].borrow_raw())
        .expect("Initialising bad_mac failed");
    bad_mac.update(DIR_AUTH_UNREGISTER);
    let bad_info = create_unregister_request(1, &bad_mac.finalize().into_bytes().to_vec());
    expect_fail(&client.unregister(Request::new(bad_info)).await);

    // test successful unregister
    let mut mac = Hmac::<Sha256>::new_varkey(&mix_key_vec[(0) as usize].borrow_raw())
        .expect("Initialising mac failed");
    mac.update(DIR_AUTH_UNREGISTER);
    let info = create_unregister_request(1, &mac.finalize().into_bytes().to_vec());
    let resp = client.unregister(Request::new(info)).await;
    assert_eq!(resp.unwrap().into_inner(), UnregisterAck {});
}

async fn register_mix(client: &mut DirectoryClient<tonic::transport::Channel>, index: u8) -> Key {
    let (pk, sk) = x448::generate_keypair();
    let req = Request::new(create_register_request(index, &pk));
    let reply = client
        .register(req)
        .await
        .expect("Register failed")
        .into_inner();

    let pk_reply = Key::move_from_vec(reply.public_dh);
    let shared_secret =
        x448::generate_shared_secret(&pk_reply, &sk).expect("Key exchange with directory failed");
    let auth_key = hkdf_sha256(
        &shared_secret,
        None,
        Some(DIR_AUTH_KEY_INFO),
        DIR_AUTH_KEY_SIZE,
    )
    .expect("Key exchange with directory failed");
    assert_eq!(pk_reply.len(), pk.len());
    return auth_key;
}

async fn send_pk(
    client: &mut DirectoryClient<tonic::transport::Channel>,
    index: u8,
    counter: u32,
    map: &mut BTreeMap<(u8, u32), Key>,
    auth_key: Key,
) {
    let pk = Key::new(x448::POINT_SIZE);
    //generate mac
    let mut mac =
        Hmac::<Sha256>::new_varkey(auth_key.borrow_raw()).expect("Initialising mac failed");
    mac.update(&counter.to_le_bytes());
    mac.update(pk.borrow_raw());
    let reply = client
        .add_static_dh(Request::new(create_dh_msg(
            index,
            counter,
            &pk,
            &mac.finalize().into_bytes().to_vec(),
        )))
        .await
        .expect("Adding DH key failed")
        .into_inner();
    assert_eq!(reply.counter, counter);
    map.insert((index, counter), pk);
}

fn create_register_request(index: u8, pk: &Key) -> RegisterRequest {
    RegisterRequest {
        fingerprint: format!("mix-{}", index),
        address: vec![10, 0, 0, index],
        entry_port: 4242,
        relay_port: 1337,
        rendezvous_port: 1337,
        public_dh: pk.clone_to_vec(),
    }
}

fn create_dh_msg(index: u8, counter: u32, pk: &Key, auth_tag: &[u8]) -> DhMessage {
    DhMessage {
        fingerprint: format!("mix-{}", index),
        counter,
        public_dh: pk.clone_to_vec(),
        auth_tag: auth_tag.to_vec(),
    }
}

fn create_unregister_request(index: u8, auth_tag: &[u8]) -> UnregisterRequest {
    UnregisterRequest {
        fingerprint: format!("mix-{}", index),
        auth_tag: auth_tag.to_vec(),
    }
}

fn expect_fail<T>(reply: &Result<T, tonic::Status>) {
    match *reply {
        Ok(_) => panic!("Expected fail did not occure"),
        Err(_) => (),
    }
}
