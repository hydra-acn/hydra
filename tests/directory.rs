use hydra::directory::grpc;
use hydra::directory::state::State;
use hydra::epoch::MAX_EPOCH_NO;
use hydra::tonic_directory::directory_client::DirectoryClient;
use hydra::tonic_directory::DirectoryRequest;

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

        // TODO use ephemeral port
        let port = 4242u16;
        let local_addr = format!("127.0.0.1:{}", port)
            .parse()
            .expect("This address should be valid");
        let timeout = time::delay_for(Duration::from_secs(2));
        let grpc_handle =
            grpc::spawn_service_with_shutdown(state.clone(), local_addr, Some(timeout));

        let client_handle = tokio::spawn(client_task(port));

        let _ = tokio::try_join!(grpc_handle, client_handle).expect("Something failed");
    })
}

async fn client_task(port: u16) {
    // wait to avoid race condition client/server
    time::delay_for(Duration::from_millis(100)).await;
    let mut client = DirectoryClient::connect(format!("http://127.0.0.1:{}", port))
        .await
        .expect("Connecting failed");

    // test expected empty response by explicitly querying >= MAX_EPOCH_NO
    let req = Request::new(DirectoryRequest {
        min_epoch_no: MAX_EPOCH_NO,
    });
    let reply = client.query_directory(req).await.expect("Query failed").into_inner();
    assert_eq!(reply.epochs.len(), 0, "Did not expect epochs >= MAX_EPOCH_NO"); 
}
