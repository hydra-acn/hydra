use log::*;
use std::convert::TryInto;
use std::io::Read;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;

use crate::defs::CELL_LEN;
use crate::net::cell::Cell;

use super::cell_processor::cell_rss_t;

pub struct State {
    cell_rx_queue: cell_rss_t::RxQueue,
}

impl State {
    pub fn new(cell_rx_queue: cell_rss_t::RxQueue) -> Self {
        State { cell_rx_queue }
    }

    fn handle_cell(&self, c: Cell) {
        self.cell_rx_queue.enqueue(c);
    }
}

fn handle_stream(state: Arc<State>, mut stream: TcpStream) {
    debug!(
        "Accepted new TCP stream from {}",
        stream.peer_addr().expect("Could not get peer addr")
    );
    stream
        .set_read_timeout(None)
        .expect("Setting read timeout failed");
    // TODO performance: buffer size
    let mut buf = [0u8; CELL_LEN];
    loop {
        match stream.read_exact(&mut buf) {
            Ok(()) => (),
            Err(e) => {
                warn!("Reading from TCP stream failed, giving up ({})", e);
                break;
            }
        }
        let cell = buf.to_vec().try_into().expect("Read exact broken?");
        state.handle_cell(cell);
    }
}

pub fn accept(state: Arc<State>, local_addr: SocketAddr) {
    let listener = TcpListener::bind(local_addr).expect("Cannot bind TCP");
    for s in listener.incoming() {
        match s {
            Ok(stream) => {
                let cloned_state = state.clone();
                std::thread::spawn(move || handle_stream(cloned_state, stream));
            }
            Err(e) => {
                warn!("Accepting TCP connection failed: {}", e);
                continue;
            }
        }
    }
}
