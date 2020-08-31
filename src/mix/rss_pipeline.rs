//! Pipeline for parallel processing/transformation of requests. The requests have to implement the
//! `Scalable` trait in order to determine the assignment to a thread.
//! Generic types are `I` for the type of the incomming requests and `O` for the result of each
//! request throughout the module.
use log::*;
use rand::{thread_rng, Rng};
use rayon::prelude::*;
use std::collections::VecDeque;
use std::time::Duration;

use crate::epoch::current_time;

pub type Pipeline<I, O> = (RxQueue<I>, Processor<I, O>, TxQueue<O>);

/// Create a new pipeline with `size` threads.
pub fn new_pipeline<I: std::fmt::Debug + Scalable + Send, O: Send>(size: usize) -> Pipeline<I, O> {
    let mut senders = Vec::new();
    let mut threads = Vec::new();
    for _ in 0..size {
        let (rx_sender, rx_receiver) = crossbeam_channel::unbounded();
        senders.push(rx_sender);
        let t = ThreadState {
            in_queue: rx_receiver,
            out_queue: Vec::new(),
            pending_queue: VecDeque::new(),
        };
        threads.push(t);
    }
    let rx = RxQueue { queues: senders };
    let (tx_sender, tx_receiver) = crossbeam_channel::unbounded();
    let processor = Processor {
        threads,
        tx_queue: tx_sender,
    };
    (rx, processor, tx_receiver)
}

/// Trait for valid requests to an RSS pipeline.
pub trait Scalable {
    /// Given the `size` of the pipeline (number of threads), return the id of the thread this
    /// request should run on. Return value has to be in the range `0..size`, otherwise the request
    /// is dropped.
    /// Default implementation: random
    fn thread_id(&self, size: usize) -> usize {
        thread_rng().gen_range(0, size)
    }
}

/// Sender end of the rx channel.
pub struct RxQueue<I: Scalable + std::fmt::Debug> {
    queues: Vec<crossbeam_channel::Sender<I>>,
}

impl<I: Scalable + std::fmt::Debug> RxQueue<I> {
    /// Add a new request to the pipeline. Panics if the matching processor is gone.
    pub fn enqueue(&self, req: I) {
        let size = self.queues.len();
        let tid = req.thread_id(size);
        if tid < size {
            self.queues[tid].send(req).expect("Processor is gone");
        } else {
            warn!("Dropping request to RSS pipeline because thread id is too big");
        }
    }
}

/// Receiver end of the tx channel.
pub type TxQueue<O> = crossbeam_channel::Receiver<Vec<Vec<O>>>;

/// Enum to decide what should happen after processing a request.
pub enum ProcessResult<I, O> {
    /// Regular output.
    Ok(O),
    /// Generate multiple outputs from one request.
    Multiple(Vec<O>),
    /// Requeue the request for the next call to `process`.
    Requeue(I),
    /// Drop the request without producing an output.
    Drop,
}

struct ThreadState<I, O> {
    in_queue: crossbeam_channel::Receiver<I>,
    pending_queue: VecDeque<I>,
    out_queue: Vec<O>,
}

/// Struct for doing the work on requests of type `I`, transforming each to output type `O`.
pub struct Processor<I: Send, O: Send> {
    threads: Vec<ThreadState<I, O>>,
    tx_queue: crossbeam_channel::Sender<Vec<Vec<O>>>,
}

impl<I: Send, O: Send> Processor<I, O> {
    /// Return the number of threads.
    pub fn size(&self) -> usize {
        self.threads.len()
    }

    /// Receives and processes new request by applying the function `f` to each until the given
    /// `time` is reached. Also blocks till `time` is reached if no new requests arrive.
    /// Returns `true` iff all requests have been processed in time.
    /// Panics if the matching rx queue is gone.
    pub fn process_till<F: FnMut(I) -> ProcessResult<I, O> + Clone + Sync>(
        &mut self,
        f: F,
        time: Duration,
    ) -> bool {
        // pending requests
        self.threads
            .par_iter_mut()
            .for_each(|t| process_pending(t, f.clone(), time));

        // new requests in a loop
        loop {
            if let None = time.checked_sub(current_time()) {
                // time limit reached
                break;
            }
            self.threads
                .par_iter_mut()
                .for_each(|t| process_new(t, f.clone(), time));
            // don't poll too hard
            std::thread::sleep(Duration::from_millis(1));
        }

        // check if there are still incomming requests
        for t in self.threads.iter() {
            if t.in_queue.len() > 0 {
                return false;
            }
        }
        true
    }

    /// Send the output to the tx queue by moving.
    /// Panics if the matching tx queue is gone.
    pub fn send(&mut self) {
        let out = self.output();
        self.tx_queue.send(out).expect("Pipeline consumer gone?");
    }

    /// Get the output by moving.
    pub fn output(&mut self) -> Vec<Vec<O>> {
        let mut out = Vec::new();
        for t in self.threads.iter_mut() {
            out.push(std::mem::replace(&mut t.out_queue, Vec::new()));
        }
        out
    }

    /// Get requeued requests by moving.
    pub fn requeued(&mut self) -> Vec<VecDeque<I>> {
        let mut out = Vec::new();
        for t in self.threads.iter_mut() {
            out.push(std::mem::replace(&mut t.pending_queue, VecDeque::new()));
        }
        out
    }

    /// Drop all new requests.
    pub fn drop(&mut self) {
        // TODO security: this still might result in an endless loop when DoSed -> rate limit on
        // gRPC side?
        self.threads.par_iter_mut().for_each(|t| {
            while let Ok(_) = t.in_queue.try_recv() {
                // drop
            }
        });
    }

    /// Drop all new requests and requests that were requeued.
    pub fn drop_all(&mut self) {
        self.drop();
        for t in self.threads.iter_mut() {
            t.pending_queue.clear();
        }
    }
}

fn process_pending<I, O, F: FnMut(I) -> ProcessResult<I, O>>(
    thread: &mut ThreadState<I, O>,
    mut f: F,
    time: Duration,
) {
    let mut new_pending = VecDeque::new();
    while let Some(req) = thread.pending_queue.pop_front() {
        // TODO performance: don't call current_time so often
        if let None = time.checked_sub(current_time()) {
            // time limit reached
            return;
        }
        match f(req) {
            ProcessResult::Ok(out) => thread.out_queue.push(out),
            ProcessResult::Multiple(out_vec) => thread.out_queue.extend(out_vec),
            ProcessResult::Requeue(req) => new_pending.push_back(req),
            ProcessResult::Drop => (),
        }
    }
    thread.pending_queue = new_pending;
}

fn process_new<I, O, F: FnMut(I) -> ProcessResult<I, O>>(
    thread: &mut ThreadState<I, O>,
    mut f: F,
    time: Duration,
) {
    // TODO performance: don't call current_time so often
    loop {
        if let None = time.checked_sub(current_time()) {
            // time limit reached
            return;
        }
        match thread.in_queue.try_recv().map(|req| match f(req) {
            ProcessResult::Ok(out) => thread.out_queue.push(out),
            ProcessResult::Multiple(out_vec) => thread.out_queue.extend(out_vec),
            ProcessResult::Requeue(req) => thread.pending_queue.push_back(req),
            ProcessResult::Drop => (),
        }) {
            Ok(()) => (),
            Err(e) => match e {
                crossbeam_channel::TryRecvError::Empty => break,
                crossbeam_channel::TryRecvError::Disconnected => {
                    unreachable!("Pipeline producer is gone")
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    impl Scalable for u32 {
        fn thread_id(&self, size: usize) -> usize {
            *self as usize % size
        }
    }

    #[test]
    pub fn test_simple_type() {
        let (rx, mut proc, tx) = new_pipeline::<u32, u32>(3);
        rx.enqueue(0);
        rx.enqueue(42);
        rx.enqueue(2);
        rx.enqueue(1337);
        rx.enqueue(9);

        let sum = Arc::new(Mutex::new(0u32));
        let inc = 1u32;
        let f = |x: u32| match x {
            0 => ProcessResult::Multiple(vec![0, 0]),
            42 => ProcessResult::Requeue(x),
            1337 => ProcessResult::Drop,
            _ => {
                let y = x + inc;
                *sum.lock().unwrap() += y;
                ProcessResult::Ok(y)
            }
        };

        let till = current_time() + Duration::from_millis(500);
        proc.process_till(f, till);
        proc.send();

        let out = tx.try_recv().expect("Expected to receive output");
        let expected_out: Vec<Vec<u32>> = vec![vec![0, 0, 10], vec![], vec![3]];
        let requeued = proc.requeued();
        let mut pending = VecDeque::new();
        pending.push_back(42);
        let requeued_expected = vec![pending, VecDeque::new(), VecDeque::new()];

        assert_eq!(out, expected_out);
        assert_eq!(requeued, requeued_expected);
        assert_eq!(*sum.lock().unwrap(), 13);
    }
}
