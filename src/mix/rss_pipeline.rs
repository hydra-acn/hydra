//! Pipeline for parallel processing/transformation of requests. The requests have to implement the
//! `Scalable` trait in order to determine the assignment to a thread.  Generic types are `I` for
//! the type of the incomming requests, `O` for the regular output and `A` for an alternative
//! output of each request throughout the module.
use log::*;
use rand::{thread_rng, Rng};
use rayon::prelude::*;
use std::collections::VecDeque;
use std::time::Duration;

use crate::epoch::current_time;

pub type Pipeline<I, O, A> = (RxQueue<I>, Processor<I, O, A>, TxQueue<O>, TxQueue<A>);

/// Create a new pipeline with `size` threads. Panics if `size == 0`.
pub fn new_pipeline<I: std::fmt::Debug + Scalable + Send, O: Send, A: Send>(
    size: usize,
) -> Pipeline<I, O, A> {
    assert!(size > 0);
    let mut senders = Vec::new();
    let mut threads = Vec::new();
    for _ in 0..size {
        let (rx_sender, rx_receiver) = crossbeam_channel::unbounded();
        senders.push(rx_sender);
        let t = ThreadState {
            in_queue: rx_receiver,
            out_queue: Vec::new(),
            alt_out_queue: Vec::new(),
            pending_queue: VecDeque::new(),
        };
        threads.push(t);
    }
    let rx = RxQueue { queues: senders };
    let (tx_sender, tx_receiver) = crossbeam_channel::unbounded();
    let (alt_tx_sender, alt_tx_receiver) = crossbeam_channel::unbounded();
    let processor = Processor {
        threads,
        tx_queue: tx_sender,
        alt_tx_queue: alt_tx_sender,
    };
    (rx, processor, tx_receiver, alt_tx_receiver)
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
#[derive(Clone)]
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
pub type TxQueue<T> = crossbeam_channel::Receiver<(Vec<Vec<T>>, Option<Duration>)>;

/// Enum to decide what should happen after processing a request.
pub enum ProcessResult<I, O, A> {
    /// Regular output.
    Out(O),
    /// Alternative output.
    Alt(A),
    /// Generate multiple outputs from one request.
    Multiple(Vec<O>),
    /// Generate multiple alternative outputs from one request.
    MultipleAlt(Vec<A>),
    /// Requeue the request for the next call to `process`.
    Requeue(I),
    /// Drop the request without producing an output.
    Drop,
}

struct ThreadState<I, O, A> {
    in_queue: crossbeam_channel::Receiver<I>,
    pending_queue: VecDeque<I>,
    out_queue: Vec<O>,
    alt_out_queue: Vec<A>,
}

/// Struct for doing the work on requests of type `I`, transforming each to output of type `O` or
/// type `A`.
pub struct Processor<I: Send, O: Send, A: Send> {
    threads: Vec<ThreadState<I, O, A>>,
    tx_queue: crossbeam_channel::Sender<(Vec<Vec<O>>, Option<Duration>)>,
    alt_tx_queue: crossbeam_channel::Sender<(Vec<Vec<A>>, Option<Duration>)>,
}

impl<I: Send, O: Send, A: Send> Processor<I, O, A> {
    /// Return the number of threads.
    pub fn size(&self) -> usize {
        self.threads.len()
    }

    /// Receives and processes new request by applying the function `f` to each until the given
    /// `time` is reached. Also blocks till `time` is reached if no new requests arrive.
    /// Panics if the matching rx queue is gone.
    pub fn process_till<F: FnMut(I) -> ProcessResult<I, O, A> + Clone + Sync>(
        &mut self,
        f: F,
        deadline: Duration,
    ) {
        // pending requests
        self.threads
            .par_iter_mut()
            .for_each(|t| process_pending(t, f.clone(), deadline));

        // new requests
        self.threads
            .par_iter_mut()
            .for_each(|t| process_new(t, f.clone(), deadline));
    }

    /// Add additional output that does not belong to any input.
    pub fn pad(&mut self, out: Vec<O>) {
        self.threads[0].out_queue.extend(out.into_iter());
    }

    /// Add additional alternative output that does not belong to any input.
    pub fn alt_pad(&mut self, out: Vec<A>) {
        self.threads[0].alt_out_queue.extend(out.into_iter());
    }

    /// Send the output to the tx queue by moving.
    /// Panics if the matching tx queue is gone.
    pub fn send(&mut self, deadline: Option<Duration>) {
        let out = self.output();
        self.tx_queue
            .send((out, deadline))
            .expect("Pipeline consumer gone?");
    }

    /// Send the alternative output to the tx queue by moving.
    /// Panics if the matching tx queue is gone.
    pub fn alt_send(&mut self, deadline: Option<Duration>) {
        let out = self.alt_output();
        self.alt_tx_queue
            .send((out, deadline))
            .expect("Pipeline consumer gone?");
    }

    /// Get the output by moving.
    pub fn output(&mut self) -> Vec<Vec<O>> {
        let mut out = Vec::new();
        for t in self.threads.iter_mut() {
            out.push(std::mem::replace(&mut t.out_queue, Vec::new()));
        }
        out
    }

    /// Get the alternative output by moving.
    pub fn alt_output(&mut self) -> Vec<Vec<A>> {
        let mut out = Vec::new();
        for t in self.threads.iter_mut() {
            out.push(std::mem::replace(&mut t.alt_out_queue, Vec::new()));
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

fn process_pending<I, O, A, F: FnMut(I) -> ProcessResult<I, O, A>>(
    thread: &mut ThreadState<I, O, A>,
    mut f: F,
    time: Duration,
) {
    let mut new_pending = VecDeque::new();
    while let Some(req) = thread.pending_queue.pop_front() {
        if let None = time.checked_sub(current_time()) {
            // time limit reached
            return;
        }
        match f(req) {
            ProcessResult::Out(out) => thread.out_queue.push(out),
            ProcessResult::Alt(out) => thread.alt_out_queue.push(out),
            ProcessResult::Multiple(out_vec) => thread.out_queue.extend(out_vec),
            ProcessResult::MultipleAlt(out_vec) => thread.alt_out_queue.extend(out_vec),
            ProcessResult::Requeue(req) => new_pending.push_back(req),
            ProcessResult::Drop => (),
        }
    }
    thread.pending_queue = new_pending;
}

fn process_new<I, O, A, F: FnMut(I) -> ProcessResult<I, O, A>>(
    thread: &mut ThreadState<I, O, A>,
    mut f: F,
    deadline: Duration,
) {
    let poll_interval = Duration::from_millis(1);
    loop {
        if let None = deadline.checked_sub(current_time()) {
            // time limit reached
            return;
        }
        match thread.in_queue.try_recv().map(|req| match f(req) {
            ProcessResult::Out(out) => thread.out_queue.push(out),
            ProcessResult::Alt(out) => thread.alt_out_queue.push(out),
            ProcessResult::Multiple(out_vec) => thread.out_queue.extend(out_vec),
            ProcessResult::MultipleAlt(out_vec) => thread.alt_out_queue.extend(out_vec),
            ProcessResult::Requeue(req) => thread.pending_queue.push_back(req),
            ProcessResult::Drop => (),
        }) {
            Ok(()) => (),
            Err(e) => match e {
                crossbeam_channel::TryRecvError::Empty => {
                    // don't poll too hard
                    std::thread::sleep(poll_interval);
                }
                crossbeam_channel::TryRecvError::Disconnected => {
                    unreachable!("Pipeline producer is gone")
                }
            },
        }
    }
}

#[macro_export]
macro_rules! define_pipeline_types {
    ($modname:ident, $in_t:ty, $out_t:ty, $alt_t:ty) => {
        pub mod $modname {
            use super::*;

            pub type In = $in_t;
            pub type Out = $out_t;
            pub type Alt = $alt_t;
            pub type Result = crate::mix::rss_pipeline::ProcessResult<In, Out, Alt>;
            pub type RxQueue = crate::mix::rss_pipeline::RxQueue<In>;
            pub type Processor = crate::mix::rss_pipeline::Processor<In, Out, Alt>;
            pub type TxQueue = crate::mix::rss_pipeline::TxQueue<Out>;
            pub type AltTxQueue = crate::mix::rss_pipeline::TxQueue<Alt>;
            pub type Pipeline = crate::mix::rss_pipeline::Pipeline<In, Out, Alt>;
        }
    };
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
        let (rx, mut proc, tx, alt_tx) = new_pipeline::<u32, u32, bool>(3);
        rx.enqueue(0);
        rx.enqueue(42);
        rx.enqueue(2);
        rx.enqueue(1337);
        rx.enqueue(9);
        rx.enqueue(9001);
        rx.enqueue(10001);

        let sum = Arc::new(Mutex::new(0u32));
        let inc = 1u32;
        let f = |x: u32| match x {
            0 => ProcessResult::Multiple(vec![0, 0]),
            42 => ProcessResult::Requeue(x),
            1337 => ProcessResult::Drop,
            10001 => ProcessResult::MultipleAlt(vec![true, true]),
            i if i > 9000 => ProcessResult::Alt(true),
            _ => {
                let y = x + inc;
                *sum.lock().unwrap() += y;
                ProcessResult::Out(y)
            }
        };

        let till = current_time() + Duration::from_millis(1000);
        proc.process_till(f, till);
        proc.pad(vec![1, 3, 3, 7]);
        proc.alt_pad(vec![false, false]);
        proc.send(None);
        proc.alt_send(None);

        let out = tx.try_recv().unwrap().0;
        let expected_out: Vec<Vec<u32>> = vec![vec![0, 0, 10, 1, 3, 3, 7], vec![], vec![3]];
        let requeued = proc.requeued();
        let mut pending = VecDeque::new();
        pending.push_back(42);
        let requeued_expected = vec![pending, VecDeque::new(), VecDeque::new()];

        assert_eq!(out, expected_out);
        assert_eq!(requeued, requeued_expected);
        assert_eq!(*sum.lock().unwrap(), 13);

        let alt_out = alt_tx.try_recv().unwrap().0;
        let expected_alt_out = vec![vec![false, false], vec![true], vec![true, true]];
        assert_eq!(alt_out, expected_alt_out);
    }
}
