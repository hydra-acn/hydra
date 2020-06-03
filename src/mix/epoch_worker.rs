//! processing of one epoch
use log::*;
use std::sync::Arc;
use tokio::time::{delay_for, Duration};

use super::directory_client;
use crate::epoch::current_time;
use crate::epoch::EpochInfo;

pub struct Worker {
    dir_client: Arc<directory_client::Client>,
}

impl Worker {
    pub fn new(dir_client: Arc<directory_client::Client>) -> Self {
        Worker { dir_client }
    }

    /// endless loop for processing the epochs, starting with the upcomming epoch (setup)
    pub async fn run(&self) {
        let first_epoch;
        // poll directory client till we get an answer
        loop {
            match self.dir_client.next_epoch_info() {
                Some(epoch) => {
                    first_epoch = epoch;
                    break;
                }
                None => (),
            }
            // don't poll too hard
            delay_for(Duration::from_secs(1)).await;
        }

        info!(
            "Mix is getting busy, starting with epoch {}",
            first_epoch.epoch_no
        );
        let mut is_first = true;
        let mut setup_epoch = first_epoch;

        // hope that timing of communication rounds did not change compared to the previous epoch
        // and take it as first reference when to process setup packets
        let mut communication_epoch = setup_epoch.clone();

        // "endless" processing of epochs; life as a mix never gets boring!
        loop {
            // note: process_epoch waits till the start of the epoch(s) automatically
            self.process_epoch(&setup_epoch, &communication_epoch, is_first)
                .await;

            // one more epoch done, some more to come!
            is_first = false;
            communication_epoch = setup_epoch;
            // TODO robustness: we should try to recover from not knowing the next epoch
            setup_epoch = self
                .dir_client
                .get_epoch_info(communication_epoch.epoch_no + 1)
                .expect("Don't know the next epoch");
        }
    }

    async fn process_epoch(
        &self,
        setup_epoch: &EpochInfo,
        communication_epoch: &EpochInfo,
        is_first: bool,
    ) {
        let round_duration = Duration::from_secs(communication_epoch.round_duration as u64);
        let round_waiting = Duration::from_secs(communication_epoch.round_waiting as u64);
        let interval = round_duration + round_waiting;
        let mut next_round_start = match is_first {
            false => Duration::from_secs(communication_epoch.communication_start_time),
            true => Duration::from_secs(setup_epoch.setup_start_time),
        };
        let mut next_round_end = next_round_start + round_duration;

        info!(
            "Next up: setup for epoch {}, and communication for epoch {}",
            setup_epoch.epoch_no, communication_epoch.epoch_no
        );
        assert!(
            communication_epoch.number_of_rounds >= setup_epoch.path_length,
            "Not enough rounds to setup next epoch"
        );

        for round_no in 0..communication_epoch.number_of_rounds {
            // TODO move waiting inside the process functions?
            // wait till start of next round
            let wait_time = next_round_start
                .checked_sub(current_time())
                .expect("Did not finish last setup in time?");
            delay_for(wait_time).await;
            next_round_start += interval;

            if is_first == false {
                self.process_communication_round(&communication_epoch, round_no)
                    .await;
            } else {
                info!("If this wasn't the first epoch, we would process round {} now", round_no)
            }

            // wait till round end
            let wait_time = next_round_end
                .checked_sub(current_time())
                .expect("Did not finish round in time?");
            delay_for(wait_time).await;
            next_round_end += interval;

            let setup_layer = round_no;
            if setup_layer < setup_epoch.path_length {
                self.process_setup_layer(&setup_epoch, setup_layer).await;
            }
        }
    }

    async fn process_communication_round(&self, epoch: &EpochInfo, round_no: u32) {
        info!("Processing round {} of epoch {}", round_no, epoch.epoch_no);
    }

    async fn process_setup_layer(&self, epoch: &EpochInfo, layer: u32) {
        info!(
            "Processing setup layer {} of epoch {}",
            layer, epoch.epoch_no
        );
    }
}

pub async fn run(worker: Arc<Worker>) {
    worker.run().await;
}
