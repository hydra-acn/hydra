use super::directory_client;
use crate::epoch::EpochNo;
use crate::tonic_directory::MixStatistics;
use log::*;
use std::convert::{TryFrom, TryInto};
use std::time::Duration;

#[derive(Clone, Default)]
pub struct Statistics {
    epoch_no: EpochNo,
    setup_time: Vec<u32>,
    total_processing_time: Vec<u64>,
    counter: Vec<u64>,
    no_circuits_per_layer: Vec<u32>,
}

impl Statistics {
    pub fn new(epoch_no: EpochNo, path_length: u32) -> Self {
        Statistics {
            epoch_no,
            setup_time: vec![0; path_length as usize],
            total_processing_time: vec![0; path_length as usize],
            counter: vec![0; path_length as usize],
            no_circuits_per_layer: vec![0; path_length as usize],
        }
    }

    pub fn record_setup_time(&mut self, start: Duration, end: Duration, layer: u32) {
        if start <= end {
            match u32::try_from((end - start).as_micros()) {
                Ok(val) => self.setup_time[layer as usize] = val,
                Err(e) => {
                    warn!(
                        "Not able to convert setup_time into u32: {}. Use maximum value instead.",
                        e
                    );
                    self.setup_time[layer as usize] = std::u32::MAX;
                }
            };
        } else {
            warn!("Negative timespan for setup time in layer {}", layer);
            self.setup_time[layer as usize] = 0;
        }
    }

    pub fn record_processing_time(&mut self, start: Duration, end: Duration, layer: u32) {
        if start <= end {
            match u32::try_from((end - start).as_micros()) {
                Ok(val) => self.total_processing_time[layer as usize] += val as u64,
                Err(e) => {
                    warn!("Not able to convert processing_time into u32: {}. Use maximum value instead.", e);
                    self.total_processing_time[layer as usize] += std::u32::MAX as u64;
                }
            };
        } else {
            warn!("Negative timespan for processing time in layer {}", layer);
            self.total_processing_time[layer as usize] += 0;
        }
        self.counter[layer as usize] += 1;
    }

    pub fn record_circuit_count(&mut self, count: usize, count_dummys: usize, layer: u32) {
        let no_circuits: u32 = (count + count_dummys)
            .try_into()
            .expect("Failed to get total number of circuits");
        self.no_circuits_per_layer[layer as usize] = no_circuits;
    }

    pub fn send_statistic_msg(&self, dir_client: &directory_client::Client) {
        let mut avg_processing_time_per_layer = vec![0; self.total_processing_time.len()];
        for i in 0..self.total_processing_time.len() {
            if self.counter[i] != 0 {
                avg_processing_time_per_layer[i] =
                    (self.total_processing_time[i] / self.counter[i]) as u32;
            } else {
                avg_processing_time_per_layer[i] = 0;
            }
        }

        let mut statistic_msg = MixStatistics {
            epoch_no: self.epoch_no,
            fingerprint: dir_client.fingerprint().to_string(),
            auth_tag: vec![],
            no_circuits_per_layer: self.no_circuits_per_layer.clone(),
            setup_time_per_layer: self.setup_time.clone(),
            avg_processing_time_per_layer,
        };
        statistic_msg.set_auth_tag(&dir_client);
        dir_client.queue_statistic_msg(statistic_msg);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hmac::Mac;
    #[test]
    fn test_statistic() {
        let epoch_no = 77;
        let mock_dir_client = directory_client::mocks::new(epoch_no);
        let mut test_stat = Statistics::new(epoch_no, 3);
        //insert per layer setup time
        test_stat.record_setup_time(Duration::from_micros(100), Duration::from_micros(120), 0);
        test_stat.record_setup_time(Duration::from_micros(200), Duration::from_micros(180), 1);
        test_stat.record_setup_time(Duration::from_micros(1), Duration::from_micros(std::u64::MAX), 2);
        //insert processing time for one layer
        test_stat.record_processing_time(
            Duration::from_micros(2000),
            Duration::from_micros(2500),
            1,
        );
        test_stat.record_processing_time(
            Duration::from_micros(2500),
            Duration::from_micros(2000),
            1,
        );
        test_stat.record_processing_time(
            Duration::from_micros(55),
            Duration::from_micros(std::u64::MAX),
            1,
        );
        //insert circuit count
        test_stat.record_circuit_count(2566, 920, 0);
        test_stat.record_circuit_count(29704, 83476, 1);
        test_stat.record_circuit_count(5, 7, 2);
        //expected vectors
        let no_circuits_per_layer: Vec<u32> = vec![3486, 113180, 12];
        let setup_time_per_layer: Vec<u32> = vec![20, 0, std::u32::MAX];
        let avg_processing_time_per_layer: Vec<u32> =
            vec![0, ((500 + 0 + std::u32::MAX as u64) / 3) as u32, 0];
        //generate mac and update it accordingly
        let mut mac = mock_dir_client.init_mac().unwrap();
        mac.update(&epoch_no.to_le_bytes());
        mac.update(&mock_dir_client.fingerprint().as_bytes());

        for val in no_circuits_per_layer.iter() {
            mac.update(&val.to_le_bytes());
        }
        for val in setup_time_per_layer.iter() {
            mac.update(&val.to_le_bytes());
        }
        for val in avg_processing_time_per_layer.iter() {
            mac.update(&val.to_le_bytes());
        }
        //generate statistic message, queue it and compare expected message with queued message
        test_stat.send_statistic_msg(&mock_dir_client);
        let mut msg_queue = directory_client::mocks::get_msg_queue(&mock_dir_client);
        assert_eq!(msg_queue.len(), 1);
        assert_eq!(
            msg_queue.pop(),
            Some(MixStatistics {
                epoch_no: 77,
                fingerprint: mock_dir_client.fingerprint().to_string(),
                auth_tag: mac.finalize().into_bytes().to_vec(),
                no_circuits_per_layer: no_circuits_per_layer.to_vec(),
                setup_time_per_layer: setup_time_per_layer.to_vec(),
                avg_processing_time_per_layer: avg_processing_time_per_layer.to_vec(),
            })
        )
    }
}
