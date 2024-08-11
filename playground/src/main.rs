use std::time::Duration;
use rand::Rng;
use core::lsm;
use core::lsm_options;
use core::key;

fn main() {
    let mut lsm = lsm::new(lsm_options::builder()
        .base_path(String::from("C:\\programacion\\mini-lsm\\playground\\resources"))
        .memtable_max_size_bytes(8192)
        .sst_size_bytes(65536)
        .compaction_strategy(lsm_options::CompactionStrategy::SimpleLeveled)
        .compaction_task_frequency_ms(10)
        .build());

    loop {
        let value = next_value();
        let key = next_key();

        lsm.set(&key, &value)
            .expect("Failed to write key in LSM");

        std::thread::sleep(Duration::from_millis(1));
    }
}

fn next_key() -> key::Key {
    let mut rng = rand::thread_rng();
    let random_string: String = (0..3)
        .map(|_| rng.gen_range(65..90) as u8 as char) // Generate a random ASCII character
        .collect();
    key::new(random_string.as_str())
}

fn next_value() -> Vec<u8> {
    vec![1, 2, 3]
}