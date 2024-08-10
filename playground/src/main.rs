use std::collections::btree_map::Keys;
use rand::Rng;
use core::lsm;
use core::lsm_options;
use core::key;

fn main() {
    let lsm = lsm::new(lsm_options::builder()
        .base_path(String::from("C:\\programacion\\mini-lsm\\playground\\resources"))
        .memtable_max_size_bytes(8192)
        .compaction_strategy(lsm_options::CompactionStrategy::SimpleLeveled)
        .build());

    loop {
        let key = next_key();
    }

    println!("Hello, world!");
}

fn next_key() -> key::Key {
    let mut rng = rand::thread_rng();
    let random_string: String = (0..3)
        .map(|_| rng.gen_range(33..127) as u8 as char) // Generate a random ASCII character
        .collect();
    key::new(random_string.as_str())
}