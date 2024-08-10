use rand::Rng;
use core::lsm;
use core::lsm_options;
use core::key;

fn main() {
    let mut lsm = lsm::new(lsm_options::builder()
        .base_path(String::from("C:\\programacion\\mini-lsm\\playground\\resources"))
        .memtable_max_size_bytes(8192)
        .compaction_strategy(lsm_options::CompactionStrategy::SimpleLeveled)
        .build());

    loop {
        let value = next_value();
        let key = next_key();

        lsm.set(&key, &value)
            .expect("Failed to write key in LSM");

        println!("SET({} = {:?})", key, value);
    }
}

fn next_key() -> key::Key {
    let mut rng = rand::thread_rng();
    let random_string: String = (0..3)
        .map(|_| rng.gen_range(33..127) as u8 as char) // Generate a random ASCII character
        .collect();
    key::new(random_string.as_str())
}

fn next_value() -> Vec<u8> {
    vec![1, 2, 3]
}