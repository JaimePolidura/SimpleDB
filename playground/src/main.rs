use rand::Rng;
use core::lsm;
use core::lsm_options;
use core::key;

fn main() {
    let mut lsm = lsm::new(lsm_options::builder()
        .base_path(String::from("C:\\programacion\\mini-lsm\\playground\\resources"))
        .compaction_strategy(lsm_options::CompactionStrategy::SimpleLeveled)
        .durability_level(lsm_options::DurabilityLevel::Strong)
        .memtable_max_size_bytes(8192)
        .compaction_task_frequency_ms(10)
        .sst_size_bytes(65536)
        .build());

    write(&mut lsm);
    // read(&mut lsm);
}

fn read(lsm: &mut lsm::Lsm) {
    let value = lsm.get("AAB");
    if value.is_some() {
    }
}

fn write(lsm: &mut lsm::Lsm)  {
    loop {
        let value = next_value();
        let key = next_key();

        lsm.set(&key, &value)
            .expect("Failed to write key in LSM");

        // std::thread::sleep(Duration::from_millis(1));
    }
}

fn next_key() -> String {
    let mut rng = rand::thread_rng();
    (0..3)
        .map(|_| rng.gen_range(65..90) as u8 as char) // Generate a random ASCII character
        .collect()
}

fn next_value() -> Vec<u8> {
    vec![1, 2, 3]
}