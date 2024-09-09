use rand::Rng;

fn main() {
    let mut lsm = storage::create(shared::start_simpledb_options_builder()
        .base_path("C:\\programacion\\mini-lsm\\playground\\resources")
        .compaction_strategy(shared::CompactionStrategy::SimpleLeveled)
        .durability_level(shared::DurabilityLevel::Strong)
        .memtable_max_size_bytes(8192)
        .compaction_task_frequency_ms(10)
        .sst_size_bytes(65536)
        .build())
        .unwrap();

    // let k = lsm.create_keyspace().unwrap();

    // transactions(&mut lsm);
    write(&mut lsm, 1);
    //read(&mut lsm);
}

//"Resources" folder should be cleared before running this function
fn transactions(lsm: &mut storage::Storage) {
    let keyspace = lsm.create_keyspace().unwrap();

    let transaction1 = lsm.start_transaction();
    let transaction2 = lsm.start_transaction();

    lsm.set_with_transaction(keyspace, &transaction1, "aaa", &vec![1]);

    let value1 = lsm.get_with_transaction(keyspace, &transaction1, "aaa")
        .unwrap();
    assert!(value1.is_some());
    assert_eq!(value1.unwrap(), vec![1]);

    let value2 = lsm.get_with_transaction(keyspace, &transaction2, "aaa")
        .unwrap();
    assert!(value2.is_none());

    lsm.commit_transaction(transaction1);

    let value2 = lsm.get_with_transaction(keyspace, &transaction2, "aaa")
        .unwrap();
    assert!(value2.is_none());

    lsm.delete_with_transaction(keyspace, &transaction2, "aaa");

    lsm.rollback_transaction(transaction2);

    let value1 = lsm.get(keyspace, "aaa")
        .unwrap();
    assert!(value1.is_some());
    assert_eq!(value1.unwrap(), vec![1]);
}

fn read(lsm: &mut storage::Storage, keyspace_id: shared::KeyspaceId) {
    let value = lsm.get(keyspace_id, "AAB").unwrap();
    if value.is_some() {
    }
}

fn write(lsm: &mut storage::Storage, keyspace_id: shared::KeyspaceId)  {
    loop {
        let value = next_value();
        let key = next_key();

        lsm.set(keyspace_id, &key, &value)
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