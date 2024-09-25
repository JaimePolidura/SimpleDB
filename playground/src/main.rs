use db::simple_db::Context;

fn main() {
    let db = db::simple_db::create(shared::start_simpledb_options_builder()
        .base_path("C:\\programacion\\SimpleDB\\playground\\resources")
        .compaction_strategy(shared::CompactionStrategy::SimpleLeveled)
        .durability_level(shared::DurabilityLevel::Strong)
        .memtable_max_size_bytes(8192)
        .compaction_task_frequency_ms(10)
        .sst_size_bytes(65536)
        .build())
        .unwrap();

    db.execute(&Context::empty(), "CREATE DATABASE prueba;").expect("");
    db.execute(&Context::with_database("prueba"), "CREATE TABLE personas (id I64 PRIMARY KEY, nombre VARCHAR, dinero F64);")
        .expect("");

    let transaction = db.execute_only_one(&Context::with_database("prueba"), "START_TRANSACTION;")
        .unwrap()
        .get_transaction();

    let context = Context::with("prueba", transaction);

    db.execute_only_one(&context, "INSERT INTO personas (id, nombre, dinero) VALUES (1, \"Jaime\", 10);");
    db.execute_only_one(&context, "INSERT INTO personas (id, nombre, dinero) VALUES (2, \"Molon\", 11);");
    db.execute_only_one(&context, "INSERT INTO personas (id, nombre, dinero) VALUES (3, \"Walo\", 12);");

    let mut data = db.execute_only_one(&context, "SELECT * FROM personas WHERE dinero > 10;")
        .expect("")
        .data();

    while let Some(row) = data.next().unwrap() {
        println!("{}", row);
    }
}