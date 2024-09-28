use db::simple_db::Context;
use db::SimpleDb;

fn main() {
    std::fs::remove_dir_all("C:\\programacion\\SimpleDB\\playground\\resources\\prueba");

    let db = db::simple_db::create(shared::start_simpledb_options_builder()
        .base_path("C:\\programacion\\SimpleDB\\playground\\resources")
        .compaction_strategy(shared::CompactionStrategy::SimpleLeveled)
        .durability_level(shared::DurabilityLevel::Strong)
        .memtable_max_size_bytes(8192)
        .compaction_task_frequency_ms(10)
        .sst_size_bytes(65536)
        .build_arc())
        .unwrap();

    db.execute(&Context::empty(), "CREATE DATABASE prueba;").expect("");
    db.execute(&Context::create_with_database("prueba"), "CREATE TABLE personas (id I64 PRIMARY KEY, nombre VARCHAR, dinero F64);")
        .expect("");

    let transaction = db.execute_only_one(&Context::create_with_database("prueba"), "START_TRANSACTION;")
        .unwrap()
        .get_transaction();

    let context = Context::create("prueba", transaction);

    db.execute_only_one(&context, "INSERT INTO personas (id, nombre, dinero) VALUES (1, \"Jaime\", 10.0);").unwrap();
    db.execute_only_one(&context, "INSERT INTO personas (id, nombre, dinero) VALUES (2, \"Molon\", 11.0);").unwrap();
    db.execute_only_one(&context, "INSERT INTO personas (id, nombre, dinero) VALUES (3, \"Walo\", 12.0);").unwrap();

    delete(db, context);
}

fn delete(mut db: SimpleDb, context: Context) {
    // Delete (NOT FOUND)
    println!("DELETE FROM personas WHERE id = 100;");
    db.execute_only_one(&context, "DELETE FROM personas WHERE id == 100;").unwrap();
    let mut data = db.execute_only_one(&context, "SELECT * FROM personas;")
        .expect("")
        .data();
    while let Some(row) = data.next().unwrap() {
        println!("{}", row);
    }

    // Delete only id 3
    println!("DELETE FROM personas WHERE id = > 2;");
    db.execute_only_one(&context, "DELETE FROM personas WHERE id > 2;").unwrap();
    let mut data = db.execute_only_one(&context, "SELECT * FROM personas;")
        .expect("")
        .data();
    while let Some(row) = data.next().unwrap() {
        println!("{}", row);
    }

    //Delete all
    println!("DELETE FROM personas;");
    db.execute_only_one(&context, "DELETE FROM personas;").unwrap();
    let mut data = db.execute_only_one(&context, "SELECT * FROM personas;")
        .expect("")
        .data();
    while let Some(row) = data.next().unwrap() {
        println!("{}", row);
    }
}

fn update(mut db: SimpleDb, context: Context) {
    // Single upate
    println!("UPDATE personas SET nombre = \"JaimeTruman\", SET dinero = dinero + 100 WHERE id == 1;");
    db.execute_only_one(&context, "UPDATE personas SET nombre = \"JaimeTruman\", SET dinero = dinero + 100 WHERE id == 1;").unwrap();
    let mut data = db.execute_only_one(&context, "SELECT * FROM personas WHERE id == 1;")
        .expect("")
        .data();
    while let Some(row) = data.next().unwrap() {
        println!("{}", row);
    }

    //Multiple upate
    println!("UPDATE personas SET dinero = 0.0 WHERE id > 1;");
    db.execute_only_one(&context, "UPDATE personas SET dinero = 0.0 WHERE id > 1;").unwrap();
    let mut data = db.execute_only_one(&context, "SELECT * FROM personas;")
        .expect("")
        .data();
    while let Some(row) = data.next().unwrap() {
        println!("{}", row);
    }
}

fn selects(mut db: SimpleDb, context: Context) {
    //FullScan
    println!("SELECT * FROM personas WHERE dinero > 10.0;");
    let mut data = db.execute_only_one(&context, "SELECT * FROM personas WHERE dinero > 10.0;")
        .expect("")
        .data();
    while let Some(row) = data.next().unwrap() {
        println!("{}", row);
    }

    //ExactScan
    println!("SELECT * FROM personas WHERE id == 1;");
    let mut data = db.execute_only_one(&context, "SELECT * FROM personas WHERE id == 1;")
        .expect("")
        .data();
    while let Some(row) = data.next().unwrap() {
        println!("{}", row);
    }

    //Range Scan
    println!("SELECT * FROM personas WHERE id > 1 AND dinero == 12.0;");
    let mut data = db.execute_only_one(&context, "SELECT * FROM personas WHERE id > 1 AND dinero == 12.0;")
        .expect("")
        .data();
    while let Some(row) = data.next().unwrap() {
        println!("{}", row);
    }

    //Full scan, only two columns
    println!("SELECT nombre FROM personas;");
    let mut data = db.execute_only_one(&context, "SELECT nombre FROM personas;")
        .expect("")
        .data();
    while let Some(row) = data.next().unwrap() {
        println!("{}", row);
    }
}