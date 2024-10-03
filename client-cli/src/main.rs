mod simpledb_server;
mod request;
mod response;
mod simple_db_cli;
mod table_print;

use crate::simple_db_cli::SimpleDbCli;
use std::env;

fn main() {
    let (address, password) = get_database_args();
    let mut app = SimpleDbCli::create(address, password);
    app.start()
}

//Address, Password
fn get_database_args() -> (String, String) {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        panic!("Invalid args. Expect <address> <password>")
    }

    (args[1].clone(), args[2].clone())
}
