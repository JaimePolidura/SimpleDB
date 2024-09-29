mod simpledb_server;
mod request;
mod response;

use std::process::exit;
use std::{env, io};

fn main() {
    let (address, password) = get_database_args();

    loop {
        let input = read_input_from_user();

        if input.to_lowercase() == "exit" {
            exit(1);
        }
    }
}

fn read_input_from_user() -> String {
    let mut line = String::from("");

    io::stdin()
        .read_line(&mut line)
        .expect("Failed to read line");

    line
}

//Address, Password
fn get_database_args() -> (String, String) {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        panic!("Invalid args. Expect <address> <password>")
    }

    (args[0].clone(), args[1].clone())
}