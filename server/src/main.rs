use crate::options_file::load_options;
use crate::server::Server;
use std::env;
use std::sync::Arc;

mod server;
mod options_file;
mod request;
mod response;

fn main() {
    let options_path = get_simpledb_options_path()
        .expect("Provide the simple db program path");
    let options = load_options(options_path)
        .expect("Error while loading options. Please make sure that the simpledb path is correct");
    let server = Arc::new(Server::create(options).unwrap());

    server.start()
}

fn get_simpledb_options_path() -> Result<String, ()> {
    let args: Vec<String> = env::args().collect();
    if args.len() > 0 {
        Ok(args[0].clone())
    } else {
        match env::var("SIMPLEDB_PATH") {
            Ok(path) => Ok(path),
            Err(_) => Err(())
        }
    }
}