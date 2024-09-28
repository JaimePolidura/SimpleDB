use std::sync::Arc;
use crate::options_file::OptionsFile;
use crate::server::Server;

mod server;
mod options_file;
mod request;
mod response;
mod connection;

fn main() {
    let options_file = OptionsFile::create();
    let options = options_file.load_options().unwrap();
    let server = Arc::new(Server::create(options).unwrap());
    server.start()
}
