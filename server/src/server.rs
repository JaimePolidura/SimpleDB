use crate::connection::Connection;
use crate::request::Request;
use crate::response::Response;
use crossbeam_skiplist::SkipMap;
use db::{Context, SimpleDb};
use shared::{SimpleDbError, SimpleDbOptions};
use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use threadpool::ThreadPool;

pub struct Server {
    simple_db: Arc<SimpleDb>,
    options: Arc<SimpleDbOptions>,
    pool: ThreadPool,

    next_connection_id: AtomicUsize,
    context_by_connection_id: SkipMap<usize, Context>,
}

impl Server {
    pub fn create(
        options: Arc<SimpleDbOptions>
    ) -> Result<Server, SimpleDbError> {
        let simple_db = db::simple_db::create(options.clone())?;
        Ok(Server {
            pool: ThreadPool::new(options.server_n_worker_threads as usize),
            next_connection_id: AtomicUsize::new(0),
            context_by_connection_id: SkipMap::new(),
            simple_db: Arc::new(simple_db),
            options
        })
    }

    pub fn start(self) -> ! {
        let listener = TcpListener::bind(self.server_address_to_str())
            .unwrap();

        loop {
            let (mut socket, _) = listener.accept().unwrap();
            self.pool.execute(move || {
                match self.handle_tcp_stream(socket) {
                    Ok(result) => {
                        let serialized = result.serialize();
                        socket.write(serialized.as_slice()).unwrap();
                    }
                    Err(error) => {
                        let response = Response::from_simpledb_error(error);
                        let serialized = response.serialize();
                        socket.write(serialized.as_slice()).unwrap();
                    }
                }
            });
        }
    }

    fn handle_tcp_stream(&self, tcp_stream: TcpStream)  -> Result<Response, SimpleDbError> {
        let mut connection = Connection::create(tcp_stream);
        let request = Request::deserialize_from_connection(&mut connection)?;

        match request {
            Request::Init => {
                let connection_id = self.next_connection_id.fetch_add(1, Ordering::Relaxed);
                self.context_by_connection_id.insert(connection_id, Context::empty());
                Ok(Response::Init(connection_id))
            }
            Request::Statement => {
                Ok(Response::Init(1))
            }
            Request::Close => {
                Ok(Response::Init(1))
            }
        }
    }

    fn server_address_to_str(&self) -> String {
        let mut address = String::from("127.0.0.1:");
        address.push_str(self.options.server_port.to_string().as_str());
        address
    }
}