use crate::connection::Connection;
use crate::request::Request;
use crate::response::{QueryDataResponse, Response, StatementResponse};
use crossbeam_skiplist::SkipMap;
use db::simple_db::StatementResult;
use db::{Context, SimpleDb};
use shared::{SimpleDbError, SimpleDbOptions};
use std::io::Write;
use std::net::TcpListener;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use threadpool::ThreadPool;

pub type ConnectionId = usize;

pub struct Server {
    simple_db: Arc<SimpleDb>,
    options: Arc<SimpleDbOptions>,
    pool: ThreadPool,

    next_connection_id: AtomicUsize,
    context_by_connection_id: SkipMap<ConnectionId, Context>,
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

    pub fn start(self: Arc<Self>) -> ! {
        let listener = TcpListener::bind(self.server_address_to_str())
            .unwrap();

        loop {
            let (socket, _) = listener.accept().unwrap();
            let self_cloned = self.clone();

            self.pool.execute(move || {
                let mut connection = Connection::create(socket);

                match Self::handle_connection(&mut connection, self_cloned) {
                    Ok(result) => {
                        let serialized = result.serialize();
                        connection.write(serialized).unwrap();
                    }
                    Err(error) => {
                        let response = Response::from_simpledb_error(error);
                        let serialized = response.serialize();
                        connection.write(serialized).unwrap();
                    }
                }
            });
        }
    }

    fn handle_connection(
        connection: &mut Connection,
        server: Arc<Server>,
    )  -> Result<Response, SimpleDbError> {
        let request = Request::deserialize_from_connection(connection)?;

        match request {
            Request::Init(database) => {
                let connection_id = Self::handle_init_connection_request(server, database)?;
                Ok(Response::Init(connection_id))
            },
            Request::Statement(connection_id, statement) => {
                let statement_result = Self::handle_statement_request(connection_id, server, statement)?;
                Ok(Response::Statement(statement_result))
            },
            Request::Close(connection_id) => {
                Self::handle_close_request(server, connection_id);
                Ok(Response::Ok)
            }
        }
    }

    fn handle_statement_request(
        connection_id: ConnectionId,
        server: Arc<Server>,
        statement: String
    ) -> Result<StatementResponse, SimpleDbError> {
        let context = server.context_by_connection_id.get(&connection_id);
        if context.is_none() {
            return Err(SimpleDbError::IllegalMessageProtocolState);
        }

        let context = context.as_ref().unwrap().value();
        let statement_result = server.simple_db.execute_only_one(context, &statement)?;

        match statement_result {
            StatementResult::Describe(describe) => Ok(StatementResponse::Describe(describe)),
            StatementResult::Databases(databases) => Ok(StatementResponse::Databases(databases)),
            StatementResult::Tables(tables) => Ok(StatementResponse::Tables(tables)),
            StatementResult::Ok(n) => Ok(StatementResponse::Ok(n)),
            StatementResult::TransactionStarted(transaction) => {
                let mut context = context.clone();
                context.with_transaction(transaction);
                server.context_by_connection_id.insert(connection_id, context);
                Ok(StatementResponse::Ok(0))
            },
            StatementResult::Data(mut query_iterator) => {
                let rows = query_iterator.all()?;
                Ok(StatementResponse::Data(QueryDataResponse::create(
                    query_iterator.columns_descriptor_selection().clone(), rows
                )))
            }
        }
    }

    fn handle_init_connection_request(server: Arc<Server>, database_name: String) -> Result<ConnectionId, SimpleDbError> {
        let connection_id = server.next_connection_id.fetch_add(1, Ordering::Relaxed) as ConnectionId;
        server.context_by_connection_id.insert(connection_id, Context::create_with_database(database_name.as_str()));
        Ok(connection_id)
    }

    fn handle_close_request(server: Arc<Server>, connection_id: ConnectionId) {
        if let Some(context_entry) = server.context_by_connection_id.get(&connection_id) {
            let context = context_entry.value();
            if context.has_transaction() {
                server.simple_db.execute(context, "ROLLBACK;");
            }
        }
    }

    fn server_address_to_str(&self) -> String {
        let mut address = String::from("127.0.0.1:");
        address.push_str(self.options.server_port.to_string().as_str());
        address
    }
}