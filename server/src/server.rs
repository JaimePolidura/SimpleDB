use crate::request::Request;
use crate::response::{Response, RowsResponse, StatementResponse};
use crossbeam_skiplist::SkipMap;
use db::simple_db::StatementResult;
use db::{Context, SimpleDb, Statement};
use shared::connection::Connection;
use shared::logger::{logger, Logger, SimpleDbLayer};
use shared::SimpleDbError::InvalidPassword;
use shared::{SimpleDbError, SimpleDbOptions};
use std::net::TcpListener;
use std::sync::Arc;
use std::thread;

pub type ConnectionId = usize;

pub struct Server {
    simple_db: Arc<SimpleDb>,
    options: Arc<SimpleDbOptions>,

    context_by_connection_id: SkipMap<ConnectionId, Context>,
}

impl Server {
    pub fn create(
        options: Arc<SimpleDbOptions>
    ) -> Result<Server, SimpleDbError> {
        Logger::init(options.clone());

        logger().info(
            SimpleDbLayer::Server,
            &format!("Initializing server at address 127.0.0:{}", options.server_port)
        );

        let simple_db = db::simple_db::create(options.clone())?;
        Ok(Server {
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
            let server = self.clone();
            logger().debug(SimpleDbLayer::Server, &format!("Accepted new connection {}", socket.peer_addr().unwrap()));
            let connection = Connection::create(socket);

            thread::spawn(|| {
                Self::handle_connection(connection, server);
            });
        }
    }

    fn handle_connection(mut connection: Connection, server: Arc<Server>) {
        let connection_id = connection.connection_id();
        server.context_by_connection_id.insert(connection_id, Context::empty());

        loop {
            let response = Self::handle_request(&mut connection, server.clone())
                .unwrap_or_else(|error| Response::from_simpledb_error(error));

            match connection.write(response.serialize()) {
                //The connection was closed
                Ok(n) => if n == 0 {
                    server.context_by_connection_id.remove(&connection_id);
                    break;
                }
                Err(_) => {
                    break;
                }
            };
        }
    }

    fn handle_request(
        connection: &mut Connection,
        server: Arc<Server>,
    )  -> Result<Response, SimpleDbError> {
        let request = Request::deserialize_from_connection(connection)?;
        let connection_id = connection.connection_id();

        Self::authenticate(&server, &request)?;

        match request {
            Request::UseDatabase(_, database) => {
                Self::handle_use_database_connection_request(server, &database, connection_id)?;
                logger().debug(SimpleDbLayer::Server, &format!("Executed use database. Connection ID: {} Database: {}",
                    connection.connection_id(), database));
                Ok(Response::Ok)
            },
            Request::Statement(_, is_stand_alone, statement) => {
                let statement_result = Self::handle_statement_request(connection_id, server, is_stand_alone, statement)?;
                Ok(Response::Statement(statement_result))
            },
            Request::Close(_) => {
                Self::handle_close_request(server, connection_id);
                logger().debug(SimpleDbLayer::Server, &format!("Executed close request with connection ID: {}", connection_id));
                Ok(Response::Ok)
            }
        }
    }

    fn authenticate(
        server: &Arc<Server>,
        request: &Request
    ) -> Result<(), SimpleDbError> {
        let authentication = request.get_authentication();
        if authentication.password != server.options.server_password {
            Err(InvalidPassword)
        } else {
            Ok(())
        }
    }

    fn handle_statement_request(
        connection_id: ConnectionId,
        server: Arc<Server>,
        is_stand_alone: bool,
        statement_string: String
    ) -> Result<StatementResponse, SimpleDbError> {
        let mut context = match server.context_by_connection_id.get(&connection_id) {
            Some(context_entry) => context_entry.value().clone(),
            None => Context::empty()
        };

        let statement = server.simple_db.parse(&statement_string)?;
        let statement_desc = statement.get_descriptor();
        let is_explained = statement.is_explained();

        if statement_desc.requires_transaction() && !context.has_transaction() && is_stand_alone {
            let transaction = server.simple_db.execute(&context, Statement::StartTransaction)?
                .get_transaction();
            context.with_transaction(transaction);
        }

        match server.simple_db.execute(&context, statement) {
            Ok(statement_result) => {
                if statement_desc.requires_transaction() && is_stand_alone {
                    server.simple_db.execute(&context, Statement::Commit)?;
                }
                if statement_desc.creates_transaction() {
                    context.with_transaction(statement_result.get_transaction());
                    server.context_by_connection_id.insert(connection_id, context);
                } else if statement_desc.terminates_transaction() {
                    context.clear_transaction();
                    server.context_by_connection_id.insert(connection_id, context);
                }

                Self::create_response(statement_result, connection_id, statement_string, is_explained)
            }
            Err(error) => {
                if statement_desc.requires_transaction() && is_stand_alone {
                    server.simple_db.execute(&context, Statement::Rollback)?;
                }

                Err(error)
            }
        }
    }

    fn handle_use_database_connection_request(
        server: Arc<Server>,
        database_name: &String,
        connection_id: ConnectionId
    )  -> Result<(), SimpleDbError> {
        server.simple_db.get_databases().get_database_or_err(database_name)?;

        match server.context_by_connection_id.get(&connection_id) {
            Some(context) => {
                let context = context.value();
                //Rollback previous transaction
                let _ = server.simple_db.execute(context, Statement::Rollback);
                server.context_by_connection_id.insert(connection_id, Context::create_with_database(&database_name));
            }
            None => {
                server.context_by_connection_id.insert(connection_id, Context::create_with_database(&database_name));
            }
        };

        Ok(())
    }

    fn create_response(
        statement_result: StatementResult,
        connection_id: ConnectionId,
        statement: String,
        is_explained: bool,
    ) -> Result<StatementResponse, SimpleDbError> {
        match statement_result {
            StatementResult::Describe(describe) => {
                logger().debug(SimpleDbLayer::Server, &format!(
                    "Executed describe request Connection ID: {} Entries to return {}",
                    connection_id, describe.len())
                );
                Ok(StatementResponse::Describe(describe))
            },
            StatementResult::Databases(databases) => {
                logger().debug(SimpleDbLayer::Server, &format!(
                    "Executed show databases request Connection ID: {} Entries to return {}",
                    connection_id, databases.len())
                );
                Ok(StatementResponse::Databases(databases))
            },
            StatementResult::Indexes(indexes) => {
                logger().debug(SimpleDbLayer::Server, &format!(
                    "Executed show indexes request Connection ID: {} Entries to return {}",
                    connection_id, indexes.len())
                );
                Ok(StatementResponse::Indexes(indexes))
            }
            StatementResult::Tables(tables) => {
                logger().debug(SimpleDbLayer::Server, &format!(
                    "Executed show tables request Connection ID: {} Entries to return {}",
                    connection_id, tables.len())
                );
                Ok(StatementResponse::Tables(tables))
            },
            StatementResult::Ok(n) => {
                logger().debug(SimpleDbLayer::Server, &format!(
                    "Executed statement request Connection ID: {} Rows affected {}. Statement: {}",
                    connection_id, n, statement
                ));
                Ok(StatementResponse::Ok(n))
            },
            StatementResult::TransactionStarted(transaction) => {
                logger().debug(SimpleDbLayer::Server, &format!(
                    "Executed start transaction request Connection ID: {} Transaction ID: {}",
                    connection_id, transaction.id()
                ));
                Ok(StatementResponse::Ok(0))
            },
            StatementResult::Data(mut query_iterator) => {
                if !is_explained {
                    let rows = query_iterator.all()?;
                    logger().debug(SimpleDbLayer::Server, &format!(
                        "Executed query request request Connection ID: {} Rows returned: {} Statement: {}",
                        connection_id, rows.len(), statement
                    ));
                    // Ok(StatementResponse::Rows(RowsResponse::create(
                    //     query_iterator.schema().clone(),
                    //     rows
                    // )))
                    Ok(StatementResponse::Ok(1))
                } else {
                    let explanation = query_iterator.get_plan_desc();
                    logger().debug(SimpleDbLayer::Server, &format!(
                        "Executed query explain request request Connection ID: {} Statement: {}",
                        connection_id, statement
                    ));
                    // Ok(StatementResponse::Rows(QueryResponse::Explanation()))
                    Ok(StatementResponse::Ok(1))
                }
            }
        }
    }

    fn handle_close_request(server: Arc<Server>, connection_id: ConnectionId) {
        if let Some(context_entry) = server.context_by_connection_id.get(&connection_id) {
            let context = context_entry.value();
            if context.has_transaction() {
                server.simple_db.execute(context, Statement::Rollback).expect("Cannot close connection");
            }

            server.context_by_connection_id.remove(&connection_id);
        }
    }

    fn server_address_to_str(&self) -> String {
        let mut address = String::from("127.0.0.1:");
        address.push_str(self.options.server_port.to_string().as_str());
        address
    }
}