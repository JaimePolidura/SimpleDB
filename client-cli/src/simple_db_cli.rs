use crate::request::Request;
use crate::response::{ColumnDescriptor, QueryDataResponse, Response, StatementResponse};
use crate::simpledb_server::SimpleDbServer;
use crate::table_print::TablePrint;
use std::cmp::Ordering;
use std::io;
use std::io::{stdout, Write};
use std::process::exit;

pub enum SimpleDbClientState {
    ConnectedToDatabase(usize),
    NotConnectedToDatabase,
}

pub struct SimpleDbCli {
    state: SimpleDbClientState,
    server: SimpleDbServer,
    password: String,
}

impl SimpleDbCli {
    pub fn create(
        address: String,
        password: String,
    ) -> SimpleDbCli {
        SimpleDbCli {
            server: SimpleDbServer::create(address),
            state: SimpleDbClientState::NotConnectedToDatabase,
            password
        }
    }

    pub fn start(&mut self) -> ! {
        loop {
            print!("simpledb> ");
            let _ = stdout().flush();
            let input = self.read_input_from_user().to_lowercase();
            let input = input.trim_end();

            if input.eq("show databases;") {
                self.show_databases_command();
            } else if input.eq("use")  {
                self.use_command(input);
            } else if input.eq("exit") {
                self.exit_command();
            } else {
                self.statement_command(input);
            }
        }
    }

    fn show_databases_command(&mut self) {
        let response = self.server.send_request(Request::Statement(
            self.password.clone(), 0, String::from("SHOW DATABASES;")
        ));
        self.print_response(response);
    }

    fn statement_command(&mut self, statement: &str) {
        match self.state {
            SimpleDbClientState::ConnectedToDatabase(connection_id) => {
                let response = self.server.send_request(Request::Statement(
                    self.password.clone(), connection_id, statement.to_string()
                ));
                self.print_response(response);
            }
            SimpleDbClientState::NotConnectedToDatabase => println!("No database selected!"),
        }
    }

    fn print_response(&mut self, response: Response) {
        match response {
            Response::Statement(statement_result) => {
                match statement_result {
                    StatementResponse::Ok(n_rows_affected) => println!("{} rows affected!", n_rows_affected),
                    StatementResponse::Data(data) => self.print_query_data(data),
                    StatementResponse::Databases(databases) => self.print_vec_string_as_tabble("Databases", databases),
                    StatementResponse::Tables(tables) => self.print_vec_string_as_tabble("Tables", tables),
                    StatementResponse::Describe(desc) => self.print_table_describe(&desc)
                };
            }
            Response::Init(_) => {
                println!("Connected to database!");
            }
            Response::Error(usize) => {
                println!("Received error: {}", usize);
            }
            Response::Ok => {
                println!("Ok");
            }
        }
    }

    fn print_query_data(&self, query_data: QueryDataResponse) {
        let mut columns_desc = query_data.columns_desc;
        columns_desc.sort_by(|a, b| {
            if a.is_primary {
                return Ordering::Greater
            } else if b.is_primary {
                return Ordering::Less
            } else {
                return Ordering::Equal
            }
        });
        let mut query_data_table = TablePrint::create(columns_desc.len());

        for current_column_desc in &columns_desc {
            query_data_table.add_header(current_column_desc.column_name.as_str());
        }

        for row in &query_data.rows {
            for current_column_desc in columns_desc.iter() {
                if let Some(column_value) = row.columns.get(&current_column_desc.column_id) {
                    query_data_table.add_column_value(current_column_desc.column_type.bytes_to_string(column_value));
                } else {
                    query_data_table.add_column_value("N/A".to_string());
                }
            }
        }

        query_data_table.print();
    }

    fn print_table_describe(&self, columns_desc: &Vec<ColumnDescriptor>) {
        let mut table = TablePrint::create(3);
        table.add_header("Field");
        table.add_header("Type");
        table.add_header("Primary");

        for column_desc in columns_desc {
            table.add_column_value(column_desc.column_name.clone());
            table.add_column_value(column_desc.column_type.to_string().to_string());

            if column_desc.is_primary {
                table.add_column_value("True".to_string());
            } else {
                table.add_column_value("False".to_string());
            }
        }

        table.print();
    }

    fn print_vec_string_as_tabble(&self, table_header_name: &str, vec: Vec<String>) {
        let mut table = TablePrint::create(1);
        table.add_header(table_header_name);
        for item in vec {
            table.add_column_value(item);
        }
        table.print();
    }

    fn exit_command(&mut self) {
        if matches!(self.state, SimpleDbClientState::ConnectedToDatabase(_)) {
            self.disconnect_from_current_database();
        }
        
        println!("Bye");
        exit(0)
    }

    fn use_command(&mut self, input: &str) {
        if let Some(database_name) = input.split_whitespace().next() {
            self.connect_to_database(database_name.to_string());
            println!("Changed current database!");
        } else {
            println!("Invalid syntax. Usage: USE <Database name>");
        }
    }

    fn connect_to_database(&mut self, database_name: String) {
        match self.state {
            SimpleDbClientState::ConnectedToDatabase(_) => self.disconnect_from_current_database(),
            _ => {}
        };

        let response = self.server.send_request(Request::InitConnection(
            self.password.clone(), database_name.clone()
        ));
        match response {
            Response::Init(connection_id) => {
                self.state = SimpleDbClientState::ConnectedToDatabase(connection_id);
            },
            _ => {
                println!("Cannot connect to database")
            }
        }
    }

    fn disconnect_from_current_database(&mut self) {
        self.server.send_request(Request::Close(
            self.password.clone(), self.state.get_connection_id()
        ));
        println!("mysql> Exited ")
    }

    fn read_input_from_user(&self) -> String {
        let mut line = String::from("");

        io::stdin()
            .read_line(&mut line)
            .expect("Failed to read line");

        line
    }
}

impl SimpleDbClientState {
    pub fn get_connection_id(&self) -> usize {
        match self {
            SimpleDbClientState::ConnectedToDatabase(connection_id) => *connection_id,
            SimpleDbClientState::NotConnectedToDatabase => panic!("Illegal code path")
        }
    }
}