use crate::request::Request;
use crate::response::{ColumnDescriptor, QueryDataResponse, Response, StatementResponse};
use crate::simpledb_server::SimpleDbServer;
use crate::table_print::TablePrint;
use std::cmp::Ordering;
use std::io;
use std::io::{stdout, Write};
use std::process::exit;
use shared::ErrorTypeId;

pub struct SimpleDbCli {
    server: SimpleDbServer,
    password: String,
    is_standalone: bool,
}

impl SimpleDbCli {
    pub fn create(
        address: String,
        password: String,
    ) -> SimpleDbCli {
        SimpleDbCli {
            server: SimpleDbServer::create(address),
            is_standalone: true,
            password
        }
    }

    pub fn start(&mut self) -> ! {
        loop {
            print!("simpledb> ");
            let _ = stdout().flush();
            let input = self.read_input_from_user().to_lowercase();
            let input = input.trim_end();

            if input.starts_with("use") {
                self.use_command(input);
            } else if input.eq("exit") {
                self.exit_command();
            } else {
                self.statement_command(input);
            }
        }
    }

    fn statement_command(&mut self, statement: &str) {
        if statement.starts_with("start_transaction") {
            self.is_standalone = false;
        }

        let response = self.server.send_request(Request::Statement(
            self.password.clone(), self.is_standalone, statement.to_string()
        ));
        self.print_response(response);

        if statement.starts_with("rollback") || statement.starts_with("commit") {
            self.is_standalone = true;
        }
    }

    fn print_response(&mut self, response: Response) {
        match response {
            Response::Statement(statement_result) => {
                match statement_result {
                    StatementResponse::Ok(n_rows_affected) => println!("{} rows affected!", n_rows_affected),
                    StatementResponse::Data(data) => self.print_query_data(data),
                    StatementResponse::Databases(databases) => self.print_vec_string_as_table("Databases", databases),
                    StatementResponse::Tables(tables) => self.print_vec_string_as_table("Tables", tables),
                    StatementResponse::Describe(desc) => self.print_table_describe(&desc)
                };
            }
            Response::Error(error_type_id) => {
                Self::print_error(error_type_id);
            }
            Response::Ok => {
                println!("Ok");
            }
        };
        print!("\n");
    }

    fn print_query_data(&self, query_data: QueryDataResponse) {
        let mut columns_desc = query_data.columns_desc;
        columns_desc.sort_by(|a, b| {
            if a.is_primary {
                return Ordering::Less
            } else if b.is_primary {
                return Ordering::Greater
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
        let mut table = TablePrint::create(4);
        table.add_header("Field");
        table.add_header("Type");
        table.add_header("Primary");
        table.add_header("Indexed");

        for column_desc in columns_desc {
            table.add_column_value(column_desc.column_name.clone());
            table.add_column_value(column_desc.column_type.to_string().to_string());

            if column_desc.is_primary {
                table.add_column_value("True".to_string());
            } else {
                table.add_column_value("False".to_string());
            }

            if !column_desc.is_primary && column_desc.is_indexed {
                table.add_column_value("True".to_string());
            } else if column_desc.is_primary {
                table.add_column_value("True (Primary key)".to_string());
            } else if !column_desc.is_indexed {
                table.add_column_value("False".to_string());
            }
        }

        table.print();
    }

    fn print_vec_string_as_table(&self, table_header_name: &str, vec: Vec<String>) {
        let mut table = TablePrint::create(1);
        table.add_header(table_header_name);
        for item in vec {
            table.add_column_value(item);
        }
        table.print();
    }

    fn exit_command(&mut self) {
        self.server.send_request(Request::Close(self.password.clone()));

        println!("Bye");
        exit(0)
    }

    fn use_command(&mut self, input: &str) {
        let statement_split_by_space: Vec<&str> = input.split_whitespace().collect();

        if let Some(database_name) = statement_split_by_space.get(1) {
            let database_name = database_name.replace(';', "");
            self.connect_to_database(database_name.to_string());
        } else {
            println!("Invalid syntax. Usage: USE <Database name>");
        }
    }

    fn connect_to_database(&mut self, database_name: String) {
        let response = self.server.send_request(Request::UseDatabase(
            self.password.clone(), database_name.clone()
        ));
        self.print_response(response);
    }

    fn read_input_from_user(&self) -> String {
        let mut line = String::from("");

        io::stdin()
            .read_line(&mut line)
            .expect("Failed to read line");

        line
    }

    fn print_error(error_type_id: ErrorTypeId) {
        match error_type_id {
            57 => println!("Invalid password!"),
            56 => println!("Invalid request binary format!"),
            2 => println!("Range scan is not allowed!"),
            5 => println!("Full scan is not allowed!"),
            3 | 4 => println!("Invalid query syntax"),
            6 => println!("You should connect to a database or start a transaction first"),
            7 => println!("Column not found"),
            8 => println!("Table not found"),
            9 => println!("Table already exists"),
            10 => println!("Primary column should be included when creating a table"),
            11 => println!("Only one primary column can be created in a table"),
            12 => println!("Column already exists"),
            13 => println!("Column not found"),
            14 => println!("Invalid column type"),
            16 => println!("Database already exists"),
            17 => println!("Database not found"),
            _ => println!("Received error {} code from server", error_type_id)
        }
    }

}