use crate::request::Request;
use crate::response::{Column, IndexType, RowsResponse, Response, StatementResponse};
use crate::simpledb_server::SimpleDbServer;
use crate::table_print::TablePrint;
use std::cmp::Ordering;
use std::io;
use std::io::{stdout, Write};
use std::process::exit;
use std::time::Duration;
use shared::ErrorTypeId;
use crate::utils::duration_to_string;

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

        let (response, duration) = self.server.send_request(Request::Statement(
            self.password.clone(), self.is_standalone, statement.to_string()
        ));
        self.print_response(response, duration);

        if statement.starts_with("rollback") || statement.starts_with("commit") {
            self.is_standalone = true;
        }
    }

    fn print_response(&mut self, response: Response, duration: Duration) {
        match response {
            Response::Statement(statement_result) => {
                match statement_result {
                    StatementResponse::Ok(n_rows_affected) => println!("{} rows affected! ({})", n_rows_affected, duration_to_string(duration)),
                    StatementResponse::Rows(data) => self.print_query_data(data, duration),
                    StatementResponse::Databases(databases) => self.print_vec_string_as_table("Databases", databases, duration),
                    StatementResponse::Tables(tables) => self.print_vec_string_as_table("Tables", tables, duration),
                    StatementResponse::Explain(explain_lines) => self.print_explain_lines(explain_lines, duration),
                    StatementResponse::Describe(desc) => self.print_table_describe(&desc, duration),
                    StatementResponse::Indexes(indexes) => self.print_show_indexes(indexes, duration),
                };
            }
            Response::Error(error_type_id, error_message) => {
                Self::print_error(error_type_id, error_message);
            }
            Response::Ok => {
                println!("Ok ({})", duration_to_string(duration));
            }
        };

        print!("\n");
    }

    fn print_explain_lines(&self, lines: Vec<String>, duration: Duration) {
        let mut table = TablePrint::create(1);
        table.add_header("Step");

        for line in lines {
            table.add_column_value(line);
        }

        table.print(duration)
    }

    fn print_show_indexes(&self, mut indexes: Vec<(String, IndexType)>, duration: Duration) {
        let mut table = TablePrint::create(2);
        table.add_header("Field");
        table.add_header("Type");

        for (index_column_name, index_type) in indexes {
            table.add_column_value(index_column_name);
            match index_type {
                IndexType::Secondary => table.add_column_value("Secondary".to_string()),
                IndexType::Primary => table.add_column_value("Primary".to_string())
            };
        }
        
        table.print(duration);
    }

    fn print_query_data(&self, query_data: RowsResponse, duration: Duration) {
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

        query_data_table.print(duration);
    }

    fn print_table_describe(
        &self,
        columns_desc: &Vec<Column>,
        duration: Duration
    ) {
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

        table.print(duration);
    }

    fn print_vec_string_as_table(
        &self,
        table_header_name: &str,
        vec: Vec<String>,
        duration: Duration
    ) {
        let mut table = TablePrint::create(1);
        table.add_header(table_header_name);
        for item in vec {
            table.add_column_value(item);
        }
        table.print(duration);
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
        let (response, duration) = self.server.send_request(Request::UseDatabase(
            self.password.clone(), database_name.clone()
        ));
        self.print_response(response, duration);
    }

    fn read_input_from_user(&self) -> String {
        let mut line = String::from("");

        io::stdin()
            .read_line(&mut line)
            .expect("Failed to read line");

        line
    }

    fn print_error(error_type_id: ErrorTypeId, error_message: String) {
        match error_type_id {
            57 => print!("Invalid password!"),
            56 => print!("Invalid request binary format!"),
            2 => print!("Range scan is not allowed!"),
            5 => print!("Full scan is not allowed!"),
            3 | 4 => print!("Invalid query syntax"),
            6 => print!("You should connect to a database or start a transaction first"),
            7 => print!("Column not found"),
            8 => print!("Table not found"),
            9 => print!("Table already exists"),
            10 => print!("Primary column should be included when creating a table"),
            11 => print!("Only one primary column can be created in a table"),
            12 => print!("Column already exists"),
            13 => print!("Column not found"),
            14 => print!("Invalid column type"),
            16 => print!("Database already exists"),
            17 => print!("Database not found"),
            _ => print!("Received error {} code from server", error_type_id)
        };

        print!(" Message: {}\n", error_message);
    }
}