use std::cmp::max;

pub struct TablePrint {
    header: Vec<String>,
    rows: Vec<Vec<String>>,
    columns_width: Vec<usize>,

    n_columns: usize,
}

impl TablePrint {
    pub fn create(n_columns: usize) -> TablePrint {
        let mut columns_width = Vec::new();
        for _ in 0..n_columns {
            columns_width.push(0);
        }

        TablePrint {
            header: Vec::new(),
            rows: Vec::new(),
            columns_width,
            n_columns
        }
    }

    pub fn add_header(&mut self, header: &str) {
        let column_index = self.header.len();
        self.header.push(header.to_string());

        self.columns_width[column_index] = max(self.columns_width[column_index], header.len());
    }

    pub fn add_column_value(&mut self, value: String) {
        if self.rows.is_empty() {
            self.rows.push(Vec::new());
        }

        let mut row_vec_index = self.rows.len() - 1;
        let mut row_vec = &mut self.rows[row_vec_index];
        if row_vec.len() == self.n_columns {
            let new_row_vec = Vec::new();
            self.rows.push(new_row_vec);
            row_vec_index = self.rows.len() - 1;
            row_vec = &mut self.rows[row_vec_index];
        }

        let n_column_index = row_vec.len();
        let value_width = value.len();

        row_vec.push(value);

        self.columns_width[n_column_index] = max(self.columns_width[n_column_index], value_width);
    }

    pub fn print(&self) {
        self.print_horizontal_line();

        self.print_header_row();
        self.print_rows();

        self.print_horizontal_line();

        self.print_resume();
    }

    fn print_resume(&self) {
        println!("{} rows in set", self.rows.len());
    }

    fn print_rows(&self) {
        if !self.rows.is_empty() {
            print!("|");
        }

        for (row_index, row) in self.rows.iter().enumerate() {
            for (column_index, cell) in row.iter().enumerate() {
                let column_width: usize = self.columns_width[column_index];

                print!("{}", cell);
                for _ in 0..(column_width - cell.len()) {
                    print!(" ");
                }

                print!("|");
                if column_index + 1 == self.n_columns {
                    print!("\n");
                }
            }

            if row_index + 1 < self.rows.len() {
                self.print_horizontal_line();
            }
        }
    }

    fn print_header_row(&self) {
        print!("|");
        for (column_index, header) in self.header.iter().enumerate() {
            print!("{}", header);
            let column_width: usize = self.columns_width[column_index];
            for _ in 0..(column_width - header.len()) {
                print!(" ");
            }

            print!("|");
        }
        print!("\n");
        self.print_horizontal_line();
    }

    fn print_horizontal_line(&self) {
        print!("+");
        print!("{}", "-".repeat(self.total_width()));
        print!("+\n");
    }

    fn total_width(&self) -> usize {
        let mut total_width = 0;
        for column_max_width in &self.columns_width {
            total_width = column_max_width + total_width;
        }

        total_width
    }

}