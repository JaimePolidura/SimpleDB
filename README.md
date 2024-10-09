# SimpleDB
SQL database built over a LSM storage engine.

This project consist of three layers:
- Storage engine
- DB
- Server

## Features
- Tables
- SQL-Like queries
- MVCC Transactions
- Secondary indexes

## Missing features
- SSL/TLS encryption is not supported.
- Triggers.
- Multiple index types.
- Serializable transaction support.
- Joins and inner queries.
- 
## Storage engine (/storage)
The storage engine exposes an API which is used by the upper layer (DB).
- <b>LSM Based</b> The engine is based on a Log-Structured Merge (LSM) tree, following the <a link="https://skyzh.github.io/mini-lsm/00-preface.html">LSM in a week</a> guide. 
- <b>Simple API</b> The engine exposes simple API operations like: get(key), set(key, value), delete(key), scan_all() and scan_from(key).
- <b>MVCC Transaction support </b> The storage engine exposes an API to support transactions: start_transaction(), commit() and rollback(). 
- <b>Consistency and durability</b> It has a transaction log and a memtable WAL to ensure durability and consistency during crashes.
- <b>Compaction</b> The storage engine provides two compaction algorithms: SimpleLeveled and SizeTiered.
- <b>Structure</b> An instance of a storage engine, consists of multiple keyspaces (like SQL tables, where keys can be written or read) and a transaction log.

## DB (/db)
The DB layer uses the storage engine layer to create tables, rows and databases. And it exposes an API to run SQL-like queries.

- <b>Database and Tables (database.rs table.rs)</b> For each database created, an instance of the storage engine is created. Each table in the database corresponds to a keyspace in the database's storage engine instance.
- <b>Table schema (table_descriptor.rs)</b> As SQL-like databases, every table will have columns with its name and type. Every table will have a file, which stores their schema.
- <b>Row mapping (row.rs record.rs)</b> The key used in the storage engine will be the primary key of the row, and the value used in the storage engine will represent the column values.
  - For example to insert a value in a table, the storage engine operation would be: set(key = Primary key, value = |Column ID #1 | Column Value #1 |Column ID #2 | Column Value #2 |...(binary))
- <b>Append only updates</b> Update operations (like SQL updates, deletes or inserts) in the storage engine are append only. For example to update a row, the operation would be: set(key = Primary key of the row being updated, value = |Column ID being updated | New Column value |)
- <b>Read operations (table_iterator.rs)</b> Scans are performed using iterators. Since row data may be scattered across SSTables and Memtables due to the append-only update mechanism, the iterators must reassemble the full row before returning it to the user.
- <b>Secondary indexes (secondary_index.rs)</b>. Secondary indexes map the indexed column value to a list of primary keys that contain that value. A separate storage engine keyspace will be created for each secondary index.
  - For example to update a secondary index value, the storage engine operation would be: set(key = Indexed value, value = |Primary key #1 | Primary key #2|...)
- <b>Queries (statement.rs)</b>  Once a table interface is established for updating values, inserting records, and reading rows, queries can be parsed and executed. Each query undergoes several steps:
  - <b>Tokenization (tokenizer.rs)</b>. The query is transformed into a stream of tokens.
  - <b>Parsing (statement.rs)</b>. Given the list of tokens is converted into an Abstract Syntax Tree (AST).
  - <b>Validation (validator.rs)</b>. The AST is validated by checking the types, column names, and table names to ensure they are correct and consistent.
  - <b>Scan type analysis (scan_type_analyzer.rs)</b>. For expressions that require scanning a table, an analysis is performed to determine the appropriate scan method: full scan, range scan, secondary index scan, or merge index scan.
  - <b>Plan creation (planner.rs)</b>. Given a scan type and a statement, a plan is created. A plan is just a series of steps to execute a query. 
  - <b>Execution (executor.rs)</b>. Finally, the query is executed according to the generated plan.

## Server (/server)
- Exposes simple TCP server to execute client requests. The default port is 8888
- Includes custom binary format.
- Includes authentication passwords. The default password is 123456

## Client (/client-cli)
- Simple CLI client like mysql.
