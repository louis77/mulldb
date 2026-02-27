# mulldb

A lightweight SQL database written from scratch in Go that speaks the PostgreSQL wire protocol. Standard tools like `psql` and any PG-compatible driver work out of the box.

mulldb is designed for correctness and clarity over raw performance — a usable tool for light workloads, not a toy, but not aiming for Postgres-level completeness.

## Features

- **PostgreSQL wire protocol (v3)** — connect with `psql`, `pgx`, `node-postgres`, or any PG driver
- **Persistent storage** — write-ahead log (WAL) with CRC32 checksums and fsync for crash recovery
- **SQL support** — CREATE TABLE, DROP TABLE, INSERT, SELECT (with WHERE, LIMIT, OFFSET, and column aliases via AS), UPDATE, DELETE
- **PRIMARY KEY constraints** — single-column primary keys with uniqueness enforcement, backed by B-tree indexes for O(log n) lookups
- **Aggregate functions** — `COUNT(*)`, `COUNT(col)`, `SUM(col)`, `MIN(col)`, `MAX(col)`
- **Scalar functions** — `VERSION()` and a registration pattern for adding more
- **Data types** — INTEGER (64-bit), TEXT, BOOLEAN, NULL
- **WHERE clauses** — comparisons (`=`, `!=`, `<>`, `<`, `>`, `<=`, `>=`), logical (`AND`, `OR`), parenthesized expressions
- **Full UTF-8 support** — identifiers, string literals, and all data are UTF-8 throughout; no other character encoding exists
- **Double-quoted identifiers** — use reserved words as identifiers, preserve exact casing (`"select"`, `"Order"`), Unicode identifiers (`"café"`, `"名前"`)
- **WAL migration** — versioned WAL format with opt-in `--migrate` flag and backup preservation
- **Concurrent access** — single-writer / multi-reader via RWMutex, safe for multiple connections
- **Cleartext password authentication** — simple username/password access control
- **Graceful shutdown** — drains active connections on SIGINT/SIGTERM
- **Proper error codes** — PostgreSQL SQLSTATE codes in ErrorResponse messages

## Quick Start

### Build

```bash
go build -o mulldb .
```

### Run

```bash
./mulldb --port 5433 --datadir ./data --user admin --password secret
```

### Connect

```bash
psql -h 127.0.0.1 -p 5433 -U admin
```

### Try it out

```sql
CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, active BOOLEAN);

INSERT INTO users (id, name, active) VALUES (1, 'alice', TRUE), (2, 'bob', FALSE);

SELECT * FROM users;
--  id | name  | active
-- ----+-------+--------
--   1 | alice | t
--   2 | bob   | f

SELECT name FROM users WHERE active = TRUE;
--  name
-- -------
--  alice

UPDATE users SET active = TRUE WHERE id = 2;

DELETE FROM users WHERE id = 1;

DROP TABLE users;
```

## Configuration

All options can be set via CLI flags or environment variables. Environment variables take precedence over defaults but flags take precedence over environment variables.

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `--port` | `MULLDB_PORT` | `5433` | TCP port to listen on |
| `--datadir` | `MULLDB_DATADIR` | `./data` | Directory for WAL and data files |
| `--user` | `MULLDB_USER` | `admin` | Username for authentication |
| `--password` | `MULLDB_PASSWORD` | *(empty)* | Password for authentication |
| `--log-level` | `MULLDB_LOG_LEVEL` | `0` | Log verbosity: `0` = off, `1` = log SQL statements with outcome (`OK`/`ERROR`) and row counts |
| `--migrate` | — | `false` | Migrate WAL file format if needed (see [WAL Migration](#wal-migration)) |

Example with environment variables:

```bash
export MULLDB_PORT=5433
export MULLDB_DATADIR=/var/lib/mulldb
export MULLDB_USER=myuser
export MULLDB_PASSWORD=mypass
export MULLDB_LOG_LEVEL=1
./mulldb
```

## SQL Reference

### Supported Statements

```sql
-- Create a table
CREATE TABLE <name> (<column> <type>, ...);
CREATE TABLE <name> (<column> <type> PRIMARY KEY, ...);  -- with primary key

-- Drop a table
DROP TABLE <name>;

-- Insert one or more rows
INSERT INTO <table> (<columns>) VALUES (<values>), (<values>);
INSERT INTO <table> VALUES (<values>);  -- all columns, in order

-- Query rows
SELECT * FROM <table>;
SELECT <columns> FROM <table> WHERE <condition>;
SELECT <expr> AS <alias>, ... FROM <table>;  -- column aliases
SELECT * FROM <table> LIMIT <n>;             -- return at most n rows
SELECT * FROM <table> OFFSET <n>;            -- skip first n rows
SELECT * FROM <table> LIMIT <n> OFFSET <m>;  -- pagination

-- Static SELECT (no table required)
SELECT 1;
SELECT 1, 'hello', TRUE, NULL;
SELECT VERSION();

-- Aggregate queries (returns a single row)
SELECT COUNT(*) FROM <table>;
SELECT COUNT(<column>) FROM <table>;
SELECT SUM(<column>) FROM <table>;
SELECT MIN(<column>) FROM <table>;
SELECT MAX(<column>) FROM <table>;
SELECT COUNT(*), SUM(<column>), MIN(<column>), MAX(<column>) FROM <table>;

-- Update rows
UPDATE <table> SET <column> = <value>, ... WHERE <condition>;
UPDATE <table> SET <column> = <value>;  -- all rows

-- Delete rows
DELETE FROM <table> WHERE <condition>;
DELETE FROM <table>;  -- all rows
```

### Character Encoding

mulldb uses **UTF-8 exclusively** — there is no encoding configuration and no other character set. All layers handle UTF-8 natively:

- **Identifiers** — table and column names can contain any Unicode letter (`café`, `名前`, `αβγ`), both unquoted and double-quoted
- **String literals** — `'München'`, `'東京'`, `'hello 🌍'` all work as expected
- **Storage and WAL** — strings are stored as raw UTF-8 bytes with byte-length prefixes
- **Wire protocol** — UTF-8 bytes are sent as-is over the PostgreSQL wire protocol, which is encoding-aware

String comparison is **binary** (byte-order). There is no locale-aware collation — `'a' < 'b'` works, but locale-specific sort orders (e.g. German `ä` sorting with `a`) are not supported.

### Data Types

| Type | Go representation | Description |
|------|------------------|-------------|
| `INTEGER` | `int64` | 64-bit signed integer (aliases: `INT`, `SMALLINT`, `BIGINT`) |
| `TEXT` | `string` | Variable-length UTF-8 string |
| `BOOLEAN` | `bool` | `TRUE` or `FALSE` |
| `NULL` | `nil` | Absence of a value (any column) |

### Aggregate Functions

Aggregate functions collapse all matching rows into a single result row. Multiple aggregates can appear in the same `SELECT`. Mixing aggregate and non-aggregate columns in the same `SELECT` is an error (SQLSTATE `42803`) — `GROUP BY` is not supported.

| Function | Argument | Returns | Description |
|----------|----------|---------|-------------|
| `COUNT(*)` | — | `INTEGER` | Count of all rows |
| `COUNT(col)` | any column | `INTEGER` | Count of non-NULL values in `col` |
| `SUM(col)` | `INTEGER` column | `INTEGER` | Sum of all non-NULL values |
| `MIN(col)` | `INTEGER` or `TEXT` column | same as `col` | Smallest non-NULL value |
| `MAX(col)` | `INTEGER` or `TEXT` column | same as `col` | Largest non-NULL value |

Function names are case-insensitive (`sum`, `Sum`, `SUM` all work).

**Examples:**

```sql
CREATE TABLE orders (amount INTEGER, status TEXT);
INSERT INTO orders VALUES (10, 'paid'), (25, 'paid'), (5, 'pending'), (40, 'paid');

SELECT COUNT(*) FROM orders;
--  count
-- -------
--      4

SELECT SUM(amount) FROM orders;
--  sum
-- -----
--   80

SELECT MIN(amount), MAX(amount) FROM orders;
--  min | max
-- -----+-----
--    5 |  40

SELECT COUNT(*), SUM(amount), MIN(amount), MAX(amount) FROM orders;
--  count | sum | min | max
-- -------+-----+-----+-----
--      4 |  80 |   5 |  40
```

### Column Aliases (AS)

Any column expression in a `SELECT` can be renamed with `AS <alias>`. This works with plain columns, aggregate functions, and static expressions.

**Examples:**

```sql
SELECT name AS username, id AS user_id FROM users;
--  username | user_id
-- ----------+---------
--  alice    |       1

SELECT COUNT(*) AS total FROM orders;
--  total
-- -------
--      4

SELECT COUNT(*) AS n, SUM(amount) AS total FROM orders;
--  n | total
-- ---+-------
--  4 |    80

SELECT 1 AS num, 'hello' AS greeting;
--  num | greeting
-- -----+----------
--    1 | hello
```

### LIMIT and OFFSET

`LIMIT` restricts the number of rows returned; `OFFSET` skips rows before returning. Both are optional and can appear in either order. Without `ORDER BY`, the order of rows is undefined.

**Examples:**

```sql
CREATE TABLE items (id INTEGER, name TEXT);
INSERT INTO items VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e');

SELECT * FROM items LIMIT 3;
-- Returns 3 rows

SELECT * FROM items OFFSET 2;
-- Skips 2 rows, returns the remaining 3

SELECT * FROM items LIMIT 2 OFFSET 1;
-- Skips 1 row, then returns the next 2

SELECT * FROM items LIMIT 0;
-- Returns 0 rows (valid)

SELECT * FROM items OFFSET 100;
-- Returns 0 rows (offset beyond row count)

SELECT * FROM items WHERE id > 1 LIMIT 2;
-- LIMIT applies after WHERE filtering
```

### Scalar Functions

Scalar functions can be called in a `SELECT` without a `FROM` clause. They take zero or more arguments and return a single value.

| Function | Returns | Description |
|----------|---------|-------------|
| `VERSION()` | `TEXT` | PostgreSQL-compatible version string identifying the mulldb build |

Function names are case-insensitive.

**Examples:**

```sql
SELECT VERSION();
--                           version
-- ----------------------------------------------------------
--  PostgreSQL 15.0 (mulldb dev, commit abc1234, built ...)

SELECT 1, 'hello', TRUE, NULL;
--  ?column? | ?column? | ?column? | ?column?
-- ----------+----------+----------+----------
--         1 | hello    | t        |
```

Calling an unknown function returns SQLSTATE `42883`. Calling a function with the wrong number of arguments also returns `42883`.

### Catalog Tables

mulldb exposes virtual catalog tables that mimic PostgreSQL system catalogs. These are read-only — `INSERT`, `UPDATE`, and `DELETE` return an error (SQLSTATE `42809`).

Tables can be accessed with or without schema qualification. Unqualified names check `pg_catalog` first (matching PostgreSQL behavior). `information_schema` tables require explicit schema qualification.

| Table | Columns | Description |
|-------|---------|-------------|
| `pg_type` / `pg_catalog.pg_type` | `oid` (INTEGER), `typname` (TEXT) | Type information for supported data types |
| `pg_database` / `pg_catalog.pg_database` | `datname` (TEXT) | Database names (always returns `mulldb`) |
| `information_schema.tables` | `table_schema` (TEXT), `table_name` (TEXT), `table_type` (TEXT) | Lists all user tables and system catalog tables |
| `information_schema.columns` | `table_schema` (TEXT), `table_name` (TEXT), `column_name` (TEXT), `ordinal_position` (INTEGER), `data_type` (TEXT), `is_nullable` (TEXT) | Column metadata for all tables |

**Examples:**

```sql
SELECT * FROM pg_type;
SELECT * FROM pg_catalog.pg_type;  -- same result

SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = 'public';
--  table_name | table_type
-- ------------+------------
--  users      | BASE TABLE
--  orders     | BASE TABLE

SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = 'users';
--  column_name | data_type | is_nullable
-- -------------+-----------+-------------
--  id          | integer   | NO
--  name        | text      | YES
--  active      | boolean   | YES
```

### WHERE Expressions

- **Comparisons**: `=`, `!=`, `<>`, `<`, `>`, `<=`, `>=`
- **Logical operators**: `AND`, `OR`
- **Parentheses**: `(expr)` for grouping
- **Literals**: integers, `'single-quoted strings'`, `TRUE`, `FALSE`, `NULL`

Operator precedence (lowest to highest): `OR` → `AND` → comparisons → primary.

## Architecture

```
psql / PG drivers
       │ TCP
       ▼
┌─────────────────────┐
│   Network Layer      │  Accept connections, goroutine per connection
│   (server/)          │
├─────────────────────┤
│   PG Wire Protocol   │  Startup handshake, auth, SimpleQuery,
│   (pgwire/)          │  RowDescription, DataRow, CommandComplete
├─────────────────────┤
│   SQL Parser         │  Lexer → tokens → recursive descent → AST
│   (parser/)          │
├─────────────────────┤
│   Query Executor     │  Walk AST, evaluate WHERE, call storage
│   (executor/)        │
├─────────────────────┤
│   Storage Engine     │
│   (storage/)         │
│   ├─ Catalog         │  In-memory table schemas (rebuilt from WAL)
│   ├─ Heap            │  In-memory row data per table
│   ├─ Index           │  B-tree indexes for primary key columns
│   └─ WAL             │  Append-only log for crash recovery
└─────────────────────┘
       │
    Data dir
    └── wal.dat         Write-ahead log
```

### Design Principles

- **Modular via interfaces** — every layer boundary is a Go interface. Packages depend on contracts, never on concrete types from other layers.
- **No circular dependencies** — dependency flows downward: `server` → `executor` → `parser` + `storage`. `main.go` is the composition root.
- **Testable in isolation** — each package has unit tests that don't require a running server or real disk.
- **WAL-first writes** — every mutation is logged to the WAL before being applied to in-memory state. On startup, the WAL is replayed to reconstruct the full database.

### Concurrency Model

Multiple clients can connect simultaneously. The server spawns a goroutine per connection (`server/server.go`), and all goroutines share a single stateless executor that forwards calls to the storage engine.

**Global RWMutex.** The storage engine (`storage/engine.go`) uses a single `sync.RWMutex` to protect all state:

| Lock mode | Operations |
|-----------|-----------|
| Read lock (`RLock`) | `GetTable`, `ListTables`, `Scan`, `LookupByPK` |
| Write lock (`Lock`) | `CreateTable`, `DropTable`, `Insert`, `Update`, `Delete` |

Multiple readers can run concurrently across any tables. Only one writer can proceed at a time, and it blocks all readers while held. The lock is global, not per-table — a write to table A blocks reads from table B.

**Snapshot iterators.** `Scan` copies all matching rows into a new slice while the read lock is held, then returns an iterator over that private snapshot. The iterator is safe to consume after the lock is released. `LookupByPK` similarly returns a copied row.

**WAL and index safety.** Neither the WAL (`storage/wal.go`) nor the B-tree index (`storage/index/btree.go`) have internal locks. They rely on being called only while the engine's global lock is held.

**Atomic batch writes.** Multi-row `INSERT` and `UPDATE` validate all constraints (PK uniqueness, column count) before writing anything. If validation passes, WAL entries are written and in-memory state is updated within a single lock acquisition — no partial writes on constraint violation.

### Persistence

Every write goes through the WAL before being applied in memory:

1. Caller invokes `engine.Insert(...)` (or Update, Delete, etc.)
2. Engine acquires write lock
3. WAL entry is written and fsynced: `[4-byte length][1-byte op][payload][4-byte CRC32]`
4. In-memory heap is updated
5. Lock is released

On startup, `Open()` replays the WAL from the beginning, calling `OnCreateTable`, `OnInsert`, `OnDelete`, `OnUpdate` to rebuild the full in-memory state (including indexes). This means the WAL is the sole source of truth — there are no separate data files.

The WAL file uses a versioned binary format (`[4-byte magic "MWAL"][uint16 version][entries...]`). When the format changes between releases, the `--migrate` flag must be used to upgrade the file. See [WAL Migration](#wal-migration).

## WAL Migration

The WAL (write-ahead log) uses a versioned binary format. When a new release changes the format, the engine will refuse to start:

```
open storage: WAL file is format version 1 but version 2 is required; restart with --migrate flag
```

To migrate, restart with `--migrate`:

```bash
./mulldb --datadir ./data --migrate
```

The migration:

1. Checks that enough disk space is available (roughly 2x the WAL file size)
2. Writes a new WAL file in the current format
3. Preserves the original as `data/wal.dat.bak` (or `.bak.1`, `.bak.2`, etc. if a backup already exists)
4. Starts the engine normally

After verifying the database works correctly, you can manually delete the backup file. The engine will never delete it for you.

If `--migrate` is passed but no migration is needed, the engine logs an info message and starts normally.

## Project Structure

```
mulldb/
├── main.go                 Entry point, signal handling, wiring
├── go.mod
├── PLAN.md                 Design document
├── DESIGN.md               Architecture details and WAL format
├── STANDARD.md             SQL standard (Core SQL) conformance checklist
├── CLAUDE.md               Project conventions (AI-assistant facing)
│
├── config/
│   └── config.go           CLI flags + env var parsing
│
├── server/
│   ├── server.go           TCP listener, accept loop, graceful shutdown
│   └── connection.go       Per-connection lifecycle, query dispatch
│
├── pgwire/
│   ├── protocol.go         PG v3 message types and constants
│   ├── reader.go           Read PG messages from net.Conn
│   └── writer.go           Write PG messages to net.Conn
│
├── parser/
│   ├── token.go            Token types and keywords
│   ├── lexer.go            Tokenizer (SQL → tokens)
│   ├── ast.go              AST node types
│   ├── parser.go           Recursive descent parser (tokens → AST)
│   └── parser_test.go
│
├── executor/
│   ├── executor.go         Query execution (AST → storage → results)
│   ├── scalar.go           Scalar function registry and static SELECT evaluation
│   ├── fn_version.go       VERSION() implementation (registers via init())
│   ├── result.go           Result types, QueryError, SQLSTATE mapping
│   └── executor_test.go
│
├── version/
│   └── version.go          Build-info package; Tag/GitCommit/BuildTime set via -ldflags
│
└── storage/
    ├── types.go            Data types, typed errors, Engine interface
    ├── catalog.go          In-memory table schema management
    ├── heap.go             In-memory row storage per table
    ├── compare.go          Type-aware value comparison
    ├── wal.go              Write-ahead log (write, replay, checksums)
    ├── wal_migrate.go      WAL format migration framework
    ├── wal_test.go         WAL migration tests
    ├── row.go              Binary row encoding/decoding
    ├── engine.go           WAL-first engine with RWMutex concurrency
    ├── engine_test.go
    │
    └── index/
        ├── index.go        Index interface
        └── btree.go        In-memory B-tree index implementation
```

## Testing

Run the full test suite:

```bash
go test ./...
```

Run with the race detector:

```bash
go test -race ./...
```

The test suite covers:
- **Parser**: all 6 statement types, WHERE with AND/OR/precedence, operators, aggregate and scalar function syntax, column aliases (AS), optional FROM clause, UTF-8 identifiers and string literals, error cases
- **Storage**: CRUD operations, WAL replay across restart, typed errors, concurrent reads and writes
- **Executor**: full round-trip (CREATE → INSERT → SELECT → UPDATE → DELETE), aggregate functions (COUNT/SUM/MIN/MAX), LIMIT/OFFSET, column aliases, static SELECT (literals and scalar functions), SQLSTATE codes, column resolution, NULL handling

## Error Handling

mulldb returns proper PostgreSQL SQLSTATE codes in ErrorResponse messages:

| SQLSTATE | Condition | Example |
|----------|-----------|---------|
| `42601` | Syntax error | `FROBNICATE` |
| `42P01` | Undefined table | `SELECT * FROM nonexistent` |
| `42P07` | Duplicate table | `CREATE TABLE t (...)` when `t` exists |
| `42703` | Undefined column | `SELECT bad_col FROM t` |
| `22023` | Invalid parameter value | Wrong number of INSERT values |
| `23505` | Unique violation | Inserting a duplicate primary key value |
| `42803` | Grouping error | Mixing aggregate and non-aggregate columns |
| `42809` | Wrong object type | `INSERT INTO pg_type ...` (catalog is read-only) |
| `42883` | Undefined function | Unknown aggregate function or type mismatch |

## Limitations

mulldb is intentionally minimal. Things it does **not** support:

- **Secondary indexes** — only primary key columns are indexed; other columns do full table scans
- **Multi-column primary keys** — only single-column PRIMARY KEY is supported
- **Transactions** — no BEGIN/COMMIT/ROLLBACK
- **JOINs** — single-table queries only
- **ORDER BY / GROUP BY / HAVING**
- **AVG** — not implemented (use `SUM` / `COUNT` manually)
- **ALTER TABLE**
- **Expressions in SELECT** — arithmetic like `1 + 2` is not supported; only literals and scalar functions work without `FROM`
- **Subqueries**
- **Extended query protocol** — only SimpleQuery flow
- **TLS/SSL** — connections are unencrypted (SSL negotiation is refused)
- **Multiple databases** — single database per instance

## License

MIT License. See [LICENSE](LICENSE) for details.
