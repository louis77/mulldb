# mulldb — A SQL Database in Go

## Context
Building a lightweight SQL database from scratch in Go as a usable tool for light workloads. The database speaks the PostgreSQL wire protocol (v3), so standard `psql` and PG drivers work out of the box. The goal is a correct, understandable implementation — not a toy, but not aiming for Postgres-level completeness.

## Decisions

| Area | Decision |
|---|---|
| Goal | Usable tool — correct, simple, light workloads |
| Language | Go |
| Wire protocol | PostgreSQL v3 (simple query flow) |
| Auth | Cleartext password (AuthenticationCleartextPassword) |
| Parser | Hand-written lexer + recursive descent parser |
| SQL scope | Minimal CRUD: `CREATE TABLE`, `DROP TABLE`, `INSERT`, `SELECT` (with `WHERE`, `LIMIT`, `OFFSET`), `UPDATE`, `DELETE`. Double-quoted identifiers for reserved words and case preservation. |
| Data types | `INTEGER`, `TEXT`, `BOOLEAN` |
| Storage engine | Append-only data log + in-memory index (rebuilt on startup) |
| Durability | Write-ahead log (WAL) — every mutation logged before applied |
| Concurrency | Single-writer, multi-reader (one write goroutine, concurrent reads) |
| Config | CLI flags with env var fallbacks (`MULLDB_PORT`, etc.) |
| Process model | Foreground daemon, graceful shutdown on SIGINT/SIGTERM |
| Modularity | Go interfaces at every layer boundary; packages interact only through contracts |

## Modularity — Interface Contracts

Each layer exposes a Go interface. Packages never import concrete types from other layers — only interfaces and shared data types. This makes every component independently testable (via mocks) and swappable.

### Core Interfaces

```go
// parser exposes this — the executor depends on it, not on parser internals
type Parser interface {
    Parse(sql string) (Statement, error)
}

// Statement is the AST union type returned by the parser
// Concrete types: CreateTableStmt, DropTableStmt, InsertStmt, SelectStmt, UpdateStmt, DeleteStmt
type Statement interface {
    statementNode()
}

// storage exposes this — the executor depends on it, not on storage internals
type Engine interface {
    CreateTable(name string, columns []ColumnDef) error
    DropTable(name string) error
    Insert(table string, columns []string, values [][]Value) (int64, error)
    Scan(table string) (RowIterator, error)
    Update(table string, sets map[string]Value, filter func(Row) bool) (int64, error)
    Delete(table string, filter func(Row) bool) (int64, error)
}

// RowIterator allows streaming rows without loading everything in memory
type RowIterator interface {
    Next() (Row, bool)
    Close() error
}

// executor exposes this — the pgwire/server layer depends on it
type Executor interface {
    Execute(stmt Statement) (*Result, error)
}
```

### Dependency Direction

```
main.go  (wires everything together)
  │
  ├─→ config      (no deps — pure config parsing)
  ├─→ server      (depends on: Executor interface)
  ├─→ pgwire      (depends on: shared types only)
  ├─→ parser      (depends on: shared AST types only)
  ├─→ executor    (depends on: Parser + Engine interfaces)
  └─→ storage     (depends on: shared types only)
```

- **No circular dependencies** — dependency flows downward
- **`main.go` is the composition root** — it creates concrete implementations and wires interfaces together
- **Shared types** (Value, Row, ColumnDef, Result) live in a small `types/` package or are defined by the interface-owning package

### Testing Strategy Per Module

| Package | How to test in isolation |
|---|---|
| `parser` | Feed SQL strings, assert AST output. No other deps. |
| `storage` | Create engine with temp dir, call interface methods, verify data. No parser or network. |
| `executor` | Mock `Engine` interface, feed AST nodes, assert results. No disk or network. |
| `pgwire` | Feed raw bytes (simulating a PG client), assert response bytes. No real TCP needed. |
| `server` | Integration tests with a mock `Executor`. |

## Architecture

```
psql / PG drivers
       │ TCP
       ▼
┌─────────────────────┐
│   Network Layer      │  Accept connections, goroutine per connection
│   (net.Listener)     │
├─────────────────────┤
│   PG Wire Protocol   │  Startup handshake, auth, SimpleQuery,
│                      │  RowDescription, DataRow, CommandComplete,
│                      │  ReadyForQuery, ErrorResponse
├─────────────────────┤
│   SQL Parser         │  Lexer → tokens → recursive descent → AST
│                      │  Supports double-quoted identifiers ("col")
├─────────────────────┤
│   Query Executor     │  Walk AST, read/write via storage engine
│                      │  Single-writer serialization here
├─────────────────────┤
│   Storage Engine     │
│   ├─ Catalog         │  Table definitions (schema metadata)
│   ├─ Heap (data log) │  Append-only row storage per table
│   ├─ WAL             │  Write-ahead log for crash recovery
│   └─ Index           │  In-memory index (rebuilt from heap on startup)
└─────────────────────┘
       │
    Data dir: ./data/
    ├── wal/          WAL segments
    ├── catalog.dat   Table schemas
    └── tables/
        └── <table>/  One data file per table
```

## Project Structure

```
mulldb/
├── main.go                 Entry point, CLI flags, daemon startup
├── go.mod
│
├── server/
│   ├── server.go           TCP listener, accept loop, graceful shutdown
│   └── connection.go       Per-connection goroutine, dispatches queries
│
├── pgwire/
│   ├── protocol.go         PG wire protocol message types, constants
│   ├── reader.go           Read PG messages from net.Conn
│   ├── writer.go           Write PG messages to net.Conn
│   └── auth.go             Startup handshake + cleartext password auth
│
├── parser/
│   ├── lexer.go            Tokenizer (SQL → tokens)
│   ├── token.go            Token types
│   ├── parser.go           Recursive descent parser (tokens → AST)
│   └── ast.go              AST node types (CreateTable, Insert, Select, etc.)
│
├── executor/
│   ├── executor.go         Query execution engine (AST → results)
│   └── result.go           Query result types (rows, columns, affected count)
│
├── storage/
│   ├── engine.go           Storage engine interface + implementation
│   ├── catalog.go          Table schema management (create/drop/lookup)
│   ├── heap.go             Append-only row storage (per-table data files)
│   ├── wal.go              Write-ahead log (write, replay on recovery)
│   ├── types.go            Data type definitions (INTEGER, TEXT, BOOLEAN)
│   └── row.go              Row encoding/decoding (serialize to/from bytes)
│
└── config/
    └── config.go           CLI flags + env var parsing
```

## Implementation Phases

### Phase 1: Skeleton + Wire Protocol
Get `psql` to connect and receive a response.
- `main.go`: CLI flags (`--port`, `--datadir`, `--user`, `--password`), start server
- `config/config.go`: Parse flags with env var fallbacks
- `server/server.go`: TCP listener on configured port
- `server/connection.go`: Accept, spawn goroutine per connection
- `pgwire/`: Implement startup message parsing, cleartext auth handshake, SimpleQuery reading, and static response (e.g. return empty result for any query)
- **Milestone**: `psql -h localhost -p 5433 -U admin` connects, authenticates, and gets `ReadyForQuery`

### Phase 2: Parser
Hand-written SQL parser for the minimal CRUD set.
- `parser/token.go`: Token types (keywords, identifiers, literals, operators)
- `parser/lexer.go`: Tokenize SQL strings (including double-quoted identifiers with `""` escape)
- `parser/ast.go`: AST nodes for `CREATE TABLE`, `DROP TABLE`, `INSERT`, `SELECT`, `UPDATE`, `DELETE`
- `parser/parser.go`: Recursive descent parser producing AST
- **Milestone**: Parse `CREATE TABLE foo (id INTEGER, name TEXT, active BOOLEAN)` into an AST

### Phase 3: Storage Engine
Persistent storage with WAL.
- `storage/types.go`: Type definitions and value representation
- `storage/row.go`: Binary row encoding/decoding
- `storage/catalog.go`: Store/load table schemas from `catalog.dat`
- `storage/heap.go`: Append-only data files per table, sequential scan
- `storage/wal.go`: WAL write (before mutation), WAL replay on startup
- `storage/engine.go`: Unified interface — `CreateTable`, `DropTable`, `Insert`, `Scan`, `Update`, `Delete`
- **Milestone**: Data survives daemon restart; WAL replays correctly after crash

### Phase 4: Query Executor
Wire the parser and storage together.
- `executor/result.go`: Result types (column metadata + row data)
- `executor/executor.go`: Walk AST, call storage engine, evaluate `WHERE` clauses, produce results
- Hook executor output into pgwire writer (RowDescription + DataRow messages)
- **Milestone**: Full round-trip — `CREATE TABLE`, `INSERT`, `SELECT` from `psql`

### Phase 5: Concurrency + Polish
- Implement single-writer/multi-reader with `sync.RWMutex` or a write channel
- Graceful shutdown (drain connections on SIGINT/SIGTERM)
- Error handling and PG ErrorResponse messages with proper SQLSTATE codes
- **Milestone**: Multiple `psql` sessions work concurrently; clean shutdown preserves data

## Wire Protocol Details (PG v3 — Simple Query)

**Startup flow:**
1. Client sends `StartupMessage` (version 3.0, params: `user`, `database`)
2. Server sends `AuthenticationCleartextPassword`
3. Client sends `PasswordMessage`
4. Server validates → sends `AuthenticationOk` + `ReadyForQuery`

**Query flow:**
1. Client sends `Query` message (SQL string)
2. Server parses + executes
3. For SELECT: `RowDescription` → N × `DataRow` → `CommandComplete` → `ReadyForQuery`
4. For INSERT/UPDATE/DELETE: `CommandComplete` (with row count) → `ReadyForQuery`
5. On error: `ErrorResponse` → `ReadyForQuery`

**Key message types:**
| Byte | Message | Direction |
|------|---------|-----------|
| — | StartupMessage | Client→Server |
| `R` | Authentication* | Server→Client |
| `p` | PasswordMessage | Client→Server |
| `Q` | Query | Client→Server |
| `T` | RowDescription | Server→Client |
| `D` | DataRow | Server→Client |
| `C` | CommandComplete | Server→Client |
| `E` | ErrorResponse | Server→Client |
| `Z` | ReadyForQuery | Server→Client |

## Verification
- **Phase 1**: `psql -h 127.0.0.1 -p 5433 -U admin` connects and authenticates
- **Phase 2**: Unit tests for lexer and parser covering all statement types
- **Phase 3**: Unit tests for storage — insert rows, restart engine, verify data present
- **Phase 4**: `psql` session: CREATE TABLE → INSERT rows → SELECT back → UPDATE → DELETE → SELECT
- **Phase 5**: Two concurrent `psql` sessions, one writing, one reading; kill -9 and restart, verify data intact
