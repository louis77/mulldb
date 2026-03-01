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
| SQL scope | Minimal CRUD: `CREATE TABLE`, `DROP TABLE`, `ALTER TABLE` (`ADD COLUMN`, `DROP COLUMN`), `INSERT`, `SELECT` (with `WHERE`, `ORDER BY`, `LIMIT`, `OFFSET`, `INNER JOIN`), `UPDATE`, `DELETE`. `CREATE [UNIQUE] INDEX`, `DROP INDEX`. Arithmetic expressions (`+`, `-`, `*`, `/`, `%`, unary minus). Pattern matching (`LIKE`, `NOT LIKE`, `ILIKE`, `NOT ILIKE`, `ESCAPE`). IN predicate (`IN`, `NOT IN`). Double-quoted identifiers for reserved words and case preservation. |
| Data types | `INTEGER`, `FLOAT` (64-bit IEEE 754), `TEXT`, `BOOLEAN`, `TIMESTAMP` (UTC-only) |
| Storage engine | Append-only data log + in-memory index (rebuilt on startup) |
| Durability | Write-ahead log (WAL) — every mutation logged before applied |
| Concurrency | Per-table locking: concurrent writes to independent tables, multi-reader per table |
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
// Concrete types: CreateTableStmt, DropTableStmt, AlterTableAddColumnStmt, AlterTableDropColumnStmt, InsertStmt, SelectStmt, UpdateStmt, DeleteStmt
type Statement interface {
    statementNode()
}

// storage exposes this — the executor depends on it, not on storage internals
type Engine interface {
    CreateTable(name string, columns []ColumnDef) error
    DropTable(name string) error
    AddColumn(table string, col ColumnDef) error
    DropColumn(table string, colName string) error
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
│   ├─ Heap (data log) │  In-memory row storage per table
│   ├─ WAL             │  Per-table write-ahead logs for crash recovery
│   └─ Index           │  In-memory B-tree index (rebuilt from WAL on startup)
└─────────────────────┘
       │
    Data dir: ./data/
    ├── catalog.wal      DDL log (CREATE/DROP TABLE)
    └── tables/
        └── <name>.wal   Per-table DML log
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

## Design Philosophy

### Near-Zero Configuration

Traditional SQL databases (PostgreSQL, MySQL/MariaDB) carry decades of historical
baggage: hundreds of configuration knobs, complex authentication systems, manual
vacuuming, replication setup, encoding negotiation, and operational rituals that
overwhelm most use cases. mulldb rejects this complexity.

**Principles:**
- A single binary with sensible defaults — start it and it works
- CLI flags with env var fallbacks for the few things that vary (port, data dir, credentials)
- No `postgresql.conf` equivalent — no tuning knobs for buffer pools, WAL segments, checkpoint intervals
- UTF-8 only — no character set negotiation or encoding configuration
- Per-table WAL files and locking — concurrency works correctly without user intervention
- Authentication is a single username/password pair, not a rule-based `pg_hba.conf`

The target user has a workload that fits comfortably in memory and wants a SQL
database that speaks PG wire protocol without the operational overhead.

### Native Nested Data

Every SQL database today flattens JOINs into a Cartesian product. An order with 3
items produces 3 rows with the order columns duplicated. Applications must
de-duplicate and reconstruct the object graph — either manually or through an ORM.

The common workaround is JSON aggregation (`json_agg`, `json_build_object` in
PostgreSQL), but this serializes structured data into a text format only to have the
client parse it back into objects. This is unnecessary overhead at both ends.

mulldb will support returning nested data natively from JOINs.

**Approach — `NEST()` with PG composite/array types:**

```sql
SELECT o.id, o.date, NEST(i.id, i.name, i.qty) AS items
FROM orders o
JOIN order_items i ON o.id = i.order_id
```

`NEST(...)` returns an array of composite rows — transmitted over the wire as PG
binary array-of-record, not JSON. This stays within the PostgreSQL wire protocol
specification:

- **Wire-legal**: PostgreSQL already defines array types and composite (record) types
  with their own OIDs and binary encodings. No protocol extensions needed.
- **Driver-compatible**: Major drivers (`pgx` for Go, `asyncpg` for Python,
  `node-postgres`) already decode composite arrays.
- **`psql`-friendly**: Text representation renders naturally in the terminal.
- **No pre-defined types**: mulldb synthesizes the composite type from the query's
  column list at execution time — no `CREATE TYPE` ceremony.

**Semantics:**

When `NEST()` is used with a JOIN, the executor:
1. Implicitly groups rows by all non-nested columns — no explicit `GROUP BY` required
2. Collects the nested columns into an array of composites per group
3. Returns one row per group with the nested array as a column

This eliminates the Cartesian duplication at the source rather than pushing
reconstruction to the client.

**Prerequisites**: ~~JOIN support (parser + executor)~~ (**Done** — INNER JOIN with nested-loop execution, table aliases, qualified column references), composite/array type encoding in pgwire.

---

## Current State & MVP Gap Analysis

### ✅ What Has Been Implemented (Verified)

All features described in the README have been **verified as implemented**:

| Category | Features |
|----------|----------|
| **Wire Protocol** | PG v3 startup handshake, cleartext auth, SimpleQuery, all message types (RowDescription, DataRow, CommandComplete, ErrorResponse, ReadyForQuery) |
| **SQL Parser** | CREATE/DROP TABLE, ALTER TABLE (ADD/DROP COLUMN), INSERT, SELECT, UPDATE, DELETE, BEGIN/COMMIT/ROLLBACK |
| **SELECT Features** | WHERE, ORDER BY (multi-column, NULLs last), LIMIT/OFFSET, INNER JOIN (multi-table, aliases, qualified columns), column aliases (AS) |
| **Expressions** | Arithmetic (`+`, `-`, `*`, `/`, `%`, unary `-`), string concatenation (`||`), comparisons, logical operators (AND/OR/NOT), IS NULL/IS NOT NULL, IN/NOT IN |
| **Pattern Matching** | LIKE/NOT LIKE, ILIKE/NOT ILIKE (case-insensitive), ESCAPE clause, Unicode-aware `_` and `%` |
| **IN Predicate** | IN/NOT IN with value lists, SQL-standard three-valued NULL logic |
| **Data Types** | INTEGER (64-bit), FLOAT (64-bit IEEE 754, aliases: DOUBLE PRECISION), TEXT, BOOLEAN, TIMESTAMP (UTC-only), NULL |
| **Constraints** | PRIMARY KEY (single-column only) with B-tree index enforcement; NOT NULL column constraints with INSERT/UPDATE validation |
| **Functions** | COUNT(*)/COUNT(col), SUM, MIN, MAX, LENGTH/CHAR_LENGTH/CHARACTER_LENGTH, OCTET_LENGTH, CONCAT, NOW, VERSION, ABS, ROUND, CEIL/CEILING, FLOOR, POWER/POW, SQRT, MOD |
| **Identifiers** | Double-quoted identifiers (preserve case, reserved words), UTF-8 throughout |
| **Comments** | Single-line (`--`) and nested block (`/* */`) |
| **Catalog Tables** | pg_type, pg_database, pg_namespace, information_schema.tables, information_schema.columns, information_schema.table_constraints, information_schema.key_column_usage |
| **Storage** | Split WAL (catalog.wal + per-table WALs), CRC32 checksums, fsync, WAL replay, WAL migration (v1→v2→v3→v4, single→split), batched WAL writes (single entry + single fsync for multi-row INSERT/UPDATE/DELETE) |
| **Concurrency** | Per-table locking (RW mutex), concurrent writes to independent tables, multiple readers |
| **Observability** | Statement tracing (SET trace = on/off, SHOW TRACE), SQLSTATE error codes |

### 🎯 Missing Features for MVP

The following features are **required** to move from "correct prototype" to "minimum viable product":

#### Tier 1: Absolute Minimum (Deal-breakers for Production)

| Priority | Feature | Gap Analysis | Implementation Notes |
|----------|---------|--------------|---------------------|
| P0 | **ACID Transactions** | `BEGIN/COMMIT/ROLLBACK` are no-ops; every statement auto-commits. Concurrent writes to the same table can leave partial state on crash. | Need transaction manager with undo log, atomic commit protocol. Current per-table locking is insufficient for atomic multi-table operations. |
| ~~P0~~ | ~~**Secondary Indexes**~~ | ✅ Done. `CREATE [UNIQUE] INDEX [name] ON table(column)`, `DROP INDEX name ON table`. Table-scoped names, auto-generated names, NULL handling. Explicit `INDEXED BY <name>` syntax for query acceleration (no automatic index selection). | Implemented with `MultiIndex` interface for non-unique indexes, WAL ops 8/9, rebuild on replay. |
| ~~P0~~ | ~~**UNIQUE Constraints**~~ | ✅ Done (via `CREATE UNIQUE INDEX`). Business keys enforce uniqueness through secondary indexes. Multiple NULLs allowed per SQL standard. | Uses same B-tree infrastructure as PK indexes. Full rollback on violation. |
| P0 | **Foreign Key Constraints** | No referential integrity checking. JOIN tables can have orphaned references. | Need FK metadata in catalog, validation on INSERT/UPDATE (parent exists), cascading actions, deferred checks. |
| P0 | **CHECK Constraints** | No data validation beyond type checking. Invalid data (e.g., negative prices) can be inserted. | Parser has expression framework; need constraint metadata, evaluation on write. |

#### Tier 2: Important (Major Limitations Without These)

| Priority | Feature | Gap Analysis | Implementation Notes |
|----------|---------|--------------|---------------------|
| P1 | **Subqueries** (`IN (SELECT ...)`, `EXISTS`, correlated) | `IN` with value lists is implemented; subquery form (`IN (SELECT ...)`) is not. Cannot express "find orders where total > avg" or "users in CA". Parser rejects subqueries entirely. | Requires AST nodes for subqueries, executor support for correlated evaluation (row-by-row subquery execution) or unnesting. |
| P1 | **GROUP BY + HAVING** | Aggregates only work on full table. Cannot do "sales per category" or "categories with >5 items". | Need grouping operator in executor, hash-based or sort-based aggregation, HAVING filter post-aggregation. |
| P1 | **LEFT OUTER JOIN** | Only INNER JOIN implemented. Missing rows from left table are silently dropped. | Extend parser for LEFT/RIGHT/FULL keywords, executor needs to preserve outer side rows with NULL padding. |
| P1 | **Prepared Statements** | Only SimpleQuery protocol. No parameter binding (`$1`, `$2`). SQL injection risk, re-parsing overhead. | Need Extended Query protocol (Parse, Bind, Execute, Close), portal/cursor management, param type inference. |
| P1 | **Savepoints** | Without transactions, partial rollback is impossible. Complex operations are all-or-nothing at statement level. | Depends on Tier 1 transactions. Need nested transaction state, partial rollback to savepoint. |

#### Tier 3: Solid (Production-Grade)

| Priority | Feature | Gap Analysis | Implementation Notes |
|----------|---------|--------------|---------------------|
| ~~P2~~ | ~~**CREATE/DROP INDEX**~~ | ✅ Done. See Secondary Indexes in Tier 1. | Implemented in Phase 7. |
| P2 | **Advanced ALTER TABLE** | Only ADD/DROP COLUMN. Cannot rename columns, change types, add constraints without table rebuild. | Ordinals currently immutable; need column rename metadata-only ops, type coercion for ALTER COLUMN. |
| P2 | **Views** | No way to encapsulate complex queries. No security through abstraction. | View metadata in catalog, view expansion in executor (replace view ref with subquery). |
| P2 | **Basic Query Optimizer** | No statistics; nested-loop joins only; no index-vs-scan decision. Query performance unpredictable. | Need table statistics (row counts, distinct values), cost model, join ordering heuristics. |
| P2 | **Row-Level Locking / MVCC** | Current table-level RWMutex blocks all writers and prevents reader-writer concurrency on same table. | Replace table mutex with row-level locks or MVCC (multi-version concurrency control) with snapshot isolation. |

### 📋 Recommended Implementation Roadmap

#### Phase 6: Transactions & Constraints (MVP Core)
1. Transaction manager with BEGIN/COMMIT/ROLLBACK
2. Undo log for rollback
3. UNIQUE and CHECK constraints
4. Foreign key constraints

#### Phase 7: Indexes & Performance
1. ~~Secondary index infrastructure (B-tree reuse)~~ ✅
2. ~~`CREATE INDEX` / `DROP INDEX`~~ ✅
3. ~~Explicit `INDEXED BY <name>` syntax (no automatic index selection)~~ ✅
4. Row-level locking (replace table-level mutex)

#### Phase 8: Advanced SQL
1. Subqueries (uncorrelated first, then correlated)
2. GROUP BY + HAVING
3. LEFT/RIGHT/FULL OUTER JOIN
4. Views

#### Phase 9: Protocol & Polish
1. Extended Query protocol (prepared statements)
2. Savepoints
3. Advanced ALTER TABLE operations
4. Query statistics and EXPLAIN
