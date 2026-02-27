# mulldb Design

This document describes the architecture and design reasoning behind mulldb, a lightweight SQL database written in Go that speaks the PostgreSQL wire protocol.

## Design Philosophy

mulldb is built around three convictions:

1. **Clarity over performance.** Every layer is written to be read and understood, not to squeeze microseconds. Where a simpler design is 10x slower but fits in your head, we take the simpler design.

2. **Correctness through isolation.** Each package depends only on interfaces and shared types, never on the internals of another package. This makes bugs local: a storage bug can't corrupt the parser, a parser bug can't crash the wire protocol.

3. **PostgreSQL compatibility as a forcing function.** Instead of inventing a custom client protocol, we implement the real PostgreSQL v3 wire protocol. This constrains the design in useful ways — error codes must be real SQLSTATE codes, column metadata must include type OIDs, result tags must follow PG conventions. The payoff is that `psql`, `pgcli`, and every PostgreSQL driver work out of the box.

## Dependency Architecture

```
main.go  (composition root — creates concretes, wires interfaces)
  │
  ├─→ config      (no deps)
  ├─→ storage     (no deps on other mulldb packages)
  ├─→ parser      (no deps on other mulldb packages)
  ├─→ executor    (depends on parser AST types + storage.Engine interface)
  └─→ server      (depends on executor + pgwire + config)
        └─→ pgwire  (no business logic deps — pure protocol bytes)
```

Dependencies flow strictly downward. There are no circular imports and no package depends on a concrete type from another package's implementation. `main.go` is the only place that knows about all concrete types — it creates a `storage.Engine`, wraps it in an `executor.Executor`, and hands that to a `server.Server`.

This means every package can be tested in isolation: feed the parser a SQL string, give the executor a mock engine, give the server a mock executor. No running server needed to test the parser, no disk needed to test the executor.

## The Wire Protocol Layer

### Why PostgreSQL v3

The PostgreSQL wire protocol is well-documented, widely supported, and just complex enough to be interesting without being overwhelming. We implement only the **simple query flow** — the client sends a SQL string, the server parses and executes it in one shot, and sends back results. We skip the extended query protocol (prepared statements, parameter binding, pipelining) because the simple flow covers the entire `psql` experience and most driver usage patterns.

### Message Structure

Every PG message after startup follows the same envelope: a one-byte type tag, a four-byte big-endian length (inclusive of itself), and then the payload. This makes the protocol self-describing — the reader can always determine message boundaries without understanding message contents.

```
[type: 1 byte][length: 4 bytes][payload: length-4 bytes]
```

The startup message is the exception — it has no type byte, just length + version + null-terminated key-value parameters. We handle this with a separate `ReadStartup()` method.

### SSL Negotiation

When a client connects, it may first send an SSL request (magic number `80877103` where the version field would normally be). We refuse SSL and the client retries with a normal unencrypted startup. The reader sits in a loop to handle this — read a startup message, check if it's an SSL request, refuse and loop if so, otherwise proceed with authentication.

### Authentication

We use cleartext password authentication. The server sends `AuthenticationCleartextPassword`, the client responds with a `PasswordMessage`, and the server validates against the configured password. This is intentionally simple — the project targets localhost and trusted-network deployments where TLS and SCRAM-SHA-256 would add complexity without meaningful security gain. The password is configured via CLI flag or environment variable.

After authentication succeeds, the server sends a burst of messages that PostgreSQL clients expect: `AuthenticationOk`, several `ParameterStatus` messages (server version, encoding, date style), a `BackendKeyData` (process ID for cancel requests, which we accept but ignore), and finally `ReadyForQuery` to signal the session is live.

### Query Flow

The query loop reads messages in a `for` loop. A `Query` message (`'Q'`) triggers parsing and execution. The result determines what gets sent back:

- **SELECT**: `RowDescription` (column names, type OIDs, sizes) followed by one `DataRow` per row, then `CommandComplete` with a tag like `"SELECT 5"`.
- **INSERT/UPDATE/DELETE/DDL**: Just `CommandComplete` with the appropriate tag (`"INSERT 0 3"`, `"UPDATE 2"`, `"CREATE TABLE"`).
- **Error**: `ErrorResponse` with severity, SQLSTATE code, and human-readable message.

Every response sequence ends with `ReadyForQuery` to tell the client the server is idle and ready for the next query.

### Buffering and Flushing

The pgwire `Writer` builds each message in a reusable byte buffer, then writes the complete message to a `bufio.Writer`. This batches small writes into fewer syscalls. An explicit `Flush()` call pushes bytes to the socket — the server flushes after each complete response sequence (after `ReadyForQuery`), so the client sees an atomic response rather than a trickle of partial messages.

## The Parser

### Why Hand-Written

A hand-written recursive-descent parser is more code than a parser generator, but it's also more debuggable, produces better error messages, and requires no build-time tooling. Since our SQL grammar is small (six statement types, three data types, a handful of operators), the parser stays well under 500 lines.

### Lexer Design

The lexer is a character-by-character state machine. It consumes the input string one byte at a time, skipping whitespace, and emits tokens. Each token carries a type, a literal string, and a position (for error messages).

Keywords are case-insensitive: the lexer reads an identifier, looks it up in a keyword table (after uppercasing), and returns the keyword token type if it matches. Bare identifiers that aren't keywords get `TokenIdent`.

Double-quoted identifiers (`"select"`, `"My Column"`) get special treatment. The lexer reads everything between double quotes, handling `""` as an escape for a literal double-quote character. This allows reserved words as identifiers and preserves exact casing — unquoted identifiers are case-insensitive, but quoted ones are case-sensitive, matching PostgreSQL behavior.

### Expression Parsing and Precedence

Expressions are parsed with precedence climbing via function nesting. Each precedence level is a function that calls the next-tighter level:

```
parseExpr → parseOr → parseAnd → parseComparison → parsePrimary
```

`parsePrimary` handles atoms: integer literals, string literals, booleans, NULL, column references, parenthesized sub-expressions, and function calls. Function call detection is done by lookahead — after reading an identifier, if the next token is `(`, it's a function call.

This approach naturally handles left-associativity (`a AND b AND c` becomes `AND(AND(a, b), c)`) and is straightforward to extend with new precedence levels.

### AST Design

The AST uses two marker interfaces — `Statement` and `Expr` — with unexported marker methods. This prevents external packages from accidentally satisfying the interface. Each SQL statement is a concrete struct (`CreateTableStmt`, `SelectStmt`, etc.) and each expression is a concrete struct (`IntegerLit`, `BinaryExpr`, `ColumnRef`, etc.).

A few design specifics worth noting:

- **ORDER BY** is `[]OrderByClause` on `SelectStmt`, where each clause has a column name and a `Desc` bool. A nil slice means no ORDER BY. Parsed after WHERE and before LIMIT/OFFSET, matching SQL clause ordering.
- **LIMIT and OFFSET** are `*int64` pointers on `SelectStmt`. A nil pointer means the clause was omitted; a zero-valued pointer means the user explicitly wrote `LIMIT 0`. This distinction matters for correct semantics.
- **Table references** are a `TableRef` struct with optional `Schema` and required `Name` fields, supporting both `users` and `information_schema.tables`.
- **Aliases** are represented by wrapping any expression in an `AliasExpr`, keeping the alias orthogonal to the expression type.

## The Storage Engine

### Interface as Contract

The storage layer exposes an `Engine` interface that the executor depends on:

```go
type Engine interface {
    CreateTable(name string, columns []ColumnDef) error
    DropTable(name string) error
    GetTable(name string) (*TableDef, bool)
    ListTables() []*TableDef
    Insert(table string, columns []string, values [][]any) (int64, error)
    Scan(table string) (RowIterator, error)
    Update(table string, sets map[string]any, filter func(Row) bool) (int64, error)
    Delete(table string, filter func(Row) bool) (int64, error)
    LookupByPK(table string, value any) (*Row, error)
    Close() error
}
```

Two design choices stand out in this interface:

**Filter functions.** `Update` and `Delete` take a `func(Row) bool` predicate. This pushes WHERE evaluation into the executor, where it belongs, without requiring the storage layer to understand SQL expressions. The storage layer just iterates rows and asks "keep this one?" — clean separation.

**Typed errors.** The interface returns errors like `TableNotFoundError`, `UniqueViolationError`, and `ColumnNotFoundError` as concrete types. The executor uses `errors.As()` to map these to SQLSTATE codes. This avoids string-matching on error messages and keeps the storage layer unaware of PostgreSQL error conventions.

### In-Memory Heap

Each table is stored as a `tableHeap`: a `map[int64][]any` mapping internal row IDs to column value slices. Row IDs are sequential, never reused (even after deletion), and purely internal — they're not exposed to SQL.

Why a map instead of a slice? Deletions. A slice would leave holes or require shifting elements; a map gives O(1) insert, delete, and lookup by row ID. The values are stored as `[]any` (column-ordered) rather than as a struct or map because the executor knows column indices and array access is faster.

### Scan Snapshots

When the executor calls `Scan()`, the heap copies all its rows into a slice and returns a `sliceIterator`. This snapshot is safe to use after the lock is released — the iterator holds its own copy of the data, so concurrent writes don't corrupt reads.

The cost is O(n) memory per scan. For a database targeting light workloads, this is an acceptable trade-off for the simplicity it buys: no cursor invalidation, no lock holding during query processing, no complicated concurrency between iterators and writers.

### Write-Ahead Log

Every mutation follows the WAL-first rule: write to the log, fsync, then apply to the in-memory heap. On restart, the WAL is replayed from the beginning to reconstruct the full in-memory state.

**Per-table file layout.** The WAL is split into per-table files rather than a single monolithic log:

```
<dataDir>/
├── catalog.wal          # DDL only: CreateTable / DropTable entries
└── tables/
    ├── users.wal        # DML for "users" table
    └── orders.wal       # DML for "orders" table
```

`catalog.wal` contains only DDL entries (CreateTable, DropTable). Each surviving table gets its own WAL file under `tables/` containing only DML entries (Insert, Delete, Update). DML entries still include the table name as a safety cross-check during replay.

This split provides three benefits: DROP TABLE instantly reclaims disk space (delete the file), concurrent writes to different tables hit different files (no contention), and per-table replay is trivially parallelizable (though currently sequential).

**Table name encoding.** Table names are percent-encoded for filesystem safety: characters outside `[a-zA-Z0-9_-]` are encoded as `%XX` (e.g. `"my table"` → `my%20table.wal`). The encoding is reversible for orphan cleanup on startup.

**Two-phase replay.** On startup, `Open()` replays the catalog WAL first (learning all table schemas and which tables were dropped), then opens and replays each surviving table's WAL file to populate heaps and indexes. Two dedicated replay handlers enforce separation: `catalogReplayHandler` rejects DML entries, `dmlReplayHandler` rejects DDL entries and validates that each entry's table name matches the expected table.

**Orphan cleanup.** After catalog replay, the engine scans the `tables/` directory and deletes WAL files for tables that don't exist in the catalog. This handles the case where a crash occurred between writing the DROP TABLE entry to the catalog WAL and deleting the table's WAL file.

**Per-file binary format.** Each WAL file begins with a 6-byte header: a 4-byte magic number (`"MWAL"`) followed by a uint16 format version. The `WAL` struct is reused as-is for every file — one instance for the catalog, one per table. After the header, the file is a sequence of entries:

```
[header: "MWAL" + uint16 version]
[entry 1][entry 2][...][entry N]
```

**Entry format:**
```
[uint32 totalLen][byte op][payload bytes][uint32 crc32]
```

The length prefix allows reading entry boundaries without parsing. The CRC-32 checksum (IEEE polynomial over op + payload) catches disk corruption. The operation byte identifies the type: CreateTable, DropTable, Insert, Delete, or Update.

**Values are encoded** with a tag-length-value scheme: a one-byte type tag (null, integer, text, boolean), followed by the value in a fixed format. Integers are 8 bytes big-endian; text is a uint16 length prefix followed by UTF-8 bytes; booleans are a single byte. Big-endian encoding ensures portability across architectures.

**Fsync on every write.** After writing each WAL entry, we call `file.Sync()`. This is conservative — it forces the OS to flush to disk before the engine applies the change to memory. If the process crashes between the WAL write and the heap update, the next startup replays the WAL entry and reaches the same state. If the process crashes during the WAL write, the partial entry is detected by CRC failure or truncation, and replay stops at the last valid entry.

This fsync-per-write strategy is slow for high-throughput workloads (group commits would batch multiple operations into one fsync). But for light workloads, correctness is more valuable than throughput.

### WAL Migration

The WAL binary format evolves as features are added. When a new version of the binary opens an older WAL file, it needs to understand the old format and convert it. Rather than requiring users to wipe their data directory on upgrades, the engine supports explicit WAL migration via the `--migrate` CLI flag.

**Explicit opt-in.** Migration is never automatic. If the engine detects an old WAL version on startup, it exits with an error telling the user to restart with `--migrate`. This prevents accidental data format changes and gives the user control over when the conversion happens.

**Version detection.** `OpenWAL` reads the first 4 bytes of the file. If they match the `"MWAL"` magic, it reads the uint16 version that follows. If they don't match, the file is a legacy v1 file (written before versioned headers existed). This detection is safe because a legacy file starts with a uint32 entry length, which can't equal the ASCII bytes `"MWAL"` — that would encode as a 1.2 GB entry.

**Disk space check.** Before migrating, the engine estimates the required disk space (2x the original WAL file size, since both the backup and the new file must coexist) and checks available space via `Statfs`. The migration is refused if there isn't enough room.

**Migration chain.** Migrations are registered in a `map[uint16]entryMigrateFunc` keyed by source version. Each function transforms individual WAL entries from version N to version N+1. To migrate across multiple versions, the functions are applied sequentially: v1→v2, then v2→v3, and so on. Entry types that didn't change between versions pass through untouched.

**Safe file handling.** Migration reads all entries from the old file, transforms them, and writes a new file (`wal.dat.mig`) with the current-version header and migrated entries. After fsync, the original is renamed to `wal.dat.bak` (preserving it as a backup), and the new file is moved into place. If a `.bak` file already exists, a numbered suffix is used (`.bak.1`, `.bak.2`, etc.). The user is told they can manually delete the backup after verifying. If the process crashes mid-migration, the original file is still intact.

The first migration (v1→v2) handles the addition of the primary key flag byte to CreateTable column entries. Old columns get `PrimaryKey: false` since the concept didn't exist in v1.

**Split WAL migration.** When the engine detects a legacy single `wal.dat` file (and no `catalog.wal`), it requires a structural migration to the per-table layout. The migration reads all entries from `wal.dat`, classifies them as DDL or DML, tracks which tables survive after all CREATE/DROP sequences, and writes: `catalog.wal` (all DDL entries), plus `tables/<name>.wal` for each surviving table (only that table's DML entries). DML for dropped tables is discarded, immediately reclaiming space. The original `wal.dat` is preserved as `wal.dat.bak`. If the legacy file also needs a format version upgrade (e.g. v1→v2), that migration runs first, then the split migration follows.

### Primary Key Index

Tables with a primary key column get an in-memory B-tree index (`storage/index/btree.go`). The B-tree is order-64, meaning each node holds up to 63 entries. It supports three operations: `Put` (insert with duplicate detection), `Get` (lookup by key), and `Delete` (remove by key).

The B-tree's deletion implementation is deliberately simplified — it doesn't rebalance after deletion (no sibling borrowing or node merging). For an in-memory index that gets rebuilt from the WAL on every restart, this is acceptable. The small temporary imbalance has negligible impact on lookup performance.

The index is used for two things: **fast unique constraint checking** during Insert and Update (O(log n) instead of O(n) scan), and **primary key lookups** in the executor when a WHERE clause is a simple equality on the PK column.

### Pre-Validation Before WAL

Insert and Update operations validate all constraints (unique violations, null PK, batch duplicates) before writing to the WAL. This is a deliberate design choice: if validation fails, no WAL entry is written and no state changes. This gives atomic semantics — either all rows in a batch insert succeed, or none do — without needing a rollback mechanism.

For batch inserts, the engine first checks for duplicates within the batch itself (using a temporary `map[any]bool`), then checks each key against the existing index. Only after all rows pass validation does the WAL write proceed.

## The Executor

### Statement Dispatch

`executor.Execute()` parses the SQL string into an AST, then dispatches on the statement type with a type switch. Each statement type has a dedicated handler method (`execCreateTable`, `execSelect`, `execInsert`, etc.) that returns a `*Result`.

The `Result` struct is designed to map directly to the wire protocol's needs:

```go
type Result struct {
    Columns []Column    // nil for non-SELECT statements
    Rows    [][][]byte  // text-encoded values (nil entry = SQL NULL)
    Tag     string      // "SELECT 5", "INSERT 0 3", "CREATE TABLE"
}
```

All values are text-encoded because the PostgreSQL simple query protocol transmits data as text. Column metadata includes PostgreSQL type OIDs (20 for int8, 25 for text, 16 for boolean) so that clients can interpret the values correctly.

### WHERE Compilation

WHERE clauses are compiled into closures rather than interpreted on each row. `compileExpr()` walks the expression AST once and produces a `func(Row) any` that evaluates the expression against a row by accessing column values by index, performing comparisons, and combining boolean results.

For example, `WHERE id > 5 AND name = 'Alice'` compiles into a closure that:
1. Reads `row.Values[0]` (id column), compares to int64(5)
2. Reads `row.Values[1]` (name column), compares to `"Alice"`
3. ANDs the two boolean results

This is faster than re-walking the AST for every row, which matters when scanning large tables.

**NULL semantics.** Comparison operators (`=`, `!=`, `<`, `>`, `<=`, `>=`) return `nil` (SQL NULL) when either operand is NULL, following the SQL standard. The `buildFilter()` function already treats `nil` as row-rejection (`ok && b` where `ok` is false for non-bool values), so NULL-yielding comparisons correctly exclude rows without special handling. `IS NULL` and `IS NOT NULL` are compiled as simple nil-checks on the inner expression's result.

### Aggregate Functions

Queries with aggregate functions (COUNT, SUM, MIN, MAX) follow a separate code path from regular SELECT. The executor first detects whether a query is all-aggregate, all-non-aggregate, or mixed. Mixed queries (like `SELECT id, COUNT(*) FROM t`) are rejected with SQLSTATE code 42803, matching PostgreSQL behavior (no GROUP BY support yet).

For all-aggregate queries, the executor makes a single pass over the table, updating accumulator state for each function. COUNT increments a counter (skipping NULLs for `COUNT(col)`, not for `COUNT(*)`). SUM adds values. MIN and MAX track extrema. After the scan, a single result row is produced.

### Primary Key Optimization

Before falling back to a full table scan, the executor checks if the WHERE clause is a simple equality on the primary key column (`WHERE id = 42`). If so, it calls `engine.LookupByPK()` for an O(log n) B-tree lookup instead of an O(n) scan. This optimization handles the most common single-row access pattern.

The check is deliberately narrow: only exact equality on a single PK column, with a literal value, and no other conditions. Anything more complex falls through to the scan path. This keeps the optimizer trivial while covering the highest-value case.

### ORDER BY

When a SELECT includes ORDER BY, the executor switches from a streaming row-emission path to a buffered sort path. All matching rows (after WHERE filtering) are collected into a `[]storage.Row` slice, sorted with `sort.SliceStable()`, and then LIMIT/OFFSET is applied to the sorted result.

The sort comparator is built from the ORDER BY columns at plan time. For each sort key, the executor resolves the column index and direction (ASC/DESC). Multi-column sorting compares left-to-right — the first non-equal comparison wins. NULL values always sort last regardless of direction: in ASC order, NULLs come after all non-NULL values; in DESC order, NULLs still come last. This matches PostgreSQL's default `NULLS LAST` behavior.

The stable sort preserves insertion order for rows with equal sort keys, giving deterministic results without a tiebreaker column.

When ORDER BY is absent, the executor keeps the existing streaming path with early LIMIT termination — no rows are buffered, and the scan stops as soon as LIMIT is satisfied. This means adding ORDER BY support has zero performance impact on queries that don't use it.

ORDER BY with aggregate queries (COUNT, SUM, etc.) returns SQLSTATE `0A000` (feature not supported), since ORDER BY on a single aggregate result row is meaningless without GROUP BY.

### Catalog Tables

PostgreSQL clients expect to query system catalogs like `pg_catalog.pg_type` and `information_schema.tables`. The executor maintains a registry of virtual catalog tables that are populated on demand from the storage engine's metadata. These tables participate in normal SELECT execution — the same WHERE, LIMIT, OFFSET, and column projection logic applies.

Catalog tables are registered in `init()` functions using a simple registry pattern. Adding a new system table is just defining its schema and a function that generates its rows.

### Scalar Functions

Scalar functions like `VERSION()` follow a registry pattern. Each function registers itself in an `init()` function with `RegisterScalar(name, fn)`. The executor resolves function calls by looking up the registry, evaluates arguments, and delegates to the registered function. This keeps function implementations decoupled from the executor core.

## Concurrency Model

mulldb uses per-table locking to allow concurrent writes to independent tables. The locking scheme has two levels:

**Catalog lock (`catalogMu`).** A `sync.RWMutex` protects the table registry (the `catalog` and `tableStates` map). DDL operations (`CreateTable`, `DropTable`) take the write lock. DML operations take a brief read lock to look up the target table's `tableState`, then release the catalog lock before acquiring the table lock.

**Per-table lock (`tableState.mu`).** Each table has its own `sync.RWMutex` embedded in a `tableState` struct alongside its heap, WAL file handle, and a `dropped` flag. DML write operations (`Insert`, `Update`, `Delete`) take the table's write lock; reads (`Scan`, `LookupByPK`) take the table's read lock.

Lock ordering is always catalog before table (never reversed), which prevents deadlocks. The `acquireTableWrite` and `acquireTableRead` helpers encapsulate this pattern: brief catalog read lock → look up `tableState` → release catalog lock → acquire table lock → check `dropped` flag.

**DROP TABLE race guard.** A DML goroutine could grab a `tableState` pointer under the catalog read lock, release the catalog lock, and then find the table was dropped before it acquires the table lock. The `dropped` boolean flag in `tableState` catches this — DML checks it after acquiring the table lock and returns `TableNotFoundError` if set. DROP TABLE sets `dropped = true` while holding both the catalog write lock and the table write lock.

The scan-snapshot design (copying rows before releasing the lock) means readers hold the table lock only briefly — just long enough to copy the data — so writes aren't blocked for long even during large SELECTs.

What we don't have: transactions spanning multiple statements (each statement is atomic in isolation), or MVCC (readers see the latest committed state, not a consistent snapshot across statements).

## Error Handling

Errors cross three boundaries in mulldb, and each crossing is designed to be clean:

**Storage to executor:** Typed error values (`TableNotFoundError`, `UniqueViolationError`, etc.) that the executor matches with `errors.As()` and maps to SQLSTATE codes.

**Executor to server:** `QueryError` structs carrying a 5-character SQLSTATE code and a human-readable message. The server doesn't need to understand error types — it just extracts the code and message and formats them into a wire protocol `ErrorResponse`.

**Server to client:** PG `ErrorResponse` messages with field codes: `'S'` for severity, `'C'` for SQLSTATE, `'M'` for message. This is the format `psql` and PG drivers expect.

Parse errors get SQLSTATE `42601` (syntax error). Unknown statement types get `42000`. The full mapping follows PostgreSQL conventions so that client-side error handling works as expected.

## Connection Lifecycle

Each TCP connection gets its own goroutine. The lifecycle is: startup (SSL negotiation, authentication, parameter exchange), then a query loop until the client sends Terminate or the connection drops. Goroutines are tracked with a `sync.WaitGroup` for graceful shutdown.

On shutdown (SIGINT/SIGTERM), the server closes the listener (stopping new connections), signals the accept loop to exit, and waits for in-flight goroutines to finish with a 5-second timeout. This ensures clients get clean responses to in-flight queries rather than a TCP reset.

## What We Don't Have (and Why)

- **Transactions:** Each statement is atomic on its own. Multi-statement transactions would require undo logs, savepoints, and isolation levels — significant complexity for a project focused on simplicity.
- **Extended query protocol:** Prepared statements and parameter binding would double the wire protocol code. The simple query flow covers all interactive use cases.
- **Disk-based storage:** All data lives in memory (reconstructed from WAL on startup). A disk-based B-tree or LSM tree would be the natural next step for datasets larger than RAM.
- **Query optimizer:** There is no cost-based optimizer. The only optimization is the PK index lookup. Everything else is a sequential scan with filter. This is fine for small tables and keeps execution predictable.
- **GROUP BY / HAVING / JOIN:** These require more complex execution operators (hash join, sort-merge, grouping). The current aggregate path handles the simplest case (whole-table aggregation). ORDER BY is supported for non-aggregate queries.
- **MVCC:** Readers see the latest committed state. There is no multi-version concurrency control or snapshot isolation across statements.
