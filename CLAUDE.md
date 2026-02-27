# mulldb

A lightweight SQL database written in Go that speaks the PostgreSQL wire protocol.

## Project Goals

- Build a usable, correct SQL database for light workloads
- Full compatibility with `psql` and standard PostgreSQL drivers via the PG v3 wire protocol
- Persistent storage with write-ahead logging for crash recovery
- Simple username/password authentication
- Clean, understandable codebase — prioritize clarity over performance

## Design Reference

See [PLAN.md](PLAN.md) for the full architecture, implementation phases, and design decisions.

## Architecture Principles

- **Modular via interfaces**: Every layer boundary is a Go interface. Packages depend on interfaces, never on concrete types from other layers.
- **No circular deps**: Dependency flows downward — `server` → `executor` → `parser` + `storage`. `main.go` is the composition root that wires implementations together.
- **Testable in isolation**: Each package can be unit tested with mocks/stubs — no need for a running server or real disk to test the parser, no need for a parser to test storage.
- See [PLAN.md](PLAN.md) for the full interface contracts and dependency diagram.

## SQL Features

- CRUD: `CREATE TABLE`, `DROP TABLE`, `INSERT`, `SELECT` (with `WHERE`), `UPDATE`, `DELETE`
- Data types: `INTEGER`, `TEXT`, `BOOLEAN`
- Single-column `PRIMARY KEY` constraints with uniqueness enforcement and indexed lookups
- Aggregate functions: `COUNT(*)`, `COUNT(col)`, `SUM`, `MIN`, `MAX`
- `LIMIT` and `OFFSET` for result pagination
- Column aliases with `AS`
- Schema-qualified table names (`schema.table`)
- Double-quoted identifiers (`"select"`, `"public"."names"`) — allows reserved words as identifiers, preserves exact casing, supports `""` escape for literal double-quote

## Indexing

- Primary key columns are automatically backed by an in-memory B-tree index
- `SELECT ... WHERE pk_col = value` uses O(log n) index lookup instead of a full table scan
- The index system is modular: `storage/index/` defines an `Index` interface, with the B-tree as the first implementation — other index types can be added by implementing the same interface
- Indexes are rebuilt automatically from the WAL on startup
- WAL files use a versioned header; when the format changes, the engine requires the `--migrate` flag to perform the migration (original WAL is preserved as `.bak`)

## Go Conventions

- Use `any` instead of `interface{}`
- Standard Go project layout with packages: `server/`, `pgwire/`, `parser/`, `executor/`, `storage/`, `storage/index/`, `config/`

## Building & Running

```bash
go build -o mulldb .
./mulldb --port 5433 --datadir ./data --user admin --password secret
```

### WAL Migration

The WAL (write-ahead log) file uses a versioned binary format. When a new release changes the format, the engine will refuse to start:

```
open storage: open WAL: WAL file is format version 1 but version 2 is required; restart with --migrate flag
```

To migrate, restart with `--migrate`:

```bash
./mulldb --datadir ./data --migrate
```

The migration workflow:

1. The engine checks that enough disk space is available (roughly 2x the WAL file size)
2. A new WAL file is written in the current format
3. The original file is preserved as `data/wal.dat.bak` (or `.bak.1`, `.bak.2`, etc. if a backup already exists)
4. The engine starts normally

After verifying the database works correctly, you can manually delete the backup file. The engine will never delete it for you.

If `--migrate` is passed but no migration is needed, the engine logs an info message and starts normally.

## Testing

```bash
go test ./...
```

Connect with psql:
```bash
psql -h 127.0.0.1 -p 5433 -U admin
```
