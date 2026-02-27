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
- Aggregate functions: `COUNT(*)`, `COUNT(col)`, `SUM`, `MIN`, `MAX`
- Column aliases with `AS`
- Schema-qualified table names (`schema.table`)
- Double-quoted identifiers (`"select"`, `"public"."names"`) — allows reserved words as identifiers, preserves exact casing, supports `""` escape for literal double-quote

## Go Conventions

- Use `any` instead of `interface{}`
- Standard Go project layout with packages: `server/`, `pgwire/`, `parser/`, `executor/`, `storage/`, `config/`

## Building & Running

```bash
go build -o mulldb .
./mulldb --port 5433 --datadir ./data --user admin --password secret
```

## Testing

```bash
go test ./...
```

Connect with psql:
```bash
psql -h 127.0.0.1 -p 5433 -U admin
```
