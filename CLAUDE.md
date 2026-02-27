# mulldb

A lightweight SQL database written in Go that speaks the PostgreSQL wire protocol.

See [README.md](README.md) for features, SQL reference, configuration, architecture overview, and usage instructions.
See [PLAN.md](PLAN.md) for the full architecture, implementation phases, and design decisions.
See [DESIGN.md](DESIGN.md) for detailed architecture notes and WAL format specification.

## Project Goals

- Build a usable, correct SQL database for light workloads
- Full compatibility with `psql` and standard PostgreSQL drivers via the PG v3 wire protocol
- Persistent storage with write-ahead logging for crash recovery
- Simple username/password authentication
- Clean, understandable codebase — prioritize clarity over performance

## Architecture Principles

- **Modular via interfaces**: Every layer boundary is a Go interface. Packages depend on interfaces, never on concrete types from other layers.
- **No circular deps**: Dependency flows downward — `server` → `executor` → `parser` + `storage`. `main.go` is the composition root that wires implementations together.
- **Testable in isolation**: Each package can be unit tested with mocks/stubs — no need for a running server or real disk to test the parser, no need for a parser to test storage.

## Go Conventions

- Use `any` instead of `interface{}`
- Standard Go project layout with packages: `server/`, `pgwire/`, `parser/`, `executor/`, `storage/`, `storage/index/`, `config/`

## Building & Running

```bash
go build -o mulldb .
./mulldb --port 5433 --datadir ./data --user admin --password secret
go test ./...
```
