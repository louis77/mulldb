# walviewer

A human-friendly WAL (Write-Ahead Log) file viewer for mulldb.

## Overview

`walviewer` parses and displays mulldb WAL files in a readable format. It supports all WAL format versions (v1-v4) and all operation types including CREATE-TABLE, INSERT, UPDATE, DELETE, and more.

## Usage

```bash
walviewer [flags] <wal-file>
```

## Flags

| Flag | Description | Default |
|------|-------------|---------|
| `--page-size N` | Number of entries per page in interactive mode | 20 |
| `--op TYPE` | Filter by operation type (see below) | - |
| `--no-page` | Disable interactive paging, print all entries | false |
| `--raw` | Show raw hex dump of each entry | false |
| `--help`, `-h` | Show help message | - |

## Operation Types for `--op` Filter

- `create-table`
- `drop-table`
- `insert`
- `insert-batch`
- `delete`
- `update`
- `add-column`
- `drop-column`
- `create-index`
- `drop-index`

## Interactive Commands

When using the interactive pager (default mode):

| Command | Description |
|---------|-------------|
| `n` or `<space>` | Next page |
| `p` or `b` | Previous page |
| `g N` | Go to entry number N |
| `q` or `Ctrl+C` | Quit |

## Examples

### View a WAL file with interactive paging

```bash
./walviewer data/catalog.wal
```

### Print all entries without paging

```bash
./walviewer --no-page data/catalog.wal
```

### Filter for CREATE operations only

```bash
./walviewer --no-page --op create data/catalog.wal
```

### Filter for INSERT operations

```bash
./walviewer --no-page --op insert data/tables/users.wal
```

### Show raw hex dump of entries

```bash
./walviewer --no-page --raw data/tables/names.wal
```

### Custom page size

```bash
./walviewer --page-size 50 data/catalog.wal
```

## Sample Output

```
======================================================================
WAL File: data/catalog.wal
Version:  4
Entries:  29
======================================================================

[1] CREATE-TABLE | table=names, columns=[id INTEGER, name TEXT] | CRC:OK

[2] DROP-TABLE | table=names | CRC:OK

[3] CREATE-TABLE | table=names, columns=[id INTEGER [PK, NOT NULL], name TEXT] | CRC:OK

[4] INSERT | table=names, rowID=1, values=[1, "Louis"] | CRC:OK
```

## WAL Format Support

The viewer automatically detects and handles:

- **v1**: Legacy format without header
- **v2**: Magic + version header with PK flag support
- **v3**: Added ordinal + ALTER TABLE support
- **v4**: Added NOT NULL flag support

## Error Handling

- Invalid or truncated entries are reported with offset information
- CRC checksums are validated and displayed (OK/FAIL)
- Unknown operation codes are shown as `UNKNOWN(op_code)`

## Building

```bash
go build -o walviewer ./cmd/walviewer/
```

Or from the project root:

```bash
go build ./cmd/walviewer/
```
