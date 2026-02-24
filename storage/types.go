package storage

import "fmt"

// DataType identifies a column's data type.
type DataType uint8

const (
	TypeInteger DataType = iota
	TypeText
	TypeBoolean
)

func (d DataType) String() string {
	switch d {
	case TypeInteger:
		return "INTEGER"
	case TypeText:
		return "TEXT"
	case TypeBoolean:
		return "BOOLEAN"
	default:
		return "UNKNOWN"
	}
}

// ColumnDef describes a column in a table.
type ColumnDef struct {
	Name     string
	DataType DataType
}

// TableDef describes the schema of a table.
type TableDef struct {
	Name    string
	Columns []ColumnDef
}

// Row is a single row of data with an internal ID.
// Values are in column-definition order. Each value is one of:
//
//	int64   (INTEGER)
//	string  (TEXT)
//	bool    (BOOLEAN)
//	nil     (NULL)
type Row struct {
	ID     int64
	Values []any
}

// RowIterator streams rows from a scan.
type RowIterator interface {
	Next() (Row, bool)
	Close() error
}

// -------------------------------------------------------------------------
// Typed errors — used by the executor to map to SQLSTATE codes
// -------------------------------------------------------------------------

// TableExistsError is returned when creating a table that already exists.
type TableExistsError struct{ Name string }

func (e *TableExistsError) Error() string {
	return fmt.Sprintf("table %q already exists", e.Name)
}

// TableNotFoundError is returned when referencing a table that does not exist.
type TableNotFoundError struct{ Name string }

func (e *TableNotFoundError) Error() string {
	return fmt.Sprintf("table %q does not exist", e.Name)
}

// ColumnNotFoundError is returned when referencing a column that does not exist.
type ColumnNotFoundError struct{ Column, Table string }

func (e *ColumnNotFoundError) Error() string {
	return fmt.Sprintf("column %q not found in table %q", e.Column, e.Table)
}

// ValueCountError is returned when the number of values doesn't match columns.
type ValueCountError struct{ Expected, Got int }

func (e *ValueCountError) Error() string {
	return fmt.Sprintf("expected %d values, got %d", e.Expected, e.Got)
}

// Engine is the storage layer interface. The executor depends on this
// contract, never on the concrete implementation.
type Engine interface {
	CreateTable(name string, columns []ColumnDef) error
	DropTable(name string) error
	GetTable(name string) (*TableDef, bool)
	Insert(table string, columns []string, values [][]any) (int64, error)
	Scan(table string) (RowIterator, error)
	Update(table string, sets map[string]any, filter func(Row) bool) (int64, error)
	Delete(table string, filter func(Row) bool) (int64, error)
	Close() error
}
