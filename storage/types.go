package storage

import "fmt"

// DataType identifies a column's data type.
type DataType uint8

const (
	TypeInteger DataType = iota
	TypeText
	TypeBoolean
	TypeTimestamp
	TypeFloat
)

func (d DataType) String() string {
	switch d {
	case TypeInteger:
		return "INTEGER"
	case TypeText:
		return "TEXT"
	case TypeBoolean:
		return "BOOLEAN"
	case TypeTimestamp:
		return "TIMESTAMP"
	case TypeFloat:
		return "FLOAT"
	default:
		return "UNKNOWN"
	}
}

// ColumnDef describes a column in a table.
type ColumnDef struct {
	Name       string
	DataType   DataType
	PrimaryKey bool
	Ordinal    int // permanent position index; never reused after DROP COLUMN
}

// IndexDef describes a secondary index on a table.
type IndexDef struct {
	Name   string // index name (unique within the table)
	Column string // indexed column name
	Unique bool   // true for UNIQUE indexes
}

// TableDef describes the schema of a table.
type TableDef struct {
	Name        string
	Columns     []ColumnDef
	NextOrdinal int // next ordinal to assign on ADD COLUMN
	Indexes     []IndexDef
}

// PrimaryKeyColumn returns the ordinal of the primary key column,
// or -1 if the table has no primary key.
func (d *TableDef) PrimaryKeyColumn() int {
	for _, col := range d.Columns {
		if col.PrimaryKey {
			return col.Ordinal
		}
	}
	return -1
}

// RowValue returns the value at the given ordinal from a row's values
// slice. If the row is shorter than the ordinal (e.g. row predates an
// ADD COLUMN), it returns nil (NULL).
func RowValue(values []any, ordinal int) any {
	if ordinal < len(values) {
		return values[ordinal]
	}
	return nil
}

// Row is a single row of data with an internal ID.
// Values are in column-definition order. Each value is one of:
//
//	int64      (INTEGER)
//	float64    (FLOAT)
//	string     (TEXT)
//	bool       (BOOLEAN)
//	time.Time  (TIMESTAMP)
//	nil        (NULL)
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

// UniqueViolationError is returned when an INSERT or UPDATE would
// violate a uniqueness constraint (primary key or unique index).
type UniqueViolationError struct {
	Table  string
	Column string
	Value  any
	Index  string // index name, if violation came from a secondary index
}

func (e *UniqueViolationError) Error() string {
	return fmt.Sprintf("duplicate key value violates unique constraint on column %q of table %q", e.Column, e.Table)
}

// ColumnExistsError is returned when adding a column that already exists.
type ColumnExistsError struct {
	Column string
	Table  string
}

func (e *ColumnExistsError) Error() string {
	return fmt.Sprintf("column %q of relation %q already exists", e.Column, e.Table)
}

// IndexExistsError is returned when creating an index that already exists.
type IndexExistsError struct {
	Name  string
	Table string
}

func (e *IndexExistsError) Error() string {
	return fmt.Sprintf("index %q already exists on table %q", e.Name, e.Table)
}

// IndexNotFoundError is returned when referencing an index that does not exist.
type IndexNotFoundError struct {
	Name  string
	Table string
}

func (e *IndexNotFoundError) Error() string {
	return fmt.Sprintf("index %q does not exist on table %q", e.Name, e.Table)
}

// Engine is the storage layer interface. The executor depends on this
// contract, never on the concrete implementation.
type Engine interface {
	CreateTable(name string, columns []ColumnDef) error
	DropTable(name string) error
	AddColumn(table string, col ColumnDef) error
	DropColumn(table string, colName string) error
	GetTable(name string) (*TableDef, bool)
	ListTables() []*TableDef
	Insert(table string, columns []string, values [][]any) (int64, error)
	Scan(table string) (RowIterator, error)
	Update(table string, sets map[string]any, filter func(Row) bool) (int64, error)
	Delete(table string, filter func(Row) bool) (int64, error)
	LookupByPK(table string, value any) (*Row, error)
	CreateIndex(table string, idx IndexDef) error
	DropIndex(table string, indexName string) error
	LookupByIndex(table string, indexName string, value any) ([]Row, error)
	RowCount(table string) (int64, error)
	Close() error
}
