package storage

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
