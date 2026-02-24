package executor

// Column describes a column in a query result.
type Column struct {
	Name     string
	TypeOID  int32 // PostgreSQL type OID for wire protocol
	TypeSize int16 // type size in bytes (-1 for variable length)
}

// Result is the outcome of executing a single SQL statement.
type Result struct {
	// Columns is set for SELECT results. nil for non-SELECT.
	Columns []Column

	// Rows holds the result data for SELECT. Each row is a slice of
	// text-encoded values (nil entry means NULL). Outer slice = rows,
	// inner slice = columns.
	Rows [][][]byte

	// Tag is the CommandComplete tag, e.g. "SELECT 2", "INSERT 0 1".
	Tag string
}

// PostgreSQL type OIDs for the three supported types.
const (
	OIDInt8    int32 = 20   // INT8 / BIGINT
	OIDText    int32 = 25   // TEXT
	OIDBool    int32 = 16   // BOOLEAN
	OIDUnknown int32 = 705  // UNKNOWN (used for NULL columns)
)
