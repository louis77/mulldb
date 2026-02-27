package executor

import (
	"errors"

	"mulldb/storage"
)

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
	OIDInt8    int32 = 20  // INT8 / BIGINT
	OIDText    int32 = 25  // TEXT
	OIDBool    int32 = 16  // BOOLEAN
	OIDUnknown int32 = 705 // UNKNOWN (used for NULL columns)
)

// -------------------------------------------------------------------------
// QueryError — wraps errors with a PostgreSQL SQLSTATE code
// -------------------------------------------------------------------------

// QueryError is an error annotated with a PostgreSQL SQLSTATE code.
// The server layer extracts the code to build a proper ErrorResponse.
type QueryError struct {
	Code    string // 5-character SQLSTATE code (e.g. "42P01")
	Message string
}

func (e *QueryError) Error() string {
	return e.Message
}

// sqlstateForError maps a storage-layer error to a PostgreSQL SQLSTATE code.
// See https://www.postgresql.org/docs/current/errcodes-appendix.html
func sqlstateForError(err error) string {
	var tableExists *storage.TableExistsError
	if errors.As(err, &tableExists) {
		return "42P07" // duplicate_table
	}

	var tableNotFound *storage.TableNotFoundError
	if errors.As(err, &tableNotFound) {
		return "42P01" // undefined_table
	}

	var colNotFound *storage.ColumnNotFoundError
	if errors.As(err, &colNotFound) {
		return "42703" // undefined_column
	}

	var valCount *storage.ValueCountError
	if errors.As(err, &valCount) {
		return "22023" // invalid_parameter_value
	}

	var uniqueViolation *storage.UniqueViolationError
	if errors.As(err, &uniqueViolation) {
		return "23505" // unique_violation
	}

	// Fallback: syntax error or general error.
	return "42000"
}

// WrapError wraps an error with the appropriate SQLSTATE code.
func WrapError(err error) error {
	if err == nil {
		return nil
	}
	// Already a QueryError — pass through.
	var qe *QueryError
	if errors.As(err, &qe) {
		return err
	}
	return &QueryError{
		Code:    sqlstateForError(err),
		Message: err.Error(),
	}
}
