package executor

import (
	"fmt"
	"time"
)

// Trace captures timing and metadata for a single statement execution.
// Only populated when tracing is enabled (ExecuteTraced).
type Trace struct {
	Total        time.Duration
	Parse        time.Duration // lexer + parser
	Plan         time.Duration // column resolution, filter building, aggregate detection
	Exec         time.Duration // storage engine calls (scan, insert, update, delete)
	RowsScanned  int64
	RowsReturned int64
	UsedIndex    bool
	Table        string
	StmtType     string // "SELECT", "INSERT", etc.
}

// TraceToResult formats a Trace as a result set with columns "step" and "duration".
func TraceToResult(tr *Trace) *Result {
	if tr == nil {
		return &Result{
			Columns: []Column{
				{Name: "message", TypeOID: OIDText, TypeSize: -1},
			},
			Rows: [][][]byte{
				{[]byte("no trace available")},
			},
			Tag: "SELECT 1",
		}
	}

	cols := []Column{
		{Name: "step", TypeOID: OIDText, TypeSize: -1},
		{Name: "duration", TypeOID: OIDText, TypeSize: -1},
	}

	rows := [][][]byte{
		{[]byte("Parse"), []byte(tr.Parse.String())},
		{[]byte("Plan"), []byte(tr.Plan.String())},
		{[]byte("Execute"), []byte(tr.Exec.String())},
		{[]byte("Total"), []byte(tr.Total.String())},
		{[]byte("Statement"), []byte(tr.StmtType)},
	}

	if tr.Table != "" {
		rows = append(rows, [][]byte{[]byte("Table"), []byte(tr.Table)})
	}

	rows = append(rows, [][]byte{[]byte("Rows Scanned"), []byte(fmt.Sprintf("%d", tr.RowsScanned))})
	rows = append(rows, [][]byte{[]byte("Rows Returned"), []byte(fmt.Sprintf("%d", tr.RowsReturned))})

	if tr.UsedIndex {
		rows = append(rows, [][]byte{[]byte("Used Index"), []byte("true")})
	}

	return &Result{
		Columns: cols,
		Rows:    rows,
		Tag:     fmt.Sprintf("SELECT %d", len(rows)),
	}
}
