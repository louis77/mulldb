package executor

import (
	"fmt"
	"strconv"
	"strings"

	"mulldb/parser"
	"mulldb/storage"
)

// Executor takes a parsed SQL statement and executes it against the
// storage engine, returning a Result suitable for the wire protocol.
type Executor struct {
	engine storage.Engine
}

// New creates an Executor backed by the given storage engine.
func New(engine storage.Engine) *Executor {
	return &Executor{engine: engine}
}

// Execute runs a single SQL statement.
func (e *Executor) Execute(sql string) (*Result, error) {
	stmt, err := parser.Parse(sql)
	if err != nil {
		return nil, err
	}

	switch s := stmt.(type) {
	case *parser.CreateTableStmt:
		return e.execCreateTable(s)
	case *parser.DropTableStmt:
		return e.execDropTable(s)
	case *parser.InsertStmt:
		return e.execInsert(s)
	case *parser.SelectStmt:
		return e.execSelect(s)
	case *parser.UpdateStmt:
		return e.execUpdate(s)
	case *parser.DeleteStmt:
		return e.execDelete(s)
	default:
		return nil, fmt.Errorf("unsupported statement type %T", stmt)
	}
}

// -------------------------------------------------------------------------
// Statement executors
// -------------------------------------------------------------------------

func (e *Executor) execCreateTable(s *parser.CreateTableStmt) (*Result, error) {
	cols := make([]storage.ColumnDef, len(s.Columns))
	for i, c := range s.Columns {
		dt, err := parseDataType(c.DataType)
		if err != nil {
			return nil, err
		}
		cols[i] = storage.ColumnDef{Name: c.Name, DataType: dt}
	}
	if err := e.engine.CreateTable(s.Name, cols); err != nil {
		return nil, err
	}
	return &Result{Tag: "CREATE TABLE"}, nil
}

func (e *Executor) execDropTable(s *parser.DropTableStmt) (*Result, error) {
	if err := e.engine.DropTable(s.Name); err != nil {
		return nil, err
	}
	return &Result{Tag: "DROP TABLE"}, nil
}

func (e *Executor) execInsert(s *parser.InsertStmt) (*Result, error) {
	def, ok := e.engine.GetTable(s.Table)
	if !ok {
		return nil, fmt.Errorf("table %q does not exist", s.Table)
	}

	rows := make([][]any, len(s.Values))
	for i, exprRow := range s.Values {
		vals := make([]any, len(exprRow))
		for j, expr := range exprRow {
			v, err := evalLiteral(expr)
			if err != nil {
				return nil, fmt.Errorf("row %d, value %d: %w", i, j, err)
			}
			vals[j] = v
		}
		rows[i] = vals
	}

	n, err := e.engine.Insert(s.Table, s.Columns, rows)
	if err != nil {
		return nil, err
	}

	_ = def // used above for context; insert delegates column resolution to engine
	return &Result{Tag: fmt.Sprintf("INSERT 0 %d", n)}, nil
}

func (e *Executor) execSelect(s *parser.SelectStmt) (*Result, error) {
	def, ok := e.engine.GetTable(s.From)
	if !ok {
		return nil, fmt.Errorf("table %q does not exist", s.From)
	}

	// Resolve which columns to return.
	colIndices, resultCols, err := resolveSelectColumns(s.Columns, def)
	if err != nil {
		return nil, err
	}

	// Build the WHERE filter.
	var filter func(storage.Row) bool
	if s.Where != nil {
		filter, err = buildFilter(s.Where, def)
		if err != nil {
			return nil, err
		}
	}

	// Scan and filter rows.
	it, err := e.engine.Scan(s.From)
	if err != nil {
		return nil, err
	}
	defer it.Close()

	var resultRows [][][]byte
	for {
		row, ok := it.Next()
		if !ok {
			break
		}
		if filter != nil && !filter(row) {
			continue
		}
		textRow := make([][]byte, len(colIndices))
		for i, idx := range colIndices {
			textRow[i] = formatValue(row.Values[idx])
		}
		resultRows = append(resultRows, textRow)
	}

	return &Result{
		Columns: resultCols,
		Rows:    resultRows,
		Tag:     fmt.Sprintf("SELECT %d", len(resultRows)),
	}, nil
}

func (e *Executor) execUpdate(s *parser.UpdateStmt) (*Result, error) {
	def, ok := e.engine.GetTable(s.Table)
	if !ok {
		return nil, fmt.Errorf("table %q does not exist", s.Table)
	}

	// Evaluate SET values.
	sets := make(map[string]any, len(s.Sets))
	for _, sc := range s.Sets {
		v, err := evalLiteral(sc.Value)
		if err != nil {
			return nil, fmt.Errorf("SET %s: %w", sc.Column, err)
		}
		sets[sc.Column] = v
	}

	// Build WHERE filter.
	var filter func(storage.Row) bool
	var err error
	if s.Where != nil {
		filter, err = buildFilter(s.Where, def)
		if err != nil {
			return nil, err
		}
	}

	n, err := e.engine.Update(s.Table, sets, filter)
	if err != nil {
		return nil, err
	}
	return &Result{Tag: fmt.Sprintf("UPDATE %d", n)}, nil
}

func (e *Executor) execDelete(s *parser.DeleteStmt) (*Result, error) {
	def, ok := e.engine.GetTable(s.Table)
	if !ok {
		return nil, fmt.Errorf("table %q does not exist", s.Table)
	}

	var filter func(storage.Row) bool
	var err error
	if s.Where != nil {
		filter, err = buildFilter(s.Where, def)
		if err != nil {
			return nil, err
		}
	}

	n, err := e.engine.Delete(s.Table, filter)
	if err != nil {
		return nil, err
	}
	return &Result{Tag: fmt.Sprintf("DELETE %d", n)}, nil
}

// -------------------------------------------------------------------------
// Column resolution
// -------------------------------------------------------------------------

func resolveSelectColumns(exprs []parser.Expr, def *storage.TableDef) ([]int, []Column, error) {
	var indices []int
	var cols []Column

	for _, expr := range exprs {
		switch e := expr.(type) {
		case *parser.StarExpr:
			for i, c := range def.Columns {
				indices = append(indices, i)
				cols = append(cols, Column{
					Name:     c.Name,
					TypeOID:  typeOID(c.DataType),
					TypeSize: typeSize(c.DataType),
				})
			}
		case *parser.ColumnRef:
			idx := columnIndex(def, e.Name)
			if idx < 0 {
				return nil, nil, fmt.Errorf("column %q not found in table %q", e.Name, def.Name)
			}
			c := def.Columns[idx]
			indices = append(indices, idx)
			cols = append(cols, Column{
				Name:     c.Name,
				TypeOID:  typeOID(c.DataType),
				TypeSize: typeSize(c.DataType),
			})
		default:
			return nil, nil, fmt.Errorf("unsupported select expression %T", expr)
		}
	}
	return indices, cols, nil
}

// -------------------------------------------------------------------------
// WHERE filter builder
// -------------------------------------------------------------------------

// buildFilter compiles a parser.Expr into a row filter function.
func buildFilter(expr parser.Expr, def *storage.TableDef) (func(storage.Row) bool, error) {
	eval, err := compileExpr(expr, def)
	if err != nil {
		return nil, err
	}
	return func(r storage.Row) bool {
		v := eval(r)
		b, ok := v.(bool)
		return ok && b
	}, nil
}

// exprFunc evaluates an expression against a row, returning a Go value.
type exprFunc func(storage.Row) any

func compileExpr(expr parser.Expr, def *storage.TableDef) (exprFunc, error) {
	switch e := expr.(type) {
	case *parser.ColumnRef:
		idx := columnIndex(def, e.Name)
		if idx < 0 {
			return nil, fmt.Errorf("column %q not found", e.Name)
		}
		return func(r storage.Row) any { return r.Values[idx] }, nil

	case *parser.IntegerLit:
		v := e.Value
		return func(storage.Row) any { return v }, nil

	case *parser.StringLit:
		v := e.Value
		return func(storage.Row) any { return v }, nil

	case *parser.BoolLit:
		v := e.Value
		return func(storage.Row) any { return v }, nil

	case *parser.NullLit:
		return func(storage.Row) any { return nil }, nil

	case *parser.BinaryExpr:
		return compileBinaryExpr(e, def)

	default:
		return nil, fmt.Errorf("unsupported expression type %T", expr)
	}
}

func compileBinaryExpr(e *parser.BinaryExpr, def *storage.TableDef) (exprFunc, error) {
	left, err := compileExpr(e.Left, def)
	if err != nil {
		return nil, err
	}
	right, err := compileExpr(e.Right, def)
	if err != nil {
		return nil, err
	}

	switch e.Op {
	case "AND":
		return func(r storage.Row) any {
			lv, lok := left(r).(bool)
			rv, rok := right(r).(bool)
			if !lok || !rok {
				return nil
			}
			return lv && rv
		}, nil
	case "OR":
		return func(r storage.Row) any {
			lv, lok := left(r).(bool)
			rv, rok := right(r).(bool)
			if !lok || !rok {
				return nil
			}
			return lv || rv
		}, nil
	case "=":
		return func(r storage.Row) any { return compareValues(left(r), right(r)) == 0 }, nil
	case "!=":
		return func(r storage.Row) any { return compareValues(left(r), right(r)) != 0 }, nil
	case "<":
		return func(r storage.Row) any {
			c := compareValues(left(r), right(r))
			return c != -2 && c < 0
		}, nil
	case ">":
		return func(r storage.Row) any {
			c := compareValues(left(r), right(r))
			return c != -2 && c > 0
		}, nil
	case "<=":
		return func(r storage.Row) any {
			c := compareValues(left(r), right(r))
			return c != -2 && c <= 0
		}, nil
	case ">=":
		return func(r storage.Row) any {
			c := compareValues(left(r), right(r))
			return c != -2 && c >= 0
		}, nil
	default:
		return nil, fmt.Errorf("unsupported operator %q", e.Op)
	}
}

// compareValues returns -1, 0, or 1 for ordering, or -2 if the values
// are not comparable (e.g. NULL or type mismatch).
func compareValues(a, b any) int {
	if a == nil || b == nil {
		return -2
	}
	switch av := a.(type) {
	case int64:
		bv, ok := b.(int64)
		if !ok {
			return -2
		}
		switch {
		case av < bv:
			return -1
		case av > bv:
			return 1
		default:
			return 0
		}
	case string:
		bv, ok := b.(string)
		if !ok {
			return -2
		}
		return strings.Compare(av, bv)
	case bool:
		bv, ok := b.(bool)
		if !ok {
			return -2
		}
		if av == bv {
			return 0
		}
		if !av && bv {
			return -1
		}
		return 1
	default:
		return -2
	}
}

// -------------------------------------------------------------------------
// Helpers
// -------------------------------------------------------------------------

// evalLiteral evaluates a parser.Expr that must be a literal value
// (used for INSERT values and UPDATE SET values).
func evalLiteral(expr parser.Expr) (any, error) {
	switch e := expr.(type) {
	case *parser.IntegerLit:
		return e.Value, nil
	case *parser.StringLit:
		return e.Value, nil
	case *parser.BoolLit:
		return e.Value, nil
	case *parser.NullLit:
		return nil, nil
	default:
		return nil, fmt.Errorf("expected literal value, got %T", expr)
	}
}

func parseDataType(s string) (storage.DataType, error) {
	switch strings.ToUpper(s) {
	case "INTEGER":
		return storage.TypeInteger, nil
	case "TEXT":
		return storage.TypeText, nil
	case "BOOLEAN":
		return storage.TypeBoolean, nil
	default:
		return 0, fmt.Errorf("unknown data type %q", s)
	}
}

func columnIndex(def *storage.TableDef, name string) int {
	for i, c := range def.Columns {
		if strings.EqualFold(c.Name, name) {
			return i
		}
	}
	return -1
}

func typeOID(dt storage.DataType) int32 {
	switch dt {
	case storage.TypeInteger:
		return OIDInt8
	case storage.TypeText:
		return OIDText
	case storage.TypeBoolean:
		return OIDBool
	default:
		return OIDUnknown
	}
}

func typeSize(dt storage.DataType) int16 {
	switch dt {
	case storage.TypeInteger:
		return 8
	case storage.TypeBoolean:
		return 1
	default:
		return -1 // variable length
	}
}

// formatValue converts a storage value to its text-encoded wire format.
// nil means SQL NULL.
func formatValue(v any) []byte {
	if v == nil {
		return nil
	}
	switch val := v.(type) {
	case int64:
		return []byte(strconv.FormatInt(val, 10))
	case string:
		return []byte(val)
	case bool:
		if val {
			return []byte("t")
		}
		return []byte("f")
	default:
		return []byte(fmt.Sprintf("%v", v))
	}
}
