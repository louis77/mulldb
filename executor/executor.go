package executor

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

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

// Execute runs a single SQL statement (no tracing overhead).
func (e *Executor) Execute(sql string) (*Result, error) {
	return e.execute(sql, nil)
}

// ExecuteTraced runs a single SQL statement with timing instrumentation.
func (e *Executor) ExecuteTraced(sql string) (*Result, *Trace, error) {
	tr := &Trace{}
	start := time.Now()
	result, err := e.execute(sql, tr)
	tr.Total = time.Since(start)
	return result, tr, err
}

func (e *Executor) execute(sql string, tr *Trace) (*Result, error) {
	var parseStart time.Time
	if tr != nil {
		parseStart = time.Now()
	}

	stmt, err := parser.Parse(sql)

	if tr != nil {
		tr.Parse = time.Since(parseStart)
	}
	if err != nil {
		return nil, &QueryError{Code: "42601", Message: err.Error()} // syntax_error
	}

	switch s := stmt.(type) {
	case *parser.CreateTableStmt:
		if tr != nil {
			tr.StmtType = "CREATE TABLE"
			tr.Table = s.Name.Name
		}
		return e.execCreateTable(s, tr)
	case *parser.DropTableStmt:
		if tr != nil {
			tr.StmtType = "DROP TABLE"
			tr.Table = s.Name.Name
		}
		return e.execDropTable(s, tr)
	case *parser.InsertStmt:
		if tr != nil {
			tr.StmtType = "INSERT"
			tr.Table = s.Table.Name
		}
		return e.execInsert(s, tr)
	case *parser.SelectStmt:
		if tr != nil {
			tr.StmtType = "SELECT"
			if !s.From.IsEmpty() {
				tr.Table = s.From.String()
			}
		}
		return e.execSelect(s, tr)
	case *parser.UpdateStmt:
		if tr != nil {
			tr.StmtType = "UPDATE"
			tr.Table = s.Table.Name
		}
		return e.execUpdate(s, tr)
	case *parser.DeleteStmt:
		if tr != nil {
			tr.StmtType = "DELETE"
			tr.Table = s.Table.Name
		}
		return e.execDelete(s, tr)
	case *parser.BeginStmt:
		if tr != nil {
			tr.StmtType = "BEGIN"
		}
		return &Result{Tag: "BEGIN"}, nil
	case *parser.CommitStmt:
		if tr != nil {
			tr.StmtType = "COMMIT"
		}
		return &Result{Tag: "COMMIT"}, nil
	case *parser.RollbackStmt:
		if tr != nil {
			tr.StmtType = "ROLLBACK"
		}
		return &Result{Tag: "ROLLBACK"}, nil
	default:
		return nil, &QueryError{Code: "42601", Message: fmt.Sprintf("unsupported statement type %T", stmt)}
	}
}

// -------------------------------------------------------------------------
// Statement executors
// -------------------------------------------------------------------------

func (e *Executor) execCreateTable(s *parser.CreateTableStmt, tr *Trace) (*Result, error) {
	var planStart time.Time
	if tr != nil {
		planStart = time.Now()
	}

	cols := make([]storage.ColumnDef, len(s.Columns))
	for i, c := range s.Columns {
		dt, err := parseDataType(c.DataType)
		if err != nil {
			return nil, WrapError(err)
		}
		cols[i] = storage.ColumnDef{Name: c.Name, DataType: dt, PrimaryKey: c.PrimaryKey}
	}

	if tr != nil {
		tr.Plan = time.Since(planStart)
	}

	var execStart time.Time
	if tr != nil {
		execStart = time.Now()
	}

	if err := e.engine.CreateTable(s.Name.Name, cols); err != nil {
		return nil, WrapError(err)
	}

	if tr != nil {
		tr.Exec = time.Since(execStart)
	}

	return &Result{Tag: "CREATE TABLE"}, nil
}

func (e *Executor) execDropTable(s *parser.DropTableStmt, tr *Trace) (*Result, error) {
	var execStart time.Time
	if tr != nil {
		execStart = time.Now()
	}

	if err := e.engine.DropTable(s.Name.Name); err != nil {
		return nil, WrapError(err)
	}

	if tr != nil {
		tr.Exec = time.Since(execStart)
	}

	return &Result{Tag: "DROP TABLE"}, nil
}

func (e *Executor) execInsert(s *parser.InsertStmt, tr *Trace) (*Result, error) {
	if isCatalogTable(s.Table.Schema, s.Table.Name) {
		return nil, &QueryError{Code: "42809", Message: fmt.Sprintf("cannot insert into catalog table %q", s.Table.String())}
	}

	var planStart time.Time
	if tr != nil {
		planStart = time.Now()
	}

	def, ok := e.engine.GetTable(s.Table.Name)
	if !ok {
		return nil, WrapError(&storage.TableNotFoundError{Name: s.Table.String()})
	}

	rows := make([][]any, len(s.Values))
	for i, exprRow := range s.Values {
		vals := make([]any, len(exprRow))
		for j, expr := range exprRow {
			v, err := evalLiteral(expr)
			if err != nil {
				return nil, WrapError(fmt.Errorf("row %d, value %d: %w", i, j, err))
			}
			vals[j] = v
		}
		rows[i] = vals
	}

	if tr != nil {
		tr.Plan = time.Since(planStart)
	}

	var execStart time.Time
	if tr != nil {
		execStart = time.Now()
	}

	n, err := e.engine.Insert(s.Table.Name, s.Columns, rows)
	if err != nil {
		return nil, WrapError(err)
	}

	if tr != nil {
		tr.Exec = time.Since(execStart)
		tr.RowsReturned = int64(n)
	}

	_ = def // used above for context; insert delegates column resolution to engine
	return &Result{Tag: fmt.Sprintf("INSERT 0 %d", n)}, nil
}

func (e *Executor) execSelect(s *parser.SelectStmt, tr *Trace) (*Result, error) {
	if s.From.IsEmpty() {
		return execSelectStatic(s.Columns)
	}

	var planStart time.Time
	if tr != nil {
		planStart = time.Now()
	}

	// Check catalog tables before the storage engine.
	var def *storage.TableDef
	var isCatalog bool
	if def, isCatalog = getCatalogTable(s.From.Schema, s.From.Name); !isCatalog {
		var ok bool
		def, ok = e.engine.GetTable(s.From.Name)
		if !ok {
			return nil, WrapError(&storage.TableNotFoundError{Name: s.From.String()})
		}
	}

	// Validate LIMIT/OFFSET values.
	if s.Limit != nil && *s.Limit < 0 {
		return nil, &QueryError{Code: "2201W", Message: "LIMIT must not be negative"}
	}
	if s.Offset != nil && *s.Offset < 0 {
		return nil, &QueryError{Code: "2201X", Message: "OFFSET must not be negative"}
	}

	// Detect aggregate vs non-aggregate columns.
	hasAgg, hasNonAgg := false, false
	for _, col := range s.Columns {
		expr := col
		if a, ok := expr.(*parser.AliasExpr); ok {
			expr = a.Expr
		}
		if _, ok := expr.(*parser.FunctionCallExpr); ok {
			hasAgg = true
		} else {
			hasNonAgg = true
		}
	}
	if hasAgg && hasNonAgg {
		return nil, &QueryError{
			Code:    "42803",
			Message: "aggregate and non-aggregate columns cannot be mixed without GROUP BY",
		}
	}
	if hasAgg {
		if len(s.OrderBy) > 0 {
			return nil, &QueryError{
				Code:    "0A000",
				Message: "ORDER BY is not supported with aggregate functions without GROUP BY",
			}
		}
		return e.execSelectAggregate(s, def, tr)
	}

	// Resolve which columns to return.
	colEvals, resultCols, err := resolveSelectColumns(s.Columns, def)
	if err != nil {
		return nil, WrapError(err)
	}

	// Build the WHERE filter.
	var filter func(storage.Row) bool
	if s.Where != nil {
		filter, err = buildFilter(s.Where, def)
		if err != nil {
			return nil, WrapError(err)
		}
	}

	// Validate ORDER BY columns and resolve their indices.
	type orderKey struct {
		colIdx int
		desc   bool
	}
	var orderKeys []orderKey
	for _, ob := range s.OrderBy {
		idx := columnIndex(def, ob.Column)
		if idx < 0 {
			return nil, WrapError(fmt.Errorf("column %q not found in table %q", ob.Column, def.Name))
		}
		orderKeys = append(orderKeys, orderKey{colIdx: idx, desc: ob.Desc})
	}

	if tr != nil {
		tr.Plan = time.Since(planStart)
	}

	var execStart time.Time
	if tr != nil {
		execStart = time.Now()
	}

	// Try PK index lookup for simple equality on the primary key column.
	if !isCatalog && s.Where != nil {
		if row, ok := e.tryPKLookup(s.Where, def); ok {
			if tr != nil {
				tr.UsedIndex = true
				tr.RowsScanned = 1
			}
			// Apply OFFSET/LIMIT to the single-row result.
			var resultRows [][][]byte
			skip := s.Offset != nil && *s.Offset > 0
			empty := s.Limit != nil && *s.Limit == 0
			if !skip && !empty {
				textRow := make([][]byte, len(colEvals))
				for i, eval := range colEvals {
					textRow[i] = formatValue(eval(*row))
				}
				resultRows = [][][]byte{textRow}
			}
			if tr != nil {
				tr.RowsReturned = int64(len(resultRows))
				tr.Exec = time.Since(execStart)
			}
			return &Result{
				Columns: resultCols,
				Rows:    resultRows,
				Tag:     fmt.Sprintf("SELECT %d", len(resultRows)),
			}, nil
		}
	}

	// Scan and filter rows.
	var it storage.RowIterator
	if isCatalog {
		it, err = scanCatalogTable(s.From.Schema, s.From.Name, e.engine)
	} else {
		it, err = e.engine.Scan(s.From.Name)
	}
	if err != nil {
		return nil, WrapError(err)
	}
	defer it.Close()

	// Apply LIMIT/OFFSET during row collection for efficiency.
	var offset int64
	if s.Offset != nil {
		offset = *s.Offset
	}
	limit := int64(-1) // -1 means no limit
	if s.Limit != nil {
		limit = *s.Limit
	}

	var resultRows [][][]byte
	var scanned int64

	if len(orderKeys) > 0 {
		// ORDER BY path: collect all matching rows, sort, then apply LIMIT/OFFSET.
		var matched []storage.Row
		for {
			row, ok := it.Next()
			if !ok {
				break
			}
			scanned++
			if filter != nil && !filter(row) {
				continue
			}
			matched = append(matched, row)
		}

		// Sort using stable sort to preserve insertion order for equal keys.
		var sortStart time.Time
		if tr != nil {
			sortStart = time.Now()
		}
		sort.SliceStable(matched, func(i, j int) bool {
			for _, key := range orderKeys {
				av := matched[i].Values[key.colIdx]
				bv := matched[j].Values[key.colIdx]

				// NULLs always sort last regardless of direction.
				if av == nil && bv == nil {
					continue
				}
				if av == nil {
					return false // NULL sorts last
				}
				if bv == nil {
					return true // NULL sorts last
				}

				cmp := storage.CompareValues(av, bv)
				if cmp == 0 {
					continue
				}
				if key.desc {
					return cmp > 0
				}
				return cmp < 0
			}
			return false
		})
		if tr != nil {
			tr.Sort = time.Since(sortStart)
		}

		// Apply OFFSET.
		start := int64(0)
		if offset > 0 {
			start = offset
		}
		if start > int64(len(matched)) {
			start = int64(len(matched))
		}

		// Apply LIMIT.
		end := int64(len(matched))
		if limit >= 0 && start+limit < end {
			end = start + limit
		}

		for _, row := range matched[start:end] {
			textRow := make([][]byte, len(colEvals))
			for i, eval := range colEvals {
				textRow[i] = formatValue(eval(row))
			}
			resultRows = append(resultRows, textRow)
		}
	} else {
		// No ORDER BY: streaming path with early LIMIT termination.
		var matched int64
		for {
			row, ok := it.Next()
			if !ok {
				break
			}
			scanned++
			if filter != nil && !filter(row) {
				continue
			}
			matched++
			if matched <= offset {
				continue
			}
			if limit == 0 {
				break
			}
			textRow := make([][]byte, len(colEvals))
			for i, eval := range colEvals {
				textRow[i] = formatValue(eval(row))
			}
			resultRows = append(resultRows, textRow)
			if limit > 0 && int64(len(resultRows)) >= limit {
				break
			}
		}
	}

	if tr != nil {
		tr.RowsScanned = scanned
		tr.RowsReturned = int64(len(resultRows))
		tr.Exec = time.Since(execStart)
	}

	return &Result{
		Columns: resultCols,
		Rows:    resultRows,
		Tag:     fmt.Sprintf("SELECT %d", len(resultRows)),
	}, nil
}

func (e *Executor) execSelectAggregate(s *parser.SelectStmt, def *storage.TableDef, tr *Trace) (*Result, error) {
	var planStart time.Time
	if tr != nil {
		planStart = time.Now()
	}

	type aggAcc struct {
		funcName  string
		colIdx    int // -1 for COUNT(*)
		inputType storage.DataType
		count     int64
		sumI      int64
		minV      any
		maxV      any
		hasV      bool
	}

	accs := make([]*aggAcc, len(s.Columns))
	resultCols := make([]Column, len(s.Columns))

	for i, expr := range s.Columns {
		// Unwrap alias if present.
		alias := ""
		inner := expr
		if a, ok := inner.(*parser.AliasExpr); ok {
			alias = a.Alias
			inner = a.Expr
		}
		fn := inner.(*parser.FunctionCallExpr)
		acc := &aggAcc{funcName: fn.Name, colIdx: -1}

		if len(fn.Args) == 1 {
			switch arg := fn.Args[0].(type) {
			case *parser.StarExpr:
				acc.colIdx = -1
			case *parser.ColumnRef:
				idx := columnIndex(def, arg.Name)
				if idx < 0 {
					return nil, WrapError(fmt.Errorf("column %q not found in table %q", arg.Name, def.Name))
				}
				acc.colIdx = idx
				acc.inputType = def.Columns[idx].DataType
			}
		}

		switch fn.Name {
		case "SUM":
			if acc.colIdx < 0 {
				return nil, &QueryError{Code: "42883", Message: "SUM requires a column argument"}
			}
			if acc.inputType != storage.TypeInteger {
				return nil, &QueryError{Code: "42883", Message: fmt.Sprintf("SUM: column must be INTEGER, got %s", acc.inputType)}
			}
		case "MIN", "MAX":
			if acc.colIdx < 0 {
				return nil, &QueryError{Code: "42883", Message: fn.Name + " requires a column argument"}
			}
		case "COUNT":
			// COUNT(*) or COUNT(col) — both valid
		default:
			return nil, &QueryError{Code: "42883", Message: fmt.Sprintf("unknown aggregate function %q", fn.Name)}
		}

		accs[i] = acc
		colName := strings.ToLower(fn.Name)
		if alias != "" {
			colName = alias
		}
		resultCols[i] = Column{
			Name:     colName,
			TypeOID:  aggregateTypeOID(fn.Name, acc.inputType),
			TypeSize: aggregateTypeSize(fn.Name, acc.inputType),
		}
	}

	if tr != nil {
		tr.Plan = time.Since(planStart)
	}

	var execStart time.Time
	if tr != nil {
		execStart = time.Now()
	}

	// Scan all rows and accumulate.
	var it storage.RowIterator
	var err error
	if isCatalogTable(s.From.Schema, s.From.Name) {
		it, err = scanCatalogTable(s.From.Schema, s.From.Name, e.engine)
	} else {
		it, err = e.engine.Scan(s.From.Name)
	}
	if err != nil {
		return nil, WrapError(err)
	}
	defer it.Close()

	var scanned int64
	for {
		row, ok := it.Next()
		if !ok {
			break
		}
		scanned++
		for _, acc := range accs {
			switch acc.funcName {
			case "COUNT":
				if acc.colIdx < 0 || row.Values[acc.colIdx] != nil {
					acc.count++
				}
			case "SUM":
				if v, ok := row.Values[acc.colIdx].(int64); ok {
					acc.sumI += v
				}
			case "MIN":
				v := row.Values[acc.colIdx]
				if v == nil {
					continue
				}
				if !acc.hasV || storage.CompareValues(v, acc.minV) < 0 {
					acc.minV = v
					acc.hasV = true
				}
			case "MAX":
				v := row.Values[acc.colIdx]
				if v == nil {
					continue
				}
				if !acc.hasV || storage.CompareValues(v, acc.maxV) > 0 {
					acc.maxV = v
					acc.hasV = true
				}
			}
		}
	}

	// Build the single result row.
	resultRow := make([][]byte, len(accs))
	for i, acc := range accs {
		switch acc.funcName {
		case "COUNT":
			resultRow[i] = formatValue(acc.count)
		case "SUM":
			resultRow[i] = formatValue(acc.sumI)
		case "MIN":
			resultRow[i] = formatValue(acc.minV)
		case "MAX":
			resultRow[i] = formatValue(acc.maxV)
		}
	}

	// Apply LIMIT/OFFSET to the single aggregate result row.
	rows := [][][]byte{resultRow}
	if s.Offset != nil && *s.Offset > 0 {
		rows = nil
	}
	if s.Limit != nil && *s.Limit == 0 {
		rows = nil
	}

	if tr != nil {
		tr.RowsScanned = scanned
		tr.RowsReturned = int64(len(rows))
		tr.Exec = time.Since(execStart)
	}

	return &Result{
		Columns: resultCols,
		Rows:    rows,
		Tag:     fmt.Sprintf("SELECT %d", len(rows)),
	}, nil
}

// execSelectStatic handles SELECT with no FROM clause (e.g. SELECT 1, VERSION()).
func execSelectStatic(exprs []parser.Expr) (*Result, error) {
	var cols []Column
	var row [][]byte

	for _, expr := range exprs {
		// Unwrap alias if present.
		alias := ""
		inner := expr
		if a, ok := inner.(*parser.AliasExpr); ok {
			alias = a.Alias
			inner = a.Expr
		}
		val, col, err := evalStaticExpr(inner)
		if err != nil {
			return nil, err
		}
		if alias != "" {
			col.Name = alias
		}
		cols = append(cols, col)
		row = append(row, formatValue(val))
	}

	return &Result{
		Columns: cols,
		Rows:    [][][]byte{row},
		Tag:     "SELECT 1",
	}, nil
}


func (e *Executor) execUpdate(s *parser.UpdateStmt, tr *Trace) (*Result, error) {
	if isCatalogTable(s.Table.Schema, s.Table.Name) {
		return nil, &QueryError{Code: "42809", Message: fmt.Sprintf("cannot update catalog table %q", s.Table.String())}
	}

	var planStart time.Time
	if tr != nil {
		planStart = time.Now()
	}

	def, ok := e.engine.GetTable(s.Table.Name)
	if !ok {
		return nil, WrapError(&storage.TableNotFoundError{Name: s.Table.String()})
	}

	// Evaluate SET values.
	sets := make(map[string]any, len(s.Sets))
	for _, sc := range s.Sets {
		v, err := evalLiteral(sc.Value)
		if err != nil {
			return nil, WrapError(fmt.Errorf("SET %s: %w", sc.Column, err))
		}
		sets[sc.Column] = v
	}

	// Build WHERE filter.
	var filter func(storage.Row) bool
	var err error
	if s.Where != nil {
		filter, err = buildFilter(s.Where, def)
		if err != nil {
			return nil, WrapError(err)
		}
	}

	if tr != nil {
		tr.Plan = time.Since(planStart)
	}

	var execStart time.Time
	if tr != nil {
		execStart = time.Now()
	}

	n, err := e.engine.Update(s.Table.Name, sets, filter)
	if err != nil {
		return nil, WrapError(err)
	}

	if tr != nil {
		tr.RowsReturned = int64(n)
		tr.Exec = time.Since(execStart)
	}

	return &Result{Tag: fmt.Sprintf("UPDATE %d", n)}, nil
}

func (e *Executor) execDelete(s *parser.DeleteStmt, tr *Trace) (*Result, error) {
	if isCatalogTable(s.Table.Schema, s.Table.Name) {
		return nil, &QueryError{Code: "42809", Message: fmt.Sprintf("cannot delete from catalog table %q", s.Table.String())}
	}

	var planStart time.Time
	if tr != nil {
		planStart = time.Now()
	}

	def, ok := e.engine.GetTable(s.Table.Name)
	if !ok {
		return nil, WrapError(&storage.TableNotFoundError{Name: s.Table.String()})
	}

	var filter func(storage.Row) bool
	var err error
	if s.Where != nil {
		filter, err = buildFilter(s.Where, def)
		if err != nil {
			return nil, WrapError(err)
		}
	}

	if tr != nil {
		tr.Plan = time.Since(planStart)
	}

	var execStart time.Time
	if tr != nil {
		execStart = time.Now()
	}

	n, err := e.engine.Delete(s.Table.Name, filter)
	if err != nil {
		return nil, WrapError(err)
	}

	if tr != nil {
		tr.RowsReturned = int64(n)
		tr.Exec = time.Since(execStart)
	}

	return &Result{Tag: fmt.Sprintf("DELETE %d", n)}, nil
}

// -------------------------------------------------------------------------
// Column resolution
// -------------------------------------------------------------------------

func resolveSelectColumns(exprs []parser.Expr, def *storage.TableDef) ([]exprFunc, []Column, error) {
	var evals []exprFunc
	var cols []Column

	for _, expr := range exprs {
		// Unwrap alias if present.
		alias := ""
		inner := expr
		if a, ok := inner.(*parser.AliasExpr); ok {
			alias = a.Alias
			inner = a.Expr
		}

		switch e := inner.(type) {
		case *parser.StarExpr:
			for i, c := range def.Columns {
				idx := i
				evals = append(evals, func(r storage.Row) any { return r.Values[idx] })
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
			evals = append(evals, func(r storage.Row) any { return r.Values[idx] })
			name := c.Name
			if alias != "" {
				name = alias
			}
			cols = append(cols, Column{
				Name:     name,
				TypeOID:  typeOID(c.DataType),
				TypeSize: typeSize(c.DataType),
			})
		case *parser.IntegerLit:
			v := e.Value
			evals = append(evals, func(r storage.Row) any { return v })
			name := "?column?"
			if alias != "" {
				name = alias
			}
			cols = append(cols, Column{Name: name, TypeOID: OIDInt8, TypeSize: 8})
		case *parser.StringLit:
			v := e.Value
			evals = append(evals, func(r storage.Row) any { return v })
			name := "?column?"
			if alias != "" {
				name = alias
			}
			cols = append(cols, Column{Name: name, TypeOID: OIDText, TypeSize: -1})
		case *parser.BoolLit:
			v := e.Value
			evals = append(evals, func(r storage.Row) any { return v })
			name := "?column?"
			if alias != "" {
				name = alias
			}
			cols = append(cols, Column{Name: name, TypeOID: OIDBool, TypeSize: 1})
		case *parser.NullLit:
			evals = append(evals, func(r storage.Row) any { return nil })
			name := "?column?"
			if alias != "" {
				name = alias
			}
			cols = append(cols, Column{Name: name, TypeOID: OIDUnknown, TypeSize: -1})
		default:
			compiled, err := compileExpr(inner, def)
			if err != nil {
				return nil, nil, err
			}
			evals = append(evals, compiled)
			name := "?column?"
			if alias != "" {
				name = alias
			}
			cols = append(cols, Column{Name: name, TypeOID: OIDInt8, TypeSize: 8})
		}
	}
	return evals, cols, nil
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

	case *parser.IsNullExpr:
		inner, err := compileExpr(e.Expr, def)
		if err != nil {
			return nil, err
		}
		if e.Not {
			return func(r storage.Row) any { return inner(r) != nil }, nil
		}
		return func(r storage.Row) any { return inner(r) == nil }, nil

	case *parser.UnaryExpr:
		inner, err := compileExpr(e.Expr, def)
		if err != nil {
			return nil, err
		}
		return func(r storage.Row) any {
			v := inner(r)
			if v == nil {
				return nil
			}
			iv, ok := v.(int64)
			if !ok {
				return nil
			}
			return -iv
		}, nil

	case *parser.NotExpr:
		inner, err := compileExpr(e.Expr, def)
		if err != nil {
			return nil, err
		}
		return func(r storage.Row) any {
			v, ok := inner(r).(bool)
			if !ok {
				return nil
			}
			return !v
		}, nil

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
		return func(r storage.Row) any {
			c := storage.CompareValues(left(r), right(r))
			if c == -2 {
				return nil
			}
			return c == 0
		}, nil
	case "!=":
		return func(r storage.Row) any {
			c := storage.CompareValues(left(r), right(r))
			if c == -2 {
				return nil
			}
			return c != 0
		}, nil
	case "<":
		return func(r storage.Row) any {
			c := storage.CompareValues(left(r), right(r))
			if c == -2 {
				return nil
			}
			return c < 0
		}, nil
	case ">":
		return func(r storage.Row) any {
			c := storage.CompareValues(left(r), right(r))
			if c == -2 {
				return nil
			}
			return c > 0
		}, nil
	case "<=":
		return func(r storage.Row) any {
			c := storage.CompareValues(left(r), right(r))
			if c == -2 {
				return nil
			}
			return c <= 0
		}, nil
	case ">=":
		return func(r storage.Row) any {
			c := storage.CompareValues(left(r), right(r))
			if c == -2 {
				return nil
			}
			return c >= 0
		}, nil
	case "+", "-", "*", "/", "%":
		op := e.Op
		return func(r storage.Row) any {
			lv, rv := left(r), right(r)
			if lv == nil || rv == nil {
				return nil
			}
			li, lok := lv.(int64)
			ri, rok := rv.(int64)
			if !lok || !rok {
				return nil
			}
			switch op {
			case "+":
				return li + ri
			case "-":
				return li - ri
			case "*":
				return li * ri
			case "/":
				if ri == 0 {
					return nil // division by zero handled at query level
				}
				return li / ri
			case "%":
				if ri == 0 {
					return nil
				}
				return li % ri
			}
			return nil
		}, nil

	default:
		return nil, fmt.Errorf("unsupported operator %q", e.Op)
	}
}


// -------------------------------------------------------------------------
// PK index lookup
// -------------------------------------------------------------------------

// tryPKLookup checks if the WHERE expression is a simple "pk_column = literal"
// equality and if so, performs an indexed lookup. Returns the row and true if
// found via index, or (nil, false) if not applicable or no match.
func (e *Executor) tryPKLookup(where parser.Expr, def *storage.TableDef) (*storage.Row, bool) {
	pkCol := def.PrimaryKeyColumn()
	if pkCol < 0 {
		return nil, false
	}

	bin, ok := where.(*parser.BinaryExpr)
	if !ok || bin.Op != "=" {
		return nil, false
	}

	// Match pk_col = literal or literal = pk_col.
	colRef, lit := extractColumnAndLiteral(bin)
	if colRef == nil || lit == nil {
		return nil, false
	}

	// Verify the column is the PK column.
	if columnIndex(def, colRef.Name) != pkCol {
		return nil, false
	}

	val, err := evalLiteral(lit)
	if err != nil || val == nil {
		return nil, false
	}

	row, err := e.engine.LookupByPK(def.Name, val)
	if err != nil || row == nil {
		return nil, false
	}
	return row, true
}

// extractColumnAndLiteral checks if a binary expression has a ColumnRef on one
// side and a literal on the other. Returns (column, literal) or (nil, nil).
func extractColumnAndLiteral(bin *parser.BinaryExpr) (*parser.ColumnRef, parser.Expr) {
	if col, ok := bin.Left.(*parser.ColumnRef); ok {
		if isLiteralExpr(bin.Right) {
			return col, bin.Right
		}
	}
	if col, ok := bin.Right.(*parser.ColumnRef); ok {
		if isLiteralExpr(bin.Left) {
			return col, bin.Left
		}
	}
	return nil, nil
}

func isLiteralExpr(e parser.Expr) bool {
	switch e.(type) {
	case *parser.IntegerLit, *parser.StringLit, *parser.BoolLit:
		return true
	}
	return false
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
	case *parser.BinaryExpr:
		val, _, err := evalStaticExpr(e)
		return val, err
	case *parser.UnaryExpr:
		val, _, err := evalStaticExpr(e)
		return val, err
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

func aggregateTypeOID(funcName string, inputType storage.DataType) int32 {
	switch funcName {
	case "COUNT", "SUM":
		return OIDInt8
	case "MIN", "MAX":
		return typeOID(inputType)
	default:
		return OIDUnknown
	}
}

func aggregateTypeSize(funcName string, inputType storage.DataType) int16 {
	switch funcName {
	case "COUNT", "SUM":
		return 8
	case "MIN", "MAX":
		return typeSize(inputType)
	default:
		return -1
	}
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
