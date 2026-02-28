package executor

import (
	"fmt"
	"math"
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
	case *parser.AlterTableAddColumnStmt:
		if tr != nil {
			tr.StmtType = "ALTER TABLE"
			tr.Table = s.Table.Name
		}
		return e.execAlterTableAddColumn(s, tr)
	case *parser.AlterTableDropColumnStmt:
		if tr != nil {
			tr.StmtType = "ALTER TABLE"
			tr.Table = s.Table.Name
		}
		return e.execAlterTableDropColumn(s, tr)
	case *parser.CreateIndexStmt:
		if tr != nil {
			tr.StmtType = "CREATE INDEX"
			tr.Table = s.Table.Name
		}
		return e.execCreateIndex(s, tr)
	case *parser.DropIndexStmt:
		if tr != nil {
			tr.StmtType = "DROP INDEX"
			tr.Table = s.Table.Name
		}
		return e.execDropIndex(s, tr)
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
		cols[i] = storage.ColumnDef{Name: c.Name, DataType: dt, PrimaryKey: c.PrimaryKey, NotNull: c.NotNull || c.PrimaryKey}
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

func (e *Executor) execAlterTableAddColumn(s *parser.AlterTableAddColumnStmt, tr *Trace) (*Result, error) {
	if isCatalogTable(s.Table.Schema, s.Table.Name) {
		return nil, &QueryError{Code: "42809", Message: fmt.Sprintf("cannot alter catalog table %q", s.Table.String())}
	}

	if s.Column.NotNull {
		return nil, &QueryError{Code: "0A000", Message: "cannot add a NOT NULL column without a default value"}
	}

	dt, err := parseDataType(s.Column.DataType)
	if err != nil {
		return nil, WrapError(err)
	}
	col := storage.ColumnDef{
		Name:     s.Column.Name,
		DataType: dt,
	}

	var execStart time.Time
	if tr != nil {
		execStart = time.Now()
	}

	if err := e.engine.AddColumn(s.Table.Name, col); err != nil {
		return nil, WrapError(err)
	}

	if tr != nil {
		tr.Exec = time.Since(execStart)
	}

	return &Result{Tag: "ALTER TABLE"}, nil
}

func (e *Executor) execAlterTableDropColumn(s *parser.AlterTableDropColumnStmt, tr *Trace) (*Result, error) {
	if isCatalogTable(s.Table.Schema, s.Table.Name) {
		return nil, &QueryError{Code: "42809", Message: fmt.Sprintf("cannot alter catalog table %q", s.Table.String())}
	}

	var execStart time.Time
	if tr != nil {
		execStart = time.Now()
	}

	if err := e.engine.DropColumn(s.Table.Name, s.Column); err != nil {
		return nil, WrapError(err)
	}

	if tr != nil {
		tr.Exec = time.Since(execStart)
	}

	return &Result{Tag: "ALTER TABLE"}, nil
}

func (e *Executor) execCreateIndex(s *parser.CreateIndexStmt, tr *Trace) (*Result, error) {
	if isCatalogTable(s.Table.Schema, s.Table.Name) {
		return nil, &QueryError{Code: "42809", Message: fmt.Sprintf("cannot create index on catalog table %q", s.Table.String())}
	}

	name := s.Name
	if name == "" {
		name = "idx_" + s.Column
	}

	var execStart time.Time
	if tr != nil {
		execStart = time.Now()
	}

	idx := storage.IndexDef{
		Name:   name,
		Column: s.Column,
		Unique: s.Unique,
	}
	if err := e.engine.CreateIndex(s.Table.Name, idx); err != nil {
		return nil, WrapError(err)
	}

	if tr != nil {
		tr.Exec = time.Since(execStart)
	}

	return &Result{Tag: "CREATE INDEX"}, nil
}

func (e *Executor) execDropIndex(s *parser.DropIndexStmt, tr *Trace) (*Result, error) {
	if isCatalogTable(s.Table.Schema, s.Table.Name) {
		return nil, &QueryError{Code: "42809", Message: fmt.Sprintf("cannot drop index on catalog table %q", s.Table.String())}
	}

	var execStart time.Time
	if tr != nil {
		execStart = time.Now()
	}

	if err := e.engine.DropIndex(s.Table.Name, s.Name); err != nil {
		return nil, WrapError(err)
	}

	if tr != nil {
		tr.Exec = time.Since(execStart)
	}

	return &Result{Tag: "DROP INDEX"}, nil
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

	// Branch to join execution if joins are present.
	if len(s.Joins) > 0 {
		return e.execSelectJoin(s, tr)
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
	isAggFunc := func(name string) bool {
		switch name {
		case "COUNT", "SUM", "MIN", "MAX", "AVG":
			return true
		}
		return false
	}
	hasAgg, hasNonAgg := false, false
	for _, col := range s.Columns {
		expr := col
		if a, ok := expr.(*parser.AliasExpr); ok {
			expr = a.Expr
		}
		if fn, ok := expr.(*parser.FunctionCallExpr); ok && isAggFunc(fn.Name) {
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
				tr.IndexName = "PRIMARY"
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

	// Explicit INDEXED BY: use named secondary index.
	if !isCatalog && s.IndexedBy != "" {
		rows, err := e.lookupByNamedIndex(s.IndexedBy, s.Where, def)
		if err != nil {
			return nil, err
		}
		if tr != nil {
			tr.IndexName = s.IndexedBy
			tr.RowsScanned = int64(len(rows))
		}
		var resultRows [][][]byte
		var offset int64
		if s.Offset != nil {
			offset = *s.Offset
		}
		limit := int64(-1)
		if s.Limit != nil {
			limit = *s.Limit
		}

		// Optionally sort.
		if len(orderKeys) > 0 {
			sort.SliceStable(rows, func(i, j int) bool {
				for _, ok := range orderKeys {
					vi := storage.RowValue(rows[i].Values, ok.colIdx)
					vj := storage.RowValue(rows[j].Values, ok.colIdx)
					c := storage.CompareValues(vi, vj)
					if c == -2 {
						if vi == nil && vj == nil {
							continue
						}
						if vi == nil {
							return false
						}
						return true
					}
					if c == 0 {
						continue
					}
					if ok.desc {
						return c > 0
					}
					return c < 0
				}
				return false
			})
		}

		var skipped int64
		for _, row := range rows {
			if filter != nil && !filter(row) {
				continue
			}
			if skipped < offset {
				skipped++
				continue
			}
			if limit >= 0 && int64(len(resultRows)) >= limit {
				break
			}
			textRow := make([][]byte, len(colEvals))
			for i, eval := range colEvals {
				textRow[i] = formatValue(eval(row))
			}
			resultRows = append(resultRows, textRow)
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
				av := storage.RowValue(matched[i].Values, key.colIdx)
				bv := storage.RowValue(matched[j].Values, key.colIdx)

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
		sumF      float64
		minV         any
		maxV         any
		hasV         bool
		countNonNull int64
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
				acc.inputType = columnByOrdinal(def, idx).DataType
			}
		}

		switch fn.Name {
		case "SUM":
			if acc.colIdx < 0 {
				return nil, &QueryError{Code: "42883", Message: "SUM requires a column argument"}
			}
			if acc.inputType != storage.TypeInteger && acc.inputType != storage.TypeFloat {
				return nil, &QueryError{Code: "42883", Message: fmt.Sprintf("SUM: column must be INTEGER or FLOAT, got %s", acc.inputType)}
			}
		case "AVG":
			if acc.colIdx < 0 {
				return nil, &QueryError{Code: "42883", Message: "AVG requires a column argument"}
			}
			if acc.inputType != storage.TypeInteger && acc.inputType != storage.TypeFloat {
				return nil, &QueryError{Code: "42883", Message: fmt.Sprintf("AVG: column must be INTEGER or FLOAT, got %s", acc.inputType)}
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
				if acc.colIdx < 0 || storage.RowValue(row.Values, acc.colIdx) != nil {
					acc.count++
				}
			case "SUM":
				val := storage.RowValue(row.Values, acc.colIdx)
				switch v := val.(type) {
				case int64:
					acc.sumI += v
				case float64:
					acc.sumF += v
				}
			case "MIN":
				v := storage.RowValue(row.Values, acc.colIdx)
				if v == nil {
					continue
				}
				if !acc.hasV || storage.CompareValues(v, acc.minV) < 0 {
					acc.minV = v
					acc.hasV = true
				}
			case "MAX":
				v := storage.RowValue(row.Values, acc.colIdx)
				if v == nil {
					continue
				}
				if !acc.hasV || storage.CompareValues(v, acc.maxV) > 0 {
					acc.maxV = v
					acc.hasV = true
				}
			case "AVG":
				val := storage.RowValue(row.Values, acc.colIdx)
				switch v := val.(type) {
				case int64:
					acc.sumI += v
					acc.countNonNull++
				case float64:
					acc.sumF += v
					acc.countNonNull++
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
			if acc.inputType == storage.TypeFloat {
				resultRow[i] = formatValue(acc.sumF)
			} else {
				resultRow[i] = formatValue(acc.sumI)
			}
		case "MIN":
			resultRow[i] = formatValue(acc.minV)
		case "MAX":
			resultRow[i] = formatValue(acc.maxV)
		case "AVG":
			if acc.countNonNull == 0 {
				resultRow[i] = nil
			} else if acc.inputType == storage.TypeFloat {
				resultRow[i] = formatValue(acc.sumF / float64(acc.countNonNull))
			} else {
				resultRow[i] = formatValue(float64(acc.sumI) / float64(acc.countNonNull))
			}
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


// -------------------------------------------------------------------------
// JOIN execution
// -------------------------------------------------------------------------

// scopeTable represents one table in a join scope.
type scopeTable struct {
	schema    string           // schema name ("information_schema", etc.), "" for user tables
	name      string           // original table name
	alias     string           // alias (or name if no alias)
	def       *storage.TableDef
	offset    int              // index into merged row where this table's columns start
	isCatalog bool             // true for virtual catalog tables
}

// scopeColumn represents one column in the merged join row.
type scopeColumn struct {
	tableIdx int    // index into joinScope.tables
	colIdx   int    // index into that table's Columns
	name     string
	def      storage.ColumnDef
}

// joinScope represents the merged column layout for a join.
type joinScope struct {
	columns []scopeColumn
	tables  []scopeTable
}

// resolveColumn finds a column in the scope by optional table qualifier and name.
// Returns the merged row index or an error.
func (s *joinScope) resolveColumn(table, name string) (int, error) {
	if table != "" {
		// Find the matching table by alias (or name).
		tableIdx := -1
		for i, t := range s.tables {
			if strings.EqualFold(t.alias, table) {
				tableIdx = i
				break
			}
		}
		if tableIdx < 0 {
			return -1, fmt.Errorf("table %q not found in FROM clause", table)
		}
		// Find column within that table.
		for i, c := range s.columns {
			if c.tableIdx == tableIdx && strings.EqualFold(c.name, name) {
				return i, nil
			}
		}
		return -1, fmt.Errorf("column %q not found in table %q", name, table)
	}
	// Unqualified: search all tables.
	found := -1
	for i, c := range s.columns {
		if strings.EqualFold(c.name, name) {
			if found >= 0 {
				return -1, fmt.Errorf("column reference %q is ambiguous", name)
			}
			found = i
		}
	}
	if found < 0 {
		return -1, fmt.Errorf("column %q not found", name)
	}
	return found, nil
}

// buildJoinScope creates a joinScope from the FROM table and all JOIN tables.
func (e *Executor) buildJoinScope(s *parser.SelectStmt) (*joinScope, error) {
	scope := &joinScope{}
	offset := 0

	// FROM table.
	var def *storage.TableDef
	var fromIsCatalog bool
	if catDef, ok := getCatalogTable(s.From.Schema, s.From.Name); ok {
		def = catDef
		fromIsCatalog = true
	} else {
		var ok bool
		def, ok = e.engine.GetTable(s.From.Name)
		if !ok {
			return nil, &storage.TableNotFoundError{Name: s.From.String()}
		}
	}
	alias := s.FromAlias
	if alias == "" {
		alias = s.From.Name
	}
	scope.tables = append(scope.tables, scopeTable{
		schema: s.From.Schema, name: s.From.Name, alias: alias,
		def: def, offset: offset, isCatalog: fromIsCatalog,
	})
	for i, c := range def.Columns {
		scope.columns = append(scope.columns, scopeColumn{
			tableIdx: 0, colIdx: i, name: c.Name, def: c,
		})
	}
	offset += len(def.Columns)

	// JOIN tables.
	for ji, j := range s.Joins {
		var jdef *storage.TableDef
		var jIsCatalog bool
		if catDef, ok := getCatalogTable(j.Table.Schema, j.Table.Name); ok {
			jdef = catDef
			jIsCatalog = true
		} else {
			var ok bool
			jdef, ok = e.engine.GetTable(j.Table.Name)
			if !ok {
				return nil, &storage.TableNotFoundError{Name: j.Table.String()}
			}
		}
		jalias := j.Alias
		if jalias == "" {
			jalias = j.Table.Name
		}
		tableIdx := ji + 1
		scope.tables = append(scope.tables, scopeTable{
			schema: j.Table.Schema, name: j.Table.Name, alias: jalias,
			def: jdef, offset: offset, isCatalog: jIsCatalog,
		})
		for i, c := range jdef.Columns {
			scope.columns = append(scope.columns, scopeColumn{
				tableIdx: tableIdx, colIdx: i, name: c.Name, def: c,
			})
		}
		offset += len(jdef.Columns)
	}

	return scope, nil
}

// compileJoinExpr compiles an expression against a join scope.
func compileJoinExpr(expr parser.Expr, scope *joinScope) (exprFunc, error) {
	switch e := expr.(type) {
	case *parser.ColumnRef:
		idx, err := scope.resolveColumn(e.Table, e.Name)
		if err != nil {
			return nil, err
		}
		return func(r storage.Row) any { return storage.RowValue(r.Values, idx) }, nil

	case *parser.IntegerLit:
		v := e.Value
		return func(storage.Row) any { return v }, nil

	case *parser.FloatLit:
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
		return compileJoinBinaryExpr(e, scope)

	case *parser.IsNullExpr:
		inner, err := compileJoinExpr(e.Expr, scope)
		if err != nil {
			return nil, err
		}
		if e.Not {
			return func(r storage.Row) any { return inner(r) != nil }, nil
		}
		return func(r storage.Row) any { return inner(r) == nil }, nil

	case *parser.UnaryExpr:
		inner, err := compileJoinExpr(e.Expr, scope)
		if err != nil {
			return nil, err
		}
		return func(r storage.Row) any {
			v := inner(r)
			if v == nil {
				return nil
			}
			switch n := v.(type) {
			case int64:
				return -n
			case float64:
				return -n
			default:
				return nil
			}
		}, nil

	case *parser.NotExpr:
		inner, err := compileJoinExpr(e.Expr, scope)
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

	case *parser.LikeExpr:
		return compileLikeExpr(e, func(expr parser.Expr) (exprFunc, error) {
			return compileJoinExpr(expr, scope)
		})

	case *parser.InExpr:
		return compileInExpr(e, func(expr parser.Expr) (exprFunc, error) {
			return compileJoinExpr(expr, scope)
		})

	case *parser.CastExpr:
		inner, err := compileJoinExpr(e.Expr, scope)
		if err != nil {
			return nil, err
		}
		typeName := e.TypeName
		return func(r storage.Row) any { return castValue(inner(r), typeName) }, nil

	case *parser.FunctionCallExpr:
		fn, ok := scalarRegistry[e.Name]
		if !ok {
			return nil, fmt.Errorf("function %s() does not exist", strings.ToLower(e.Name))
		}
		argEvals := make([]exprFunc, len(e.Args))
		for i, arg := range e.Args {
			compiled, err := compileJoinExpr(arg, scope)
			if err != nil {
				return nil, err
			}
			argEvals[i] = compiled
		}
		return func(r storage.Row) any {
			args := make([]any, len(argEvals))
			for i, eval := range argEvals {
				args[i] = eval(r)
			}
			val, _, err := fn(args)
			if err != nil {
				return nil
			}
			return val
		}, nil

	default:
		return nil, fmt.Errorf("unsupported expression type %T in join", expr)
	}
}

func compileJoinBinaryExpr(e *parser.BinaryExpr, scope *joinScope) (exprFunc, error) {
	left, err := compileJoinExpr(e.Left, scope)
	if err != nil {
		return nil, err
	}
	right, err := compileJoinExpr(e.Right, scope)
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
	case "||":
		return func(r storage.Row) any {
			lv, rv := left(r), right(r)
			if lv == nil || rv == nil {
				return nil
			}
			_, lIsStr := lv.(string)
			_, rIsStr := rv.(string)
			if !lIsStr && !rIsStr {
				return nil
			}
			ls, lok := coerceToText(lv)
			rs, rok := coerceToText(rv)
			if !lok || !rok {
				return nil
			}
			return ls + rs
		}, nil
	case "+", "-", "*", "/", "%":
		op := e.Op
		return func(r storage.Row) any {
			lv, rv := left(r), right(r)
			if lv == nil || rv == nil {
				return nil
			}
			// Try integer arithmetic first.
			li, lok := lv.(int64)
			ri, rok := rv.(int64)
			if lok && rok {
				switch op {
				case "+":
					return li + ri
				case "-":
					return li - ri
				case "*":
					return li * ri
				case "/":
					if ri == 0 {
						return nil
					}
					return li / ri
				case "%":
					if ri == 0 {
						return nil
					}
					return li % ri
				}
				return nil
			}
			// Fall back to float arithmetic with int→float promotion.
			lf, lfOk := toFloat64(lv)
			rf, rfOk := toFloat64(rv)
			if !lfOk || !rfOk {
				return nil
			}
			switch op {
			case "+":
				return lf + rf
			case "-":
				return lf - rf
			case "*":
				return lf * rf
			case "/":
				if rf == 0 {
					return nil
				}
				return lf / rf
			case "%":
				if rf == 0 {
					return nil
				}
				return math.Mod(lf, rf)
			}
			return nil
		}, nil

	default:
		return nil, fmt.Errorf("unsupported operator %q", e.Op)
	}
}

// buildJoinFilter compiles an expression into a row filter for joined rows.
func buildJoinFilter(expr parser.Expr, scope *joinScope) (func(storage.Row) bool, error) {
	eval, err := compileJoinExpr(expr, scope)
	if err != nil {
		return nil, err
	}
	return func(r storage.Row) bool {
		v := eval(r)
		b, ok := v.(bool)
		return ok && b
	}, nil
}

// resolveJoinSelectColumns resolves SELECT column expressions against a join scope.
func resolveJoinSelectColumns(exprs []parser.Expr, scope *joinScope) ([]exprFunc, []Column, error) {
	var evals []exprFunc
	var cols []Column

	for _, expr := range exprs {
		alias := ""
		inner := expr
		if a, ok := inner.(*parser.AliasExpr); ok {
			alias = a.Alias
			inner = a.Expr
		}

		switch e := inner.(type) {
		case *parser.StarExpr:
			for i, c := range scope.columns {
				idx := i
				evals = append(evals, func(r storage.Row) any { return storage.RowValue(r.Values, idx) })
				cols = append(cols, Column{
					Name:     c.name,
					TypeOID:  typeOID(c.def.DataType),
					TypeSize: typeSize(c.def.DataType),
				})
			}
		case *parser.ColumnRef:
			idx, err := scope.resolveColumn(e.Table, e.Name)
			if err != nil {
				return nil, nil, err
			}
			c := scope.columns[idx]
			evals = append(evals, func(r storage.Row) any { return storage.RowValue(r.Values, idx) })
			name := c.name
			if alias != "" {
				name = alias
			}
			cols = append(cols, Column{
				Name:     name,
				TypeOID:  typeOID(c.def.DataType),
				TypeSize: typeSize(c.def.DataType),
			})
		case *parser.BinaryExpr:
			compiled, err := compileJoinExpr(inner, scope)
			if err != nil {
				return nil, nil, err
			}
			evals = append(evals, compiled)
			name := "?column?"
			if alias != "" {
				name = alias
			}
			if e.Op == "||" {
				cols = append(cols, Column{Name: name, TypeOID: OIDText, TypeSize: -1})
			} else {
				cols = append(cols, Column{Name: name, TypeOID: OIDInt8, TypeSize: 8})
			}
		case *parser.CastExpr:
			compiled, err := compileJoinExpr(inner, scope)
			if err != nil {
				return nil, nil, err
			}
			evals = append(evals, compiled)
			name := "?column?"
			if alias != "" {
				name = alias
			}
			cols = append(cols, Column{Name: name, TypeOID: castTypeOID(e.TypeName), TypeSize: castTypeSize(e.TypeName)})
		default:
			compiled, err := compileJoinExpr(inner, scope)
			if err != nil {
				return nil, nil, err
			}
			evals = append(evals, compiled)
			name := "?column?"
			if alias != "" {
				name = alias
			}
			cols = append(cols, Column{Name: name, TypeOID: OIDUnknown, TypeSize: -1})
		}
	}
	return evals, cols, nil
}

// execSelectJoin handles SELECT with JOIN clauses using nested-loop execution.
func (e *Executor) execSelectJoin(s *parser.SelectStmt, tr *Trace) (*Result, error) {
	if s.IndexedBy != "" {
		return nil, &QueryError{Code: "0A000", Message: "INDEXED BY is not supported with JOIN"}
	}

	var planStart time.Time
	if tr != nil {
		planStart = time.Now()
	}

	// Validate LIMIT/OFFSET values.
	if s.Limit != nil && *s.Limit < 0 {
		return nil, &QueryError{Code: "2201W", Message: "LIMIT must not be negative"}
	}
	if s.Offset != nil && *s.Offset < 0 {
		return nil, &QueryError{Code: "2201X", Message: "OFFSET must not be negative"}
	}

	// Build the join scope.
	scope, err := e.buildJoinScope(s)
	if err != nil {
		return nil, WrapError(err)
	}

	// Compile ON conditions for each join. Cross-joins (On == nil) have no filter.
	onFilters := make([]func(storage.Row) bool, len(s.Joins))
	for i, j := range s.Joins {
		if j.On == nil {
			continue // implicit cross-join — no ON condition
		}
		f, err := buildJoinFilter(j.On, scope)
		if err != nil {
			return nil, WrapError(err)
		}
		onFilters[i] = f
	}

	// Compile WHERE filter.
	var whereFilter func(storage.Row) bool
	if s.Where != nil {
		whereFilter, err = buildJoinFilter(s.Where, scope)
		if err != nil {
			return nil, WrapError(err)
		}
	}

	// Resolve SELECT columns.
	colEvals, resultCols, err := resolveJoinSelectColumns(s.Columns, scope)
	if err != nil {
		return nil, WrapError(err)
	}

	// Resolve ORDER BY columns against scope.
	type orderKey struct {
		colIdx int
		desc   bool
	}
	var orderKeys []orderKey
	for _, ob := range s.OrderBy {
		idx, err := scope.resolveColumn(ob.Table, ob.Column)
		if err != nil {
			return nil, WrapError(err)
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

	// Collect all rows from each table.
	tableRows := make([][]storage.Row, len(scope.tables))
	var scanned int64
	for i, t := range scope.tables {
		var it storage.RowIterator
		if t.isCatalog {
			it, err = scanCatalogTable(t.schema, t.name, e.engine)
		} else {
			it, err = e.engine.Scan(t.name)
		}
		if err != nil {
			return nil, WrapError(err)
		}
		var rows []storage.Row
		for {
			row, ok := it.Next()
			if !ok {
				break
			}
			rows = append(rows, row)
			scanned++
		}
		it.Close()
		tableRows[i] = rows
	}

	// Nested-loop join: build merged rows.
	var joinLoopStart time.Time
	if tr != nil {
		joinLoopStart = time.Now()
	}

	var matched []storage.Row
	totalCols := len(scope.columns)

	// Recursive function for N-way join.
	var joinLoop func(tableIdx int, current []any)
	joinLoop = func(tableIdx int, current []any) {
		if tableIdx >= len(scope.tables) {
			// All tables joined — we have a complete merged row.
			merged := storage.Row{Values: make([]any, totalCols)}
			copy(merged.Values, current)

			// Apply ON conditions for all joins.
			for _, onFilter := range onFilters {
				if onFilter != nil && !onFilter(merged) {
					return
				}
			}

			// Apply WHERE filter.
			if whereFilter != nil && !whereFilter(merged) {
				return
			}

			matched = append(matched, merged)
			return
		}

		off := scope.tables[tableIdx].offset
		tableCols := scope.tables[tableIdx].def.Columns
		for _, row := range tableRows[tableIdx] {
			// Place this table's values into the merged row.
			for j, col := range tableCols {
				current[off+j] = storage.RowValue(row.Values, col.Ordinal)
			}
			joinLoop(tableIdx+1, current)
		}
	}

	working := make([]any, totalCols)
	joinLoop(0, working)

	if tr != nil {
		tr.JoinLoop = time.Since(joinLoopStart)
	}

	// Apply ORDER BY.
	if len(orderKeys) > 0 {
		var sortStart time.Time
		if tr != nil {
			sortStart = time.Now()
		}
		sort.SliceStable(matched, func(i, j int) bool {
			for _, key := range orderKeys {
				av := storage.RowValue(matched[i].Values, key.colIdx)
				bv := storage.RowValue(matched[j].Values, key.colIdx)
				if av == nil && bv == nil {
					continue
				}
				if av == nil {
					return false
				}
				if bv == nil {
					return true
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
	}

	// Apply OFFSET.
	start := int64(0)
	if s.Offset != nil {
		start = *s.Offset
	}
	if start > int64(len(matched)) {
		start = int64(len(matched))
	}

	// Apply LIMIT.
	end := int64(len(matched))
	if s.Limit != nil && start+*s.Limit < end {
		end = start + *s.Limit
	}

	// Build result rows.
	var resultRows [][][]byte
	for _, row := range matched[start:end] {
		textRow := make([][]byte, len(colEvals))
		for i, eval := range colEvals {
			textRow[i] = formatValue(eval(row))
		}
		resultRows = append(resultRows, textRow)
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

	// If INDEXED BY is specified, wrap the filter to only consider rows from the index lookup.
	if s.IndexedBy != "" {
		rows, err := e.lookupByNamedIndex(s.IndexedBy, s.Where, def)
		if err != nil {
			return nil, err
		}
		if tr != nil {
			tr.IndexName = s.IndexedBy
		}
		idSet := make(map[int64]struct{}, len(rows))
		for _, r := range rows {
			idSet[r.ID] = struct{}{}
		}
		baseFilter := filter
		filter = func(r storage.Row) bool {
			if _, ok := idSet[r.ID]; !ok {
				return false
			}
			if baseFilter != nil {
				return baseFilter(r)
			}
			return true
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

	// If INDEXED BY is specified, wrap the filter to only consider rows from the index lookup.
	if s.IndexedBy != "" {
		rows, err := e.lookupByNamedIndex(s.IndexedBy, s.Where, def)
		if err != nil {
			return nil, err
		}
		if tr != nil {
			tr.IndexName = s.IndexedBy
		}
		idSet := make(map[int64]struct{}, len(rows))
		for _, r := range rows {
			idSet[r.ID] = struct{}{}
		}
		baseFilter := filter
		filter = func(r storage.Row) bool {
			if _, ok := idSet[r.ID]; !ok {
				return false
			}
			if baseFilter != nil {
				return baseFilter(r)
			}
			return true
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
			for _, c := range def.Columns {
				ord := c.Ordinal
				evals = append(evals, func(r storage.Row) any { return storage.RowValue(r.Values, ord) })
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
			c := columnByOrdinal(def, idx)
			evals = append(evals, func(r storage.Row) any { return storage.RowValue(r.Values, idx) })
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
		case *parser.FloatLit:
			v := e.Value
			evals = append(evals, func(r storage.Row) any { return v })
			name := "?column?"
			if alias != "" {
				name = alias
			}
			cols = append(cols, Column{Name: name, TypeOID: OIDFloat8, TypeSize: 8})
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
		case *parser.FunctionCallExpr:
			compiled, err := compileExpr(e, def)
			if err != nil {
				return nil, nil, err
			}
			evals = append(evals, compiled)
			// Get column metadata from the scalar function.
			col := Column{Name: "?column?", TypeOID: OIDUnknown, TypeSize: -1}
			if fn, ok := scalarRegistry[e.Name]; ok {
				if _, meta, err := fn([]any{nil}); err == nil {
					col = meta
				}
			}
			if alias != "" {
				col.Name = alias
			}
			cols = append(cols, col)
		case *parser.BinaryExpr:
			compiled, err := compileExpr(e, def)
			if err != nil {
				return nil, nil, err
			}
			evals = append(evals, compiled)
			name := "?column?"
			if alias != "" {
				name = alias
			}
			if e.Op == "||" {
				cols = append(cols, Column{Name: name, TypeOID: OIDText, TypeSize: -1})
			} else {
				cols = append(cols, Column{Name: name, TypeOID: OIDInt8, TypeSize: 8})
			}
		case *parser.CastExpr:
			compiled, err := compileExpr(e, def)
			if err != nil {
				return nil, nil, err
			}
			evals = append(evals, compiled)
			name := "?column?"
			if alias != "" {
				name = alias
			}
			cols = append(cols, Column{Name: name, TypeOID: castTypeOID(e.TypeName), TypeSize: castTypeSize(e.TypeName)})
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
		return func(r storage.Row) any { return storage.RowValue(r.Values, idx) }, nil

	case *parser.IntegerLit:
		v := e.Value
		return func(storage.Row) any { return v }, nil

	case *parser.FloatLit:
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
			switch n := v.(type) {
			case int64:
				return -n
			case float64:
				return -n
			default:
				return nil
			}
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

	case *parser.LikeExpr:
		return compileLikeExpr(e, func(expr parser.Expr) (exprFunc, error) {
			return compileExpr(expr, def)
		})

	case *parser.InExpr:
		return compileInExpr(e, func(expr parser.Expr) (exprFunc, error) {
			return compileExpr(expr, def)
		})

	case *parser.CastExpr:
		inner, err := compileExpr(e.Expr, def)
		if err != nil {
			return nil, err
		}
		typeName := e.TypeName
		return func(r storage.Row) any { return castValue(inner(r), typeName) }, nil

	case *parser.FunctionCallExpr:
		fn, ok := scalarRegistry[e.Name]
		if !ok {
			return nil, fmt.Errorf("function %s() does not exist", strings.ToLower(e.Name))
		}
		argEvals := make([]exprFunc, len(e.Args))
		for i, arg := range e.Args {
			compiled, err := compileExpr(arg, def)
			if err != nil {
				return nil, err
			}
			argEvals[i] = compiled
		}
		return func(r storage.Row) any {
			args := make([]any, len(argEvals))
			for i, eval := range argEvals {
				args[i] = eval(r)
			}
			val, _, err := fn(args)
			if err != nil {
				return nil
			}
			return val
		}, nil

	default:
		return nil, fmt.Errorf("unsupported expression type %T", expr)
	}
}

// compileLikeExpr compiles a LikeExpr using the provided compile function for
// sub-expressions. This allows reuse between compileExpr and compileJoinExpr.
func compileLikeExpr(e *parser.LikeExpr, compile func(parser.Expr) (exprFunc, error)) (exprFunc, error) {
	valFn, err := compile(e.Expr)
	if err != nil {
		return nil, err
	}
	patFn, err := compile(e.Pattern)
	if err != nil {
		return nil, err
	}

	// Resolve escape rune if ESCAPE clause is present.
	var escRune rune
	hasEscape := e.Escape != nil
	var escFn exprFunc
	if hasEscape {
		if lit, ok := e.Escape.(*parser.StringLit); ok {
			r, escErr := resolveEscapeRune(lit.Value)
			if escErr != nil {
				return nil, escErr
			}
			escRune = r
		} else {
			escFn, err = compile(e.Escape)
			if err != nil {
				return nil, err
			}
		}
	}

	// Static pattern optimization: pre-compile regex if pattern is a string literal.
	if lit, ok := e.Pattern.(*parser.StringLit); ok && escFn == nil {
		re, reErr := likeToRegex(lit.Value, escRune, hasEscape, e.CaseInsensitive)
		if reErr != nil {
			return nil, reErr
		}
		not := e.Not
		return func(r storage.Row) any {
			v := valFn(r)
			if v == nil {
				return nil
			}
			s, ok := v.(string)
			if !ok {
				return nil
			}
			result := re.MatchString(s)
			if not {
				return !result
			}
			return result
		}, nil
	}

	// Dynamic pattern: compile regex per-row.
	not := e.Not
	ci := e.CaseInsensitive
	return func(r storage.Row) any {
		val, pat := valFn(r), patFn(r)
		if val == nil || pat == nil {
			return nil
		}
		vs, vok := val.(string)
		ps, pok := pat.(string)
		if !vok || !pok {
			return nil
		}
		esc := escRune
		he := hasEscape
		if escFn != nil {
			ev := escFn(r)
			if ev == nil {
				return nil
			}
			var escErr error
			esc, escErr = resolveEscapeRune(ev)
			if escErr != nil {
				return nil
			}
			he = true
		}
		re, err := likeToRegex(ps, esc, he, ci)
		if err != nil {
			return nil
		}
		result := re.MatchString(vs)
		if not {
			return !result
		}
		return result
	}, nil
}

// compileInExpr compiles an InExpr using the provided compile function for
// sub-expressions. This allows reuse between compileExpr and compileJoinExpr.
func compileInExpr(e *parser.InExpr, compile func(parser.Expr) (exprFunc, error)) (exprFunc, error) {
	lhsFn, err := compile(e.Expr)
	if err != nil {
		return nil, err
	}
	valFns := make([]exprFunc, len(e.Values))
	for i, v := range e.Values {
		fn, err := compile(v)
		if err != nil {
			return nil, err
		}
		valFns[i] = fn
	}
	not := e.Not
	return func(r storage.Row) any {
		lhs := lhsFn(r)
		if lhs == nil {
			return nil
		}
		hasNull := false
		for _, vFn := range valFns {
			v := vFn(r)
			if v == nil {
				hasNull = true
				continue
			}
			if storage.CompareValues(lhs, v) == 0 {
				return !not
			}
		}
		if hasNull {
			return nil
		}
		return not
	}, nil
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
	case "||":
		return func(r storage.Row) any {
			lv, rv := left(r), right(r)
			if lv == nil || rv == nil {
				return nil
			}
			_, lIsStr := lv.(string)
			_, rIsStr := rv.(string)
			if !lIsStr && !rIsStr {
				return nil
			}
			ls, lok := coerceToText(lv)
			rs, rok := coerceToText(rv)
			if !lok || !rok {
				return nil
			}
			return ls + rs
		}, nil
	case "+", "-", "*", "/", "%":
		op := e.Op
		return func(r storage.Row) any {
			lv, rv := left(r), right(r)
			if lv == nil || rv == nil {
				return nil
			}
			// Try integer arithmetic first.
			li, lok := lv.(int64)
			ri, rok := rv.(int64)
			if lok && rok {
				switch op {
				case "+":
					return li + ri
				case "-":
					return li - ri
				case "*":
					return li * ri
				case "/":
					if ri == 0 {
						return nil
					}
					return li / ri
				case "%":
					if ri == 0 {
						return nil
					}
					return li % ri
				}
				return nil
			}
			// Fall back to float arithmetic with int→float promotion.
			lf, lfOk := toFloat64(lv)
			rf, rfOk := toFloat64(rv)
			if !lfOk || !rfOk {
				return nil
			}
			switch op {
			case "+":
				return lf + rf
			case "-":
				return lf - rf
			case "*":
				return lf * rf
			case "/":
				if rf == 0 {
					return nil
				}
				return lf / rf
			case "%":
				if rf == 0 {
					return nil
				}
				return math.Mod(lf, rf)
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

// extractEqualityValue walks a WHERE tree (descending into AND nodes) to find
// a simple equality predicate of the form col = literal for the given column name.
// Returns the literal value, or nil if not found.
func extractEqualityValue(expr parser.Expr, colName string) any {
	if expr == nil {
		return nil
	}
	switch e := expr.(type) {
	case *parser.BinaryExpr:
		if e.Op == "AND" {
			if v := extractEqualityValue(e.Left, colName); v != nil {
				return v
			}
			return extractEqualityValue(e.Right, colName)
		}
		if e.Op != "=" {
			return nil
		}
		col, lit := extractColumnAndLiteral(e)
		if col == nil || lit == nil {
			return nil
		}
		if !strings.EqualFold(col.Name, colName) {
			return nil
		}
		val, err := evalLiteral(lit)
		if err != nil || val == nil {
			return nil
		}
		return val
	}
	return nil
}

// lookupByNamedIndex validates a named index exists and is applicable to the WHERE clause,
// then performs the index lookup. Returns error if the index is not found or not applicable.
func (e *Executor) lookupByNamedIndex(indexName string, where parser.Expr, def *storage.TableDef) ([]storage.Row, error) {
	// Find the named index in the table definition.
	var found bool
	var idxColumn string
	for _, idx := range def.Indexes {
		if strings.EqualFold(idx.Name, indexName) {
			found = true
			idxColumn = idx.Column
			break
		}
	}
	if !found {
		return nil, &QueryError{Code: "42704", Message: fmt.Sprintf("index %q not found on table %q", indexName, def.Name)}
	}

	if where == nil {
		return nil, &QueryError{Code: "0A000", Message: fmt.Sprintf("INDEXED BY %q requires a WHERE clause with an equality predicate on column %q", indexName, idxColumn)}
	}

	val := extractEqualityValue(where, idxColumn)
	if val == nil {
		return nil, &QueryError{Code: "0A000", Message: fmt.Sprintf("INDEXED BY %q requires an equality predicate on column %q in WHERE clause", indexName, idxColumn)}
	}

	rows, err := e.engine.LookupByIndex(def.Name, indexName, val)
	if err != nil {
		return nil, WrapError(err)
	}
	return rows, nil
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
	case *parser.FloatLit:
		return e.Value, nil
	case *parser.UnaryExpr:
		val, _, err := evalStaticExpr(e)
		return val, err
	case *parser.FunctionCallExpr:
		val, _, err := evalScalarFunction(e)
		return val, err
	case *parser.CastExpr:
		val, err := evalLiteral(e.Expr)
		if err != nil {
			return nil, err
		}
		return castValue(val, e.TypeName), nil
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
	case "TIMESTAMP":
		return storage.TypeTimestamp, nil
	case "FLOAT":
		return storage.TypeFloat, nil
	default:
		return 0, fmt.Errorf("unknown data type %q", s)
	}
}

func columnIndex(def *storage.TableDef, name string) int {
	for _, c := range def.Columns {
		if strings.EqualFold(c.Name, name) {
			return c.Ordinal
		}
	}
	return -1
}

// columnByOrdinal returns the ColumnDef with the given ordinal, or a zero value.
func columnByOrdinal(def *storage.TableDef, ordinal int) storage.ColumnDef {
	for _, c := range def.Columns {
		if c.Ordinal == ordinal {
			return c
		}
	}
	return storage.ColumnDef{}
}

func aggregateTypeOID(funcName string, inputType storage.DataType) int32 {
	switch funcName {
	case "COUNT":
		return OIDInt8
	case "SUM":
		if inputType == storage.TypeFloat {
			return OIDFloat8
		}
		return OIDInt8
	case "AVG":
		return OIDFloat8
	case "MIN", "MAX":
		return typeOID(inputType)
	default:
		return OIDUnknown
	}
}

func aggregateTypeSize(funcName string, inputType storage.DataType) int16 {
	switch funcName {
	case "COUNT", "SUM", "AVG":
		return 8 // int64 and float64 are both 8 bytes
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
	case storage.TypeTimestamp:
		return OIDTimestampTZ
	case storage.TypeFloat:
		return OIDFloat8
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
	case storage.TypeTimestamp:
		return 8
	case storage.TypeFloat:
		return 8
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
	case float64:
		return []byte(strconv.FormatFloat(val, 'g', -1, 64))
	case string:
		return []byte(val)
	case bool:
		if val {
			return []byte("t")
		}
		return []byte("f")
	case time.Time:
		return []byte(val.Format("2006-01-02 15:04:05+00"))
	default:
		return []byte(fmt.Sprintf("%v", v))
	}
}

// toFloat64 converts a numeric value to float64.
// Returns the float64 value and true on success.
func toFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int64:
		return float64(n), true
	default:
		return 0, false
	}
}
