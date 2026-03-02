package executor

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"mulldb/parser"
	"mulldb/storage"
)

// correlatedFunc evaluates an expression against both inner and outer rows.
type correlatedFunc func(innerRow, outerRow storage.Row) any

// compileNestColumn compiles a NestExpr into an exprFunc that executes a
// correlated subquery for each outer row and returns the formatted result.
func (e *Executor) compileNestColumn(nest *parser.NestExpr, outerDef *storage.TableDef, outerAlias string) (exprFunc, Column, error) {
	q := nest.Query

	// Validate: must have FROM.
	if q.From.IsEmpty() {
		return nil, Column{}, &QueryError{Code: "42601", Message: "NEST subquery must have a FROM clause"}
	}
	// No JOINs in inner SELECT.
	if len(q.Joins) > 0 {
		return nil, Column{}, &QueryError{Code: "0A000", Message: "NEST subquery does not support JOINs"}
	}
	// No GROUP BY.
	if len(q.GroupBy) > 0 {
		return nil, Column{}, &QueryError{Code: "0A000", Message: "NEST subquery does not support GROUP BY"}
	}

	// Look up inner table.
	innerDef, ok := e.engine.GetTable(q.From.Name)
	if !ok {
		return nil, Column{}, WrapError(&storage.TableNotFoundError{Name: q.From.String()})
	}
	innerAlias := q.FromAlias
	if innerAlias == "" {
		innerAlias = q.From.Name
	}
	if outerAlias == "" {
		outerAlias = outerDef.Name
	}

	// Compile inner SELECT columns and capture column names.
	var innerColFns []correlatedFunc
	var innerColNames []string
	for _, col := range q.Columns {
		inner := col
		name := ""
		if a, ok := inner.(*parser.AliasExpr); ok {
			name = a.Alias
			inner = a.Expr
		}
		if name == "" {
			if ref, ok := inner.(*parser.ColumnRef); ok {
				name = ref.Name
			} else {
				name = "?column?"
			}
		}
		// Reject nested NEST.
		if _, ok := inner.(*parser.NestExpr); ok {
			return nil, Column{}, &QueryError{Code: "0A000", Message: "nested NEST is not supported"}
		}
		compiled, err := compileCorrelatedExpr(inner, innerDef, innerAlias, outerDef, outerAlias)
		if err != nil {
			return nil, Column{}, err
		}
		innerColFns = append(innerColFns, compiled)
		innerColNames = append(innerColNames, name)
	}

	// Compile inner WHERE.
	var filterFn correlatedFunc
	if q.Where != nil {
		var err error
		filterFn, err = compileCorrelatedExpr(q.Where, innerDef, innerAlias, outerDef, outerAlias)
		if err != nil {
			return nil, Column{}, err
		}
	}

	// Compile ORDER BY for inner query.
	type orderKey struct {
		colIdx int
		desc   bool
	}
	var orderKeys []orderKey
	for _, ob := range q.OrderBy {
		idx := columnIndex(innerDef, ob.Column)
		if idx < 0 {
			return nil, Column{}, WrapError(fmt.Errorf("column %q not found in table %q", ob.Column, innerDef.Name))
		}
		orderKeys = append(orderKeys, orderKey{colIdx: idx, desc: ob.Desc})
	}

	innerLimit := q.Limit
	innerOffset := q.Offset
	numInnerCols := len(innerColFns)
	innerTableName := q.From.Name
	eng := e.engine
	format := nest.Format

	eval := func(outerRow storage.Row) any {
		iter, err := eng.Scan(innerTableName)
		if err != nil {
			return nil
		}
		defer iter.Close()

		type matchedRow struct {
			innerRow storage.Row
			values   []any
		}
		var matches []matchedRow

		for {
			innerRow, ok := iter.Next()
			if !ok {
				break
			}
			if filterFn != nil {
				v := filterFn(innerRow, outerRow)
				b, ok := v.(bool)
				if !ok || !b {
					continue
				}
			}
			vals := make([]any, numInnerCols)
			for i, fn := range innerColFns {
				vals[i] = fn(innerRow, outerRow)
			}
			matches = append(matches, matchedRow{innerRow: innerRow, values: vals})
		}

		// Sort by ORDER BY keys.
		if len(orderKeys) > 0 {
			sort.SliceStable(matches, func(i, j int) bool {
				ri := matches[i].innerRow
				rj := matches[j].innerRow
				for _, k := range orderKeys {
					vi := storage.RowValue(ri.Values, k.colIdx)
					vj := storage.RowValue(rj.Values, k.colIdx)
					cmp := storage.CompareValues(vi, vj)
					if cmp == 0 || cmp == -2 {
						continue
					}
					if k.desc {
						return cmp > 0
					}
					return cmp < 0
				}
				return false
			})
		}

		// Apply OFFSET.
		if innerOffset != nil && *innerOffset > 0 {
			off := int(*innerOffset)
			if off >= len(matches) {
				matches = nil
			} else {
				matches = matches[off:]
			}
		}
		// Apply LIMIT.
		if innerLimit != nil {
			lim := int(*innerLimit)
			if lim < len(matches) {
				matches = matches[:lim]
			}
		}

		if len(matches) == 0 {
			return nil // SQL NULL
		}

		rows := make([][]any, len(matches))
		for i, m := range matches {
			rows[i] = m.values
		}
		switch format {
		case "JSON":
			return string(formatNestJSON(rows, innerColNames))
		case "JSONA":
			return string(formatNestJSONA(rows))
		default:
			return string(formatNest(rows))
		}
	}

	col := Column{Name: "nest", TypeOID: OIDText, TypeSize: -1}
	return eval, col, nil
}

// compileCorrelatedExpr compiles an expression that can reference both inner and outer table columns.
func compileCorrelatedExpr(expr parser.Expr, innerDef *storage.TableDef, innerAlias string, outerDef *storage.TableDef, outerAlias string) (correlatedFunc, error) {
	switch e := expr.(type) {
	case *parser.ColumnRef:
		return resolveCorrelatedColumn(e, innerDef, innerAlias, outerDef, outerAlias)

	case *parser.IntegerLit:
		v := e.Value
		return func(_, _ storage.Row) any { return v }, nil

	case *parser.FloatLit:
		v := e.Value
		return func(_, _ storage.Row) any { return v }, nil

	case *parser.StringLit:
		v := e.Value
		return func(_, _ storage.Row) any { return v }, nil

	case *parser.BoolLit:
		v := e.Value
		return func(_, _ storage.Row) any { return v }, nil

	case *parser.NullLit:
		return func(_, _ storage.Row) any { return nil }, nil

	case *parser.BinaryExpr:
		return compileCorrelatedBinaryExpr(e, innerDef, innerAlias, outerDef, outerAlias)

	case *parser.IsNullExpr:
		inner, err := compileCorrelatedExpr(e.Expr, innerDef, innerAlias, outerDef, outerAlias)
		if err != nil {
			return nil, err
		}
		if e.Not {
			return func(ir, or storage.Row) any { return inner(ir, or) != nil }, nil
		}
		return func(ir, or storage.Row) any { return inner(ir, or) == nil }, nil

	case *parser.NotExpr:
		inner, err := compileCorrelatedExpr(e.Expr, innerDef, innerAlias, outerDef, outerAlias)
		if err != nil {
			return nil, err
		}
		return func(ir, or storage.Row) any {
			v, ok := inner(ir, or).(bool)
			if !ok {
				return nil
			}
			return !v
		}, nil

	case *parser.UnaryExpr:
		inner, err := compileCorrelatedExpr(e.Expr, innerDef, innerAlias, outerDef, outerAlias)
		if err != nil {
			return nil, err
		}
		return func(ir, or storage.Row) any {
			v := inner(ir, or)
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

	case *parser.LikeExpr:
		return compileCorrelatedLikeExpr(e, innerDef, innerAlias, outerDef, outerAlias)

	case *parser.InExpr:
		return compileCorrelatedInExpr(e, innerDef, innerAlias, outerDef, outerAlias)

	case *parser.BetweenExpr:
		return compileCorrelatedBetweenExpr(e, innerDef, innerAlias, outerDef, outerAlias)

	case *parser.FunctionCallExpr:
		fn, ok := scalarRegistry[e.Name]
		if !ok {
			return nil, fmt.Errorf("function %s() does not exist", strings.ToLower(e.Name))
		}
		argEvals := make([]correlatedFunc, len(e.Args))
		for i, arg := range e.Args {
			compiled, err := compileCorrelatedExpr(arg, innerDef, innerAlias, outerDef, outerAlias)
			if err != nil {
				return nil, err
			}
			argEvals[i] = compiled
		}
		return func(ir, or storage.Row) any {
			args := make([]any, len(argEvals))
			for i, eval := range argEvals {
				args[i] = eval(ir, or)
			}
			val, _, err := fn(args)
			if err != nil {
				return nil
			}
			return val
		}, nil

	case *parser.CastExpr:
		inner, err := compileCorrelatedExpr(e.Expr, innerDef, innerAlias, outerDef, outerAlias)
		if err != nil {
			return nil, err
		}
		typeName := e.TypeName
		return func(ir, or storage.Row) any { return castValue(inner(ir, or), typeName) }, nil

	default:
		return nil, fmt.Errorf("unsupported expression type %T in NEST subquery", expr)
	}
}

// resolveCorrelatedColumn resolves a ColumnRef against inner or outer table.
func resolveCorrelatedColumn(ref *parser.ColumnRef, innerDef *storage.TableDef, innerAlias string, outerDef *storage.TableDef, outerAlias string) (correlatedFunc, error) {
	if ref.Table != "" {
		// Qualified: resolve by alias/table name.
		if strings.EqualFold(ref.Table, innerAlias) || strings.EqualFold(ref.Table, innerDef.Name) {
			idx := columnIndex(innerDef, ref.Name)
			if idx < 0 {
				return nil, fmt.Errorf("column %q not found in table %q", ref.Name, innerDef.Name)
			}
			return func(ir, _ storage.Row) any { return storage.RowValue(ir.Values, idx) }, nil
		}
		if strings.EqualFold(ref.Table, outerAlias) || strings.EqualFold(ref.Table, outerDef.Name) {
			idx := columnIndex(outerDef, ref.Name)
			if idx < 0 {
				return nil, fmt.Errorf("column %q not found in table %q", ref.Name, outerDef.Name)
			}
			return func(_, or storage.Row) any { return storage.RowValue(or.Values, idx) }, nil
		}
		return nil, fmt.Errorf("table %q not found in NEST subquery context", ref.Table)
	}

	// Unqualified: try inner first, then outer.
	if idx := columnIndex(innerDef, ref.Name); idx >= 0 {
		return func(ir, _ storage.Row) any { return storage.RowValue(ir.Values, idx) }, nil
	}
	if idx := columnIndex(outerDef, ref.Name); idx >= 0 {
		return func(_, or storage.Row) any { return storage.RowValue(or.Values, idx) }, nil
	}
	return nil, fmt.Errorf("column %q not found in NEST subquery context", ref.Name)
}

// compileCorrelatedBinaryExpr compiles a binary expression in correlated context.
func compileCorrelatedBinaryExpr(e *parser.BinaryExpr, innerDef *storage.TableDef, innerAlias string, outerDef *storage.TableDef, outerAlias string) (correlatedFunc, error) {
	leftFn, err := compileCorrelatedExpr(e.Left, innerDef, innerAlias, outerDef, outerAlias)
	if err != nil {
		return nil, err
	}
	rightFn, err := compileCorrelatedExpr(e.Right, innerDef, innerAlias, outerDef, outerAlias)
	if err != nil {
		return nil, err
	}

	switch e.Op {
	case "AND":
		return func(ir, or storage.Row) any {
			lv, lok := leftFn(ir, or).(bool)
			if lok && !lv {
				return false
			}
			rv, rok := rightFn(ir, or).(bool)
			if rok && !rv {
				return false
			}
			if !lok || !rok {
				return nil
			}
			return true
		}, nil

	case "OR":
		return func(ir, or storage.Row) any {
			lv, lok := leftFn(ir, or).(bool)
			if lok && lv {
				return true
			}
			rv, rok := rightFn(ir, or).(bool)
			if rok && rv {
				return true
			}
			if !lok || !rok {
				return nil
			}
			return false
		}, nil

	case "||":
		return func(ir, or storage.Row) any {
			lv := leftFn(ir, or)
			rv := rightFn(ir, or)
			if lv == nil || rv == nil {
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
		return compileCorrelatedArithmeticExpr(e.Op, leftFn, rightFn), nil

	case "=", "!=", "<", ">", "<=", ">=":
		return func(ir, or storage.Row) any {
			lv := leftFn(ir, or)
			rv := rightFn(ir, or)
			if lv == nil || rv == nil {
				return nil
			}
			cmp := storage.CompareValues(lv, rv)
			if cmp == -2 {
				return nil
			}
			switch e.Op {
			case "=":
				return cmp == 0
			case "!=":
				return cmp != 0
			case "<":
				return cmp < 0
			case ">":
				return cmp > 0
			case "<=":
				return cmp <= 0
			case ">=":
				return cmp >= 0
			}
			return nil
		}, nil

	default:
		return nil, fmt.Errorf("unsupported operator %q in NEST subquery", e.Op)
	}
}

// compileCorrelatedArithmeticExpr compiles arithmetic operators in correlated context.
func compileCorrelatedArithmeticExpr(op string, leftFn, rightFn correlatedFunc) correlatedFunc {
	return func(ir, or storage.Row) any {
		lv := leftFn(ir, or)
		rv := rightFn(ir, or)
		if lv == nil || rv == nil {
			return nil
		}
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
		}
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
			return lf - rf*float64(int64(lf/rf))
		}
		return nil
	}
}

// compileCorrelatedLikeExpr compiles a LIKE/ILIKE in correlated context.
func compileCorrelatedLikeExpr(e *parser.LikeExpr, innerDef *storage.TableDef, innerAlias string, outerDef *storage.TableDef, outerAlias string) (correlatedFunc, error) {
	valFn, err := compileCorrelatedExpr(e.Expr, innerDef, innerAlias, outerDef, outerAlias)
	if err != nil {
		return nil, err
	}
	patFn, err := compileCorrelatedExpr(e.Pattern, innerDef, innerAlias, outerDef, outerAlias)
	if err != nil {
		return nil, err
	}
	var escapeFn correlatedFunc
	if e.Escape != nil {
		escapeFn, err = compileCorrelatedExpr(e.Escape, innerDef, innerAlias, outerDef, outerAlias)
		if err != nil {
			return nil, err
		}
	}
	not := e.Not
	ci := e.CaseInsensitive

	return func(ir, or storage.Row) any {
		val := valFn(ir, or)
		pat := patFn(ir, or)
		if val == nil || pat == nil {
			return nil
		}
		vs, vok := val.(string)
		ps, pok := pat.(string)
		if !vok || !pok {
			return nil
		}
		var escChar rune
		hasEscape := false
		if escapeFn != nil {
			ev := escapeFn(ir, or)
			r, err := resolveEscapeRune(ev)
			if err != nil {
				return nil
			}
			escChar = r
			hasEscape = true
		}
		re, err := likeToRegex(ps, escChar, hasEscape, ci)
		if err != nil {
			return nil
		}
		result := re.MatchString(vs)
		if not {
			result = !result
		}
		return result
	}, nil
}

// compileCorrelatedInExpr compiles an IN expression in correlated context.
func compileCorrelatedInExpr(e *parser.InExpr, innerDef *storage.TableDef, innerAlias string, outerDef *storage.TableDef, outerAlias string) (correlatedFunc, error) {
	exprFn, err := compileCorrelatedExpr(e.Expr, innerDef, innerAlias, outerDef, outerAlias)
	if err != nil {
		return nil, err
	}
	valFns := make([]correlatedFunc, len(e.Values))
	for i, v := range e.Values {
		valFns[i], err = compileCorrelatedExpr(v, innerDef, innerAlias, outerDef, outerAlias)
		if err != nil {
			return nil, err
		}
	}
	not := e.Not

	return func(ir, or storage.Row) any {
		lhs := exprFn(ir, or)
		if lhs == nil {
			return nil
		}
		hasNull := false
		for _, vfn := range valFns {
			rv := vfn(ir, or)
			if rv == nil {
				hasNull = true
				continue
			}
			if storage.CompareValues(lhs, rv) == 0 {
				if not {
					return false
				}
				return true
			}
		}
		if hasNull {
			return nil
		}
		return not
	}, nil
}

// compileCorrelatedBetweenExpr compiles a BETWEEN expression in correlated context.
func compileCorrelatedBetweenExpr(e *parser.BetweenExpr, innerDef *storage.TableDef, innerAlias string, outerDef *storage.TableDef, outerAlias string) (correlatedFunc, error) {
	exprFn, err := compileCorrelatedExpr(e.Expr, innerDef, innerAlias, outerDef, outerAlias)
	if err != nil {
		return nil, err
	}
	lowFn, err := compileCorrelatedExpr(e.Low, innerDef, innerAlias, outerDef, outerAlias)
	if err != nil {
		return nil, err
	}
	highFn, err := compileCorrelatedExpr(e.High, innerDef, innerAlias, outerDef, outerAlias)
	if err != nil {
		return nil, err
	}
	not := e.Not

	return func(ir, or storage.Row) any {
		v := exprFn(ir, or)
		lo := lowFn(ir, or)
		hi := highFn(ir, or)
		if v == nil || lo == nil || hi == nil {
			return nil
		}
		cmpLo := storage.CompareValues(v, lo)
		if cmpLo == -2 {
			return nil
		}
		cmpHi := storage.CompareValues(v, hi)
		if cmpHi == -2 {
			return nil
		}
		result := cmpLo >= 0 && cmpHi <= 0
		if not {
			result = !result
		}
		return result
	}, nil
}

// formatNest formats NEST result rows as text.
// Empty → nil (SQL NULL).
// Single column: (val1, val2, ...)
// Multiple columns: ((v1a, v1b), (v2a, v2b))
func formatNest(rows [][]any) []byte {
	if len(rows) == 0 {
		return nil
	}

	var b strings.Builder
	numCols := len(rows[0])

	if numCols == 1 {
		b.WriteByte('(')
		for i, row := range rows {
			if i > 0 {
				b.WriteString(", ")
			}
			formatNestValue(&b, row[0])
		}
		b.WriteByte(')')
	} else {
		b.WriteByte('(')
		for i, row := range rows {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteByte('(')
			for j, val := range row {
				if j > 0 {
					b.WriteString(", ")
				}
				formatNestValue(&b, val)
			}
			b.WriteByte(')')
		}
		b.WriteByte(')')
	}
	return []byte(b.String())
}

// formatNestValue writes a single value to the builder.
func formatNestValue(b *strings.Builder, v any) {
	if v == nil {
		b.WriteString("NULL")
		return
	}
	b.Write(formatValue(v))
}

// nestJSONValue converts a storage value to a JSON-compatible value.
func nestJSONValue(v any) any {
	if v == nil {
		return nil
	}
	switch val := v.(type) {
	case int64, float64, string, bool:
		return val
	case time.Time:
		return val.Format(time.RFC3339)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// formatNestJSON formats rows as a JSON array of objects with column names as keys.
func formatNestJSON(rows [][]any, colNames []string) []byte {
	arr := make([]map[string]any, len(rows))
	for i, row := range rows {
		obj := make(map[string]any, len(colNames))
		for j, name := range colNames {
			obj[name] = nestJSONValue(row[j])
		}
		arr[i] = obj
	}
	b, _ := json.Marshal(arr)
	return b
}

// formatNestJSONA formats rows as a JSON array of arrays (positional, no column names).
func formatNestJSONA(rows [][]any) []byte {
	arr := make([][]any, len(rows))
	for i, row := range rows {
		converted := make([]any, len(row))
		for j, v := range row {
			converted[j] = nestJSONValue(v)
		}
		arr[i] = converted
	}
	b, _ := json.Marshal(arr)
	return b
}
