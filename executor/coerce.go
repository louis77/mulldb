package executor

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"mulldb/parser"
	"mulldb/storage"
)

// coerceLiteral converts a Go literal value to the target storage DataType.
// Returns a QueryError with SQLSTATE 22P02 on failure.
func coerceLiteral(val any, target storage.DataType) (any, error) {
	switch target {
	case storage.TypeInteger:
		switch v := val.(type) {
		case int64:
			return v, nil
		case float64:
			if v != math.Trunc(v) {
				return nil, &QueryError{Code: "22P02", Message: fmt.Sprintf("invalid input syntax for type integer: %q", fmt.Sprint(val))}
			}
			return int64(v), nil
		case string:
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, &QueryError{Code: "22P02", Message: fmt.Sprintf("invalid input syntax for type integer: %q", v)}
			}
			return n, nil
		case bool:
			return nil, &QueryError{Code: "22P02", Message: fmt.Sprintf("invalid input syntax for type integer: %q", fmt.Sprint(val))}
		}

	case storage.TypeFloat:
		switch v := val.(type) {
		case float64:
			return v, nil
		case int64:
			return float64(v), nil
		case string:
			f, err := strconv.ParseFloat(v, 64)
			if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
				return nil, &QueryError{Code: "22P02", Message: fmt.Sprintf("invalid input syntax for type float: %q", v)}
			}
			return f, nil
		case bool:
			return nil, &QueryError{Code: "22P02", Message: fmt.Sprintf("invalid input syntax for type float: %q", fmt.Sprint(val))}
		}

	case storage.TypeText:
		switch v := val.(type) {
		case string:
			return v, nil
		case int64:
			return strconv.FormatInt(v, 10), nil
		case float64:
			return strconv.FormatFloat(v, 'f', -1, 64), nil
		case bool:
			if v {
				return "true", nil
			}
			return "false", nil
		}

	case storage.TypeBoolean:
		switch v := val.(type) {
		case bool:
			return v, nil
		case string:
			switch strings.ToLower(v) {
			case "true", "t", "1":
				return true, nil
			case "false", "f", "0":
				return false, nil
			default:
				return nil, &QueryError{Code: "22P02", Message: fmt.Sprintf("invalid input syntax for type boolean: %q", v)}
			}
		default:
			return nil, &QueryError{Code: "22P02", Message: fmt.Sprintf("invalid input syntax for type boolean: %q", fmt.Sprint(val))}
		}

	case storage.TypeTimestamp:
		switch v := val.(type) {
		case string:
			t, err := storage.ParseTimestamp(v)
			if err != nil {
				return nil, &QueryError{Code: "22P02", Message: fmt.Sprintf("invalid input syntax for type timestamp: %q", v)}
			}
			return t, nil
		default:
			return nil, &QueryError{Code: "22P02", Message: fmt.Sprintf("invalid input syntax for type timestamp: %q", fmt.Sprint(val))}
		}
	}

	return nil, &QueryError{Code: "22P02", Message: fmt.Sprintf("cannot cast %T to %s", val, target)}
}

// resolveExprType returns the column's DataType if expr is a ColumnRef that
// resolves in def. Returns (0, false) for non-column expressions.
func resolveExprType(expr parser.Expr, def *storage.TableDef) (storage.DataType, bool) {
	ref, ok := expr.(*parser.ColumnRef)
	if !ok {
		return 0, false
	}
	for _, c := range def.Columns {
		if strings.EqualFold(c.Name, ref.Name) {
			return c.DataType, true
		}
	}
	return 0, false
}

// resolveJoinExprType returns the column's DataType if expr is a ColumnRef
// that resolves in the join scope. Returns (0, false) for non-column expressions.
func resolveJoinExprType(expr parser.Expr, scope *joinScope) (storage.DataType, bool) {
	ref, ok := expr.(*parser.ColumnRef)
	if !ok {
		return 0, false
	}
	idx, err := scope.resolveColumn(ref.Table, ref.Name)
	if err != nil {
		return 0, false
	}
	return scope.columns[idx].def.DataType, true
}

// literalValue returns the Go value of a literal AST node.
// Returns (nil, false) for non-literal expressions.
func literalValue(expr parser.Expr) (any, bool) {
	switch e := expr.(type) {
	case *parser.IntegerLit:
		return e.Value, true
	case *parser.FloatLit:
		return e.Value, true
	case *parser.StringLit:
		return e.Value, true
	case *parser.BoolLit:
		return e.Value, true
	default:
		return nil, false
	}
}

// goTypeMatchesDataType reports whether val is already the correct Go type
// for the given DataType.
func goTypeMatchesDataType(val any, dt storage.DataType) bool {
	switch dt {
	case storage.TypeInteger:
		_, ok := val.(int64)
		return ok
	case storage.TypeFloat:
		_, ok := val.(float64)
		return ok
	case storage.TypeText:
		_, ok := val.(string)
		return ok
	case storage.TypeBoolean:
		_, ok := val.(bool)
		return ok
	case storage.TypeTimestamp:
		// Literals are never time.Time from the parser, so always needs coercion.
		return false
	default:
		return false
	}
}

// tryCoerceOperands checks if one side is a column and the other is a literal,
// and coerces the literal to the column's type at compile time.
// If neither side is a column+literal pair, returns unchanged.
func tryCoerceOperands(
	leftExpr, rightExpr parser.Expr,
	left, right exprFunc,
	def *storage.TableDef,
) (exprFunc, exprFunc, error) {
	// Try left=column, right=literal.
	if colType, ok := resolveExprType(leftExpr, def); ok {
		if litVal, ok := literalValue(rightExpr); ok {
			if !goTypeMatchesDataType(litVal, colType) {
				coerced, err := coerceLiteral(litVal, colType)
				if err != nil {
					return nil, nil, err
				}
				right = func(storage.Row) any { return coerced }
			}
			return left, right, nil
		}
	}
	// Try right=column, left=literal.
	if colType, ok := resolveExprType(rightExpr, def); ok {
		if litVal, ok := literalValue(leftExpr); ok {
			if !goTypeMatchesDataType(litVal, colType) {
				coerced, err := coerceLiteral(litVal, colType)
				if err != nil {
					return nil, nil, err
				}
				left = func(storage.Row) any { return coerced }
			}
			return left, right, nil
		}
	}
	return left, right, nil
}

// tryCoerceJoinOperands is like tryCoerceOperands but resolves columns
// via a joinScope instead of a single TableDef.
func tryCoerceJoinOperands(
	leftExpr, rightExpr parser.Expr,
	left, right exprFunc,
	scope *joinScope,
) (exprFunc, exprFunc, error) {
	if colType, ok := resolveJoinExprType(leftExpr, scope); ok {
		if litVal, ok := literalValue(rightExpr); ok {
			if !goTypeMatchesDataType(litVal, colType) {
				coerced, err := coerceLiteral(litVal, colType)
				if err != nil {
					return nil, nil, err
				}
				right = func(storage.Row) any { return coerced }
			}
			return left, right, nil
		}
	}
	if colType, ok := resolveJoinExprType(rightExpr, scope); ok {
		if litVal, ok := literalValue(leftExpr); ok {
			if !goTypeMatchesDataType(litVal, colType) {
				coerced, err := coerceLiteral(litVal, colType)
				if err != nil {
					return nil, nil, err
				}
				left = func(storage.Row) any { return coerced }
			}
			return left, right, nil
		}
	}
	return left, right, nil
}

// coerceInValues coerces literal values in an IN list to match the column type
// of the LHS expression. Non-literal values are left unchanged.
func coerceInValues(lhsExpr parser.Expr, values []parser.Expr, valFns []exprFunc, def *storage.TableDef) ([]exprFunc, error) {
	colType, ok := resolveExprType(lhsExpr, def)
	if !ok {
		return valFns, nil
	}
	for i, v := range values {
		litVal, ok := literalValue(v)
		if !ok {
			continue
		}
		if goTypeMatchesDataType(litVal, colType) {
			continue
		}
		coerced, err := coerceLiteral(litVal, colType)
		if err != nil {
			return nil, err
		}
		valFns[i] = func(storage.Row) any { return coerced }
	}
	return valFns, nil
}

// coerceJoinInValues is like coerceInValues but resolves columns via joinScope.
func coerceJoinInValues(lhsExpr parser.Expr, values []parser.Expr, valFns []exprFunc, scope *joinScope) ([]exprFunc, error) {
	colType, ok := resolveJoinExprType(lhsExpr, scope)
	if !ok {
		return valFns, nil
	}
	for i, v := range values {
		litVal, ok := literalValue(v)
		if !ok {
			continue
		}
		if goTypeMatchesDataType(litVal, colType) {
			continue
		}
		coerced, err := coerceLiteral(litVal, colType)
		if err != nil {
			return nil, err
		}
		valFns[i] = func(storage.Row) any { return coerced }
	}
	return valFns, nil
}
