package executor

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"mulldb/parser"
)

// coerceToText converts a Go value to its text representation.
// Returns the string and true on success, or "" and false for unsupported types.
func coerceToText(v any) (string, bool) {
	switch x := v.(type) {
	case string:
		return x, true
	case int64:
		return strconv.FormatInt(x, 10), true
	case bool:
		if x {
			return "true", true
		}
		return "false", true
	case time.Time:
		return x.Format("2006-01-02 15:04:05+00"), true
	default:
		return "", false
	}
}

// ScalarFunc is the signature all registered scalar functions must implement.
// args contains pre-evaluated argument values (nil = SQL NULL).
// Returns the result value and its column descriptor.
type ScalarFunc func(args []any) (any, Column, error)

var scalarRegistry = map[string]ScalarFunc{}

// RegisterScalar registers a scalar function by name (case-insensitive).
func RegisterScalar(name string, fn ScalarFunc) {
	scalarRegistry[strings.ToUpper(name)] = fn
}

// evalStaticExpr evaluates a single expression with no row context (no table).
func evalStaticExpr(expr parser.Expr) (any, Column, error) {
	switch e := expr.(type) {
	case *parser.IntegerLit:
		return e.Value, Column{Name: "?column?", TypeOID: OIDInt8, TypeSize: 8}, nil
	case *parser.StringLit:
		return e.Value, Column{Name: "?column?", TypeOID: OIDText, TypeSize: -1}, nil
	case *parser.BoolLit:
		return e.Value, Column{Name: "?column?", TypeOID: OIDBool, TypeSize: 1}, nil
	case *parser.NullLit:
		return nil, Column{Name: "?column?", TypeOID: OIDUnknown, TypeSize: -1}, nil
	case *parser.FunctionCallExpr:
		return evalScalarFunction(e)
	case *parser.BinaryExpr:
		return evalStaticBinaryExpr(e)
	case *parser.UnaryExpr:
		return evalStaticUnaryExpr(e)
	default:
		return nil, Column{}, &QueryError{
			Code:    "42601",
			Message: fmt.Sprintf("expression %T requires a FROM clause", expr),
		}
	}
}

// evalScalarFunction looks up a registered scalar function and calls it with
// pre-evaluated arguments.
func evalScalarFunction(e *parser.FunctionCallExpr) (any, Column, error) {
	fn, ok := scalarRegistry[e.Name] // parser already uppercases function names
	if !ok {
		return nil, Column{}, &QueryError{
			Code:    "42883",
			Message: fmt.Sprintf("function %s() does not exist", strings.ToLower(e.Name)),
		}
	}

	args := make([]any, len(e.Args))
	for i, argExpr := range e.Args {
		val, _, err := evalStaticExpr(argExpr)
		if err != nil {
			return nil, Column{}, err
		}
		args[i] = val
	}

	return fn(args)
}

func evalStaticBinaryExpr(e *parser.BinaryExpr) (any, Column, error) {
	lv, _, err := evalStaticExpr(e.Left)
	if err != nil {
		return nil, Column{}, err
	}
	rv, _, err := evalStaticExpr(e.Right)
	if err != nil {
		return nil, Column{}, err
	}

	if e.Op == "||" {
		col := Column{Name: "?column?", TypeOID: OIDText, TypeSize: -1}
		if lv == nil || rv == nil {
			return nil, col, nil
		}
		_, lIsStr := lv.(string)
		_, rIsStr := rv.(string)
		if !lIsStr && !rIsStr {
			return nil, Column{}, &QueryError{
				Code:    "42883",
				Message: "operator || is not defined for the given types",
			}
		}
		ls, lok := coerceToText(lv)
		rs, rok := coerceToText(rv)
		if !lok || !rok {
			return nil, Column{}, &QueryError{
				Code:    "42883",
				Message: "operator || is not defined for the given types",
			}
		}
		return ls + rs, col, nil
	}

	col := Column{Name: "?column?", TypeOID: OIDInt8, TypeSize: 8}
	if lv == nil || rv == nil {
		return nil, col, nil
	}
	li, lok := lv.(int64)
	ri, rok := rv.(int64)
	if !lok || !rok {
		return nil, Column{}, &QueryError{
			Code:    "42883",
			Message: fmt.Sprintf("operator %s is not defined for the given types", e.Op),
		}
	}
	switch e.Op {
	case "+":
		return li + ri, col, nil
	case "-":
		return li - ri, col, nil
	case "*":
		return li * ri, col, nil
	case "/":
		if ri == 0 {
			return nil, Column{}, &QueryError{Code: "22012", Message: "division by zero"}
		}
		return li / ri, col, nil
	case "%":
		if ri == 0 {
			return nil, Column{}, &QueryError{Code: "22012", Message: "division by zero"}
		}
		return li % ri, col, nil
	default:
		return nil, Column{}, &QueryError{
			Code:    "42601",
			Message: fmt.Sprintf("operator %q not supported in static context", e.Op),
		}
	}
}

func evalStaticUnaryExpr(e *parser.UnaryExpr) (any, Column, error) {
	v, _, err := evalStaticExpr(e.Expr)
	if err != nil {
		return nil, Column{}, err
	}
	col := Column{Name: "?column?", TypeOID: OIDInt8, TypeSize: 8}
	if v == nil {
		return nil, col, nil
	}
	iv, ok := v.(int64)
	if !ok {
		return nil, Column{}, &QueryError{
			Code:    "42883",
			Message: "unary minus is not defined for the given type",
		}
	}
	return -iv, col, nil
}
