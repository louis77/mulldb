package executor

import (
	"fmt"
	"strings"

	"mulldb/parser"
)

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
