package executor

import (
	"fmt"
	"math"
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
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64), true
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

// castValue coerces a Go value to the target SQL type name.
// Returns nil for nil input. Returns the value unchanged if already the right type.
func castValue(v any, typeName string) any {
	if v == nil {
		return nil
	}
	switch typeName {
	case "INTEGER":
		switch x := v.(type) {
		case int64:
			return x
		case float64:
			return int64(x)
		case bool:
			if x {
				return int64(1)
			}
			return int64(0)
		case string:
			n, err := strconv.ParseInt(x, 10, 64)
			if err != nil {
				return nil
			}
			return n
		}
	case "TEXT":
		s, ok := coerceToText(v)
		if ok {
			return s
		}
	case "BOOLEAN":
		switch x := v.(type) {
		case bool:
			return x
		case int64:
			return x != 0
		case string:
			switch strings.ToLower(x) {
			case "true", "t", "yes", "on", "1":
				return true
			case "false", "f", "no", "off", "0":
				return false
			}
		}
	case "FLOAT":
		switch x := v.(type) {
		case float64:
			return x
		case int64:
			return float64(x)
		case string:
			f, err := strconv.ParseFloat(x, 64)
			if err != nil {
				return nil
			}
			return f
		}
	}
	return v
}

func castTypeOID(typeName string) int32 {
	switch typeName {
	case "INTEGER":
		return OIDInt8
	case "TEXT":
		return OIDText
	case "BOOLEAN":
		return OIDBool
	case "FLOAT":
		return OIDFloat8
	case "TIMESTAMP":
		return OIDTimestampTZ
	default:
		return OIDUnknown
	}
}

func castTypeSize(typeName string) int16 {
	switch typeName {
	case "INTEGER":
		return 8
	case "BOOLEAN":
		return 1
	case "FLOAT":
		return 8
	default:
		return -1
	}
}

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
	case *parser.FloatLit:
		return e.Value, Column{Name: "?column?", TypeOID: OIDFloat8, TypeSize: 8}, nil
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
	case *parser.CastExpr:
		val, col, err := evalStaticExpr(e.Expr)
		if err != nil {
			return nil, Column{}, err
		}
		val = castValue(val, e.TypeName)
		col.TypeOID = castTypeOID(e.TypeName)
		col.TypeSize = castTypeSize(e.TypeName)
		return val, col, nil
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

	if lv == nil || rv == nil {
		return nil, Column{Name: "?column?", TypeOID: OIDInt8, TypeSize: 8}, nil
	}

	// Try integer arithmetic first.
	li, lok := lv.(int64)
	ri, rok := rv.(int64)
	if lok && rok {
		col := Column{Name: "?column?", TypeOID: OIDInt8, TypeSize: 8}
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

	// Fall back to float arithmetic with int→float promotion.
	lf, lfOk := toFloat64(lv)
	rf, rfOk := toFloat64(rv)
	if !lfOk || !rfOk {
		return nil, Column{}, &QueryError{
			Code:    "42883",
			Message: fmt.Sprintf("operator %s is not defined for the given types", e.Op),
		}
	}
	col := Column{Name: "?column?", TypeOID: OIDFloat8, TypeSize: 8}
	switch e.Op {
	case "+":
		return lf + rf, col, nil
	case "-":
		return lf - rf, col, nil
	case "*":
		return lf * rf, col, nil
	case "/":
		if rf == 0 {
			return nil, Column{}, &QueryError{Code: "22012", Message: "division by zero"}
		}
		return lf / rf, col, nil
	case "%":
		if rf == 0 {
			return nil, Column{}, &QueryError{Code: "22012", Message: "division by zero"}
		}
		return math.Mod(lf, rf), col, nil
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
	if v == nil {
		return nil, Column{Name: "?column?", TypeOID: OIDInt8, TypeSize: 8}, nil
	}
	switch n := v.(type) {
	case int64:
		return -n, Column{Name: "?column?", TypeOID: OIDInt8, TypeSize: 8}, nil
	case float64:
		return -n, Column{Name: "?column?", TypeOID: OIDFloat8, TypeSize: 8}, nil
	default:
		return nil, Column{}, &QueryError{
			Code:    "42883",
			Message: "unary minus is not defined for the given type",
		}
	}
}
