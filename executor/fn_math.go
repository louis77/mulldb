package executor

import (
	"fmt"
	"math"
)

func init() {
	RegisterScalar("ABS", fnAbs)
	RegisterScalar("ROUND", fnRound)
	RegisterScalar("CEIL", fnCeil)
	RegisterScalar("CEILING", fnCeil)
	RegisterScalar("FLOOR", fnFloor)
	RegisterScalar("POWER", fnPower)
	RegisterScalar("POW", fnPower)
	RegisterScalar("SQRT", fnSqrt)
	RegisterScalar("MOD", fnMod)
}

var floatCol = Column{Name: "?column?", TypeOID: OIDFloat8, TypeSize: 8}
var intCol = Column{Name: "?column?", TypeOID: OIDInt8, TypeSize: 8}

func fnAbs(args []any) (any, Column, error) {
	if len(args) != 1 {
		return nil, Column{}, &QueryError{Code: "42883", Message: "ABS() takes exactly 1 argument"}
	}
	if args[0] == nil {
		return nil, floatCol, nil
	}
	switch v := args[0].(type) {
	case int64:
		if v < 0 {
			return -v, intCol, nil
		}
		return v, intCol, nil
	case float64:
		return math.Abs(v), floatCol, nil
	default:
		return nil, Column{}, &QueryError{Code: "42883", Message: "ABS() requires a numeric argument"}
	}
}

func fnRound(args []any) (any, Column, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, Column{}, &QueryError{Code: "42883", Message: "ROUND() takes 1 or 2 arguments"}
	}
	if args[0] == nil {
		return nil, floatCol, nil
	}
	x, ok := toFloat64(args[0])
	if !ok {
		return nil, Column{}, &QueryError{Code: "42883", Message: "ROUND() requires a numeric argument"}
	}
	if len(args) == 1 {
		return math.Round(x), floatCol, nil
	}
	// ROUND(x, n) — round to n decimal places.
	if args[1] == nil {
		return nil, floatCol, nil
	}
	n, ok := args[1].(int64)
	if !ok {
		return nil, Column{}, &QueryError{Code: "42883", Message: "ROUND() second argument must be an integer"}
	}
	p := math.Pow(10, float64(n))
	return math.Round(x*p) / p, floatCol, nil
}

func fnCeil(args []any) (any, Column, error) {
	if len(args) != 1 {
		return nil, Column{}, &QueryError{Code: "42883", Message: "CEIL() takes exactly 1 argument"}
	}
	if args[0] == nil {
		return nil, floatCol, nil
	}
	x, ok := toFloat64(args[0])
	if !ok {
		return nil, Column{}, &QueryError{Code: "42883", Message: "CEIL() requires a numeric argument"}
	}
	return math.Ceil(x), floatCol, nil
}

func fnFloor(args []any) (any, Column, error) {
	if len(args) != 1 {
		return nil, Column{}, &QueryError{Code: "42883", Message: "FLOOR() takes exactly 1 argument"}
	}
	if args[0] == nil {
		return nil, floatCol, nil
	}
	x, ok := toFloat64(args[0])
	if !ok {
		return nil, Column{}, &QueryError{Code: "42883", Message: "FLOOR() requires a numeric argument"}
	}
	return math.Floor(x), floatCol, nil
}

func fnPower(args []any) (any, Column, error) {
	if len(args) != 2 {
		return nil, Column{}, &QueryError{Code: "42883", Message: "POWER() takes exactly 2 arguments"}
	}
	if args[0] == nil || args[1] == nil {
		return nil, floatCol, nil
	}
	base, ok := toFloat64(args[0])
	if !ok {
		return nil, Column{}, &QueryError{Code: "42883", Message: "POWER() requires numeric arguments"}
	}
	exp, ok := toFloat64(args[1])
	if !ok {
		return nil, Column{}, &QueryError{Code: "42883", Message: "POWER() requires numeric arguments"}
	}
	return math.Pow(base, exp), floatCol, nil
}

func fnSqrt(args []any) (any, Column, error) {
	if len(args) != 1 {
		return nil, Column{}, &QueryError{Code: "42883", Message: "SQRT() takes exactly 1 argument"}
	}
	if args[0] == nil {
		return nil, floatCol, nil
	}
	x, ok := toFloat64(args[0])
	if !ok {
		return nil, Column{}, &QueryError{Code: "42883", Message: "SQRT() requires a numeric argument"}
	}
	if x < 0 {
		return nil, Column{}, &QueryError{
			Code:    "2201F",
			Message: fmt.Sprintf("cannot take square root of a negative number: %g", x),
		}
	}
	return math.Sqrt(x), floatCol, nil
}

func fnMod(args []any) (any, Column, error) {
	if len(args) != 2 {
		return nil, Column{}, &QueryError{Code: "42883", Message: "MOD() takes exactly 2 arguments"}
	}
	if args[0] == nil || args[1] == nil {
		return nil, intCol, nil
	}
	// Try integer MOD first.
	li, lok := args[0].(int64)
	ri, rok := args[1].(int64)
	if lok && rok {
		if ri == 0 {
			return nil, Column{}, &QueryError{Code: "22012", Message: "division by zero"}
		}
		return li % ri, intCol, nil
	}
	// Fall back to float MOD.
	lf, lfOk := toFloat64(args[0])
	rf, rfOk := toFloat64(args[1])
	if !lfOk || !rfOk {
		return nil, Column{}, &QueryError{Code: "42883", Message: "MOD() requires numeric arguments"}
	}
	if rf == 0 {
		return nil, Column{}, &QueryError{Code: "22012", Message: "division by zero"}
	}
	return math.Mod(lf, rf), floatCol, nil
}
