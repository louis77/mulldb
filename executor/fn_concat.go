package executor

import "strings"

func init() {
	RegisterScalar("CONCAT", fnConcat)
}

func fnConcat(args []any) (any, Column, error) {
	col := Column{Name: "concat", TypeOID: OIDText, TypeSize: -1}
	if len(args) == 0 {
		return nil, Column{}, &QueryError{Code: "42883", Message: "CONCAT() requires at least one argument"}
	}
	var b strings.Builder
	for _, a := range args {
		if a == nil {
			continue
		}
		s, ok := coerceToText(a)
		if !ok {
			return nil, Column{}, &QueryError{Code: "42883", Message: "CONCAT() could not coerce argument to text"}
		}
		b.WriteString(s)
	}
	return b.String(), col, nil
}
