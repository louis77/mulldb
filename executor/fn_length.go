package executor

import "unicode/utf8"

func init() {
	RegisterScalar("LENGTH", fnLength)
	RegisterScalar("CHARACTER_LENGTH", fnLength)
	RegisterScalar("CHAR_LENGTH", fnLength)
}

func fnLength(args []any) (any, Column, error) {
	if len(args) != 1 {
		return nil, Column{}, &QueryError{Code: "42883", Message: "LENGTH() takes exactly one argument"}
	}
	if args[0] == nil {
		return nil, Column{Name: "length", TypeOID: OIDInt8, TypeSize: 8}, nil
	}
	s, ok := args[0].(string)
	if !ok {
		return nil, Column{}, &QueryError{Code: "42883", Message: "LENGTH() requires a TEXT argument"}
	}
	return int64(utf8.RuneCountInString(s)), Column{Name: "length", TypeOID: OIDInt8, TypeSize: 8}, nil
}
