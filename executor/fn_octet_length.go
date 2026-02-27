package executor

func init() {
	RegisterScalar("OCTET_LENGTH", fnOctetLength)
}

func fnOctetLength(args []any) (any, Column, error) {
	if len(args) != 1 {
		return nil, Column{}, &QueryError{Code: "42883", Message: "OCTET_LENGTH() takes exactly one argument"}
	}
	if args[0] == nil {
		return nil, Column{Name: "octet_length", TypeOID: OIDInt8, TypeSize: 8}, nil
	}
	s, ok := args[0].(string)
	if !ok {
		return nil, Column{}, &QueryError{Code: "42883", Message: "OCTET_LENGTH() requires a TEXT argument"}
	}
	return int64(len(s)), Column{Name: "octet_length", TypeOID: OIDInt8, TypeSize: 8}, nil
}
