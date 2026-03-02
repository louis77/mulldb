package executor

func init() {
	RegisterScalar("COALESCE", fnCoalesce)
}

func fnCoalesce(args []any) (any, Column, error) {
	if len(args) < 1 {
		return nil, Column{}, &QueryError{Code: "42883", Message: "COALESCE() requires at least one argument"}
	}

	// Return the first non-NULL value
	for _, arg := range args {
		if arg != nil {
			// Determine type OID and size based on the value type
			var typeOID int32
			var typeSize int16

			switch arg.(type) {
			case int64:
				typeOID = OIDInt8
				typeSize = 8
			case float64:
				typeOID = OIDFloat8
				typeSize = 8
			case string:
				typeOID = OIDText
				typeSize = -1
			case bool:
				typeOID = OIDBool
				typeSize = 1
			default:
				typeOID = OIDUnknown
				typeSize = -1
			}

			return arg, Column{Name: "coalesce", TypeOID: typeOID, TypeSize: typeSize}, nil
		}
	}

	// All arguments were NULL
	return nil, Column{Name: "coalesce", TypeOID: OIDUnknown, TypeSize: -1}, nil
}
