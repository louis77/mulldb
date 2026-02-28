package executor

import "time"

func init() {
	RegisterScalar("NOW", fnNow)
}

func fnNow(args []any) (any, Column, error) {
	if len(args) != 0 {
		return nil, Column{}, &QueryError{Code: "42883", Message: "NOW() takes no arguments"}
	}
	return time.Now().UTC(), Column{Name: "now", TypeOID: OIDTimestampTZ, TypeSize: 8}, nil
}
