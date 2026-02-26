package executor

import "mulldb/version"

func init() {
	RegisterScalar("VERSION", fnVersion)
}

func fnVersion(args []any) (any, Column, error) {
	if len(args) != 0 {
		return nil, Column{}, &QueryError{Code: "42883", Message: "VERSION() takes no arguments"}
	}
	return version.String(), Column{Name: "version", TypeOID: OIDText, TypeSize: -1}, nil
}
