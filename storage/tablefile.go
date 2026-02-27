package storage

import (
	"fmt"
	"strings"
)

// tableFileName converts a table name to a filesystem-safe filename
// by percent-encoding characters outside [a-zA-Z0-9_-].
// For example, "my table" → "my%20table.wal".
func tableFileName(name string) string {
	var b strings.Builder
	for _, c := range []byte(name) {
		if isFilenameSafe(c) {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	b.WriteString(".wal")
	return b.String()
}

// tableNameFromFile reverses tableFileName: strips the ".wal" suffix
// and percent-decodes the remainder. Returns an error if the input
// is malformed.
func tableNameFromFile(filename string) (string, error) {
	if !strings.HasSuffix(filename, ".wal") {
		return "", fmt.Errorf("missing .wal suffix: %q", filename)
	}
	encoded := strings.TrimSuffix(filename, ".wal")

	var b strings.Builder
	i := 0
	for i < len(encoded) {
		if encoded[i] == '%' {
			if i+2 >= len(encoded) {
				return "", fmt.Errorf("truncated percent-encoding in %q at position %d", filename, i)
			}
			hi := unhex(encoded[i+1])
			lo := unhex(encoded[i+2])
			if hi < 0 || lo < 0 {
				return "", fmt.Errorf("invalid percent-encoding in %q at position %d", filename, i)
			}
			b.WriteByte(byte(hi<<4 | lo))
			i += 3
		} else {
			b.WriteByte(encoded[i])
			i++
		}
	}
	return b.String(), nil
}

func isFilenameSafe(c byte) bool {
	return (c >= 'a' && c <= 'z') ||
		(c >= 'A' && c <= 'Z') ||
		(c >= '0' && c <= '9') ||
		c == '_' || c == '-'
}

func unhex(c byte) int {
	switch {
	case c >= '0' && c <= '9':
		return int(c - '0')
	case c >= 'A' && c <= 'F':
		return int(c-'A') + 10
	case c >= 'a' && c <= 'f':
		return int(c-'a') + 10
	default:
		return -1
	}
}
