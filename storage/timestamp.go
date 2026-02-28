package storage

import (
	"fmt"
	"time"
)

// timestampFormats lists the accepted input formats for TIMESTAMP values,
// tried in order. All parsed times are converted to UTC.
var timestampFormats = []string{
	"2006-01-02 15:04:05.999999Z07:00", // full with fractional seconds and timezone
	"2006-01-02 15:04:05Z07:00",        // full with timezone
	"2006-01-02T15:04:05.999999Z07:00", // ISO 8601 with fractional seconds
	"2006-01-02T15:04:05Z07:00",        // ISO 8601
	"2006-01-02 15:04:05.999999",       // no timezone, fractional seconds (assumed UTC)
	"2006-01-02 15:04:05",              // no timezone (assumed UTC)
	"2006-01-02T15:04:05",              // ISO 8601 no timezone (assumed UTC)
	"2006-01-02",                        // date only (midnight UTC)
}

// ParseTimestamp parses a string into a time.Time in UTC.
// It tries multiple common formats and always returns UTC.
func ParseTimestamp(s string) (time.Time, error) {
	for _, layout := range timestampFormats {
		t, err := time.Parse(layout, s)
		if err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid timestamp %q", s)
}

// coerceRowValues validates and coerces values to match the column types
// in def. Currently only TIMESTAMP columns need coercion (string → time.Time).
func coerceRowValues(def *TableDef, values []any) ([]any, error) {
	for i, col := range def.Columns {
		if i >= len(values) || values[i] == nil {
			continue
		}
		if col.DataType == TypeTimestamp {
			if _, ok := values[i].(time.Time); ok {
				continue // already a time.Time
			}
			s, ok := values[i].(string)
			if !ok {
				return nil, fmt.Errorf("column %q expects TIMESTAMP, got %T", col.Name, values[i])
			}
			t, err := ParseTimestamp(s)
			if err != nil {
				return nil, fmt.Errorf("column %q: %w", col.Name, err)
			}
			values[i] = t
		}
	}
	return values, nil
}
