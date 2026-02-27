package storage

import "strings"

// CompareValues returns -1, 0, or 1 for ordering, or -2 if the values
// are not comparable (e.g. NULL or type mismatch).
func CompareValues(a, b any) int {
	if a == nil || b == nil {
		return -2
	}
	switch av := a.(type) {
	case int64:
		bv, ok := b.(int64)
		if !ok {
			return -2
		}
		switch {
		case av < bv:
			return -1
		case av > bv:
			return 1
		default:
			return 0
		}
	case string:
		bv, ok := b.(string)
		if !ok {
			return -2
		}
		return strings.Compare(av, bv)
	case bool:
		bv, ok := b.(bool)
		if !ok {
			return -2
		}
		if av == bv {
			return 0
		}
		if !av && bv {
			return -1
		}
		return 1
	default:
		return -2
	}
}
