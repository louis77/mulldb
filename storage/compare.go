package storage

import (
	"strings"
	"time"
)

// CompareValues returns -1, 0, or 1 for ordering, or -2 if the values
// are not comparable (e.g. NULL or type mismatch).
func CompareValues(a, b any) int {
	if a == nil || b == nil {
		return -2
	}
	switch av := a.(type) {
	case int64:
		switch bv := b.(type) {
		case int64:
			switch {
			case av < bv:
				return -1
			case av > bv:
				return 1
			default:
				return 0
			}
		case float64:
			return compareFloat64(float64(av), bv)
		default:
			return -2
		}
	case string:
		switch bv := b.(type) {
		case string:
			return strings.Compare(av, bv)
		case time.Time:
			t, err := ParseTimestamp(av)
			if err != nil {
				return -2
			}
			return CompareValues(t, bv)
		default:
			return -2
		}
	case float64:
		switch bv := b.(type) {
		case float64:
			return compareFloat64(av, bv)
		case int64:
			return compareFloat64(av, float64(bv))
		default:
			return -2
		}
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
	case time.Time:
		switch bv := b.(type) {
		case time.Time:
			switch {
			case av.Before(bv):
				return -1
			case av.After(bv):
				return 1
			default:
				return 0
			}
		case string:
			t, err := ParseTimestamp(bv)
			if err != nil {
				return -2
			}
			return CompareValues(av, t)
		default:
			return -2
		}
	default:
		return -2
	}
}

func compareFloat64(a, b float64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}
