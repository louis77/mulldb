// Package deepsize provides a reflection-based deep memory size calculator.
package deepsize

import (
	"reflect"
	"unsafe"
)

// Of returns an estimate of the total memory occupied by v, including
// all reachable heap allocations (strings, slices, pointers, maps, etc.).
// It detects pointer cycles to avoid infinite recursion.
func Of(v any) int64 {
	if v == nil {
		return 0
	}
	seen := make(map[uintptr]bool)
	return sizeOf(reflect.ValueOf(v), seen)
}

func sizeOf(v reflect.Value, seen map[uintptr]bool) int64 {
	if !v.IsValid() {
		return 0
	}

	switch v.Kind() {
	case reflect.Ptr:
		if v.IsNil() {
			return int64(v.Type().Size())
		}
		ptr := v.Pointer()
		if seen[ptr] {
			return int64(v.Type().Size())
		}
		seen[ptr] = true
		return int64(v.Type().Size()) + sizeOf(v.Elem(), seen)

	case reflect.String:
		// string header + backing array
		return int64(v.Type().Size()) + int64(v.Len())

	case reflect.Slice:
		if v.IsNil() {
			return int64(v.Type().Size())
		}
		// slice header
		s := int64(v.Type().Size())
		// backing array: cap * elem size
		elemSize := int64(v.Type().Elem().Size())
		s += int64(v.Cap()) * elemSize
		// recurse into elements if they contain pointers
		if containsPointers(v.Type().Elem()) {
			for i := 0; i < v.Len(); i++ {
				s += sizeOfIndirect(v.Index(i), seen)
			}
		}
		return s

	case reflect.Array:
		s := int64(0)
		if containsPointers(v.Type().Elem()) {
			for i := 0; i < v.Len(); i++ {
				s += sizeOfIndirect(v.Index(i), seen)
			}
		}
		return int64(v.Type().Size()) + s

	case reflect.Struct:
		s := int64(0)
		for i := 0; i < v.NumField(); i++ {
			s += sizeOfIndirect(v.Field(i), seen)
		}
		// Use the struct's full size (includes padding) rather than
		// summing field sizes, to account for alignment.
		fieldIndirect := s
		return int64(v.Type().Size()) + fieldIndirect

	case reflect.Map:
		if v.IsNil() {
			return int64(v.Type().Size())
		}
		// hmap header
		s := int64(v.Type().Size()) + int64(unsafe.Sizeof(uint64(0)))*8 // rough hmap size
		iter := v.MapRange()
		for iter.Next() {
			s += sizeOf(iter.Key(), seen)
			s += sizeOf(iter.Value(), seen)
		}
		return s

	case reflect.Interface:
		if v.IsNil() {
			return int64(v.Type().Size())
		}
		return int64(v.Type().Size()) + sizeOf(v.Elem(), seen)

	default:
		// bool, int*, uint*, float*, complex*
		return int64(v.Type().Size())
	}
}

// sizeOfIndirect returns only the indirect (heap-allocated) size of v,
// excluding the inline storage already counted by the parent container.
func sizeOfIndirect(v reflect.Value, seen map[uintptr]bool) int64 {
	if !v.IsValid() {
		return 0
	}

	switch v.Kind() {
	case reflect.Ptr:
		if v.IsNil() {
			return 0
		}
		ptr := v.Pointer()
		if seen[ptr] {
			return 0
		}
		seen[ptr] = true
		return int64(v.Elem().Type().Size()) + sizeOfIndirect(v.Elem(), seen)

	case reflect.String:
		return int64(v.Len())

	case reflect.Slice:
		if v.IsNil() {
			return 0
		}
		elemSize := int64(v.Type().Elem().Size())
		s := int64(v.Cap()) * elemSize
		if containsPointers(v.Type().Elem()) {
			for i := 0; i < v.Len(); i++ {
				s += sizeOfIndirect(v.Index(i), seen)
			}
		}
		return s

	case reflect.Map:
		if v.IsNil() {
			return 0
		}
		s := int64(unsafe.Sizeof(uint64(0))) * 8
		iter := v.MapRange()
		for iter.Next() {
			s += sizeOf(iter.Key(), seen)
			s += sizeOf(iter.Value(), seen)
		}
		return s

	case reflect.Interface:
		if v.IsNil() {
			return 0
		}
		return sizeOf(v.Elem(), seen)

	case reflect.Struct:
		s := int64(0)
		for i := 0; i < v.NumField(); i++ {
			s += sizeOfIndirect(v.Field(i), seen)
		}
		return s

	case reflect.Array:
		s := int64(0)
		if containsPointers(v.Type().Elem()) {
			for i := 0; i < v.Len(); i++ {
				s += sizeOfIndirect(v.Index(i), seen)
			}
		}
		return s

	default:
		return 0
	}
}

// containsPointers reports whether a type might contain heap-allocated data.
func containsPointers(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Ptr, reflect.Map, reflect.Slice, reflect.String,
		reflect.Interface:
		return true
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			if containsPointers(t.Field(i).Type) {
				return true
			}
		}
	case reflect.Array:
		return containsPointers(t.Elem())
	}
	return false
}
