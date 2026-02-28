package deepsize

import (
	"testing"
	"unsafe"
)

func TestOf_Nil(t *testing.T) {
	if got := Of(nil); got != 0 {
		t.Errorf("Of(nil) = %d, want 0", got)
	}
}

func TestOf_Primitives(t *testing.T) {
	// int64 should be 8 bytes
	got := Of(int64(42))
	if got != int64(unsafe.Sizeof(int64(0))) {
		t.Errorf("Of(int64) = %d, want %d", got, unsafe.Sizeof(int64(0)))
	}

	// bool should be 1 byte
	got = Of(true)
	if got != int64(unsafe.Sizeof(true)) {
		t.Errorf("Of(bool) = %d, want %d", got, unsafe.Sizeof(true))
	}
}

func TestOf_String(t *testing.T) {
	s := "hello"
	got := Of(s)
	// string header (16 bytes on 64-bit) + 5 bytes of content
	headerSize := int64(unsafe.Sizeof(s))
	want := headerSize + 5
	if got != want {
		t.Errorf("Of(%q) = %d, want %d", s, got, want)
	}
}

func TestOf_Slice(t *testing.T) {
	s := make([]int64, 3, 5)
	got := Of(s)
	// slice header (24) + cap(5) * 8
	headerSize := int64(unsafe.Sizeof(s))
	want := headerSize + 5*8
	if got != want {
		t.Errorf("Of([]int64 len=3 cap=5) = %d, want %d", got, want)
	}
}

func TestOf_SliceOfStrings(t *testing.T) {
	s := []string{"ab", "cde"}
	got := Of(s)
	// Should include slice header + string headers + string content
	if got <= 0 {
		t.Errorf("Of([]string) = %d, want > 0", got)
	}
	// At minimum: slice header + 2*string_header + 2+3 chars
	headerSize := int64(unsafe.Sizeof(s))
	strHeader := int64(unsafe.Sizeof(""))
	minExpected := headerSize + 2*strHeader + 5
	if got < minExpected {
		t.Errorf("Of([]string) = %d, want >= %d", got, minExpected)
	}
}

func TestOf_NestedStruct(t *testing.T) {
	type inner struct {
		Name string
		Val  int64
	}
	type outer struct {
		A inner
		B *inner
	}

	v := outer{
		A: inner{Name: "test", Val: 42},
		B: &inner{Name: "ptr", Val: 99},
	}
	got := Of(v)
	if got <= 0 {
		t.Errorf("Of(nested struct) = %d, want > 0", got)
	}
	// Should be larger than just the outer struct size due to string content + pointer target
	minExpected := int64(unsafe.Sizeof(v)) + 4 + 3 // "test" + "ptr"
	if got < minExpected {
		t.Errorf("Of(nested struct) = %d, want >= %d", got, minExpected)
	}
}

func TestOf_NilSlice(t *testing.T) {
	var s []int64
	got := Of(s)
	// nil slice: just the header
	want := int64(unsafe.Sizeof(s))
	if got != want {
		t.Errorf("Of(nil slice) = %d, want %d", got, want)
	}
}

func TestOf_CycleDetection(t *testing.T) {
	type node struct {
		Next *node
		Val  int
	}
	a := &node{Val: 1}
	b := &node{Val: 2}
	a.Next = b
	b.Next = a // cycle

	// Should not hang or panic
	got := Of(a)
	if got <= 0 {
		t.Errorf("Of(cycle) = %d, want > 0", got)
	}
}

func TestOf_SliceOfAny(t *testing.T) {
	s := []any{int64(1), "hello", nil, true}
	got := Of(s)
	if got <= 0 {
		t.Errorf("Of([]any) = %d, want > 0", got)
	}
}

func TestOf_Map(t *testing.T) {
	m := map[string]int64{"a": 1, "bb": 2}
	got := Of(m)
	if got <= 0 {
		t.Errorf("Of(map) = %d, want > 0", got)
	}
}
