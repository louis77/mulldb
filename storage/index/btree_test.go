package index

import (
	"strings"
	"testing"
)

func cmp(a, b any) int {
	if a == nil || b == nil {
		return -2
	}
	switch av := a.(type) {
	case int64:
		bv := b.(int64)
		switch {
		case av < bv:
			return -1
		case av > bv:
			return 1
		default:
			return 0
		}
	case string:
		return strings.Compare(av, b.(string))
	case bool:
		bv := b.(bool)
		if av == bv {
			return 0
		}
		if !av {
			return -1
		}
		return 1
	}
	return -2
}

func TestBTree_PutAndGet(t *testing.T) {
	bt := NewBTree(cmp)
	if !bt.Put(int64(10), 1) {
		t.Fatal("put 10 should succeed")
	}
	if !bt.Put(int64(20), 2) {
		t.Fatal("put 20 should succeed")
	}
	if !bt.Put(int64(5), 3) {
		t.Fatal("put 5 should succeed")
	}

	id, ok := bt.Get(int64(10))
	if !ok || id != 1 {
		t.Errorf("get 10 = (%d, %v), want (1, true)", id, ok)
	}
	id, ok = bt.Get(int64(20))
	if !ok || id != 2 {
		t.Errorf("get 20 = (%d, %v), want (2, true)", id, ok)
	}
	id, ok = bt.Get(int64(5))
	if !ok || id != 3 {
		t.Errorf("get 5 = (%d, %v), want (3, true)", id, ok)
	}

	_, ok = bt.Get(int64(99))
	if ok {
		t.Error("get 99 should return false")
	}
}

func TestBTree_PutDuplicate(t *testing.T) {
	bt := NewBTree(cmp)
	bt.Put(int64(10), 1)
	if bt.Put(int64(10), 2) {
		t.Error("duplicate put should return false")
	}
	// Original mapping should be preserved.
	id, _ := bt.Get(int64(10))
	if id != 1 {
		t.Errorf("get 10 = %d, want 1 (original)", id)
	}
}

func TestBTree_Delete(t *testing.T) {
	bt := NewBTree(cmp)
	bt.Put(int64(10), 1)
	bt.Put(int64(20), 2)
	bt.Put(int64(30), 3)

	if !bt.Delete(int64(20)) {
		t.Fatal("delete 20 should return true")
	}
	if _, ok := bt.Get(int64(20)); ok {
		t.Error("get 20 should return false after delete")
	}
	// Others still present.
	if _, ok := bt.Get(int64(10)); !ok {
		t.Error("10 should still exist")
	}
	if _, ok := bt.Get(int64(30)); !ok {
		t.Error("30 should still exist")
	}

	// Delete non-existent key.
	if bt.Delete(int64(99)) {
		t.Error("delete 99 should return false")
	}
}

func TestBTree_DeleteEmpty(t *testing.T) {
	bt := NewBTree(cmp)
	if bt.Delete(int64(1)) {
		t.Error("delete from empty tree should return false")
	}
}

func TestBTree_DeleteAll(t *testing.T) {
	bt := NewBTree(cmp)
	bt.Put(int64(1), 1)
	bt.Delete(int64(1))
	if _, ok := bt.Get(int64(1)); ok {
		t.Error("tree should be empty")
	}
	// Re-insert after deleting all.
	if !bt.Put(int64(1), 2) {
		t.Error("put after delete-all should succeed")
	}
	id, ok := bt.Get(int64(1))
	if !ok || id != 2 {
		t.Errorf("get 1 = (%d, %v), want (2, true)", id, ok)
	}
}

func TestBTree_StringKeys(t *testing.T) {
	bt := NewBTree(cmp)
	bt.Put("alice", 1)
	bt.Put("bob", 2)
	bt.Put("carol", 3)

	id, ok := bt.Get("bob")
	if !ok || id != 2 {
		t.Errorf("get bob = (%d, %v), want (2, true)", id, ok)
	}
	if bt.Put("bob", 99) {
		t.Error("duplicate string put should return false")
	}
}

func TestBTree_BoolKeys(t *testing.T) {
	bt := NewBTree(cmp)
	bt.Put(true, 1)
	bt.Put(false, 2)

	id, ok := bt.Get(true)
	if !ok || id != 1 {
		t.Errorf("get true = (%d, %v), want (1, true)", id, ok)
	}
	id, ok = bt.Get(false)
	if !ok || id != 2 {
		t.Errorf("get false = (%d, %v), want (2, true)", id, ok)
	}
	if bt.Put(true, 3) {
		t.Error("duplicate bool put should return false")
	}
}

func TestBTree_LargeInsert(t *testing.T) {
	bt := NewBTree(cmp)
	const n = 10000
	for i := int64(0); i < n; i++ {
		if !bt.Put(i, i*10) {
			t.Fatalf("put %d should succeed", i)
		}
	}
	// Verify all keys.
	for i := int64(0); i < n; i++ {
		id, ok := bt.Get(i)
		if !ok || id != i*10 {
			t.Fatalf("get %d = (%d, %v), want (%d, true)", i, id, ok, i*10)
		}
	}
	// Verify duplicate detection.
	if bt.Put(int64(500), 99) {
		t.Error("duplicate put should return false")
	}
}

func TestBTree_LargeInsertReverse(t *testing.T) {
	bt := NewBTree(cmp)
	const n = 10000
	for i := int64(n - 1); i >= 0; i-- {
		if !bt.Put(i, i) {
			t.Fatalf("put %d should succeed", i)
		}
	}
	for i := int64(0); i < n; i++ {
		id, ok := bt.Get(i)
		if !ok || id != i {
			t.Fatalf("get %d = (%d, %v), want (%d, true)", i, id, ok, i)
		}
	}
}

func TestBTree_LargeDelete(t *testing.T) {
	bt := NewBTree(cmp)
	const n = 1000
	for i := int64(0); i < n; i++ {
		bt.Put(i, i)
	}
	// Delete even keys.
	for i := int64(0); i < n; i += 2 {
		if !bt.Delete(i) {
			t.Fatalf("delete %d should return true", i)
		}
	}
	// Verify odd keys remain.
	for i := int64(0); i < n; i++ {
		_, ok := bt.Get(i)
		if i%2 == 0 && ok {
			t.Errorf("get %d should return false (deleted)", i)
		}
		if i%2 != 0 && !ok {
			t.Errorf("get %d should return true", i)
		}
	}
}

func TestBTree_GetEmpty(t *testing.T) {
	bt := NewBTree(cmp)
	_, ok := bt.Get(int64(1))
	if ok {
		t.Error("get from empty tree should return false")
	}
}
