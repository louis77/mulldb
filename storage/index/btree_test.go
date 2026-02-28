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

func TestBTree_Size(t *testing.T) {
	bt := NewBTree(cmp)
	emptySize := bt.Size()
	if emptySize != 0 {
		t.Errorf("empty BTree.Size() = %d, want 0", emptySize)
	}

	for i := int64(0); i < 100; i++ {
		bt.Put(i, i*10)
	}
	populatedSize := bt.Size()
	if populatedSize <= 0 {
		t.Errorf("populated BTree.Size() = %d, want > 0", populatedSize)
	}
}

func TestMultiBTree_Size(t *testing.T) {
	mt := NewMultiBTree(cmp)
	emptySize := mt.Size()
	if emptySize != 0 {
		t.Errorf("empty MultiBTree.Size() = %d, want 0", emptySize)
	}

	for i := int64(0); i < 50; i++ {
		mt.Put(int64(i%10), i)
	}
	populatedSize := mt.Size()
	if populatedSize <= 0 {
		t.Errorf("populated MultiBTree.Size() = %d, want > 0", populatedSize)
	}
}

func TestBTree_GetEmpty(t *testing.T) {
	bt := NewBTree(cmp)
	_, ok := bt.Get(int64(1))
	if ok {
		t.Error("get from empty tree should return false")
	}
}

// -------------------------------------------------------------------------
// MultiBTree tests
// -------------------------------------------------------------------------

func TestMultiBTree_PutAndGetAll(t *testing.T) {
	mt := NewMultiBTree(cmp)
	mt.Put(int64(10), 1)
	mt.Put(int64(10), 2)
	mt.Put(int64(10), 3)
	mt.Put(int64(20), 4)

	ids := mt.GetAll(int64(10))
	if len(ids) != 3 {
		t.Fatalf("GetAll(10) returned %d results, want 3", len(ids))
	}
	// Should be in rowID order.
	for i, want := range []int64{1, 2, 3} {
		if ids[i] != want {
			t.Errorf("GetAll(10)[%d] = %d, want %d", i, ids[i], want)
		}
	}

	ids = mt.GetAll(int64(20))
	if len(ids) != 1 || ids[0] != 4 {
		t.Errorf("GetAll(20) = %v, want [4]", ids)
	}

	ids = mt.GetAll(int64(99))
	if len(ids) != 0 {
		t.Errorf("GetAll(99) = %v, want []", ids)
	}
}

func TestMultiBTree_Delete(t *testing.T) {
	mt := NewMultiBTree(cmp)
	mt.Put(int64(10), 1)
	mt.Put(int64(10), 2)
	mt.Put(int64(10), 3)

	if !mt.Delete(int64(10), 2) {
		t.Fatal("delete (10, 2) should return true")
	}

	ids := mt.GetAll(int64(10))
	if len(ids) != 2 {
		t.Fatalf("GetAll(10) after delete returned %d results, want 2", len(ids))
	}
	for i, want := range []int64{1, 3} {
		if ids[i] != want {
			t.Errorf("GetAll(10)[%d] = %d, want %d", i, ids[i], want)
		}
	}

	// Delete non-existent pair.
	if mt.Delete(int64(10), 99) {
		t.Error("delete (10, 99) should return false")
	}
}

func TestMultiBTree_DeleteAll(t *testing.T) {
	mt := NewMultiBTree(cmp)
	mt.Put(int64(5), 1)
	mt.Put(int64(5), 2)

	mt.Delete(int64(5), 1)
	mt.Delete(int64(5), 2)

	ids := mt.GetAll(int64(5))
	if len(ids) != 0 {
		t.Errorf("GetAll(5) after deleting all = %v, want []", ids)
	}

	// Re-insert after deleting all.
	mt.Put(int64(5), 10)
	ids = mt.GetAll(int64(5))
	if len(ids) != 1 || ids[0] != 10 {
		t.Errorf("GetAll(5) after re-insert = %v, want [10]", ids)
	}
}

func TestMultiBTree_GetAllEmpty(t *testing.T) {
	mt := NewMultiBTree(cmp)
	ids := mt.GetAll(int64(1))
	if ids != nil {
		t.Errorf("GetAll on empty tree = %v, want nil", ids)
	}
}

func TestMultiBTree_StringKeys(t *testing.T) {
	mt := NewMultiBTree(cmp)
	mt.Put("alice", 1)
	mt.Put("alice", 2)
	mt.Put("bob", 3)

	ids := mt.GetAll("alice")
	if len(ids) != 2 {
		t.Fatalf("GetAll(alice) returned %d results, want 2", len(ids))
	}
	if ids[0] != 1 || ids[1] != 2 {
		t.Errorf("GetAll(alice) = %v, want [1 2]", ids)
	}

	ids = mt.GetAll("bob")
	if len(ids) != 1 || ids[0] != 3 {
		t.Errorf("GetAll(bob) = %v, want [3]", ids)
	}
}

func TestMultiBTree_LargeInsert(t *testing.T) {
	mt := NewMultiBTree(cmp)
	const nKeys = 100
	const nRowsPerKey = 50

	// Insert nRowsPerKey rows for each of nKeys keys.
	for k := int64(0); k < nKeys; k++ {
		for r := int64(0); r < nRowsPerKey; r++ {
			mt.Put(k, k*1000+r)
		}
	}

	// Verify each key returns exactly nRowsPerKey results.
	for k := int64(0); k < nKeys; k++ {
		ids := mt.GetAll(k)
		if len(ids) != nRowsPerKey {
			t.Fatalf("GetAll(%d) returned %d results, want %d", k, len(ids), nRowsPerKey)
		}
		// Verify rowIDs are in order.
		for i := 1; i < len(ids); i++ {
			if ids[i] <= ids[i-1] {
				t.Fatalf("GetAll(%d) not in order: ids[%d]=%d >= ids[%d]=%d",
					k, i-1, ids[i-1], i, ids[i])
			}
		}
	}

	// Non-existent key.
	ids := mt.GetAll(int64(999))
	if len(ids) != 0 {
		t.Errorf("GetAll(999) = %v, want []", ids)
	}
}

func TestMultiBTree_LargeDelete(t *testing.T) {
	mt := NewMultiBTree(cmp)

	// Insert 5 rows per key for 200 keys.
	for k := int64(0); k < 200; k++ {
		for r := int64(0); r < 5; r++ {
			mt.Put(k, k*100+r)
		}
	}

	// Delete even-numbered rowIDs for each key.
	for k := int64(0); k < 200; k++ {
		for r := int64(0); r < 5; r += 2 {
			if !mt.Delete(k, k*100+r) {
				t.Fatalf("delete (%d, %d) should return true", k, k*100+r)
			}
		}
	}

	// Verify odd-numbered rowIDs remain.
	for k := int64(0); k < 200; k++ {
		ids := mt.GetAll(k)
		if len(ids) != 2 { // rows 1 and 3 remain
			t.Fatalf("GetAll(%d) after delete returned %d results, want 2", k, len(ids))
		}
	}
}
