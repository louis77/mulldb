package storage

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func tempDir(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(os.TempDir(), "mulldb-test-"+t.Name())
	os.RemoveAll(dir)
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func openEngine(t *testing.T, dir string) Engine {
	t.Helper()
	eng, err := Open(dir, false)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return eng
}

// collectRows drains a RowIterator into a slice.
func collectRows(t *testing.T, it RowIterator) []Row {
	t.Helper()
	var rows []Row
	for {
		row, ok := it.Next()
		if !ok {
			break
		}
		rows = append(rows, row)
	}
	it.Close()
	return rows
}

var testColumns = []ColumnDef{
	{Name: "id", DataType: TypeInteger},
	{Name: "name", DataType: TypeText},
	{Name: "active", DataType: TypeBoolean},
}

// -------------------------------------------------------------------------
// Basic operations
// -------------------------------------------------------------------------

func TestEngine_CreateAndScan(t *testing.T) {
	dir := tempDir(t)
	eng := openEngine(t, dir)
	defer eng.Close()

	if err := eng.CreateTable("users", testColumns); err != nil {
		t.Fatal(err)
	}

	n, err := eng.Insert("users", nil, [][]any{
		{int64(1), "alice", true},
		{int64(2), "bob", false},
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("inserted %d, want 2", n)
	}

	it, err := eng.Scan("users")
	if err != nil {
		t.Fatal(err)
	}
	rows := collectRows(t, it)
	if len(rows) != 2 {
		t.Fatalf("scanned %d rows, want 2", len(rows))
	}
}

func TestEngine_InsertWithColumns(t *testing.T) {
	dir := tempDir(t)
	eng := openEngine(t, dir)
	defer eng.Close()

	eng.CreateTable("users", testColumns)
	n, err := eng.Insert("users", []string{"name", "id"}, [][]any{
		{"carol", int64(3)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("inserted %d, want 1", n)
	}

	rows := collectRows(t, must(eng.Scan("users")))
	if len(rows) != 1 {
		t.Fatalf("scanned %d rows, want 1", len(rows))
	}
	r := rows[0]
	// Column order: id, name, active
	if r.Values[0] != int64(3) {
		t.Errorf("id = %v, want 3", r.Values[0])
	}
	if r.Values[1] != "carol" {
		t.Errorf("name = %v, want carol", r.Values[1])
	}
	if r.Values[2] != nil {
		t.Errorf("active = %v, want nil", r.Values[2])
	}
}

func TestEngine_Update(t *testing.T) {
	dir := tempDir(t)
	eng := openEngine(t, dir)
	defer eng.Close()

	eng.CreateTable("users", testColumns)
	eng.Insert("users", nil, [][]any{
		{int64(1), "alice", true},
		{int64(2), "bob", false},
	})

	updated, err := eng.Update("users",
		map[string]any{"active": true},
		func(r Row) bool { return r.Values[0] == int64(2) },
	)
	if err != nil {
		t.Fatal(err)
	}
	if updated != 1 {
		t.Fatalf("updated %d, want 1", updated)
	}

	rows := collectRows(t, must(eng.Scan("users")))
	for _, r := range rows {
		if r.Values[2] != true {
			t.Errorf("row %d: active = %v, want true", r.ID, r.Values[2])
		}
	}
}

func TestEngine_Delete(t *testing.T) {
	dir := tempDir(t)
	eng := openEngine(t, dir)
	defer eng.Close()

	eng.CreateTable("users", testColumns)
	eng.Insert("users", nil, [][]any{
		{int64(1), "alice", true},
		{int64(2), "bob", false},
		{int64(3), "carol", true},
	})

	deleted, err := eng.Delete("users",
		func(r Row) bool { return r.Values[0] == int64(2) },
	)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 1 {
		t.Fatalf("deleted %d, want 1", deleted)
	}

	rows := collectRows(t, must(eng.Scan("users")))
	if len(rows) != 2 {
		t.Fatalf("remaining %d rows, want 2", len(rows))
	}
}

func TestEngine_DropTable(t *testing.T) {
	dir := tempDir(t)
	eng := openEngine(t, dir)
	defer eng.Close()

	eng.CreateTable("users", testColumns)
	if err := eng.DropTable("users"); err != nil {
		t.Fatal(err)
	}
	if _, ok := eng.GetTable("users"); ok {
		t.Error("table still exists after drop")
	}
	if _, err := eng.Scan("users"); err == nil {
		t.Error("scan should fail after drop")
	}
}

// -------------------------------------------------------------------------
// WAL replay — data survives restart
// -------------------------------------------------------------------------

func TestEngine_Restart(t *testing.T) {
	dir := tempDir(t)

	// First session: create table, insert, update, delete.
	eng := openEngine(t, dir)
	eng.CreateTable("users", testColumns)
	eng.Insert("users", nil, [][]any{
		{int64(1), "alice", true},
		{int64(2), "bob", false},
		{int64(3), "carol", true},
	})
	eng.Update("users",
		map[string]any{"name": "robert"},
		func(r Row) bool { return r.Values[0] == int64(2) },
	)
	eng.Delete("users",
		func(r Row) bool { return r.Values[0] == int64(3) },
	)
	eng.Close()

	// Second session: reopen and verify state.
	eng2 := openEngine(t, dir)
	defer eng2.Close()

	def, ok := eng2.GetTable("users")
	if !ok {
		t.Fatal("table users not found after restart")
	}
	if len(def.Columns) != 3 {
		t.Fatalf("columns = %d, want 3", len(def.Columns))
	}

	rows := collectRows(t, must(eng2.Scan("users")))
	if len(rows) != 2 {
		t.Fatalf("rows after restart = %d, want 2", len(rows))
	}

	byID := make(map[int64]Row)
	for _, r := range rows {
		byID[r.ID] = r
	}

	alice := byID[1]
	if alice.Values[1] != "alice" {
		t.Errorf("alice name = %v", alice.Values[1])
	}

	robert := byID[2]
	if robert.Values[1] != "robert" {
		t.Errorf("bob should be renamed to robert, got %v", robert.Values[1])
	}

	if _, ok := byID[3]; ok {
		t.Error("carol should have been deleted")
	}
}

func TestEngine_RestartDroppedTable(t *testing.T) {
	dir := tempDir(t)

	eng := openEngine(t, dir)
	eng.CreateTable("temp", testColumns)
	eng.Insert("temp", nil, [][]any{{int64(1), "x", false}})
	eng.DropTable("temp")
	eng.Close()

	eng2 := openEngine(t, dir)
	defer eng2.Close()

	if _, ok := eng2.GetTable("temp"); ok {
		t.Error("dropped table should not exist after restart")
	}
}

// -------------------------------------------------------------------------
// Error cases
// -------------------------------------------------------------------------

func TestEngine_Errors(t *testing.T) {
	dir := tempDir(t)
	eng := openEngine(t, dir)
	defer eng.Close()

	// Table doesn't exist.
	if _, err := eng.Scan("nope"); err == nil {
		t.Error("scan non-existent table should fail")
	}
	if _, err := eng.Insert("nope", nil, [][]any{{int64(1)}}); err == nil {
		t.Error("insert non-existent table should fail")
	}
	if err := eng.DropTable("nope"); err == nil {
		t.Error("drop non-existent table should fail")
	}

	// Duplicate table.
	eng.CreateTable("t", testColumns)
	if err := eng.CreateTable("t", testColumns); err == nil {
		t.Error("duplicate create should fail")
	}

	// Wrong value count.
	if _, err := eng.Insert("t", nil, [][]any{{int64(1)}}); err == nil {
		t.Error("insert with wrong column count should fail")
	}
}

// -------------------------------------------------------------------------
// Value encoding round-trip
// -------------------------------------------------------------------------

func TestValueEncoding(t *testing.T) {
	values := []any{int64(42), "hello world", true, nil, false, int64(-1), ""}
	buf := encodeValues(nil, values)
	decoded, rest, err := decodeValues(buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(rest) != 0 {
		t.Fatalf("leftover bytes: %d", len(rest))
	}
	if len(decoded) != len(values) {
		t.Fatalf("decoded %d values, want %d", len(decoded), len(values))
	}
	for i, want := range values {
		got := decoded[i]
		if got != want {
			t.Errorf("value[%d] = %v (%T), want %v (%T)", i, got, got, want, want)
		}
	}
}

// -------------------------------------------------------------------------
// Typed errors
// -------------------------------------------------------------------------

func TestEngine_TypedErrors(t *testing.T) {
	dir := tempDir(t)
	eng := openEngine(t, dir)
	defer eng.Close()

	// TableNotFoundError from Scan.
	_, err := eng.Scan("nope")
	if err == nil {
		t.Fatal("expected error")
	}
	var tnf *TableNotFoundError
	if !errors.As(err, &tnf) {
		t.Errorf("Scan: expected TableNotFoundError, got %T: %v", err, err)
	}

	// TableExistsError from duplicate CreateTable.
	eng.CreateTable("t", testColumns)
	err = eng.CreateTable("t", testColumns)
	if err == nil {
		t.Fatal("expected error")
	}
	var te *TableExistsError
	if !errors.As(err, &te) {
		t.Errorf("CreateTable: expected TableExistsError, got %T: %v", err, err)
	}

	// ValueCountError from wrong column count.
	_, err = eng.Insert("t", nil, [][]any{{int64(1)}})
	if err == nil {
		t.Fatal("expected error")
	}
	var vc *ValueCountError
	if !errors.As(err, &vc) {
		t.Errorf("Insert: expected ValueCountError, got %T: %v", err, err)
	}
}

// -------------------------------------------------------------------------
// Concurrency
// -------------------------------------------------------------------------

func TestEngine_ConcurrentReadsAndWrites(t *testing.T) {
	dir := tempDir(t)
	eng := openEngine(t, dir)
	defer eng.Close()

	eng.CreateTable("t", []ColumnDef{
		{Name: "id", DataType: TypeInteger},
		{Name: "val", DataType: TypeText},
	})

	const numWriters = 4
	const numReaders = 8
	const opsPerWorker = 50

	// Insert some seed data so readers always have something to scan.
	eng.Insert("t", nil, [][]any{
		{int64(0), "seed"},
	})

	errs := make(chan error, numWriters+numReaders)
	var wg sync.WaitGroup

	// Writers: insert rows concurrently.
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				_, err := eng.Insert("t", nil, [][]any{
					{int64(writerID*1000 + i), fmt.Sprintf("w%d-%d", writerID, i)},
				})
				if err != nil {
					errs <- fmt.Errorf("writer %d, op %d: %w", writerID, i, err)
					return
				}
			}
		}(w)
	}

	// Readers: scan the table concurrently.
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				it, err := eng.Scan("t")
				if err != nil {
					errs <- fmt.Errorf("reader %d, op %d: scan: %w", readerID, i, err)
					return
				}
				count := 0
				for {
					_, ok := it.Next()
					if !ok {
						break
					}
					count++
				}
				it.Close()
				if count == 0 {
					errs <- fmt.Errorf("reader %d, op %d: got 0 rows", readerID, i)
					return
				}
			}
		}(r)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Error(err)
	}

	// Verify final row count: 1 seed + numWriters * opsPerWorker
	expectedRows := 1 + numWriters*opsPerWorker
	it, err := eng.Scan("t")
	if err != nil {
		t.Fatal(err)
	}
	rows := collectRows(t, it)
	if len(rows) != expectedRows {
		t.Errorf("final rows = %d, want %d", len(rows), expectedRows)
	}
}

// -------------------------------------------------------------------------
// Primary Key
// -------------------------------------------------------------------------

var pkColumns = []ColumnDef{
	{Name: "id", DataType: TypeInteger, PrimaryKey: true},
	{Name: "name", DataType: TypeText},
}

func TestEngine_PrimaryKey_Insert(t *testing.T) {
	dir := tempDir(t)
	eng := openEngine(t, dir)
	defer eng.Close()

	eng.CreateTable("users", pkColumns)
	n, err := eng.Insert("users", nil, [][]any{
		{int64(1), "alice"},
		{int64(2), "bob"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("inserted %d, want 2", n)
	}
}

func TestEngine_PrimaryKey_DuplicateInsert(t *testing.T) {
	dir := tempDir(t)
	eng := openEngine(t, dir)
	defer eng.Close()

	eng.CreateTable("users", pkColumns)
	eng.Insert("users", nil, [][]any{{int64(1), "alice"}})

	_, err := eng.Insert("users", nil, [][]any{{int64(1), "bob"}})
	if err == nil {
		t.Fatal("expected error on duplicate PK")
	}
	var uv *UniqueViolationError
	if !errors.As(err, &uv) {
		t.Fatalf("expected UniqueViolationError, got %T: %v", err, err)
	}
}

func TestEngine_PrimaryKey_DuplicateInBatch(t *testing.T) {
	dir := tempDir(t)
	eng := openEngine(t, dir)
	defer eng.Close()

	eng.CreateTable("users", pkColumns)
	_, err := eng.Insert("users", nil, [][]any{
		{int64(1), "alice"},
		{int64(1), "bob"}, // duplicate within same batch
	})
	if err == nil {
		t.Fatal("expected error on duplicate PK within batch")
	}
	var uv *UniqueViolationError
	if !errors.As(err, &uv) {
		t.Fatalf("expected UniqueViolationError, got %T: %v", err, err)
	}
}

func TestEngine_PrimaryKey_NullPK(t *testing.T) {
	dir := tempDir(t)
	eng := openEngine(t, dir)
	defer eng.Close()

	eng.CreateTable("users", pkColumns)
	_, err := eng.Insert("users", nil, [][]any{{nil, "alice"}})
	if err == nil {
		t.Fatal("expected error on NULL PK")
	}
	var uv *UniqueViolationError
	if !errors.As(err, &uv) {
		t.Fatalf("expected UniqueViolationError, got %T: %v", err, err)
	}
}

func TestEngine_PrimaryKey_DeleteAndReinsert(t *testing.T) {
	dir := tempDir(t)
	eng := openEngine(t, dir)
	defer eng.Close()

	eng.CreateTable("users", pkColumns)
	eng.Insert("users", nil, [][]any{{int64(1), "alice"}})

	eng.Delete("users", func(r Row) bool { return r.Values[0] == int64(1) })

	// Should be able to reinsert same PK.
	_, err := eng.Insert("users", nil, [][]any{{int64(1), "bob"}})
	if err != nil {
		t.Fatalf("reinsert after delete failed: %v", err)
	}
}

func TestEngine_PrimaryKey_UpdateNonPK(t *testing.T) {
	dir := tempDir(t)
	eng := openEngine(t, dir)
	defer eng.Close()

	eng.CreateTable("users", pkColumns)
	eng.Insert("users", nil, [][]any{{int64(1), "alice"}})

	// Update non-PK column should succeed.
	n, err := eng.Update("users",
		map[string]any{"name": "alicia"},
		func(r Row) bool { return r.Values[0] == int64(1) },
	)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("updated %d, want 1", n)
	}
}

func TestEngine_PrimaryKey_UpdatePKToDuplicate(t *testing.T) {
	dir := tempDir(t)
	eng := openEngine(t, dir)
	defer eng.Close()

	eng.CreateTable("users", pkColumns)
	eng.Insert("users", nil, [][]any{
		{int64(1), "alice"},
		{int64(2), "bob"},
	})

	_, err := eng.Update("users",
		map[string]any{"id": int64(1)},
		func(r Row) bool { return r.Values[0] == int64(2) },
	)
	if err == nil {
		t.Fatal("expected error on PK update to duplicate value")
	}
	var uv *UniqueViolationError
	if !errors.As(err, &uv) {
		t.Fatalf("expected UniqueViolationError, got %T: %v", err, err)
	}
}

func TestEngine_PrimaryKey_LookupByPK(t *testing.T) {
	dir := tempDir(t)
	eng := openEngine(t, dir)
	defer eng.Close()

	eng.CreateTable("users", pkColumns)
	eng.Insert("users", nil, [][]any{
		{int64(1), "alice"},
		{int64(2), "bob"},
	})

	row, err := eng.LookupByPK("users", int64(2))
	if err != nil {
		t.Fatal(err)
	}
	if row == nil {
		t.Fatal("expected row, got nil")
	}
	if row.Values[1] != "bob" {
		t.Errorf("name = %v, want bob", row.Values[1])
	}

	// Non-existent key.
	row, err = eng.LookupByPK("users", int64(99))
	if err != nil {
		t.Fatal(err)
	}
	if row != nil {
		t.Errorf("expected nil for missing key, got %v", row)
	}
}

func TestEngine_PrimaryKey_LookupByPK_NoPK(t *testing.T) {
	dir := tempDir(t)
	eng := openEngine(t, dir)
	defer eng.Close()

	eng.CreateTable("users", testColumns) // no PK
	eng.Insert("users", nil, [][]any{{int64(1), "alice", true}})

	row, err := eng.LookupByPK("users", int64(1))
	if err != nil {
		t.Fatal(err)
	}
	if row != nil {
		t.Error("expected nil for table without PK")
	}
}

func TestEngine_PrimaryKey_Restart(t *testing.T) {
	dir := tempDir(t)

	// First session.
	eng := openEngine(t, dir)
	eng.CreateTable("users", pkColumns)
	eng.Insert("users", nil, [][]any{
		{int64(1), "alice"},
		{int64(2), "bob"},
	})
	eng.Close()

	// Second session — PK enforcement should still work.
	eng2 := openEngine(t, dir)
	defer eng2.Close()

	_, err := eng2.Insert("users", nil, [][]any{{int64(1), "dup"}})
	if err == nil {
		t.Fatal("expected duplicate PK error after restart")
	}
	var uv *UniqueViolationError
	if !errors.As(err, &uv) {
		t.Fatalf("expected UniqueViolationError, got %T: %v", err, err)
	}

	// Verify LookupByPK works after restart.
	row, err := eng2.LookupByPK("users", int64(2))
	if err != nil {
		t.Fatal(err)
	}
	if row == nil || row.Values[1] != "bob" {
		t.Errorf("lookup after restart: got %v, want bob", row)
	}
}

func TestEngine_PrimaryKey_TextKey(t *testing.T) {
	dir := tempDir(t)
	eng := openEngine(t, dir)
	defer eng.Close()

	cols := []ColumnDef{
		{Name: "code", DataType: TypeText, PrimaryKey: true},
		{Name: "desc", DataType: TypeText},
	}
	eng.CreateTable("codes", cols)
	eng.Insert("codes", nil, [][]any{{"US", "United States"}, {"UK", "United Kingdom"}})

	_, err := eng.Insert("codes", nil, [][]any{{"US", "duplicate"}})
	if err == nil {
		t.Fatal("expected error on duplicate text PK")
	}

	row, _ := eng.LookupByPK("codes", "UK")
	if row == nil || row.Values[1] != "United Kingdom" {
		t.Errorf("text key lookup: got %v", row)
	}
}

// -------------------------------------------------------------------------
// Helpers
// -------------------------------------------------------------------------

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// -------------------------------------------------------------------------
// Benchmarks
// -------------------------------------------------------------------------

func BenchmarkSumScan(b *testing.B) {
	const rowCount = 10_000_000
	def := TableDef{Name: "bench", Columns: []ColumnDef{{Name: "val", DataType: TypeInteger}}}
	h := newTableHeap(def)
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < rowCount; i++ {
		h.insertWithID(int64(i+1), []any{int64(rng.Intn(6))})
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		it := h.scan()
		var sum int64
		for {
			row, ok := it.Next()
			if !ok {
				break
			}
			sum += row.Values[0].(int64)
		}
		it.Close()
		_ = sum
	}
}
