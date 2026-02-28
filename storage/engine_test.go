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
// Split WAL — file layout and migration
// -------------------------------------------------------------------------

func TestEngine_SplitWAL_FileLayout(t *testing.T) {
	dir := tempDir(t)
	eng := openEngine(t, dir)

	eng.CreateTable("users", testColumns)
	eng.CreateTable("orders", []ColumnDef{
		{Name: "id", DataType: TypeInteger},
		{Name: "total", DataType: TypeInteger},
	})
	eng.Insert("users", nil, [][]any{{int64(1), "alice", true}})
	eng.Insert("orders", nil, [][]any{{int64(1), int64(100)}})
	eng.Close()

	// Verify file layout.
	if !fileExists(filepath.Join(dir, "catalog.wal")) {
		t.Error("catalog.wal not found")
	}
	if !fileExists(filepath.Join(dir, "tables", "users.wal")) {
		t.Error("tables/users.wal not found")
	}
	if !fileExists(filepath.Join(dir, "tables", "orders.wal")) {
		t.Error("tables/orders.wal not found")
	}
	// Legacy wal.dat should NOT exist for a fresh database.
	if fileExists(filepath.Join(dir, "wal.dat")) {
		t.Error("wal.dat should not exist for a fresh database")
	}
}

func TestEngine_SplitWAL_DropRemovesFile(t *testing.T) {
	dir := tempDir(t)
	eng := openEngine(t, dir)

	eng.CreateTable("temp", testColumns)
	eng.Insert("temp", nil, [][]any{{int64(1), "x", false}})

	walPath := filepath.Join(dir, "tables", "temp.wal")
	if !fileExists(walPath) {
		t.Fatal("table WAL file should exist before drop")
	}

	eng.DropTable("temp")

	if fileExists(walPath) {
		t.Error("table WAL file should be deleted after drop")
	}

	eng.Close()
}

func TestEngine_SplitWAL_Migration(t *testing.T) {
	dir := tempDir(t)
	os.MkdirAll(dir, 0755)

	// Create a v2 single-WAL file manually (simulates legacy data).
	walPath := filepath.Join(dir, "wal.dat")
	w, err := OpenWAL(walPath, false)
	if err != nil {
		t.Fatalf("create legacy WAL: %v", err)
	}
	cols := []ColumnDef{
		{Name: "id", DataType: TypeInteger, PrimaryKey: true},
		{Name: "name", DataType: TypeText},
	}
	w.WriteCreateTable("users", cols)
	w.WriteInsert("users", 1, []any{int64(1), "alice"})
	w.WriteInsert("users", 2, []any{int64(2), "bob"})

	// Also create+drop a table to test that dropped tables are pruned.
	w.WriteCreateTable("temp", []ColumnDef{{Name: "x", DataType: TypeInteger}})
	w.WriteInsert("temp", 1, []any{int64(42)})
	w.WriteDropTable("temp")
	w.Close()

	// Open with migrate=true.
	eng, err := Open(dir, true)
	if err != nil {
		t.Fatalf("Open with migration: %v", err)
	}

	// Verify users survived.
	def, ok := eng.GetTable("users")
	if !ok {
		t.Fatal("users table not found after migration")
	}
	if len(def.Columns) != 2 {
		t.Fatalf("columns = %d, want 2", len(def.Columns))
	}
	if !def.Columns[0].PrimaryKey {
		t.Error("PK flag lost during migration")
	}

	rows := collectRows(t, must(eng.Scan("users")))
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(rows))
	}

	// Verify temp table was dropped.
	if _, ok := eng.GetTable("temp"); ok {
		t.Error("dropped table 'temp' should not exist after migration")
	}

	// Verify no WAL file exists for the dropped table.
	if fileExists(filepath.Join(dir, "tables", "temp.wal")) {
		t.Error("temp.wal should not exist after migration (table was dropped)")
	}

	// Verify backup exists.
	if !fileExists(filepath.Join(dir, "wal.dat.bak")) {
		t.Error("wal.dat.bak should exist after migration")
	}

	eng.Close()

	// Reopen without --migrate to verify the split format works.
	eng2, err := Open(dir, false)
	if err != nil {
		t.Fatalf("reopen after migration: %v", err)
	}
	defer eng2.Close()

	rows2 := collectRows(t, must(eng2.Scan("users")))
	if len(rows2) != 2 {
		t.Fatalf("rows after reopen = %d, want 2", len(rows2))
	}
}

func TestEngine_SplitWAL_RestartWithMultipleTables(t *testing.T) {
	dir := tempDir(t)

	eng := openEngine(t, dir)
	eng.CreateTable("t1", []ColumnDef{{Name: "v", DataType: TypeInteger}})
	eng.CreateTable("t2", []ColumnDef{{Name: "v", DataType: TypeText}})
	eng.Insert("t1", nil, [][]any{{int64(1)}, {int64(2)}})
	eng.Insert("t2", nil, [][]any{{"a"}, {"b"}, {"c"}})
	eng.Close()

	eng2 := openEngine(t, dir)
	defer eng2.Close()

	rows1 := collectRows(t, must(eng2.Scan("t1")))
	if len(rows1) != 2 {
		t.Errorf("t1 rows = %d, want 2", len(rows1))
	}
	rows2 := collectRows(t, must(eng2.Scan("t2")))
	if len(rows2) != 3 {
		t.Errorf("t2 rows = %d, want 3", len(rows2))
	}
}

func TestEngine_SplitWAL_SpecialCharTableNames(t *testing.T) {
	dir := tempDir(t)
	eng := openEngine(t, dir)

	// Table names with special characters.
	eng.CreateTable("my table", []ColumnDef{{Name: "v", DataType: TypeInteger}})
	eng.Insert("my table", nil, [][]any{{int64(1)}})
	eng.Close()

	// Verify the WAL file has percent-encoded name.
	if !fileExists(filepath.Join(dir, "tables", "my%20table.wal")) {
		t.Error("expected percent-encoded WAL filename")
	}

	// Reopen and verify data.
	eng2 := openEngine(t, dir)
	defer eng2.Close()

	rows := collectRows(t, must(eng2.Scan("my table")))
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rows))
	}
	if rows[0].Values[0] != int64(1) {
		t.Errorf("value = %v, want 1", rows[0].Values[0])
	}
}

func TestEngine_SplitWAL_OrphanCleanup(t *testing.T) {
	dir := tempDir(t)

	// Create a normal database.
	eng := openEngine(t, dir)
	eng.CreateTable("keep", []ColumnDef{{Name: "v", DataType: TypeInteger}})
	eng.Close()

	// Simulate an orphan WAL file (table was dropped but file lingers).
	tablesDir := filepath.Join(dir, "tables")
	orphanPath := filepath.Join(tablesDir, "orphan.wal")
	orphanFile, err := os.Create(orphanPath)
	if err != nil {
		t.Fatal(err)
	}
	writeWALHeader(orphanFile)
	orphanFile.Close()

	if !fileExists(orphanPath) {
		t.Fatal("orphan file should exist before reopen")
	}

	// Reopen — orphan should be cleaned up.
	eng2 := openEngine(t, dir)
	defer eng2.Close()

	if fileExists(orphanPath) {
		t.Error("orphan WAL file should be deleted on startup")
	}

	// Keep table should still work.
	if _, ok := eng2.GetTable("keep"); !ok {
		t.Error("'keep' table should still exist")
	}
}

func TestEngine_SplitWAL_ConcurrentDifferentTables(t *testing.T) {
	dir := tempDir(t)
	eng := openEngine(t, dir)
	defer eng.Close()

	eng.CreateTable("t1", []ColumnDef{
		{Name: "id", DataType: TypeInteger},
		{Name: "val", DataType: TypeText},
	})
	eng.CreateTable("t2", []ColumnDef{
		{Name: "id", DataType: TypeInteger},
		{Name: "val", DataType: TypeText},
	})

	const ops = 100
	errs := make(chan error, 4)
	var wg sync.WaitGroup

	// Writer to t1.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < ops; i++ {
			_, err := eng.Insert("t1", nil, [][]any{
				{int64(i), fmt.Sprintf("t1-%d", i)},
			})
			if err != nil {
				errs <- fmt.Errorf("t1 insert %d: %w", i, err)
				return
			}
		}
	}()

	// Writer to t2.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < ops; i++ {
			_, err := eng.Insert("t2", nil, [][]any{
				{int64(i), fmt.Sprintf("t2-%d", i)},
			})
			if err != nil {
				errs <- fmt.Errorf("t2 insert %d: %w", i, err)
				return
			}
		}
	}()

	// Reader of t1.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < ops; i++ {
			it, err := eng.Scan("t1")
			if err != nil {
				errs <- fmt.Errorf("t1 scan %d: %w", i, err)
				return
			}
			for {
				if _, ok := it.Next(); !ok {
					break
				}
			}
			it.Close()
		}
	}()

	// Reader of t2.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < ops; i++ {
			it, err := eng.Scan("t2")
			if err != nil {
				errs <- fmt.Errorf("t2 scan %d: %w", i, err)
				return
			}
			for {
				if _, ok := it.Next(); !ok {
					break
				}
			}
			it.Close()
		}
	}()

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Error(err)
	}

	// Verify final counts.
	rows1 := collectRows(t, must(eng.Scan("t1")))
	if len(rows1) != ops {
		t.Errorf("t1 rows = %d, want %d", len(rows1), ops)
	}
	rows2 := collectRows(t, must(eng.Scan("t2")))
	if len(rows2) != ops {
		t.Errorf("t2 rows = %d, want %d", len(rows2), ops)
	}
}

func TestEngine_SplitWAL_DropWhileOtherTableActive(t *testing.T) {
	dir := tempDir(t)
	eng := openEngine(t, dir)
	defer eng.Close()

	eng.CreateTable("keep", []ColumnDef{{Name: "v", DataType: TypeInteger}})
	eng.CreateTable("drop_me", []ColumnDef{{Name: "v", DataType: TypeInteger}})
	eng.Insert("keep", nil, [][]any{{int64(1)}})
	eng.Insert("drop_me", nil, [][]any{{int64(2)}})

	// Drop one table.
	if err := eng.DropTable("drop_me"); err != nil {
		t.Fatal(err)
	}

	// The other table should still work fine.
	rows := collectRows(t, must(eng.Scan("keep")))
	if len(rows) != 1 {
		t.Errorf("keep rows = %d, want 1", len(rows))
	}

	// DML on dropped table should fail.
	_, err := eng.Scan("drop_me")
	if err == nil {
		t.Error("scan on dropped table should fail")
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

// -------------------------------------------------------------------------
// ALTER TABLE (ADD COLUMN / DROP COLUMN)
// -------------------------------------------------------------------------

func TestEngine_AddColumn(t *testing.T) {
	dir := tempDir(t)
	eng := openEngine(t, dir)
	defer eng.Close()

	eng.CreateTable("t", []ColumnDef{
		{Name: "id", DataType: TypeInteger},
	})
	eng.Insert("t", nil, [][]any{{int64(1)}})

	// Add a column.
	if err := eng.AddColumn("t", ColumnDef{Name: "name", DataType: TypeText}); err != nil {
		t.Fatal(err)
	}

	// Old rows should return NULL for the new column.
	rows := collectRows(t, must(eng.Scan("t")))
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	// New column ordinal = 1, row length = 1 (short row), so RowValue should return nil.
	if RowValue(rows[0].Values, 1) != nil {
		t.Errorf("new column = %v, want nil", RowValue(rows[0].Values, 1))
	}

	// Insert with the new column.
	eng.Insert("t", []string{"id", "name"}, [][]any{{int64(2), "alice"}})
	rows = collectRows(t, must(eng.Scan("t")))
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}
}

func TestEngine_DropColumn(t *testing.T) {
	dir := tempDir(t)
	eng := openEngine(t, dir)
	defer eng.Close()

	eng.CreateTable("t", []ColumnDef{
		{Name: "id", DataType: TypeInteger},
		{Name: "name", DataType: TypeText},
	})
	eng.Insert("t", nil, [][]any{{int64(1), "alice"}})

	if err := eng.DropColumn("t", "name"); err != nil {
		t.Fatal(err)
	}

	// Schema should only have one column.
	def, ok := eng.GetTable("t")
	if !ok {
		t.Fatal("table not found")
	}
	if len(def.Columns) != 1 {
		t.Fatalf("got %d columns, want 1", len(def.Columns))
	}
	if def.Columns[0].Name != "id" {
		t.Errorf("remaining column = %q, want id", def.Columns[0].Name)
	}
}

func TestEngine_DropColumn_PK_Error(t *testing.T) {
	dir := tempDir(t)
	eng := openEngine(t, dir)
	defer eng.Close()

	eng.CreateTable("t", []ColumnDef{
		{Name: "id", DataType: TypeInteger, PrimaryKey: true},
		{Name: "name", DataType: TypeText},
	})

	err := eng.DropColumn("t", "id")
	if err == nil {
		t.Fatal("expected error for dropping PK column")
	}
}

func TestEngine_DropColumn_LastColumn_Error(t *testing.T) {
	dir := tempDir(t)
	eng := openEngine(t, dir)
	defer eng.Close()

	eng.CreateTable("t", []ColumnDef{
		{Name: "id", DataType: TypeInteger},
	})

	err := eng.DropColumn("t", "id")
	if err == nil {
		t.Fatal("expected error for dropping last column")
	}
}

func TestEngine_AddColumn_Duplicate_Error(t *testing.T) {
	dir := tempDir(t)
	eng := openEngine(t, dir)
	defer eng.Close()

	eng.CreateTable("t", []ColumnDef{
		{Name: "id", DataType: TypeInteger},
	})

	err := eng.AddColumn("t", ColumnDef{Name: "id", DataType: TypeText})
	if err == nil {
		t.Fatal("expected error for duplicate column")
	}
	var colExists *ColumnExistsError
	if !errors.As(err, &colExists) {
		t.Errorf("got error %T, want *ColumnExistsError", err)
	}
}

func TestEngine_DropColumn_NotFound_Error(t *testing.T) {
	dir := tempDir(t)
	eng := openEngine(t, dir)
	defer eng.Close()

	eng.CreateTable("t", []ColumnDef{
		{Name: "id", DataType: TypeInteger},
	})

	err := eng.DropColumn("t", "nonexistent")
	if err == nil {
		t.Fatal("expected error for dropping non-existent column")
	}
}

func TestEngine_AddDropColumn_WAL_Replay(t *testing.T) {
	dir := tempDir(t)

	// Create, insert, add column, insert again, drop column.
	eng := openEngine(t, dir)
	eng.CreateTable("t", []ColumnDef{
		{Name: "a", DataType: TypeInteger},
		{Name: "b", DataType: TypeText},
	})
	eng.Insert("t", nil, [][]any{{int64(1), "x"}})
	eng.AddColumn("t", ColumnDef{Name: "c", DataType: TypeInteger})
	eng.Insert("t", []string{"a", "b", "c"}, [][]any{{int64(2), "y", int64(42)}})
	eng.DropColumn("t", "b")
	eng.Close()

	// Reopen and verify.
	eng = openEngine(t, dir)
	defer eng.Close()

	def, ok := eng.GetTable("t")
	if !ok {
		t.Fatal("table not found after replay")
	}
	if len(def.Columns) != 2 {
		t.Fatalf("got %d columns, want 2 (a, c)", len(def.Columns))
	}
	if def.Columns[0].Name != "a" || def.Columns[1].Name != "c" {
		t.Errorf("columns = [%s, %s], want [a, c]", def.Columns[0].Name, def.Columns[1].Name)
	}

	rows := collectRows(t, must(eng.Scan("t")))
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}
}

func TestEngine_MigrateV2ToV3(t *testing.T) {
	dir := tempDir(t)
	os.MkdirAll(filepath.Join(dir, "tables"), 0755)

	// Write a v2 catalog WAL manually.
	catPath := filepath.Join(dir, "catalog.wal")
	f, err := os.Create(catPath)
	if err != nil {
		t.Fatal(err)
	}
	// Write v2 header.
	var hdr [6]byte
	copy(hdr[:4], "MWAL")
	hdr[4] = 0
	hdr[5] = 2
	f.Write(hdr[:])

	// Write a v2 CREATE TABLE entry (no ordinals).
	cols := []ColumnDef{
		{Name: "id", DataType: TypeInteger, PrimaryKey: true},
		{Name: "name", DataType: TypeText},
	}
	buf := encodeString(nil, "users")
	buf = appendUint16(buf, uint16(len(cols)))
	for _, col := range cols {
		buf = encodeString(buf, col.Name)
		buf = append(buf, byte(col.DataType))
		var pk byte
		if col.PrimaryKey {
			pk = 1
		}
		buf = append(buf, pk)
		// v2 does NOT have ordinal — that's what migration adds
	}
	writeRawEntry(f, 1, buf) // opCreateTable = 1
	f.Close()

	// Create empty table WAL.
	tablePath := filepath.Join(dir, "tables", "users.wal")
	tf, _ := os.Create(tablePath)
	writeWALHeader(tf)
	tf.Close()

	// Open with migrate=true — should migrate v2→v3.
	eng, err := Open(dir, true)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer eng.Close()

	def, ok := eng.GetTable("users")
	if !ok {
		t.Fatal("table not found after migration")
	}
	if len(def.Columns) != 2 {
		t.Fatalf("got %d columns, want 2", len(def.Columns))
	}
	// Ordinals should be assigned sequentially by migration.
	if def.Columns[0].Ordinal != 0 {
		t.Errorf("col[0].Ordinal = %d, want 0", def.Columns[0].Ordinal)
	}
	if def.Columns[1].Ordinal != 1 {
		t.Errorf("col[1].Ordinal = %d, want 1", def.Columns[1].Ordinal)
	}
	if def.NextOrdinal != 2 {
		t.Errorf("NextOrdinal = %d, want 2", def.NextOrdinal)
	}
}

func TestEngine_MigrateV3ToV4(t *testing.T) {
	dir := tempDir(t)
	os.MkdirAll(filepath.Join(dir, "tables"), 0755)

	// Write a v3 catalog WAL manually.
	catPath := filepath.Join(dir, "catalog.wal")
	f, err := os.Create(catPath)
	if err != nil {
		t.Fatal(err)
	}
	// Write v3 header.
	var hdr [6]byte
	copy(hdr[:4], "MWAL")
	hdr[4] = 0
	hdr[5] = 3
	f.Write(hdr[:])

	// Write a v3 CREATE TABLE entry (with ordinals, no notNull).
	buf := encodeString(nil, "users")
	buf = appendUint16(buf, 2) // 2 columns
	// col 0: id INTEGER PRIMARY KEY
	buf = encodeString(buf, "id")
	buf = append(buf, byte(TypeInteger))
	buf = append(buf, 1) // pk = true
	buf = appendUint16(buf, 0)
	// col 1: name TEXT
	buf = encodeString(buf, "name")
	buf = append(buf, byte(TypeText))
	buf = append(buf, 0) // pk = false
	buf = appendUint16(buf, 1)
	writeRawEntry(f, 1, buf) // opCreateTable = 1
	f.Close()

	// Create empty table WAL.
	tablePath := filepath.Join(dir, "tables", "users.wal")
	tf, _ := os.Create(tablePath)
	writeWALHeader(tf)
	tf.Close()

	// Open with migrate=true — should migrate v3→v4.
	eng, err := Open(dir, true)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer eng.Close()

	def, ok := eng.GetTable("users")
	if !ok {
		t.Fatal("table not found after migration")
	}
	if len(def.Columns) != 2 {
		t.Fatalf("got %d columns, want 2", len(def.Columns))
	}
	// PK column should have NotNull=true after migration.
	if !def.Columns[0].NotNull {
		t.Error("col[0].NotNull = false, want true (PK column)")
	}
	// Non-PK column should have NotNull=false after migration.
	if def.Columns[1].NotNull {
		t.Error("col[1].NotNull = true, want false (non-PK column)")
	}
}

// appendUint16 is a test helper for encoding uint16 big-endian.
func appendUint16(buf []byte, v uint16) []byte {
	return append(buf, byte(v>>8), byte(v))
}
