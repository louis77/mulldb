package storage

import (
	"os"
	"path/filepath"
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
	eng, err := Open(dir)
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
// Helpers
// -------------------------------------------------------------------------

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
