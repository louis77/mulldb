package storage

import (
	"errors"
	"os"
	"testing"
)

// -------------------------------------------------------------------------
// Transaction overlay tests
// -------------------------------------------------------------------------

func TestTxEngine_InsertCommit(t *testing.T) {
	dir := tempDir(t)
	eng := openEngine(t, dir)
	defer eng.Close()

	if err := eng.CreateTable("users", []ColumnDef{
		{Name: "id", DataType: TypeInteger},
		{Name: "name", DataType: TypeText},
	}); err != nil {
		t.Fatal(err)
	}

	tx := NewTxEngine(eng)

	// Insert inside transaction.
	n, err := tx.Insert("users", nil, [][]any{{int64(1), "alice"}})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("inserted %d, want 1", n)
	}

	// Transaction should see its own insert.
	it, err := tx.Scan("users")
	if err != nil {
		t.Fatal(err)
	}
	rows := collectRows(t, it)
	if len(rows) != 1 {
		t.Fatalf("tx scan got %d rows, want 1", len(rows))
	}
	if rows[0].Values[1] != "alice" {
		t.Fatalf("got name=%v, want alice", rows[0].Values[1])
	}

	// Real engine should NOT see the insert yet.
	it2, err := eng.Scan("users")
	if err != nil {
		t.Fatal(err)
	}
	rows2 := collectRows(t, it2)
	if len(rows2) != 0 {
		t.Fatalf("real engine scan got %d rows, want 0 (uncommitted)", len(rows2))
	}

	// Commit.
	if err := tx.CommitOverlay(); err != nil {
		t.Fatal(err)
	}

	// Now the real engine should see it.
	it3, err := eng.Scan("users")
	if err != nil {
		t.Fatal(err)
	}
	rows3 := collectRows(t, it3)
	if len(rows3) != 1 {
		t.Fatalf("post-commit scan got %d rows, want 1", len(rows3))
	}
}

func TestTxEngine_InsertRollback(t *testing.T) {
	dir := tempDir(t)
	eng := openEngine(t, dir)
	defer eng.Close()

	if err := eng.CreateTable("users", []ColumnDef{
		{Name: "id", DataType: TypeInteger},
		{Name: "name", DataType: TypeText},
	}); err != nil {
		t.Fatal(err)
	}

	tx := NewTxEngine(eng)

	// Insert inside transaction.
	if _, err := tx.Insert("users", nil, [][]any{{int64(1), "alice"}}); err != nil {
		t.Fatal(err)
	}

	// Discard overlay (rollback) — just stop using tx.
	// Real engine should have no rows.
	it, err := eng.Scan("users")
	if err != nil {
		t.Fatal(err)
	}
	rows := collectRows(t, it)
	if len(rows) != 0 {
		t.Fatalf("after rollback, scan got %d rows, want 0", len(rows))
	}
}

func TestTxEngine_DeleteCommit(t *testing.T) {
	dir := tempDir(t)
	eng := openEngine(t, dir)
	defer eng.Close()

	if err := eng.CreateTable("users", []ColumnDef{
		{Name: "id", DataType: TypeInteger},
		{Name: "name", DataType: TypeText},
	}); err != nil {
		t.Fatal(err)
	}

	// Pre-populate.
	if _, err := eng.Insert("users", nil, [][]any{
		{int64(1), "alice"},
		{int64(2), "bob"},
	}); err != nil {
		t.Fatal(err)
	}

	tx := NewTxEngine(eng)

	// Delete alice in transaction.
	n, err := tx.Delete("users", func(r Row) bool {
		return RowValue(r.Values, 0) == int64(1)
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("deleted %d, want 1", n)
	}

	// Transaction sees only bob.
	it, err := tx.Scan("users")
	if err != nil {
		t.Fatal(err)
	}
	rows := collectRows(t, it)
	if len(rows) != 1 {
		t.Fatalf("tx scan got %d rows, want 1", len(rows))
	}
	if rows[0].Values[1] != "bob" {
		t.Fatalf("expected bob, got %v", rows[0].Values[1])
	}

	// Real engine still sees both.
	it2, err := eng.Scan("users")
	if err != nil {
		t.Fatal(err)
	}
	rows2 := collectRows(t, it2)
	if len(rows2) != 2 {
		t.Fatalf("real engine scan got %d rows, want 2", len(rows2))
	}

	// Commit.
	if err := tx.CommitOverlay(); err != nil {
		t.Fatal(err)
	}

	// Real engine now sees only bob.
	it3, err := eng.Scan("users")
	if err != nil {
		t.Fatal(err)
	}
	rows3 := collectRows(t, it3)
	if len(rows3) != 1 {
		t.Fatalf("post-commit scan got %d rows, want 1", len(rows3))
	}
}

func TestTxEngine_UpdateCommit(t *testing.T) {
	dir := tempDir(t)
	eng := openEngine(t, dir)
	defer eng.Close()

	if err := eng.CreateTable("users", []ColumnDef{
		{Name: "id", DataType: TypeInteger},
		{Name: "name", DataType: TypeText},
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := eng.Insert("users", nil, [][]any{
		{int64(1), "alice"},
	}); err != nil {
		t.Fatal(err)
	}

	tx := NewTxEngine(eng)

	// Update inside transaction.
	n, err := tx.Update("users", map[string]any{"name": "ALICE"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("updated %d, want 1", n)
	}

	// Transaction sees the update.
	it, err := tx.Scan("users")
	if err != nil {
		t.Fatal(err)
	}
	rows := collectRows(t, it)
	if len(rows) != 1 || rows[0].Values[1] != "ALICE" {
		t.Fatalf("tx scan: got %v, want [1 ALICE]", rows)
	}

	// Real engine still sees the original.
	it2, err := eng.Scan("users")
	if err != nil {
		t.Fatal(err)
	}
	rows2 := collectRows(t, it2)
	if len(rows2) != 1 || rows2[0].Values[1] != "alice" {
		t.Fatalf("real engine: got %v, want [1 alice]", rows2)
	}

	// Commit.
	if err := tx.CommitOverlay(); err != nil {
		t.Fatal(err)
	}

	// Real engine now sees the update.
	it3, err := eng.Scan("users")
	if err != nil {
		t.Fatal(err)
	}
	rows3 := collectRows(t, it3)
	if len(rows3) != 1 || rows3[0].Values[1] != "ALICE" {
		t.Fatalf("post-commit: got %v, want [1 ALICE]", rows3)
	}
}

func TestTxEngine_DDLRejected(t *testing.T) {
	dir := tempDir(t)
	eng := openEngine(t, dir)
	defer eng.Close()

	tx := NewTxEngine(eng)

	if err := tx.CreateTable("t", nil); err == nil {
		t.Fatal("expected error for CREATE TABLE inside tx")
	}
	if err := tx.DropTable("t"); err == nil {
		t.Fatal("expected error for DROP TABLE inside tx")
	}
	if err := tx.AddColumn("t", ColumnDef{}); err == nil {
		t.Fatal("expected error for ADD COLUMN inside tx")
	}
	if err := tx.DropColumn("t", "c"); err == nil {
		t.Fatal("expected error for DROP COLUMN inside tx")
	}
	if err := tx.CreateIndex("t", IndexDef{}); err == nil {
		t.Fatal("expected error for CREATE INDEX inside tx")
	}
	if err := tx.DropIndex("t", "i"); err == nil {
		t.Fatal("expected error for DROP INDEX inside tx")
	}
}

func TestTxEngine_ReadYourOwnWrites(t *testing.T) {
	dir := tempDir(t)
	eng := openEngine(t, dir)
	defer eng.Close()

	if err := eng.CreateTable("t", []ColumnDef{
		{Name: "id", DataType: TypeInteger, PrimaryKey: true},
		{Name: "val", DataType: TypeText},
	}); err != nil {
		t.Fatal(err)
	}

	// Pre-populate.
	if _, err := eng.Insert("t", nil, [][]any{{int64(1), "a"}}); err != nil {
		t.Fatal(err)
	}

	tx := NewTxEngine(eng)

	// Insert a new row.
	if _, err := tx.Insert("t", nil, [][]any{{int64(2), "b"}}); err != nil {
		t.Fatal(err)
	}

	// LookupByPK for the inserted row.
	row, err := tx.LookupByPK("t", int64(2))
	if err != nil {
		t.Fatal(err)
	}
	if row == nil {
		t.Fatal("LookupByPK returned nil for overlay insert")
	}
	if row.Values[1] != "b" {
		t.Fatalf("got %v, want b", row.Values[1])
	}

	// LookupByPK for the existing row.
	row2, err := tx.LookupByPK("t", int64(1))
	if err != nil {
		t.Fatal(err)
	}
	if row2 == nil || row2.Values[1] != "a" {
		t.Fatalf("got %v, want a", row2)
	}

	// Delete existing row and verify it's gone from PK lookup.
	if _, err := tx.Delete("t", func(r Row) bool {
		return RowValue(r.Values, 0) == int64(1)
	}); err != nil {
		t.Fatal(err)
	}

	row3, err := tx.LookupByPK("t", int64(1))
	if err != nil {
		t.Fatal(err)
	}
	if row3 != nil {
		t.Fatal("LookupByPK should return nil for deleted row")
	}
}

func TestTxEngine_MultiTableCommit(t *testing.T) {
	dir := tempDir(t)
	eng := openEngine(t, dir)
	defer eng.Close()

	if err := eng.CreateTable("a", []ColumnDef{
		{Name: "id", DataType: TypeInteger},
	}); err != nil {
		t.Fatal(err)
	}
	if err := eng.CreateTable("b", []ColumnDef{
		{Name: "id", DataType: TypeInteger},
	}); err != nil {
		t.Fatal(err)
	}

	tx := NewTxEngine(eng)

	if _, err := tx.Insert("a", nil, [][]any{{int64(1)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Insert("b", nil, [][]any{{int64(2)}}); err != nil {
		t.Fatal(err)
	}

	// Neither table has rows in real engine.
	itA, _ := eng.Scan("a")
	if len(collectRows(t, itA)) != 0 {
		t.Fatal("table a should be empty before commit")
	}
	itB, _ := eng.Scan("b")
	if len(collectRows(t, itB)) != 0 {
		t.Fatal("table b should be empty before commit")
	}

	// Commit.
	if err := tx.CommitOverlay(); err != nil {
		t.Fatal(err)
	}

	// Both tables have rows.
	itA2, _ := eng.Scan("a")
	if len(collectRows(t, itA2)) != 1 {
		t.Fatal("table a should have 1 row after commit")
	}
	itB2, _ := eng.Scan("b")
	if len(collectRows(t, itB2)) != 1 {
		t.Fatal("table b should have 1 row after commit")
	}
}

func TestTxEngine_PKConflictOnCommit(t *testing.T) {
	dir := tempDir(t)
	eng := openEngine(t, dir)
	defer eng.Close()

	if err := eng.CreateTable("t", []ColumnDef{
		{Name: "id", DataType: TypeInteger, PrimaryKey: true},
	}); err != nil {
		t.Fatal(err)
	}

	tx := NewTxEngine(eng)

	// Insert id=1 in transaction.
	if _, err := tx.Insert("t", nil, [][]any{{int64(1)}}); err != nil {
		t.Fatal(err)
	}

	// Meanwhile, insert id=1 directly in real engine (simulating another tx committing).
	if _, err := eng.Insert("t", nil, [][]any{{int64(1)}}); err != nil {
		t.Fatal(err)
	}

	// Commit should fail with unique violation.
	err := tx.CommitOverlay()
	if err == nil {
		t.Fatal("expected unique violation on commit")
	}
	var uv *UniqueViolationError
	if !isUniqueViolation(err) {
		t.Fatalf("expected UniqueViolationError, got %T: %v", err, err)
	}
	_ = uv
}

func isUniqueViolation(err error) bool {
	_, ok := err.(*UniqueViolationError)
	return ok
}

func TestTxEngine_DeleteThenInsertSamePK(t *testing.T) {
	dir := tempDir(t)
	eng := openEngine(t, dir)
	defer eng.Close()

	if err := eng.CreateTable("t", []ColumnDef{
		{Name: "id", DataType: TypeInteger, PrimaryKey: true},
		{Name: "val", DataType: TypeText},
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := eng.Insert("t", nil, [][]any{{int64(1), "old"}}); err != nil {
		t.Fatal(err)
	}

	tx := NewTxEngine(eng)

	// Delete id=1.
	if _, err := tx.Delete("t", func(r Row) bool {
		return RowValue(r.Values, 0) == int64(1)
	}); err != nil {
		t.Fatal(err)
	}

	// Re-insert id=1 with new value.
	if _, err := tx.Insert("t", nil, [][]any{{int64(1), "new"}}); err != nil {
		t.Fatal(err)
	}

	// Should see the new value.
	row, err := tx.LookupByPK("t", int64(1))
	if err != nil {
		t.Fatal(err)
	}
	if row == nil || row.Values[1] != "new" {
		t.Fatalf("expected 'new', got %v", row)
	}

	// Commit.
	if err := tx.CommitOverlay(); err != nil {
		t.Fatal(err)
	}

	// Verify final state.
	row2, err := eng.LookupByPK("t", int64(1))
	if err != nil {
		t.Fatal(err)
	}
	if row2 == nil || row2.Values[1] != "new" {
		t.Fatalf("post-commit: expected 'new', got %v", row2)
	}
}

func TestTxEngine_UpdateOverlayInsert(t *testing.T) {
	dir := tempDir(t)
	eng := openEngine(t, dir)
	defer eng.Close()

	if err := eng.CreateTable("t", []ColumnDef{
		{Name: "id", DataType: TypeInteger},
		{Name: "val", DataType: TypeText},
	}); err != nil {
		t.Fatal(err)
	}

	tx := NewTxEngine(eng)

	// Insert in transaction.
	if _, err := tx.Insert("t", nil, [][]any{{int64(1), "first"}}); err != nil {
		t.Fatal(err)
	}

	// Update the overlay insert.
	n, err := tx.Update("t", map[string]any{"val": "second"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("updated %d, want 1", n)
	}

	// Scan should show updated value.
	it, err := tx.Scan("t")
	if err != nil {
		t.Fatal(err)
	}
	rows := collectRows(t, it)
	if len(rows) != 1 || rows[0].Values[1] != "second" {
		t.Fatalf("expected 'second', got %v", rows)
	}

	// Commit and verify.
	if err := tx.CommitOverlay(); err != nil {
		t.Fatal(err)
	}

	it2, err := eng.Scan("t")
	if err != nil {
		t.Fatal(err)
	}
	rows2 := collectRows(t, it2)
	if len(rows2) != 1 || rows2[0].Values[1] != "second" {
		t.Fatalf("post-commit: expected 'second', got %v", rows2)
	}
}

func TestTxEngine_DeleteOverlayInsert(t *testing.T) {
	dir := tempDir(t)
	eng := openEngine(t, dir)
	defer eng.Close()

	if err := eng.CreateTable("t", []ColumnDef{
		{Name: "id", DataType: TypeInteger},
		{Name: "val", DataType: TypeText},
	}); err != nil {
		t.Fatal(err)
	}

	tx := NewTxEngine(eng)

	// Insert in transaction.
	if _, err := tx.Insert("t", nil, [][]any{{int64(1), "a"}}); err != nil {
		t.Fatal(err)
	}

	// Delete it.
	n, err := tx.Delete("t", func(r Row) bool {
		return RowValue(r.Values, 0) == int64(1)
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("deleted %d, want 1", n)
	}

	// Scan should be empty.
	it, err := tx.Scan("t")
	if err != nil {
		t.Fatal(err)
	}
	rows := collectRows(t, it)
	if len(rows) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(rows))
	}

	// Commit.
	if err := tx.CommitOverlay(); err != nil {
		t.Fatal(err)
	}

	// Real engine should also be empty.
	it2, err := eng.Scan("t")
	if err != nil {
		t.Fatal(err)
	}
	rows2 := collectRows(t, it2)
	if len(rows2) != 0 {
		t.Fatalf("post-commit: expected 0 rows, got %d", len(rows2))
	}
}

func TestTxEngine_RowCount(t *testing.T) {
	dir := tempDir(t)
	eng := openEngine(t, dir)
	defer eng.Close()

	if err := eng.CreateTable("t", []ColumnDef{
		{Name: "id", DataType: TypeInteger},
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := eng.Insert("t", nil, [][]any{{int64(1)}, {int64(2)}}); err != nil {
		t.Fatal(err)
	}

	tx := NewTxEngine(eng)

	count, err := tx.RowCount("t")
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("row count = %d, want 2", count)
	}

	if _, err := tx.Insert("t", nil, [][]any{{int64(3)}}); err != nil {
		t.Fatal(err)
	}
	count, err = tx.RowCount("t")
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf("row count = %d, want 3", count)
	}

	if _, err := tx.Delete("t", func(r Row) bool {
		return RowValue(r.Values, 0) == int64(1)
	}); err != nil {
		t.Fatal(err)
	}
	count, err = tx.RowCount("t")
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("row count = %d, want 2", count)
	}
}

// -------------------------------------------------------------------------
// WAL crash recovery — incomplete transactions should be discarded
// -------------------------------------------------------------------------

func TestWAL_IncompleteTransactionDiscarded(t *testing.T) {
	dir := tempDir(t)
	eng := openEngine(t, dir)
	eng.SetFsync(false) // speed up test

	if err := eng.CreateTable("t", []ColumnDef{
		{Name: "id", DataType: TypeInteger},
		{Name: "val", DataType: TypeText},
	}); err != nil {
		t.Fatal(err)
	}

	// Insert a committed row normally.
	if _, err := eng.Insert("t", nil, [][]any{{int64(1), "committed"}}); err != nil {
		t.Fatal(err)
	}

	// Manually write an incomplete transaction to the table WAL.
	realEng := eng.(*engine)
	ts := realEng.tableStates["t"]

	// Write begin marker.
	if err := ts.wal.WriteBeginTx(); err != nil {
		t.Fatal(err)
	}
	// Write an insert inside the "transaction" but no commit marker.
	if err := ts.wal.WriteInsertBatchNoSync("t", []rowInsert{
		{RowID: 99, Values: []any{int64(99), "uncommitted"}},
	}); err != nil {
		t.Fatal(err)
	}
	// Intentionally do NOT write CommitTx — simulating a crash.

	eng.Close()

	// Re-open and replay.
	eng2 := openEngine(t, dir)
	defer eng2.Close()

	// Should only see the committed row.
	it, err := eng2.Scan("t")
	if err != nil {
		t.Fatal(err)
	}
	rows := collectRows(t, it)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row after crash recovery, got %d", len(rows))
	}
	if rows[0].Values[1] != "committed" {
		t.Fatalf("expected 'committed', got %v", rows[0].Values[1])
	}
}

func TestWAL_CompleteTransactionReplayed(t *testing.T) {
	dir := tempDir(t)
	eng := openEngine(t, dir)
	eng.SetFsync(false)

	if err := eng.CreateTable("t", []ColumnDef{
		{Name: "id", DataType: TypeInteger},
		{Name: "val", DataType: TypeText},
	}); err != nil {
		t.Fatal(err)
	}

	// Use TxEngine to commit a transaction.
	tx := NewTxEngine(eng)
	if _, err := tx.Insert("t", nil, [][]any{{int64(1), "txrow"}}); err != nil {
		t.Fatal(err)
	}
	if err := tx.CommitOverlay(); err != nil {
		t.Fatal(err)
	}

	eng.Close()

	// Re-open and verify the committed row survives.
	eng2 := openEngine(t, dir)
	defer eng2.Close()

	it, err := eng2.Scan("t")
	if err != nil {
		t.Fatal(err)
	}
	rows := collectRows(t, it)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row after recovery, got %d", len(rows))
	}
	if rows[0].Values[1] != "txrow" {
		t.Fatalf("expected 'txrow', got %v", rows[0].Values[1])
	}
}

// -------------------------------------------------------------------------
// Isolation — two TxEngines should not see each other's uncommitted data
// -------------------------------------------------------------------------

func TestTxEngine_Isolation(t *testing.T) {
	dir := tempDir(t)
	eng := openEngine(t, dir)
	defer eng.Close()

	if err := eng.CreateTable("t", []ColumnDef{
		{Name: "id", DataType: TypeInteger},
		{Name: "val", DataType: TypeText},
	}); err != nil {
		t.Fatal(err)
	}

	txA := NewTxEngine(eng)
	txB := NewTxEngine(eng)

	// A inserts a row.
	if _, err := txA.Insert("t", nil, [][]any{{int64(1), "from_A"}}); err != nil {
		t.Fatal(err)
	}

	// B should not see A's insert.
	it, err := txB.Scan("t")
	if err != nil {
		t.Fatal(err)
	}
	rows := collectRows(t, it)
	if len(rows) != 0 {
		t.Fatalf("txB should see 0 rows, got %d", len(rows))
	}

	// A commits.
	if err := txA.CommitOverlay(); err != nil {
		t.Fatal(err)
	}

	// B now sees A's committed data (READ COMMITTED).
	it2, err := txB.Scan("t")
	if err != nil {
		t.Fatal(err)
	}
	rows2 := collectRows(t, it2)
	if len(rows2) != 1 {
		t.Fatalf("txB should see 1 row after A commits, got %d", len(rows2))
	}
}

// -------------------------------------------------------------------------
// EmptyCommit — committing with no changes is a no-op
// -------------------------------------------------------------------------

func TestTxEngine_EmptyCommit(t *testing.T) {
	dir := tempDir(t)
	eng := openEngine(t, dir)
	defer eng.Close()

	tx := NewTxEngine(eng)
	if err := tx.CommitOverlay(); err != nil {
		t.Fatal(err)
	}
}

// -------------------------------------------------------------------------
// WAL format — verify incomplete tx at end of file doesn't corrupt data
// -------------------------------------------------------------------------

func TestWAL_TruncatedEntryInTransaction(t *testing.T) {
	dir := tempDir(t)
	eng := openEngine(t, dir)
	eng.SetFsync(false)

	if err := eng.CreateTable("t", []ColumnDef{
		{Name: "id", DataType: TypeInteger},
	}); err != nil {
		t.Fatal(err)
	}

	// Insert a committed row.
	if _, err := eng.Insert("t", nil, [][]any{{int64(1)}}); err != nil {
		t.Fatal(err)
	}

	// Manually write begin + partial data.
	realEng := eng.(*engine)
	ts := realEng.tableStates["t"]
	if err := ts.wal.WriteBeginTx(); err != nil {
		t.Fatal(err)
	}
	// Write a valid insert.
	if err := ts.wal.WriteInsertBatchNoSync("t", []rowInsert{
		{RowID: 99, Values: []any{int64(99)}},
	}); err != nil {
		t.Fatal(err)
	}

	// Now truncate the WAL file to simulate crash mid-write.
	walPath := ts.wal.file.Name()
	info, _ := os.Stat(walPath)
	eng.Close()

	// Truncate to remove the last few bytes (corrupt the last entry).
	os.Truncate(walPath, info.Size()-3)

	// Re-open.
	eng2 := openEngine(t, dir)
	defer eng2.Close()

	it, err := eng2.Scan("t")
	if err != nil {
		t.Fatal(err)
	}
	rows := collectRows(t, it)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row after crash recovery with truncated tx, got %d", len(rows))
	}
}

// -------------------------------------------------------------------------
// NOT NULL validation on UPDATE inside transactions
// -------------------------------------------------------------------------

func TestTxEngine_UpdateNotNull(t *testing.T) {
	dir := tempDir(t)
	eng := openEngine(t, dir)
	defer eng.Close()

	if err := eng.CreateTable("users", []ColumnDef{
		{Name: "id", DataType: TypeInteger, PrimaryKey: true},
		{Name: "name", DataType: TypeText, NotNull: true},
	}); err != nil {
		t.Fatal(err)
	}

	// Insert a valid row.
	if _, err := eng.Insert("users", nil, [][]any{{int64(1), "alice"}}); err != nil {
		t.Fatal(err)
	}

	// Try to UPDATE name = NULL inside a transaction — should fail.
	tx := NewTxEngine(eng)
	_, err := tx.Update("users", map[string]any{"name": nil}, nil)
	if err == nil {
		t.Fatal("expected NOT NULL violation error, got nil")
	}
	var nnErr *NotNullViolationError
	if !errors.As(err, &nnErr) {
		t.Fatalf("expected NotNullViolationError, got %T: %v", err, err)
	}
	if nnErr.Column != "name" {
		t.Fatalf("expected column 'name', got %q", nnErr.Column)
	}
}

// -------------------------------------------------------------------------
// Multi-table crash recovery — verifies atomicity across tables.
//
// Scenario: A transaction inserts into tables A and B. The commit writes
// DML+BeginTx to both table WALs and the TxCommit record to the catalog
// WAL, but crashes BEFORE writing CommitTx to the per-table WALs.
//
// On recovery, the catalog WAL tells us both tables are committed, so
// ReplayWithTxRecovery should apply the incomplete transaction groups.
// -------------------------------------------------------------------------

func TestWAL_MultiTableCrashRecovery(t *testing.T) {
	dir := tempDir(t)
	eng := openEngine(t, dir)
	eng.SetFsync(false)

	// Create two tables.
	if err := eng.CreateTable("orders", []ColumnDef{
		{Name: "id", DataType: TypeInteger, PrimaryKey: true},
		{Name: "item", DataType: TypeText},
	}); err != nil {
		t.Fatal(err)
	}
	if err := eng.CreateTable("payments", []ColumnDef{
		{Name: "id", DataType: TypeInteger, PrimaryKey: true},
		{Name: "amount", DataType: TypeInteger},
	}); err != nil {
		t.Fatal(err)
	}

	// Pre-existing committed rows (should survive crash).
	if _, err := eng.Insert("orders", nil, [][]any{{int64(100), "existing_order"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Insert("payments", nil, [][]any{{int64(100), int64(999)}}); err != nil {
		t.Fatal(err)
	}

	// --- Simulate the commit protocol manually (without writing CommitTx) ---
	realEng := eng.(*engine)

	// Build an overlay with inserts into both tables.
	overlay := NewTxOverlay()
	// Row IDs must not conflict with existing committed rows.
	// allocateID() starts at nextID=1; after 1 committed insert per table, nextID=2.
	overlay.AddInsert("orders", 2, []any{int64(1), "widget"})
	overlay.AddInsert("orders", 3, []any{int64(2), "gadget"})
	overlay.AddInsert("payments", 2, []any{int64(1), int64(50)})

	// Phase 1: Write BeginTx + DML to each table WAL (no fsync).
	tables := overlay.TouchedTables() // deterministic order
	for _, tbl := range tables {
		ts := realEng.tableStates[tbl]
		if err := ts.wal.WriteBeginTx(); err != nil {
			t.Fatal(err)
		}
		for _, ins := range overlay.Inserts[tbl] {
			if err := ts.wal.WriteInsertBatchNoSync(tbl, []rowInsert{ins}); err != nil {
				t.Fatal(err)
			}
		}
	}

	// Phase 2: Fsync table WALs.
	for _, tbl := range tables {
		ts := realEng.tableStates[tbl]
		if err := ts.wal.Sync(); err != nil {
			t.Fatal(err)
		}
	}

	// Phase 3: Write TxCommit to catalog WAL (the atomic commit point).
	if err := realEng.catalogWAL.WriteTxCommit(tables); err != nil {
		t.Fatal(err)
	}

	// Phase 4 is SKIPPED — simulates a crash before CommitTx markers.
	eng.Close()

	// --- Recovery: re-open the engine ---
	eng2 := openEngine(t, dir)
	defer eng2.Close()

	// Verify orders table: should have existing row + 2 recovered rows.
	it, err := eng2.Scan("orders")
	if err != nil {
		t.Fatal(err)
	}
	orderRows := collectRows(t, it)
	if len(orderRows) != 3 {
		t.Fatalf("orders: expected 3 rows (1 existing + 2 recovered), got %d", len(orderRows))
	}

	// Verify payments table: should have existing row + 1 recovered row.
	it2, err := eng2.Scan("payments")
	if err != nil {
		t.Fatal(err)
	}
	paymentRows := collectRows(t, it2)
	if len(paymentRows) != 2 {
		t.Fatalf("payments: expected 2 rows (1 existing + 1 recovered), got %d", len(paymentRows))
	}

	// Verify the recovered data is correct via PK lookup.
	row, err := eng2.LookupByPK("orders", int64(1))
	if err != nil {
		t.Fatal(err)
	}
	if row == nil {
		t.Fatal("orders PK=1: expected row, got nil")
	}
	if row.Values[1] != "widget" {
		t.Fatalf("orders PK=1: expected item='widget', got %v", row.Values[1])
	}

	row2, err := eng2.LookupByPK("payments", int64(1))
	if err != nil {
		t.Fatal(err)
	}
	if row2 == nil {
		t.Fatal("payments PK=1: expected row, got nil")
	}
	if row2.Values[1] != int64(50) {
		t.Fatalf("payments PK=1: expected amount=50, got %v", row2.Values[1])
	}
}

// -------------------------------------------------------------------------
// Multi-table crash WITHOUT catalog TxCommit — should discard both tables.
//
// Scenario: crash after Phase 1 (BeginTx + DML written to table WALs)
// but BEFORE Phase 3 (no TxCommit in catalog). Both tables' uncommitted
// data should be discarded.
// -------------------------------------------------------------------------

func TestWAL_MultiTableCrashNoCommit(t *testing.T) {
	dir := tempDir(t)
	eng := openEngine(t, dir)
	eng.SetFsync(false)

	if err := eng.CreateTable("a", []ColumnDef{
		{Name: "id", DataType: TypeInteger},
	}); err != nil {
		t.Fatal(err)
	}
	if err := eng.CreateTable("b", []ColumnDef{
		{Name: "id", DataType: TypeInteger},
	}); err != nil {
		t.Fatal(err)
	}

	// One committed row in each.
	if _, err := eng.Insert("a", nil, [][]any{{int64(1)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Insert("b", nil, [][]any{{int64(1)}}); err != nil {
		t.Fatal(err)
	}

	// Write incomplete transaction groups to both table WALs.
	realEng := eng.(*engine)
	for _, tbl := range []string{"a", "b"} {
		ts := realEng.tableStates[tbl]
		if err := ts.wal.WriteBeginTx(); err != nil {
			t.Fatal(err)
		}
		if err := ts.wal.WriteInsertBatchNoSync(tbl, []rowInsert{
			{RowID: 99, Values: []any{int64(99)}},
		}); err != nil {
			t.Fatal(err)
		}
	}

	// NO TxCommit written to catalog — simulates crash before Phase 3.
	eng.Close()

	// Re-open.
	eng2 := openEngine(t, dir)
	defer eng2.Close()

	// Both tables should have only the 1 committed row each.
	for _, tbl := range []string{"a", "b"} {
		it, err := eng2.Scan(tbl)
		if err != nil {
			t.Fatal(err)
		}
		rows := collectRows(t, it)
		if len(rows) != 1 {
			t.Fatalf("table %s: expected 1 row (uncommitted discarded), got %d", tbl, len(rows))
		}
	}
}
