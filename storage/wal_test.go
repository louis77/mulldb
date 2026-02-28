package storage

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// writeV1Entry writes a single WAL entry in the legacy (headerless) format.
func writeV1Entry(f *os.File, op byte, payload []byte) error {
	totalLen := uint32(4 + 1 + len(payload) + 4)
	entry := make([]byte, 0, totalLen)
	entry = binary.BigEndian.AppendUint32(entry, totalLen)
	entry = append(entry, op)
	entry = append(entry, payload...)
	entry = binary.BigEndian.AppendUint32(entry, crc32.ChecksumIEEE(entry[4:]))
	_, err := f.Write(entry)
	return err
}

// writeV1CreateTable writes a v1-format CREATE TABLE entry (no PK flag).
// v1 column format: [string name][byte dataType]
func writeV1CreateTable(f *os.File, table string, cols []ColumnDef) error {
	buf := encodeString(nil, table)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(cols)))
	for _, col := range cols {
		buf = encodeString(buf, col.Name)
		buf = append(buf, byte(col.DataType))
		// No PK flag byte — that's the v1 format.
	}
	return writeV1Entry(f, opCreateTable, buf)
}

// writeV1Insert writes a v1-format INSERT entry (same as v2).
func writeV1Insert(f *os.File, table string, rowID int64, values []any) error {
	buf := encodeString(nil, table)
	buf = binary.BigEndian.AppendUint64(buf, uint64(rowID))
	buf = encodeValues(buf, values)
	return writeV1Entry(f, opInsert, buf)
}

// writeV1Delete writes a v1-format DELETE entry (same as v2).
func writeV1Delete(f *os.File, table string, rowIDs []int64) error {
	buf := encodeString(nil, table)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(rowIDs)))
	for _, id := range rowIDs {
		buf = binary.BigEndian.AppendUint64(buf, uint64(id))
	}
	return writeV1Entry(f, opDelete, buf)
}

// writeV1Update writes a v1-format UPDATE entry (same as v2).
func writeV1Update(f *os.File, table string, updates []rowUpdate) error {
	buf := encodeString(nil, table)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(updates)))
	for _, u := range updates {
		buf = binary.BigEndian.AppendUint64(buf, uint64(u.RowID))
		buf = encodeValues(buf, u.Values)
	}
	return writeV1Entry(f, opUpdate, buf)
}

// createV1WAL creates a v1 WAL file at walPath with a CREATE TABLE and
// two INSERT entries.
func createV1WAL(t *testing.T, walPath string) {
	t.Helper()
	f, err := os.Create(walPath)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	cols := []ColumnDef{
		{Name: "id", DataType: TypeInteger},
		{Name: "name", DataType: TypeText},
	}
	writeV1CreateTable(f, "users", cols)
	writeV1Insert(f, "users", 1, []any{int64(1), "alice"})
	writeV1Insert(f, "users", 2, []any{int64(2), "bob"})
	f.Close()
}

func TestWAL_NewFileHasHeader(t *testing.T) {
	dir := tempDir(t)
	walPath := filepath.Join(dir, "wal.dat")
	os.MkdirAll(dir, 0755)

	w, err := OpenWAL(walPath, false)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	w.Close()

	// Read the first 6 bytes and verify header.
	f, err := os.Open(walPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()

	var hdr [walHeaderSize]byte
	if _, err := io.ReadFull(f, hdr[:]); err != nil {
		t.Fatalf("read header: %v", err)
	}
	if string(hdr[:4]) != walMagic {
		t.Errorf("magic = %q, want %q", string(hdr[:4]), walMagic)
	}
	ver := binary.BigEndian.Uint16(hdr[4:])
	if ver != walCurrentVersion {
		t.Errorf("version = %d, want %d", ver, walCurrentVersion)
	}
}

func TestWAL_MigrationNeededError(t *testing.T) {
	dir := tempDir(t)
	os.MkdirAll(dir, 0755)
	walPath := filepath.Join(dir, "wal.dat")

	createV1WAL(t, walPath)

	// Open without migrate — should fail with WALMigrationNeededError.
	_, err := OpenWAL(walPath, false)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	var migErr *WALMigrationNeededError
	if !errors.As(err, &migErr) {
		t.Fatalf("expected WALMigrationNeededError, got: %T: %v", err, err)
	}
	if migErr.CurrentVersion != 1 || migErr.RequiredVersion != walCurrentVersion {
		t.Errorf("versions = (%d, %d), want (1, %d)",
			migErr.CurrentVersion, migErr.RequiredVersion, walCurrentVersion)
	}
}

func TestWAL_MigrateV1ToV2(t *testing.T) {
	dir := tempDir(t)
	os.MkdirAll(dir, 0755)
	walPath := filepath.Join(dir, "wal.dat")

	// Create a v1 WAL (no header, old CREATE TABLE format).
	f, err := os.Create(walPath)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	cols := []ColumnDef{
		{Name: "id", DataType: TypeInteger},
		{Name: "name", DataType: TypeText},
	}
	if err := writeV1CreateTable(f, "users", cols); err != nil {
		t.Fatalf("write CREATE: %v", err)
	}
	if err := writeV1Insert(f, "users", 1, []any{int64(1), "alice"}); err != nil {
		t.Fatalf("write INSERT: %v", err)
	}
	f.Close()

	// Open with migrate=true.
	w, err := OpenWAL(walPath, true)
	if err != nil {
		t.Fatalf("OpenWAL after migration: %v", err)
	}

	// Replay and verify.
	h := &testReplayHandler{}
	if err := w.Replay(h); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	w.Close()

	if len(h.creates) != 1 {
		t.Fatalf("creates = %d, want 1", len(h.creates))
	}
	create := h.creates[0]
	if create.name != "users" {
		t.Errorf("table name = %q, want %q", create.name, "users")
	}
	if len(create.cols) != 2 {
		t.Fatalf("cols = %d, want 2", len(create.cols))
	}
	// PK flags should default to false after migration.
	for i, col := range create.cols {
		if col.PrimaryKey {
			t.Errorf("col %d (%s): PrimaryKey = true, want false", i, col.Name)
		}
	}
	if len(h.inserts) != 1 {
		t.Fatalf("inserts = %d, want 1", len(h.inserts))
	}
	ins := h.inserts[0]
	if ins.table != "users" || ins.rowID != 1 {
		t.Errorf("insert = (%q, %d), want (users, 1)", ins.table, ins.rowID)
	}

	// Verify the file now has a proper header.
	f2, _ := os.Open(walPath)
	defer f2.Close()
	var hdr [walHeaderSize]byte
	io.ReadFull(f2, hdr[:])
	if string(hdr[:4]) != walMagic {
		t.Errorf("post-migration magic = %q, want %q", string(hdr[:4]), walMagic)
	}
	ver := binary.BigEndian.Uint16(hdr[4:])
	if ver != walCurrentVersion {
		t.Errorf("post-migration version = %d, want %d", ver, walCurrentVersion)
	}
}

func TestWAL_MigratePreservesAllOps(t *testing.T) {
	dir := tempDir(t)
	os.MkdirAll(dir, 0755)
	walPath := filepath.Join(dir, "wal.dat")

	// Create a v1 WAL with all operation types.
	f, err := os.Create(walPath)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	cols := []ColumnDef{
		{Name: "id", DataType: TypeInteger},
		{Name: "name", DataType: TypeText},
		{Name: "active", DataType: TypeBoolean},
	}
	writeV1CreateTable(f, "items", cols)
	writeV1Insert(f, "items", 1, []any{int64(10), "foo", true})
	writeV1Insert(f, "items", 2, []any{int64(20), "bar", false})
	writeV1Update(f, "items", []rowUpdate{
		{RowID: 1, Values: []any{int64(10), "updated", true}},
	})
	writeV1Delete(f, "items", []int64{2})
	f.Close()

	// Open with migrate=true.
	w, err := OpenWAL(walPath, true)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}

	h := &testReplayHandler{}
	if err := w.Replay(h); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	w.Close()

	// Verify all operations survived.
	if len(h.creates) != 1 {
		t.Fatalf("creates = %d, want 1", len(h.creates))
	}
	if h.creates[0].name != "items" || len(h.creates[0].cols) != 3 {
		t.Errorf("create mismatch: %+v", h.creates[0])
	}
	if len(h.inserts) != 2 {
		t.Fatalf("inserts = %d, want 2", len(h.inserts))
	}
	if h.inserts[0].rowID != 1 || h.inserts[1].rowID != 2 {
		t.Errorf("insert rowIDs: %d, %d", h.inserts[0].rowID, h.inserts[1].rowID)
	}
	if len(h.updates) != 1 {
		t.Fatalf("updates = %d, want 1", len(h.updates))
	}
	if h.updates[0].table != "items" || len(h.updates[0].entries) != 1 {
		t.Errorf("update mismatch: %+v", h.updates[0])
	}
	if h.updates[0].entries[0].RowID != 1 {
		t.Errorf("update rowID = %d, want 1", h.updates[0].entries[0].RowID)
	}
	if len(h.deletes) != 1 {
		t.Fatalf("deletes = %d, want 1", len(h.deletes))
	}
	if h.deletes[0].table != "items" || len(h.deletes[0].rowIDs) != 1 || h.deletes[0].rowIDs[0] != 2 {
		t.Errorf("delete mismatch: %+v", h.deletes[0])
	}
}

func TestWAL_MigratePreservesBackup(t *testing.T) {
	dir := tempDir(t)
	os.MkdirAll(dir, 0755)
	walPath := filepath.Join(dir, "wal.dat")

	createV1WAL(t, walPath)

	// Record original file contents.
	origData, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("read original: %v", err)
	}

	// Migrate.
	w, err := OpenWAL(walPath, true)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	w.Close()

	// Verify .bak exists and contains the original data.
	bakPath := walPath + ".bak"
	bakData, err := os.ReadFile(bakPath)
	if err != nil {
		t.Fatalf("read backup: %v", err)
	}
	if len(bakData) != len(origData) {
		t.Errorf("backup size = %d, want %d", len(bakData), len(origData))
	}
	for i := range origData {
		if bakData[i] != origData[i] {
			t.Fatalf("backup differs at byte %d", i)
		}
	}
}

func TestWAL_MigrateBackupNumbering(t *testing.T) {
	dir := tempDir(t)
	os.MkdirAll(dir, 0755)
	walPath := filepath.Join(dir, "wal.dat")

	// Create a dummy .bak file so the first migration uses .bak.1.
	os.WriteFile(walPath+".bak", []byte("old backup"), 0644)

	createV1WAL(t, walPath)

	w, err := OpenWAL(walPath, true)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	w.Close()

	// .bak already existed, so migration should have created .bak.1
	if _, err := os.Stat(walPath + ".bak.1"); err != nil {
		t.Errorf("expected .bak.1 file: %v", err)
	}
	// Original .bak should be untouched.
	data, _ := os.ReadFile(walPath + ".bak")
	if string(data) != "old backup" {
		t.Errorf("original .bak was modified")
	}
}

func TestWAL_CurrentVersionNoMigration(t *testing.T) {
	dir := tempDir(t)
	os.MkdirAll(dir, 0755)
	walPath := filepath.Join(dir, "wal.dat")

	// Create a current-version WAL with some data.
	w, err := OpenWAL(walPath, false)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	cols := []ColumnDef{
		{Name: "id", DataType: TypeInteger, PrimaryKey: true},
		{Name: "val", DataType: TypeText},
	}
	w.WriteCreateTable("test", cols)
	w.WriteInsert("test", 1, []any{int64(1), "hello"})
	w.Close()

	// Reopen — should NOT trigger migration.
	w2, err := OpenWAL(walPath, false)
	if err != nil {
		t.Fatalf("OpenWAL reopen: %v", err)
	}

	h := &testReplayHandler{}
	if err := w2.Replay(h); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	w2.Close()

	// Verify data is intact.
	if len(h.creates) != 1 {
		t.Fatalf("creates = %d, want 1", len(h.creates))
	}
	// PK flag should be preserved.
	if !h.creates[0].cols[0].PrimaryKey {
		t.Error("PrimaryKey flag lost after reopen")
	}
	if len(h.inserts) != 1 {
		t.Fatalf("inserts = %d, want 1", len(h.inserts))
	}

	// Verify no backup file was created (migration didn't run).
	if _, err := os.Stat(walPath + ".bak"); !os.IsNotExist(err) {
		t.Error("backup file exists, but migration should not have run")
	}
}

func TestWAL_MigrateV1FullEngineReplay(t *testing.T) {
	dir := tempDir(t)
	os.MkdirAll(dir, 0755)
	walPath := filepath.Join(dir, "wal.dat")

	createV1WAL(t, walPath)

	// Open as a full engine with migrate=true.
	eng, err := Open(dir, true)
	if err != nil {
		t.Fatalf("Open engine with v1 WAL: %v", err)
	}
	defer eng.Close()

	// Verify data.
	def, ok := eng.GetTable("users")
	if !ok {
		t.Fatal("table 'users' not found after migration+replay")
	}
	if len(def.Columns) != 2 {
		t.Fatalf("columns = %d, want 2", len(def.Columns))
	}

	it, err := eng.Scan("users")
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	rows := collectRows(t, it)
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(rows))
	}
}

func TestWAL_EngineRefusesMigrationWithoutFlag(t *testing.T) {
	dir := tempDir(t)
	os.MkdirAll(dir, 0755)
	walPath := filepath.Join(dir, "wal.dat")

	createV1WAL(t, walPath)

	// Open as a full engine without migrate — should fail.
	_, err := Open(dir, false)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	var migErr *SplitWALMigrationNeededError
	if !errors.As(err, &migErr) {
		t.Fatalf("expected SplitWALMigrationNeededError, got: %T: %v", err, err)
	}
}

// -------------------------------------------------------------------------
// testReplayHandler — captures replayed entries for assertion
// -------------------------------------------------------------------------

type createRecord struct {
	name string
	cols []ColumnDef
}

type insertRecord struct {
	table string
	rowID int64
	vals  []any
}

type updateRecord struct {
	table   string
	entries []rowUpdate
}

type deleteRecord struct {
	table  string
	rowIDs []int64
}

type testReplayHandler struct {
	creates []createRecord
	inserts []insertRecord
	updates []updateRecord
	deletes []deleteRecord
}

func (h *testReplayHandler) OnCreateTable(name string, columns []ColumnDef) error {
	h.creates = append(h.creates, createRecord{name: name, cols: columns})
	return nil
}

func (h *testReplayHandler) OnDropTable(name string) error {
	return nil
}

func (h *testReplayHandler) OnAddColumn(table string, col ColumnDef) error {
	return nil
}

func (h *testReplayHandler) OnDropColumn(table string, colName string) error {
	return nil
}

func (h *testReplayHandler) OnInsert(table string, rowID int64, values []any) error {
	h.inserts = append(h.inserts, insertRecord{table: table, rowID: rowID, vals: values})
	return nil
}

func (h *testReplayHandler) OnDelete(table string, rowIDs []int64) error {
	h.deletes = append(h.deletes, deleteRecord{table: table, rowIDs: rowIDs})
	return nil
}

func (h *testReplayHandler) OnUpdate(table string, updates []rowUpdate) error {
	h.updates = append(h.updates, updateRecord{table: table, entries: updates})
	return nil
}

func (h *testReplayHandler) OnCreateIndex(string, IndexDef) error { return nil }
func (h *testReplayHandler) OnDropIndex(string, string) error     { return nil }
