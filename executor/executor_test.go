package executor

import (
	"os"
	"path/filepath"
	"testing"

	"mulldb/storage"
)

func tempDir(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(os.TempDir(), "mulldb-exec-test-"+t.Name())
	os.RemoveAll(dir)
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func setup(t *testing.T) *Executor {
	t.Helper()
	eng, err := storage.Open(tempDir(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { eng.Close() })
	return New(eng)
}

func exec(t *testing.T, e *Executor, sql string) *Result {
	t.Helper()
	r, err := e.Execute(sql)
	if err != nil {
		t.Fatalf("Execute(%q): %v", sql, err)
	}
	return r
}

// -------------------------------------------------------------------------
// Full round-trip tests
// -------------------------------------------------------------------------

func TestExecutor_CreateInsertSelect(t *testing.T) {
	e := setup(t)

	r := exec(t, e, "CREATE TABLE users (id INTEGER, name TEXT, active BOOLEAN)")
	if r.Tag != "CREATE TABLE" {
		t.Errorf("tag = %q, want CREATE TABLE", r.Tag)
	}

	r = exec(t, e, "INSERT INTO users (id, name, active) VALUES (1, 'alice', TRUE), (2, 'bob', FALSE)")
	if r.Tag != "INSERT 0 2" {
		t.Errorf("tag = %q, want INSERT 0 2", r.Tag)
	}

	r = exec(t, e, "SELECT * FROM users")
	if r.Tag != "SELECT 2" {
		t.Errorf("tag = %q, want SELECT 2", r.Tag)
	}
	if len(r.Columns) != 3 {
		t.Fatalf("columns = %d, want 3", len(r.Columns))
	}
	if len(r.Rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(r.Rows))
	}
}

func TestExecutor_SelectSpecificColumns(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (a INTEGER, b TEXT, c BOOLEAN)")
	exec(t, e, "INSERT INTO t VALUES (1, 'x', TRUE)")

	r := exec(t, e, "SELECT b, a FROM t")
	if len(r.Columns) != 2 {
		t.Fatalf("columns = %d, want 2", len(r.Columns))
	}
	if r.Columns[0].Name != "b" || r.Columns[1].Name != "a" {
		t.Errorf("columns = [%s, %s], want [b, a]", r.Columns[0].Name, r.Columns[1].Name)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(r.Rows))
	}
	if string(r.Rows[0][0]) != "x" {
		t.Errorf("row[0][0] = %q, want x", r.Rows[0][0])
	}
	if string(r.Rows[0][1]) != "1" {
		t.Errorf("row[0][1] = %q, want 1", r.Rows[0][1])
	}
}

func TestExecutor_SelectWhere(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER, name TEXT)")
	exec(t, e, "INSERT INTO t VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")

	r := exec(t, e, "SELECT name FROM t WHERE id = 2")
	if len(r.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(r.Rows))
	}
	if string(r.Rows[0][0]) != "bob" {
		t.Errorf("name = %q, want bob", r.Rows[0][0])
	}
}

func TestExecutor_SelectWhereAnd(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER, name TEXT, active BOOLEAN)")
	exec(t, e, "INSERT INTO t VALUES (1, 'alice', TRUE), (2, 'bob', FALSE), (3, 'carol', TRUE)")

	r := exec(t, e, "SELECT name FROM t WHERE active = TRUE AND id > 1")
	if len(r.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(r.Rows))
	}
	if string(r.Rows[0][0]) != "carol" {
		t.Errorf("name = %q, want carol", r.Rows[0][0])
	}
}

func TestExecutor_Update(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER, name TEXT)")
	exec(t, e, "INSERT INTO t VALUES (1, 'alice'), (2, 'bob')")

	r := exec(t, e, "UPDATE t SET name = 'robert' WHERE id = 2")
	if r.Tag != "UPDATE 1" {
		t.Errorf("tag = %q, want UPDATE 1", r.Tag)
	}

	r = exec(t, e, "SELECT name FROM t WHERE id = 2")
	if len(r.Rows) != 1 || string(r.Rows[0][0]) != "robert" {
		t.Errorf("updated name = %q, want robert", r.Rows[0][0])
	}
}

func TestExecutor_Delete(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER, name TEXT)")
	exec(t, e, "INSERT INTO t VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")

	r := exec(t, e, "DELETE FROM t WHERE id = 2")
	if r.Tag != "DELETE 1" {
		t.Errorf("tag = %q, want DELETE 1", r.Tag)
	}

	r = exec(t, e, "SELECT * FROM t")
	if r.Tag != "SELECT 2" {
		t.Errorf("after delete: tag = %q, want SELECT 2", r.Tag)
	}
}

func TestExecutor_DropTable(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER)")

	r := exec(t, e, "DROP TABLE t")
	if r.Tag != "DROP TABLE" {
		t.Errorf("tag = %q, want DROP TABLE", r.Tag)
	}

	_, err := e.Execute("SELECT * FROM t")
	if err == nil {
		t.Error("SELECT from dropped table should fail")
	}
}

func TestExecutor_NullValues(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER, name TEXT)")
	exec(t, e, "INSERT INTO t VALUES (1, NULL)")

	r := exec(t, e, "SELECT * FROM t")
	if len(r.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(r.Rows))
	}
	if r.Rows[0][1] != nil {
		t.Errorf("null value = %v, want nil", r.Rows[0][1])
	}
}

func TestExecutor_TypeOIDs(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (a INTEGER, b TEXT, c BOOLEAN)")
	exec(t, e, "INSERT INTO t VALUES (1, 'x', TRUE)")

	r := exec(t, e, "SELECT * FROM t")
	if r.Columns[0].TypeOID != OIDInt8 {
		t.Errorf("a type OID = %d, want %d", r.Columns[0].TypeOID, OIDInt8)
	}
	if r.Columns[1].TypeOID != OIDText {
		t.Errorf("b type OID = %d, want %d", r.Columns[1].TypeOID, OIDText)
	}
	if r.Columns[2].TypeOID != OIDBool {
		t.Errorf("c type OID = %d, want %d", r.Columns[2].TypeOID, OIDBool)
	}
}

func TestExecutor_Errors(t *testing.T) {
	e := setup(t)

	cases := []string{
		"SELECT * FROM nonexistent",
		"DROP TABLE nonexistent",
		"INSERT INTO nonexistent VALUES (1)",
		"FROBNICATE",
	}
	for _, sql := range cases {
		if _, err := e.Execute(sql); err == nil {
			t.Errorf("expected error for %q", sql)
		}
	}
}
