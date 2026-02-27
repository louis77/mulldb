package executor

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

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
	eng, err := storage.Open(tempDir(t), false)
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

func TestExecutor_SQLSTATECodes(t *testing.T) {
	e := setup(t)

	// Parse error → 42601 (syntax_error).
	_, err := e.Execute("FROBNICATE")
	assertSQLSTATE(t, err, "42601")

	// Undefined table → 42P01.
	_, err = e.Execute("SELECT * FROM nonexistent")
	assertSQLSTATE(t, err, "42P01")

	// Duplicate table → 42P07.
	exec(t, e, "CREATE TABLE t (id INTEGER)")
	_, err = e.Execute("CREATE TABLE t (id INTEGER)")
	assertSQLSTATE(t, err, "42P07")

	// Drop nonexistent → 42P01.
	_, err = e.Execute("DROP TABLE nonexistent")
	assertSQLSTATE(t, err, "42P01")
}

// -------------------------------------------------------------------------
// Static SELECT (no FROM)
// -------------------------------------------------------------------------

func TestExecutor_StaticSelect_IntLit(t *testing.T) {
	e := setup(t)
	r := exec(t, e, "SELECT 1")
	if len(r.Columns) != 1 {
		t.Fatalf("columns = %d, want 1", len(r.Columns))
	}
	if r.Columns[0].Name != "?column?" {
		t.Errorf("col name = %q, want ?column?", r.Columns[0].Name)
	}
	if r.Columns[0].TypeOID != OIDInt8 {
		t.Errorf("col OID = %d, want %d (OIDInt8)", r.Columns[0].TypeOID, OIDInt8)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(r.Rows))
	}
	if string(r.Rows[0][0]) != "1" {
		t.Errorf("value = %q, want 1", r.Rows[0][0])
	}
}

func TestExecutor_StaticSelect_MultipleTypes(t *testing.T) {
	e := setup(t)
	r := exec(t, e, "SELECT 42, 'hello', TRUE, NULL")
	if len(r.Columns) != 4 {
		t.Fatalf("columns = %d, want 4", len(r.Columns))
	}
	if string(r.Rows[0][0]) != "42" {
		t.Errorf("int = %q, want 42", r.Rows[0][0])
	}
	if string(r.Rows[0][1]) != "hello" {
		t.Errorf("str = %q, want hello", r.Rows[0][1])
	}
	if string(r.Rows[0][2]) != "t" {
		t.Errorf("bool = %q, want t", r.Rows[0][2])
	}
	if r.Rows[0][3] != nil {
		t.Errorf("null = %v, want nil", r.Rows[0][3])
	}
}

func TestExecutor_StaticSelect_Version(t *testing.T) {
	e := setup(t)
	r := exec(t, e, "SELECT VERSION()")
	if len(r.Columns) != 1 {
		t.Fatalf("columns = %d, want 1", len(r.Columns))
	}
	if r.Columns[0].Name != "version" {
		t.Errorf("col name = %q, want version", r.Columns[0].Name)
	}
	if r.Columns[0].TypeOID != OIDText {
		t.Errorf("col OID = %d, want %d (OIDText)", r.Columns[0].TypeOID, OIDText)
	}
	if len(r.Rows) != 1 || len(r.Rows[0][0]) == 0 {
		t.Fatal("expected non-empty version string")
	}
	v := string(r.Rows[0][0])
	if !startsWith(v, "PostgreSQL") {
		t.Errorf("version = %q, want prefix PostgreSQL", v)
	}
}

func TestExecutor_StaticSelect_UnknownFunction(t *testing.T) {
	e := setup(t)
	_, err := e.Execute("SELECT FROBNICATE()")
	assertSQLSTATE(t, err, "42883")
}

func TestExecutor_StaticSelect_VersionWithArgs(t *testing.T) {
	e := setup(t)
	_, err := e.Execute("SELECT VERSION(1)")
	assertSQLSTATE(t, err, "42883")
}

func startsWith(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

// -------------------------------------------------------------------------
// Aggregate functions
// -------------------------------------------------------------------------

func TestExecutor_Aggregate_CountStar(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (val INTEGER)")
	exec(t, e, "INSERT INTO t VALUES (1), (3), (2), (4), (0)")

	r := exec(t, e, "SELECT COUNT(*) FROM t")
	if len(r.Columns) != 1 {
		t.Fatalf("columns = %d, want 1", len(r.Columns))
	}
	if r.Columns[0].Name != "count" {
		t.Errorf("col name = %q, want count", r.Columns[0].Name)
	}
	if r.Columns[0].TypeOID != OIDInt8 {
		t.Errorf("col OID = %d, want %d", r.Columns[0].TypeOID, OIDInt8)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(r.Rows))
	}
	if string(r.Rows[0][0]) != "5" {
		t.Errorf("count = %q, want 5", r.Rows[0][0])
	}
}

func TestExecutor_Aggregate_Sum(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (val INTEGER)")
	exec(t, e, "INSERT INTO t VALUES (1), (3), (2), (4), (0)")

	r := exec(t, e, "SELECT SUM(val) FROM t")
	if len(r.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(r.Rows))
	}
	if string(r.Rows[0][0]) != "10" {
		t.Errorf("sum = %q, want 10", r.Rows[0][0])
	}
}

func TestExecutor_Aggregate_Min(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (val INTEGER)")
	exec(t, e, "INSERT INTO t VALUES (5), (2), (8), (1), (4)")

	r := exec(t, e, "SELECT MIN(val) FROM t")
	if len(r.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(r.Rows))
	}
	if string(r.Rows[0][0]) != "1" {
		t.Errorf("min = %q, want 1", r.Rows[0][0])
	}
}

func TestExecutor_Aggregate_Max(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (val INTEGER)")
	exec(t, e, "INSERT INTO t VALUES (5), (2), (8), (1), (4)")

	r := exec(t, e, "SELECT MAX(val) FROM t")
	if len(r.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(r.Rows))
	}
	if string(r.Rows[0][0]) != "8" {
		t.Errorf("max = %q, want 8", r.Rows[0][0])
	}
}

func TestExecutor_Aggregate_MultipleAggregates(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (val INTEGER)")
	exec(t, e, "INSERT INTO t VALUES (1), (3), (2), (4), (0)")

	r := exec(t, e, "SELECT COUNT(*), SUM(val) FROM t")
	if len(r.Columns) != 2 {
		t.Fatalf("columns = %d, want 2", len(r.Columns))
	}
	if len(r.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(r.Rows))
	}
	if string(r.Rows[0][0]) != "5" {
		t.Errorf("count = %q, want 5", r.Rows[0][0])
	}
	if string(r.Rows[0][1]) != "10" {
		t.Errorf("sum = %q, want 10", r.Rows[0][1])
	}
}

func TestExecutor_Aggregate_EmptyTable(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (val INTEGER)")

	r := exec(t, e, "SELECT COUNT(*) FROM t")
	if string(r.Rows[0][0]) != "0" {
		t.Errorf("count of empty table = %q, want 0", r.Rows[0][0])
	}

	r = exec(t, e, "SELECT SUM(val) FROM t")
	if string(r.Rows[0][0]) != "0" {
		t.Errorf("sum of empty table = %q, want 0", r.Rows[0][0])
	}
}

func TestExecutor_Aggregate_MixedError(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER, val INTEGER)")
	exec(t, e, "INSERT INTO t VALUES (1, 10)")

	_, err := e.Execute("SELECT id, SUM(val) FROM t")
	if err == nil {
		t.Fatal("expected error for mixed aggregate + non-aggregate")
	}
	var qe *QueryError
	if !errors.As(err, &qe) {
		t.Fatalf("expected QueryError, got %T", err)
	}
	if qe.Code != "42803" {
		t.Errorf("SQLSTATE = %q, want 42803", qe.Code)
	}
}

// -------------------------------------------------------------------------
// AS alias
// -------------------------------------------------------------------------

func TestExecutor_SelectColumnAlias(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER, name TEXT)")
	exec(t, e, "INSERT INTO t VALUES (1, 'alice')")

	r := exec(t, e, "SELECT id AS user_id, name AS user_name FROM t")
	if len(r.Columns) != 2 {
		t.Fatalf("columns = %d, want 2", len(r.Columns))
	}
	if r.Columns[0].Name != "user_id" {
		t.Errorf("col[0] = %q, want user_id", r.Columns[0].Name)
	}
	if r.Columns[1].Name != "user_name" {
		t.Errorf("col[1] = %q, want user_name", r.Columns[1].Name)
	}
	if string(r.Rows[0][0]) != "1" {
		t.Errorf("row[0][0] = %q, want 1", r.Rows[0][0])
	}
	if string(r.Rows[0][1]) != "alice" {
		t.Errorf("row[0][1] = %q, want alice", r.Rows[0][1])
	}
}

func TestExecutor_AggregateAlias(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (val INTEGER)")
	exec(t, e, "INSERT INTO t VALUES (1), (2), (3)")

	r := exec(t, e, "SELECT COUNT(*) AS total, SUM(val) AS total_val FROM t")
	if len(r.Columns) != 2 {
		t.Fatalf("columns = %d, want 2", len(r.Columns))
	}
	if r.Columns[0].Name != "total" {
		t.Errorf("col[0] = %q, want total", r.Columns[0].Name)
	}
	if r.Columns[1].Name != "total_val" {
		t.Errorf("col[1] = %q, want total_val", r.Columns[1].Name)
	}
	if string(r.Rows[0][0]) != "3" {
		t.Errorf("count = %q, want 3", r.Rows[0][0])
	}
	if string(r.Rows[0][1]) != "6" {
		t.Errorf("sum = %q, want 6", r.Rows[0][1])
	}
}

func TestExecutor_StaticSelectAlias(t *testing.T) {
	e := setup(t)
	r := exec(t, e, "SELECT 1 AS num, 'hello' AS greeting")
	if len(r.Columns) != 2 {
		t.Fatalf("columns = %d, want 2", len(r.Columns))
	}
	if r.Columns[0].Name != "num" {
		t.Errorf("col[0] = %q, want num", r.Columns[0].Name)
	}
	if r.Columns[1].Name != "greeting" {
		t.Errorf("col[1] = %q, want greeting", r.Columns[1].Name)
	}
}

// -------------------------------------------------------------------------
// Double-quoted identifiers
// -------------------------------------------------------------------------

func TestExecutor_QuotedIdentifiers(t *testing.T) {
	e := setup(t)

	// Create table with a reserved-word name and reserved-word columns.
	exec(t, e, `CREATE TABLE "table" ("select" INTEGER, "from" TEXT)`)

	// Insert using quoted identifiers.
	exec(t, e, `INSERT INTO "table" ("select", "from") VALUES (1, 'hello'), (2, 'world')`)

	// Select using quoted table and column names.
	r := exec(t, e, `SELECT "select", "from" FROM "table"`)
	if len(r.Columns) != 2 {
		t.Fatalf("columns = %d, want 2", len(r.Columns))
	}
	if r.Columns[0].Name != "select" {
		t.Errorf("col[0] = %q, want select", r.Columns[0].Name)
	}
	if r.Columns[1].Name != "from" {
		t.Errorf("col[1] = %q, want from", r.Columns[1].Name)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(r.Rows))
	}
	if string(r.Rows[0][0]) != "1" {
		t.Errorf("row[0][0] = %q, want 1", r.Rows[0][0])
	}
	if string(r.Rows[0][1]) != "hello" {
		t.Errorf("row[0][1] = %q, want hello", r.Rows[0][1])
	}

	// COUNT with quoted table name.
	r = exec(t, e, `SELECT COUNT(*) FROM "table"`)
	if string(r.Rows[0][0]) != "2" {
		t.Errorf("count = %q, want 2", r.Rows[0][0])
	}
}

// -------------------------------------------------------------------------
// LIMIT / OFFSET
// -------------------------------------------------------------------------

func TestExecutor_SelectLimit(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER, name TEXT)")
	exec(t, e, "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')")

	r := exec(t, e, "SELECT * FROM t LIMIT 3")
	if len(r.Rows) != 3 {
		t.Fatalf("rows = %d, want 3", len(r.Rows))
	}
	if r.Tag != "SELECT 3" {
		t.Errorf("tag = %q, want SELECT 3", r.Tag)
	}
}

func TestExecutor_SelectOffset(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER, name TEXT)")
	exec(t, e, "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')")

	r := exec(t, e, "SELECT * FROM t OFFSET 2")
	if len(r.Rows) != 3 {
		t.Fatalf("rows = %d, want 3", len(r.Rows))
	}
}

func TestExecutor_SelectLimitOffset(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER, name TEXT)")
	exec(t, e, "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')")

	r := exec(t, e, "SELECT * FROM t LIMIT 2 OFFSET 1")
	if len(r.Rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(r.Rows))
	}
}

func TestExecutor_SelectLimitZero(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER)")
	exec(t, e, "INSERT INTO t VALUES (1), (2), (3)")

	r := exec(t, e, "SELECT * FROM t LIMIT 0")
	if len(r.Rows) != 0 {
		t.Fatalf("rows = %d, want 0", len(r.Rows))
	}
	if r.Tag != "SELECT 0" {
		t.Errorf("tag = %q, want SELECT 0", r.Tag)
	}
}

func TestExecutor_SelectOffsetBeyondRows(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER)")
	exec(t, e, "INSERT INTO t VALUES (1), (2), (3)")

	r := exec(t, e, "SELECT * FROM t OFFSET 100")
	if len(r.Rows) != 0 {
		t.Fatalf("rows = %d, want 0", len(r.Rows))
	}
}

func TestExecutor_SelectLimitExceedsRows(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER)")
	exec(t, e, "INSERT INTO t VALUES (1), (2), (3)")

	r := exec(t, e, "SELECT * FROM t LIMIT 100")
	if len(r.Rows) != 3 {
		t.Fatalf("rows = %d, want 3", len(r.Rows))
	}
}

func TestExecutor_SelectOffsetWithoutLimit(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER)")
	exec(t, e, "INSERT INTO t VALUES (1), (2), (3), (4), (5)")

	r := exec(t, e, "SELECT * FROM t OFFSET 3")
	if len(r.Rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(r.Rows))
	}
}

func TestExecutor_SelectLimitWithWhere(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER, name TEXT)")
	exec(t, e, "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')")

	// WHERE id > 1 matches 4 rows; LIMIT 2 returns only 2.
	r := exec(t, e, "SELECT * FROM t WHERE id > 1 LIMIT 2")
	if len(r.Rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(r.Rows))
	}
}

func TestExecutor_SelectNegativeLimit(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER)")
	exec(t, e, "INSERT INTO t VALUES (1)")

	// Negative values are rejected at parse time (lexer produces ILLEGAL for '-').
	_, err := e.Execute("SELECT * FROM t LIMIT -1")
	if err == nil {
		t.Fatal("expected error for negative LIMIT")
	}
}

func TestExecutor_SelectNegativeOffset(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER)")
	exec(t, e, "INSERT INTO t VALUES (1)")

	_, err := e.Execute("SELECT * FROM t OFFSET -1")
	if err == nil {
		t.Fatal("expected error for negative OFFSET")
	}
}

// -------------------------------------------------------------------------
// Primary Key
// -------------------------------------------------------------------------

func TestExecutor_PrimaryKey_CreateInsertSelect(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	exec(t, e, "INSERT INTO users VALUES (1, 'alice'), (2, 'bob')")

	r := exec(t, e, "SELECT * FROM users")
	if len(r.Rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(r.Rows))
	}
}

func TestExecutor_PrimaryKey_DuplicateInsert_23505(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	exec(t, e, "INSERT INTO users VALUES (1, 'alice')")

	_, err := e.Execute("INSERT INTO users VALUES (1, 'bob')")
	assertSQLSTATE(t, err, "23505")
}

func TestExecutor_PrimaryKey_NullPK_23505(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")

	_, err := e.Execute("INSERT INTO users VALUES (NULL, 'alice')")
	assertSQLSTATE(t, err, "23505")
}

func TestExecutor_PrimaryKey_UpdateViolation_23505(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	exec(t, e, "INSERT INTO users VALUES (1, 'alice'), (2, 'bob')")

	_, err := e.Execute("UPDATE users SET id = 1 WHERE id = 2")
	assertSQLSTATE(t, err, "23505")
}

func TestExecutor_PrimaryKey_IndexedLookup(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	exec(t, e, "INSERT INTO users VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")

	// Simple equality on PK column should use the index (result should be correct).
	r := exec(t, e, "SELECT name FROM users WHERE id = 2")
	if len(r.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(r.Rows))
	}
	if string(r.Rows[0][0]) != "bob" {
		t.Errorf("name = %q, want bob", r.Rows[0][0])
	}
}

func TestExecutor_PrimaryKey_IndexedLookupNotFound(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	exec(t, e, "INSERT INTO users VALUES (1, 'alice')")

	r := exec(t, e, "SELECT * FROM users WHERE id = 99")
	if len(r.Rows) != 0 {
		t.Fatalf("rows = %d, want 0", len(r.Rows))
	}
}

func TestExecutor_PrimaryKey_NonEqualityFallsBackToScan(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	exec(t, e, "INSERT INTO users VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')")

	// Range query — can't use PK index, falls back to scan.
	r := exec(t, e, "SELECT * FROM users WHERE id > 3")
	if len(r.Rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(r.Rows))
	}
}

func TestExecutor_PrimaryKey_DeleteAndReinsert(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	exec(t, e, "INSERT INTO users VALUES (1, 'alice')")
	exec(t, e, "DELETE FROM users WHERE id = 1")

	// Should be able to reinsert same PK.
	exec(t, e, "INSERT INTO users VALUES (1, 'bob')")

	r := exec(t, e, "SELECT name FROM users WHERE id = 1")
	if len(r.Rows) != 1 || string(r.Rows[0][0]) != "bob" {
		t.Errorf("after reinsert: got %v", r.Rows)
	}
}

func TestExecutor_PrimaryKey_TextKey(t *testing.T) {
	e := setup(t)
	exec(t, e, `CREATE TABLE codes (code TEXT PRIMARY KEY, "desc" TEXT)`)
	exec(t, e, "INSERT INTO codes VALUES ('US', 'United States'), ('UK', 'United Kingdom')")

	_, err := e.Execute("INSERT INTO codes VALUES ('US', 'duplicate')")
	assertSQLSTATE(t, err, "23505")

	r := exec(t, e, `SELECT "desc" FROM codes WHERE code = 'UK'`)
	if len(r.Rows) != 1 || string(r.Rows[0][0]) != "United Kingdom" {
		t.Errorf("text PK lookup: got %v", r.Rows)
	}
}

func TestExecutor_PrimaryKey_LimitWithIndex(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	exec(t, e, "INSERT INTO users VALUES (1, 'alice')")

	r := exec(t, e, "SELECT * FROM users WHERE id = 1 LIMIT 0")
	if len(r.Rows) != 0 {
		t.Fatalf("rows = %d, want 0 with LIMIT 0", len(r.Rows))
	}

	r = exec(t, e, "SELECT * FROM users WHERE id = 1 OFFSET 1")
	if len(r.Rows) != 0 {
		t.Fatalf("rows = %d, want 0 with OFFSET 1", len(r.Rows))
	}
}

// -------------------------------------------------------------------------
// ExecuteTraced
// -------------------------------------------------------------------------

func TestExecuteTraced_Select(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
	exec(t, e, "INSERT INTO t VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")

	result, tr, err := e.ExecuteTraced("SELECT * FROM t")
	if err != nil {
		t.Fatalf("ExecuteTraced: %v", err)
	}
	if len(result.Rows) != 3 {
		t.Fatalf("rows = %d, want 3", len(result.Rows))
	}
	if tr.StmtType != "SELECT" {
		t.Errorf("StmtType = %q, want SELECT", tr.StmtType)
	}
	if tr.Table != "t" {
		t.Errorf("Table = %q, want t", tr.Table)
	}
	if tr.Total == 0 {
		t.Error("Total duration should be non-zero")
	}
	if tr.Parse == 0 {
		t.Error("Parse duration should be non-zero")
	}
	if tr.RowsScanned != 3 {
		t.Errorf("RowsScanned = %d, want 3", tr.RowsScanned)
	}
	if tr.RowsReturned != 3 {
		t.Errorf("RowsReturned = %d, want 3", tr.RowsReturned)
	}
}

func TestExecuteTraced_SelectWithPKLookup(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
	exec(t, e, "INSERT INTO t VALUES (1, 'alice'), (2, 'bob')")

	_, tr, err := e.ExecuteTraced("SELECT name FROM t WHERE id = 1")
	if err != nil {
		t.Fatalf("ExecuteTraced: %v", err)
	}
	if !tr.UsedIndex {
		t.Error("expected UsedIndex = true for PK equality lookup")
	}
	if tr.RowsScanned != 1 {
		t.Errorf("RowsScanned = %d, want 1", tr.RowsScanned)
	}
	if tr.RowsReturned != 1 {
		t.Errorf("RowsReturned = %d, want 1", tr.RowsReturned)
	}
}

func TestExecuteTraced_Insert(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER, name TEXT)")

	result, tr, err := e.ExecuteTraced("INSERT INTO t VALUES (1, 'alice'), (2, 'bob')")
	if err != nil {
		t.Fatalf("ExecuteTraced: %v", err)
	}
	if result.Tag != "INSERT 0 2" {
		t.Errorf("tag = %q, want INSERT 0 2", result.Tag)
	}
	if tr.StmtType != "INSERT" {
		t.Errorf("StmtType = %q, want INSERT", tr.StmtType)
	}
	if tr.Total == 0 {
		t.Error("Total duration should be non-zero")
	}
}

func TestExecuteTraced_CreateTable(t *testing.T) {
	e := setup(t)

	_, tr, err := e.ExecuteTraced("CREATE TABLE t (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("ExecuteTraced: %v", err)
	}
	if tr.StmtType != "CREATE TABLE" {
		t.Errorf("StmtType = %q, want CREATE TABLE", tr.StmtType)
	}
	if tr.Table != "t" {
		t.Errorf("Table = %q, want t", tr.Table)
	}
}

func TestTraceToResult(t *testing.T) {
	// nil trace returns "no trace available".
	r := TraceToResult(nil)
	if len(r.Rows) != 1 {
		t.Fatalf("nil trace rows = %d, want 1", len(r.Rows))
	}
	if string(r.Rows[0][0]) != "no trace available" {
		t.Errorf("nil trace message = %q", r.Rows[0][0])
	}

	// Non-nil trace returns timing rows.
	tr := &Trace{
		StmtType: "SELECT",
		Table:    "users",
	}
	r = TraceToResult(tr)
	if len(r.Columns) != 2 {
		t.Fatalf("columns = %d, want 2", len(r.Columns))
	}
	if r.Columns[0].Name != "step" {
		t.Errorf("col[0] = %q, want step", r.Columns[0].Name)
	}
	if r.Columns[1].Name != "duration" {
		t.Errorf("col[1] = %q, want duration", r.Columns[1].Name)
	}
	// Should have at least Parse, Plan, Execute, Total, Statement, Table, Rows Scanned, Rows Returned.
	if len(r.Rows) < 8 {
		t.Errorf("rows = %d, want at least 8", len(r.Rows))
	}

	// With Sort > 0, should include a Sort row.
	tr2 := &Trace{
		StmtType: "SELECT",
		Table:    "users",
		Sort:     5 * time.Microsecond,
	}
	r2 := TraceToResult(tr2)
	hasSortRow := false
	for _, row := range r2.Rows {
		if string(row[0]) == "Sort" {
			hasSortRow = true
			break
		}
	}
	if !hasSortRow {
		t.Error("expected Sort row in trace when Sort > 0")
	}
}

func TestExecutor_SelectLiterals(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE lit (id INTEGER PRIMARY KEY, name TEXT)")
	exec(t, e, "INSERT INTO lit VALUES (1, 'alice')")
	exec(t, e, "INSERT INTO lit VALUES (2, 'bob')")

	t.Run("string literal with column", func(t *testing.T) {
		r := exec(t, e, "SELECT id, 'hello' FROM lit")
		if len(r.Columns) != 2 {
			t.Fatalf("columns = %d, want 2", len(r.Columns))
		}
		if r.Columns[1].Name != "?column?" {
			t.Errorf("col[1].Name = %q, want ?column?", r.Columns[1].Name)
		}
		if len(r.Rows) != 2 {
			t.Fatalf("rows = %d, want 2", len(r.Rows))
		}
		if string(r.Rows[0][1]) != "hello" {
			t.Errorf("row[0][1] = %q, want hello", r.Rows[0][1])
		}
	})

	t.Run("integer literal with column", func(t *testing.T) {
		r := exec(t, e, "SELECT 42, name FROM lit WHERE id = 1")
		if len(r.Columns) != 2 {
			t.Fatalf("columns = %d, want 2", len(r.Columns))
		}
		if string(r.Rows[0][0]) != "42" {
			t.Errorf("row[0][0] = %q, want 42", r.Rows[0][0])
		}
		if string(r.Rows[0][1]) != "alice" {
			t.Errorf("row[0][1] = %q, want alice", r.Rows[0][1])
		}
	})

	t.Run("bool literal", func(t *testing.T) {
		r := exec(t, e, "SELECT id, true, false FROM lit WHERE id = 1")
		if len(r.Columns) != 3 {
			t.Fatalf("columns = %d, want 3", len(r.Columns))
		}
		if string(r.Rows[0][1]) != "t" {
			t.Errorf("row[0][1] = %q, want t", r.Rows[0][1])
		}
		if string(r.Rows[0][2]) != "f" {
			t.Errorf("row[0][2] = %q, want f", r.Rows[0][2])
		}
	})

	t.Run("alias on literal", func(t *testing.T) {
		r := exec(t, e, "SELECT 'constant' AS label, id FROM lit")
		if r.Columns[0].Name != "label" {
			t.Errorf("col[0].Name = %q, want label", r.Columns[0].Name)
		}
		if string(r.Rows[0][0]) != "constant" {
			t.Errorf("row[0][0] = %q, want constant", r.Rows[0][0])
		}
	})

	t.Run("literal with PK lookup", func(t *testing.T) {
		r := exec(t, e, "SELECT id, 'found' FROM lit WHERE id = 1")
		if len(r.Rows) != 1 {
			t.Fatalf("rows = %d, want 1", len(r.Rows))
		}
		if string(r.Rows[0][0]) != "1" {
			t.Errorf("row[0][0] = %q, want 1", r.Rows[0][0])
		}
		if string(r.Rows[0][1]) != "found" {
			t.Errorf("row[0][1] = %q, want found", r.Rows[0][1])
		}
	})

	t.Run("star with literal", func(t *testing.T) {
		r := exec(t, e, "SELECT *, true FROM lit WHERE id = 1")
		if len(r.Columns) != 3 {
			t.Fatalf("columns = %d, want 3", len(r.Columns))
		}
		if r.Columns[0].Name != "id" {
			t.Errorf("col[0].Name = %q, want id", r.Columns[0].Name)
		}
		if r.Columns[1].Name != "name" {
			t.Errorf("col[1].Name = %q, want name", r.Columns[1].Name)
		}
		if r.Columns[2].Name != "?column?" {
			t.Errorf("col[2].Name = %q, want ?column?", r.Columns[2].Name)
		}
		if len(r.Rows) != 1 {
			t.Fatalf("rows = %d, want 1", len(r.Rows))
		}
		if string(r.Rows[0][2]) != "t" {
			t.Errorf("row[0][2] = %q, want t", r.Rows[0][2])
		}
	})
}

// -------------------------------------------------------------------------
// ORDER BY
// -------------------------------------------------------------------------

func TestExecutor_OrderBy_IntegerAsc(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER, name TEXT)")
	exec(t, e, "INSERT INTO t VALUES (3, 'c'), (1, 'a'), (2, 'b')")

	r := exec(t, e, "SELECT * FROM t ORDER BY id")
	if len(r.Rows) != 3 {
		t.Fatalf("rows = %d, want 3", len(r.Rows))
	}
	if string(r.Rows[0][0]) != "1" || string(r.Rows[1][0]) != "2" || string(r.Rows[2][0]) != "3" {
		t.Errorf("order = [%s, %s, %s], want [1, 2, 3]",
			r.Rows[0][0], r.Rows[1][0], r.Rows[2][0])
	}
}

func TestExecutor_OrderBy_IntegerDesc(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER, name TEXT)")
	exec(t, e, "INSERT INTO t VALUES (3, 'c'), (1, 'a'), (2, 'b')")

	r := exec(t, e, "SELECT * FROM t ORDER BY id DESC")
	if string(r.Rows[0][0]) != "3" || string(r.Rows[1][0]) != "2" || string(r.Rows[2][0]) != "1" {
		t.Errorf("order = [%s, %s, %s], want [3, 2, 1]",
			r.Rows[0][0], r.Rows[1][0], r.Rows[2][0])
	}
}

func TestExecutor_OrderBy_TextAsc(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (name TEXT)")
	exec(t, e, "INSERT INTO t VALUES ('charlie'), ('alice'), ('bob')")

	r := exec(t, e, "SELECT * FROM t ORDER BY name")
	if string(r.Rows[0][0]) != "alice" || string(r.Rows[1][0]) != "bob" || string(r.Rows[2][0]) != "charlie" {
		t.Errorf("order = [%s, %s, %s], want [alice, bob, charlie]",
			r.Rows[0][0], r.Rows[1][0], r.Rows[2][0])
	}
}

func TestExecutor_OrderBy_TextDesc(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (name TEXT)")
	exec(t, e, "INSERT INTO t VALUES ('charlie'), ('alice'), ('bob')")

	r := exec(t, e, "SELECT * FROM t ORDER BY name DESC")
	if string(r.Rows[0][0]) != "charlie" || string(r.Rows[1][0]) != "bob" || string(r.Rows[2][0]) != "alice" {
		t.Errorf("order = [%s, %s, %s], want [charlie, bob, alice]",
			r.Rows[0][0], r.Rows[1][0], r.Rows[2][0])
	}
}

func TestExecutor_OrderBy_MultiColumn(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (score INTEGER, name TEXT)")
	exec(t, e, "INSERT INTO t VALUES (90, 'charlie'), (90, 'alice'), (70, 'dave'), (80, 'bob')")

	r := exec(t, e, "SELECT * FROM t ORDER BY score DESC, name")
	// Expected: (90,alice), (90,charlie), (80,bob), (70,dave)
	want := []struct{ score, name string }{
		{"90", "alice"}, {"90", "charlie"}, {"80", "bob"}, {"70", "dave"},
	}
	if len(r.Rows) != 4 {
		t.Fatalf("rows = %d, want 4", len(r.Rows))
	}
	for i, w := range want {
		if string(r.Rows[i][0]) != w.score || string(r.Rows[i][1]) != w.name {
			t.Errorf("row[%d] = (%s, %s), want (%s, %s)",
				i, r.Rows[i][0], r.Rows[i][1], w.score, w.name)
		}
	}
}

func TestExecutor_OrderBy_NullsLast(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER, score INTEGER)")
	exec(t, e, "INSERT INTO t VALUES (1, 90), (2, NULL), (3, 70), (4, NULL)")

	// ASC: NULLs should be last.
	r := exec(t, e, "SELECT * FROM t ORDER BY score")
	if len(r.Rows) != 4 {
		t.Fatalf("rows = %d, want 4", len(r.Rows))
	}
	if string(r.Rows[0][1]) != "70" {
		t.Errorf("row[0].score = %q, want 70", r.Rows[0][1])
	}
	if string(r.Rows[1][1]) != "90" {
		t.Errorf("row[1].score = %q, want 90", r.Rows[1][1])
	}
	if r.Rows[2][1] != nil {
		t.Errorf("row[2].score = %q, want NULL", r.Rows[2][1])
	}
	if r.Rows[3][1] != nil {
		t.Errorf("row[3].score = %q, want NULL", r.Rows[3][1])
	}

	// DESC: NULLs should still be last.
	r = exec(t, e, "SELECT * FROM t ORDER BY score DESC")
	if string(r.Rows[0][1]) != "90" {
		t.Errorf("desc row[0].score = %q, want 90", r.Rows[0][1])
	}
	if string(r.Rows[1][1]) != "70" {
		t.Errorf("desc row[1].score = %q, want 70", r.Rows[1][1])
	}
	if r.Rows[2][1] != nil {
		t.Errorf("desc row[2].score = %q, want NULL", r.Rows[2][1])
	}
	if r.Rows[3][1] != nil {
		t.Errorf("desc row[3].score = %q, want NULL", r.Rows[3][1])
	}
}

func TestExecutor_OrderBy_WithLimitOffset(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER, name TEXT)")
	exec(t, e, "INSERT INTO t VALUES (3, 'c'), (1, 'a'), (5, 'e'), (2, 'b'), (4, 'd')")

	r := exec(t, e, "SELECT * FROM t ORDER BY id LIMIT 2 OFFSET 1")
	if len(r.Rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(r.Rows))
	}
	// Sorted: 1,2,3,4,5; offset 1 → skip 1; limit 2 → [2, 3]
	if string(r.Rows[0][0]) != "2" || string(r.Rows[1][0]) != "3" {
		t.Errorf("order = [%s, %s], want [2, 3]", r.Rows[0][0], r.Rows[1][0])
	}
}

func TestExecutor_OrderBy_NonexistentColumn(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER, name TEXT)")
	exec(t, e, "INSERT INTO t VALUES (1, 'a')")

	_, err := e.Execute("SELECT * FROM t ORDER BY nonexistent")
	if err == nil {
		t.Fatal("expected error for ORDER BY nonexistent column")
	}
}

func TestExecuteTraced_OrderBy(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER, name TEXT)")
	exec(t, e, "INSERT INTO t VALUES (3, 'c'), (1, 'a'), (2, 'b')")

	result, tr, err := e.ExecuteTraced("SELECT * FROM t ORDER BY id")
	if err != nil {
		t.Fatalf("ExecuteTraced: %v", err)
	}
	if len(result.Rows) != 3 {
		t.Fatalf("rows = %d, want 3", len(result.Rows))
	}
	if tr.Sort == 0 {
		t.Error("Sort duration should be non-zero for ORDER BY query")
	}
	// Verify sorted order.
	if string(result.Rows[0][0]) != "1" || string(result.Rows[1][0]) != "2" || string(result.Rows[2][0]) != "3" {
		t.Errorf("order = [%s, %s, %s], want [1, 2, 3]",
			result.Rows[0][0], result.Rows[1][0], result.Rows[2][0])
	}
}

func TestExecuteTraced_NoOrderBy_ZeroSort(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER)")
	exec(t, e, "INSERT INTO t VALUES (1), (2)")

	_, tr, err := e.ExecuteTraced("SELECT * FROM t")
	if err != nil {
		t.Fatalf("ExecuteTraced: %v", err)
	}
	if tr.Sort != 0 {
		t.Errorf("Sort = %v, want 0 for query without ORDER BY", tr.Sort)
	}
}

func TestExecutor_OrderBy_AggregateError(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (val INTEGER)")
	exec(t, e, "INSERT INTO t VALUES (1), (2), (3)")

	_, err := e.Execute("SELECT COUNT(*) FROM t ORDER BY val")
	if err == nil {
		t.Fatal("expected error for ORDER BY with aggregates")
	}
	assertSQLSTATE(t, err, "0A000")
}

func assertSQLSTATE(t *testing.T, err error, expected string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error with SQLSTATE %s, got nil", expected)
	}
	var qe *QueryError
	if !errors.As(err, &qe) {
		t.Fatalf("expected QueryError, got %T: %v", err, err)
	}
	if qe.Code != expected {
		t.Errorf("SQLSTATE = %q, want %q (message: %s)", qe.Code, expected, qe.Message)
	}
}

// -------------------------------------------------------------------------
// IS NULL / IS NOT NULL and NULL comparison fix
// -------------------------------------------------------------------------

func TestExecutor_IsNull(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER, name TEXT)")
	exec(t, e, "INSERT INTO t VALUES (1, 'alice')")
	exec(t, e, "INSERT INTO t VALUES (2, NULL)")
	exec(t, e, "INSERT INTO t VALUES (3, 'bob')")

	r := exec(t, e, "SELECT * FROM t WHERE name IS NULL")
	if len(r.Rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(r.Rows))
	}
	if string(r.Rows[0][0]) != "2" {
		t.Errorf("id = %s, want 2", r.Rows[0][0])
	}
}

func TestExecutor_IsNotNull(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER, name TEXT)")
	exec(t, e, "INSERT INTO t VALUES (1, 'alice')")
	exec(t, e, "INSERT INTO t VALUES (2, NULL)")
	exec(t, e, "INSERT INTO t VALUES (3, 'bob')")

	r := exec(t, e, "SELECT * FROM t WHERE name IS NOT NULL")
	if len(r.Rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(r.Rows))
	}
}

func TestExecutor_NullComparison_NotEquals(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER, name TEXT)")
	exec(t, e, "INSERT INTO t VALUES (1, 'alice')")
	exec(t, e, "INSERT INTO t VALUES (2, NULL)")
	exec(t, e, "INSERT INTO t VALUES (3, 'bob')")

	// name != NULL should return 0 rows (not 2) per SQL standard
	r := exec(t, e, "SELECT * FROM t WHERE name != NULL")
	if len(r.Rows) != 0 {
		t.Fatalf("got %d rows, want 0 (NULL comparisons yield NULL)", len(r.Rows))
	}
}

func TestExecutor_NullComparison_Equals(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER, name TEXT)")
	exec(t, e, "INSERT INTO t VALUES (1, 'alice')")
	exec(t, e, "INSERT INTO t VALUES (2, NULL)")

	// name = NULL should return 0 rows per SQL standard
	r := exec(t, e, "SELECT * FROM t WHERE name = NULL")
	if len(r.Rows) != 0 {
		t.Fatalf("got %d rows, want 0 (NULL comparisons yield NULL)", len(r.Rows))
	}
}

// -------------------------------------------------------------------------
// BEGIN / COMMIT / ROLLBACK (no-ops)
// -------------------------------------------------------------------------

func TestExecutor_Begin(t *testing.T) {
	e := setup(t)
	r := exec(t, e, "BEGIN")
	if r.Tag != "BEGIN" {
		t.Errorf("tag = %q, want BEGIN", r.Tag)
	}
}

func TestExecutor_Commit(t *testing.T) {
	e := setup(t)
	r := exec(t, e, "COMMIT")
	if r.Tag != "COMMIT" {
		t.Errorf("tag = %q, want COMMIT", r.Tag)
	}
}

func TestExecutor_Rollback(t *testing.T) {
	e := setup(t)
	r := exec(t, e, "ROLLBACK")
	if r.Tag != "ROLLBACK" {
		t.Errorf("tag = %q, want ROLLBACK", r.Tag)
	}
}

// -------------------------------------------------------------------------
// NOT operator
// -------------------------------------------------------------------------

func TestExecutor_NotBoolean(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER, active BOOLEAN)")
	exec(t, e, "INSERT INTO t VALUES (1, TRUE)")
	exec(t, e, "INSERT INTO t VALUES (2, FALSE)")
	exec(t, e, "INSERT INTO t VALUES (3, TRUE)")

	r := exec(t, e, "SELECT * FROM t WHERE NOT active")
	if len(r.Rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(r.Rows))
	}
	if string(r.Rows[0][0]) != "2" {
		t.Errorf("id = %s, want 2", r.Rows[0][0])
	}
}

func TestExecutor_NotComparison(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER, x INTEGER)")
	exec(t, e, "INSERT INTO t VALUES (1, 3)")
	exec(t, e, "INSERT INTO t VALUES (2, 7)")
	exec(t, e, "INSERT INTO t VALUES (3, 10)")

	r := exec(t, e, "SELECT * FROM t WHERE NOT (x > 5)")
	if len(r.Rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(r.Rows))
	}
	if string(r.Rows[0][0]) != "1" {
		t.Errorf("id = %s, want 1", r.Rows[0][0])
	}
}

func TestExecutor_NotWithAnd(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER, active BOOLEAN)")
	exec(t, e, "INSERT INTO t VALUES (1, TRUE)")
	exec(t, e, "INSERT INTO t VALUES (2, FALSE)")
	exec(t, e, "INSERT INTO t VALUES (3, TRUE)")

	r := exec(t, e, "SELECT * FROM t WHERE NOT active AND id > 1")
	if len(r.Rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(r.Rows))
	}
	if string(r.Rows[0][0]) != "2" {
		t.Errorf("id = %s, want 2", r.Rows[0][0])
	}
}

func TestExecutor_NotNull(t *testing.T) {
	// NOT on NULL should yield NULL (row excluded)
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER, active BOOLEAN)")
	exec(t, e, "INSERT INTO t VALUES (1, TRUE)")
	exec(t, e, "INSERT INTO t VALUES (2, NULL)")
	exec(t, e, "INSERT INTO t VALUES (3, FALSE)")

	r := exec(t, e, "SELECT * FROM t WHERE NOT active")
	if len(r.Rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(r.Rows))
	}
	if string(r.Rows[0][0]) != "3" {
		t.Errorf("id = %s, want 3", r.Rows[0][0])
	}
}

func TestExecutor_NotUpdate(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER, active BOOLEAN)")
	exec(t, e, "INSERT INTO t VALUES (1, TRUE)")
	exec(t, e, "INSERT INTO t VALUES (2, FALSE)")

	r := exec(t, e, "UPDATE t SET active = TRUE WHERE NOT active")
	if r.Tag != "UPDATE 1" {
		t.Errorf("tag = %q, want UPDATE 1", r.Tag)
	}
}

func TestExecutor_NotDelete(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER, active BOOLEAN)")
	exec(t, e, "INSERT INTO t VALUES (1, TRUE)")
	exec(t, e, "INSERT INTO t VALUES (2, FALSE)")
	exec(t, e, "INSERT INTO t VALUES (3, FALSE)")

	r := exec(t, e, "DELETE FROM t WHERE NOT active")
	if r.Tag != "DELETE 2" {
		t.Errorf("tag = %q, want DELETE 2", r.Tag)
	}
}

func TestExecutor_IsNull_UpdateDelete(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER, name TEXT)")
	exec(t, e, "INSERT INTO t VALUES (1, 'alice')")
	exec(t, e, "INSERT INTO t VALUES (2, NULL)")
	exec(t, e, "INSERT INTO t VALUES (3, NULL)")

	// UPDATE with IS NULL
	r := exec(t, e, "UPDATE t SET name = 'unknown' WHERE name IS NULL")
	if r.Tag != "UPDATE 2" {
		t.Errorf("tag = %q, want UPDATE 2", r.Tag)
	}

	// DELETE with IS NOT NULL
	r = exec(t, e, "DELETE FROM t WHERE name IS NOT NULL")
	if r.Tag != "DELETE 3" {
		t.Errorf("tag = %q, want DELETE 3", r.Tag)
	}
}

// -------------------------------------------------------------------------
// Arithmetic expressions
// -------------------------------------------------------------------------

func TestExecutor_ArithmeticStatic(t *testing.T) {
	e := setup(t)

	r := exec(t, e, "SELECT 1 + 2")
	if string(r.Rows[0][0]) != "3" {
		t.Errorf("1+2 = %q, want 3", r.Rows[0][0])
	}

	r = exec(t, e, "SELECT 2 + 3 * 4")
	if string(r.Rows[0][0]) != "14" {
		t.Errorf("2+3*4 = %q, want 14", r.Rows[0][0])
	}

	r = exec(t, e, "SELECT 10 / 3")
	if string(r.Rows[0][0]) != "3" {
		t.Errorf("10/3 = %q, want 3", r.Rows[0][0])
	}

	r = exec(t, e, "SELECT 10 % 3")
	if string(r.Rows[0][0]) != "1" {
		t.Errorf("10%%3 = %q, want 1", r.Rows[0][0])
	}

	r = exec(t, e, "SELECT 5 - 3")
	if string(r.Rows[0][0]) != "2" {
		t.Errorf("5-3 = %q, want 2", r.Rows[0][0])
	}

	r = exec(t, e, "SELECT -42")
	if string(r.Rows[0][0]) != "-42" {
		t.Errorf("-42 = %q, want -42", r.Rows[0][0])
	}
}

func TestExecutor_ArithmeticWithAlias(t *testing.T) {
	e := setup(t)

	r := exec(t, e, "SELECT 1 + 2 AS sum")
	if r.Columns[0].Name != "sum" {
		t.Errorf("column name = %q, want sum", r.Columns[0].Name)
	}
	if string(r.Rows[0][0]) != "3" {
		t.Errorf("1+2 = %q, want 3", r.Rows[0][0])
	}
}

func TestExecutor_ArithmeticFromTable(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (a INTEGER, b INTEGER)")
	exec(t, e, "INSERT INTO t VALUES (10, 3)")

	r := exec(t, e, "SELECT a + b FROM t")
	if string(r.Rows[0][0]) != "13" {
		t.Errorf("a+b = %q, want 13", r.Rows[0][0])
	}

	r = exec(t, e, "SELECT a * b, a / b, a % b FROM t")
	if string(r.Rows[0][0]) != "30" {
		t.Errorf("a*b = %q, want 30", r.Rows[0][0])
	}
	if string(r.Rows[0][1]) != "3" {
		t.Errorf("a/b = %q, want 3", r.Rows[0][1])
	}
	if string(r.Rows[0][2]) != "1" {
		t.Errorf("a%%b = %q, want 1", r.Rows[0][2])
	}

	r = exec(t, e, "SELECT a - b AS diff FROM t")
	if string(r.Rows[0][0]) != "7" {
		t.Errorf("a-b = %q, want 7", r.Rows[0][0])
	}
	if r.Columns[0].Name != "diff" {
		t.Errorf("column name = %q, want diff", r.Columns[0].Name)
	}
}

func TestExecutor_ArithmeticInWhere(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE orders (price INTEGER, qty INTEGER)")
	exec(t, e, "INSERT INTO orders VALUES (10, 5)")
	exec(t, e, "INSERT INTO orders VALUES (20, 3)")
	exec(t, e, "INSERT INTO orders VALUES (5, 8)")

	r := exec(t, e, "SELECT * FROM orders WHERE price * qty > 50")
	if r.Tag != "SELECT 1" {
		t.Errorf("tag = %q, want SELECT 1", r.Tag)
	}
}

func TestExecutor_ArithmeticNullPropagation(t *testing.T) {
	e := setup(t)

	r := exec(t, e, "SELECT NULL + 1")
	if r.Rows[0][0] != nil {
		t.Errorf("NULL + 1 = %q, want NULL", string(r.Rows[0][0]))
	}
}

func TestExecutor_DivisionByZero(t *testing.T) {
	e := setup(t)

	_, err := e.Execute("SELECT 1 / 0")
	if err == nil {
		t.Fatal("expected division by zero error")
	}
	var qe *QueryError
	if !errors.As(err, &qe) || qe.Code != "22012" {
		t.Errorf("error = %v, want QueryError with code 22012", err)
	}
}

func TestExecutor_ModuloByZero(t *testing.T) {
	e := setup(t)

	_, err := e.Execute("SELECT 1 % 0")
	if err == nil {
		t.Fatal("expected division by zero error")
	}
	var qe *QueryError
	if !errors.As(err, &qe) || qe.Code != "22012" {
		t.Errorf("error = %v, want QueryError with code 22012", err)
	}
}

func TestExecutor_UnaryMinusInWhere(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (x INTEGER)")
	exec(t, e, "INSERT INTO t VALUES (5)")
	exec(t, e, "INSERT INTO t VALUES (-3)")

	r := exec(t, e, "SELECT * FROM t WHERE x = -3")
	if r.Tag != "SELECT 1" {
		t.Errorf("tag = %q, want SELECT 1", r.Tag)
	}
	if string(r.Rows[0][0]) != "-3" {
		t.Errorf("x = %q, want -3", r.Rows[0][0])
	}
}

func TestExecutor_InsertWithArithmetic(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (x INTEGER)")
	exec(t, e, "INSERT INTO t VALUES (1 + 2)")

	r := exec(t, e, "SELECT * FROM t")
	if string(r.Rows[0][0]) != "3" {
		t.Errorf("x = %q, want 3", r.Rows[0][0])
	}
}

func TestExecutor_UnaryMinusSelect(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (x INTEGER)")
	exec(t, e, "INSERT INTO t VALUES (42)")

	r := exec(t, e, "SELECT -x FROM t")
	if string(r.Rows[0][0]) != "-42" {
		t.Errorf("-x = %q, want -42", r.Rows[0][0])
	}
}
