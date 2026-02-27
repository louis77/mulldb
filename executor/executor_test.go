package executor

import (
	"errors"
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
