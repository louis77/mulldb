package executor

import (
	"errors"
	"testing"
)

func TestCatalog_SelectStar(t *testing.T) {
	e := setup(t)
	r := exec(t, e, "SELECT * FROM pg_type")

	if len(r.Columns) != 2 {
		t.Fatalf("columns = %d, want 2", len(r.Columns))
	}
	if r.Columns[0].Name != "oid" || r.Columns[1].Name != "typname" {
		t.Errorf("columns = [%s, %s], want [oid, typname]", r.Columns[0].Name, r.Columns[1].Name)
	}
	if len(r.Rows) != 6 {
		t.Fatalf("rows = %d, want 6", len(r.Rows))
	}

	// Verify all type rows.
	expected := []struct {
		oid     string
		typname string
	}{
		{"16", "bool"},
		{"20", "int8"},
		{"25", "text"},
		{"1184", "timestamptz"},
		{"9900", "geometry"},
		{"9901", "geography"},
	}
	for i, exp := range expected {
		if string(r.Rows[i][0]) != exp.oid {
			t.Errorf("row %d oid = %q, want %q", i, r.Rows[i][0], exp.oid)
		}
		if string(r.Rows[i][1]) != exp.typname {
			t.Errorf("row %d typname = %q, want %q", i, r.Rows[i][1], exp.typname)
		}
	}
}

func TestCatalog_SelectSpecificColumns(t *testing.T) {
	e := setup(t)
	r := exec(t, e, "SELECT oid, typname FROM pg_type")

	if len(r.Columns) != 2 {
		t.Fatalf("columns = %d, want 2", len(r.Columns))
	}
	if r.Columns[0].Name != "oid" {
		t.Errorf("col[0] = %q, want oid", r.Columns[0].Name)
	}
	if r.Columns[1].Name != "typname" {
		t.Errorf("col[1] = %q, want typname", r.Columns[1].Name)
	}
	if len(r.Rows) != 6 {
		t.Fatalf("rows = %d, want 6", len(r.Rows))
	}
}

func TestCatalog_SelectWhereFilter(t *testing.T) {
	e := setup(t)
	r := exec(t, e, "SELECT oid, typname FROM pg_type WHERE typname = 'text'")

	if len(r.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(r.Rows))
	}
	if string(r.Rows[0][0]) != "25" {
		t.Errorf("oid = %q, want 25", r.Rows[0][0])
	}
	if string(r.Rows[0][1]) != "text" {
		t.Errorf("typname = %q, want text", r.Rows[0][1])
	}
}

func TestCatalog_SelectWhereNoMatch(t *testing.T) {
	e := setup(t)
	r := exec(t, e, "SELECT oid, typname FROM pg_type WHERE typname = 'jsonb' OR typname = 'xml'")

	if len(r.Rows) != 0 {
		t.Fatalf("rows = %d, want 0", len(r.Rows))
	}
}

func TestCatalog_CountStar(t *testing.T) {
	e := setup(t)
	r := exec(t, e, "SELECT COUNT(*) FROM pg_type")

	if len(r.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(r.Rows))
	}
	if string(r.Rows[0][0]) != "6" {
		t.Errorf("count = %q, want 6", r.Rows[0][0])
	}
}

func TestCatalog_InsertReadOnly(t *testing.T) {
	e := setup(t)
	_, err := e.Execute("INSERT INTO pg_type (oid, typname) VALUES (100, 'custom')")
	if err == nil {
		t.Fatal("expected error inserting into catalog table")
	}
	var qe *QueryError
	if !errors.As(err, &qe) {
		t.Fatalf("expected QueryError, got %T", err)
	}
	if qe.Code != "42809" {
		t.Errorf("SQLSTATE = %q, want 42809", qe.Code)
	}
}

func TestCatalog_UpdateReadOnly(t *testing.T) {
	e := setup(t)
	_, err := e.Execute("UPDATE pg_type SET typname = 'custom' WHERE oid = 16")
	if err == nil {
		t.Fatal("expected error updating catalog table")
	}
	var qe *QueryError
	if !errors.As(err, &qe) {
		t.Fatalf("expected QueryError, got %T", err)
	}
	if qe.Code != "42809" {
		t.Errorf("SQLSTATE = %q, want 42809", qe.Code)
	}
}

func TestCatalog_DeleteReadOnly(t *testing.T) {
	e := setup(t)
	_, err := e.Execute("DELETE FROM pg_type WHERE oid = 16")
	if err == nil {
		t.Fatal("expected error deleting from catalog table")
	}
	var qe *QueryError
	if !errors.As(err, &qe) {
		t.Fatalf("expected QueryError, got %T", err)
	}
	if qe.Code != "42809" {
		t.Errorf("SQLSTATE = %q, want 42809", qe.Code)
	}
}

func TestCatalog_PGDatabase(t *testing.T) {
	e := setup(t)
	r := exec(t, e, "SELECT datname FROM pg_database")

	if len(r.Columns) != 1 {
		t.Fatalf("columns = %d, want 1", len(r.Columns))
	}
	if r.Columns[0].Name != "datname" {
		t.Errorf("col name = %q, want datname", r.Columns[0].Name)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(r.Rows))
	}
	if string(r.Rows[0][0]) != "mulldb" {
		t.Errorf("datname = %q, want mulldb", r.Rows[0][0])
	}
}

// ---------------------------------------------------------------------------
// Schema-qualified catalog access
// ---------------------------------------------------------------------------

func TestCatalog_QualifiedPGType(t *testing.T) {
	e := setup(t)
	r := exec(t, e, "SELECT * FROM pg_catalog.pg_type")

	if len(r.Columns) != 2 {
		t.Fatalf("columns = %d, want 2", len(r.Columns))
	}
	if len(r.Rows) != 6 {
		t.Fatalf("rows = %d, want 6", len(r.Rows))
	}
}

func TestCatalog_InformationSchemaTables(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE users (id INTEGER, name TEXT)")
	exec(t, e, "CREATE TABLE orders (id INTEGER)")

	r := exec(t, e, "SELECT table_schema, table_name, table_type FROM information_schema.tables")

	if len(r.Columns) != 3 {
		t.Fatalf("columns = %d, want 3", len(r.Columns))
	}

	// Should contain user tables + catalog tables.
	// User tables: orders, users (sorted).
	// Catalog tables: information_schema.columns, information_schema.tables,
	//   pg_catalog.pg_database, pg_catalog.pg_type (sorted).
	if len(r.Rows) < 6 {
		t.Fatalf("rows = %d, want at least 6", len(r.Rows))
	}

	// First two rows should be user tables (sorted alphabetically).
	if string(r.Rows[0][0]) != "public" || string(r.Rows[0][1]) != "orders" || string(r.Rows[0][2]) != "BASE TABLE" {
		t.Errorf("row 0 = [%s, %s, %s], want [public, orders, BASE TABLE]",
			r.Rows[0][0], r.Rows[0][1], r.Rows[0][2])
	}
	if string(r.Rows[1][0]) != "public" || string(r.Rows[1][1]) != "users" || string(r.Rows[1][2]) != "BASE TABLE" {
		t.Errorf("row 1 = [%s, %s, %s], want [public, users, BASE TABLE]",
			r.Rows[1][0], r.Rows[1][1], r.Rows[1][2])
	}
}

func TestCatalog_InformationSchemaTablesWherePublic(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t1 (id INTEGER)")

	r := exec(t, e, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
	if len(r.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(r.Rows))
	}
	if string(r.Rows[0][0]) != "t1" {
		t.Errorf("table_name = %q, want t1", r.Rows[0][0])
	}
}

// ---------------------------------------------------------------------------
// information_schema.columns
// ---------------------------------------------------------------------------

func TestCatalog_InformationSchemaColumns(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, active BOOLEAN)")

	r := exec(t, e, "SELECT column_name, ordinal_position, data_type, is_nullable FROM information_schema.columns WHERE table_name = 'users'")

	if len(r.Rows) != 3 {
		t.Fatalf("rows = %d, want 3", len(r.Rows))
	}

	expected := []struct {
		name     string
		ordinal  string
		dataType string
		nullable string
	}{
		{"id", "1", "integer", "NO"},
		{"name", "2", "text", "YES"},
		{"active", "3", "boolean", "YES"},
	}
	for i, exp := range expected {
		if string(r.Rows[i][0]) != exp.name {
			t.Errorf("row %d column_name = %q, want %q", i, r.Rows[i][0], exp.name)
		}
		if string(r.Rows[i][1]) != exp.ordinal {
			t.Errorf("row %d ordinal_position = %q, want %q", i, r.Rows[i][1], exp.ordinal)
		}
		if string(r.Rows[i][2]) != exp.dataType {
			t.Errorf("row %d data_type = %q, want %q", i, r.Rows[i][2], exp.dataType)
		}
		if string(r.Rows[i][3]) != exp.nullable {
			t.Errorf("row %d is_nullable = %q, want %q", i, r.Rows[i][3], exp.nullable)
		}
	}
}

func TestCatalog_InformationSchemaColumnsFilter(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t1 (a INTEGER, b TEXT)")
	exec(t, e, "CREATE TABLE t2 (x INTEGER)")

	r := exec(t, e, "SELECT table_name, column_name FROM information_schema.columns WHERE table_name = 't1'")

	if len(r.Rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(r.Rows))
	}
	if string(r.Rows[0][1]) != "a" {
		t.Errorf("row 0 column_name = %q, want a", r.Rows[0][1])
	}
	if string(r.Rows[1][1]) != "b" {
		t.Errorf("row 1 column_name = %q, want b", r.Rows[1][1])
	}
}

func TestCatalog_InformationSchemaColumnsInsertReadOnly(t *testing.T) {
	e := setup(t)
	_, err := e.Execute("INSERT INTO information_schema.columns (table_schema, table_name, column_name) VALUES ('public', 'fake', 'col')")
	if err == nil {
		t.Fatal("expected error inserting into information_schema.columns")
	}
	var qe *QueryError
	if !errors.As(err, &qe) {
		t.Fatalf("expected QueryError, got %T", err)
	}
	if qe.Code != "42809" {
		t.Errorf("SQLSTATE = %q, want 42809", qe.Code)
	}
}

// ---------------------------------------------------------------------------
// information_schema.table_constraints
// ---------------------------------------------------------------------------

func TestCatalog_TableConstraintsPrimaryKey(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")

	r := exec(t, e, "SELECT constraint_name, table_name, constraint_type FROM information_schema.table_constraints WHERE table_name = 'users'")

	if len(r.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(r.Rows))
	}
	if string(r.Rows[0][0]) != "users_pkey" {
		t.Errorf("constraint_name = %q, want users_pkey", r.Rows[0][0])
	}
	if string(r.Rows[0][1]) != "users" {
		t.Errorf("table_name = %q, want users", r.Rows[0][1])
	}
	if string(r.Rows[0][2]) != "PRIMARY KEY" {
		t.Errorf("constraint_type = %q, want PRIMARY KEY", r.Rows[0][2])
	}
}

func TestCatalog_TableConstraintsUnique(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)")
	exec(t, e, "CREATE UNIQUE INDEX users_email_idx ON users (email)")

	r := exec(t, e, "SELECT constraint_name, constraint_type FROM information_schema.table_constraints WHERE table_name = 'users' ORDER BY constraint_name")

	if len(r.Rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(r.Rows))
	}
	// Sorted: users_email_idx < users_pkey
	if string(r.Rows[0][0]) != "users_email_idx" || string(r.Rows[0][1]) != "UNIQUE" {
		t.Errorf("row 0 = [%s, %s], want [users_email_idx, UNIQUE]", r.Rows[0][0], r.Rows[0][1])
	}
	if string(r.Rows[1][0]) != "users_pkey" || string(r.Rows[1][1]) != "PRIMARY KEY" {
		t.Errorf("row 1 = [%s, %s], want [users_pkey, PRIMARY KEY]", r.Rows[1][0], r.Rows[1][1])
	}
}

func TestCatalog_TableConstraintsNoPK(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE logs (msg TEXT)")

	r := exec(t, e, "SELECT constraint_name FROM information_schema.table_constraints WHERE table_name = 'logs'")

	if len(r.Rows) != 0 {
		t.Fatalf("rows = %d, want 0", len(r.Rows))
	}
}

// ---------------------------------------------------------------------------
// information_schema.key_column_usage
// ---------------------------------------------------------------------------

func TestCatalog_KeyColumnUsagePrimaryKey(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")

	r := exec(t, e, "SELECT constraint_name, column_name, ordinal_position FROM information_schema.key_column_usage WHERE table_name = 'users'")

	if len(r.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(r.Rows))
	}
	if string(r.Rows[0][0]) != "users_pkey" {
		t.Errorf("constraint_name = %q, want users_pkey", r.Rows[0][0])
	}
	if string(r.Rows[0][1]) != "id" {
		t.Errorf("column_name = %q, want id", r.Rows[0][1])
	}
	if string(r.Rows[0][2]) != "1" {
		t.Errorf("ordinal_position = %q, want 1", r.Rows[0][2])
	}
}

func TestCatalog_KeyColumnUsageUnique(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)")
	exec(t, e, "CREATE UNIQUE INDEX users_email_idx ON users (email)")

	r := exec(t, e, "SELECT constraint_name, column_name FROM information_schema.key_column_usage WHERE table_name = 'users' ORDER BY constraint_name")

	if len(r.Rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(r.Rows))
	}
	if string(r.Rows[0][0]) != "users_email_idx" || string(r.Rows[0][1]) != "email" {
		t.Errorf("row 0 = [%s, %s], want [users_email_idx, email]", r.Rows[0][0], r.Rows[0][1])
	}
	if string(r.Rows[1][0]) != "users_pkey" || string(r.Rows[1][1]) != "id" {
		t.Errorf("row 1 = [%s, %s], want [users_pkey, id]", r.Rows[1][0], r.Rows[1][1])
	}
}

// ---------------------------------------------------------------------------
// Implicit cross-join and catalog tables in JOINs
// ---------------------------------------------------------------------------

func TestCatalog_ImplicitCrossJoin(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, a TEXT)")
	exec(t, e, "CREATE TABLE t2 (id INTEGER PRIMARY KEY, b TEXT)")
	exec(t, e, "INSERT INTO t1 (id, a) VALUES (1, 'x')")
	exec(t, e, "INSERT INTO t2 (id, b) VALUES (1, 'y')")

	r := exec(t, e, "SELECT p.a, q.b FROM t1 p, t2 q WHERE p.id = q.id")

	if len(r.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(r.Rows))
	}
	if string(r.Rows[0][0]) != "x" || string(r.Rows[0][1]) != "y" {
		t.Errorf("row = [%s, %s], want [x, y]", r.Rows[0][0], r.Rows[0][1])
	}
}

func TestCatalog_TablePlusConstraintQuery(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE addresses (id INTEGER PRIMARY KEY, street TEXT, city TEXT)")

	r := exec(t, e, `SELECT tc.constraint_name AS constraint_name, kc.column_name AS column_name
		FROM information_schema.table_constraints tc, information_schema.key_column_usage kc
		WHERE tc.constraint_type = 'PRIMARY KEY'
		AND kc.table_name = tc.table_name
		AND kc.table_schema = tc.table_schema
		AND kc.constraint_name = tc.constraint_name
		AND tc.table_schema = 'public'
		AND tc.table_name = 'addresses'
		ORDER BY kc.ordinal_position`)

	if len(r.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(r.Rows))
	}
	if string(r.Rows[0][0]) != "addresses_pkey" {
		t.Errorf("constraint_name = %q, want addresses_pkey", r.Rows[0][0])
	}
	if string(r.Rows[0][1]) != "id" {
		t.Errorf("column_name = %q, want id", r.Rows[0][1])
	}
}

// ---------------------------------------------------------------------------
// pg_class
// ---------------------------------------------------------------------------

func TestCatalog_PGClassUserTables(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE names (id INTEGER PRIMARY KEY, name TEXT)")
	exec(t, e, "INSERT INTO names (id, name) VALUES (1, 'Alice')")
	exec(t, e, "INSERT INTO names (id, name) VALUES (2, 'Bob')")

	r := exec(t, e, "SELECT relname, relkind, reltuples FROM pg_class WHERE relname = 'names'")

	if len(r.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(r.Rows))
	}
	if string(r.Rows[0][0]) != "names" {
		t.Errorf("relname = %q, want names", r.Rows[0][0])
	}
	if string(r.Rows[0][1]) != "r" {
		t.Errorf("relkind = %q, want r", r.Rows[0][1])
	}
	if string(r.Rows[0][2]) != "2" {
		t.Errorf("reltuples = %q, want 2", r.Rows[0][2])
	}
}

func TestCatalog_PGClassJoinNamespace(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE names (id INTEGER PRIMARY KEY, name TEXT)")
	exec(t, e, "INSERT INTO names (id, name) VALUES (1, 'Alice')")
	exec(t, e, "INSERT INTO names (id, name) VALUES (2, 'Bob')")
	exec(t, e, "INSERT INTO names (id, name) VALUES (3, 'Charlie')")

	r := exec(t, e, `SELECT reltuples::int8 AS count
		FROM pg_class c
		JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		WHERE nspname = 'public' AND relname = 'names'`)

	if len(r.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(r.Rows))
	}
	if string(r.Rows[0][0]) != "3" {
		t.Errorf("count = %q, want 3", r.Rows[0][0])
	}
	if r.Columns[0].Name != "count" {
		t.Errorf("column name = %q, want count", r.Columns[0].Name)
	}
}

func TestCatalog_PGClassCatalogTables(t *testing.T) {
	e := setup(t)

	r := exec(t, e, "SELECT relname, relkind FROM pg_class WHERE relkind = 'v' ORDER BY relname")

	if len(r.Rows) == 0 {
		t.Fatal("expected catalog tables in pg_class")
	}
	// All should have relkind = 'v'.
	for i, row := range r.Rows {
		if string(row[1]) != "v" {
			t.Errorf("row %d relkind = %q, want v", i, row[1])
		}
	}
}

func TestCatalog_InformationSchemaInsertReadOnly(t *testing.T) {
	e := setup(t)
	_, err := e.Execute("INSERT INTO information_schema.tables (table_schema, table_name, table_type) VALUES ('public', 'fake', 'BASE TABLE')")
	if err == nil {
		t.Fatal("expected error inserting into information_schema.tables")
	}
	var qe *QueryError
	if !errors.As(err, &qe) {
		t.Fatalf("expected QueryError, got %T", err)
	}
	if qe.Code != "42809" {
		t.Errorf("SQLSTATE = %q, want 42809", qe.Code)
	}
}
