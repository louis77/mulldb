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
	if len(r.Rows) != 5 {
		t.Fatalf("rows = %d, want 5", len(r.Rows))
	}

	// Verify all type rows.
	expected := []struct {
		oid     string
		typname string
	}{
		{"16", "bool"},
		{"20", "int8"},
		{"25", "text"},
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
	if len(r.Rows) != 5 {
		t.Fatalf("rows = %d, want 5", len(r.Rows))
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
	if string(r.Rows[0][0]) != "5" {
		t.Errorf("count = %q, want 5", r.Rows[0][0])
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
	if len(r.Rows) != 5 {
		t.Fatalf("rows = %d, want 5", len(r.Rows))
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
	// Catalog tables: information_schema.tables, pg_catalog.pg_database, pg_catalog.pg_type (sorted).
	if len(r.Rows) < 5 {
		t.Fatalf("rows = %d, want at least 5", len(r.Rows))
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
