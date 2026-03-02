package executor

import (
	"errors"
	"testing"
)

func TestCoalesce_Basic(t *testing.T) {
	e := setup(t)

	r := exec(t, e, "SELECT COALESCE(NULL, 'a', 'b')")
	if len(r.Rows) != 1 || string(r.Rows[0][0]) != "a" {
		t.Fatalf("COALESCE(NULL, 'a', 'b') = %q, want 'a'", r.Rows[0][0])
	}
}

func TestCoalesce_FirstNonNull(t *testing.T) {
	e := setup(t)

	r := exec(t, e, "SELECT COALESCE(1, 2, 3)")
	if len(r.Rows) != 1 || string(r.Rows[0][0]) != "1" {
		t.Fatalf("COALESCE(1, 2, 3) = %q, want 1", r.Rows[0][0])
	}
}

func TestCoalesce_SkipsNulls(t *testing.T) {
	e := setup(t)

	r := exec(t, e, "SELECT COALESCE(NULL, NULL, 'last')")
	if len(r.Rows) != 1 || string(r.Rows[0][0]) != "last" {
		t.Fatalf("COALESCE(NULL, NULL, 'last') = %q, want 'last'", r.Rows[0][0])
	}
}

func TestCoalesce_AllNull(t *testing.T) {
	e := setup(t)

	r := exec(t, e, "SELECT COALESCE(NULL, NULL)")
	if len(r.Rows) != 1 || r.Rows[0][0] != nil {
		t.Fatalf("COALESCE(NULL, NULL) = %v, want nil", r.Rows[0][0])
	}
}

func TestCoalesce_SingleArg(t *testing.T) {
	e := setup(t)

	r := exec(t, e, "SELECT COALESCE('single')")
	if len(r.Rows) != 1 || string(r.Rows[0][0]) != "single" {
		t.Fatalf("COALESCE('single') = %q, want 'single'", r.Rows[0][0])
	}
}

func TestCoalesce_SingleNull(t *testing.T) {
	e := setup(t)

	r := exec(t, e, "SELECT COALESCE(NULL)")
	if len(r.Rows) != 1 || r.Rows[0][0] != nil {
		t.Fatalf("COALESCE(NULL) = %v, want nil", r.Rows[0][0])
	}
}

func TestCoalesce_Integer(t *testing.T) {
	e := setup(t)

	r := exec(t, e, "SELECT COALESCE(NULL, 42)")
	if len(r.Rows) != 1 || string(r.Rows[0][0]) != "42" {
		t.Fatalf("COALESCE(NULL, 42) = %q, want 42", r.Rows[0][0])
	}
}

func TestCoalesce_Float(t *testing.T) {
	e := setup(t)

	r := exec(t, e, "SELECT COALESCE(NULL, 3.14)")
	if len(r.Rows) != 1 || string(r.Rows[0][0]) != "3.14" {
		t.Fatalf("COALESCE(NULL, 3.14) = %q, want 3.14", r.Rows[0][0])
	}
}

func TestCoalesce_Boolean(t *testing.T) {
	e := setup(t)

	r := exec(t, e, "SELECT COALESCE(NULL, TRUE)")
	if len(r.Rows) != 1 || string(r.Rows[0][0]) != "t" {
		t.Fatalf("COALESCE(NULL, TRUE) = %q, want t", r.Rows[0][0])
	}
}

func TestCoalesce_NoArgs(t *testing.T) {
	e := setup(t)

	_, err := e.Execute("SELECT COALESCE()")
	if err == nil {
		t.Fatal("expected error for COALESCE()")
	}
	var qe *QueryError
	if !errors.As(err, &qe) || qe.Code != "42883" {
		t.Errorf("got error %v, want QueryError with code 42883", err)
	}
}

func TestCoalesce_FromTable(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (a TEXT, b TEXT)")
	exec(t, e, "INSERT INTO t (a, b) VALUES ('first', 'second'), (NULL, 'fallback'), (NULL, NULL)")

	r := exec(t, e, "SELECT COALESCE(a, b) FROM t")
	if len(r.Rows) != 3 {
		t.Fatalf("got %d rows, want 3", len(r.Rows))
	}
	if string(r.Rows[0][0]) != "first" {
		t.Errorf("row 0: got %q, want 'first'", r.Rows[0][0])
	}
	if string(r.Rows[1][0]) != "fallback" {
		t.Errorf("row 1: got %q, want 'fallback'", r.Rows[1][0])
	}
	if r.Rows[2][0] != nil {
		t.Errorf("row 2: got %v, want nil", r.Rows[2][0])
	}
}

func TestCoalesce_InWhere(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (a TEXT, b TEXT)")
	exec(t, e, "INSERT INTO t (a, b) VALUES ('match', NULL), (NULL, 'match'), ('nomatch', 'nomatch')")

	r := exec(t, e, "SELECT * FROM t WHERE COALESCE(a, b) = 'match'")
	if len(r.Rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(r.Rows))
	}
}

func TestCoalesce_ColumnMetadata(t *testing.T) {
	e := setup(t)

	r := exec(t, e, "SELECT COALESCE('test')")
	if r.Columns[0].Name != "coalesce" {
		t.Errorf("column name = %q, want coalesce", r.Columns[0].Name)
	}
	if r.Columns[0].TypeOID != OIDText {
		t.Errorf("TypeOID = %d, want %d", r.Columns[0].TypeOID, OIDText)
	}
}

func TestCoalesce_ColumnMetadataInteger(t *testing.T) {
	e := setup(t)

	r := exec(t, e, "SELECT COALESCE(NULL, 42)")
	if r.Columns[0].Name != "coalesce" {
		t.Errorf("column name = %q, want coalesce", r.Columns[0].Name)
	}
	if r.Columns[0].TypeOID != OIDInt8 {
		t.Errorf("TypeOID = %d, want %d", r.Columns[0].TypeOID, OIDInt8)
	}
}

func TestCoalesce_ColumnMetadataFloat(t *testing.T) {
	e := setup(t)

	r := exec(t, e, "SELECT COALESCE(NULL, 3.14)")
	if r.Columns[0].Name != "coalesce" {
		t.Errorf("column name = %q, want coalesce", r.Columns[0].Name)
	}
	if r.Columns[0].TypeOID != OIDFloat8 {
		t.Errorf("TypeOID = %d, want %d", r.Columns[0].TypeOID, OIDFloat8)
	}
}

func TestCoalesce_ColumnMetadataBoolean(t *testing.T) {
	e := setup(t)

	r := exec(t, e, "SELECT COALESCE(NULL, TRUE)")
	if r.Columns[0].Name != "coalesce" {
		t.Errorf("column name = %q, want coalesce", r.Columns[0].Name)
	}
	if r.Columns[0].TypeOID != OIDBool {
		t.Errorf("TypeOID = %d, want %d", r.Columns[0].TypeOID, OIDBool)
	}
}

func TestCoalesce_ColumnMetadataNull(t *testing.T) {
	e := setup(t)

	r := exec(t, e, "SELECT COALESCE(NULL, NULL)")
	if r.Columns[0].Name != "coalesce" {
		t.Errorf("column name = %q, want coalesce", r.Columns[0].Name)
	}
	if r.Columns[0].TypeOID != OIDUnknown {
		t.Errorf("TypeOID = %d, want %d", r.Columns[0].TypeOID, OIDUnknown)
	}
}

func TestCoalesce_WithAlias(t *testing.T) {
	e := setup(t)

	r := exec(t, e, "SELECT COALESCE('test') AS val")
	if r.Columns[0].Name != "val" {
		t.Errorf("column name = %q, want val", r.Columns[0].Name)
	}
}

func TestCoalesce_MixedTypes(t *testing.T) {
	e := setup(t)

	// First non-NULL determines the type
	r := exec(t, e, "SELECT COALESCE(NULL, NULL, 'text', 42)")
	if len(r.Rows) != 1 || string(r.Rows[0][0]) != "text" {
		t.Fatalf("COALESCE(NULL, NULL, 'text', 42) = %q, want 'text'", r.Rows[0][0])
	}
	if r.Columns[0].TypeOID != OIDText {
		t.Errorf("TypeOID = %d, want %d (TEXT)", r.Columns[0].TypeOID, OIDText)
	}
}

func TestCoalesce_CaseInsensitive(t *testing.T) {
	e := setup(t)

	r := exec(t, e, "SELECT coalesce(NULL, 'test')")
	if len(r.Rows) != 1 || string(r.Rows[0][0]) != "test" {
		t.Fatalf("coalesce(NULL, 'test') = %q, want 'test'", r.Rows[0][0])
	}

	r = exec(t, e, "SELECT Coalesce(NULL, 'test')")
	if len(r.Rows) != 1 || string(r.Rows[0][0]) != "test" {
		t.Fatalf("Coalesce(NULL, 'test') = %q, want 'test'", r.Rows[0][0])
	}
}
