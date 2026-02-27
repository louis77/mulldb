package executor

import (
	"errors"
	"testing"
)

func TestOctetLength_Static(t *testing.T) {
	e := setup(t)

	r := exec(t, e, "SELECT OCTET_LENGTH('hello')")
	if len(r.Rows) != 1 || string(r.Rows[0][0]) != "5" {
		t.Fatalf("OCTET_LENGTH('hello') = %q, want 5", r.Rows[0][0])
	}
}

func TestOctetLength_UTF8(t *testing.T) {
	e := setup(t)

	r := exec(t, e, "SELECT OCTET_LENGTH('héllo')")
	if len(r.Rows) != 1 || string(r.Rows[0][0]) != "6" {
		t.Fatalf("OCTET_LENGTH('héllo') = %q, want 6", r.Rows[0][0])
	}
}

func TestOctetLength_Empty(t *testing.T) {
	e := setup(t)

	r := exec(t, e, "SELECT OCTET_LENGTH('')")
	if len(r.Rows) != 1 || string(r.Rows[0][0]) != "0" {
		t.Fatalf("OCTET_LENGTH('') = %q, want 0", r.Rows[0][0])
	}
}

func TestOctetLength_Null(t *testing.T) {
	e := setup(t)

	r := exec(t, e, "SELECT OCTET_LENGTH(NULL)")
	if len(r.Rows) != 1 || r.Rows[0][0] != nil {
		t.Fatalf("OCTET_LENGTH(NULL) = %v, want nil", r.Rows[0][0])
	}
}

func TestOctetLength_FromTable(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (name TEXT)")
	exec(t, e, "INSERT INTO t (name) VALUES ('hi'), ('hello')")

	r := exec(t, e, "SELECT OCTET_LENGTH(name) FROM t")
	if len(r.Rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(r.Rows))
	}
	if string(r.Rows[0][0]) != "2" {
		t.Errorf("OCTET_LENGTH('hi') = %q, want 2", r.Rows[0][0])
	}
	if string(r.Rows[1][0]) != "5" {
		t.Errorf("OCTET_LENGTH('hello') = %q, want 5", r.Rows[1][0])
	}
}

func TestOctetLength_InWhere(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (name TEXT)")
	exec(t, e, "INSERT INTO t (name) VALUES ('hi'), ('hello'), ('hey')")

	r := exec(t, e, "SELECT * FROM t WHERE OCTET_LENGTH(name) > 3")
	if len(r.Rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(r.Rows))
	}
	if string(r.Rows[0][0]) != "hello" {
		t.Errorf("got %q, want hello", r.Rows[0][0])
	}
}

func TestOctetLength_WrongType(t *testing.T) {
	e := setup(t)

	_, err := e.Execute("SELECT OCTET_LENGTH(123)")
	if err == nil {
		t.Fatal("expected error for OCTET_LENGTH(123)")
	}
	var qe *QueryError
	if !errors.As(err, &qe) || qe.Code != "42883" {
		t.Errorf("got error %v, want QueryError with code 42883", err)
	}
}

func TestOctetLength_ColumnMetadata(t *testing.T) {
	e := setup(t)

	r := exec(t, e, "SELECT OCTET_LENGTH('test')")
	if r.Columns[0].Name != "octet_length" {
		t.Errorf("column name = %q, want octet_length", r.Columns[0].Name)
	}
	if r.Columns[0].TypeOID != OIDInt8 {
		t.Errorf("TypeOID = %d, want %d", r.Columns[0].TypeOID, OIDInt8)
	}
}
