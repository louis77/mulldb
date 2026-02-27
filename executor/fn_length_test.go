package executor

import (
	"errors"
	"testing"
)

func TestLength_Static(t *testing.T) {
	e := setup(t)

	r := exec(t, e, "SELECT LENGTH('hello')")
	if len(r.Rows) != 1 || string(r.Rows[0][0]) != "5" {
		t.Fatalf("LENGTH('hello') = %q, want 5", r.Rows[0][0])
	}
}

func TestLength_UTF8(t *testing.T) {
	e := setup(t)

	r := exec(t, e, "SELECT LENGTH('héllo')")
	if len(r.Rows) != 1 || string(r.Rows[0][0]) != "5" {
		t.Fatalf("LENGTH('héllo') = %q, want 5", r.Rows[0][0])
	}
}

func TestLength_Empty(t *testing.T) {
	e := setup(t)

	r := exec(t, e, "SELECT LENGTH('')")
	if len(r.Rows) != 1 || string(r.Rows[0][0]) != "0" {
		t.Fatalf("LENGTH('') = %q, want 0", r.Rows[0][0])
	}
}

func TestLength_Null(t *testing.T) {
	e := setup(t)

	r := exec(t, e, "SELECT LENGTH(NULL)")
	if len(r.Rows) != 1 || r.Rows[0][0] != nil {
		t.Fatalf("LENGTH(NULL) = %v, want nil", r.Rows[0][0])
	}
}

func TestLength_FromTable(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (name TEXT)")
	exec(t, e, "INSERT INTO t (name) VALUES ('hi'), ('hello')")

	r := exec(t, e, "SELECT LENGTH(name) FROM t")
	if len(r.Rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(r.Rows))
	}
	if string(r.Rows[0][0]) != "2" {
		t.Errorf("LENGTH('hi') = %q, want 2", r.Rows[0][0])
	}
	if string(r.Rows[1][0]) != "5" {
		t.Errorf("LENGTH('hello') = %q, want 5", r.Rows[1][0])
	}
}

func TestLength_InWhere(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (name TEXT)")
	exec(t, e, "INSERT INTO t (name) VALUES ('hi'), ('hello'), ('hey')")

	r := exec(t, e, "SELECT * FROM t WHERE LENGTH(name) > 3")
	if len(r.Rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(r.Rows))
	}
	if string(r.Rows[0][0]) != "hello" {
		t.Errorf("got %q, want hello", r.Rows[0][0])
	}
}

func TestLength_WrongType(t *testing.T) {
	e := setup(t)

	_, err := e.Execute("SELECT LENGTH(123)")
	if err == nil {
		t.Fatal("expected error for LENGTH(123)")
	}
	var qe *QueryError
	if !errors.As(err, &qe) || qe.Code != "42883" {
		t.Errorf("got error %v, want QueryError with code 42883", err)
	}
}

func TestLength_WrongArity(t *testing.T) {
	e := setup(t)

	// Parser only supports 0-1 args for function calls, so multi-arg is a parse error.
	_, err := e.Execute("SELECT LENGTH('a', 'b')")
	if err == nil {
		t.Fatal("expected error for LENGTH('a', 'b')")
	}

	// Zero-arg call should return a runtime arity error.
	_, err = e.Execute("SELECT LENGTH()")
	if err == nil {
		t.Fatal("expected error for LENGTH()")
	}
	var qe *QueryError
	if !errors.As(err, &qe) || qe.Code != "42883" {
		t.Errorf("got error %v, want QueryError with code 42883", err)
	}
}

func TestLength_ColumnMetadata(t *testing.T) {
	e := setup(t)

	r := exec(t, e, "SELECT LENGTH('test')")
	if r.Columns[0].Name != "length" {
		t.Errorf("column name = %q, want length", r.Columns[0].Name)
	}
	if r.Columns[0].TypeOID != OIDInt8 {
		t.Errorf("TypeOID = %d, want %d", r.Columns[0].TypeOID, OIDInt8)
	}
}

func TestLength_WithAlias(t *testing.T) {
	e := setup(t)

	r := exec(t, e, "SELECT LENGTH('test') AS len")
	if r.Columns[0].Name != "len" {
		t.Errorf("column name = %q, want len", r.Columns[0].Name)
	}
}

func TestLength_CharacterLength(t *testing.T) {
	e := setup(t)

	r := exec(t, e, "SELECT CHARACTER_LENGTH('hello')")
	if len(r.Rows) != 1 || string(r.Rows[0][0]) != "5" {
		t.Fatalf("CHARACTER_LENGTH('hello') = %q, want 5", r.Rows[0][0])
	}

	r = exec(t, e, "SELECT CHAR_LENGTH('hello')")
	if len(r.Rows) != 1 || string(r.Rows[0][0]) != "5" {
		t.Fatalf("CHAR_LENGTH('hello') = %q, want 5", r.Rows[0][0])
	}
}

func TestLength_FromTableColumnMetadata(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (name TEXT)")
	exec(t, e, "INSERT INTO t (name) VALUES ('hello')")

	r := exec(t, e, "SELECT LENGTH(name) FROM t")
	if r.Columns[0].Name != "length" {
		t.Errorf("column name = %q, want length", r.Columns[0].Name)
	}
	if r.Columns[0].TypeOID != OIDInt8 {
		t.Errorf("TypeOID = %d, want %d", r.Columns[0].TypeOID, OIDInt8)
	}
}
