package executor

import (
	"testing"
)

func TestConcat_Static(t *testing.T) {
	e := setup(t)
	r := exec(t, e, "SELECT CONCAT('hello', ' ', 'world')")
	got := string(r.Rows[0][0])
	if got != "hello world" {
		t.Errorf("got %q, want %q", got, "hello world")
	}
}

func TestConcat_NullSkipped(t *testing.T) {
	e := setup(t)
	r := exec(t, e, "SELECT CONCAT('a', NULL, 'b')")
	got := string(r.Rows[0][0])
	if got != "ab" {
		t.Errorf("got %q, want %q", got, "ab")
	}
}

func TestConcat_AllNull(t *testing.T) {
	e := setup(t)
	r := exec(t, e, "SELECT CONCAT(NULL, NULL)")
	if r.Rows[0][0] == nil {
		t.Fatal("expected empty string, got NULL")
	}
	got := string(r.Rows[0][0])
	if got != "" {
		t.Errorf("got %q, want empty string", got)
	}
}

func TestConcat_SingleArg(t *testing.T) {
	e := setup(t)
	r := exec(t, e, "SELECT CONCAT('hello')")
	got := string(r.Rows[0][0])
	if got != "hello" {
		t.Errorf("got %q, want %q", got, "hello")
	}
}

func TestConcat_IntCoercion(t *testing.T) {
	e := setup(t)
	r := exec(t, e, "SELECT CONCAT('count: ', 42)")
	got := string(r.Rows[0][0])
	if got != "count: 42" {
		t.Errorf("got %q, want %q", got, "count: 42")
	}
}

func TestConcat_BoolCoercion(t *testing.T) {
	e := setup(t)
	r := exec(t, e, "SELECT CONCAT('is: ', TRUE, ', not: ', FALSE)")
	got := string(r.Rows[0][0])
	if got != "is: true, not: false" {
		t.Errorf("got %q, want %q", got, "is: true, not: false")
	}
}

func TestConcat_FromTable(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE people (first_name TEXT, last_name TEXT)")
	exec(t, e, "INSERT INTO people (first_name, last_name) VALUES ('John', 'Doe')")
	r := exec(t, e, "SELECT CONCAT(first_name, ' ', last_name) FROM people")
	got := string(r.Rows[0][0])
	if got != "John Doe" {
		t.Errorf("got %q, want %q", got, "John Doe")
	}
}

func TestConcat_InWhere(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (a TEXT, b TEXT)")
	exec(t, e, "INSERT INTO t (a, b) VALUES ('foo', 'bar')")
	exec(t, e, "INSERT INTO t (a, b) VALUES ('hello', 'world')")
	r := exec(t, e, "SELECT a FROM t WHERE CONCAT(a, b) = 'foobar'")
	if len(r.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(r.Rows))
	}
	if string(r.Rows[0][0]) != "foo" {
		t.Errorf("got %q, want %q", string(r.Rows[0][0]), "foo")
	}
}

func TestConcat_NoArgsError(t *testing.T) {
	e := setup(t)
	_, err := e.Execute("SELECT CONCAT()")
	if err == nil {
		t.Fatal("expected error for CONCAT() with no args")
	}
	qe, ok := err.(*QueryError)
	if !ok {
		t.Fatalf("expected *QueryError, got %T", err)
	}
	if qe.Code != "42883" {
		t.Errorf("SQLSTATE = %q, want 42883", qe.Code)
	}
}

func TestConcat_ColumnMetadata(t *testing.T) {
	e := setup(t)
	r := exec(t, e, "SELECT CONCAT('a', 'b')")
	if r.Columns[0].Name != "concat" {
		t.Errorf("column name = %q, want %q", r.Columns[0].Name, "concat")
	}
	if r.Columns[0].TypeOID != OIDText {
		t.Errorf("TypeOID = %d, want %d (OIDText)", r.Columns[0].TypeOID, OIDText)
	}
}
