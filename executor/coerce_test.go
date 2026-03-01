package executor

import (
	"testing"
	"time"

	"mulldb/storage"
)

func TestCoerceLiteral(t *testing.T) {
	tests := []struct {
		name    string
		val     any
		target  storage.DataType
		want    any
		wantErr bool
	}{
		// string → integer
		{"str→int ok", "42", storage.TypeInteger, int64(42), false},
		{"str→int negative", "-7", storage.TypeInteger, int64(-7), false},
		{"str→int fail", "hello", storage.TypeInteger, nil, true},
		{"str→int float string", "3.14", storage.TypeInteger, nil, true},

		// float64 → integer
		{"float→int whole", float64(5), storage.TypeInteger, int64(5), false},
		{"float→int fractional", 3.14, storage.TypeInteger, nil, true},

		// int64 → integer (same type)
		{"int→int", int64(10), storage.TypeInteger, int64(10), false},

		// bool → integer (not supported)
		{"bool→int", true, storage.TypeInteger, nil, true},

		// string → float
		{"str→float ok", "3.14", storage.TypeFloat, 3.14, false},
		{"str→float fail", "abc", storage.TypeFloat, nil, true},

		// int64 → float
		{"int→float", int64(5), storage.TypeFloat, float64(5), false},

		// float64 → float (same type)
		{"float→float", 2.5, storage.TypeFloat, 2.5, false},

		// bool → float (not supported)
		{"bool→float", true, storage.TypeFloat, nil, true},

		// int64 → text
		{"int→text", int64(42), storage.TypeText, "42", false},

		// float64 → text
		{"float→text", 3.14, storage.TypeText, "3.14", false},

		// bool → text
		{"bool→text true", true, storage.TypeText, "true", false},
		{"bool→text false", false, storage.TypeText, "false", false},

		// string → text (same type)
		{"str→text", "hello", storage.TypeText, "hello", false},

		// string → boolean
		{"str→bool true", "true", storage.TypeBoolean, true, false},
		{"str→bool TRUE", "TRUE", storage.TypeBoolean, true, false},
		{"str→bool t", "t", storage.TypeBoolean, true, false},
		{"str→bool 1", "1", storage.TypeBoolean, true, false},
		{"str→bool false", "false", storage.TypeBoolean, false, false},
		{"str→bool f", "f", storage.TypeBoolean, false, false},
		{"str→bool 0", "0", storage.TypeBoolean, false, false},
		{"str→bool fail", "maybe", storage.TypeBoolean, nil, true},

		// bool → boolean (same type)
		{"bool→bool", true, storage.TypeBoolean, true, false},

		// int → boolean (not supported)
		{"int→bool", int64(1), storage.TypeBoolean, nil, true},

		// string → timestamp
		{"str→timestamp ok", "2024-01-15 10:30:00", storage.TypeTimestamp, func() any {
			t, _ := time.Parse("2006-01-02 15:04:05", "2024-01-15 10:30:00")
			return t.UTC()
		}(), false},
		{"str→timestamp fail", "not-a-date", storage.TypeTimestamp, nil, true},

		// int → timestamp (not supported)
		{"int→timestamp", int64(123), storage.TypeTimestamp, nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := coerceLiteral(tt.val, tt.target)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got %v", got)
				}
				qe, ok := err.(*QueryError)
				if !ok {
					t.Fatalf("expected *QueryError, got %T", err)
				}
				if qe.Code != "22P02" {
					t.Fatalf("expected SQLSTATE 22P02, got %q", qe.Code)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			// Compare time.Time values specially.
			if wantTime, ok := tt.want.(time.Time); ok {
				gotTime, ok := got.(time.Time)
				if !ok {
					t.Fatalf("expected time.Time, got %T", got)
				}
				if !wantTime.Equal(gotTime) {
					t.Fatalf("got %v, want %v", gotTime, wantTime)
				}
				return
			}
			if got != tt.want {
				t.Fatalf("got %v (%T), want %v (%T)", got, got, tt.want, tt.want)
			}
		})
	}
}

func TestCoercion_IntegerColumnStringLiteral(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
	exec(t, e, "INSERT INTO t VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")

	// WHERE id = '1' should coerce '1' to int64 and match.
	r := exec(t, e, "SELECT * FROM t WHERE id = '1'")
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	if string(r.Rows[0][1]) != "alice" {
		t.Fatalf("expected 'alice', got %v", r.Rows[0][1])
	}
}

func TestCoercion_IntegerColumnStringLiteral_Error(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
	exec(t, e, "INSERT INTO t VALUES (1, 'alice')")

	// WHERE id = 'hello' should error with SQLSTATE 22P02.
	_, err := e.Execute("SELECT * FROM t WHERE id = 'hello'")
	if err == nil {
		t.Fatal("expected error for non-numeric string")
	}
	qe, ok := err.(*QueryError)
	if !ok {
		t.Fatalf("expected *QueryError, got %T: %v", err, err)
	}
	if qe.Code != "22P02" {
		t.Fatalf("expected SQLSTATE 22P02, got %q", qe.Code)
	}
}

func TestCoercion_InList(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
	exec(t, e, "INSERT INTO t VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")

	// WHERE id IN ('1', '3') should coerce and match rows 1, 3.
	r := exec(t, e, "SELECT * FROM t WHERE id IN ('1', '3')")
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r.Rows))
	}
}

func TestCoercion_InList_Error(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
	exec(t, e, "INSERT INTO t VALUES (1, 'alice')")

	_, err := e.Execute("SELECT * FROM t WHERE id IN ('1', 'bad')")
	if err == nil {
		t.Fatal("expected error for non-numeric string in IN list")
	}
	qe, ok := err.(*QueryError)
	if !ok {
		t.Fatalf("expected *QueryError, got %T: %v", err, err)
	}
	if qe.Code != "22P02" {
		t.Fatalf("expected SQLSTATE 22P02, got %q", qe.Code)
	}
}

func TestCoercion_ComparisonOperators(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
	exec(t, e, "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")

	// Test > with string literal.
	r := exec(t, e, "SELECT * FROM t WHERE id > '1'")
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows for id > '1', got %d", len(r.Rows))
	}

	// Test < with string literal.
	r = exec(t, e, "SELECT * FROM t WHERE id < '3'")
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows for id < '3', got %d", len(r.Rows))
	}

	// Test != with string literal.
	r = exec(t, e, "SELECT * FROM t WHERE id != '2'")
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows for id != '2', got %d", len(r.Rows))
	}
}

func TestCoercion_ReversedOperands(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
	exec(t, e, "INSERT INTO t VALUES (1, 'a'), (2, 'b')")

	// Literal on the left side: '1' = id.
	r := exec(t, e, "SELECT * FROM t WHERE '1' = id")
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
}

func TestCoercion_FloatColumn(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER PRIMARY KEY, score FLOAT)")
	exec(t, e, "INSERT INTO t VALUES (1, 3.14), (2, 2.71)")

	// Integer literal compared to float column.
	r := exec(t, e, "SELECT * FROM t WHERE score > 3")
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row for score > 3, got %d", len(r.Rows))
	}

	// String literal compared to float column.
	r = exec(t, e, "SELECT * FROM t WHERE score = '3.14'")
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row for score = '3.14', got %d", len(r.Rows))
	}
}

func TestCoercion_BooleanColumn(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER PRIMARY KEY, active BOOLEAN)")
	exec(t, e, "INSERT INTO t VALUES (1, TRUE), (2, FALSE)")

	r := exec(t, e, "SELECT * FROM t WHERE active = 'true'")
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
}

func TestCoercion_TextColumnIntLiteral(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER PRIMARY KEY, code TEXT)")
	exec(t, e, "INSERT INTO t VALUES (1, '42'), (2, 'hello')")

	// int literal 42 compared to text column should coerce to "42".
	r := exec(t, e, "SELECT * FROM t WHERE code = 42")
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
}

func TestCoercion_UpdateWhere(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
	exec(t, e, "INSERT INTO t VALUES (1, 'old')")

	exec(t, e, "UPDATE t SET name = 'new' WHERE id = '1'")
	r := exec(t, e, "SELECT name FROM t WHERE id = 1")
	if len(r.Rows) != 1 || string(r.Rows[0][0]) != "new" {
		t.Fatalf("expected updated name 'new', got %v", r.Rows)
	}
}

func TestCoercion_DeleteWhere(t *testing.T) {
	e := setup(t)
	exec(t, e, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
	exec(t, e, "INSERT INTO t VALUES (1, 'alice'), (2, 'bob')")

	exec(t, e, "DELETE FROM t WHERE id = '1'")
	r := exec(t, e, "SELECT * FROM t")
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row after delete, got %d", len(r.Rows))
	}
}
