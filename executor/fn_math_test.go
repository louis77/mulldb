package executor

import (
	"errors"
	"testing"
)

func TestFnAbs(t *testing.T) {
	e := setup(t)

	r := exec(t, e, "SELECT ABS(-5)")
	if string(r.Rows[0][0]) != "5" {
		t.Errorf("ABS(-5) = %q, want 5", r.Rows[0][0])
	}
	if r.Columns[0].TypeOID != OIDInt8 {
		t.Errorf("ABS(-5) OID = %d, want %d (int)", r.Columns[0].TypeOID, OIDInt8)
	}

	r = exec(t, e, "SELECT ABS(-3.14)")
	if string(r.Rows[0][0]) != "3.14" {
		t.Errorf("ABS(-3.14) = %q, want 3.14", r.Rows[0][0])
	}
	if r.Columns[0].TypeOID != OIDFloat8 {
		t.Errorf("ABS(-3.14) OID = %d, want %d (float)", r.Columns[0].TypeOID, OIDFloat8)
	}

	r = exec(t, e, "SELECT ABS(5)")
	if string(r.Rows[0][0]) != "5" {
		t.Errorf("ABS(5) = %q, want 5", r.Rows[0][0])
	}
}

func TestFnRound(t *testing.T) {
	e := setup(t)

	r := exec(t, e, "SELECT ROUND(3.7)")
	if string(r.Rows[0][0]) != "4" {
		t.Errorf("ROUND(3.7) = %q, want 4", r.Rows[0][0])
	}

	r = exec(t, e, "SELECT ROUND(3.14159, 2)")
	if string(r.Rows[0][0]) != "3.14" {
		t.Errorf("ROUND(3.14159, 2) = %q, want 3.14", r.Rows[0][0])
	}

	r = exec(t, e, "SELECT ROUND(2.5)")
	// Go math.Round rounds half away from zero, so 2.5 → 3.
	// PostgreSQL rounds half away from zero for numeric, but to even for float.
	// We use math.Round which matches Go behavior.
	want := "3" // math.Round(2.5) = 3
	if string(r.Rows[0][0]) != want {
		t.Errorf("ROUND(2.5) = %q, want %s", r.Rows[0][0], want)
	}

	// Round with integer input.
	r = exec(t, e, "SELECT ROUND(5)")
	if string(r.Rows[0][0]) != "5" {
		t.Errorf("ROUND(5) = %q, want 5", r.Rows[0][0])
	}
}

func TestFnCeil(t *testing.T) {
	e := setup(t)

	r := exec(t, e, "SELECT CEIL(2.3)")
	if string(r.Rows[0][0]) != "3" {
		t.Errorf("CEIL(2.3) = %q, want 3", r.Rows[0][0])
	}

	r = exec(t, e, "SELECT CEILING(-2.3)")
	if string(r.Rows[0][0]) != "-2" {
		t.Errorf("CEILING(-2.3) = %q, want -2", r.Rows[0][0])
	}

	r = exec(t, e, "SELECT CEIL(5)")
	if string(r.Rows[0][0]) != "5" {
		t.Errorf("CEIL(5) = %q, want 5", r.Rows[0][0])
	}
}

func TestFnFloor(t *testing.T) {
	e := setup(t)

	r := exec(t, e, "SELECT FLOOR(2.7)")
	if string(r.Rows[0][0]) != "2" {
		t.Errorf("FLOOR(2.7) = %q, want 2", r.Rows[0][0])
	}

	r = exec(t, e, "SELECT FLOOR(-2.3)")
	if string(r.Rows[0][0]) != "-3" {
		t.Errorf("FLOOR(-2.3) = %q, want -3", r.Rows[0][0])
	}
}

func TestFnPower(t *testing.T) {
	e := setup(t)

	r := exec(t, e, "SELECT POWER(2, 10)")
	if string(r.Rows[0][0]) != "1024" {
		t.Errorf("POWER(2, 10) = %q, want 1024", r.Rows[0][0])
	}

	r = exec(t, e, "SELECT POW(2.0, 0.5)")
	want := "1.4142135623730951"
	if string(r.Rows[0][0]) != want {
		t.Errorf("POW(2.0, 0.5) = %q, want %s", r.Rows[0][0], want)
	}
}

func TestFnSqrt(t *testing.T) {
	e := setup(t)

	r := exec(t, e, "SELECT SQRT(16)")
	if string(r.Rows[0][0]) != "4" {
		t.Errorf("SQRT(16) = %q, want 4", r.Rows[0][0])
	}

	r = exec(t, e, "SELECT SQRT(2.0)")
	want := "1.4142135623730951"
	if string(r.Rows[0][0]) != want {
		t.Errorf("SQRT(2.0) = %q, want %s", r.Rows[0][0], want)
	}

	// Negative input should error.
	_, err := e.Execute("SELECT SQRT(-1)")
	if err == nil {
		t.Fatal("expected error for SQRT(-1)")
	}
	var qe *QueryError
	if !errors.As(err, &qe) || qe.Code != "2201F" {
		t.Errorf("expected SQLSTATE 2201F, got %v", err)
	}
}

func TestFnMod(t *testing.T) {
	e := setup(t)

	r := exec(t, e, "SELECT MOD(7, 3)")
	if string(r.Rows[0][0]) != "1" {
		t.Errorf("MOD(7, 3) = %q, want 1", r.Rows[0][0])
	}
	if r.Columns[0].TypeOID != OIDInt8 {
		t.Errorf("MOD(7, 3) OID = %d, want %d (int)", r.Columns[0].TypeOID, OIDInt8)
	}

	r = exec(t, e, "SELECT MOD(7.5, 2.0)")
	if string(r.Rows[0][0]) != "1.5" {
		t.Errorf("MOD(7.5, 2.0) = %q, want 1.5", r.Rows[0][0])
	}

	// Division by zero.
	_, err := e.Execute("SELECT MOD(5, 0)")
	if err == nil {
		t.Fatal("expected error for MOD(5, 0)")
	}
	var qe *QueryError
	if !errors.As(err, &qe) || qe.Code != "22012" {
		t.Errorf("expected SQLSTATE 22012, got %v", err)
	}
}

func TestFnMathNullPropagation(t *testing.T) {
	e := setup(t)

	r := exec(t, e, "SELECT ABS(NULL)")
	if r.Rows[0][0] != nil {
		t.Errorf("ABS(NULL) = %q, want NULL", r.Rows[0][0])
	}

	r = exec(t, e, "SELECT ROUND(NULL)")
	if r.Rows[0][0] != nil {
		t.Errorf("ROUND(NULL) = %q, want NULL", r.Rows[0][0])
	}

	r = exec(t, e, "SELECT SQRT(NULL)")
	if r.Rows[0][0] != nil {
		t.Errorf("SQRT(NULL) = %q, want NULL", r.Rows[0][0])
	}

	r = exec(t, e, "SELECT POWER(NULL, 2)")
	if r.Rows[0][0] != nil {
		t.Errorf("POWER(NULL, 2) = %q, want NULL", r.Rows[0][0])
	}
}
