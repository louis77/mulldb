package parser

import (
	"testing"
)

// ---------------------------------------------------------------------------
// Lexer tests
// ---------------------------------------------------------------------------

func TestLexer_Tokens(t *testing.T) {
	input := "SELECT *, id FROM foo WHERE age >= 21 AND name != 'bob';"

	want := []struct {
		typ TokenType
		lit string
	}{
		{TokenSelect, "SELECT"},
		{TokenStar, "*"},
		{TokenComma, ","},
		{TokenIdent, "id"},
		{TokenFrom, "FROM"},
		{TokenIdent, "foo"},
		{TokenWhere, "WHERE"},
		{TokenIdent, "age"},
		{TokenGtEq, ">="},
		{TokenIntLit, "21"},
		{TokenAnd, "AND"},
		{TokenIdent, "name"},
		{TokenNotEq, "!="},
		{TokenStrLit, "bob"},
		{TokenSemicolon, ";"},
		{TokenEOF, ""},
	}

	lex := NewLexer(input)
	for i, w := range want {
		tok := lex.NextToken()
		if tok.Type != w.typ {
			t.Fatalf("token[%d]: type = %s, want %s", i, tok.Type, w.typ)
		}
		if tok.Literal != w.lit {
			t.Fatalf("token[%d]: literal = %q, want %q", i, tok.Literal, w.lit)
		}
	}
}

func TestLexer_Operators(t *testing.T) {
	input := "= != <> < > <= >="
	want := []TokenType{
		TokenEq, TokenNotEq, TokenNotEq, TokenLt, TokenGt, TokenLtEq, TokenGtEq, TokenEOF,
	}
	lex := NewLexer(input)
	for i, w := range want {
		tok := lex.NextToken()
		if tok.Type != w {
			t.Fatalf("token[%d]: type = %s, want %s", i, tok.Type, w)
		}
	}
}

func TestLexer_KeywordsCaseInsensitive(t *testing.T) {
	input := "select FROM Where"
	want := []TokenType{TokenSelect, TokenFrom, TokenWhere, TokenEOF}
	lex := NewLexer(input)
	for i, w := range want {
		tok := lex.NextToken()
		if tok.Type != w {
			t.Fatalf("token[%d]: type = %s, want %s", i, tok.Type, w)
		}
	}
}

// ---------------------------------------------------------------------------
// CREATE TABLE
// ---------------------------------------------------------------------------

func TestParse_CreateTable(t *testing.T) {
	stmt, err := Parse("CREATE TABLE users (id INTEGER, name TEXT, active BOOLEAN)")
	if err != nil {
		t.Fatal(err)
	}
	ct, ok := stmt.(*CreateTableStmt)
	if !ok {
		t.Fatalf("got %T, want *CreateTableStmt", stmt)
	}
	if ct.Name.Name != "users" {
		t.Errorf("table name = %q, want %q", ct.Name.Name, "users")
	}
	if len(ct.Columns) != 3 {
		t.Fatalf("columns count = %d, want 3", len(ct.Columns))
	}
	wantCols := []ColumnDef{
		{"id", "INTEGER", false, false},
		{"name", "TEXT", false, false},
		{"active", "BOOLEAN", false, false},
	}
	for i, want := range wantCols {
		got := ct.Columns[i]
		if got != want {
			t.Errorf("column[%d] = %+v, want %+v", i, got, want)
		}
	}
}

func TestParse_CreateTablePrimaryKey(t *testing.T) {
	stmt, err := Parse("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatal(err)
	}
	ct := stmt.(*CreateTableStmt)
	if len(ct.Columns) != 2 {
		t.Fatalf("columns = %d, want 2", len(ct.Columns))
	}
	if !ct.Columns[0].PrimaryKey {
		t.Error("column[0].PrimaryKey = false, want true")
	}
	if ct.Columns[1].PrimaryKey {
		t.Error("column[1].PrimaryKey = true, want false")
	}
}

func TestParse_CreateTableMultiplePKError(t *testing.T) {
	_, err := Parse("CREATE TABLE t (a INTEGER PRIMARY KEY, b TEXT PRIMARY KEY)")
	if err == nil {
		t.Fatal("expected error for multiple primary keys")
	}
}

func TestParse_CreateTableNotNull(t *testing.T) {
	stmt, err := Parse("CREATE TABLE t (id INTEGER NOT NULL, name TEXT)")
	if err != nil {
		t.Fatal(err)
	}
	ct := stmt.(*CreateTableStmt)
	if !ct.Columns[0].NotNull {
		t.Error("column[0].NotNull = false, want true")
	}
	if ct.Columns[1].NotNull {
		t.Error("column[1].NotNull = true, want false")
	}
}

func TestParse_CreateTableNotNullPrimaryKey(t *testing.T) {
	tests := []struct {
		sql  string
		desc string
	}{
		{"CREATE TABLE t (id INTEGER PRIMARY KEY NOT NULL, name TEXT)", "PK then NOT NULL"},
		{"CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, name TEXT)", "NOT NULL then PK"},
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if err != nil {
				t.Fatal(err)
			}
			ct := stmt.(*CreateTableStmt)
			if !ct.Columns[0].PrimaryKey {
				t.Error("column[0].PrimaryKey = false, want true")
			}
			if !ct.Columns[0].NotNull {
				t.Error("column[0].NotNull = false, want true")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// DROP TABLE
// ---------------------------------------------------------------------------

func TestParse_DropTable(t *testing.T) {
	stmt, err := Parse("DROP TABLE users;")
	if err != nil {
		t.Fatal(err)
	}
	dt, ok := stmt.(*DropTableStmt)
	if !ok {
		t.Fatalf("got %T, want *DropTableStmt", stmt)
	}
	if dt.Name.Name != "users" {
		t.Errorf("table name = %q, want %q", dt.Name.Name, "users")
	}
}

// ---------------------------------------------------------------------------
// INSERT
// ---------------------------------------------------------------------------

func TestParse_InsertWithColumns(t *testing.T) {
	stmt, err := Parse("INSERT INTO users (id, name, active) VALUES (1, 'alice', TRUE)")
	if err != nil {
		t.Fatal(err)
	}
	ins, ok := stmt.(*InsertStmt)
	if !ok {
		t.Fatalf("got %T, want *InsertStmt", stmt)
	}
	if ins.Table.Name != "users" {
		t.Errorf("table = %q, want %q", ins.Table.Name, "users")
	}
	if len(ins.Columns) != 3 {
		t.Fatalf("columns = %v, want [id name active]", ins.Columns)
	}
	if len(ins.Values) != 1 {
		t.Fatalf("value rows = %d, want 1", len(ins.Values))
	}
	row := ins.Values[0]
	if len(row) != 3 {
		t.Fatalf("values in row = %d, want 3", len(row))
	}
	assertIntLit(t, row[0], 1)
	assertStrLit(t, row[1], "alice")
	assertBoolLit(t, row[2], true)
}

func TestParse_InsertWithoutColumns(t *testing.T) {
	stmt, err := Parse("INSERT INTO users VALUES (1, 'bob', FALSE)")
	if err != nil {
		t.Fatal(err)
	}
	ins := stmt.(*InsertStmt)
	if ins.Columns != nil {
		t.Errorf("columns = %v, want nil", ins.Columns)
	}
	if len(ins.Values) != 1 {
		t.Fatalf("value rows = %d, want 1", len(ins.Values))
	}
}

func TestParse_InsertMultipleRows(t *testing.T) {
	stmt, err := Parse("INSERT INTO users (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c')")
	if err != nil {
		t.Fatal(err)
	}
	ins := stmt.(*InsertStmt)
	if len(ins.Values) != 3 {
		t.Fatalf("value rows = %d, want 3", len(ins.Values))
	}
	for i, row := range ins.Values {
		if len(row) != 2 {
			t.Errorf("row[%d] values = %d, want 2", i, len(row))
		}
	}
}

func TestParse_InsertNull(t *testing.T) {
	stmt, err := Parse("INSERT INTO users (id, name) VALUES (1, NULL)")
	if err != nil {
		t.Fatal(err)
	}
	ins := stmt.(*InsertStmt)
	row := ins.Values[0]
	if _, ok := row[1].(*NullLit); !ok {
		t.Errorf("values[1] is %T, want *NullLit", row[1])
	}
}

// ---------------------------------------------------------------------------
// SELECT
// ---------------------------------------------------------------------------

func TestParse_SelectStar(t *testing.T) {
	stmt, err := Parse("SELECT * FROM users")
	if err != nil {
		t.Fatal(err)
	}
	sel, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("got %T, want *SelectStmt", stmt)
	}
	if len(sel.Columns) != 1 {
		t.Fatalf("columns = %d, want 1", len(sel.Columns))
	}
	if _, ok := sel.Columns[0].(*StarExpr); !ok {
		t.Errorf("column[0] is %T, want *StarExpr", sel.Columns[0])
	}
	if sel.From.Name != "users" {
		t.Errorf("from = %q, want %q", sel.From.Name, "users")
	}
	if sel.Where != nil {
		t.Errorf("where should be nil")
	}
}

func TestParse_SelectStarWithColumns(t *testing.T) {
	t.Run("star then column", func(t *testing.T) {
		stmt, err := Parse("SELECT *, name FROM t")
		if err != nil {
			t.Fatal(err)
		}
		sel := stmt.(*SelectStmt)
		if len(sel.Columns) != 2 {
			t.Fatalf("columns = %d, want 2", len(sel.Columns))
		}
		if _, ok := sel.Columns[0].(*StarExpr); !ok {
			t.Errorf("column[0] is %T, want *StarExpr", sel.Columns[0])
		}
		assertColumnRef(t, sel.Columns[1], "name")
	})

	t.Run("column then star", func(t *testing.T) {
		stmt, err := Parse("SELECT id, * FROM t")
		if err != nil {
			t.Fatal(err)
		}
		sel := stmt.(*SelectStmt)
		if len(sel.Columns) != 2 {
			t.Fatalf("columns = %d, want 2", len(sel.Columns))
		}
		assertColumnRef(t, sel.Columns[0], "id")
		if _, ok := sel.Columns[1].(*StarExpr); !ok {
			t.Errorf("column[1] is %T, want *StarExpr", sel.Columns[1])
		}
	})
}

func TestParse_SelectColumns(t *testing.T) {
	stmt, err := Parse("SELECT id, name FROM users")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if len(sel.Columns) != 2 {
		t.Fatalf("columns = %d, want 2", len(sel.Columns))
	}
	assertColumnRef(t, sel.Columns[0], "id")
	assertColumnRef(t, sel.Columns[1], "name")
}

func TestParse_SelectWhere(t *testing.T) {
	stmt, err := Parse("SELECT * FROM users WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if sel.Where == nil {
		t.Fatal("where is nil")
	}
	bin := sel.Where.(*BinaryExpr)
	assertColumnRef(t, bin.Left, "id")
	if bin.Op != "=" {
		t.Errorf("op = %q, want %q", bin.Op, "=")
	}
	assertIntLit(t, bin.Right, 1)
}

func TestParse_SelectWhereAndOr(t *testing.T) {
	stmt, err := Parse("SELECT * FROM users WHERE age > 18 AND active = TRUE OR name = 'admin'")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)

	// Should parse as: (age > 18 AND active = TRUE) OR (name = 'admin')
	or := sel.Where.(*BinaryExpr)
	if or.Op != "OR" {
		t.Fatalf("top-level op = %q, want OR", or.Op)
	}

	and := or.Left.(*BinaryExpr)
	if and.Op != "AND" {
		t.Fatalf("left op = %q, want AND", and.Op)
	}

	gt := and.Left.(*BinaryExpr)
	if gt.Op != ">" {
		t.Errorf("and.left op = %q, want >", gt.Op)
	}

	nameEq := or.Right.(*BinaryExpr)
	assertColumnRef(t, nameEq.Left, "name")
	assertStrLit(t, nameEq.Right, "admin")
}

func TestParse_SelectWhereComparisons(t *testing.T) {
	ops := []struct {
		sql string
		op  string
	}{
		{"SELECT * FROM t WHERE a = 1", "="},
		{"SELECT * FROM t WHERE a != 1", "!="},
		{"SELECT * FROM t WHERE a <> 1", "!="},
		{"SELECT * FROM t WHERE a < 1", "<"},
		{"SELECT * FROM t WHERE a > 1", ">"},
		{"SELECT * FROM t WHERE a <= 1", "<="},
		{"SELECT * FROM t WHERE a >= 1", ">="},
	}
	for _, tc := range ops {
		stmt, err := Parse(tc.sql)
		if err != nil {
			t.Fatalf("%s: %v", tc.sql, err)
		}
		sel := stmt.(*SelectStmt)
		bin := sel.Where.(*BinaryExpr)
		if bin.Op != tc.op {
			t.Errorf("%s: op = %q, want %q", tc.sql, bin.Op, tc.op)
		}
	}
}

// ---------------------------------------------------------------------------
// UPDATE
// ---------------------------------------------------------------------------

func TestParse_Update(t *testing.T) {
	stmt, err := Parse("UPDATE users SET name = 'bob', active = FALSE WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	upd, ok := stmt.(*UpdateStmt)
	if !ok {
		t.Fatalf("got %T, want *UpdateStmt", stmt)
	}
	if upd.Table.Name != "users" {
		t.Errorf("table = %q, want %q", upd.Table.Name, "users")
	}
	if len(upd.Sets) != 2 {
		t.Fatalf("sets = %d, want 2", len(upd.Sets))
	}
	if upd.Sets[0].Column != "name" {
		t.Errorf("sets[0].column = %q, want %q", upd.Sets[0].Column, "name")
	}
	assertStrLit(t, upd.Sets[0].Value, "bob")
	if upd.Sets[1].Column != "active" {
		t.Errorf("sets[1].column = %q, want %q", upd.Sets[1].Column, "active")
	}
	assertBoolLit(t, upd.Sets[1].Value, false)

	if upd.Where == nil {
		t.Fatal("where is nil")
	}
	bin := upd.Where.(*BinaryExpr)
	assertColumnRef(t, bin.Left, "id")
	assertIntLit(t, bin.Right, 1)
}

func TestParse_UpdateNoWhere(t *testing.T) {
	stmt, err := Parse("UPDATE users SET active = TRUE")
	if err != nil {
		t.Fatal(err)
	}
	upd := stmt.(*UpdateStmt)
	if upd.Where != nil {
		t.Errorf("where should be nil")
	}
}

// ---------------------------------------------------------------------------
// DELETE
// ---------------------------------------------------------------------------

func TestParse_Delete(t *testing.T) {
	stmt, err := Parse("DELETE FROM users WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	del, ok := stmt.(*DeleteStmt)
	if !ok {
		t.Fatalf("got %T, want *DeleteStmt", stmt)
	}
	if del.Table.Name != "users" {
		t.Errorf("table = %q, want %q", del.Table.Name, "users")
	}
	if del.Where == nil {
		t.Fatal("where is nil")
	}
}

func TestParse_DeleteNoWhere(t *testing.T) {
	stmt, err := Parse("DELETE FROM users")
	if err != nil {
		t.Fatal(err)
	}
	del := stmt.(*DeleteStmt)
	if del.Where != nil {
		t.Errorf("where should be nil")
	}
}

// ---------------------------------------------------------------------------
// Error cases
// ---------------------------------------------------------------------------

func TestParse_Errors(t *testing.T) {
	cases := []string{
		"",
		"FROBNICATE",
		"SELECT",
		"SELECT * FROM",
		"CREATE TABLE",
		"CREATE TABLE t ()",
		"INSERT INTO",
		"INSERT INTO t VALUES",
		"SELECT * FROM t WHERE",
	}
	for _, sql := range cases {
		_, err := Parse(sql)
		if err == nil {
			t.Errorf("expected error for %q", sql)
		}
	}
}

func TestParse_TrailingSemicolon(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t;")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := stmt.(*SelectStmt); !ok {
		t.Fatalf("got %T, want *SelectStmt", stmt)
	}
}

func TestParse_ParenthesizedExpr(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t WHERE (a = 1 OR b = 2) AND c = 3")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	and := sel.Where.(*BinaryExpr)
	if and.Op != "AND" {
		t.Fatalf("top op = %q, want AND", and.Op)
	}
	or := and.Left.(*BinaryExpr)
	if or.Op != "OR" {
		t.Fatalf("left op = %q, want OR", or.Op)
	}
}

// ---------------------------------------------------------------------------
// SELECT without FROM
// ---------------------------------------------------------------------------

func TestParse_SelectNoFrom_IntLit(t *testing.T) {
	stmt, err := Parse("SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	sel, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("got %T, want *SelectStmt", stmt)
	}
	if !sel.From.IsEmpty() {
		t.Errorf("from = %q, want empty", sel.From.String())
	}
	if len(sel.Columns) != 1 {
		t.Fatalf("columns = %d, want 1", len(sel.Columns))
	}
	assertIntLit(t, sel.Columns[0], 1)
}

func TestParse_SelectNoFrom_MultipleExprs(t *testing.T) {
	stmt, err := Parse("SELECT 1, 'hello', TRUE, NULL")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if !sel.From.IsEmpty() {
		t.Errorf("from = %q, want empty", sel.From.String())
	}
	if len(sel.Columns) != 4 {
		t.Fatalf("columns = %d, want 4", len(sel.Columns))
	}
	assertIntLit(t, sel.Columns[0], 1)
	assertStrLit(t, sel.Columns[1], "hello")
	assertBoolLit(t, sel.Columns[2], true)
	if _, ok := sel.Columns[3].(*NullLit); !ok {
		t.Errorf("columns[3] is %T, want *NullLit", sel.Columns[3])
	}
}

func TestParse_SelectNoFrom_FunctionCall(t *testing.T) {
	stmt, err := Parse("SELECT VERSION()")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if !sel.From.IsEmpty() {
		t.Errorf("from = %q, want empty", sel.From.String())
	}
	fn, ok := sel.Columns[0].(*FunctionCallExpr)
	if !ok {
		t.Fatalf("got %T, want *FunctionCallExpr", sel.Columns[0])
	}
	if fn.Name != "VERSION" {
		t.Errorf("name = %q, want VERSION", fn.Name)
	}
	if len(fn.Args) != 0 {
		t.Errorf("args = %d, want 0", len(fn.Args))
	}
}

// ---------------------------------------------------------------------------
// Aggregate functions
// ---------------------------------------------------------------------------

func TestParse_Aggregate_CountStar(t *testing.T) {
	stmt, err := Parse("SELECT COUNT(*) FROM t")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if len(sel.Columns) != 1 {
		t.Fatalf("columns = %d, want 1", len(sel.Columns))
	}
	fn, ok := sel.Columns[0].(*FunctionCallExpr)
	if !ok {
		t.Fatalf("got %T, want *FunctionCallExpr", sel.Columns[0])
	}
	if fn.Name != "COUNT" {
		t.Errorf("name = %q, want COUNT", fn.Name)
	}
	if len(fn.Args) != 1 {
		t.Fatalf("args = %d, want 1", len(fn.Args))
	}
	if _, ok := fn.Args[0].(*StarExpr); !ok {
		t.Errorf("arg[0] is %T, want *StarExpr", fn.Args[0])
	}
}

func TestParse_Aggregate_Sum(t *testing.T) {
	stmt, err := Parse("SELECT SUM(val) FROM t")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	fn, ok := sel.Columns[0].(*FunctionCallExpr)
	if !ok {
		t.Fatalf("got %T, want *FunctionCallExpr", sel.Columns[0])
	}
	if fn.Name != "SUM" {
		t.Errorf("name = %q, want SUM", fn.Name)
	}
	if len(fn.Args) != 1 {
		t.Fatalf("args = %d, want 1", len(fn.Args))
	}
	assertColumnRef(t, fn.Args[0], "val")
}

func TestParse_Aggregate_MinMax(t *testing.T) {
	for _, sql := range []string{"SELECT MIN(score) FROM t", "SELECT MAX(score) FROM t"} {
		stmt, err := Parse(sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		sel := stmt.(*SelectStmt)
		fn, ok := sel.Columns[0].(*FunctionCallExpr)
		if !ok {
			t.Fatalf("%s: got %T, want *FunctionCallExpr", sql, sel.Columns[0])
		}
		if fn.Name != "MIN" && fn.Name != "MAX" {
			t.Errorf("%s: name = %q, want MIN or MAX", sql, fn.Name)
		}
		assertColumnRef(t, fn.Args[0], "score")
	}
}

func TestParse_Aggregate_MultipleAggregates(t *testing.T) {
	stmt, err := Parse("SELECT COUNT(*), SUM(val) FROM t")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if len(sel.Columns) != 2 {
		t.Fatalf("columns = %d, want 2", len(sel.Columns))
	}
	if _, ok := sel.Columns[0].(*FunctionCallExpr); !ok {
		t.Errorf("col[0] is %T, want *FunctionCallExpr", sel.Columns[0])
	}
	if _, ok := sel.Columns[1].(*FunctionCallExpr); !ok {
		t.Errorf("col[1] is %T, want *FunctionCallExpr", sel.Columns[1])
	}
}

func TestParse_Aggregate_CaseInsensitive(t *testing.T) {
	stmt, err := Parse("SELECT sum(val), count(*) FROM t")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	names := []string{"SUM", "COUNT"}
	for i, want := range names {
		fn := sel.Columns[i].(*FunctionCallExpr)
		if fn.Name != want {
			t.Errorf("col[%d].Name = %q, want %q", i, fn.Name, want)
		}
	}
}

// ---------------------------------------------------------------------------
// AS alias
// ---------------------------------------------------------------------------

func TestParse_SelectColumnAlias(t *testing.T) {
	stmt, err := Parse("SELECT id AS user_id, name AS user_name FROM users")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if len(sel.Columns) != 2 {
		t.Fatalf("columns = %d, want 2", len(sel.Columns))
	}
	a0, ok := sel.Columns[0].(*AliasExpr)
	if !ok {
		t.Fatalf("col[0] is %T, want *AliasExpr", sel.Columns[0])
	}
	assertColumnRef(t, a0.Expr, "id")
	if a0.Alias != "user_id" {
		t.Errorf("alias[0] = %q, want user_id", a0.Alias)
	}
	a1, ok := sel.Columns[1].(*AliasExpr)
	if !ok {
		t.Fatalf("col[1] is %T, want *AliasExpr", sel.Columns[1])
	}
	assertColumnRef(t, a1.Expr, "name")
	if a1.Alias != "user_name" {
		t.Errorf("alias[1] = %q, want user_name", a1.Alias)
	}
}

func TestParse_SelectAggregateAlias(t *testing.T) {
	stmt, err := Parse("SELECT COUNT(*) AS total FROM t")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if len(sel.Columns) != 1 {
		t.Fatalf("columns = %d, want 1", len(sel.Columns))
	}
	a, ok := sel.Columns[0].(*AliasExpr)
	if !ok {
		t.Fatalf("col[0] is %T, want *AliasExpr", sel.Columns[0])
	}
	fn, ok := a.Expr.(*FunctionCallExpr)
	if !ok {
		t.Fatalf("alias.Expr is %T, want *FunctionCallExpr", a.Expr)
	}
	if fn.Name != "COUNT" {
		t.Errorf("fn.Name = %q, want COUNT", fn.Name)
	}
	if a.Alias != "total" {
		t.Errorf("alias = %q, want total", a.Alias)
	}
}

func TestParse_SelectMixedAliasNoAlias(t *testing.T) {
	stmt, err := Parse("SELECT id, name AS n FROM t")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if len(sel.Columns) != 2 {
		t.Fatalf("columns = %d, want 2", len(sel.Columns))
	}
	// First column: no alias, plain ColumnRef.
	assertColumnRef(t, sel.Columns[0], "id")
	// Second column: alias.
	a, ok := sel.Columns[1].(*AliasExpr)
	if !ok {
		t.Fatalf("col[1] is %T, want *AliasExpr", sel.Columns[1])
	}
	assertColumnRef(t, a.Expr, "name")
	if a.Alias != "n" {
		t.Errorf("alias = %q, want n", a.Alias)
	}
}

// ---------------------------------------------------------------------------
// LIMIT / OFFSET
// ---------------------------------------------------------------------------

func TestParse_SelectLimit(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t LIMIT 10")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if sel.Limit == nil || *sel.Limit != 10 {
		t.Errorf("limit = %v, want 10", sel.Limit)
	}
	if sel.Offset != nil {
		t.Errorf("offset = %v, want nil", sel.Offset)
	}
}

func TestParse_SelectOffset(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t OFFSET 5")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if sel.Offset == nil || *sel.Offset != 5 {
		t.Errorf("offset = %v, want 5", sel.Offset)
	}
	if sel.Limit != nil {
		t.Errorf("limit = %v, want nil", sel.Limit)
	}
}

func TestParse_SelectLimitOffset(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t LIMIT 10 OFFSET 5")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if sel.Limit == nil || *sel.Limit != 10 {
		t.Errorf("limit = %v, want 10", sel.Limit)
	}
	if sel.Offset == nil || *sel.Offset != 5 {
		t.Errorf("offset = %v, want 5", sel.Offset)
	}
}

func TestParse_SelectOffsetLimit(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t OFFSET 5 LIMIT 10")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if sel.Limit == nil || *sel.Limit != 10 {
		t.Errorf("limit = %v, want 10", sel.Limit)
	}
	if sel.Offset == nil || *sel.Offset != 5 {
		t.Errorf("offset = %v, want 5", sel.Offset)
	}
}

func TestParse_SelectWhereLimitOffset(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t WHERE id > 1 LIMIT 10 OFFSET 5")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if sel.Where == nil {
		t.Fatal("where is nil")
	}
	if sel.Limit == nil || *sel.Limit != 10 {
		t.Errorf("limit = %v, want 10", sel.Limit)
	}
	if sel.Offset == nil || *sel.Offset != 5 {
		t.Errorf("offset = %v, want 5", sel.Offset)
	}
}

// ---------------------------------------------------------------------------
// Double-quoted identifiers
// ---------------------------------------------------------------------------

func TestLexer_QuotedIdent(t *testing.T) {
	lex := NewLexer(`"users"`)
	tok := lex.NextToken()
	if tok.Type != TokenIdent || tok.Literal != "users" {
		t.Fatalf("got %s %q, want IDENT users", tok.Type, tok.Literal)
	}
}

func TestLexer_QuotedIdentEscape(t *testing.T) {
	lex := NewLexer(`"say""hello"`)
	tok := lex.NextToken()
	if tok.Type != TokenIdent || tok.Literal != `say"hello` {
		t.Fatalf("got %s %q, want IDENT say\"hello", tok.Type, tok.Literal)
	}
}

func TestLexer_QuotedIdentReservedWord(t *testing.T) {
	lex := NewLexer(`"select"`)
	tok := lex.NextToken()
	if tok.Type != TokenIdent {
		t.Fatalf("got %s, want IDENT (reserved word should be identifier when quoted)", tok.Type)
	}
	if tok.Literal != "select" {
		t.Errorf("literal = %q, want select", tok.Literal)
	}
}

func TestLexer_QuotedIdentUnterminated(t *testing.T) {
	lex := NewLexer(`"oops`)
	tok := lex.NextToken()
	if tok.Type != TokenIllegal {
		t.Fatalf("got %s, want ILLEGAL for unterminated quoted ident", tok.Type)
	}
}

func TestParse_SelectQuotedTableColumns(t *testing.T) {
	stmt, err := Parse(`SELECT "id" FROM "users"`)
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	assertColumnRef(t, sel.Columns[0], "id")
	if sel.From.Name != "users" {
		t.Errorf("from = %q, want users", sel.From.Name)
	}
}

func TestParse_SelectSchemaQualifiedQuoted(t *testing.T) {
	stmt, err := Parse(`SELECT * FROM "public"."names"`)
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if sel.From.Schema != "public" {
		t.Errorf("schema = %q, want public", sel.From.Schema)
	}
	if sel.From.Name != "names" {
		t.Errorf("name = %q, want names", sel.From.Name)
	}
}

func TestParse_SelectReservedWordAsColumn(t *testing.T) {
	stmt, err := Parse(`SELECT "select" FROM t`)
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	assertColumnRef(t, sel.Columns[0], "select")
}

func TestParse_CreateTableTypeAliases(t *testing.T) {
	stmt, err := Parse("CREATE TABLE t (a INT, b SMALLINT, c INTEGER, d BIGINT)")
	if err != nil {
		t.Fatal(err)
	}
	ct := stmt.(*CreateTableStmt)
	for i, col := range ct.Columns {
		if col.DataType != "INTEGER" {
			t.Errorf("column[%d] %q DataType = %q, want INTEGER", i, col.Name, col.DataType)
		}
	}
}

func TestParse_CreateTableReservedWords(t *testing.T) {
	stmt, err := Parse(`CREATE TABLE "table" ("select" INTEGER, "from" TEXT)`)
	if err != nil {
		t.Fatal(err)
	}
	ct := stmt.(*CreateTableStmt)
	if ct.Name.Name != "table" {
		t.Errorf("table name = %q, want table", ct.Name.Name)
	}
	if ct.Columns[0].Name != "select" {
		t.Errorf("col[0] = %q, want select", ct.Columns[0].Name)
	}
	if ct.Columns[1].Name != "from" {
		t.Errorf("col[1] = %q, want from", ct.Columns[1].Name)
	}
}

// ---------------------------------------------------------------------------
// Dot token / schema-qualified names
// ---------------------------------------------------------------------------

func TestLexer_DotToken(t *testing.T) {
	lex := NewLexer("pg_catalog.pg_type")
	tok := lex.NextToken()
	if tok.Type != TokenIdent || tok.Literal != "pg_catalog" {
		t.Fatalf("token[0] = %s %q, want IDENT pg_catalog", tok.Type, tok.Literal)
	}
	tok = lex.NextToken()
	if tok.Type != TokenDot || tok.Literal != "." {
		t.Fatalf("token[1] = %s %q, want DOT .", tok.Type, tok.Literal)
	}
	tok = lex.NextToken()
	if tok.Type != TokenIdent || tok.Literal != "pg_type" {
		t.Fatalf("token[2] = %s %q, want IDENT pg_type", tok.Type, tok.Literal)
	}
}

func TestParse_SelectSchemaQualified(t *testing.T) {
	stmt, err := Parse("SELECT * FROM information_schema.tables")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if sel.From.Schema != "information_schema" {
		t.Errorf("schema = %q, want information_schema", sel.From.Schema)
	}
	if sel.From.Name != "tables" {
		t.Errorf("name = %q, want tables", sel.From.Name)
	}
}

func TestParse_SelectPGCatalogQualified(t *testing.T) {
	stmt, err := Parse("SELECT * FROM pg_catalog.pg_type")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if sel.From.Schema != "pg_catalog" {
		t.Errorf("schema = %q, want pg_catalog", sel.From.Schema)
	}
	if sel.From.Name != "pg_type" {
		t.Errorf("name = %q, want pg_type", sel.From.Name)
	}
}

func TestParse_InsertSchemaQualified(t *testing.T) {
	stmt, err := Parse("INSERT INTO myschema.t (id) VALUES (1)")
	if err != nil {
		t.Fatal(err)
	}
	ins := stmt.(*InsertStmt)
	if ins.Table.Schema != "myschema" {
		t.Errorf("schema = %q, want myschema", ins.Table.Schema)
	}
	if ins.Table.Name != "t" {
		t.Errorf("name = %q, want t", ins.Table.Name)
	}
}

func TestTableRef_String(t *testing.T) {
	ref := TableRef{Schema: "information_schema", Name: "tables"}
	if ref.String() != "information_schema.tables" {
		t.Errorf("got %q, want information_schema.tables", ref.String())
	}
	ref2 := TableRef{Name: "users"}
	if ref2.String() != "users" {
		t.Errorf("got %q, want users", ref2.String())
	}
}

// ---------------------------------------------------------------------------
// ORDER BY
// ---------------------------------------------------------------------------

func TestParse_SelectOrderBySingle(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t ORDER BY name")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if len(sel.OrderBy) != 1 {
		t.Fatalf("orderby = %d, want 1", len(sel.OrderBy))
	}
	if sel.OrderBy[0].Column != "name" {
		t.Errorf("column = %q, want name", sel.OrderBy[0].Column)
	}
	if sel.OrderBy[0].Desc {
		t.Error("desc = true, want false (default ASC)")
	}
}

func TestParse_SelectOrderByDesc(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t ORDER BY name DESC")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if len(sel.OrderBy) != 1 {
		t.Fatalf("orderby = %d, want 1", len(sel.OrderBy))
	}
	if !sel.OrderBy[0].Desc {
		t.Error("desc = false, want true")
	}
}

func TestParse_SelectOrderByMultiColumn(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t ORDER BY name ASC, age DESC")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if len(sel.OrderBy) != 2 {
		t.Fatalf("orderby = %d, want 2", len(sel.OrderBy))
	}
	if sel.OrderBy[0].Column != "name" || sel.OrderBy[0].Desc {
		t.Errorf("orderby[0] = %+v, want {name, ASC}", sel.OrderBy[0])
	}
	if sel.OrderBy[1].Column != "age" || !sel.OrderBy[1].Desc {
		t.Errorf("orderby[1] = %+v, want {age, DESC}", sel.OrderBy[1])
	}
}

func TestParse_SelectOrderByWithLimit(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t ORDER BY name LIMIT 10")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if len(sel.OrderBy) != 1 {
		t.Fatalf("orderby = %d, want 1", len(sel.OrderBy))
	}
	if sel.Limit == nil || *sel.Limit != 10 {
		t.Errorf("limit = %v, want 10", sel.Limit)
	}
}

func TestParse_SelectOrderByWithLimitOffset(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t ORDER BY name LIMIT 10 OFFSET 5")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if len(sel.OrderBy) != 1 {
		t.Fatalf("orderby = %d, want 1", len(sel.OrderBy))
	}
	if sel.Limit == nil || *sel.Limit != 10 {
		t.Errorf("limit = %v, want 10", sel.Limit)
	}
	if sel.Offset == nil || *sel.Offset != 5 {
		t.Errorf("offset = %v, want 5", sel.Offset)
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func assertIntLit(t *testing.T, e Expr, want int64) {
	t.Helper()
	lit, ok := e.(*IntegerLit)
	if !ok {
		t.Fatalf("got %T, want *IntegerLit", e)
	}
	if lit.Value != want {
		t.Errorf("integer = %d, want %d", lit.Value, want)
	}
}

func assertStrLit(t *testing.T, e Expr, want string) {
	t.Helper()
	lit, ok := e.(*StringLit)
	if !ok {
		t.Fatalf("got %T, want *StringLit", e)
	}
	if lit.Value != want {
		t.Errorf("string = %q, want %q", lit.Value, want)
	}
}

func assertBoolLit(t *testing.T, e Expr, want bool) {
	t.Helper()
	lit, ok := e.(*BoolLit)
	if !ok {
		t.Fatalf("got %T, want *BoolLit", e)
	}
	if lit.Value != want {
		t.Errorf("bool = %v, want %v", lit.Value, want)
	}
}

func assertColumnRef(t *testing.T, e Expr, want string) {
	t.Helper()
	col, ok := e.(*ColumnRef)
	if !ok {
		t.Fatalf("got %T, want *ColumnRef", e)
	}
	if col.Name != want {
		t.Errorf("column = %q, want %q", col.Name, want)
	}
}

func assertQualifiedColumnRef(t *testing.T, e Expr, wantTable, wantName string) {
	t.Helper()
	col, ok := e.(*ColumnRef)
	if !ok {
		t.Fatalf("got %T, want *ColumnRef", e)
	}
	if col.Table != wantTable {
		t.Errorf("table = %q, want %q", col.Table, wantTable)
	}
	if col.Name != wantName {
		t.Errorf("column = %q, want %q", col.Name, wantName)
	}
}

// ---------------------------------------------------------------------------
// IS NULL / IS NOT NULL
// ---------------------------------------------------------------------------

func TestParse_IsNull(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t WHERE col IS NULL")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	isn, ok := sel.Where.(*IsNullExpr)
	if !ok {
		t.Fatalf("WHERE = %T, want *IsNullExpr", sel.Where)
	}
	assertColumnRef(t, isn.Expr, "col")
	if isn.Not {
		t.Error("Not = true, want false")
	}
}

func TestParse_IsNotNull(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t WHERE col IS NOT NULL")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	isn, ok := sel.Where.(*IsNullExpr)
	if !ok {
		t.Fatalf("WHERE = %T, want *IsNullExpr", sel.Where)
	}
	assertColumnRef(t, isn.Expr, "col")
	if !isn.Not {
		t.Error("Not = false, want true")
	}
}

// ---------------------------------------------------------------------------
// BEGIN / COMMIT / ROLLBACK
// ---------------------------------------------------------------------------

func TestParse_Begin(t *testing.T) {
	stmt, err := Parse("BEGIN")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := stmt.(*BeginStmt); !ok {
		t.Fatalf("got %T, want *BeginStmt", stmt)
	}
}

func TestParse_Commit(t *testing.T) {
	stmt, err := Parse("COMMIT")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := stmt.(*CommitStmt); !ok {
		t.Fatalf("got %T, want *CommitStmt", stmt)
	}
}

func TestParse_Rollback(t *testing.T) {
	stmt, err := Parse("ROLLBACK")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := stmt.(*RollbackStmt); !ok {
		t.Fatalf("got %T, want *RollbackStmt", stmt)
	}
}

func TestParse_BeginSemicolon(t *testing.T) {
	stmt, err := Parse("BEGIN;")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := stmt.(*BeginStmt); !ok {
		t.Fatalf("got %T, want *BeginStmt", stmt)
	}
}

// ---------------------------------------------------------------------------
// NOT operator
// ---------------------------------------------------------------------------

func TestParse_NotColumn(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t WHERE NOT active")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	not, ok := sel.Where.(*NotExpr)
	if !ok {
		t.Fatalf("WHERE = %T, want *NotExpr", sel.Where)
	}
	assertColumnRef(t, not.Expr, "active")
}

func TestParse_NotParenthesized(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t WHERE NOT (x > 5)")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	not, ok := sel.Where.(*NotExpr)
	if !ok {
		t.Fatalf("WHERE = %T, want *NotExpr", sel.Where)
	}
	bin, ok := not.Expr.(*BinaryExpr)
	if !ok {
		t.Fatalf("NOT inner = %T, want *BinaryExpr", not.Expr)
	}
	if bin.Op != ">" {
		t.Errorf("Op = %q, want >", bin.Op)
	}
}

func TestParse_NotWithAnd(t *testing.T) {
	// NOT binds tighter than AND: "NOT a AND b" → "(NOT a) AND b"
	stmt, err := Parse("SELECT * FROM t WHERE NOT active AND x = 1")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	bin, ok := sel.Where.(*BinaryExpr)
	if !ok {
		t.Fatalf("WHERE = %T, want *BinaryExpr (AND)", sel.Where)
	}
	if bin.Op != "AND" {
		t.Fatalf("Op = %q, want AND", bin.Op)
	}
	not, ok := bin.Left.(*NotExpr)
	if !ok {
		t.Fatalf("Left = %T, want *NotExpr", bin.Left)
	}
	assertColumnRef(t, not.Expr, "active")
}

func TestParse_DoubleNot(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t WHERE NOT NOT active")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	outer, ok := sel.Where.(*NotExpr)
	if !ok {
		t.Fatalf("WHERE = %T, want *NotExpr", sel.Where)
	}
	inner, ok := outer.Expr.(*NotExpr)
	if !ok {
		t.Fatalf("inner = %T, want *NotExpr", outer.Expr)
	}
	assertColumnRef(t, inner.Expr, "active")
}

func TestParse_IsNullWithAnd(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t WHERE a IS NULL AND b = 1")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	bin, ok := sel.Where.(*BinaryExpr)
	if !ok {
		t.Fatalf("WHERE = %T, want *BinaryExpr (AND)", sel.Where)
	}
	if bin.Op != "AND" {
		t.Fatalf("Op = %q, want AND", bin.Op)
	}
	isn, ok := bin.Left.(*IsNullExpr)
	if !ok {
		t.Fatalf("Left = %T, want *IsNullExpr", bin.Left)
	}
	assertColumnRef(t, isn.Expr, "a")
	if isn.Not {
		t.Error("Not = true, want false")
	}
}

// ---------------------------------------------------------------------------
// Comments (integration)
// ---------------------------------------------------------------------------

func TestParse_CommentIntegration(t *testing.T) {
	stmt, err := Parse("SELECT /* cols */ id -- comment\nFROM users")
	if err != nil {
		t.Fatal(err)
	}
	sel, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("got %T, want *SelectStmt", stmt)
	}
	if len(sel.Columns) != 1 {
		t.Fatalf("columns = %d, want 1", len(sel.Columns))
	}
	assertColumnRef(t, sel.Columns[0], "id")
	if sel.From.Name != "users" {
		t.Errorf("from = %q, want users", sel.From.Name)
	}
}

// ---------------------------------------------------------------------------
// Arithmetic tokens
// ---------------------------------------------------------------------------

func TestLexer_ArithmeticTokens(t *testing.T) {
	input := "+ - / %"
	want := []TokenType{TokenPlus, TokenMinus, TokenSlash, TokenPercent, TokenEOF}
	lex := NewLexer(input)
	for i, w := range want {
		tok := lex.NextToken()
		if tok.Type != w {
			t.Fatalf("token[%d]: type = %s, want %s", i, tok.Type, w)
		}
	}
}

// ---------------------------------------------------------------------------
// Arithmetic expressions
// ---------------------------------------------------------------------------

func TestParse_ArithmeticPrecedence(t *testing.T) {
	// 1 + 2 * 3 should parse as 1 + (2 * 3)
	stmt, err := Parse("SELECT 1 + 2 * 3")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	add := sel.Columns[0].(*BinaryExpr)
	if add.Op != "+" {
		t.Fatalf("top op = %q, want +", add.Op)
	}
	assertIntLit(t, add.Left, 1)
	mul := add.Right.(*BinaryExpr)
	if mul.Op != "*" {
		t.Fatalf("right op = %q, want *", mul.Op)
	}
	assertIntLit(t, mul.Left, 2)
	assertIntLit(t, mul.Right, 3)
}

func TestParse_UnaryMinus(t *testing.T) {
	stmt, err := Parse("SELECT -5")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	u, ok := sel.Columns[0].(*UnaryExpr)
	if !ok {
		t.Fatalf("got %T, want *UnaryExpr", sel.Columns[0])
	}
	if u.Op != "-" {
		t.Errorf("op = %q, want -", u.Op)
	}
	assertIntLit(t, u.Expr, 5)
}

func TestParse_UnaryMinusColumn(t *testing.T) {
	stmt, err := Parse("SELECT -col FROM t")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	u, ok := sel.Columns[0].(*UnaryExpr)
	if !ok {
		t.Fatalf("got %T, want *UnaryExpr", sel.Columns[0])
	}
	assertColumnRef(t, u.Expr, "col")
}

func TestParse_UnaryMinusParenthesized(t *testing.T) {
	stmt, err := Parse("SELECT -(1 + 2)")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	u, ok := sel.Columns[0].(*UnaryExpr)
	if !ok {
		t.Fatalf("got %T, want *UnaryExpr", sel.Columns[0])
	}
	bin := u.Expr.(*BinaryExpr)
	if bin.Op != "+" {
		t.Errorf("inner op = %q, want +", bin.Op)
	}
}

func TestParse_ArithmeticWithComparison(t *testing.T) {
	// a + 1 > b * 2
	stmt, err := Parse("SELECT * FROM t WHERE a + 1 > b * 2")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	gt := sel.Where.(*BinaryExpr)
	if gt.Op != ">" {
		t.Fatalf("top op = %q, want >", gt.Op)
	}
	add := gt.Left.(*BinaryExpr)
	if add.Op != "+" {
		t.Fatalf("left op = %q, want +", add.Op)
	}
	mul := gt.Right.(*BinaryExpr)
	if mul.Op != "*" {
		t.Fatalf("right op = %q, want *", mul.Op)
	}
}

func TestParse_ArithmeticLeftAssociative(t *testing.T) {
	// 1 - 2 - 3 should parse as (1 - 2) - 3
	stmt, err := Parse("SELECT 1 - 2 - 3")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	outer := sel.Columns[0].(*BinaryExpr)
	if outer.Op != "-" {
		t.Fatalf("top op = %q, want -", outer.Op)
	}
	assertIntLit(t, outer.Right, 3)
	inner := outer.Left.(*BinaryExpr)
	if inner.Op != "-" {
		t.Fatalf("left op = %q, want -", inner.Op)
	}
	assertIntLit(t, inner.Left, 1)
	assertIntLit(t, inner.Right, 2)
}

// ---------------------------------------------------------------------------
// JOIN
// ---------------------------------------------------------------------------

func TestParse_JoinBasic(t *testing.T) {
	stmt, err := Parse("SELECT a, b FROM t1 JOIN t2 ON t1.id = t2.t1_id")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if sel.From.Name != "t1" {
		t.Errorf("from = %q, want t1", sel.From.Name)
	}
	if len(sel.Joins) != 1 {
		t.Fatalf("joins = %d, want 1", len(sel.Joins))
	}
	j := sel.Joins[0]
	if j.Table.Name != "t2" {
		t.Errorf("join table = %q, want t2", j.Table.Name)
	}
	if j.Alias != "" {
		t.Errorf("join alias = %q, want empty", j.Alias)
	}
	// ON condition: t1.id = t2.t1_id
	bin := j.On.(*BinaryExpr)
	if bin.Op != "=" {
		t.Errorf("on op = %q, want =", bin.Op)
	}
	assertQualifiedColumnRef(t, bin.Left, "t1", "id")
	assertQualifiedColumnRef(t, bin.Right, "t2", "t1_id")
}

func TestParse_InnerJoinExplicit(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.t1_id")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if len(sel.Joins) != 1 {
		t.Fatalf("joins = %d, want 1", len(sel.Joins))
	}
	if sel.Joins[0].Table.Name != "t2" {
		t.Errorf("join table = %q, want t2", sel.Joins[0].Table.Name)
	}
}

func TestParse_JoinWithAliases(t *testing.T) {
	stmt, err := Parse("SELECT a.x, b.y FROM t1 a JOIN t2 b ON a.id = b.t1_id")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if sel.FromAlias != "a" {
		t.Errorf("from alias = %q, want a", sel.FromAlias)
	}
	if sel.Joins[0].Alias != "b" {
		t.Errorf("join alias = %q, want b", sel.Joins[0].Alias)
	}
	assertQualifiedColumnRef(t, sel.Columns[0], "a", "x")
	assertQualifiedColumnRef(t, sel.Columns[1], "b", "y")
}

func TestParse_JoinQualifiedWhere(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t1 JOIN t2 ON t1.id = t2.t1_id WHERE t1.x = 5")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if sel.Where == nil {
		t.Fatal("where is nil")
	}
	bin := sel.Where.(*BinaryExpr)
	assertQualifiedColumnRef(t, bin.Left, "t1", "x")
	assertIntLit(t, bin.Right, 5)
}

func TestParse_JoinQualifiedOrderBy(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t1 JOIN t2 ON t1.id = t2.t1_id ORDER BY t1.id DESC")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if len(sel.OrderBy) != 1 {
		t.Fatalf("orderby = %d, want 1", len(sel.OrderBy))
	}
	if sel.OrderBy[0].Table != "t1" {
		t.Errorf("orderby table = %q, want t1", sel.OrderBy[0].Table)
	}
	if sel.OrderBy[0].Column != "id" {
		t.Errorf("orderby column = %q, want id", sel.OrderBy[0].Column)
	}
	if !sel.OrderBy[0].Desc {
		t.Error("orderby desc = false, want true")
	}
}

func TestParse_MultiJoin(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t1 JOIN t2 ON t1.id = t2.t1_id JOIN t3 ON t2.id = t3.t2_id")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if len(sel.Joins) != 2 {
		t.Fatalf("joins = %d, want 2", len(sel.Joins))
	}
	if sel.Joins[0].Table.Name != "t2" {
		t.Errorf("join[0] table = %q, want t2", sel.Joins[0].Table.Name)
	}
	if sel.Joins[1].Table.Name != "t3" {
		t.Errorf("join[1] table = %q, want t3", sel.Joins[1].Table.Name)
	}
}

// ---------------------------------------------------------------------------
// Implicit cross-join (FROM t1, t2)
// ---------------------------------------------------------------------------

func TestParse_ImplicitCrossJoin(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t1 a, t2 b WHERE a.id = b.id")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if sel.From.Name != "t1" {
		t.Errorf("from = %q, want t1", sel.From.Name)
	}
	if sel.FromAlias != "a" {
		t.Errorf("from alias = %q, want a", sel.FromAlias)
	}
	if len(sel.Joins) != 1 {
		t.Fatalf("joins = %d, want 1", len(sel.Joins))
	}
	if sel.Joins[0].Table.Name != "t2" {
		t.Errorf("join table = %q, want t2", sel.Joins[0].Table.Name)
	}
	if sel.Joins[0].Alias != "b" {
		t.Errorf("join alias = %q, want b", sel.Joins[0].Alias)
	}
	if sel.Joins[0].On != nil {
		t.Error("cross-join On should be nil")
	}
	if sel.Where == nil {
		t.Fatal("where should not be nil")
	}
}

func TestParse_ImplicitCrossJoinSchemaQualified(t *testing.T) {
	stmt, err := Parse("SELECT tc.constraint_name FROM information_schema.table_constraints tc, information_schema.key_column_usage kc WHERE tc.table_name = kc.table_name")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if sel.From.Schema != "information_schema" || sel.From.Name != "table_constraints" {
		t.Errorf("from = %s.%s, want information_schema.table_constraints", sel.From.Schema, sel.From.Name)
	}
	if sel.FromAlias != "tc" {
		t.Errorf("from alias = %q, want tc", sel.FromAlias)
	}
	if len(sel.Joins) != 1 {
		t.Fatalf("joins = %d, want 1", len(sel.Joins))
	}
	if sel.Joins[0].Table.Schema != "information_schema" || sel.Joins[0].Table.Name != "key_column_usage" {
		t.Errorf("join table = %s.%s, want information_schema.key_column_usage", sel.Joins[0].Table.Schema, sel.Joins[0].Table.Name)
	}
	if sel.Joins[0].Alias != "kc" {
		t.Errorf("join alias = %q, want kc", sel.Joins[0].Alias)
	}
	if sel.Joins[0].On != nil {
		t.Error("cross-join On should be nil")
	}
}

func TestParse_ImplicitCrossJoinMultiple(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t1 a, t2 b, t3 c")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if sel.From.Name != "t1" {
		t.Errorf("from = %q, want t1", sel.From.Name)
	}
	if len(sel.Joins) != 2 {
		t.Fatalf("joins = %d, want 2", len(sel.Joins))
	}
	if sel.Joins[0].Table.Name != "t2" || sel.Joins[0].Alias != "b" {
		t.Errorf("join[0] = %s (%s), want t2 (b)", sel.Joins[0].Table.Name, sel.Joins[0].Alias)
	}
	if sel.Joins[1].Table.Name != "t3" || sel.Joins[1].Alias != "c" {
		t.Errorf("join[1] = %s (%s), want t3 (c)", sel.Joins[1].Table.Name, sel.Joins[1].Alias)
	}
}

// ---------------------------------------------------------------------------
// Concatenation operator ||
// ---------------------------------------------------------------------------

func TestParse_ConcatOperator(t *testing.T) {
	stmt, err := Parse("SELECT 'a' || 'b'")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if len(sel.Columns) != 1 {
		t.Fatalf("columns = %d, want 1", len(sel.Columns))
	}
	bin, ok := sel.Columns[0].(*BinaryExpr)
	if !ok {
		t.Fatalf("got %T, want *BinaryExpr", sel.Columns[0])
	}
	if bin.Op != "||" {
		t.Errorf("op = %q, want ||", bin.Op)
	}
}

func TestParse_ConcatChained(t *testing.T) {
	stmt, err := Parse("SELECT 'a' || 'b' || 'c'")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	// Should be left-associative: (('a' || 'b') || 'c')
	outer, ok := sel.Columns[0].(*BinaryExpr)
	if !ok {
		t.Fatalf("got %T, want *BinaryExpr", sel.Columns[0])
	}
	if outer.Op != "||" {
		t.Errorf("outer op = %q, want ||", outer.Op)
	}
	inner, ok := outer.Left.(*BinaryExpr)
	if !ok {
		t.Fatalf("left = %T, want *BinaryExpr", outer.Left)
	}
	if inner.Op != "||" {
		t.Errorf("inner op = %q, want ||", inner.Op)
	}
}

// ---------------------------------------------------------------------------
// Multi-arg function calls
// ---------------------------------------------------------------------------

func TestParse_MultiArgFunction(t *testing.T) {
	stmt, err := Parse("SELECT CONCAT('a', 'b', 'c')")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	fn, ok := sel.Columns[0].(*FunctionCallExpr)
	if !ok {
		t.Fatalf("got %T, want *FunctionCallExpr", sel.Columns[0])
	}
	if fn.Name != "CONCAT" {
		t.Errorf("name = %q, want CONCAT", fn.Name)
	}
	if len(fn.Args) != 3 {
		t.Fatalf("args = %d, want 3", len(fn.Args))
	}
}

// ---------------------------------------------------------------------------
// LIKE / ILIKE predicate
// ---------------------------------------------------------------------------

func TestParse_Like(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t WHERE name LIKE '%foo%'")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	like, ok := sel.Where.(*LikeExpr)
	if !ok {
		t.Fatalf("WHERE = %T, want *LikeExpr", sel.Where)
	}
	assertColumnRef(t, like.Expr, "name")
	lit, ok := like.Pattern.(*StringLit)
	if !ok {
		t.Fatalf("Pattern = %T, want *StringLit", like.Pattern)
	}
	if lit.Value != "%foo%" {
		t.Errorf("pattern = %q, want %%foo%%", lit.Value)
	}
	if like.Not {
		t.Error("Not = true, want false")
	}
	if like.CaseInsensitive {
		t.Error("CaseInsensitive = true, want false")
	}
	if like.Escape != nil {
		t.Error("Escape should be nil")
	}
}

func TestParse_NotLike(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t WHERE name NOT LIKE 'x%'")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	like, ok := sel.Where.(*LikeExpr)
	if !ok {
		t.Fatalf("WHERE = %T, want *LikeExpr", sel.Where)
	}
	if !like.Not {
		t.Error("Not = false, want true")
	}
	if like.CaseInsensitive {
		t.Error("CaseInsensitive = true, want false")
	}
}

func TestParse_Ilike(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t WHERE name ILIKE '%foo%'")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	like, ok := sel.Where.(*LikeExpr)
	if !ok {
		t.Fatalf("WHERE = %T, want *LikeExpr", sel.Where)
	}
	if like.Not {
		t.Error("Not = true, want false")
	}
	if !like.CaseInsensitive {
		t.Error("CaseInsensitive = false, want true")
	}
}

func TestParse_NotIlike(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t WHERE name NOT ILIKE '%foo%'")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	like, ok := sel.Where.(*LikeExpr)
	if !ok {
		t.Fatalf("WHERE = %T, want *LikeExpr", sel.Where)
	}
	if !like.Not {
		t.Error("Not = false, want true")
	}
	if !like.CaseInsensitive {
		t.Error("CaseInsensitive = false, want true")
	}
}

func TestParse_LikeEscape(t *testing.T) {
	stmt, err := Parse(`SELECT * FROM t WHERE name LIKE '100\%' ESCAPE '\'`)
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	like, ok := sel.Where.(*LikeExpr)
	if !ok {
		t.Fatalf("WHERE = %T, want *LikeExpr", sel.Where)
	}
	if like.Escape == nil {
		t.Fatal("Escape should not be nil")
	}
	esc, ok := like.Escape.(*StringLit)
	if !ok {
		t.Fatalf("Escape = %T, want *StringLit", like.Escape)
	}
	if esc.Value != `\` {
		t.Errorf("escape = %q, want %q", esc.Value, `\`)
	}
}

func TestParse_LikeWithColumnRef(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t WHERE a LIKE b")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	like, ok := sel.Where.(*LikeExpr)
	if !ok {
		t.Fatalf("WHERE = %T, want *LikeExpr", sel.Where)
	}
	assertColumnRef(t, like.Expr, "a")
	assertColumnRef(t, like.Pattern, "b")
}

func TestParse_LikeWithAnd(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t WHERE a LIKE 'x%' AND b = 1")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	bin, ok := sel.Where.(*BinaryExpr)
	if !ok {
		t.Fatalf("WHERE = %T, want *BinaryExpr (AND)", sel.Where)
	}
	if bin.Op != "AND" {
		t.Fatalf("op = %q, want AND", bin.Op)
	}
	_, ok = bin.Left.(*LikeExpr)
	if !ok {
		t.Fatalf("left = %T, want *LikeExpr", bin.Left)
	}
}

func TestParse_NotWithLikeDoesNotConflict(t *testing.T) {
	// "NOT col LIKE pattern" should parse as NOT (col LIKE pattern)
	stmt, err := Parse("SELECT * FROM t WHERE NOT name LIKE '%x'")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	not, ok := sel.Where.(*NotExpr)
	if !ok {
		t.Fatalf("WHERE = %T, want *NotExpr", sel.Where)
	}
	like, ok := not.Expr.(*LikeExpr)
	if !ok {
		t.Fatalf("inner = %T, want *LikeExpr", not.Expr)
	}
	if like.Not {
		t.Error("LikeExpr.Not = true, want false (NOT is outer)")
	}
}

// ---------------------------------------------------------------------------
// ALTER TABLE tests
// ---------------------------------------------------------------------------

func TestParse_AlterTableAddColumn(t *testing.T) {
	stmt, err := Parse("ALTER TABLE t ADD COLUMN c INTEGER")
	if err != nil {
		t.Fatal(err)
	}
	alt, ok := stmt.(*AlterTableAddColumnStmt)
	if !ok {
		t.Fatalf("got %T, want *AlterTableAddColumnStmt", stmt)
	}
	if alt.Table.Name != "t" {
		t.Errorf("table = %q, want %q", alt.Table.Name, "t")
	}
	if alt.Column.Name != "c" {
		t.Errorf("column = %q, want %q", alt.Column.Name, "c")
	}
	if alt.Column.DataType != "INTEGER" {
		t.Errorf("type = %q, want INTEGER", alt.Column.DataType)
	}
}

func TestParse_AlterTableAddColumnNoKeyword(t *testing.T) {
	stmt, err := Parse("ALTER TABLE t ADD c TEXT")
	if err != nil {
		t.Fatal(err)
	}
	alt := stmt.(*AlterTableAddColumnStmt)
	if alt.Column.Name != "c" || alt.Column.DataType != "TEXT" {
		t.Errorf("got %q %q, want c TEXT", alt.Column.Name, alt.Column.DataType)
	}
}

func TestParse_AlterTableDropColumn(t *testing.T) {
	stmt, err := Parse("ALTER TABLE t DROP COLUMN c")
	if err != nil {
		t.Fatal(err)
	}
	alt, ok := stmt.(*AlterTableDropColumnStmt)
	if !ok {
		t.Fatalf("got %T, want *AlterTableDropColumnStmt", stmt)
	}
	if alt.Table.Name != "t" {
		t.Errorf("table = %q, want %q", alt.Table.Name, "t")
	}
	if alt.Column != "c" {
		t.Errorf("column = %q, want %q", alt.Column, "c")
	}
}

func TestParse_AlterTableDropColumnNoKeyword(t *testing.T) {
	stmt, err := Parse("ALTER TABLE t DROP c")
	if err != nil {
		t.Fatal(err)
	}
	alt := stmt.(*AlterTableDropColumnStmt)
	if alt.Column != "c" {
		t.Errorf("got %q, want c", alt.Column)
	}
}

func TestParse_AlterTableAddPrimaryKeyError(t *testing.T) {
	_, err := Parse("ALTER TABLE t ADD c INTEGER PRIMARY KEY")
	if err == nil {
		t.Fatal("expected error for ADD ... PRIMARY KEY")
	}
}

// ---------------------------------------------------------------------------
// IN / NOT IN tests
// ---------------------------------------------------------------------------

func TestParse_In(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t WHERE id IN (1, 2, 3)")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	in, ok := sel.Where.(*InExpr)
	if !ok {
		t.Fatalf("WHERE = %T, want *InExpr", sel.Where)
	}
	assertColumnRef(t, in.Expr, "id")
	if in.Not {
		t.Error("Not = true, want false")
	}
	if len(in.Values) != 3 {
		t.Fatalf("len(Values) = %d, want 3", len(in.Values))
	}
}

func TestParse_NotIn(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t WHERE id NOT IN (4, 5)")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	in, ok := sel.Where.(*InExpr)
	if !ok {
		t.Fatalf("WHERE = %T, want *InExpr", sel.Where)
	}
	if !in.Not {
		t.Error("Not = false, want true")
	}
	if len(in.Values) != 2 {
		t.Fatalf("len(Values) = %d, want 2", len(in.Values))
	}
}

func TestParse_InSingleValue(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t WHERE x IN (42)")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	in, ok := sel.Where.(*InExpr)
	if !ok {
		t.Fatalf("WHERE = %T, want *InExpr", sel.Where)
	}
	if len(in.Values) != 1 {
		t.Fatalf("len(Values) = %d, want 1", len(in.Values))
	}
}

func TestParse_InWithExpressions(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t WHERE x IN (1 + 2, y)")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	in := sel.Where.(*InExpr)
	if _, ok := in.Values[0].(*BinaryExpr); !ok {
		t.Fatalf("Values[0] = %T, want *BinaryExpr", in.Values[0])
	}
	if _, ok := in.Values[1].(*ColumnRef); !ok {
		t.Fatalf("Values[1] = %T, want *ColumnRef", in.Values[1])
	}
}

func TestParse_InWithNull(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t WHERE x IN (1, NULL)")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	in := sel.Where.(*InExpr)
	if len(in.Values) != 2 {
		t.Fatalf("len(Values) = %d, want 2", len(in.Values))
	}
	if _, ok := in.Values[1].(*NullLit); !ok {
		t.Fatalf("Values[1] = %T, want *NullLit", in.Values[1])
	}
}

func TestParse_InCombinedWithAnd(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t WHERE x IN (1, 2) AND y = 3")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	bin, ok := sel.Where.(*BinaryExpr)
	if !ok || bin.Op != "AND" {
		t.Fatalf("WHERE = %T (op=%v), want AND", sel.Where, bin)
	}
	if _, ok := bin.Left.(*InExpr); !ok {
		t.Fatalf("left = %T, want *InExpr", bin.Left)
	}
}

func TestParse_NotExprWithIn(t *testing.T) {
	// "NOT x IN (...)" should parse as NOT (x IN (...))
	stmt, err := Parse("SELECT * FROM t WHERE NOT x IN (1, 2)")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	not, ok := sel.Where.(*NotExpr)
	if !ok {
		t.Fatalf("WHERE = %T, want *NotExpr", sel.Where)
	}
	in, ok := not.Expr.(*InExpr)
	if !ok {
		t.Fatalf("inner = %T, want *InExpr", not.Expr)
	}
	if in.Not {
		t.Error("InExpr.Not = true, want false (NOT is outer)")
	}
}

// ---------------------------------------------------------------------------
// INDEXED BY
// ---------------------------------------------------------------------------

func TestParse_SelectIndexedBy(t *testing.T) {
	stmt, err := Parse("SELECT * FROM users INDEXED BY idx_email WHERE email = 'a@b.com'")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if sel.IndexedBy != "idx_email" {
		t.Errorf("IndexedBy = %q, want idx_email", sel.IndexedBy)
	}
}

func TestParse_SelectIndexedByWithAlias(t *testing.T) {
	stmt, err := Parse("SELECT * FROM users u INDEXED BY idx_email WHERE email = 'a@b.com'")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if sel.FromAlias != "u" {
		t.Errorf("FromAlias = %q, want u", sel.FromAlias)
	}
	if sel.IndexedBy != "idx_email" {
		t.Errorf("IndexedBy = %q, want idx_email", sel.IndexedBy)
	}
}

func TestParse_SelectWithoutIndexedBy(t *testing.T) {
	stmt, err := Parse("SELECT * FROM users WHERE email = 'a@b.com'")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if sel.IndexedBy != "" {
		t.Errorf("IndexedBy = %q, want empty", sel.IndexedBy)
	}
}

func TestParse_UpdateIndexedBy(t *testing.T) {
	stmt, err := Parse("UPDATE users INDEXED BY idx_email SET name = 'x' WHERE email = 'a@b.com'")
	if err != nil {
		t.Fatal(err)
	}
	upd := stmt.(*UpdateStmt)
	if upd.IndexedBy != "idx_email" {
		t.Errorf("IndexedBy = %q, want idx_email", upd.IndexedBy)
	}
}

func TestParse_DeleteIndexedBy(t *testing.T) {
	stmt, err := Parse("DELETE FROM users INDEXED BY idx_email WHERE email = 'a@b.com'")
	if err != nil {
		t.Fatal(err)
	}
	del := stmt.(*DeleteStmt)
	if del.IndexedBy != "idx_email" {
		t.Errorf("IndexedBy = %q, want idx_email", del.IndexedBy)
	}
}
