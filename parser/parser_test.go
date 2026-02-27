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
		{"id", "INTEGER"},
		{"name", "TEXT"},
		{"active", "BOOLEAN"},
	}
	for i, want := range wantCols {
		got := ct.Columns[i]
		if got != want {
			t.Errorf("column[%d] = %+v, want %+v", i, got, want)
		}
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
