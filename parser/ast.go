package parser

// Statement is the interface implemented by all SQL statement AST nodes.
// The unexported marker method restricts implementations to this package.
type Statement interface {
	statementNode()
}

// Expr is the interface implemented by all expression AST nodes.
type Expr interface {
	exprNode()
}

// ---------------------------------------------------------------------------
// Table references
// ---------------------------------------------------------------------------

// TableRef is a possibly schema-qualified table name (e.g. "information_schema.tables").
type TableRef struct {
	Schema string // "" when unqualified
	Name   string
}

// String returns "schema.name" for qualified refs, or just "name".
func (r TableRef) String() string {
	if r.Schema != "" {
		return r.Schema + "." + r.Name
	}
	return r.Name
}

// IsEmpty reports whether the table ref has no name set (e.g. SELECT without FROM).
func (r TableRef) IsEmpty() bool {
	return r.Name == ""
}

// ---------------------------------------------------------------------------
// Column / table definitions
// ---------------------------------------------------------------------------

// ColumnDef describes a column in a CREATE TABLE statement.
type ColumnDef struct {
	Name       string
	DataType   string // "INTEGER", "TEXT", or "BOOLEAN"
	PrimaryKey bool
}

// SetClause represents a single col = expr assignment in UPDATE ... SET.
type SetClause struct {
	Column string
	Value  Expr
}

// ---------------------------------------------------------------------------
// Statements
// ---------------------------------------------------------------------------

// CreateTableStmt: CREATE TABLE <name> (<col> <type>, ...)
type CreateTableStmt struct {
	Name    TableRef
	Columns []ColumnDef
}

// DropTableStmt: DROP TABLE <name>
type DropTableStmt struct {
	Name TableRef
}

// InsertStmt: INSERT INTO <table> [(<cols>)] VALUES (<exprs>), ...
type InsertStmt struct {
	Table   TableRef
	Columns []string // nil when omitted
	Values  [][]Expr
}

// OrderByClause represents a single column in an ORDER BY clause.
type OrderByClause struct {
	Column string // column name
	Desc   bool   // true = DESC, false = ASC (default)
}

// SelectStmt: SELECT <cols> FROM <table> [WHERE <expr>] [ORDER BY ...] [LIMIT n] [OFFSET n]
type SelectStmt struct {
	Columns []Expr // StarExpr for *, ColumnRef for named columns
	From    TableRef
	Where   Expr             // nil when no WHERE clause
	OrderBy []OrderByClause  // nil when no ORDER BY clause
	Limit   *int64           // nil = no limit
	Offset  *int64           // nil = no offset
}

// UpdateStmt: UPDATE <table> SET <sets> [WHERE <expr>]
type UpdateStmt struct {
	Table TableRef
	Sets  []SetClause
	Where Expr // nil when no WHERE clause
}

// DeleteStmt: DELETE FROM <table> [WHERE <expr>]
type DeleteStmt struct {
	Table TableRef
	Where Expr // nil when no WHERE clause
}

// BeginStmt: BEGIN (no-op transaction start)
type BeginStmt struct{}

// CommitStmt: COMMIT (no-op transaction commit)
type CommitStmt struct{}

// RollbackStmt: ROLLBACK (no-op transaction rollback)
type RollbackStmt struct{}

func (*CreateTableStmt) statementNode() {}
func (*DropTableStmt) statementNode()   {}
func (*InsertStmt) statementNode()      {}
func (*SelectStmt) statementNode()      {}
func (*UpdateStmt) statementNode()      {}
func (*DeleteStmt) statementNode()      {}
func (*BeginStmt) statementNode()       {}
func (*CommitStmt) statementNode()      {}
func (*RollbackStmt) statementNode()    {}

// ---------------------------------------------------------------------------
// Expressions
// ---------------------------------------------------------------------------

// ColumnRef references a column by name.
type ColumnRef struct {
	Name string
}

// StarExpr represents * in a SELECT column list.
type StarExpr struct{}

// IntegerLit is an integer literal.
type IntegerLit struct {
	Value int64
}

// StringLit is a single-quoted string literal.
type StringLit struct {
	Value string
}

// BoolLit is TRUE or FALSE.
type BoolLit struct {
	Value bool
}

// NullLit represents the NULL literal.
type NullLit struct{}

// UnaryExpr is a unary operation (e.g. -expr).
type UnaryExpr struct {
	Op   string // "-"
	Expr Expr
}

// BinaryExpr is a binary operation: left op right.
// Op is one of: "=", "!=", "<", ">", "<=", ">=", "AND", "OR", "+", "-", "*", "/", "%".
type BinaryExpr struct {
	Left  Expr
	Op    string
	Right Expr
}

// FunctionCallExpr represents a function call such as SUM(col) or COUNT(*).
type FunctionCallExpr struct {
	Name string // uppercased: "SUM", "COUNT", "MIN", "MAX"
	Args []Expr // COUNT(*) → []*StarExpr; column aggs → []*ColumnRef
}

// AliasExpr wraps an expression with a column alias (e.g. COUNT(*) AS total).
type AliasExpr struct {
	Expr  Expr
	Alias string
}

// IsNullExpr represents IS NULL or IS NOT NULL.
type IsNullExpr struct {
	Expr Expr
	Not  bool // true for IS NOT NULL
}

// NotExpr represents NOT <expr>.
type NotExpr struct {
	Expr Expr
}

func (*ColumnRef) exprNode()         {}
func (*StarExpr) exprNode()          {}
func (*IntegerLit) exprNode()        {}
func (*StringLit) exprNode()         {}
func (*BoolLit) exprNode()           {}
func (*NullLit) exprNode()           {}
func (*UnaryExpr) exprNode()         {}
func (*BinaryExpr) exprNode()        {}
func (*FunctionCallExpr) exprNode()  {}
func (*AliasExpr) exprNode()         {}
func (*IsNullExpr) exprNode()        {}
func (*NotExpr) exprNode()           {}
