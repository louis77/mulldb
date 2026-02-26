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
// Column / table definitions
// ---------------------------------------------------------------------------

// ColumnDef describes a column in a CREATE TABLE statement.
type ColumnDef struct {
	Name     string
	DataType string // "INTEGER", "TEXT", or "BOOLEAN"
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
	Name    string
	Columns []ColumnDef
}

// DropTableStmt: DROP TABLE <name>
type DropTableStmt struct {
	Name string
}

// InsertStmt: INSERT INTO <table> [(<cols>)] VALUES (<exprs>), ...
type InsertStmt struct {
	Table   string
	Columns []string // nil when omitted
	Values  [][]Expr
}

// SelectStmt: SELECT <cols> FROM <table> [WHERE <expr>]
type SelectStmt struct {
	Columns []Expr // StarExpr for *, ColumnRef for named columns
	From    string
	Where   Expr // nil when no WHERE clause
}

// UpdateStmt: UPDATE <table> SET <sets> [WHERE <expr>]
type UpdateStmt struct {
	Table string
	Sets  []SetClause
	Where Expr // nil when no WHERE clause
}

// DeleteStmt: DELETE FROM <table> [WHERE <expr>]
type DeleteStmt struct {
	Table string
	Where Expr // nil when no WHERE clause
}

func (*CreateTableStmt) statementNode() {}
func (*DropTableStmt) statementNode()   {}
func (*InsertStmt) statementNode()      {}
func (*SelectStmt) statementNode()      {}
func (*UpdateStmt) statementNode()      {}
func (*DeleteStmt) statementNode()      {}

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

// BinaryExpr is a binary operation: left op right.
// Op is one of: "=", "!=", "<", ">", "<=", ">=", "AND", "OR".
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

func (*ColumnRef) exprNode()         {}
func (*StarExpr) exprNode()          {}
func (*IntegerLit) exprNode()        {}
func (*StringLit) exprNode()         {}
func (*BoolLit) exprNode()           {}
func (*NullLit) exprNode()           {}
func (*BinaryExpr) exprNode()        {}
func (*FunctionCallExpr) exprNode()  {}
