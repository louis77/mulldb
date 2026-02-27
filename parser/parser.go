package parser

import (
	"fmt"
	"strconv"
	"strings"
)

// parser is the internal recursive-descent parser. Use the exported Parse
// function as the public entry point.
type parser struct {
	lexer *Lexer
	cur   Token
}

// Parse parses a single SQL statement from input.
func Parse(input string) (Statement, error) {
	p := &parser{lexer: NewLexer(input)}
	p.next()

	stmt, err := p.parseStatement()
	if err != nil {
		return nil, err
	}

	// Allow an optional trailing semicolon.
	if p.cur.Type == TokenSemicolon {
		p.next()
	}
	if p.cur.Type != TokenEOF {
		return nil, fmt.Errorf("unexpected %q after statement at position %d",
			p.cur.Literal, p.cur.Pos)
	}
	return stmt, nil
}

// -------------------------------------------------------------------------
// Helpers
// -------------------------------------------------------------------------

func (p *parser) next() {
	p.cur = p.lexer.NextToken()
}

func (p *parser) expect(t TokenType) (Token, error) {
	tok := p.cur
	if tok.Type != t {
		return tok, fmt.Errorf("expected %s, got %q at position %d",
			t, tok.Literal, tok.Pos)
	}
	p.next()
	return tok, nil
}

func (p *parser) unexpected() error {
	if p.cur.Type == TokenEOF {
		return fmt.Errorf("unexpected end of input")
	}
	return fmt.Errorf("unexpected %q at position %d", p.cur.Literal, p.cur.Pos)
}

// -------------------------------------------------------------------------
// Statement parsing
// -------------------------------------------------------------------------

func (p *parser) parseStatement() (Statement, error) {
	switch p.cur.Type {
	case TokenCreate:
		return p.parseCreateTable()
	case TokenDrop:
		return p.parseDropTable()
	case TokenInsert:
		return p.parseInsert()
	case TokenSelect:
		return p.parseSelect()
	case TokenUpdate:
		return p.parseUpdate()
	case TokenDelete:
		return p.parseDelete()
	default:
		return nil, p.unexpected()
	}
}

func (p *parser) parseTableRef() (TableRef, error) {
	name, err := p.expect(TokenIdent)
	if err != nil {
		return TableRef{}, err
	}
	if p.cur.Type == TokenDot {
		p.next() // skip dot
		second, err := p.expect(TokenIdent)
		if err != nil {
			return TableRef{}, err
		}
		return TableRef{Schema: name.Literal, Name: second.Literal}, nil
	}
	return TableRef{Name: name.Literal}, nil
}

func (p *parser) parseCreateTable() (*CreateTableStmt, error) {
	p.next() // skip CREATE
	if _, err := p.expect(TokenTable); err != nil {
		return nil, err
	}
	ref, err := p.parseTableRef()
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(TokenLParen); err != nil {
		return nil, err
	}

	var columns []ColumnDef
	for {
		col, err := p.parseColumnDef()
		if err != nil {
			return nil, err
		}
		columns = append(columns, col)
		if p.cur.Type != TokenComma {
			break
		}
		p.next() // skip comma
	}

	if _, err := p.expect(TokenRParen); err != nil {
		return nil, err
	}

	// Validate at most one column is marked PRIMARY KEY.
	pkCount := 0
	for _, col := range columns {
		if col.PrimaryKey {
			pkCount++
		}
	}
	if pkCount > 1 {
		return nil, fmt.Errorf("multiple primary keys are not allowed")
	}

	return &CreateTableStmt{Name: ref, Columns: columns}, nil
}

func (p *parser) parseColumnDef() (ColumnDef, error) {
	name, err := p.expect(TokenIdent)
	if err != nil {
		return ColumnDef{}, err
	}

	var dataType string
	switch p.cur.Type {
	case TokenIntegerKW:
		dataType = "INTEGER"
	case TokenTextKW:
		dataType = "TEXT"
	case TokenBooleanKW:
		dataType = "BOOLEAN"
	default:
		return ColumnDef{}, fmt.Errorf("expected data type, got %q at position %d",
			p.cur.Literal, p.cur.Pos)
	}
	p.next()

	// Optional PRIMARY KEY constraint.
	var pk bool
	if p.cur.Type == TokenPrimary {
		p.next()
		if _, err := p.expect(TokenKey); err != nil {
			return ColumnDef{}, err
		}
		pk = true
	}

	return ColumnDef{Name: name.Literal, DataType: dataType, PrimaryKey: pk}, nil
}

func (p *parser) parseDropTable() (*DropTableStmt, error) {
	p.next() // skip DROP
	if _, err := p.expect(TokenTable); err != nil {
		return nil, err
	}
	ref, err := p.parseTableRef()
	if err != nil {
		return nil, err
	}
	return &DropTableStmt{Name: ref}, nil
}

func (p *parser) parseInsert() (*InsertStmt, error) {
	p.next() // skip INSERT
	if _, err := p.expect(TokenInto); err != nil {
		return nil, err
	}
	ref, err := p.parseTableRef()
	if err != nil {
		return nil, err
	}

	// Optional column list.
	var columns []string
	if p.cur.Type == TokenLParen {
		p.next()
		for {
			col, err := p.expect(TokenIdent)
			if err != nil {
				return nil, err
			}
			columns = append(columns, col.Literal)
			if p.cur.Type != TokenComma {
				break
			}
			p.next()
		}
		if _, err := p.expect(TokenRParen); err != nil {
			return nil, err
		}
	}

	if _, err := p.expect(TokenValues); err != nil {
		return nil, err
	}

	var values [][]Expr
	for {
		row, err := p.parseValueRow()
		if err != nil {
			return nil, err
		}
		values = append(values, row)
		if p.cur.Type != TokenComma {
			break
		}
		p.next()
	}

	return &InsertStmt{Table: ref, Columns: columns, Values: values}, nil
}

func (p *parser) parseValueRow() ([]Expr, error) {
	if _, err := p.expect(TokenLParen); err != nil {
		return nil, err
	}
	var exprs []Expr
	for {
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, expr)
		if p.cur.Type != TokenComma {
			break
		}
		p.next()
	}
	if _, err := p.expect(TokenRParen); err != nil {
		return nil, err
	}
	return exprs, nil
}

func (p *parser) parseSelect() (*SelectStmt, error) {
	p.next() // skip SELECT

	var columns []Expr
	for {
		if p.cur.Type == TokenStar {
			columns = append(columns, &StarExpr{})
			p.next()
		} else {
			expr, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			if p.cur.Type == TokenAs {
				p.next() // consume AS
				alias, err := p.expect(TokenIdent)
				if err != nil {
					return nil, err
				}
				expr = &AliasExpr{Expr: expr, Alias: alias.Literal}
			}
			columns = append(columns, expr)
		}
		if p.cur.Type != TokenComma {
			break
		}
		p.next()
	}

	var from TableRef
	var err error
	if p.cur.Type == TokenFrom {
		p.next() // consume FROM
		from, err = p.parseTableRef()
		if err != nil {
			return nil, err
		}
	}

	var where Expr
	if p.cur.Type == TokenWhere {
		p.next()
		where, err = p.parseExpr()
		if err != nil {
			return nil, err
		}
	}

	// Parse optional ORDER BY col [ASC|DESC] [, col [ASC|DESC], ...]
	var orderBy []OrderByClause
	if p.cur.Type == TokenOrder {
		p.next() // consume ORDER
		if _, err := p.expect(TokenBy); err != nil {
			return nil, err
		}
		for {
			col, err := p.expect(TokenIdent)
			if err != nil {
				return nil, err
			}
			clause := OrderByClause{Column: col.Literal}
			if p.cur.Type == TokenDesc {
				clause.Desc = true
				p.next()
			} else if p.cur.Type == TokenAsc {
				p.next()
			}
			orderBy = append(orderBy, clause)
			if p.cur.Type != TokenComma {
				break
			}
			p.next() // consume comma
		}
	}

	// Parse optional LIMIT and OFFSET (in either order).
	var limit, offset *int64
	for i := 0; i < 2; i++ {
		if p.cur.Type == TokenLimit && limit == nil {
			p.next()
			tok, err := p.expect(TokenIntLit)
			if err != nil {
				return nil, err
			}
			v, err := strconv.ParseInt(tok.Literal, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid LIMIT value %q: %w", tok.Literal, err)
			}
			limit = &v
		} else if p.cur.Type == TokenOffset && offset == nil {
			p.next()
			tok, err := p.expect(TokenIntLit)
			if err != nil {
				return nil, err
			}
			v, err := strconv.ParseInt(tok.Literal, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid OFFSET value %q: %w", tok.Literal, err)
			}
			offset = &v
		} else {
			break
		}
	}

	return &SelectStmt{Columns: columns, From: from, Where: where, OrderBy: orderBy, Limit: limit, Offset: offset}, nil
}

func (p *parser) parseUpdate() (*UpdateStmt, error) {
	p.next() // skip UPDATE
	ref, err := p.parseTableRef()
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(TokenSet); err != nil {
		return nil, err
	}

	var sets []SetClause
	for {
		col, err := p.expect(TokenIdent)
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(TokenEq); err != nil {
			return nil, err
		}
		val, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		sets = append(sets, SetClause{Column: col.Literal, Value: val})
		if p.cur.Type != TokenComma {
			break
		}
		p.next()
	}

	var where Expr
	if p.cur.Type == TokenWhere {
		p.next()
		where, err = p.parseExpr()
		if err != nil {
			return nil, err
		}
	}

	return &UpdateStmt{Table: ref, Sets: sets, Where: where}, nil
}

func (p *parser) parseDelete() (*DeleteStmt, error) {
	p.next() // skip DELETE
	if _, err := p.expect(TokenFrom); err != nil {
		return nil, err
	}
	ref, err := p.parseTableRef()
	if err != nil {
		return nil, err
	}

	var where Expr
	if p.cur.Type == TokenWhere {
		p.next()
		where, err = p.parseExpr()
		if err != nil {
			return nil, err
		}
	}

	return &DeleteStmt{Table: ref, Where: where}, nil
}

// -------------------------------------------------------------------------
// Expression parsing (precedence: OR < AND < comparison < primary)
// -------------------------------------------------------------------------

func (p *parser) parseExpr() (Expr, error) {
	return p.parseOr()
}

func (p *parser) parseOr() (Expr, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.cur.Type == TokenOr {
		p.next()
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Op: "OR", Right: right}
	}
	return left, nil
}

func (p *parser) parseAnd() (Expr, error) {
	left, err := p.parseComparison()
	if err != nil {
		return nil, err
	}
	for p.cur.Type == TokenAnd {
		p.next()
		right, err := p.parseComparison()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Op: "AND", Right: right}
	}
	return left, nil
}

func (p *parser) parseComparison() (Expr, error) {
	left, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}

	if p.cur.Type == TokenIs {
		p.next()
		not := false
		if p.cur.Type == TokenNot {
			not = true
			p.next()
		}
		if _, err := p.expect(TokenNull); err != nil {
			return nil, err
		}
		return &IsNullExpr{Expr: left, Not: not}, nil
	}

	var op string
	switch p.cur.Type {
	case TokenEq:
		op = "="
	case TokenNotEq:
		op = "!="
	case TokenLt:
		op = "<"
	case TokenGt:
		op = ">"
	case TokenLtEq:
		op = "<="
	case TokenGtEq:
		op = ">="
	default:
		return left, nil
	}

	p.next()
	right, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}
	return &BinaryExpr{Left: left, Op: op, Right: right}, nil
}

func (p *parser) parsePrimary() (Expr, error) {
	switch p.cur.Type {
	case TokenIntLit:
		val, err := strconv.ParseInt(p.cur.Literal, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid integer %q: %w", p.cur.Literal, err)
		}
		p.next()
		return &IntegerLit{Value: val}, nil
	case TokenStrLit:
		val := p.cur.Literal
		p.next()
		return &StringLit{Value: val}, nil
	case TokenTrue:
		p.next()
		return &BoolLit{Value: true}, nil
	case TokenFalse:
		p.next()
		return &BoolLit{Value: false}, nil
	case TokenNull:
		p.next()
		return &NullLit{}, nil
	case TokenIdent:
		name := p.cur.Literal
		p.next()
		if p.cur.Type != TokenLParen {
			return &ColumnRef{Name: name}, nil
		}
		// function call: NAME(...)
		p.next() // consume (
		var args []Expr
		if p.cur.Type == TokenStar {
			args = []Expr{&StarExpr{}}
			p.next()
		} else if p.cur.Type != TokenRParen {
			arg, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			args = []Expr{arg}
		}
		if _, err := p.expect(TokenRParen); err != nil {
			return nil, err
		}
		return &FunctionCallExpr{Name: strings.ToUpper(name), Args: args}, nil
	case TokenLParen:
		p.next()
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(TokenRParen); err != nil {
			return nil, err
		}
		return expr, nil
	default:
		return nil, p.unexpected()
	}
}
