package parser

import "strings"

// TokenType identifies the kind of token produced by the lexer.
type TokenType int

const (
	// Special tokens.
	TokenEOF     TokenType = iota
	TokenIllegal           // unrecognized character

	// Literals.
	TokenIdent  // identifier (column name, table name)
	TokenIntLit // integer literal
	TokenStrLit // single-quoted string literal

	// Operators.
	TokenEq    // =
	TokenNotEq // != or <>
	TokenLt    // <
	TokenGt    // >
	TokenLtEq  // <=
	TokenGtEq  // >=

	// Punctuation.
	TokenLParen    // (
	TokenRParen    // )
	TokenComma     // ,
	TokenSemicolon // ;
	TokenStar      // *
	TokenDot       // .
	TokenPlus      // +
	TokenMinus     // -
	TokenSlash     // /
	TokenPercent   // %
	TokenConcat    // ||

	// Keywords.
	TokenSelect
	TokenFrom
	TokenWhere
	TokenInsert
	TokenInto
	TokenValues
	TokenUpdate
	TokenSet
	TokenDelete
	TokenCreate
	TokenDrop
	TokenTable
	TokenAnd
	TokenOr
	TokenNot
	TokenTrue
	TokenFalse
	TokenNull
	TokenIntegerKW // INTEGER (data type keyword)
	TokenTextKW    // TEXT (data type keyword)
	TokenBooleanKW // BOOLEAN (data type keyword)
	TokenAs        // AS
	TokenLimit     // LIMIT
	TokenOffset    // OFFSET
	TokenPrimary   // PRIMARY
	TokenKey       // KEY
	TokenOrder     // ORDER
	TokenBy        // BY
	TokenAsc       // ASC
	TokenDesc      // DESC
	TokenIs        // IS
	TokenJoin      // JOIN
	TokenInner     // INNER
	TokenOn        // ON
	TokenBegin     // BEGIN
	TokenCommit    // COMMIT
	TokenRollback  // ROLLBACK
	TokenLike      // LIKE
	TokenIlike     // ILIKE
	TokenEscape    // ESCAPE
)

var tokenNames = map[TokenType]string{
	TokenEOF:       "EOF",
	TokenIllegal:   "ILLEGAL",
	TokenIdent:     "IDENT",
	TokenIntLit:    "INT",
	TokenStrLit:    "STRING",
	TokenEq:        "=",
	TokenNotEq:     "!=",
	TokenLt:        "<",
	TokenGt:        ">",
	TokenLtEq:      "<=",
	TokenGtEq:      ">=",
	TokenLParen:    "(",
	TokenRParen:    ")",
	TokenComma:     ",",
	TokenSemicolon: ";",
	TokenStar:      "*",
	TokenDot:       ".",
	TokenPlus:      "+",
	TokenMinus:     "-",
	TokenSlash:     "/",
	TokenPercent:   "%",
	TokenConcat:    "||",
	TokenSelect:    "SELECT",
	TokenFrom:      "FROM",
	TokenWhere:     "WHERE",
	TokenInsert:    "INSERT",
	TokenInto:      "INTO",
	TokenValues:    "VALUES",
	TokenUpdate:    "UPDATE",
	TokenSet:       "SET",
	TokenDelete:    "DELETE",
	TokenCreate:    "CREATE",
	TokenDrop:      "DROP",
	TokenTable:     "TABLE",
	TokenAnd:       "AND",
	TokenOr:        "OR",
	TokenNot:       "NOT",
	TokenTrue:      "TRUE",
	TokenFalse:     "FALSE",
	TokenNull:      "NULL",
	TokenIntegerKW: "INTEGER",
	TokenTextKW:    "TEXT",
	TokenBooleanKW: "BOOLEAN",
	TokenAs:        "AS",
	TokenLimit:     "LIMIT",
	TokenOffset:    "OFFSET",
	TokenPrimary:   "PRIMARY",
	TokenKey:       "KEY",
	TokenOrder:     "ORDER",
	TokenBy:        "BY",
	TokenAsc:       "ASC",
	TokenDesc:      "DESC",
	TokenIs:        "IS",
	TokenJoin:      "JOIN",
	TokenInner:     "INNER",
	TokenOn:        "ON",
	TokenBegin:     "BEGIN",
	TokenCommit:    "COMMIT",
	TokenRollback:  "ROLLBACK",
	TokenLike:      "LIKE",
	TokenIlike:     "ILIKE",
	TokenEscape:    "ESCAPE",
}

func (t TokenType) String() string {
	if s, ok := tokenNames[t]; ok {
		return s
	}
	return "UNKNOWN"
}

// Token is a single lexical unit produced by the lexer.
type Token struct {
	Type    TokenType
	Literal string
	Pos     int // byte offset in the input
}

var keywords = map[string]TokenType{
	"SELECT":  TokenSelect,
	"FROM":    TokenFrom,
	"WHERE":   TokenWhere,
	"INSERT":  TokenInsert,
	"INTO":    TokenInto,
	"VALUES":  TokenValues,
	"UPDATE":  TokenUpdate,
	"SET":     TokenSet,
	"DELETE":  TokenDelete,
	"CREATE":  TokenCreate,
	"DROP":    TokenDrop,
	"TABLE":   TokenTable,
	"AND":     TokenAnd,
	"OR":      TokenOr,
	"NOT":     TokenNot,
	"TRUE":    TokenTrue,
	"FALSE":   TokenFalse,
	"NULL":    TokenNull,
	"SMALLINT": TokenIntegerKW,
	"INT":      TokenIntegerKW,
	"INTEGER":  TokenIntegerKW,
	"BIGINT":   TokenIntegerKW,
	"TEXT":    TokenTextKW,
	"BOOLEAN": TokenBooleanKW,
	"AS":      TokenAs,
	"LIMIT":   TokenLimit,
	"OFFSET":  TokenOffset,
	"PRIMARY": TokenPrimary,
	"KEY":     TokenKey,
	"ORDER":   TokenOrder,
	"BY":      TokenBy,
	"ASC":     TokenAsc,
	"DESC":    TokenDesc,
	"IS":       TokenIs,
	"JOIN":     TokenJoin,
	"INNER":    TokenInner,
	"ON":       TokenOn,
	"BEGIN":    TokenBegin,
	"COMMIT":   TokenCommit,
	"ROLLBACK": TokenRollback,
	"LIKE":     TokenLike,
	"ILIKE":    TokenIlike,
	"ESCAPE":   TokenEscape,
}

// LookupKeyword returns the keyword token type for ident, or TokenIdent
// if it is not a keyword.
func LookupKeyword(ident string) TokenType {
	if tok, ok := keywords[strings.ToUpper(ident)]; ok {
		return tok
	}
	return TokenIdent
}
