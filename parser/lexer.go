package parser

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// Lexer tokenizes a SQL input string.
type Lexer struct {
	input string
	pos   int  // current byte position
	width int  // byte width of current rune
	ch    rune // current character, 0 at EOF
}

// NewLexer creates a lexer for the given input.
func NewLexer(input string) *Lexer {
	l := &Lexer{input: input}
	if len(input) > 0 {
		l.ch, l.width = utf8.DecodeRuneInString(input)
	}
	return l
}

func (l *Lexer) advance() {
	l.pos += l.width
	if l.pos >= len(l.input) {
		l.ch = 0
		l.width = 0
	} else {
		l.ch, l.width = utf8.DecodeRuneInString(l.input[l.pos:])
	}
}

func (l *Lexer) peek() rune {
	next := l.pos + l.width
	if next >= len(l.input) {
		return 0
	}
	r, _ := utf8.DecodeRuneInString(l.input[next:])
	return r
}

// NextToken returns the next token from the input.
func (l *Lexer) NextToken() Token {
	l.skipWhitespace()
	start := l.pos

	switch {
	case l.ch == 0:
		return Token{Type: TokenEOF, Pos: start}
	case l.ch == '(':
		l.advance()
		return Token{Type: TokenLParen, Literal: "(", Pos: start}
	case l.ch == ')':
		l.advance()
		return Token{Type: TokenRParen, Literal: ")", Pos: start}
	case l.ch == ',':
		l.advance()
		return Token{Type: TokenComma, Literal: ",", Pos: start}
	case l.ch == ';':
		l.advance()
		return Token{Type: TokenSemicolon, Literal: ";", Pos: start}
	case l.ch == '*':
		l.advance()
		return Token{Type: TokenStar, Literal: "*", Pos: start}
	case l.ch == '.':
		l.advance()
		return Token{Type: TokenDot, Literal: ".", Pos: start}
	case l.ch == '=':
		l.advance()
		return Token{Type: TokenEq, Literal: "=", Pos: start}
	case l.ch == '!':
		if l.peek() == '=' {
			l.advance()
			l.advance()
			return Token{Type: TokenNotEq, Literal: "!=", Pos: start}
		}
		l.advance()
		return Token{Type: TokenIllegal, Literal: "!", Pos: start}
	case l.ch == '<':
		if l.peek() == '=' {
			l.advance()
			l.advance()
			return Token{Type: TokenLtEq, Literal: "<=", Pos: start}
		}
		if l.peek() == '>' {
			l.advance()
			l.advance()
			return Token{Type: TokenNotEq, Literal: "<>", Pos: start}
		}
		l.advance()
		return Token{Type: TokenLt, Literal: "<", Pos: start}
	case l.ch == '>':
		if l.peek() == '=' {
			l.advance()
			l.advance()
			return Token{Type: TokenGtEq, Literal: ">=", Pos: start}
		}
		l.advance()
		return Token{Type: TokenGt, Literal: ">", Pos: start}
	case l.ch == '\'':
		return l.readString(start)
	case l.ch == '"':
		return l.readQuotedIdent(start)
	case isDigit(l.ch):
		return l.readInteger(start)
	case isLetter(l.ch) || l.ch == '_':
		return l.readIdentOrKeyword(start)
	default:
		ch := l.ch
		l.advance()
		return Token{Type: TokenIllegal, Literal: string(ch), Pos: start}
	}
}

func (l *Lexer) skipWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
		l.advance()
	}
}

func (l *Lexer) readString(start int) Token {
	l.advance() // skip opening quote
	begin := l.pos
	for l.ch != 0 && l.ch != '\'' {
		l.advance()
	}
	str := l.input[begin:l.pos]
	if l.ch == '\'' {
		l.advance() // skip closing quote
	}
	return Token{Type: TokenStrLit, Literal: str, Pos: start}
}

func (l *Lexer) readInteger(start int) Token {
	begin := l.pos
	for isDigit(l.ch) {
		l.advance()
	}
	return Token{Type: TokenIntLit, Literal: l.input[begin:l.pos], Pos: start}
}

func (l *Lexer) readIdentOrKeyword(start int) Token {
	begin := l.pos
	for isLetter(l.ch) || isDigit(l.ch) || l.ch == '_' {
		l.advance()
	}
	literal := l.input[begin:l.pos]
	return Token{Type: LookupKeyword(literal), Literal: literal, Pos: start}
}

func (l *Lexer) readQuotedIdent(start int) Token {
	l.advance() // skip opening double-quote
	var buf strings.Builder
	for {
		if l.ch == 0 {
			return Token{Type: TokenIllegal, Literal: buf.String(), Pos: start}
		}
		if l.ch == '"' {
			if l.peek() == '"' {
				// "" escape → literal double-quote
				buf.WriteByte('"')
				l.advance()
				l.advance()
				continue
			}
			l.advance() // skip closing double-quote
			return Token{Type: TokenIdent, Literal: buf.String(), Pos: start}
		}
		buf.WriteRune(l.ch)
		l.advance()
	}
}

func isDigit(ch rune) bool  { return ch >= '0' && ch <= '9' }
func isLetter(ch rune) bool { return unicode.IsLetter(ch) }
