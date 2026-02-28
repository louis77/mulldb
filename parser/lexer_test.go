package parser

import "testing"

func TestLexerUTF8StringLiteral(t *testing.T) {
	l := NewLexer("'München'")
	tok := l.NextToken()
	if tok.Type != TokenStrLit {
		t.Fatalf("expected STRING, got %s", tok.Type)
	}
	if tok.Literal != "München" {
		t.Fatalf("expected München, got %q", tok.Literal)
	}
	if l.NextToken().Type != TokenEOF {
		t.Fatal("expected EOF")
	}
}

func TestLexerUTF8UnquotedIdent(t *testing.T) {
	l := NewLexer("café")
	tok := l.NextToken()
	if tok.Type != TokenIdent {
		t.Fatalf("expected IDENT, got %s", tok.Type)
	}
	if tok.Literal != "café" {
		t.Fatalf("expected café, got %q", tok.Literal)
	}
}

func TestLexerUTF8QuotedIdent(t *testing.T) {
	l := NewLexer(`"Ñoño"`)
	tok := l.NextToken()
	if tok.Type != TokenIdent {
		t.Fatalf("expected IDENT, got %s", tok.Type)
	}
	if tok.Literal != "Ñoño" {
		t.Fatalf("expected Ñoño, got %q", tok.Literal)
	}
}

func TestLexerUTF8QuotedIdentEscape(t *testing.T) {
	l := NewLexer(`"Stra""ße"`)
	tok := l.NextToken()
	if tok.Type != TokenIdent {
		t.Fatalf("expected IDENT, got %s", tok.Type)
	}
	if tok.Literal != `Stra"ße` {
		t.Fatalf("expected Stra\"ße, got %q", tok.Literal)
	}
}

func TestLexerCJKIdentifier(t *testing.T) {
	l := NewLexer("SELECT 名前 FROM テーブル")
	tests := []struct {
		typ TokenType
		lit string
	}{
		{TokenSelect, "SELECT"},
		{TokenIdent, "名前"},
		{TokenFrom, "FROM"},
		{TokenIdent, "テーブル"},
		{TokenEOF, ""},
	}
	for _, tt := range tests {
		tok := l.NextToken()
		if tok.Type != tt.typ {
			t.Fatalf("expected %s, got %s (literal %q)", tt.typ, tok.Type, tok.Literal)
		}
		if tt.lit != "" && tok.Literal != tt.lit {
			t.Fatalf("expected %q, got %q", tt.lit, tok.Literal)
		}
	}
}

func TestLexerEmojiInString(t *testing.T) {
	l := NewLexer("'hello 🌍 world'")
	tok := l.NextToken()
	if tok.Type != TokenStrLit {
		t.Fatalf("expected STRING, got %s", tok.Type)
	}
	if tok.Literal != "hello 🌍 world" {
		t.Fatalf("expected hello 🌍 world, got %q", tok.Literal)
	}
}

func TestLexerBytePositions(t *testing.T) {
	// "café" is 5 bytes (é = 2 bytes), then space + "1"
	l := NewLexer("café 1")
	tok1 := l.NextToken()
	if tok1.Pos != 0 {
		t.Fatalf("expected pos 0, got %d", tok1.Pos)
	}
	tok2 := l.NextToken()
	// "café" = 5 bytes, space = 1 byte → "1" starts at byte 6
	if tok2.Pos != 6 {
		t.Fatalf("expected pos 6, got %d", tok2.Pos)
	}
}

func TestLexerGreekIdentifier(t *testing.T) {
	l := NewLexer("αβγ = 42")
	tok := l.NextToken()
	if tok.Type != TokenIdent {
		t.Fatalf("expected IDENT, got %s", tok.Type)
	}
	if tok.Literal != "αβγ" {
		t.Fatalf("expected αβγ, got %q", tok.Literal)
	}
	eq := l.NextToken()
	if eq.Type != TokenEq {
		t.Fatalf("expected =, got %s", eq.Type)
	}
	num := l.NextToken()
	if num.Type != TokenIntLit || num.Literal != "42" {
		t.Fatalf("expected INT 42, got %s %q", num.Type, num.Literal)
	}
}

// ---------------------------------------------------------------------------
// Comment tests
// ---------------------------------------------------------------------------

func TestLexerCommentLineAtEnd(t *testing.T) {
	l := NewLexer("SELECT 1 -- this is a comment")
	tok := l.NextToken()
	if tok.Type != TokenSelect {
		t.Fatalf("expected SELECT, got %s", tok.Type)
	}
	tok = l.NextToken()
	if tok.Type != TokenIntLit || tok.Literal != "1" {
		t.Fatalf("expected INT 1, got %s %q", tok.Type, tok.Literal)
	}
	tok = l.NextToken()
	if tok.Type != TokenEOF {
		t.Fatalf("expected EOF, got %s %q", tok.Type, tok.Literal)
	}
}

func TestLexerCommentLineMidStatement(t *testing.T) {
	l := NewLexer("SELECT -- comment\n1")
	tok := l.NextToken()
	if tok.Type != TokenSelect {
		t.Fatalf("expected SELECT, got %s", tok.Type)
	}
	tok = l.NextToken()
	if tok.Type != TokenIntLit || tok.Literal != "1" {
		t.Fatalf("expected INT 1, got %s %q", tok.Type, tok.Literal)
	}
	tok = l.NextToken()
	if tok.Type != TokenEOF {
		t.Fatalf("expected EOF, got %s", tok.Type)
	}
}

func TestLexerCommentBlockInline(t *testing.T) {
	l := NewLexer("SELECT /* skip */ 1")
	tok := l.NextToken()
	if tok.Type != TokenSelect {
		t.Fatalf("expected SELECT, got %s", tok.Type)
	}
	tok = l.NextToken()
	if tok.Type != TokenIntLit || tok.Literal != "1" {
		t.Fatalf("expected INT 1, got %s %q", tok.Type, tok.Literal)
	}
	tok = l.NextToken()
	if tok.Type != TokenEOF {
		t.Fatalf("expected EOF, got %s", tok.Type)
	}
}

func TestLexerCommentBlockNested(t *testing.T) {
	l := NewLexer("SELECT /* outer /* inner */ outer */ 1")
	tok := l.NextToken()
	if tok.Type != TokenSelect {
		t.Fatalf("expected SELECT, got %s", tok.Type)
	}
	tok = l.NextToken()
	if tok.Type != TokenIntLit || tok.Literal != "1" {
		t.Fatalf("expected INT 1, got %s %q", tok.Type, tok.Literal)
	}
	tok = l.NextToken()
	if tok.Type != TokenEOF {
		t.Fatalf("expected EOF, got %s", tok.Type)
	}
}

func TestLexerCommentInsideStringLiteral(t *testing.T) {
	l := NewLexer("'hello -- world'")
	tok := l.NextToken()
	if tok.Type != TokenStrLit {
		t.Fatalf("expected STRING, got %s", tok.Type)
	}
	if tok.Literal != "hello -- world" {
		t.Fatalf("expected %q, got %q", "hello -- world", tok.Literal)
	}
}

func TestLexerCommentBlockInsideStringLiteral(t *testing.T) {
	l := NewLexer("'hello /* world */'")
	tok := l.NextToken()
	if tok.Type != TokenStrLit {
		t.Fatalf("expected STRING, got %s", tok.Type)
	}
	if tok.Literal != "hello /* world */" {
		t.Fatalf("expected %q, got %q", "hello /* world */", tok.Literal)
	}
}

func TestLexerCommentMinusOperatorNotConfused(t *testing.T) {
	l := NewLexer("5 - 3")
	tok := l.NextToken()
	if tok.Type != TokenIntLit || tok.Literal != "5" {
		t.Fatalf("expected INT 5, got %s %q", tok.Type, tok.Literal)
	}
	tok = l.NextToken()
	if tok.Type != TokenMinus {
		t.Fatalf("expected MINUS, got %s", tok.Type)
	}
	tok = l.NextToken()
	if tok.Type != TokenIntLit || tok.Literal != "3" {
		t.Fatalf("expected INT 3, got %s %q", tok.Type, tok.Literal)
	}
}

func TestLexerCommentSlashOperatorNotConfused(t *testing.T) {
	l := NewLexer("6 / 2")
	tok := l.NextToken()
	if tok.Type != TokenIntLit || tok.Literal != "6" {
		t.Fatalf("expected INT 6, got %s %q", tok.Type, tok.Literal)
	}
	tok = l.NextToken()
	if tok.Type != TokenSlash {
		t.Fatalf("expected SLASH, got %s", tok.Type)
	}
	tok = l.NextToken()
	if tok.Type != TokenIntLit || tok.Literal != "2" {
		t.Fatalf("expected INT 2, got %s %q", tok.Type, tok.Literal)
	}
}

func TestLexerCommentUnterminatedBlock(t *testing.T) {
	l := NewLexer("SELECT /* unterminated")
	tok := l.NextToken()
	if tok.Type != TokenSelect {
		t.Fatalf("expected SELECT, got %s", tok.Type)
	}
	tok = l.NextToken()
	if tok.Type != TokenEOF {
		t.Fatalf("expected EOF after unterminated block comment, got %s", tok.Type)
	}
}

func TestLexerCommentOnlyInput(t *testing.T) {
	l := NewLexer("-- just a comment")
	tok := l.NextToken()
	if tok.Type != TokenEOF {
		t.Fatalf("expected EOF, got %s %q", tok.Type, tok.Literal)
	}
}

func TestLexerCommentMultipleConsecutive(t *testing.T) {
	l := NewLexer("-- first\n-- second\nSELECT")
	tok := l.NextToken()
	if tok.Type != TokenSelect {
		t.Fatalf("expected SELECT, got %s %q", tok.Type, tok.Literal)
	}
}

func TestLexerCommentBlockMultiline(t *testing.T) {
	l := NewLexer("SELECT /* line1\nline2\nline3 */ 1")
	tok := l.NextToken()
	if tok.Type != TokenSelect {
		t.Fatalf("expected SELECT, got %s", tok.Type)
	}
	tok = l.NextToken()
	if tok.Type != TokenIntLit || tok.Literal != "1" {
		t.Fatalf("expected INT 1, got %s %q", tok.Type, tok.Literal)
	}
}

// ---------------------------------------------------------------------------
// Concatenation operator tests
// ---------------------------------------------------------------------------

func TestLexerConcatOperator(t *testing.T) {
	l := NewLexer("'a' || 'b'")
	tok := l.NextToken()
	if tok.Type != TokenStrLit || tok.Literal != "a" {
		t.Fatalf("expected STRING 'a', got %s %q", tok.Type, tok.Literal)
	}
	tok = l.NextToken()
	if tok.Type != TokenConcat || tok.Literal != "||" {
		t.Fatalf("expected || operator, got %s %q", tok.Type, tok.Literal)
	}
	tok = l.NextToken()
	if tok.Type != TokenStrLit || tok.Literal != "b" {
		t.Fatalf("expected STRING 'b', got %s %q", tok.Type, tok.Literal)
	}
}

func TestLexerSinglePipeIllegal(t *testing.T) {
	l := NewLexer("|")
	tok := l.NextToken()
	if tok.Type != TokenIllegal || tok.Literal != "|" {
		t.Fatalf("expected ILLEGAL '|', got %s %q", tok.Type, tok.Literal)
	}
}

func TestLexerFloatLiterals(t *testing.T) {
	tests := []struct {
		input string
		typ   TokenType
		lit   string
	}{
		{"3.14", TokenFloatLit, "3.14"},
		{".5", TokenFloatLit, ".5"},
		{"1e10", TokenFloatLit, "1e10"},
		{"2.5e-3", TokenFloatLit, "2.5e-3"},
		{"1E5", TokenFloatLit, "1E5"},
		{"3.0E+2", TokenFloatLit, "3.0E+2"},
		{"42", TokenIntLit, "42"},   // stays integer
		{"0", TokenIntLit, "0"},     // stays integer
	}
	for _, tt := range tests {
		l := NewLexer(tt.input)
		tok := l.NextToken()
		if tok.Type != tt.typ || tok.Literal != tt.lit {
			t.Errorf("input %q: got %s %q, want %s %q",
				tt.input, tok.Type, tok.Literal, tt.typ, tt.lit)
		}
	}
}

func TestLexerFloatLiteralVsDot(t *testing.T) {
	// "t.col" should be IDENT DOT IDENT, not mistaken for float.
	l := NewLexer("t.col")
	tok := l.NextToken()
	if tok.Type != TokenIdent || tok.Literal != "t" {
		t.Fatalf("expected IDENT 't', got %s %q", tok.Type, tok.Literal)
	}
	tok = l.NextToken()
	if tok.Type != TokenDot {
		t.Fatalf("expected DOT, got %s %q", tok.Type, tok.Literal)
	}
	tok = l.NextToken()
	if tok.Type != TokenIdent || tok.Literal != "col" {
		t.Fatalf("expected IDENT 'col', got %s %q", tok.Type, tok.Literal)
	}
}

func TestLexerFloatKeywords(t *testing.T) {
	l := NewLexer("FLOAT")
	tok := l.NextToken()
	if tok.Type != TokenFloatKW {
		t.Errorf("FLOAT: got %s, want FLOAT keyword", tok.Type)
	}

	l = NewLexer("DOUBLE")
	tok = l.NextToken()
	if tok.Type != TokenDoubleKW {
		t.Errorf("DOUBLE: got %s, want DOUBLE keyword", tok.Type)
	}
}
