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
