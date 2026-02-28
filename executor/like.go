package executor

import (
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"
)

// likeToRegex converts a SQL LIKE pattern into a compiled Go regexp.
// The escape rune is used when hasEscape is true; it causes the next
// character in the pattern to be treated as a literal.
// If caseInsensitive is true, the regex uses the (?i) flag.
func likeToRegex(pattern string, escape rune, hasEscape, caseInsensitive bool) (*regexp.Regexp, error) {
	var b strings.Builder
	b.WriteString("(?s)") // . matches newlines (SQL _ matches any character)
	if caseInsensitive {
		b.WriteString("(?i)")
	}
	b.WriteByte('^')

	runes := []rune(pattern)
	for i := 0; i < len(runes); i++ {
		r := runes[i]
		if hasEscape && r == escape {
			i++
			if i >= len(runes) {
				return nil, fmt.Errorf("LIKE pattern ends with escape character")
			}
			b.WriteString(regexp.QuoteMeta(string(runes[i])))
			continue
		}
		switch r {
		case '%':
			b.WriteString(".*")
		case '_':
			b.WriteByte('.')
		default:
			b.WriteString(regexp.QuoteMeta(string(r)))
		}
	}

	b.WriteByte('$')
	return regexp.Compile(b.String())
}

// resolveEscapeRune extracts the escape rune from a string value.
// The ESCAPE clause must specify exactly one character.
func resolveEscapeRune(val any) (rune, error) {
	s, ok := val.(string)
	if !ok {
		return 0, fmt.Errorf("ESCAPE value must be a single character")
	}
	if utf8.RuneCountInString(s) != 1 {
		return 0, fmt.Errorf("ESCAPE value must be a single character")
	}
	r, _ := utf8.DecodeRuneInString(s)
	return r, nil
}
