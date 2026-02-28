package executor

import "testing"

func TestLikeToRegex_Percent(t *testing.T) {
	re, err := likeToRegex("%foo%", 0, false, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		input string
		want  bool
	}{
		{"foo", true},
		{"foobar", true},
		{"barfoo", true},
		{"barfoobar", true},
		{"bar", false},
	} {
		if got := re.MatchString(tc.input); got != tc.want {
			t.Errorf("%%foo%% match %q = %v, want %v", tc.input, got, tc.want)
		}
	}
}

func TestLikeToRegex_Underscore(t *testing.T) {
	re, err := likeToRegex("_ob", 0, false, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		input string
		want  bool
	}{
		{"Bob", true},
		{"rob", true},
		{"ob", false},
		{"aaob", false},
	} {
		if got := re.MatchString(tc.input); got != tc.want {
			t.Errorf("_ob match %q = %v, want %v", tc.input, got, tc.want)
		}
	}
}

func TestLikeToRegex_Escape(t *testing.T) {
	re, err := likeToRegex(`100\%`, '\\', true, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		input string
		want  bool
	}{
		{"100%", true},
		{"100", false},
		{"100%extra", false},
	} {
		if got := re.MatchString(tc.input); got != tc.want {
			t.Errorf(`100\%% match %q = %v, want %v`, tc.input, got, tc.want)
		}
	}
}

func TestLikeToRegex_EscapeUnderscore(t *testing.T) {
	re, err := likeToRegex(`a\_b`, '\\', true, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		input string
		want  bool
	}{
		{"a_b", true},
		{"axb", false},
		{"a_bc", false},
	} {
		if got := re.MatchString(tc.input); got != tc.want {
			t.Errorf(`a\_b match %q = %v, want %v`, tc.input, got, tc.want)
		}
	}
}

func TestLikeToRegex_CaseInsensitive(t *testing.T) {
	re, err := likeToRegex("Alice%", 0, false, true)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		input string
		want  bool
	}{
		{"Alice", true},
		{"ALICE", true},
		{"alice", true},
		{"alicex", true},
		{"Bob", false},
	} {
		if got := re.MatchString(tc.input); got != tc.want {
			t.Errorf("Alice%% (ILIKE) match %q = %v, want %v", tc.input, got, tc.want)
		}
	}
}

func TestLikeToRegex_EmptyPattern(t *testing.T) {
	re, err := likeToRegex("", 0, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if !re.MatchString("") {
		t.Error("empty pattern should match empty string")
	}
	if re.MatchString("a") {
		t.Error("empty pattern should not match non-empty string")
	}
}

func TestLikeToRegex_Unicode(t *testing.T) {
	// _ should match one Unicode codepoint, not one byte
	re, err := likeToRegex("_bc", 0, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if !re.MatchString("日bc") {
		t.Error("_ should match Unicode character 日")
	}
	if !re.MatchString("ébc") {
		t.Error("_ should match Unicode character é")
	}
	if re.MatchString("bc") {
		t.Error("_ should require exactly one character")
	}
}

func TestLikeToRegex_RegexMetacharacters(t *testing.T) {
	// Regex-special characters in the pattern should be escaped
	re, err := likeToRegex("a.b", 0, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if !re.MatchString("a.b") {
		t.Error("literal dot should match")
	}
	if re.MatchString("axb") {
		t.Error("literal dot should not act as regex wildcard")
	}
}

func TestLikeToRegex_PercentOnly(t *testing.T) {
	re, err := likeToRegex("%", 0, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if !re.MatchString("") {
		t.Error("% should match empty string")
	}
	if !re.MatchString("anything") {
		t.Error("% should match anything")
	}
}

func TestLikeToRegex_TrailingEscape(t *testing.T) {
	_, err := likeToRegex(`foo\`, '\\', true, false)
	if err == nil {
		t.Error("expected error for pattern ending with escape character")
	}
}

func TestResolveEscapeRune_Valid(t *testing.T) {
	r, err := resolveEscapeRune(`\`)
	if err != nil {
		t.Fatal(err)
	}
	if r != '\\' {
		t.Errorf("rune = %q, want '\\'", r)
	}
}

func TestResolveEscapeRune_MultiChar(t *testing.T) {
	_, err := resolveEscapeRune("ab")
	if err == nil {
		t.Error("expected error for multi-character escape")
	}
}

func TestResolveEscapeRune_Empty(t *testing.T) {
	_, err := resolveEscapeRune("")
	if err == nil {
		t.Error("expected error for empty escape")
	}
}
