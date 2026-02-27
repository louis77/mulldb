package storage

import "testing"

func TestTableFileName_RoundTrip(t *testing.T) {
	cases := []struct {
		name     string
		wantFile string
	}{
		{"users", "users.wal"},
		{"my_table", "my_table.wal"},
		{"my-table", "my-table.wal"},
		{"my table", "my%20table.wal"},
		{"café", "caf%C3%A9.wal"},
		{"a.b", "a%2Eb.wal"},
		{"100%done", "100%25done.wal"},
		{"tbl/sub", "tbl%2Fsub.wal"},
		{"", ".wal"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tableFileName(tc.name)
			if got != tc.wantFile {
				t.Errorf("tableFileName(%q) = %q, want %q", tc.name, got, tc.wantFile)
			}

			decoded, err := tableNameFromFile(got)
			if err != nil {
				t.Fatalf("tableNameFromFile(%q) error: %v", got, err)
			}
			if decoded != tc.name {
				t.Errorf("round-trip: got %q, want %q", decoded, tc.name)
			}
		})
	}
}

func TestTableNameFromFile_Errors(t *testing.T) {
	cases := []string{
		"noext",
		"bad%2.wal",  // truncated percent-encoding
		"bad%GG.wal", // invalid hex
	}
	for _, tc := range cases {
		t.Run(tc, func(t *testing.T) {
			_, err := tableNameFromFile(tc)
			if err == nil {
				t.Errorf("tableNameFromFile(%q) should fail", tc)
			}
		})
	}
}
