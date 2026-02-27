// Package index defines the Index interface for key-to-rowID mappings
// and provides concrete implementations (e.g. B-tree).
package index

// Index is a unique-key index that maps a single key value to a row ID.
// Implementations must support Put (with duplicate detection), Get, and Delete.
type Index interface {
	// Put inserts a key→rowID mapping. Returns false if the key already exists.
	Put(key any, rowID int64) bool
	// Get looks up a key and returns its rowID. Returns false if not found.
	Get(key any) (int64, bool)
	// Delete removes a key. Returns false if the key was not found.
	Delete(key any) bool
}
