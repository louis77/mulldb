package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"syscall"
)

// entryMigrateFunc transforms a single WAL entry from version N to N+1.
// For entries that don't need transformation, return the op and payload
// unchanged.
type entryMigrateFunc func(op byte, payload []byte) (byte, []byte, error)

// walMigrations maps a source version to the function that migrates its
// entries to the next version. To migrate across multiple versions the
// functions are applied sequentially (v1→v2, v2→v3, …).
var walMigrations = map[uint16]entryMigrateFunc{
	1: migrateV1ToV2,
}

// rawEntry is an undecoded WAL entry (op + payload, CRC already verified).
type rawEntry struct {
	Op      byte
	Payload []byte
}

// migrateWAL reads a WAL file at path with format version fromVersion,
// migrates all entries to walCurrentVersion, and replaces the file.
// The original file is preserved as a .bak backup. Returns the backup
// file path.
func migrateWAL(path string, fromVersion uint16) (string, error) {
	// Check disk space before starting.
	if err := checkMigrationDiskSpace(path); err != nil {
		return "", err
	}

	// Open old file for reading.
	old, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer old.Close()

	// Read raw entries, skipping header if present.
	entries, err := readRawEntries(old, fromVersion > 1)
	if err != nil {
		return "", fmt.Errorf("read entries: %w", err)
	}

	// Apply migration chain.
	for v := fromVersion; v < walCurrentVersion; v++ {
		fn, ok := walMigrations[v]
		if !ok {
			return "", fmt.Errorf("no migration function for v%d→v%d", v, v+1)
		}
		for i, e := range entries {
			op, payload, err := fn(e.Op, e.Payload)
			if err != nil {
				return "", fmt.Errorf("migrate entry %d (v%d→v%d): %w", i, v, v+1, err)
			}
			entries[i] = rawEntry{Op: op, Payload: payload}
		}
	}

	// Write migrated file.
	tmpPath := path + ".mig"
	tmp, err := os.Create(tmpPath)
	if err != nil {
		return "", err
	}

	// Write header.
	var hdr [walHeaderSize]byte
	copy(hdr[:4], walMagic)
	binary.BigEndian.PutUint16(hdr[4:], walCurrentVersion)
	if _, err := tmp.Write(hdr[:]); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return "", err
	}

	// Write entries.
	for _, e := range entries {
		if err := writeRawEntry(tmp, e.Op, e.Payload); err != nil {
			tmp.Close()
			os.Remove(tmpPath)
			return "", err
		}
	}

	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return "", err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return "", err
	}

	// Preserve original as .bak (find unused backup name).
	backupPath := chooseBackupPath(path)
	if err := os.Rename(path, backupPath); err != nil {
		os.Remove(tmpPath)
		return "", fmt.Errorf("backup original WAL: %w", err)
	}

	// Move migrated file into place.
	if err := os.Rename(tmpPath, path); err != nil {
		// Try to restore original.
		os.Rename(backupPath, path)
		return "", fmt.Errorf("install migrated WAL: %w", err)
	}

	return backupPath, nil
}

// chooseBackupPath returns an unused .bak path. If path.bak exists, it
// tries path.bak.1, path.bak.2, etc.
func chooseBackupPath(path string) string {
	base := path + ".bak"
	if _, err := os.Stat(base); os.IsNotExist(err) {
		return base
	}
	for i := 1; ; i++ {
		candidate := fmt.Sprintf("%s.%d", base, i)
		if _, err := os.Stat(candidate); os.IsNotExist(err) {
			return candidate
		}
	}
}

// checkMigrationDiskSpace verifies that there is enough free disk space
// to perform the migration. The migrated file is roughly the same size
// as the original, and the original is kept as a backup, so we need at
// least 2x the original file size.
func checkMigrationDiskSpace(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	needed := uint64(info.Size()) * 2

	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return fmt.Errorf("check disk space: %w", err)
	}
	available := stat.Bavail * uint64(stat.Bsize)

	if available < needed {
		return fmt.Errorf(
			"insufficient disk space for WAL migration: need ~%d MB but only %d MB available",
			needed/(1024*1024)+1, available/(1024*1024),
		)
	}
	return nil
}

// readRawEntries reads all WAL entries as raw op+payload pairs.
// If hasHeader is true, the 6-byte header is skipped first.
func readRawEntries(f *os.File, hasHeader bool) ([]rawEntry, error) {
	offset := int64(0)
	if hasHeader {
		offset = walHeaderSize
	}
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}

	var entries []rawEntry
	for {
		var totalLen uint32
		if err := binary.Read(f, binary.BigEndian, &totalLen); err != nil {
			if err == io.EOF {
				return entries, nil
			}
			return nil, fmt.Errorf("read entry length: %w", err)
		}
		if totalLen < 9 {
			return nil, fmt.Errorf("WAL entry too short: %d bytes", totalLen)
		}

		rest := make([]byte, totalLen-4)
		if _, err := io.ReadFull(f, rest); err != nil {
			return nil, fmt.Errorf("read entry body: %w", err)
		}

		data := rest[:len(rest)-4]
		storedCRC := binary.BigEndian.Uint32(rest[len(rest)-4:])
		if crc32.ChecksumIEEE(data) != storedCRC {
			return nil, fmt.Errorf("WAL CRC mismatch")
		}

		entries = append(entries, rawEntry{Op: data[0], Payload: data[1:]})
	}
}

// writeRawEntry writes a single WAL entry (with length prefix and CRC).
func writeRawEntry(w io.Writer, op byte, payload []byte) error {
	totalLen := uint32(4 + 1 + len(payload) + 4)
	entry := make([]byte, 0, totalLen)
	entry = binary.BigEndian.AppendUint32(entry, totalLen)
	entry = append(entry, op)
	entry = append(entry, payload...)
	entry = binary.BigEndian.AppendUint32(entry, crc32.ChecksumIEEE(entry[4:]))
	_, err := w.Write(entry)
	return err
}

// -------------------------------------------------------------------------
// Migration functions
// -------------------------------------------------------------------------

// migrateV1ToV2 adds the PK flag byte (0) to each column in CREATE TABLE
// entries. All other entry types pass through unchanged.
//
// v1 CREATE TABLE column format: [string name][byte dataType]
// v2 CREATE TABLE column format: [string name][byte dataType][byte pkFlag]
func migrateV1ToV2(op byte, payload []byte) (byte, []byte, error) {
	if op != opCreateTable {
		return op, payload, nil
	}

	// Decode table name.
	name, rest, err := decodeString(payload)
	if err != nil {
		return 0, nil, fmt.Errorf("decode table name: %w", err)
	}
	if len(rest) < 2 {
		return 0, nil, fmt.Errorf("truncated column count")
	}
	count := binary.BigEndian.Uint16(rest[:2])
	rest = rest[2:]

	// Decode columns in v1 format (no PK flag).
	type v1Col struct {
		Name     string
		DataType byte
	}
	cols := make([]v1Col, count)
	for i := range cols {
		cols[i].Name, rest, err = decodeString(rest)
		if err != nil {
			return 0, nil, fmt.Errorf("column %d name: %w", i, err)
		}
		if len(rest) < 1 {
			return 0, nil, fmt.Errorf("column %d: truncated data type", i)
		}
		cols[i].DataType = rest[0]
		rest = rest[1:]
	}

	// Re-encode in v2 format (with PK flag = 0).
	buf := encodeString(nil, name)
	buf = binary.BigEndian.AppendUint16(buf, uint16(count))
	for _, col := range cols {
		buf = encodeString(buf, col.Name)
		buf = append(buf, col.DataType)
		buf = append(buf, 0) // pkFlag = false
	}

	return op, buf, nil
}
