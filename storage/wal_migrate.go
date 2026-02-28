package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
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
	2: migrateV2ToV3,
	3: migrateV3ToV4,
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

// migrateV2ToV3 adds the ordinal (uint16) to each column in CREATE TABLE
// entries. Ordinals are assigned sequentially (0, 1, 2, ...).
// All other entry types pass through unchanged.
//
// v2 CREATE TABLE column format: [string name][byte dataType][byte pkFlag]
// v3 CREATE TABLE column format: [string name][byte dataType][byte pkFlag][uint16 ordinal]
func migrateV2ToV3(op byte, payload []byte) (byte, []byte, error) {
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

	// Decode columns in v2 format (with PK flag, no ordinal).
	type v2Col struct {
		Name     string
		DataType byte
		PK       byte
	}
	cols := make([]v2Col, count)
	for i := range cols {
		cols[i].Name, rest, err = decodeString(rest)
		if err != nil {
			return 0, nil, fmt.Errorf("column %d name: %w", i, err)
		}
		if len(rest) < 2 {
			return 0, nil, fmt.Errorf("column %d: truncated data type/pk", i)
		}
		cols[i].DataType = rest[0]
		cols[i].PK = rest[1]
		rest = rest[2:]
	}

	// Re-encode in v3 format (with ordinal).
	buf := encodeString(nil, name)
	buf = binary.BigEndian.AppendUint16(buf, uint16(count))
	for i, col := range cols {
		buf = encodeString(buf, col.Name)
		buf = append(buf, col.DataType)
		buf = append(buf, col.PK)
		buf = binary.BigEndian.AppendUint16(buf, uint16(i)) // ordinal = sequential
	}

	return op, buf, nil
}

// migrateV3ToV4 adds the NOT NULL flag byte to each column in CREATE TABLE
// and ADD COLUMN entries. PK columns get notNull=1; all others get notNull=0.
// All other entry types pass through unchanged.
//
// v3 CREATE TABLE column format: [string name][byte dataType][byte pkFlag][uint16 ordinal]
// v4 CREATE TABLE column format: [string name][byte dataType][byte pkFlag][byte notNullFlag][uint16 ordinal]
//
// v3 ADD COLUMN format: [table:str][name:str][datatype:u8][pk:u8][ordinal:u16]
// v4 ADD COLUMN format: [table:str][name:str][datatype:u8][pk:u8][notNull:u8][ordinal:u16]
func migrateV3ToV4(op byte, payload []byte) (byte, []byte, error) {
	switch op {
	case opCreateTable:
		return migrateV3ToV4CreateTable(payload)
	case opAddColumn:
		return migrateV3ToV4AddColumn(payload)
	default:
		return op, payload, nil
	}
}

func migrateV3ToV4CreateTable(payload []byte) (byte, []byte, error) {
	name, rest, err := decodeString(payload)
	if err != nil {
		return 0, nil, fmt.Errorf("decode table name: %w", err)
	}
	if len(rest) < 2 {
		return 0, nil, fmt.Errorf("truncated column count")
	}
	count := binary.BigEndian.Uint16(rest[:2])
	rest = rest[2:]

	type v3Col struct {
		Name     string
		DataType byte
		PK       byte
		Ordinal  uint16
	}
	cols := make([]v3Col, count)
	for i := range cols {
		cols[i].Name, rest, err = decodeString(rest)
		if err != nil {
			return 0, nil, fmt.Errorf("column %d name: %w", i, err)
		}
		if len(rest) < 4 { // datatype(1) + pk(1) + ordinal(2)
			return 0, nil, fmt.Errorf("column %d: truncated data", i)
		}
		cols[i].DataType = rest[0]
		cols[i].PK = rest[1]
		cols[i].Ordinal = binary.BigEndian.Uint16(rest[2:4])
		rest = rest[4:]
	}

	// Re-encode in v4 format. PK columns are implicitly NOT NULL.
	buf := encodeString(nil, name)
	buf = binary.BigEndian.AppendUint16(buf, uint16(count))
	for _, col := range cols {
		buf = encodeString(buf, col.Name)
		buf = append(buf, col.DataType)
		buf = append(buf, col.PK)
		buf = append(buf, col.PK) // notNull = same as PK flag
		buf = binary.BigEndian.AppendUint16(buf, col.Ordinal)
	}
	return opCreateTable, buf, nil
}

func migrateV3ToV4AddColumn(payload []byte) (byte, []byte, error) {
	table, rest, err := decodeString(payload)
	if err != nil {
		return 0, nil, fmt.Errorf("decode table name: %w", err)
	}
	colName, rest, err := decodeString(rest)
	if err != nil {
		return 0, nil, fmt.Errorf("decode column name: %w", err)
	}
	if len(rest) < 4 { // datatype(1) + pk(1) + ordinal(2)
		return 0, nil, fmt.Errorf("truncated add column data")
	}
	dataType := rest[0]
	pk := rest[1]
	ordinal := binary.BigEndian.Uint16(rest[2:4])

	buf := encodeString(nil, table)
	buf = encodeString(buf, colName)
	buf = append(buf, dataType)
	buf = append(buf, pk)
	buf = append(buf, pk) // notNull = same as PK flag
	buf = binary.BigEndian.AppendUint16(buf, ordinal)
	return opAddColumn, buf, nil
}

// -------------------------------------------------------------------------
// Single-WAL → Split-WAL migration
// -------------------------------------------------------------------------

// migrateToSplitWAL reads the legacy single wal.dat, classifies entries
// as DDL or DML, and writes them to separate files:
//   - catalog.wal  — CreateTable / DropTable entries only
//   - tables/<name>.wal — DML entries per surviving table
//
// The original wal.dat is backed up to wal.dat.bak.
func migrateToSplitWAL(dataDir string) error {
	legacyPath := filepath.Join(dataDir, legacyWALName)

	if err := checkMigrationDiskSpace(legacyPath); err != nil {
		return err
	}

	// Read all entries from the legacy WAL.
	old, err := os.Open(legacyPath)
	if err != nil {
		return err
	}
	entries, err := readRawEntries(old, true) // v2 has header
	old.Close()
	if err != nil {
		return fmt.Errorf("read legacy WAL: %w", err)
	}

	// Classify entries. Track which tables are alive after all DDL.
	var ddlEntries []rawEntry
	// tableName → list of DML entries for that table
	dmlByTable := make(map[string][]rawEntry)
	// Track alive tables to know which DML to keep.
	alive := make(map[string]bool)

	for _, e := range entries {
		switch e.Op {
		case opCreateTable:
			ddlEntries = append(ddlEntries, e)
			name, _, err := decodeString(e.Payload)
			if err != nil {
				return fmt.Errorf("decode CREATE TABLE name: %w", err)
			}
			alive[name] = true

		case opDropTable:
			ddlEntries = append(ddlEntries, e)
			name, _, err := decodeString(e.Payload)
			if err != nil {
				return fmt.Errorf("decode DROP TABLE name: %w", err)
			}
			delete(alive, name)
			delete(dmlByTable, name) // discard DML for dropped tables

		case opAddColumn, opDropColumn:
			// ALTER TABLE ops are DDL — go to catalog WAL.
			ddlEntries = append(ddlEntries, e)

		case opInsert, opDelete, opUpdate:
			name, _, err := decodeString(e.Payload)
			if err != nil {
				return fmt.Errorf("decode DML table name: %w", err)
			}
			dmlByTable[name] = append(dmlByTable[name], e)

		default:
			return fmt.Errorf("unknown WAL op %d during migration", e.Op)
		}
	}

	// Create tables directory.
	tablesDir := filepath.Join(dataDir, tablesDirName)
	if err := os.MkdirAll(tablesDir, 0755); err != nil {
		return err
	}

	// Write catalog.wal (DDL entries only).
	catalogPath := filepath.Join(dataDir, catalogWALName)
	if err := writeWALFile(catalogPath, ddlEntries); err != nil {
		return fmt.Errorf("write catalog WAL: %w", err)
	}

	// Write per-table WAL files (DML entries for surviving tables only).
	for name := range alive {
		dmlEntries, ok := dmlByTable[name]
		if !ok {
			// Table exists but has no DML — create empty WAL.
			dmlEntries = nil
		}
		walPath := filepath.Join(tablesDir, tableFileName(name))
		if err := writeWALFile(walPath, dmlEntries); err != nil {
			return fmt.Errorf("write table WAL %q: %w", name, err)
		}
	}

	// Back up the legacy WAL.
	backupPath := chooseBackupPath(legacyPath)
	if err := os.Rename(legacyPath, backupPath); err != nil {
		return fmt.Errorf("backup legacy WAL: %w", err)
	}

	return nil
}

// writeWALFile creates a new WAL file with the current header and the
// given raw entries.
func writeWALFile(path string, entries []rawEntry) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}

	if err := writeWALHeader(f); err != nil {
		f.Close()
		os.Remove(path)
		return err
	}

	for _, e := range entries {
		if err := writeRawEntry(f, e.Op, e.Payload); err != nil {
			f.Close()
			os.Remove(path)
			return err
		}
	}

	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(path)
		return err
	}
	return f.Close()
}
