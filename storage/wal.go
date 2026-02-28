package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
)

// WAL file header: [4-byte magic "MWAL"][uint16 version]
const (
	walMagic          = "MWAL"
	walHeaderSize     = 6 // 4 (magic) + 2 (version)
	walCurrentVersion = 3 // v1 = legacy (no PK flag), v2 = PK flag, v3 = ordinals + ALTER TABLE
)

// WAL operation types.
const (
	opCreateTable byte = 1
	opDropTable   byte = 2
	opInsert      byte = 3
	opDelete      byte = 4
	opUpdate      byte = 5
	opAddColumn   byte = 6
	opDropColumn  byte = 7
)

// WALMigrationNeededError is returned when a WAL file requires migration
// but the --migrate flag was not passed.
type WALMigrationNeededError struct {
	CurrentVersion  uint16
	RequiredVersion uint16
}

func (e *WALMigrationNeededError) Error() string {
	return fmt.Sprintf(
		"WAL file is format version %d but version %d is required; restart with --migrate flag",
		e.CurrentVersion, e.RequiredVersion,
	)
}

// rowUpdate pairs a row ID with its new values for WAL update entries.
type rowUpdate struct {
	RowID  int64
	Values []any
}

// WAL manages an append-only write-ahead log file.
// Entry format: [uint32 totalLen][byte op][payload…][uint32 crc32]
// CRC covers the op byte + payload.
type WAL struct {
	file *os.File
}

// OpenWAL opens (or creates) the WAL file at path. If the file uses an
// older format version and migrate is true, it is migrated in place
// (with the original preserved as a .bak file). If migrate is false and
// the file needs migration, a WALMigrationNeededError is returned.
func OpenWAL(path string, migrate bool) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	version, err := readWALVersion(f)
	if err != nil {
		f.Close()
		return nil, err
	}

	switch {
	case version == 0:
		// Empty file — write header and proceed.
		if err := writeWALHeader(f); err != nil {
			f.Close()
			return nil, err
		}
	case version < walCurrentVersion:
		f.Close()
		if !migrate {
			return nil, &WALMigrationNeededError{
				CurrentVersion:  version,
				RequiredVersion: walCurrentVersion,
			}
		}
		// Migrate with explicit opt-in.
		log.Printf("migrating WAL from version %d to %d...", version, walCurrentVersion)
		backupPath, err := migrateWAL(path, version)
		if err != nil {
			return nil, fmt.Errorf("migrate WAL v%d→v%d: %w", version, walCurrentVersion, err)
		}
		log.Printf("WAL migration complete. Original backed up to %s", backupPath)
		log.Printf("You can manually delete the backup once you have verified the migration.")
		f, err = os.OpenFile(path, os.O_RDWR, 0644)
		if err != nil {
			return nil, err
		}
	case version == walCurrentVersion:
		if migrate {
			log.Printf("WAL is already at current version %d, no migration needed.", walCurrentVersion)
		}
	case version > walCurrentVersion:
		f.Close()
		return nil, fmt.Errorf("WAL version %d is newer than supported version %d", version, walCurrentVersion)
	}

	// Seek to end for appending new entries.
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, err
	}
	return &WAL{file: f}, nil
}

// readWALVersion detects the WAL format version from the file header.
// Returns 0 for empty files, 1 for legacy headerless files, or the
// version number from the header.
func readWALVersion(f *os.File) (uint16, error) {
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	if info.Size() == 0 {
		return 0, nil // empty file
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}

	var magic [4]byte
	if _, err := io.ReadFull(f, magic[:]); err != nil {
		return 0, fmt.Errorf("read WAL magic: %w", err)
	}

	if string(magic[:]) != walMagic {
		// No magic header — this is a legacy v1 file.
		return 1, nil
	}

	var version uint16
	if err := binary.Read(f, binary.BigEndian, &version); err != nil {
		return 0, fmt.Errorf("read WAL version: %w", err)
	}
	return version, nil
}

// writeWALHeader writes the magic + version header at the current position.
func writeWALHeader(f *os.File) error {
	var hdr [walHeaderSize]byte
	copy(hdr[:4], walMagic)
	binary.BigEndian.PutUint16(hdr[4:], walCurrentVersion)
	_, err := f.Write(hdr[:])
	return err
}

// Close closes the WAL file.
func (w *WAL) Close() error {
	return w.file.Close()
}

// writeEntry appends a single WAL entry and fsyncs.
func (w *WAL) writeEntry(op byte, payload []byte) error {
	totalLen := uint32(4 + 1 + len(payload) + 4) // len + op + payload + crc

	entry := make([]byte, 0, totalLen)
	entry = binary.BigEndian.AppendUint32(entry, totalLen)
	entry = append(entry, op)
	entry = append(entry, payload...)
	entry = binary.BigEndian.AppendUint32(entry, crc32.ChecksumIEEE(entry[4:])) // crc of op+payload

	if _, err := w.file.Write(entry); err != nil {
		return err
	}
	return w.file.Sync()
}

// WriteCreateTable logs a CREATE TABLE operation.
// v3 format: [table:str][colCount:u16] per col: [name:str][datatype:u8][pk:u8][ordinal:u16]
func (w *WAL) WriteCreateTable(name string, columns []ColumnDef) error {
	buf := encodeString(nil, name)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(columns)))
	for _, col := range columns {
		buf = encodeString(buf, col.Name)
		buf = append(buf, byte(col.DataType))
		var pkFlag byte
		if col.PrimaryKey {
			pkFlag = 1
		}
		buf = append(buf, pkFlag)
		buf = binary.BigEndian.AppendUint16(buf, uint16(col.Ordinal))
	}
	return w.writeEntry(opCreateTable, buf)
}

// WriteDropTable logs a DROP TABLE operation.
func (w *WAL) WriteDropTable(name string) error {
	return w.writeEntry(opDropTable, encodeString(nil, name))
}

// WriteAddColumn logs an ALTER TABLE ADD COLUMN operation.
// Format: [table:str][name:str][datatype:u8][pk:u8][ordinal:u16]
func (w *WAL) WriteAddColumn(table string, col ColumnDef) error {
	buf := encodeString(nil, table)
	buf = encodeString(buf, col.Name)
	buf = append(buf, byte(col.DataType))
	var pkFlag byte
	if col.PrimaryKey {
		pkFlag = 1
	}
	buf = append(buf, pkFlag)
	buf = binary.BigEndian.AppendUint16(buf, uint16(col.Ordinal))
	return w.writeEntry(opAddColumn, buf)
}

// WriteDropColumn logs an ALTER TABLE DROP COLUMN operation.
// Format: [table:str][colName:str]
func (w *WAL) WriteDropColumn(table string, colName string) error {
	buf := encodeString(nil, table)
	buf = encodeString(buf, colName)
	return w.writeEntry(opDropColumn, buf)
}

// WriteInsert logs an INSERT operation for a single row.
func (w *WAL) WriteInsert(table string, rowID int64, values []any) error {
	buf := encodeString(nil, table)
	buf = binary.BigEndian.AppendUint64(buf, uint64(rowID))
	buf = encodeValues(buf, values)
	return w.writeEntry(opInsert, buf)
}

// WriteDelete logs a DELETE operation.
func (w *WAL) WriteDelete(table string, rowIDs []int64) error {
	buf := encodeString(nil, table)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(rowIDs)))
	for _, id := range rowIDs {
		buf = binary.BigEndian.AppendUint64(buf, uint64(id))
	}
	return w.writeEntry(opDelete, buf)
}

// WriteUpdate logs an UPDATE operation.
func (w *WAL) WriteUpdate(table string, updates []rowUpdate) error {
	buf := encodeString(nil, table)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(updates)))
	for _, u := range updates {
		buf = binary.BigEndian.AppendUint64(buf, uint64(u.RowID))
		buf = encodeValues(buf, u.Values)
	}
	return w.writeEntry(opUpdate, buf)
}

// -------------------------------------------------------------------------
// Replay
// -------------------------------------------------------------------------

// ReplayHandler receives decoded WAL entries during replay.
type ReplayHandler interface {
	OnCreateTable(name string, columns []ColumnDef) error
	OnDropTable(name string) error
	OnAddColumn(table string, col ColumnDef) error
	OnDropColumn(table string, colName string) error
	OnInsert(table string, rowID int64, values []any) error
	OnDelete(table string, rowIDs []int64) error
	OnUpdate(table string, updates []rowUpdate) error
}

// Replay reads the WAL from after the header and calls handler for every
// valid entry. It returns nil on clean EOF.
func (w *WAL) Replay(handler ReplayHandler) error {
	// Skip past the header to the first entry.
	if _, err := w.file.Seek(walHeaderSize, io.SeekStart); err != nil {
		return err
	}

	for {
		var totalLen uint32
		if err := binary.Read(w.file, binary.BigEndian, &totalLen); err != nil {
			if err == io.EOF {
				return nil // clean end
			}
			return fmt.Errorf("read entry length: %w", err)
		}
		if totalLen < 9 { // 4 (len) + 1 (op) + 4 (crc)
			return fmt.Errorf("WAL entry too short: %d bytes", totalLen)
		}

		rest := make([]byte, totalLen-4)
		if _, err := io.ReadFull(w.file, rest); err != nil {
			return fmt.Errorf("read entry body: %w", err)
		}

		data := rest[:len(rest)-4]
		storedCRC := binary.BigEndian.Uint32(rest[len(rest)-4:])
		if crc32.ChecksumIEEE(data) != storedCRC {
			return fmt.Errorf("WAL CRC mismatch")
		}

		if err := replayEntry(data[0], data[1:], handler); err != nil {
			return fmt.Errorf("replay: %w", err)
		}
	}
}

func replayEntry(op byte, payload []byte, h ReplayHandler) error {
	switch op {
	case opCreateTable:
		return replayCreateTable(payload, h)
	case opDropTable:
		return replayDropTable(payload, h)
	case opAddColumn:
		return replayAddColumn(payload, h)
	case opDropColumn:
		return replayDropColumn(payload, h)
	case opInsert:
		return replayInsert(payload, h)
	case opDelete:
		return replayDelete(payload, h)
	case opUpdate:
		return replayUpdate(payload, h)
	default:
		return fmt.Errorf("unknown WAL op %d", op)
	}
}

func replayCreateTable(payload []byte, h ReplayHandler) error {
	name, rest, err := decodeString(payload)
	if err != nil {
		return err
	}
	if len(rest) < 2 {
		return fmt.Errorf("truncated column count")
	}
	count := binary.BigEndian.Uint16(rest[:2])
	rest = rest[2:]

	cols := make([]ColumnDef, count)
	for i := range cols {
		cols[i].Name, rest, err = decodeString(rest)
		if err != nil {
			return err
		}
		if len(rest) < 4 { // datatype(1) + pk(1) + ordinal(2)
			return fmt.Errorf("truncated column type/pk/ordinal")
		}
		cols[i].DataType = DataType(rest[0])
		cols[i].PrimaryKey = rest[1] != 0
		cols[i].Ordinal = int(binary.BigEndian.Uint16(rest[2:4]))
		rest = rest[4:]
	}
	return h.OnCreateTable(name, cols)
}

func replayDropTable(payload []byte, h ReplayHandler) error {
	name, _, err := decodeString(payload)
	if err != nil {
		return err
	}
	return h.OnDropTable(name)
}

func replayAddColumn(payload []byte, h ReplayHandler) error {
	table, rest, err := decodeString(payload)
	if err != nil {
		return err
	}
	var col ColumnDef
	col.Name, rest, err = decodeString(rest)
	if err != nil {
		return err
	}
	if len(rest) < 4 { // datatype(1) + pk(1) + ordinal(2)
		return fmt.Errorf("truncated add column data")
	}
	col.DataType = DataType(rest[0])
	col.PrimaryKey = rest[1] != 0
	col.Ordinal = int(binary.BigEndian.Uint16(rest[2:4]))
	return h.OnAddColumn(table, col)
}

func replayDropColumn(payload []byte, h ReplayHandler) error {
	table, rest, err := decodeString(payload)
	if err != nil {
		return err
	}
	colName, _, err := decodeString(rest)
	if err != nil {
		return err
	}
	return h.OnDropColumn(table, colName)
}

func replayInsert(payload []byte, h ReplayHandler) error {
	table, rest, err := decodeString(payload)
	if err != nil {
		return err
	}
	if len(rest) < 8 {
		return fmt.Errorf("truncated row ID")
	}
	rowID := int64(binary.BigEndian.Uint64(rest[:8]))
	rest = rest[8:]
	values, _, err := decodeValues(rest)
	if err != nil {
		return err
	}
	return h.OnInsert(table, rowID, values)
}

func replayDelete(payload []byte, h ReplayHandler) error {
	table, rest, err := decodeString(payload)
	if err != nil {
		return err
	}
	if len(rest) < 2 {
		return fmt.Errorf("truncated delete count")
	}
	count := binary.BigEndian.Uint16(rest[:2])
	rest = rest[2:]
	ids := make([]int64, count)
	for i := range ids {
		if len(rest) < 8 {
			return fmt.Errorf("truncated delete row ID")
		}
		ids[i] = int64(binary.BigEndian.Uint64(rest[:8]))
		rest = rest[8:]
	}
	return h.OnDelete(table, ids)
}

func replayUpdate(payload []byte, h ReplayHandler) error {
	table, rest, err := decodeString(payload)
	if err != nil {
		return err
	}
	if len(rest) < 2 {
		return fmt.Errorf("truncated update count")
	}
	count := binary.BigEndian.Uint16(rest[:2])
	rest = rest[2:]
	updates := make([]rowUpdate, count)
	for i := range updates {
		if len(rest) < 8 {
			return fmt.Errorf("truncated update row ID")
		}
		updates[i].RowID = int64(binary.BigEndian.Uint64(rest[:8]))
		rest = rest[8:]
		updates[i].Values, rest, err = decodeValues(rest)
		if err != nil {
			return err
		}
	}
	return h.OnUpdate(table, updates)
}
