package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// WAL operation types.
const (
	opCreateTable byte = 1
	opDropTable   byte = 2
	opInsert      byte = 3
	opDelete      byte = 4
	opUpdate      byte = 5
)

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

// OpenWAL opens (or creates) the WAL file at path.
func OpenWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	// Seek to end for appending new entries.
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, err
	}
	return &WAL{file: f}, nil
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
func (w *WAL) WriteCreateTable(name string, columns []ColumnDef) error {
	buf := encodeString(nil, name)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(columns)))
	for _, col := range columns {
		buf = encodeString(buf, col.Name)
		buf = append(buf, byte(col.DataType))
	}
	return w.writeEntry(opCreateTable, buf)
}

// WriteDropTable logs a DROP TABLE operation.
func (w *WAL) WriteDropTable(name string) error {
	return w.writeEntry(opDropTable, encodeString(nil, name))
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
	OnInsert(table string, rowID int64, values []any) error
	OnDelete(table string, rowIDs []int64) error
	OnUpdate(table string, updates []rowUpdate) error
}

// Replay reads the WAL from the beginning and calls handler for every
// valid entry. It returns nil on clean EOF.
func (w *WAL) Replay(handler ReplayHandler) error {
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
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
		if len(rest) < 1 {
			return fmt.Errorf("truncated column type")
		}
		cols[i].DataType = DataType(rest[0])
		rest = rest[1:]
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
