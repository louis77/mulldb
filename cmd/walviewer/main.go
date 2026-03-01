// cmd/walviewer is a human-friendly WAL file viewer for mulldb.
//
// Usage:
//
//	walviewer [flags] <wal-file>
//
// Flags:
//
//	--page-size N    Number of entries to show per page (default: 20)
//	--op TYPE        Filter by operation type (create-table, drop-table,
//	                 insert, insert-batch, delete, update, add-column,
//	                 drop-column, create-index, drop-index)
//	--no-page        Disable interactive paging, print all entries
//	--raw            Show raw hex dump of each entry
//
// Interactive commands (when paging):
//
//	n or <space>     Next page
//	p or b           Previous page
//	g N              Go to entry number N
//	q or Ctrl+C      Quit
package main

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

// WAL constants matching storage/wal.go
const (
	walMagic      = "MWAL"
	walHeaderSize = 6 // 4 (magic) + 2 (version)
)

// Operation types
const (
	opCreateTable byte = 1
	opDropTable   byte = 2
	opInsert      byte = 3
	opDelete      byte = 4
	opUpdate      byte = 5
	opAddColumn   byte = 6
	opDropColumn  byte = 7
	opCreateIndex byte = 8
	opDropIndex   byte = 9
	opInsertBatch byte = 10
)

// Value type tags matching storage/row.go
const (
	tagNull      byte = 0
	tagInteger   byte = 1
	tagText      byte = 2
	tagBoolean   byte = 3
	tagTimestamp byte = 4
	tagFloat     byte = 5
)

// Data types
const (
	typeInteger   byte = 0
	typeText      byte = 1
	typeBoolean   byte = 2
	typeTimestamp byte = 3
	typeFloat     byte = 4
)

// Entry represents a single WAL entry
type Entry struct {
	Number   int
	Offset   int64
	OpCode   byte
	OpName   string
	Payload  []byte
	CRC      uint32
	CRCValid bool
	Details  string
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	// Parse flags
	var (
		pageSize = 20
		opFilter string
		noPage   = false
		rawMode  = false
		filePath string
	)

	for i := 1; i < len(os.Args); i++ {
		arg := os.Args[i]
		switch arg {
		case "--help", "-h":
			printUsage()
			os.Exit(0)
		case "--page-size":
			i++
			if i >= len(os.Args) {
				fmt.Fprintln(os.Stderr, "Error: --page-size requires an argument")
				os.Exit(1)
			}
			n, err := strconv.Atoi(os.Args[i])
			if err != nil || n < 1 {
				fmt.Fprintln(os.Stderr, "Error: invalid page size")
				os.Exit(1)
			}
			pageSize = n
		case "--op":
			i++
			if i >= len(os.Args) {
				fmt.Fprintln(os.Stderr, "Error: --op requires an argument")
				os.Exit(1)
			}
			opFilter = os.Args[i]
		case "--no-page":
			noPage = true
		case "--raw":
			rawMode = true
		default:
			if strings.HasPrefix(arg, "-") {
				fmt.Fprintf(os.Stderr, "Error: unknown flag %s\n", arg)
				os.Exit(1)
			}
			filePath = arg
		}
	}

	if filePath == "" {
		fmt.Fprintln(os.Stderr, "Error: no WAL file specified")
		printUsage()
		os.Exit(1)
	}

	// Open and parse WAL file
	entries, version, err := parseWALFile(filePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	// Filter by operation type if specified
	if opFilter != "" {
		entries = filterEntries(entries, opFilter)
	}

	// Print file info
	printFileInfo(filePath, version, len(entries))

	if len(entries) == 0 {
		fmt.Println("\nNo entries found.")
		return
	}

	// Display entries
	if noPage || rawMode {
		// Print all entries without paging
		for _, e := range entries {
			printEntry(&e, rawMode)
		}
	} else {
		// Interactive paging mode
		runPager(entries, pageSize)
	}
}

func printUsage() {
	fmt.Println(`walviewer - Human-friendly WAL file viewer for mulldb

Usage: walviewer [flags] <wal-file>

Flags:
  --page-size N    Number of entries per page (default: 20)
  --op TYPE        Filter by operation type
  --no-page        Disable interactive paging
  --raw            Show raw hex dump
  --help, -h       Show this help message

Operation types for --op filter:
  create-table, drop-table, insert, insert-batch, delete, update,
  add-column, drop-column, create-index, drop-index

Interactive commands (during paging):
  n, <space>   Next page
  p, b         Previous page  
  g N          Go to entry N
  q, Ctrl+C    Quit`)
}

func parseWALFile(path string) ([]Entry, uint16, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, fmt.Errorf("open file: %w", err)
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return nil, 0, fmt.Errorf("stat file: %w", err)
	}

	if stat.Size() == 0 {
		return nil, 0, nil
	}

	// Read header
	var version uint16
	if stat.Size() >= walHeaderSize {
		var magic [4]byte
		if _, err := io.ReadFull(f, magic[:]); err != nil {
			return nil, 0, fmt.Errorf("read magic: %w", err)
		}
		if string(magic[:]) == walMagic {
			if err := binary.Read(f, binary.BigEndian, &version); err != nil {
				return nil, 0, fmt.Errorf("read version: %w", err)
			}
		} else {
			// Legacy v1 file without header
			version = 1
		}
	} else {
		// File too small for header
		version = 1
	}

	// Seek to after header for reading entries
	if version == 1 {
		_, err = f.Seek(0, io.SeekStart)
	} else {
		_, err = f.Seek(walHeaderSize, io.SeekStart)
	}
	if err != nil {
		return nil, 0, fmt.Errorf("seek: %w", err)
	}

	// Read all entries
	var entries []Entry
	offset, _ := f.Seek(0, io.SeekCurrent)
	entryNum := 1

	for {
		var totalLen uint32
		if err := binary.Read(f, binary.BigEndian, &totalLen); err != nil {
			if err == io.EOF {
				break
			}
			return nil, 0, fmt.Errorf("read entry length at offset %d: %w", offset, err)
		}

		if totalLen < 9 { // 4 (len) + 1 (op) + 4 (crc)
			return nil, 0, fmt.Errorf("invalid entry length %d at offset %d", totalLen, offset)
		}

		// Read the rest of the entry
		rest := make([]byte, totalLen-4)
		if _, err := io.ReadFull(f, rest); err != nil {
			return nil, 0, fmt.Errorf("read entry body at offset %d: %w", offset, err)
		}

		data := rest[:len(rest)-4]
		storedCRC := binary.BigEndian.Uint32(rest[len(rest)-4:])
		computedCRC := crc32.ChecksumIEEE(data)

		opCode := data[0]
		payload := data[1:]

		entry := Entry{
			Number:   entryNum,
			Offset:   offset,
			OpCode:   opCode,
			OpName:   opName(opCode),
			Payload:  payload,
			CRC:      storedCRC,
			CRCValid: computedCRC == storedCRC,
		}

		// Decode details
		entry.Details = decodeDetails(&entry)

		entries = append(entries, entry)

		entryNum++
		offset, _ = f.Seek(0, io.SeekCurrent)
	}

	return entries, version, nil
}

func opName(op byte) string {
	switch op {
	case opCreateTable:
		return "CREATE-TABLE"
	case opDropTable:
		return "DROP-TABLE"
	case opInsert:
		return "INSERT"
	case opDelete:
		return "DELETE"
	case opUpdate:
		return "UPDATE"
	case opAddColumn:
		return "ADD-COLUMN"
	case opDropColumn:
		return "DROP-COLUMN"
	case opCreateIndex:
		return "CREATE-INDEX"
	case opDropIndex:
		return "DROP-INDEX"
	case opInsertBatch:
		return "INSERT-BATCH"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", op)
	}
}

func decodeDetails(e *Entry) string {
	if !e.CRCValid {
		return "[CRC MISMATCH]"
	}

	switch e.OpCode {
	case opCreateTable:
		return decodeCreateTable(e.Payload)
	case opDropTable:
		return decodeDropTable(e.Payload)
	case opInsert:
		return decodeInsert(e.Payload)
	case opInsertBatch:
		return decodeInsertBatch(e.Payload)
	case opDelete:
		return decodeDelete(e.Payload)
	case opUpdate:
		return decodeUpdate(e.Payload)
	case opAddColumn:
		return decodeAddColumn(e.Payload)
	case opDropColumn:
		return decodeDropColumn(e.Payload)
	case opCreateIndex:
		return decodeCreateIndex(e.Payload)
	case opDropIndex:
		return decodeDropIndex(e.Payload)
	default:
		return fmt.Sprintf("[unknown op: %d, %d bytes payload]", e.OpCode, len(e.Payload))
	}
}

func decodeCreateTable(data []byte) string {
	tableName, rest, err := decodeString(data)
	if err != nil {
		return fmt.Sprintf("[error: %v]", err)
	}
	if len(rest) < 2 {
		return "[truncated column count]"
	}
	count := binary.BigEndian.Uint16(rest[:2])
	rest = rest[2:]

	var cols []string
	for i := 0; i < int(count); i++ {
		colName, r, err := decodeString(rest)
		if err != nil {
			return fmt.Sprintf("[error reading col %d: %v]", i, err)
		}
		if len(r) < 5 {
			return fmt.Sprintf("[truncated column %d def]", i)
		}
		dataType := r[0]
		pkFlag := r[1] != 0
		notNullFlag := r[2] != 0
		ordinal := binary.BigEndian.Uint16(r[3:5])
		rest = r[5:]

		typeStr := dataTypeName(dataType)
		var attrs []string
		if pkFlag {
			attrs = append(attrs, "PK")
		}
		if notNullFlag {
			attrs = append(attrs, "NOT NULL")
		}
		if ordinal != uint16(i) {
			attrs = append(attrs, fmt.Sprintf("ord=%d", ordinal))
		}

		col := fmt.Sprintf("%s %s", colName, typeStr)
		if len(attrs) > 0 {
			col += fmt.Sprintf(" [%s]", strings.Join(attrs, ", "))
		}
		cols = append(cols, col)
	}

	return fmt.Sprintf("table=%s, columns=[%s]", tableName, strings.Join(cols, ", "))
}

func decodeDropTable(data []byte) string {
	tableName, _, err := decodeString(data)
	if err != nil {
		return fmt.Sprintf("[error: %v]", err)
	}
	return fmt.Sprintf("table=%s", tableName)
}

func decodeInsert(data []byte) string {
	tableName, rest, err := decodeString(data)
	if err != nil {
		return fmt.Sprintf("[error: %v]", err)
	}
	if len(rest) < 8 {
		return "[truncated row ID]"
	}
	rowID := int64(binary.BigEndian.Uint64(rest[:8]))
	values, _, err := decodeValues(rest[8:])
	if err != nil {
		return fmt.Sprintf("[error decoding values: %v]", err)
	}
	return fmt.Sprintf("table=%s, rowID=%d, values=%s", tableName, rowID, formatValues(values))
}

func decodeInsertBatch(data []byte) string {
	tableName, rest, err := decodeString(data)
	if err != nil {
		return fmt.Sprintf("[error: %v]", err)
	}
	if len(rest) < 2 {
		return "[truncated count]"
	}
	count := binary.BigEndian.Uint16(rest[:2])
	rest = rest[2:]

	var rows []string
	for i := 0; i < int(count); i++ {
		if len(rest) < 8 {
			return fmt.Sprintf("[truncated row %d ID]", i)
		}
		rowID := int64(binary.BigEndian.Uint64(rest[:8]))
		var values []any
		values, rest, err = decodeValues(rest[8:])
		if err != nil {
			return fmt.Sprintf("[error decoding row %d: %v]", i, err)
		}
		rows = append(rows, fmt.Sprintf("rowID=%d values=%s", rowID, formatValues(values)))
	}

	return fmt.Sprintf("table=%s, rows=[%s]", tableName, strings.Join(rows, " | "))
}

func decodeDelete(data []byte) string {
	tableName, rest, err := decodeString(data)
	if err != nil {
		return fmt.Sprintf("[error: %v]", err)
	}
	if len(rest) < 2 {
		return "[truncated count]"
	}
	count := binary.BigEndian.Uint16(rest[:2])
	rest = rest[2:]

	var ids []string
	for i := 0; i < int(count); i++ {
		if len(rest) < 8 {
			return fmt.Sprintf("[truncated row %d ID]", i)
		}
		id := int64(binary.BigEndian.Uint64(rest[:8]))
		ids = append(ids, strconv.FormatInt(id, 10))
		rest = rest[8:]
	}

	return fmt.Sprintf("table=%s, rowIDs=[%s]", tableName, strings.Join(ids, ", "))
}

func decodeUpdate(data []byte) string {
	tableName, rest, err := decodeString(data)
	if err != nil {
		return fmt.Sprintf("[error: %v]", err)
	}
	if len(rest) < 2 {
		return "[truncated count]"
	}
	count := binary.BigEndian.Uint16(rest[:2])
	rest = rest[2:]

	var updates []string
	for i := 0; i < int(count); i++ {
		if len(rest) < 8 {
			return fmt.Sprintf("[truncated row %d ID]", i)
		}
		rowID := int64(binary.BigEndian.Uint64(rest[:8]))
		var values []any
		values, rest, err = decodeValues(rest[8:])
		if err != nil {
			return fmt.Sprintf("[error decoding row %d: %v]", i, err)
		}
		updates = append(updates, fmt.Sprintf("rowID=%d values=%s", rowID, formatValues(values)))
	}

	return fmt.Sprintf("table=%s, updates=[%s]", tableName, strings.Join(updates, " | "))
}

func decodeAddColumn(data []byte) string {
	tableName, rest, err := decodeString(data)
	if err != nil {
		return fmt.Sprintf("[error: %v]", err)
	}
	colName, r, err := decodeString(rest)
	if err != nil {
		return fmt.Sprintf("[error: %v]", err)
	}
	if len(r) < 5 {
		return "[truncated column def]"
	}
	dataType := r[0]
	pkFlag := r[1] != 0
	notNullFlag := r[2] != 0
	ordinal := binary.BigEndian.Uint16(r[3:5])

	typeStr := dataTypeName(dataType)
	var attrs []string
	if pkFlag {
		attrs = append(attrs, "PK")
	}
	if notNullFlag {
		attrs = append(attrs, "NOT NULL")
	}

	details := fmt.Sprintf("%s %s ord=%d", colName, typeStr, ordinal)
	if len(attrs) > 0 {
		details += fmt.Sprintf(" [%s]", strings.Join(attrs, ", "))
	}

	return fmt.Sprintf("table=%s, column=%s", tableName, details)
}

func decodeDropColumn(data []byte) string {
	tableName, rest, err := decodeString(data)
	if err != nil {
		return fmt.Sprintf("[error: %v]", err)
	}
	colName, _, err := decodeString(rest)
	if err != nil {
		return fmt.Sprintf("[error: %v]", err)
	}
	return fmt.Sprintf("table=%s, column=%s", tableName, colName)
}

func decodeCreateIndex(data []byte) string {
	tableName, rest, err := decodeString(data)
	if err != nil {
		return fmt.Sprintf("[error: %v]", err)
	}
	idxName, r, err := decodeString(rest)
	if err != nil {
		return fmt.Sprintf("[error: %v]", err)
	}
	colName, r, err := decodeString(r)
	if err != nil {
		return fmt.Sprintf("[error: %v]", err)
	}
	if len(r) < 1 {
		return "[truncated unique flag]"
	}
	unique := r[0] != 0

	if unique {
		return fmt.Sprintf("table=%s, index=%s, column=%s, UNIQUE", tableName, idxName, colName)
	}
	return fmt.Sprintf("table=%s, index=%s, column=%s", tableName, idxName, colName)
}

func decodeDropIndex(data []byte) string {
	tableName, rest, err := decodeString(data)
	if err != nil {
		return fmt.Sprintf("[error: %v]", err)
	}
	idxName, _, err := decodeString(rest)
	if err != nil {
		return fmt.Sprintf("[error: %v]", err)
	}
	return fmt.Sprintf("table=%s, index=%s", tableName, idxName)
}

// decodeString reads a uint16-length-prefixed string
func decodeString(data []byte) (string, []byte, error) {
	if len(data) < 2 {
		return "", nil, fmt.Errorf("truncated string length")
	}
	n := binary.BigEndian.Uint16(data[:2])
	data = data[2:]
	if len(data) < int(n) {
		return "", nil, fmt.Errorf("truncated string value")
	}
	return string(data[:n]), data[n:], nil
}

// decodeValues reads a uint16 count and that many values
func decodeValues(data []byte) ([]any, []byte, error) {
	if len(data) < 2 {
		return nil, nil, fmt.Errorf("truncated value count")
	}
	count := binary.BigEndian.Uint16(data[:2])
	data = data[2:]

	values := make([]any, count)
	var err error
	for i := range values {
		values[i], data, err = decodeValue(data)
		if err != nil {
			return nil, nil, fmt.Errorf("value[%d]: %w", i, err)
		}
	}
	return values, data, nil
}

// decodeValue reads a single value
func decodeValue(data []byte) (any, []byte, error) {
	if len(data) == 0 {
		return nil, nil, fmt.Errorf("empty value data")
	}
	tag := data[0]
	data = data[1:]

	switch tag {
	case tagNull:
		return nil, data, nil
	case tagInteger:
		if len(data) < 8 {
			return nil, nil, fmt.Errorf("truncated integer")
		}
		v := int64(binary.BigEndian.Uint64(data[:8]))
		return v, data[8:], nil
	case tagText:
		if len(data) < 2 {
			return nil, nil, fmt.Errorf("truncated text length")
		}
		n := binary.BigEndian.Uint16(data[:2])
		data = data[2:]
		if len(data) < int(n) {
			return nil, nil, fmt.Errorf("truncated text")
		}
		return string(data[:n]), data[n:], nil
	case tagBoolean:
		if len(data) < 1 {
			return nil, nil, fmt.Errorf("truncated boolean")
		}
		return data[0] != 0, data[1:], nil
	case tagFloat:
		if len(data) < 8 {
			return nil, nil, fmt.Errorf("truncated float")
		}
		bits := binary.BigEndian.Uint64(data[:8])
		return math.Float64frombits(bits), data[8:], nil
	case tagTimestamp:
		if len(data) < 8 {
			return nil, nil, fmt.Errorf("truncated timestamp")
		}
		usec := int64(binary.BigEndian.Uint64(data[:8]))
		return time.UnixMicro(usec).UTC(), data[8:], nil
	default:
		return nil, nil, fmt.Errorf("unknown tag %d", tag)
	}
}

func dataTypeName(t byte) string {
	switch t {
	case typeInteger:
		return "INTEGER"
	case typeText:
		return "TEXT"
	case typeBoolean:
		return "BOOLEAN"
	case typeTimestamp:
		return "TIMESTAMP"
	case typeFloat:
		return "FLOAT"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", t)
	}
}

func formatValues(values []any) string {
	if len(values) == 0 {
		return "[]"
	}
	var parts []string
	for _, v := range values {
		parts = append(parts, formatValue(v))
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func formatValue(v any) string {
	if v == nil {
		return "NULL"
	}
	switch val := v.(type) {
	case int64:
		return strconv.FormatInt(val, 10)
	case float64:
		s := strconv.FormatFloat(val, 'f', -1, 64)
		return s
	case string:
		// Truncate long strings for display
		if len(val) > 50 {
			return fmt.Sprintf("%q...(%d bytes)", val[:50], len(val))
		}
		return fmt.Sprintf("%q", val)
	case bool:
		if val {
			return "TRUE"
		}
		return "FALSE"
	case time.Time:
		return val.Format(time.RFC3339Nano)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func filterEntries(entries []Entry, opFilter string) []Entry {
	// Normalize filter
	opFilter = strings.ToLower(strings.ReplaceAll(opFilter, "_", "-"))

	var filtered []Entry
	for _, e := range entries {
		opName := strings.ToLower(strings.ReplaceAll(e.OpName, "_", "-"))
		if strings.Contains(opName, opFilter) {
			filtered = append(filtered, e)
		}
	}
	return filtered
}

func printFileInfo(path string, version uint16, entryCount int) {
	fmt.Println(strings.Repeat("=", 70))
	fmt.Printf("WAL File: %s\n", path)
	fmt.Printf("Version:  %d\n", version)
	fmt.Printf("Entries:  %d\n", entryCount)
	fmt.Println(strings.Repeat("=", 70))
}

func printEntry(e *Entry, rawMode bool) {
	crcStatus := "OK"
	if !e.CRCValid {
		crcStatus = "FAIL"
	}

	if rawMode {
		fmt.Printf("\n[%d] Offset: %d | Op: %s | CRC: %s\n",
			e.Number, e.Offset, e.OpName, crcStatus)
		fmt.Printf("Payload (%d bytes):\n%s\n",
			len(e.Payload), hex.Dump(e.Payload))
	} else {
		fmt.Printf("\n[%d] %s | %s | CRC:%s\n",
			e.Number, e.OpName, e.Details, crcStatus)
	}
}

func runPager(entries []Entry, pageSize int) {
	totalEntries := len(entries)
	totalPages := (totalEntries + pageSize - 1) / pageSize
	if totalPages == 0 {
		totalPages = 1
	}
	currentPage := 0

	reader := bufio.NewReader(os.Stdin)

	for {
		// Clear screen (works on Unix-like systems)
		fmt.Print("\033[H\033[2J")

		// Print header
		fmt.Printf("Page %d/%d (entries %d-%d of %d)\n",
			currentPage+1, totalPages,
			currentPage*pageSize+1,
			min((currentPage+1)*pageSize, totalEntries),
			totalEntries)
		fmt.Println(strings.Repeat("-", 70))

		// Print entries for this page
		start := currentPage * pageSize
		end := min(start+pageSize, totalEntries)
		for i := start; i < end; i++ {
			printEntry(&entries[i], false)
		}

		// Print footer with help
		fmt.Println()
		fmt.Println(strings.Repeat("-", 70))
		fmt.Print("Commands: n/next, p/prev, g <num>/goto, q/quit: ")

		// Read command
		input, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		input = strings.TrimSpace(strings.ToLower(input))

		parts := strings.Fields(input)
		if len(parts) == 0 {
			continue
		}

		switch parts[0] {
		case "n", "next", " ":
			if currentPage < totalPages-1 {
				currentPage++
			}
		case "p", "prev", "b":
			if currentPage > 0 {
				currentPage--
			}
		case "g", "goto":
			if len(parts) < 2 {
				fmt.Println("Usage: g <entry-number>")
				fmt.Print("Press Enter to continue...")
				reader.ReadString('\n')
				continue
			}
			n, err := strconv.Atoi(parts[1])
			if err != nil || n < 1 || n > totalEntries {
				fmt.Printf("Invalid entry number. Must be 1-%d\n", totalEntries)
				fmt.Print("Press Enter to continue...")
				reader.ReadString('\n')
				continue
			}
			currentPage = (n - 1) / pageSize
		case "q", "quit", "exit":
			return
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
