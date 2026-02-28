package executor

import (
	"fmt"
	"strconv"
)

func (e *Executor) execShowMemory(tr *Trace) (*Result, error) {
	infos := e.engine.MemoryUsage()

	columns := []Column{
		{Name: "table", TypeOID: OIDText, TypeSize: -1},
		{Name: "type", TypeOID: OIDText, TypeSize: -1},
		{Name: "name", TypeOID: OIDText, TypeSize: -1},
		{Name: "size_bytes", TypeOID: OIDInt8, TypeSize: 8},
		{Name: "size_human", TypeOID: OIDText, TypeSize: -1},
	}

	var rows [][][]byte
	var totalBytes int64

	for _, info := range infos {
		// Table row data.
		totalBytes += info.RowBytes
		rows = append(rows, [][]byte{
			[]byte(info.TableName),
			[]byte("table"),
			[]byte(info.TableName),
			[]byte(strconv.FormatInt(info.RowBytes, 10)),
			[]byte(humanBytes(info.RowBytes)),
		})

		// Primary key index.
		if info.PKIndex != nil {
			totalBytes += info.PKIndex.Bytes
			rows = append(rows, [][]byte{
				[]byte(info.TableName),
				[]byte(info.PKIndex.Type),
				[]byte(info.PKIndex.Name),
				[]byte(strconv.FormatInt(info.PKIndex.Bytes, 10)),
				[]byte(humanBytes(info.PKIndex.Bytes)),
			})
		}

		// Secondary indexes.
		for _, idx := range info.Indexes {
			totalBytes += idx.Bytes
			rows = append(rows, [][]byte{
				[]byte(info.TableName),
				[]byte(idx.Type),
				[]byte(idx.Name),
				[]byte(strconv.FormatInt(idx.Bytes, 10)),
				[]byte(humanBytes(idx.Bytes)),
			})
		}
	}

	// Total row.
	rows = append(rows, [][]byte{
		nil, // empty table name
		[]byte("total"),
		nil, // empty name
		[]byte(strconv.FormatInt(totalBytes, 10)),
		[]byte(humanBytes(totalBytes)),
	})

	return &Result{
		Columns: columns,
		Rows:    rows,
		Tag:     fmt.Sprintf("SHOW MEMORY %d", len(rows)),
	}, nil
}

func humanBytes(b int64) string {
	const (
		kb = 1024
		mb = 1024 * kb
		gb = 1024 * mb
	)
	switch {
	case b >= gb:
		return fmt.Sprintf("%.1f GB", float64(b)/float64(gb))
	case b >= mb:
		return fmt.Sprintf("%.1f MB", float64(b)/float64(mb))
	case b >= kb:
		return fmt.Sprintf("%.1f KB", float64(b)/float64(kb))
	default:
		return fmt.Sprintf("%d B", b)
	}
}
