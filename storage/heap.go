package storage

import (
	"sort"

	"mulldb/storage/index"
)

// tableHeap holds the in-memory row data for a single table.
// It is populated during WAL replay and modified by engine operations.
type tableHeap struct {
	def    TableDef
	rows   map[int64][]any // rowID → column values
	nextID int64           // next ID to assign on insert
	pkIdx  index.Index     // nil if no primary key
	pkCol  int             // column index of PK, or -1
}

func newTableHeap(def TableDef) *tableHeap {
	h := &tableHeap{
		def:    def,
		rows:   make(map[int64][]any),
		nextID: 1,
		pkCol:  def.PrimaryKeyColumn(),
	}
	if h.pkCol >= 0 {
		h.pkIdx = index.NewBTree(CompareValues)
	}
	return h
}

// allocateID reserves and returns the next row ID.
func (h *tableHeap) allocateID() int64 {
	id := h.nextID
	h.nextID++
	return id
}

// pkColumnName returns the name of the primary key column, or "".
func (h *tableHeap) pkColumnName() string {
	for _, col := range h.def.Columns {
		if col.Ordinal == h.pkCol {
			return col.Name
		}
	}
	return ""
}

// insertWithID stores a row with a specific ID (used by both live inserts
// and WAL replay). Returns an error if the row violates a PK constraint.
func (h *tableHeap) insertWithID(id int64, values []any) error {
	if h.pkIdx != nil {
		key := RowValue(values, h.pkCol)
		if key == nil {
			return &UniqueViolationError{
				Table:  h.def.Name,
				Column: h.pkColumnName(),
			}
		}
		if !h.pkIdx.Put(key, id) {
			return &UniqueViolationError{
				Table:  h.def.Name,
				Column: h.pkColumnName(),
				Value:  key,
			}
		}
	}
	row := make([]any, len(values))
	copy(row, values)
	h.rows[id] = row
	if id >= h.nextID {
		h.nextID = id + 1
	}
	return nil
}

// deleteRows removes the rows with the given IDs.
func (h *tableHeap) deleteRows(ids []int64) {
	for _, id := range ids {
		if h.pkIdx != nil {
			if vals, ok := h.rows[id]; ok {
				h.pkIdx.Delete(RowValue(vals, h.pkCol))
			}
		}
		delete(h.rows, id)
	}
}

// updateRow replaces the values for a given row ID. Returns an error if
// the update would violate a PK constraint.
func (h *tableHeap) updateRow(id int64, values []any) error {
	if h.pkIdx != nil {
		oldVals := h.rows[id]
		oldKey := RowValue(oldVals, h.pkCol)
		newKey := RowValue(values, h.pkCol)
		if CompareValues(oldKey, newKey) != 0 {
			if newKey == nil {
				return &UniqueViolationError{
					Table:  h.def.Name,
					Column: h.pkColumnName(),
				}
			}
			// PK value is changing: remove old, try inserting new.
			h.pkIdx.Delete(oldKey)
			if !h.pkIdx.Put(newKey, id) {
				// Restore old entry on failure.
				h.pkIdx.Put(oldKey, id)
				return &UniqueViolationError{
					Table:  h.def.Name,
					Column: h.pkColumnName(),
					Value:  newKey,
				}
			}
		}
	}
	row := make([]any, len(values))
	copy(row, values)
	h.rows[id] = row
	return nil
}

// lookupByPK returns the row matching the given PK value, or false if not found.
func (h *tableHeap) lookupByPK(value any) (*Row, bool) {
	if h.pkIdx == nil {
		return nil, false
	}
	rowID, ok := h.pkIdx.Get(value)
	if !ok {
		return nil, false
	}
	vals, ok := h.rows[rowID]
	if !ok {
		return nil, false
	}
	return &Row{ID: rowID, Values: vals}, true
}

// scan returns a RowIterator over all rows in the table.
// Rows are returned in insertion order (ascending row ID).
func (h *tableHeap) scan() RowIterator {
	rows := make([]Row, 0, len(h.rows))
	for id, values := range h.rows {
		rows = append(rows, Row{ID: id, Values: values})
	}
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].ID < rows[j].ID
	})
	return &sliceIterator{rows: rows}
}

// columnIndex returns the ordinal of the named column, or -1.
func (h *tableHeap) columnIndex(name string) int {
	for _, col := range h.def.Columns {
		if col.Name == name {
			return col.Ordinal
		}
	}
	return -1
}

// sliceIterator is a RowIterator backed by an in-memory slice.
type sliceIterator struct {
	rows []Row
	pos  int
}

func (it *sliceIterator) Next() (Row, bool) {
	if it.pos >= len(it.rows) {
		return Row{}, false
	}
	row := it.rows[it.pos]
	it.pos++
	return row, true
}

func (it *sliceIterator) Close() error { return nil }
