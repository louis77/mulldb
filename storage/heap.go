package storage

import (
	"sort"

	"mulldb/storage/index"
)

// tableHeap holds the in-memory row data for a single table.
// It is populated during WAL replay and modified by engine operations.
type tableHeap struct {
	def         TableDef
	rows        map[int64][]any // rowID → column values
	nextID      int64           // next ID to assign on insert
	pkIdx       index.Index     // nil if no primary key
	pkCol       int             // column index of PK, or -1
	secondaries []secondaryIdx  // secondary indexes
}

// secondaryIdx tracks a single secondary index on the table.
type secondaryIdx struct {
	def    IndexDef
	colOrd int               // ordinal of the indexed column
	unique index.Index       // non-nil for UNIQUE indexes
	multi  index.MultiIndex  // non-nil for non-unique indexes
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
	// Maintain secondary indexes.
	for i := range h.secondaries {
		si := &h.secondaries[i]
		key := RowValue(values, si.colOrd)
		if key == nil {
			continue // NULLs are not indexed
		}
		if si.unique != nil {
			if !si.unique.Put(key, id) {
				// Roll back: remove from PK index and earlier secondary indexes.
				if h.pkIdx != nil {
					h.pkIdx.Delete(RowValue(values, h.pkCol))
				}
				for j := 0; j < i; j++ {
					sj := &h.secondaries[j]
					k := RowValue(values, sj.colOrd)
					if k == nil {
						continue
					}
					if sj.unique != nil {
						sj.unique.Delete(k)
					} else {
						sj.multi.Delete(k, id)
					}
				}
				return &UniqueViolationError{
					Table:  h.def.Name,
					Column: si.def.Column,
					Value:  key,
					Index:  si.def.Name,
				}
			}
		} else {
			si.multi.Put(key, id)
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
		vals, ok := h.rows[id]
		if !ok {
			continue
		}
		if h.pkIdx != nil {
			h.pkIdx.Delete(RowValue(vals, h.pkCol))
		}
		for i := range h.secondaries {
			si := &h.secondaries[i]
			key := RowValue(vals, si.colOrd)
			if key == nil {
				continue
			}
			if si.unique != nil {
				si.unique.Delete(key)
			} else {
				si.multi.Delete(key, id)
			}
		}
		delete(h.rows, id)
	}
}

// updateRow replaces the values for a given row ID. Returns an error if
// the update would violate a PK or unique index constraint.
func (h *tableHeap) updateRow(id int64, values []any) error {
	oldVals := h.rows[id]

	if h.pkIdx != nil {
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

	// Update secondary indexes.
	for i := range h.secondaries {
		si := &h.secondaries[i]
		oldKey := RowValue(oldVals, si.colOrd)
		newKey := RowValue(values, si.colOrd)
		if CompareValues(oldKey, newKey) == 0 {
			continue // value unchanged
		}
		// Remove old entry.
		if oldKey != nil {
			if si.unique != nil {
				si.unique.Delete(oldKey)
			} else {
				si.multi.Delete(oldKey, id)
			}
		}
		// Insert new entry.
		if newKey != nil {
			if si.unique != nil {
				if !si.unique.Put(newKey, id) {
					// Restore old entry on failure.
					if oldKey != nil {
						si.unique.Put(oldKey, id)
					}
					// Roll back earlier secondary index changes.
					for j := 0; j < i; j++ {
						sj := &h.secondaries[j]
						ok := RowValue(oldVals, sj.colOrd)
						nk := RowValue(values, sj.colOrd)
						if CompareValues(ok, nk) == 0 {
							continue
						}
						// Reverse: remove new, restore old.
						if nk != nil {
							if sj.unique != nil {
								sj.unique.Delete(nk)
							} else {
								sj.multi.Delete(nk, id)
							}
						}
						if ok != nil {
							if sj.unique != nil {
								sj.unique.Put(ok, id)
							} else {
								sj.multi.Put(ok, id)
							}
						}
					}
					// Roll back PK change if it was modified.
					if h.pkIdx != nil {
						pkOld := RowValue(oldVals, h.pkCol)
						pkNew := RowValue(values, h.pkCol)
						if CompareValues(pkOld, pkNew) != 0 {
							h.pkIdx.Delete(pkNew)
							h.pkIdx.Put(pkOld, id)
						}
					}
					return &UniqueViolationError{
						Table:  h.def.Name,
						Column: si.def.Column,
						Value:  newKey,
						Index:  si.def.Name,
					}
				}
			} else {
				si.multi.Put(newKey, id)
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

// buildSecondaryIndexes populates all secondary indexes from the current rows.
// Called after WAL replay when the index definitions are known but the
// in-memory index trees are empty.
func (h *tableHeap) buildSecondaryIndexes() error {
	for i := range h.secondaries {
		si := &h.secondaries[i]
		for id, vals := range h.rows {
			key := RowValue(vals, si.colOrd)
			if key == nil {
				continue
			}
			if si.unique != nil {
				if !si.unique.Put(key, id) {
					return &UniqueViolationError{
						Table:  h.def.Name,
						Column: si.def.Column,
						Value:  key,
						Index:  si.def.Name,
					}
				}
			} else {
				si.multi.Put(key, id)
			}
		}
	}
	return nil
}

// addSecondaryIndex builds a new secondary index from the existing rows and
// adds it to the heap. Returns an error if a UNIQUE index has duplicates.
func (h *tableHeap) addSecondaryIndex(def IndexDef) error {
	colOrd := h.columnIndex(def.Column)
	if colOrd < 0 {
		return &ColumnNotFoundError{Column: def.Column, Table: h.def.Name}
	}
	si := secondaryIdx{def: def, colOrd: colOrd}
	if def.Unique {
		si.unique = index.NewBTree(CompareValues)
	} else {
		si.multi = index.NewMultiBTree(CompareValues)
	}
	// Populate from existing rows.
	for id, vals := range h.rows {
		key := RowValue(vals, colOrd)
		if key == nil {
			continue
		}
		if si.unique != nil {
			if !si.unique.Put(key, id) {
				return &UniqueViolationError{
					Table:  h.def.Name,
					Column: def.Column,
					Value:  key,
					Index:  def.Name,
				}
			}
		} else {
			si.multi.Put(key, id)
		}
	}
	h.secondaries = append(h.secondaries, si)
	return nil
}

// removeSecondaryIndex removes a secondary index by name.
func (h *tableHeap) removeSecondaryIndex(name string) {
	for i, si := range h.secondaries {
		if si.def.Name == name {
			h.secondaries = append(h.secondaries[:i], h.secondaries[i+1:]...)
			return
		}
	}
}

// lookupByIndex returns all rows matching a value in the named secondary index.
func (h *tableHeap) lookupByIndex(name string, value any) []Row {
	for i := range h.secondaries {
		si := &h.secondaries[i]
		if si.def.Name != name {
			continue
		}
		var ids []int64
		if si.unique != nil {
			id, ok := si.unique.Get(value)
			if ok {
				ids = []int64{id}
			}
		} else {
			ids = si.multi.GetAll(value)
		}
		rows := make([]Row, 0, len(ids))
		for _, id := range ids {
			if vals, ok := h.rows[id]; ok {
				rows = append(rows, Row{ID: id, Values: vals})
			}
		}
		return rows
	}
	return nil
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
