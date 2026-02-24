package storage

// tableHeap holds the in-memory row data for a single table.
// It is populated during WAL replay and modified by engine operations.
type tableHeap struct {
	def    TableDef
	rows   map[int64][]any // rowID → column values
	nextID int64           // next ID to assign on insert
}

func newTableHeap(def TableDef) *tableHeap {
	return &tableHeap{
		def:    def,
		rows:   make(map[int64][]any),
		nextID: 1,
	}
}

// allocateID reserves and returns the next row ID.
func (h *tableHeap) allocateID() int64 {
	id := h.nextID
	h.nextID++
	return id
}

// insertWithID stores a row with a specific ID (used by both live inserts
// and WAL replay).
func (h *tableHeap) insertWithID(id int64, values []any) {
	row := make([]any, len(values))
	copy(row, values)
	h.rows[id] = row
	if id >= h.nextID {
		h.nextID = id + 1
	}
}

// deleteRows removes the rows with the given IDs.
func (h *tableHeap) deleteRows(ids []int64) {
	for _, id := range ids {
		delete(h.rows, id)
	}
}

// updateRow replaces the values for a given row ID.
func (h *tableHeap) updateRow(id int64, values []any) {
	row := make([]any, len(values))
	copy(row, values)
	h.rows[id] = row
}

// scan returns a RowIterator over all rows in the table.
// The iteration order is not guaranteed.
func (h *tableHeap) scan() RowIterator {
	rows := make([]Row, 0, len(h.rows))
	for id, values := range h.rows {
		rows = append(rows, Row{ID: id, Values: values})
	}
	return &sliceIterator{rows: rows}
}

// columnIndex returns the position of the named column, or -1.
func (h *tableHeap) columnIndex(name string) int {
	for i, col := range h.def.Columns {
		if col.Name == name {
			return i
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
