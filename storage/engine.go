package storage

import (
	"fmt"
	"os"
	"path/filepath"
)

// engine is the concrete storage engine implementation. It writes every
// mutation to the WAL before applying it to the in-memory heap. On startup
// the WAL is replayed to reconstruct the full in-memory state.
type engine struct {
	catalog *catalog
	heaps   map[string]*tableHeap
	wal     *WAL
}

// Open creates or opens a storage engine rooted at dataDir. It replays
// the WAL to restore state from a previous run and returns a ready-to-use
// Engine.
func Open(dataDir string) (Engine, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	walPath := filepath.Join(dataDir, "wal.dat")
	wal, err := OpenWAL(walPath)
	if err != nil {
		return nil, fmt.Errorf("open WAL: %w", err)
	}

	e := &engine{
		catalog: newCatalog(),
		heaps:   make(map[string]*tableHeap),
		wal:     wal,
	}

	if err := wal.Replay(e); err != nil {
		wal.Close()
		return nil, fmt.Errorf("replay WAL: %w", err)
	}

	return e, nil
}

// Close closes the WAL file.
func (e *engine) Close() error {
	return e.wal.Close()
}

// -------------------------------------------------------------------------
// ReplayHandler — used during WAL replay to rebuild in-memory state
// -------------------------------------------------------------------------

func (e *engine) OnCreateTable(name string, columns []ColumnDef) error {
	if err := e.catalog.createTable(name, columns); err != nil {
		return err
	}
	e.heaps[name] = newTableHeap(*e.catalog.tables[name])
	return nil
}

func (e *engine) OnDropTable(name string) error {
	if err := e.catalog.dropTable(name); err != nil {
		return err
	}
	delete(e.heaps, name)
	return nil
}

func (e *engine) OnInsert(table string, rowID int64, values []any) error {
	heap, ok := e.heaps[table]
	if !ok {
		return fmt.Errorf("table %q not found", table)
	}
	heap.insertWithID(rowID, values)
	return nil
}

func (e *engine) OnDelete(table string, rowIDs []int64) error {
	heap, ok := e.heaps[table]
	if !ok {
		return fmt.Errorf("table %q not found", table)
	}
	heap.deleteRows(rowIDs)
	return nil
}

func (e *engine) OnUpdate(table string, updates []rowUpdate) error {
	heap, ok := e.heaps[table]
	if !ok {
		return fmt.Errorf("table %q not found", table)
	}
	for _, u := range updates {
		heap.updateRow(u.RowID, u.Values)
	}
	return nil
}

// -------------------------------------------------------------------------
// Engine interface — WAL-first, then apply to memory
// -------------------------------------------------------------------------

func (e *engine) CreateTable(name string, columns []ColumnDef) error {
	if _, exists := e.catalog.getTable(name); exists {
		return fmt.Errorf("table %q already exists", name)
	}
	if err := e.wal.WriteCreateTable(name, columns); err != nil {
		return fmt.Errorf("WAL: %w", err)
	}
	return e.OnCreateTable(name, columns)
}

func (e *engine) DropTable(name string) error {
	if _, ok := e.catalog.getTable(name); !ok {
		return fmt.Errorf("table %q does not exist", name)
	}
	if err := e.wal.WriteDropTable(name); err != nil {
		return fmt.Errorf("WAL: %w", err)
	}
	return e.OnDropTable(name)
}

func (e *engine) GetTable(name string) (*TableDef, bool) {
	return e.catalog.getTable(name)
}

func (e *engine) Insert(table string, columns []string, values [][]any) (int64, error) {
	heap, ok := e.heaps[table]
	if !ok {
		return 0, fmt.Errorf("table %q does not exist", table)
	}

	var count int64
	for _, vals := range values {
		fullRow, err := e.resolveInsertRow(heap, columns, vals)
		if err != nil {
			return count, err
		}

		id := heap.allocateID()
		if err := e.wal.WriteInsert(table, id, fullRow); err != nil {
			return count, fmt.Errorf("WAL: %w", err)
		}
		heap.insertWithID(id, fullRow)
		count++
	}
	return count, nil
}

func (e *engine) Scan(table string) (RowIterator, error) {
	heap, ok := e.heaps[table]
	if !ok {
		return nil, fmt.Errorf("table %q does not exist", table)
	}
	return heap.scan(), nil
}

func (e *engine) Update(table string, sets map[string]any, filter func(Row) bool) (int64, error) {
	heap, ok := e.heaps[table]
	if !ok {
		return 0, fmt.Errorf("table %q does not exist", table)
	}

	var updates []rowUpdate
	for id, values := range heap.rows {
		row := Row{ID: id, Values: values}
		if filter != nil && !filter(row) {
			continue
		}
		newValues := make([]any, len(values))
		copy(newValues, values)
		for colName, newVal := range sets {
			idx := heap.columnIndex(colName)
			if idx < 0 {
				return 0, fmt.Errorf("column %q not found in table %q", colName, heap.def.Name)
			}
			newValues[idx] = newVal
		}
		updates = append(updates, rowUpdate{RowID: id, Values: newValues})
	}

	if len(updates) == 0 {
		return 0, nil
	}

	if err := e.wal.WriteUpdate(table, updates); err != nil {
		return 0, fmt.Errorf("WAL: %w", err)
	}
	for _, u := range updates {
		heap.updateRow(u.RowID, u.Values)
	}
	return int64(len(updates)), nil
}

func (e *engine) Delete(table string, filter func(Row) bool) (int64, error) {
	heap, ok := e.heaps[table]
	if !ok {
		return 0, fmt.Errorf("table %q does not exist", table)
	}

	var ids []int64
	for id, values := range heap.rows {
		row := Row{ID: id, Values: values}
		if filter != nil && !filter(row) {
			continue
		}
		ids = append(ids, id)
	}

	if len(ids) == 0 {
		return 0, nil
	}

	if err := e.wal.WriteDelete(table, ids); err != nil {
		return 0, fmt.Errorf("WAL: %w", err)
	}
	heap.deleteRows(ids)
	return int64(len(ids)), nil
}

// -------------------------------------------------------------------------
// Helpers
// -------------------------------------------------------------------------

// resolveInsertRow maps named columns + values to a full row in column
// order, filling unspecified columns with nil (NULL). When columns is nil
// the values are used directly (must match the table width).
func (e *engine) resolveInsertRow(heap *tableHeap, columns []string, values []any) ([]any, error) {
	def := &heap.def

	if columns == nil {
		if len(values) != len(def.Columns) {
			return nil, fmt.Errorf("expected %d values, got %d", len(def.Columns), len(values))
		}
		return values, nil
	}

	row := make([]any, len(def.Columns))
	for i, colName := range columns {
		idx := heap.columnIndex(colName)
		if idx < 0 {
			return nil, fmt.Errorf("column %q not found in table %q", colName, def.Name)
		}
		if i >= len(values) {
			return nil, fmt.Errorf("not enough values for columns")
		}
		row[idx] = values[i]
	}
	return row, nil
}
