package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// engine is the concrete storage engine implementation. It writes every
// mutation to the WAL before applying it to the in-memory heap. On startup
// the WAL is replayed to reconstruct the full in-memory state.
//
// Concurrency: a sync.RWMutex provides single-writer / multi-reader
// access. Write operations take the write lock; read operations take the
// read lock. Scan returns a snapshot iterator that is safe to use after
// the lock is released.
type engine struct {
	mu      sync.RWMutex
	catalog *catalog
	heaps   map[string]*tableHeap
	wal     *WAL
}

// Open creates or opens a storage engine rooted at dataDir. It replays
// the WAL to restore state from a previous run and returns a ready-to-use
// Engine. If the WAL file needs migration and migrate is false, a
// WALMigrationNeededError is returned.
func Open(dataDir string, migrate bool) (Engine, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	walPath := filepath.Join(dataDir, "wal.dat")
	wal, err := OpenWAL(walPath, migrate)
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
	e.mu.Lock()
	defer e.mu.Unlock()
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
		return &TableNotFoundError{Name: table}
	}
	return heap.insertWithID(rowID, values)
}

func (e *engine) OnDelete(table string, rowIDs []int64) error {
	heap, ok := e.heaps[table]
	if !ok {
		return &TableNotFoundError{Name: table}
	}
	heap.deleteRows(rowIDs)
	return nil
}

func (e *engine) OnUpdate(table string, updates []rowUpdate) error {
	heap, ok := e.heaps[table]
	if !ok {
		return &TableNotFoundError{Name: table}
	}
	for _, u := range updates {
		if err := heap.updateRow(u.RowID, u.Values); err != nil {
			return err
		}
	}
	return nil
}

// -------------------------------------------------------------------------
// Engine interface — WAL-first, then apply to memory
// -------------------------------------------------------------------------

func (e *engine) CreateTable(name string, columns []ColumnDef) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.catalog.getTable(name); exists {
		return &TableExistsError{Name: name}
	}
	if err := e.wal.WriteCreateTable(name, columns); err != nil {
		return fmt.Errorf("WAL: %w", err)
	}
	return e.OnCreateTable(name, columns)
}

func (e *engine) DropTable(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, ok := e.catalog.getTable(name); !ok {
		return &TableNotFoundError{Name: name}
	}
	if err := e.wal.WriteDropTable(name); err != nil {
		return fmt.Errorf("WAL: %w", err)
	}
	return e.OnDropTable(name)
}

func (e *engine) GetTable(name string) (*TableDef, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.catalog.getTable(name)
}

func (e *engine) ListTables() []*TableDef {
	e.mu.RLock()
	defer e.mu.RUnlock()

	defs := make([]*TableDef, 0, len(e.catalog.tables))
	for _, def := range e.catalog.tables {
		defs = append(defs, def)
	}
	return defs
}

func (e *engine) Insert(table string, columns []string, values [][]any) (int64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	heap, ok := e.heaps[table]
	if !ok {
		return 0, &TableNotFoundError{Name: table}
	}

	// Resolve all rows first so we can pre-validate PK uniqueness.
	resolvedRows := make([][]any, 0, len(values))
	for _, vals := range values {
		fullRow, err := e.resolveInsertRow(heap, columns, vals)
		if err != nil {
			return 0, err
		}
		resolvedRows = append(resolvedRows, fullRow)
	}

	// Pre-validate PK uniqueness for all rows before writing any WAL entries.
	if heap.pkCol >= 0 {
		seen := make(map[any]bool, len(resolvedRows))
		for _, fullRow := range resolvedRows {
			key := fullRow[heap.pkCol]
			if key == nil {
				return 0, &UniqueViolationError{
					Table:  table,
					Column: heap.def.Columns[heap.pkCol].Name,
				}
			}
			if seen[key] {
				return 0, &UniqueViolationError{
					Table:  table,
					Column: heap.def.Columns[heap.pkCol].Name,
					Value:  key,
				}
			}
			seen[key] = true
			if _, exists := heap.pkIdx.Get(key); exists {
				return 0, &UniqueViolationError{
					Table:  table,
					Column: heap.def.Columns[heap.pkCol].Name,
					Value:  key,
				}
			}
		}
	}

	var count int64
	for _, fullRow := range resolvedRows {
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
	e.mu.RLock()
	defer e.mu.RUnlock()

	heap, ok := e.heaps[table]
	if !ok {
		return nil, &TableNotFoundError{Name: table}
	}
	return heap.scan(), nil
}

func (e *engine) Update(table string, sets map[string]any, filter func(Row) bool) (int64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	heap, ok := e.heaps[table]
	if !ok {
		return 0, &TableNotFoundError{Name: table}
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
				return 0, &ColumnNotFoundError{Column: colName, Table: heap.def.Name}
			}
			newValues[idx] = newVal
		}
		updates = append(updates, rowUpdate{RowID: id, Values: newValues})
	}

	if len(updates) == 0 {
		return 0, nil
	}

	// Pre-validate PK uniqueness before WAL write.
	if heap.pkCol >= 0 {
		pkColName := heap.def.Columns[heap.pkCol].Name
		if _, changing := sets[pkColName]; changing {
			// Collect all row IDs being updated for fast lookup.
			updatingIDs := make(map[int64]bool, len(updates))
			for _, u := range updates {
				updatingIDs[u.RowID] = true
			}

			seen := make(map[any]bool, len(updates))
			for _, u := range updates {
				newKey := u.Values[heap.pkCol]
				if newKey == nil {
					return 0, &UniqueViolationError{Table: table, Column: pkColName}
				}
				if seen[newKey] {
					return 0, &UniqueViolationError{Table: table, Column: pkColName, Value: newKey}
				}
				seen[newKey] = true
				// Check against existing rows that are NOT being updated.
				if existingID, found := heap.pkIdx.Get(newKey); found && !updatingIDs[existingID] {
					return 0, &UniqueViolationError{Table: table, Column: pkColName, Value: newKey}
				}
			}
		}
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
	e.mu.Lock()
	defer e.mu.Unlock()

	heap, ok := e.heaps[table]
	if !ok {
		return 0, &TableNotFoundError{Name: table}
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

func (e *engine) LookupByPK(table string, value any) (*Row, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	heap, ok := e.heaps[table]
	if !ok {
		return nil, &TableNotFoundError{Name: table}
	}
	row, ok := heap.lookupByPK(value)
	if !ok {
		return nil, nil
	}
	// Return a copy to avoid data races.
	vals := make([]any, len(row.Values))
	copy(vals, row.Values)
	return &Row{ID: row.ID, Values: vals}, nil
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
			return nil, &ValueCountError{Expected: len(def.Columns), Got: len(values)}
		}
		return values, nil
	}

	row := make([]any, len(def.Columns))
	for i, colName := range columns {
		idx := heap.columnIndex(colName)
		if idx < 0 {
			return nil, &ColumnNotFoundError{Column: colName, Table: def.Name}
		}
		if i >= len(values) {
			return nil, &ValueCountError{Expected: len(columns), Got: len(values)}
		}
		row[idx] = values[i]
	}
	return row, nil
}
