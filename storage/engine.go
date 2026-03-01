package storage

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
)

// tableState holds the per-table mutex, heap, WAL, and a flag indicating
// whether the table has been dropped (used as a race guard for concurrent
// DML during DROP TABLE).
type tableState struct {
	mu      sync.RWMutex
	heap    *tableHeap
	wal     *WAL
	dropped bool
}

// engine is the concrete storage engine implementation. It uses per-table
// WAL files and per-table locking for concurrent access to independent
// tables.
//
// File layout:
//
//	<dataDir>/
//	  catalog.wal          — DDL only: CreateTable / DropTable entries
//	  tables/
//	    <name>.wal         — DML for each table
//
// Locking protocol (catalogMu always before tableState.mu):
//   - CreateTable: catalogMu write lock (hold throughout)
//   - DropTable: catalogMu write lock → table write lock
//   - Insert/Update/Delete: catalogMu read lock (brief) → table write lock
//   - Scan/LookupByPK: catalogMu read lock (brief) → table read lock
//   - GetTable/ListTables: catalogMu read lock only
type engine struct {
	dataDir     string
	catalogMu   sync.RWMutex
	catalog     *catalog
	tableStates map[string]*tableState
	catalogWAL  *WAL
	fsync       atomic.Bool
}

const (
	catalogWALName = "catalog.wal"
	tablesDirName  = "tables"
	legacyWALName  = "wal.dat"
)

// Open creates or opens a storage engine rooted at dataDir. It detects
// whether the data directory uses the legacy single-WAL format or the
// split per-table format.
//
// Detection logic:
//   - wal.dat exists → legacy format (migrate if --migrate, else error)
//   - catalog.wal exists → split format (replay catalog + per-table WALs)
//   - neither exists → fresh database (create split format)
//
// If the WAL file needs migration and migrate is false, a
// WALMigrationNeededError is returned.
func Open(dataDir string, migrate bool) (Engine, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	legacyPath := filepath.Join(dataDir, legacyWALName)
	catalogPath := filepath.Join(dataDir, catalogWALName)
	tablesDir := filepath.Join(dataDir, tablesDirName)

	legacyExists := fileExists(legacyPath)
	catalogExists := fileExists(catalogPath)

	// Legacy single-WAL format detected — migrate or error.
	if legacyExists && !catalogExists {
		if !migrate {
			return nil, &SplitWALMigrationNeededError{}
		}
		// First, handle format version migration (e.g. v1→v2) if needed.
		if err := migrateLegacyWALVersion(legacyPath); err != nil {
			return nil, err
		}
		// Then, split single WAL into per-table WAL files.
		log.Printf("migrating from single WAL to per-table WAL format...")
		if err := migrateToSplitWAL(dataDir); err != nil {
			return nil, fmt.Errorf("split WAL migration: %w", err)
		}
		log.Printf("split WAL migration complete. Original backed up to %s", legacyPath+".bak")
	}

	// Ensure tables directory exists.
	if err := os.MkdirAll(tablesDir, 0755); err != nil {
		return nil, fmt.Errorf("create tables dir: %w", err)
	}

	// Open catalog WAL.
	catWAL, err := OpenWAL(catalogPath, migrate)
	if err != nil {
		return nil, fmt.Errorf("open catalog WAL: %w", err)
	}

	e := &engine{
		dataDir:     dataDir,
		catalog:     newCatalog(),
		tableStates: make(map[string]*tableState),
		catalogWAL:  catWAL,
	}
	e.fsync.Store(true)
	e.catalogWAL.fsync = &e.fsync

	// Phase 1: Replay catalog WAL to learn all table schemas.
	catHandler := &catalogReplayHandler{catalog: e.catalog}
	if err := catWAL.Replay(catHandler); err != nil {
		catWAL.Close()
		return nil, fmt.Errorf("replay catalog WAL: %w", err)
	}

	// Phase 2: For each surviving table, open its WAL and replay DML.
	for name, def := range e.catalog.tables {
		ts, err := e.openTableState(*def, tablesDir, migrate)
		if err != nil {
			e.closeAll()
			return nil, fmt.Errorf("open table %q: %w", name, err)
		}
		e.tableStates[name] = ts
	}

	// Orphan cleanup: remove WAL files for tables not in the catalog
	// (handles crash-during-DROP).
	if err := e.cleanOrphanWALs(tablesDir); err != nil {
		e.closeAll()
		return nil, fmt.Errorf("orphan cleanup: %w", err)
	}

	return e, nil
}

// openTableState opens a table's WAL file and replays it to build the heap.
func (e *engine) openTableState(def TableDef, tablesDir string, migrate bool) (*tableState, error) {
	walPath := filepath.Join(tablesDir, tableFileName(def.Name))
	w, err := OpenWAL(walPath, migrate)
	if err != nil {
		return nil, err
	}

	heap := newTableHeap(def)
	handler := &dmlReplayHandler{tableName: def.Name, heap: heap}
	if err := w.Replay(handler); err != nil {
		w.Close()
		return nil, fmt.Errorf("replay: %w", err)
	}

	// Initialize and populate secondary indexes from the catalog metadata.
	for _, idx := range def.Indexes {
		if err := heap.addSecondaryIndex(idx); err != nil {
			w.Close()
			return nil, fmt.Errorf("build index %q: %w", idx.Name, err)
		}
	}

	w.fsync = &e.fsync
	return &tableState{heap: heap, wal: w}, nil
}

// cleanOrphanWALs scans the tables directory and removes WAL files for
// tables that don't exist in the catalog. This handles the case where a
// crash occurred between writing the DROP TABLE entry to the catalog WAL
// and deleting the table's WAL file.
func (e *engine) cleanOrphanWALs(tablesDir string) error {
	entries, err := os.ReadDir(tablesDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name, err := tableNameFromFile(entry.Name())
		if err != nil {
			continue // skip non-table files
		}
		if _, exists := e.catalog.tables[name]; !exists {
			path := filepath.Join(tablesDir, entry.Name())
			if err := os.Remove(path); err != nil {
				return fmt.Errorf("remove orphan WAL %q: %w", entry.Name(), err)
			}
			log.Printf("removed orphan WAL file for dropped table %q", name)
		}
	}
	return nil
}

// closeAll closes the catalog WAL and all table WALs. Used during error
// cleanup in Open.
func (e *engine) closeAll() {
	for _, ts := range e.tableStates {
		ts.wal.Close()
	}
	e.catalogWAL.Close()
}

// Close closes all WAL files.
func (e *engine) Close() error {
	e.catalogMu.Lock()
	defer e.catalogMu.Unlock()

	var firstErr error
	for _, ts := range e.tableStates {
		if err := ts.wal.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if err := e.catalogWAL.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// getTableState returns the tableState for the named table, or
// TableNotFoundError if it doesn't exist. The caller must hold
// catalogMu (at least read-locked) when calling this.
func (e *engine) getTableState(name string) (*tableState, error) {
	ts, ok := e.tableStates[name]
	if !ok {
		return nil, &TableNotFoundError{Name: name}
	}
	return ts, nil
}

// -------------------------------------------------------------------------
// Replay handlers — used during WAL replay to rebuild in-memory state
// -------------------------------------------------------------------------

// catalogReplayHandler accepts only DDL entries (CreateTable / DropTable).
type catalogReplayHandler struct {
	catalog *catalog
}

func (h *catalogReplayHandler) OnCreateTable(name string, columns []ColumnDef) error {
	return h.catalog.createTable(name, columns)
}

func (h *catalogReplayHandler) OnDropTable(name string) error {
	return h.catalog.dropTable(name)
}

func (h *catalogReplayHandler) OnAddColumn(table string, col ColumnDef) error {
	return h.catalog.addColumn(table, col)
}

func (h *catalogReplayHandler) OnDropColumn(table string, colName string) error {
	return h.catalog.dropColumn(table, colName)
}

func (h *catalogReplayHandler) OnCreateIndex(table string, idx IndexDef) error {
	return h.catalog.createIndex(table, idx)
}

func (h *catalogReplayHandler) OnDropIndex(table string, indexName string) error {
	return h.catalog.dropIndex(table, indexName)
}

func (h *catalogReplayHandler) OnInsert(string, int64, []any) error {
	return fmt.Errorf("unexpected INSERT in catalog WAL")
}

func (h *catalogReplayHandler) OnDelete(string, []int64) error {
	return fmt.Errorf("unexpected DELETE in catalog WAL")
}

func (h *catalogReplayHandler) OnUpdate(string, []rowUpdate) error {
	return fmt.Errorf("unexpected UPDATE in catalog WAL")
}

// dmlReplayHandler accepts only DML entries (Insert/Delete/Update) and
// validates that the table name in each entry matches the expected table.
type dmlReplayHandler struct {
	tableName string
	heap      *tableHeap
}

func (h *dmlReplayHandler) OnCreateTable(string, []ColumnDef) error {
	return fmt.Errorf("unexpected CREATE TABLE in table WAL for %q", h.tableName)
}

func (h *dmlReplayHandler) OnDropTable(string) error {
	return fmt.Errorf("unexpected DROP TABLE in table WAL for %q", h.tableName)
}

func (h *dmlReplayHandler) OnAddColumn(string, ColumnDef) error {
	return fmt.Errorf("unexpected ADD COLUMN in table WAL for %q", h.tableName)
}

func (h *dmlReplayHandler) OnDropColumn(string, string) error {
	return fmt.Errorf("unexpected DROP COLUMN in table WAL for %q", h.tableName)
}

func (h *dmlReplayHandler) OnCreateIndex(string, IndexDef) error {
	return fmt.Errorf("unexpected CREATE INDEX in table WAL for %q", h.tableName)
}

func (h *dmlReplayHandler) OnDropIndex(string, string) error {
	return fmt.Errorf("unexpected DROP INDEX in table WAL for %q", h.tableName)
}

func (h *dmlReplayHandler) OnInsert(table string, rowID int64, values []any) error {
	if table != h.tableName {
		return fmt.Errorf("table name mismatch in WAL: got %q, want %q", table, h.tableName)
	}
	return h.heap.insertWithID(rowID, values)
}

func (h *dmlReplayHandler) OnDelete(table string, rowIDs []int64) error {
	if table != h.tableName {
		return fmt.Errorf("table name mismatch in WAL: got %q, want %q", table, h.tableName)
	}
	h.heap.deleteRows(rowIDs)
	return nil
}

func (h *dmlReplayHandler) OnUpdate(table string, updates []rowUpdate) error {
	if table != h.tableName {
		return fmt.Errorf("table name mismatch in WAL: got %q, want %q", table, h.tableName)
	}
	for _, u := range updates {
		if err := h.heap.updateRow(u.RowID, u.Values); err != nil {
			return err
		}
	}
	return nil
}

// -------------------------------------------------------------------------
// Engine interface — DDL operations
// -------------------------------------------------------------------------

func (e *engine) CreateTable(name string, columns []ColumnDef) error {
	e.catalogMu.Lock()
	defer e.catalogMu.Unlock()

	if _, exists := e.catalog.getTable(name); exists {
		return &TableExistsError{Name: name}
	}

	// Assign sequential ordinals 0..N-1.
	for i := range columns {
		columns[i].Ordinal = i
	}

	// Write DDL to catalog WAL.
	if err := e.catalogWAL.WriteCreateTable(name, columns); err != nil {
		return fmt.Errorf("catalog WAL: %w", err)
	}

	// Update catalog.
	if err := e.catalog.createTable(name, columns); err != nil {
		return err
	}

	// Create per-table WAL file.
	tablesDir := filepath.Join(e.dataDir, tablesDirName)
	walPath := filepath.Join(tablesDir, tableFileName(name))
	w, err := OpenWAL(walPath, false)
	if err != nil {
		return fmt.Errorf("create table WAL: %w", err)
	}

	w.fsync = &e.fsync
	def := *e.catalog.tables[name]
	e.tableStates[name] = &tableState{
		heap: newTableHeap(def),
		wal:  w,
	}
	return nil
}

func (e *engine) DropTable(name string) error {
	e.catalogMu.Lock()
	defer e.catalogMu.Unlock()

	ts, err := e.getTableState(name)
	if err != nil {
		return err
	}

	// Lock the table to prevent concurrent DML.
	ts.mu.Lock()
	ts.dropped = true
	ts.mu.Unlock()

	// Write DDL to catalog WAL.
	if err := e.catalogWAL.WriteDropTable(name); err != nil {
		return fmt.Errorf("catalog WAL: %w", err)
	}

	// Close and delete the table WAL file.
	walPath := filepath.Join(e.dataDir, tablesDirName, tableFileName(name))
	ts.wal.Close()
	os.Remove(walPath) // best-effort; orphan cleanup handles this on restart

	// Update catalog and remove tableState.
	e.catalog.dropTable(name)
	delete(e.tableStates, name)
	return nil
}

func (e *engine) AddColumn(table string, col ColumnDef) error {
	e.catalogMu.Lock()
	defer e.catalogMu.Unlock()

	ts, err := e.getTableState(table)
	if err != nil {
		return err
	}

	ts.mu.Lock()
	defer ts.mu.Unlock()

	if ts.dropped {
		return &TableNotFoundError{Name: table}
	}

	// Validate column name is not a duplicate.
	for _, existing := range ts.heap.def.Columns {
		if existing.Name == col.Name {
			return &ColumnExistsError{Column: col.Name, Table: table}
		}
	}

	// Assign ordinal.
	col.Ordinal = ts.heap.def.NextOrdinal

	// Write to catalog WAL.
	if err := e.catalogWAL.WriteAddColumn(table, col); err != nil {
		return fmt.Errorf("catalog WAL: %w", err)
	}

	// Update catalog + heap def.
	e.catalog.addColumn(table, col)
	ts.heap.def = *e.catalog.tables[table]
	return nil
}

func (e *engine) DropColumn(table string, colName string) error {
	e.catalogMu.Lock()
	defer e.catalogMu.Unlock()

	ts, err := e.getTableState(table)
	if err != nil {
		return err
	}

	ts.mu.Lock()
	defer ts.mu.Unlock()

	if ts.dropped {
		return &TableNotFoundError{Name: table}
	}

	// Validate: column exists, is not PK, is not last column.
	def := e.catalog.tables[table]
	colIdx := -1
	for i, col := range def.Columns {
		if col.Name == colName {
			colIdx = i
			break
		}
	}
	if colIdx < 0 {
		return &ColumnNotFoundError{Column: colName, Table: table}
	}
	if def.Columns[colIdx].PrimaryKey {
		return fmt.Errorf("cannot drop primary key column %q", colName)
	}
	if len(def.Columns) <= 1 {
		return fmt.Errorf("cannot drop the only column of table %q", table)
	}

	// Write to catalog WAL.
	if err := e.catalogWAL.WriteDropColumn(table, colName); err != nil {
		return fmt.Errorf("catalog WAL: %w", err)
	}

	// Update catalog.
	e.catalog.dropColumn(table, colName)

	// Update heap def.
	ts.heap.def = *e.catalog.tables[table]
	return nil
}

func (e *engine) CreateIndex(table string, idx IndexDef) error {
	e.catalogMu.Lock()
	defer e.catalogMu.Unlock()

	ts, err := e.getTableState(table)
	if err != nil {
		return err
	}

	ts.mu.Lock()
	defer ts.mu.Unlock()

	if ts.dropped {
		return &TableNotFoundError{Name: table}
	}

	// Validate column exists.
	colExists := false
	for _, col := range ts.heap.def.Columns {
		if col.Name == idx.Column {
			colExists = true
			break
		}
	}
	if !colExists {
		return &ColumnNotFoundError{Column: idx.Column, Table: table}
	}

	// Validate index name is unique within the table.
	for _, existing := range ts.heap.def.Indexes {
		if existing.Name == idx.Name {
			return &IndexExistsError{Name: idx.Name, Table: table}
		}
	}

	// Build the in-memory index from existing rows (validates uniqueness).
	if err := ts.heap.addSecondaryIndex(idx); err != nil {
		return err
	}

	// Write to catalog WAL.
	if err := e.catalogWAL.WriteCreateIndex(table, idx); err != nil {
		// Roll back the in-memory index.
		ts.heap.removeSecondaryIndex(idx.Name)
		return fmt.Errorf("catalog WAL: %w", err)
	}

	// Update catalog.
	e.catalog.createIndex(table, idx)
	ts.heap.def = *e.catalog.tables[table]
	return nil
}

func (e *engine) DropIndex(table string, indexName string) error {
	e.catalogMu.Lock()
	defer e.catalogMu.Unlock()

	ts, err := e.getTableState(table)
	if err != nil {
		return err
	}

	ts.mu.Lock()
	defer ts.mu.Unlock()

	if ts.dropped {
		return &TableNotFoundError{Name: table}
	}

	// Validate index exists.
	found := false
	for _, idx := range ts.heap.def.Indexes {
		if idx.Name == indexName {
			found = true
			break
		}
	}
	if !found {
		return &IndexNotFoundError{Name: indexName, Table: table}
	}

	// Write to catalog WAL.
	if err := e.catalogWAL.WriteDropIndex(table, indexName); err != nil {
		return fmt.Errorf("catalog WAL: %w", err)
	}

	// Update catalog and heap.
	e.catalog.dropIndex(table, indexName)
	ts.heap.removeSecondaryIndex(indexName)
	ts.heap.def = *e.catalog.tables[table]
	return nil
}

func (e *engine) LookupByIndex(table string, indexName string, value any) ([]Row, error) {
	ts, err := e.acquireTableRead(table)
	if err != nil {
		return nil, err
	}
	defer ts.mu.RUnlock()

	rows := ts.heap.lookupByIndex(indexName, value)
	// Return copies to avoid data races.
	result := make([]Row, len(rows))
	for i, row := range rows {
		vals := make([]any, len(row.Values))
		copy(vals, row.Values)
		result[i] = Row{ID: row.ID, Values: vals}
	}
	return result, nil
}

// -------------------------------------------------------------------------
// Engine interface — read-only metadata
// -------------------------------------------------------------------------

func (e *engine) GetTable(name string) (*TableDef, bool) {
	e.catalogMu.RLock()
	defer e.catalogMu.RUnlock()

	return e.catalog.getTable(name)
}

func (e *engine) RowCount(table string) (int64, error) {
	ts, err := e.getTableState(table)
	if err != nil {
		return 0, err
	}
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	return int64(ts.heap.count), nil
}

func (e *engine) ListTables() []*TableDef {
	e.catalogMu.RLock()
	defer e.catalogMu.RUnlock()

	defs := make([]*TableDef, 0, len(e.catalog.tables))
	for _, def := range e.catalog.tables {
		defs = append(defs, def)
	}
	return defs
}

func (e *engine) MemoryUsage() []TableMemoryInfo {
	e.catalogMu.RLock()
	defer e.catalogMu.RUnlock()

	infos := make([]TableMemoryInfo, 0, len(e.tableStates))
	for _, ts := range e.tableStates {
		ts.mu.RLock()
		info := ts.heap.memoryInfo()
		ts.mu.RUnlock()
		infos = append(infos, info)
	}
	sort.Slice(infos, func(i, j int) bool {
		return infos[i].TableName < infos[j].TableName
	})
	return infos
}

// -------------------------------------------------------------------------
// Engine interface — DML operations (per-table locking)
// -------------------------------------------------------------------------

func (e *engine) Insert(table string, columns []string, values [][]any) (int64, error) {
	ts, err := e.acquireTableWrite(table)
	if err != nil {
		return 0, err
	}
	defer ts.mu.Unlock()

	heap := ts.heap

	// Resolve all rows first so we can pre-validate PK uniqueness.
	resolvedRows := make([][]any, 0, len(values))
	for _, vals := range values {
		fullRow, err := resolveInsertRow(heap, columns, vals)
		if err != nil {
			return 0, err
		}
		resolvedRows = append(resolvedRows, fullRow)
	}

	// Pre-validate NOT NULL constraints for all rows.
	for _, col := range heap.def.Columns {
		if !col.NotNull {
			continue
		}
		for _, fullRow := range resolvedRows {
			if RowValue(fullRow, col.Ordinal) == nil {
				return 0, &NotNullViolationError{
					Table:  table,
					Column: col.Name,
				}
			}
		}
	}

	// Pre-validate PK uniqueness for all rows before writing any WAL entries.
	if heap.pkCol >= 0 {
		pkColName := heap.pkColumnName()
		seen := make(map[any]bool, len(resolvedRows))
		for _, fullRow := range resolvedRows {
			key := RowValue(fullRow, heap.pkCol)
			if key == nil {
				return 0, &UniqueViolationError{
					Table:  table,
					Column: pkColName,
				}
			}
			if seen[key] {
				return 0, &UniqueViolationError{
					Table:  table,
					Column: pkColName,
					Value:  key,
				}
			}
			seen[key] = true
			if _, exists := heap.pkIdx.Get(key); exists {
				return 0, &UniqueViolationError{
					Table:  table,
					Column: pkColName,
					Value:  key,
				}
			}
		}
	}

	// Pre-validate unique secondary index constraints.
	for i := range heap.secondaries {
		si := &heap.secondaries[i]
		if si.unique == nil {
			continue
		}
		seen := make(map[any]bool, len(resolvedRows))
		for _, fullRow := range resolvedRows {
			key := RowValue(fullRow, si.colOrd)
			if key == nil {
				continue // NULLs don't violate unique constraints
			}
			if seen[key] {
				return 0, &UniqueViolationError{
					Table:  table,
					Column: si.def.Column,
					Value:  key,
					Index:  si.def.Name,
				}
			}
			seen[key] = true
			if _, exists := si.unique.Get(key); exists {
				return 0, &UniqueViolationError{
					Table:  table,
					Column: si.def.Column,
					Value:  key,
					Index:  si.def.Name,
				}
			}
		}
	}

	var count int64
	for _, fullRow := range resolvedRows {
		id := heap.allocateID()
		if err := ts.wal.WriteInsert(table, id, fullRow); err != nil {
			return count, fmt.Errorf("WAL: %w", err)
		}
		heap.insertWithID(id, fullRow)
		count++
	}
	return count, nil
}

func (e *engine) Scan(table string) (RowIterator, error) {
	ts, err := e.acquireTableRead(table)
	if err != nil {
		return nil, err
	}
	defer ts.mu.RUnlock()

	return ts.heap.scan(), nil
}

func (e *engine) Update(table string, sets map[string]any, filter func(Row) bool) (int64, error) {
	ts, err := e.acquireTableWrite(table)
	if err != nil {
		return 0, err
	}
	defer ts.mu.Unlock()

	heap := ts.heap

	var updates []rowUpdate
	for id, values := range heap.rows {
		if values == nil {
			continue
		}
		row := Row{ID: int64(id), Values: values}
		if filter != nil && !filter(row) {
			continue
		}
		// Extend short rows to full ordinal width.
		newValues := make([]any, heap.def.NextOrdinal)
		copy(newValues, values)
		for colName, newVal := range sets {
			idx := heap.columnIndex(colName)
			if idx < 0 {
				return 0, &ColumnNotFoundError{Column: colName, Table: heap.def.Name}
			}
			newValues[idx] = newVal
		}
		coerced, err := coerceRowValues(&heap.def, newValues)
		if err != nil {
			return 0, err
		}
		updates = append(updates, rowUpdate{RowID: int64(id), Values: coerced})
	}

	if len(updates) == 0 {
		return 0, nil
	}

	// Pre-validate NOT NULL constraints for columns being SET.
	for _, col := range heap.def.Columns {
		if !col.NotNull {
			continue
		}
		if _, changing := sets[col.Name]; !changing {
			continue
		}
		for _, u := range updates {
			if RowValue(u.Values, col.Ordinal) == nil {
				return 0, &NotNullViolationError{
					Table:  table,
					Column: col.Name,
				}
			}
		}
	}

	// Pre-validate PK uniqueness before WAL write.
	if heap.pkCol >= 0 {
		pkColName := heap.pkColumnName()
		if _, changing := sets[pkColName]; changing {
			updatingIDs := make(map[int64]bool, len(updates))
			for _, u := range updates {
				updatingIDs[u.RowID] = true
			}

			seen := make(map[any]bool, len(updates))
			for _, u := range updates {
				newKey := RowValue(u.Values, heap.pkCol)
				if newKey == nil {
					return 0, &UniqueViolationError{Table: table, Column: pkColName}
				}
				if seen[newKey] {
					return 0, &UniqueViolationError{Table: table, Column: pkColName, Value: newKey}
				}
				seen[newKey] = true
				if existingID, found := heap.pkIdx.Get(newKey); found && !updatingIDs[existingID] {
					return 0, &UniqueViolationError{Table: table, Column: pkColName, Value: newKey}
				}
			}
		}
	}

	// Pre-validate unique secondary index constraints before WAL write.
	updatingIDs := make(map[int64]bool, len(updates))
	for _, u := range updates {
		updatingIDs[u.RowID] = true
	}
	for i := range heap.secondaries {
		si := &heap.secondaries[i]
		if si.unique == nil {
			continue
		}
		if _, changing := sets[si.def.Column]; !changing {
			continue
		}
		seen := make(map[any]bool, len(updates))
		for _, u := range updates {
			newKey := RowValue(u.Values, si.colOrd)
			if newKey == nil {
				continue // NULLs don't violate unique constraints
			}
			if seen[newKey] {
				return 0, &UniqueViolationError{Table: table, Column: si.def.Column, Value: newKey, Index: si.def.Name}
			}
			seen[newKey] = true
			if existingID, found := si.unique.Get(newKey); found && !updatingIDs[existingID] {
				return 0, &UniqueViolationError{Table: table, Column: si.def.Column, Value: newKey, Index: si.def.Name}
			}
		}
	}

	if err := ts.wal.WriteUpdate(table, updates); err != nil {
		return 0, fmt.Errorf("WAL: %w", err)
	}
	for _, u := range updates {
		heap.updateRow(u.RowID, u.Values)
	}
	return int64(len(updates)), nil
}

func (e *engine) Delete(table string, filter func(Row) bool) (int64, error) {
	ts, err := e.acquireTableWrite(table)
	if err != nil {
		return 0, err
	}
	defer ts.mu.Unlock()

	heap := ts.heap

	var ids []int64
	for id, values := range heap.rows {
		if values == nil {
			continue
		}
		row := Row{ID: int64(id), Values: values}
		if filter != nil && !filter(row) {
			continue
		}
		ids = append(ids, int64(id))
	}

	if len(ids) == 0 {
		return 0, nil
	}

	if err := ts.wal.WriteDelete(table, ids); err != nil {
		return 0, fmt.Errorf("WAL: %w", err)
	}
	heap.deleteRows(ids)
	return int64(len(ids)), nil
}

func (e *engine) LookupByPK(table string, value any) (*Row, error) {
	ts, err := e.acquireTableRead(table)
	if err != nil {
		return nil, err
	}
	defer ts.mu.RUnlock()

	row, ok := ts.heap.lookupByPK(value)
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

// acquireTableWrite looks up the tableState under a brief catalogMu read
// lock, then acquires the table's write lock. Returns an error if the
// table doesn't exist or was dropped concurrently.
func (e *engine) acquireTableWrite(name string) (*tableState, error) {
	e.catalogMu.RLock()
	ts, err := e.getTableState(name)
	e.catalogMu.RUnlock()
	if err != nil {
		return nil, err
	}

	ts.mu.Lock()
	if ts.dropped {
		ts.mu.Unlock()
		return nil, &TableNotFoundError{Name: name}
	}
	return ts, nil
}

// acquireTableRead looks up the tableState under a brief catalogMu read
// lock, then acquires the table's read lock.
func (e *engine) acquireTableRead(name string) (*tableState, error) {
	e.catalogMu.RLock()
	ts, err := e.getTableState(name)
	e.catalogMu.RUnlock()
	if err != nil {
		return nil, err
	}

	ts.mu.RLock()
	if ts.dropped {
		ts.mu.RUnlock()
		return nil, &TableNotFoundError{Name: name}
	}
	return ts, nil
}

// resolveInsertRow maps named columns + values to a full row in ordinal
// order, filling unspecified positions with nil (NULL). When columns is nil
// the values are mapped positionally via def.Columns[i].Ordinal.
func resolveInsertRow(heap *tableHeap, columns []string, values []any) ([]any, error) {
	def := &heap.def

	if columns == nil {
		if len(values) != len(def.Columns) {
			return nil, &ValueCountError{Expected: len(def.Columns), Got: len(values)}
		}
		// Map positional values to their ordinal positions.
		row := make([]any, def.NextOrdinal)
		for i, col := range def.Columns {
			row[col.Ordinal] = values[i]
		}
		return coerceRowValues(def, row)
	}

	row := make([]any, def.NextOrdinal)
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
	return coerceRowValues(def, row)
}

// migrateLegacyWALVersion checks whether the legacy wal.dat file needs a
// format version migration (e.g. v1→v2) and performs it if so. After this
// call, the wal.dat file is guaranteed to be at walCurrentVersion.
func migrateLegacyWALVersion(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	version, err := readWALVersion(f)
	f.Close()
	if err != nil {
		return fmt.Errorf("read WAL version: %w", err)
	}
	if version > 0 && version < walCurrentVersion {
		log.Printf("migrating WAL format from version %d to %d...", version, walCurrentVersion)
		backupPath, err := migrateWAL(path, version)
		if err != nil {
			return fmt.Errorf("migrate WAL v%d→v%d: %w", version, walCurrentVersion, err)
		}
		log.Printf("WAL format migration complete. Original backed up to %s", backupPath)
	}
	return nil
}

// fileExists returns true if path exists and is a regular file.
func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

func (e *engine) SetFsync(enabled bool) {
	e.fsync.Store(enabled)
}

func (e *engine) GetFsync() bool {
	return e.fsync.Load()
}

// SplitWALMigrationNeededError is returned when the data directory
// contains a legacy single wal.dat but --migrate was not specified.
type SplitWALMigrationNeededError struct{}

func (e *SplitWALMigrationNeededError) Error() string {
	return "data directory uses legacy single-WAL format; restart with --migrate flag to convert to per-table WAL files"
}
