package storage

import "fmt"

// TxEngine wraps a real Engine and intercepts reads/writes to use a
// transaction overlay. Writes go to the overlay; reads merge the overlay
// with the real heap. On COMMIT, the overlay is applied atomically to the
// real engine.
type TxEngine struct {
	real    *engine
	overlay *TxOverlay
}

// NewTxEngine creates a transaction engine wrapping the given engine.
func NewTxEngine(eng Engine) *TxEngine {
	return &TxEngine{
		real:    eng.(*engine),
		overlay: NewTxOverlay(),
	}
}

// Overlay returns the overlay for use during commit.
func (tx *TxEngine) Overlay() *TxOverlay {
	return tx.overlay
}

// -------------------------------------------------------------------------
// DDL — rejected inside transactions
// -------------------------------------------------------------------------

func (tx *TxEngine) CreateTable(string, []ColumnDef) error {
	return &ActiveTxError{}
}

func (tx *TxEngine) DropTable(string) error {
	return &ActiveTxError{}
}

func (tx *TxEngine) AddColumn(string, ColumnDef) error {
	return &ActiveTxError{}
}

func (tx *TxEngine) DropColumn(string, string) error {
	return &ActiveTxError{}
}

func (tx *TxEngine) CreateIndex(string, IndexDef) error {
	return &ActiveTxError{}
}

func (tx *TxEngine) DropIndex(string, string) error {
	return &ActiveTxError{}
}

// ActiveTxError is returned when DDL is attempted inside a transaction.
type ActiveTxError struct{}

func (e *ActiveTxError) Error() string {
	return "DDL commands are not allowed inside a transaction"
}

// -------------------------------------------------------------------------
// Read-only metadata — delegate to real engine
// -------------------------------------------------------------------------

func (tx *TxEngine) GetTable(name string) (*TableDef, bool) {
	return tx.real.GetTable(name)
}

func (tx *TxEngine) ListTables() []*TableDef {
	return tx.real.ListTables()
}

func (tx *TxEngine) MemoryUsage() []TableMemoryInfo {
	return tx.real.MemoryUsage()
}

func (tx *TxEngine) SetFsync(enabled bool) {
	tx.real.SetFsync(enabled)
}

func (tx *TxEngine) GetFsync() bool {
	return tx.real.GetFsync()
}

func (tx *TxEngine) Close() error {
	return nil // TxEngine does not own the real engine
}

// -------------------------------------------------------------------------
// DML — write to overlay
// -------------------------------------------------------------------------

func (tx *TxEngine) Insert(table string, columns []string, values [][]any) (int64, error) {
	// We need to acquire a brief read lock on the table to get the heap
	// for constraint validation, then release it and buffer in overlay.
	ts, err := tx.real.acquireTableRead(table)
	if err != nil {
		return 0, err
	}
	heap := ts.heap

	// Resolve all rows.
	resolvedRows := make([][]any, 0, len(values))
	for _, vals := range values {
		fullRow, err := resolveInsertRow(heap, columns, vals)
		if err != nil {
			ts.mu.RUnlock()
			return 0, err
		}
		resolvedRows = append(resolvedRows, fullRow)
	}

	// Validate NOT NULL constraints.
	for _, col := range heap.def.Columns {
		if !col.NotNull {
			continue
		}
		for _, fullRow := range resolvedRows {
			if RowValue(fullRow, col.Ordinal) == nil {
				ts.mu.RUnlock()
				return 0, &NotNullViolationError{
					Table:  table,
					Column: col.Name,
				}
			}
		}
	}

	// Validate PK uniqueness against heap + overlay.
	if heap.pkCol >= 0 {
		pkColName := heap.pkColumnName()
		seen := make(map[any]bool, len(resolvedRows))
		for _, fullRow := range resolvedRows {
			key := RowValue(fullRow, heap.pkCol)
			if key == nil {
				ts.mu.RUnlock()
				return 0, &UniqueViolationError{
					Table:  table,
					Column: pkColName,
				}
			}
			if seen[key] {
				ts.mu.RUnlock()
				return 0, &UniqueViolationError{
					Table:  table,
					Column: pkColName,
					Value:  key,
				}
			}
			seen[key] = true
			// Check real heap (only if not deleted in overlay).
			if existingID, exists := heap.pkIdx.Get(key); exists {
				if !tx.overlay.IsDeleted(table, existingID) {
					// Check if this row was updated in overlay with a different PK.
					if updVals, updated := tx.overlay.GetUpdate(table, existingID); updated {
						updKey := RowValue(updVals, heap.pkCol)
						if CompareValues(updKey, key) == 0 {
							ts.mu.RUnlock()
							return 0, &UniqueViolationError{
								Table:  table,
								Column: pkColName,
								Value:  key,
							}
						}
						// PK was changed by update, original key is available
					} else {
						ts.mu.RUnlock()
						return 0, &UniqueViolationError{
							Table:  table,
							Column: pkColName,
							Value:  key,
						}
					}
				}
			}
			// Check overlay inserts.
			for _, ins := range tx.overlay.Inserts[table] {
				insKey := RowValue(ins.Values, heap.pkCol)
				if CompareValues(insKey, key) == 0 {
					ts.mu.RUnlock()
					return 0, &UniqueViolationError{
						Table:  table,
						Column: pkColName,
						Value:  key,
					}
				}
			}
		}
	}

	// Validate unique secondary index constraints against heap + overlay.
	for i := range heap.secondaries {
		si := &heap.secondaries[i]
		if si.unique == nil {
			continue
		}
		seen := make(map[any]bool, len(resolvedRows))
		for _, fullRow := range resolvedRows {
			key := RowValue(fullRow, si.colOrd)
			if key == nil {
				continue
			}
			if seen[key] {
				ts.mu.RUnlock()
				return 0, &UniqueViolationError{
					Table:  table,
					Column: si.def.Column,
					Value:  key,
					Index:  si.def.Name,
				}
			}
			seen[key] = true
			if existingID, exists := si.unique.Get(key); exists {
				if !tx.overlay.IsDeleted(table, existingID) {
					if updVals, updated := tx.overlay.GetUpdate(table, existingID); updated {
						updKey := RowValue(updVals, si.colOrd)
						if CompareValues(updKey, key) == 0 {
							ts.mu.RUnlock()
							return 0, &UniqueViolationError{
								Table:  table,
								Column: si.def.Column,
								Value:  key,
								Index:  si.def.Name,
							}
						}
					} else {
						ts.mu.RUnlock()
						return 0, &UniqueViolationError{
							Table:  table,
							Column: si.def.Column,
							Value:  key,
							Index:  si.def.Name,
						}
					}
				}
			}
			// Check overlay inserts.
			for _, ins := range tx.overlay.Inserts[table] {
				insKey := RowValue(ins.Values, si.colOrd)
				if CompareValues(insKey, key) == 0 {
					ts.mu.RUnlock()
					return 0, &UniqueViolationError{
						Table:  table,
						Column: si.def.Column,
						Value:  key,
						Index:  si.def.Name,
					}
				}
			}
		}
	}

	// Allocate row IDs and buffer in overlay.
	for _, fullRow := range resolvedRows {
		id := heap.allocateID()
		tx.overlay.AddInsert(table, id, fullRow)
	}

	ts.mu.RUnlock()
	return int64(len(resolvedRows)), nil
}

func (tx *TxEngine) Scan(table string) (RowIterator, error) {
	ts, err := tx.real.acquireTableRead(table)
	if err != nil {
		return nil, err
	}
	defer ts.mu.RUnlock()

	heap := ts.heap

	// Build rows: scan heap, apply overlay (skip deletes, apply updates),
	// then append overlay inserts.
	rows := make([]Row, 0, heap.count)
	for id, values := range heap.rows {
		if values == nil {
			continue
		}
		rowID := int64(id)
		if tx.overlay.IsDeleted(table, rowID) {
			continue
		}
		if updVals, ok := tx.overlay.GetUpdate(table, rowID); ok {
			rows = append(rows, Row{ID: rowID, Values: updVals})
		} else {
			rows = append(rows, Row{ID: rowID, Values: values})
		}
	}
	// Append overlay inserts.
	for _, ins := range tx.overlay.Inserts[table] {
		rows = append(rows, Row{ID: ins.RowID, Values: ins.Values})
	}
	return &sliceIterator{rows: rows}, nil
}

func (tx *TxEngine) Update(table string, sets map[string]any, filter func(Row) bool) (int64, error) {
	ts, err := tx.real.acquireTableRead(table)
	if err != nil {
		return 0, err
	}
	heap := ts.heap

	// Collect rows to update: scan heap + overlay.
	type pendingUpdate struct {
		rowID     int64
		newValues []any
		isOverlay bool // true if this row came from overlay inserts
	}
	var updates []pendingUpdate

	// Scan heap rows.
	for id, values := range heap.rows {
		if values == nil {
			continue
		}
		rowID := int64(id)
		if tx.overlay.IsDeleted(table, rowID) {
			continue
		}
		currentVals := values
		if updVals, ok := tx.overlay.GetUpdate(table, rowID); ok {
			currentVals = updVals
		}
		row := Row{ID: rowID, Values: currentVals}
		if filter != nil && !filter(row) {
			continue
		}
		// Build new values.
		newValues := make([]any, heap.def.NextOrdinal)
		copy(newValues, currentVals)
		for colName, newVal := range sets {
			idx := heap.columnIndex(colName)
			if idx < 0 {
				ts.mu.RUnlock()
				return 0, &ColumnNotFoundError{Column: colName, Table: heap.def.Name}
			}
			newValues[idx] = newVal
		}
		coerced, err := coerceRowValues(&heap.def, newValues)
		if err != nil {
			ts.mu.RUnlock()
			return 0, err
		}
		updates = append(updates, pendingUpdate{rowID: rowID, newValues: coerced})
	}

	// Scan overlay inserts.
	for i, ins := range tx.overlay.Inserts[table] {
		row := Row{ID: ins.RowID, Values: ins.Values}
		if filter != nil && !filter(row) {
			continue
		}
		newValues := make([]any, heap.def.NextOrdinal)
		copy(newValues, ins.Values)
		for colName, newVal := range sets {
			idx := heap.columnIndex(colName)
			if idx < 0 {
				ts.mu.RUnlock()
				return 0, &ColumnNotFoundError{Column: colName, Table: heap.def.Name}
			}
			newValues[idx] = newVal
		}
		coerced, err := coerceRowValues(&heap.def, newValues)
		if err != nil {
			ts.mu.RUnlock()
			return 0, err
		}
		_ = i
		updates = append(updates, pendingUpdate{rowID: ins.RowID, newValues: coerced, isOverlay: true})
	}

	ts.mu.RUnlock()

	if len(updates) == 0 {
		return 0, nil
	}

	// Validate NOT NULL constraints for columns being SET.
	for _, col := range heap.def.Columns {
		if !col.NotNull {
			continue
		}
		if _, changing := sets[col.Name]; !changing {
			continue
		}
		for _, u := range updates {
			if RowValue(u.newValues, col.Ordinal) == nil {
				return 0, &NotNullViolationError{
					Table:  table,
					Column: col.Name,
				}
			}
		}
	}

	// Apply updates to overlay.
	for _, u := range updates {
		if u.isOverlay {
			// Update the overlay insert in place.
			for i := range tx.overlay.Inserts[table] {
				if tx.overlay.Inserts[table][i].RowID == u.rowID {
					tx.overlay.Inserts[table][i].Values = u.newValues
					break
				}
			}
		} else {
			tx.overlay.AddUpdate(table, u.rowID, u.newValues)
		}
	}
	return int64(len(updates)), nil
}

func (tx *TxEngine) Delete(table string, filter func(Row) bool) (int64, error) {
	ts, err := tx.real.acquireTableRead(table)
	if err != nil {
		return 0, err
	}
	heap := ts.heap

	var count int64

	// Scan heap rows.
	for id, values := range heap.rows {
		if values == nil {
			continue
		}
		rowID := int64(id)
		if tx.overlay.IsDeleted(table, rowID) {
			continue
		}
		currentVals := values
		if updVals, ok := tx.overlay.GetUpdate(table, rowID); ok {
			currentVals = updVals
		}
		row := Row{ID: rowID, Values: currentVals}
		if filter != nil && !filter(row) {
			continue
		}
		tx.overlay.AddDelete(table, rowID)
		// If there was a pending update for this row, remove it.
		if tx.overlay.Updates[table] != nil {
			delete(tx.overlay.Updates[table], rowID)
		}
		count++
	}

	// Scan overlay inserts — remove matching ones.
	if inserts, ok := tx.overlay.Inserts[table]; ok {
		remaining := inserts[:0]
		for _, ins := range inserts {
			row := Row{ID: ins.RowID, Values: ins.Values}
			if filter != nil && !filter(row) {
				remaining = append(remaining, ins)
			} else {
				count++
			}
		}
		tx.overlay.Inserts[table] = remaining
	}

	ts.mu.RUnlock()
	return count, nil
}

func (tx *TxEngine) LookupByPK(table string, value any) (*Row, error) {
	ts, err := tx.real.acquireTableRead(table)
	if err != nil {
		return nil, err
	}
	defer ts.mu.RUnlock()

	heap := ts.heap

	// Check overlay inserts first.
	if heap.pkCol >= 0 {
		for _, ins := range tx.overlay.Inserts[table] {
			key := RowValue(ins.Values, heap.pkCol)
			if CompareValues(key, value) == 0 {
				vals := make([]any, len(ins.Values))
				copy(vals, ins.Values)
				return &Row{ID: ins.RowID, Values: vals}, nil
			}
		}
	}

	// Check real heap.
	row, ok := heap.lookupByPK(value)
	if !ok {
		return nil, nil
	}

	// Check if deleted in overlay.
	if tx.overlay.IsDeleted(table, row.ID) {
		return nil, nil
	}

	// Check if updated in overlay.
	if updVals, ok := tx.overlay.GetUpdate(table, row.ID); ok {
		// Check if the PK was changed by the update.
		updKey := RowValue(updVals, heap.pkCol)
		if CompareValues(updKey, value) != 0 {
			return nil, nil // PK was changed, no longer matches
		}
		vals := make([]any, len(updVals))
		copy(vals, updVals)
		return &Row{ID: row.ID, Values: vals}, nil
	}

	vals := make([]any, len(row.Values))
	copy(vals, row.Values)
	return &Row{ID: row.ID, Values: vals}, nil
}

func (tx *TxEngine) LookupByIndex(table string, indexName string, value any) ([]Row, error) {
	ts, err := tx.real.acquireTableRead(table)
	if err != nil {
		return nil, err
	}
	defer ts.mu.RUnlock()

	heap := ts.heap

	// Look up in real heap index.
	heapRows := heap.lookupByIndex(indexName, value)
	var result []Row
	for _, row := range heapRows {
		if tx.overlay.IsDeleted(table, row.ID) {
			continue
		}
		if updVals, ok := tx.overlay.GetUpdate(table, row.ID); ok {
			// Find the index column ordinal.
			var colOrd int
			for i := range heap.secondaries {
				if heap.secondaries[i].def.Name == indexName {
					colOrd = heap.secondaries[i].colOrd
					break
				}
			}
			updKey := RowValue(updVals, colOrd)
			if CompareValues(updKey, value) == 0 {
				vals := make([]any, len(updVals))
				copy(vals, updVals)
				result = append(result, Row{ID: row.ID, Values: vals})
			}
			// else: key was changed, doesn't match anymore
		} else {
			vals := make([]any, len(row.Values))
			copy(vals, row.Values)
			result = append(result, Row{ID: row.ID, Values: vals})
		}
	}

	// Also scan overlay inserts for matching values.
	var colOrd int
	found := false
	for i := range heap.secondaries {
		if heap.secondaries[i].def.Name == indexName {
			colOrd = heap.secondaries[i].colOrd
			found = true
			break
		}
	}
	if found {
		for _, ins := range tx.overlay.Inserts[table] {
			key := RowValue(ins.Values, colOrd)
			if CompareValues(key, value) == 0 {
				vals := make([]any, len(ins.Values))
				copy(vals, ins.Values)
				result = append(result, Row{ID: ins.RowID, Values: vals})
			}
		}
	}

	return result, nil
}

func (tx *TxEngine) RowCount(table string) (int64, error) {
	ts, err := tx.real.acquireTableRead(table)
	if err != nil {
		return 0, err
	}
	defer ts.mu.RUnlock()

	count := int64(ts.heap.count)
	// Subtract deletes.
	count -= int64(len(tx.overlay.Deletes[table]))
	// Add inserts.
	count += int64(len(tx.overlay.Inserts[table]))
	return count, nil
}

// -------------------------------------------------------------------------
// Commit — atomically apply overlay to the real engine
// -------------------------------------------------------------------------

// CommitOverlay atomically applies the transaction overlay to the real
// engine. It acquires table locks in deterministic order, re-validates
// constraints, writes all DML entries to WAL with begin/commit markers,
// applies changes to the heap, and releases all locks.
func (tx *TxEngine) CommitOverlay() error {
	tables := tx.overlay.TouchedTables()
	if len(tables) == 0 {
		return nil // nothing to commit
	}

	// Acquire table write locks in alphabetical order (deterministic → no deadlocks).
	lockedStates := make([]*tableState, 0, len(tables))
	for _, t := range tables {
		ts, err := tx.real.acquireTableWrite(t)
		if err != nil {
			// Release already acquired locks.
			for _, locked := range lockedStates {
				locked.mu.Unlock()
			}
			return err
		}
		lockedStates = append(lockedStates, ts)
	}

	// Deferred unlock all.
	defer func() {
		for _, ts := range lockedStates {
			ts.mu.Unlock()
		}
	}()

	// Re-validate constraints against current heap state.
	for i, t := range tables {
		ts := lockedStates[i]
		heap := ts.heap

		// Re-validate PK uniqueness for inserts.
		if heap.pkCol >= 0 {
			pkColName := heap.pkColumnName()
			for _, ins := range tx.overlay.Inserts[t] {
				key := RowValue(ins.Values, heap.pkCol)
				if key == nil {
					return &UniqueViolationError{Table: t, Column: pkColName}
				}
				if existingID, exists := heap.pkIdx.Get(key); exists {
					if _, deleted := tx.overlay.Deletes[t][existingID]; !deleted {
						return &UniqueViolationError{Table: t, Column: pkColName, Value: key}
					}
				}
			}
		}

		// Re-validate unique secondary indexes for inserts.
		for si := range heap.secondaries {
			sec := &heap.secondaries[si]
			if sec.unique == nil {
				continue
			}
			for _, ins := range tx.overlay.Inserts[t] {
				key := RowValue(ins.Values, sec.colOrd)
				if key == nil {
					continue
				}
				if existingID, exists := sec.unique.Get(key); exists {
					if _, deleted := tx.overlay.Deletes[t][existingID]; !deleted {
						return &UniqueViolationError{
							Table:  t,
							Column: sec.def.Column,
							Value:  key,
							Index:  sec.def.Name,
						}
					}
				}
			}
		}
	}

	// Write DML entries to per-table WAL files with begin/commit markers.
	//
	// For multi-table atomicity we use a two-phase protocol:
	//
	//   Phase 1: Write BeginTx + DML entries to each table WAL (no fsync).
	//   Phase 2: Fsync all table WALs (DML is durable but uncommitted).
	//   Phase 3: Write a single TxCommit record to the catalog WAL and
	//            fsync it. This is the atomic commit point — if this
	//            record is present on recovery, all tables' changes are
	//            applied; if absent, all are discarded.
	//   Phase 4: Write CommitTx markers to each table WAL and fsync.
	//            These are convenience markers so that single-table
	//            replay works without consulting the catalog. If the
	//            process crashes before phase 4 completes, recovery
	//            uses the catalog TxCommit record to know which
	//            per-table transaction groups to apply.
	//
	// Crash scenarios:
	//   - Crash in phase 1/2: No catalog commit → all tables' incomplete
	//     transaction groups (BeginTx without CommitTx) are discarded.
	//   - Crash in phase 3: Catalog commit absent → same as above.
	//   - Crash in phase 4: Catalog commit present → recovery applies
	//     per-table groups that have BeginTx even without CommitTx,
	//     because the catalog says the transaction committed.

	// Collect which tables actually have changes (for the catalog record).
	var changedTables []string
	changedStates := make([]*tableState, 0, len(tables))

	// Phase 1: Write BeginTx + DML to each table WAL (no fsync).
	for i, t := range tables {
		ts := lockedStates[i]

		hasChanges := len(tx.overlay.Inserts[t]) > 0 ||
			len(tx.overlay.Deletes[t]) > 0 ||
			len(tx.overlay.Updates[t]) > 0

		if !hasChanges {
			continue
		}

		changedTables = append(changedTables, t)
		changedStates = append(changedStates, ts)

		if err := ts.wal.WriteBeginTx(); err != nil {
			return fmt.Errorf("WAL begin: %w", err)
		}

		if inserts := tx.overlay.Inserts[t]; len(inserts) > 0 {
			if err := ts.wal.WriteInsertBatchNoSync(t, inserts); err != nil {
				return fmt.Errorf("WAL insert: %w", err)
			}
		}

		if dels := tx.overlay.Deletes[t]; len(dels) > 0 {
			ids := make([]int64, 0, len(dels))
			for id := range dels {
				ids = append(ids, id)
			}
			if err := ts.wal.WriteDeleteNoSync(t, ids); err != nil {
				return fmt.Errorf("WAL delete: %w", err)
			}
		}

		if upds := tx.overlay.Updates[t]; len(upds) > 0 {
			updates := make([]rowUpdate, 0, len(upds))
			for rowID, vals := range upds {
				updates = append(updates, rowUpdate{RowID: rowID, Values: vals})
			}
			if err := ts.wal.WriteUpdateNoSync(t, updates); err != nil {
				return fmt.Errorf("WAL update: %w", err)
			}
		}
	}

	if len(changedTables) == 0 {
		return nil
	}

	// Phase 2: Fsync all table WALs so DML entries are durable.
	for _, ts := range changedStates {
		if err := ts.wal.Sync(); err != nil {
			return fmt.Errorf("WAL sync: %w", err)
		}
	}

	// Phase 3: Write the atomic commit record to the catalog WAL.
	// This is the single point of atomicity: if this record exists on
	// recovery, all per-table transaction groups are applied.
	tx.real.catalogMu.Lock()
	commitErr := tx.real.catalogWAL.WriteTxCommit(changedTables)
	tx.real.catalogMu.Unlock()
	if commitErr != nil {
		return fmt.Errorf("catalog WAL tx commit: %w", commitErr)
	}

	// Phase 4: Write CommitTx markers to each table WAL.
	// These allow per-table replay to work without consulting the catalog
	// for single-table transactions and for the common (no-crash) path.
	for _, ts := range changedStates {
		if err := ts.wal.WriteCommitTx(); err != nil {
			return fmt.Errorf("WAL commit: %w", err)
		}
	}

	// Apply changes to heaps.
	for i, t := range tables {
		ts := lockedStates[i]
		heap := ts.heap

		// Apply deletes first (before inserts, to free IDs).
		if dels := tx.overlay.Deletes[t]; len(dels) > 0 {
			ids := make([]int64, 0, len(dels))
			for id := range dels {
				ids = append(ids, id)
			}
			heap.deleteRows(ids)
		}

		// Apply updates.
		for rowID, vals := range tx.overlay.Updates[t] {
			heap.updateRow(rowID, vals)
		}

		// Apply inserts.
		for _, ins := range tx.overlay.Inserts[t] {
			heap.insertWithID(ins.RowID, ins.Values)
		}
	}

	return nil
}
