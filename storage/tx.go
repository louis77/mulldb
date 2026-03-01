package storage

// TxOverlay holds the pending changes for a single transaction.
// Changes are buffered here and only applied to the real heap on COMMIT.
type TxOverlay struct {
	// inserts maps table name → list of pending inserts (rowID + values).
	Inserts map[string][]rowInsert

	// deletes maps table name → set of rowIDs to hide from reads.
	Deletes map[string]map[int64]struct{}

	// updates maps table name → rowID → new values.
	Updates map[string]map[int64][]any
}

// NewTxOverlay creates an empty transaction overlay.
func NewTxOverlay() *TxOverlay {
	return &TxOverlay{
		Inserts: make(map[string][]rowInsert),
		Deletes: make(map[string]map[int64]struct{}),
		Updates: make(map[string]map[int64][]any),
	}
}

// AddInsert records a pending insert.
func (o *TxOverlay) AddInsert(table string, rowID int64, values []any) {
	o.Inserts[table] = append(o.Inserts[table], rowInsert{RowID: rowID, Values: values})
}

// AddDelete records a pending delete.
func (o *TxOverlay) AddDelete(table string, rowID int64) {
	if o.Deletes[table] == nil {
		o.Deletes[table] = make(map[int64]struct{})
	}
	o.Deletes[table][rowID] = struct{}{}
}

// AddUpdate records a pending update.
func (o *TxOverlay) AddUpdate(table string, rowID int64, values []any) {
	if o.Updates[table] == nil {
		o.Updates[table] = make(map[int64][]any)
	}
	o.Updates[table][rowID] = values
}

// IsDeleted returns true if the given row has been deleted in this transaction.
func (o *TxOverlay) IsDeleted(table string, rowID int64) bool {
	if dels, ok := o.Deletes[table]; ok {
		_, deleted := dels[rowID]
		return deleted
	}
	return false
}

// GetUpdate returns the updated values for a row, if any.
func (o *TxOverlay) GetUpdate(table string, rowID int64) ([]any, bool) {
	if upds, ok := o.Updates[table]; ok {
		vals, found := upds[rowID]
		return vals, found
	}
	return nil, false
}

// TouchedTables returns a sorted list of table names that have any changes.
func (o *TxOverlay) TouchedTables() []string {
	seen := make(map[string]bool)
	for t := range o.Inserts {
		seen[t] = true
	}
	for t := range o.Deletes {
		seen[t] = true
	}
	for t := range o.Updates {
		seen[t] = true
	}
	tables := make([]string, 0, len(seen))
	for t := range seen {
		tables = append(tables, t)
	}
	// Sort for deterministic lock ordering.
	sortStrings(tables)
	return tables
}

// sortStrings sorts a string slice in ascending order.
func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}
