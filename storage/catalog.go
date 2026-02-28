package storage

import "fmt"

// catalog manages table schemas in memory. It is rebuilt from the WAL
// on startup — there is no separate catalog file.
type catalog struct {
	tables map[string]*TableDef
}

func newCatalog() *catalog {
	return &catalog{tables: make(map[string]*TableDef)}
}

func (c *catalog) createTable(name string, columns []ColumnDef) error {
	if _, exists := c.tables[name]; exists {
		return &TableExistsError{Name: name}
	}
	// Derive NextOrdinal from the column ordinals.
	next := 0
	for _, col := range columns {
		if col.Ordinal >= next {
			next = col.Ordinal + 1
		}
	}
	c.tables[name] = &TableDef{Name: name, Columns: columns, NextOrdinal: next}
	return nil
}

func (c *catalog) dropTable(name string) error {
	if _, exists := c.tables[name]; !exists {
		return &TableNotFoundError{Name: name}
	}
	delete(c.tables, name)
	return nil
}

func (c *catalog) addColumn(tableName string, col ColumnDef) error {
	def, exists := c.tables[tableName]
	if !exists {
		return &TableNotFoundError{Name: tableName}
	}
	for _, existing := range def.Columns {
		if existing.Name == col.Name {
			return &ColumnExistsError{Column: col.Name, Table: tableName}
		}
	}
	def.Columns = append(def.Columns, col)
	if col.Ordinal >= def.NextOrdinal {
		def.NextOrdinal = col.Ordinal + 1
	}
	return nil
}

func (c *catalog) dropColumn(tableName string, colName string) error {
	def, exists := c.tables[tableName]
	if !exists {
		return &TableNotFoundError{Name: tableName}
	}
	idx := -1
	for i, col := range def.Columns {
		if col.Name == colName {
			idx = i
			break
		}
	}
	if idx < 0 {
		return &ColumnNotFoundError{Column: colName, Table: tableName}
	}
	if def.Columns[idx].PrimaryKey {
		return fmt.Errorf("cannot drop primary key column %q", colName)
	}
	if len(def.Columns) <= 1 {
		return fmt.Errorf("cannot drop the only column of table %q", tableName)
	}
	def.Columns = append(def.Columns[:idx], def.Columns[idx+1:]...)
	return nil
}

func (c *catalog) getTable(name string) (*TableDef, bool) {
	def, ok := c.tables[name]
	return def, ok
}
