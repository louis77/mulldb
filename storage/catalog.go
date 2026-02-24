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
		return fmt.Errorf("table %q already exists", name)
	}
	c.tables[name] = &TableDef{Name: name, Columns: columns}
	return nil
}

func (c *catalog) dropTable(name string) error {
	if _, exists := c.tables[name]; !exists {
		return fmt.Errorf("table %q does not exist", name)
	}
	delete(c.tables, name)
	return nil
}

func (c *catalog) getTable(name string) (*TableDef, bool) {
	def, ok := c.tables[name]
	return def, ok
}
