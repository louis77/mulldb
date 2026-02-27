package executor

import (
	"sort"
	"strings"

	"mulldb/storage"
)

// catalogTable defines a virtual read-only table that exists outside the
// storage engine. Rows are generated on demand by the rows function.
type catalogTable struct {
	def  *storage.TableDef
	rows func(storage.Engine) []storage.Row
}

// catalogTables is the registry of all virtual catalog tables, keyed by
// fully qualified name (e.g. "pg_catalog.pg_type").
var catalogTables = map[string]*catalogTable{}

func init() {
	registerPGType()
	registerPGDatabase()
	registerPGNamespace()
	registerInformationSchemaTables()
	registerInformationSchemaColumns()
}

// registerPGType adds the pg_type catalog table.
func registerPGType() {
	catalogTables["pg_catalog.pg_type"] = &catalogTable{
		def: &storage.TableDef{
			Name: "pg_type",
			Columns: []storage.ColumnDef{
				{Name: "oid", DataType: storage.TypeInteger},
				{Name: "typname", DataType: storage.TypeText},
			},
		},
		rows: func(_ storage.Engine) []storage.Row {
			return []storage.Row{
				{ID: 1, Values: []any{int64(16), "bool"}},
				{ID: 2, Values: []any{int64(20), "int8"}},
				{ID: 3, Values: []any{int64(25), "text"}},
				{ID: 4, Values: []any{int64(9900), "geometry"}},
				{ID: 5, Values: []any{int64(9901), "geography"}},
			}
		},
	}
}

// registerPGDatabase adds the pg_database catalog table.
func registerPGDatabase() {
	catalogTables["pg_catalog.pg_database"] = &catalogTable{
		def: &storage.TableDef{
			Name: "pg_database",
			Columns: []storage.ColumnDef{
				{Name: "datname", DataType: storage.TypeText},
			},
		},
		rows: func(_ storage.Engine) []storage.Row {
			return []storage.Row{
				{ID: 1, Values: []any{"mulldb"}},
			}
		},
	}
}

// registerPGNamespace adds the pg_namespace catalog table.
func registerPGNamespace() {
	catalogTables["pg_catalog.pg_namespace"] = &catalogTable{
		def: &storage.TableDef{
			Name: "pg_namespace",
			Columns: []storage.ColumnDef{
				{Name: "oid", DataType: storage.TypeInteger},
				{Name: "nspname", DataType: storage.TypeText},
			},
		},
		rows: func(_ storage.Engine) []storage.Row {
			return []storage.Row{
				{ID: 1, Values: []any{int64(11), "pg_catalog"}},
				{ID: 2, Values: []any{int64(2200), "public"}},
				{ID: 3, Values: []any{int64(13183), "information_schema"}},
			}
		},
	}
}

// registerInformationSchemaTables adds the information_schema.tables catalog table.
func registerInformationSchemaTables() {
	catalogTables["information_schema.tables"] = &catalogTable{
		def: &storage.TableDef{
			Name: "tables",
			Columns: []storage.ColumnDef{
				{Name: "table_schema", DataType: storage.TypeText},
				{Name: "table_name", DataType: storage.TypeText},
				{Name: "table_type", DataType: storage.TypeText},
			},
		},
		rows: func(eng storage.Engine) []storage.Row {
			var rows []storage.Row
			var id int64

			// User tables from the storage engine.
			if eng != nil {
				defs := eng.ListTables()
				sort.Slice(defs, func(i, j int) bool {
					return defs[i].Name < defs[j].Name
				})
				for _, def := range defs {
					id++
					rows = append(rows, storage.Row{
						ID:     id,
						Values: []any{"public", def.Name, "BASE TABLE"},
					})
				}
			}

			// Catalog tables themselves.
			var keys []string
			for k := range catalogTables {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, key := range keys {
				id++
				parts := strings.SplitN(key, ".", 2)
				rows = append(rows, storage.Row{
					ID:     id,
					Values: []any{parts[0], parts[1], "SYSTEM VIEW"},
				})
			}

			return rows
		},
	}
}

// registerInformationSchemaColumns adds the information_schema.columns catalog table.
func registerInformationSchemaColumns() {
	catalogTables["information_schema.columns"] = &catalogTable{
		def: &storage.TableDef{
			Name: "columns",
			Columns: []storage.ColumnDef{
				{Name: "table_schema", DataType: storage.TypeText},
				{Name: "table_name", DataType: storage.TypeText},
				{Name: "column_name", DataType: storage.TypeText},
				{Name: "ordinal_position", DataType: storage.TypeInteger},
				{Name: "data_type", DataType: storage.TypeText},
				{Name: "is_nullable", DataType: storage.TypeText},
			},
		},
		rows: func(eng storage.Engine) []storage.Row {
			var rows []storage.Row
			var id int64

			appendColumns := func(schema, tableName string, cols []storage.ColumnDef) {
				for i, col := range cols {
					id++
					nullable := "YES"
					if col.PrimaryKey {
						nullable = "NO"
					}
					rows = append(rows, storage.Row{
						ID: id,
						Values: []any{
							schema,
							tableName,
							col.Name,
							int64(i + 1),
							strings.ToLower(col.DataType.String()),
							nullable,
						},
					})
				}
			}

			// User tables from the storage engine.
			if eng != nil {
				defs := eng.ListTables()
				sort.Slice(defs, func(i, j int) bool {
					return defs[i].Name < defs[j].Name
				})
				for _, def := range defs {
					appendColumns("public", def.Name, def.Columns)
				}
			}

			// Catalog tables themselves.
			var keys []string
			for k := range catalogTables {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, key := range keys {
				parts := strings.SplitN(key, ".", 2)
				appendColumns(parts[0], parts[1], catalogTables[key].def.Columns)
			}

			return rows
		},
	}
}

// resolveCatalogKey maps (schema, name) to a fully qualified catalog key.
// If schema is set, it looks up "schema.name" directly.
// If unqualified, it tries "pg_catalog.name" first (PostgreSQL behavior).
func resolveCatalogKey(schema, name string) (string, bool) {
	if schema != "" {
		key := schema + "." + name
		_, ok := catalogTables[key]
		return key, ok
	}
	// Unqualified: try pg_catalog first.
	key := "pg_catalog." + name
	if _, ok := catalogTables[key]; ok {
		return key, true
	}
	return "", false
}

// getCatalogTable returns the table definition for a catalog table, or
// false if the name is not a catalog table.
func getCatalogTable(schema, name string) (*storage.TableDef, bool) {
	key, ok := resolveCatalogKey(schema, name)
	if !ok {
		return nil, false
	}
	return catalogTables[key].def, true
}

// scanCatalogTable returns a RowIterator over the catalog table's rows.
func scanCatalogTable(schema, name string, eng storage.Engine) (storage.RowIterator, error) {
	key, ok := resolveCatalogKey(schema, name)
	if !ok {
		fullName := name
		if schema != "" {
			fullName = schema + "." + name
		}
		return nil, &storage.TableNotFoundError{Name: fullName}
	}
	ct := catalogTables[key]
	return &catalogIterator{rows: ct.rows(eng), pos: 0}, nil
}

// isCatalogTable reports whether (schema, name) is a registered catalog table.
func isCatalogTable(schema, name string) bool {
	_, ok := resolveCatalogKey(schema, name)
	return ok
}

// catalogIterator implements storage.RowIterator over an in-memory slice.
type catalogIterator struct {
	rows []storage.Row
	pos  int
}

func (it *catalogIterator) Next() (storage.Row, bool) {
	if it.pos >= len(it.rows) {
		return storage.Row{}, false
	}
	r := it.rows[it.pos]
	it.pos++
	return r, true
}

func (it *catalogIterator) Close() error {
	return nil
}
