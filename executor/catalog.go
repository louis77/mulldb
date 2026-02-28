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
	registerInformationSchemaTableConstraints()
	registerInformationSchemaKeyColumnUsage()
}

// registerPGType adds the pg_type catalog table.
func registerPGType() {
	catalogTables["pg_catalog.pg_type"] = &catalogTable{
		def: &storage.TableDef{
			Name:        "pg_type",
			NextOrdinal: 2,
			Columns: []storage.ColumnDef{
				{Name: "oid", DataType: storage.TypeInteger, Ordinal: 0},
				{Name: "typname", DataType: storage.TypeText, Ordinal: 1},
			},
		},
		rows: func(_ storage.Engine) []storage.Row {
			return []storage.Row{
				{ID: 1, Values: []any{int64(16), "bool"}},
				{ID: 2, Values: []any{int64(20), "int8"}},
				{ID: 3, Values: []any{int64(25), "text"}},
				{ID: 4, Values: []any{int64(1184), "timestamptz"}},
				{ID: 5, Values: []any{int64(9900), "geometry"}},
				{ID: 6, Values: []any{int64(9901), "geography"}},
			}
		},
	}
}

// registerPGDatabase adds the pg_database catalog table.
func registerPGDatabase() {
	catalogTables["pg_catalog.pg_database"] = &catalogTable{
		def: &storage.TableDef{
			Name:        "pg_database",
			NextOrdinal: 1,
			Columns: []storage.ColumnDef{
				{Name: "datname", DataType: storage.TypeText, Ordinal: 0},
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
			Name:        "pg_namespace",
			NextOrdinal: 2,
			Columns: []storage.ColumnDef{
				{Name: "oid", DataType: storage.TypeInteger, Ordinal: 0},
				{Name: "nspname", DataType: storage.TypeText, Ordinal: 1},
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
			Name:        "tables",
			NextOrdinal: 3,
			Columns: []storage.ColumnDef{
				{Name: "table_schema", DataType: storage.TypeText, Ordinal: 0},
				{Name: "table_name", DataType: storage.TypeText, Ordinal: 1},
				{Name: "table_type", DataType: storage.TypeText, Ordinal: 2},
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
			Name:        "columns",
			NextOrdinal: 6,
			Columns: []storage.ColumnDef{
				{Name: "table_schema", DataType: storage.TypeText, Ordinal: 0},
				{Name: "table_name", DataType: storage.TypeText, Ordinal: 1},
				{Name: "column_name", DataType: storage.TypeText, Ordinal: 2},
				{Name: "ordinal_position", DataType: storage.TypeInteger, Ordinal: 3},
				{Name: "data_type", DataType: storage.TypeText, Ordinal: 4},
				{Name: "is_nullable", DataType: storage.TypeText, Ordinal: 5},
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

// registerInformationSchemaTableConstraints adds the
// information_schema.table_constraints catalog table.
func registerInformationSchemaTableConstraints() {
	catalogTables["information_schema.table_constraints"] = &catalogTable{
		def: &storage.TableDef{
			Name:        "table_constraints",
			NextOrdinal: 9,
			Columns: []storage.ColumnDef{
				{Name: "constraint_catalog", DataType: storage.TypeText, Ordinal: 0},
				{Name: "constraint_schema", DataType: storage.TypeText, Ordinal: 1},
				{Name: "constraint_name", DataType: storage.TypeText, Ordinal: 2},
				{Name: "table_catalog", DataType: storage.TypeText, Ordinal: 3},
				{Name: "table_schema", DataType: storage.TypeText, Ordinal: 4},
				{Name: "table_name", DataType: storage.TypeText, Ordinal: 5},
				{Name: "constraint_type", DataType: storage.TypeText, Ordinal: 6},
				{Name: "is_deferrable", DataType: storage.TypeText, Ordinal: 7},
				{Name: "initially_deferred", DataType: storage.TypeText, Ordinal: 8},
			},
		},
		rows: func(eng storage.Engine) []storage.Row {
			var rows []storage.Row
			var id int64
			if eng == nil {
				return rows
			}
			defs := eng.ListTables()
			sort.Slice(defs, func(i, j int) bool {
				return defs[i].Name < defs[j].Name
			})
			for _, def := range defs {
				// PRIMARY KEY constraint.
				for _, col := range def.Columns {
					if col.PrimaryKey {
						id++
						rows = append(rows, storage.Row{
							ID: id,
							Values: []any{
								"mulldb",
								"public",
								def.Name + "_pkey",
								"mulldb",
								"public",
								def.Name,
								"PRIMARY KEY",
								"NO",
								"NO",
							},
						})
						break
					}
				}
				// UNIQUE constraints from indexes.
				for _, idx := range def.Indexes {
					if idx.Unique {
						id++
						rows = append(rows, storage.Row{
							ID: id,
							Values: []any{
								"mulldb",
								"public",
								idx.Name,
								"mulldb",
								"public",
								def.Name,
								"UNIQUE",
								"NO",
								"NO",
							},
						})
					}
				}
			}
			return rows
		},
	}
}

// registerInformationSchemaKeyColumnUsage adds the
// information_schema.key_column_usage catalog table.
func registerInformationSchemaKeyColumnUsage() {
	catalogTables["information_schema.key_column_usage"] = &catalogTable{
		def: &storage.TableDef{
			Name:        "key_column_usage",
			NextOrdinal: 8,
			Columns: []storage.ColumnDef{
				{Name: "constraint_catalog", DataType: storage.TypeText, Ordinal: 0},
				{Name: "constraint_schema", DataType: storage.TypeText, Ordinal: 1},
				{Name: "constraint_name", DataType: storage.TypeText, Ordinal: 2},
				{Name: "table_catalog", DataType: storage.TypeText, Ordinal: 3},
				{Name: "table_schema", DataType: storage.TypeText, Ordinal: 4},
				{Name: "table_name", DataType: storage.TypeText, Ordinal: 5},
				{Name: "column_name", DataType: storage.TypeText, Ordinal: 6},
				{Name: "ordinal_position", DataType: storage.TypeInteger, Ordinal: 7},
			},
		},
		rows: func(eng storage.Engine) []storage.Row {
			var rows []storage.Row
			var id int64
			if eng == nil {
				return rows
			}
			defs := eng.ListTables()
			sort.Slice(defs, func(i, j int) bool {
				return defs[i].Name < defs[j].Name
			})
			for _, def := range defs {
				// PRIMARY KEY column.
				for _, col := range def.Columns {
					if col.PrimaryKey {
						id++
						rows = append(rows, storage.Row{
							ID: id,
							Values: []any{
								"mulldb",
								"public",
								def.Name + "_pkey",
								"mulldb",
								"public",
								def.Name,
								col.Name,
								int64(1),
							},
						})
						break
					}
				}
				// UNIQUE index columns.
				for _, idx := range def.Indexes {
					if idx.Unique {
						id++
						rows = append(rows, storage.Row{
							ID: id,
							Values: []any{
								"mulldb",
								"public",
								idx.Name,
								"mulldb",
								"public",
								def.Name,
								idx.Column,
								int64(1),
							},
						})
					}
				}
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
