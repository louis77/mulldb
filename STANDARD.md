# SQL Standard Conformance

Checklist of **Core SQL** mandatory features as defined by the SQL standard (SQL:1999 onward).
Every conforming SQL implementation must support all Core features.

Feature IDs and groupings follow the taxonomy in
[PostgreSQL Appendix D — SQL Conformance](https://www.postgresql.org/docs/current/features-sql-standard.html).

Legend: **Done** = implemented, **Partial** = partially implemented, **Open** = not yet implemented.

---

## E011 — Numeric data types

| ID | Feature | Status |
|----|---------|--------|
| E011-01 | INTEGER and SMALLINT data types | **Done** (INTEGER, INT, SMALLINT, BIGINT all accepted; stored as int64) |
| E011-02 | REAL, DOUBLE PRECISION, and FLOAT data types | **Done** (FLOAT and DOUBLE PRECISION accepted; stored as float64) |
| E011-03 | DECIMAL and NUMERIC data types | Open |
| E011-04 | Arithmetic operators | **Done** (`+`, `-`, `*`, `/`, `%` on integers and floats; unary minus; implicit int→float promotion; NULL propagation; division by zero → SQLSTATE 22012) |
| E011-05 | Numeric comparison | **Done** |
| E011-06 | Implicit casting among numeric data types | **Done** (implicit int64→float64 promotion in mixed arithmetic and comparisons) |

## E021 — Character string types

All character data is UTF-8. There is no encoding configuration, no `CHARACTER SET` clause, and no other character set. String comparison uses binary (byte-order) collation.

| ID | Feature | Status |
|----|---------|--------|
| E021-01 | CHARACTER data type (fixed-length) | Open |
| E021-02 | CHARACTER VARYING data type | **Done** (TEXT; UTF-8 encoded) |
| E021-03 | Character literals | **Done** (single-quoted strings; full UTF-8 support) |
| E021-04 | CHARACTER_LENGTH function | **Done** (`LENGTH()`, `CHARACTER_LENGTH()`, `CHAR_LENGTH()`; counts Unicode code points; NULL returns NULL) |
| E021-05 | OCTET_LENGTH function | **Done** (`OCTET_LENGTH()`; returns byte length of UTF-8 string; NULL returns NULL) |
| E021-06 | SUBSTRING function | Open |
| E021-07 | Character concatenation (`\|\|`) | **Done** (`\|\|` operator; implicit coercion from INTEGER/BOOLEAN; NULL propagation per SQL standard) |
| E021-08 | UPPER and LOWER functions | Open |
| E021-09 | TRIM function | Open |
| E021-10 | Implicit casting among character string types | Open (only one string type exists) |
| E021-11 | POSITION function | Open |
| E021-12 | Character comparison | **Done** (binary collation) |

## E031 — Identifiers

| ID | Feature | Status |
|----|---------|--------|
| E031-01 | Delimited identifiers | **Done** (double-quoted identifiers; full UTF-8 support) |
| E031-02 | Lower case identifiers | **Done** (bare identifiers are case-insensitive) |
| E031-03 | Trailing underscore | **Done** |

## E051 — Basic query specification

| ID | Feature | Status |
|----|---------|--------|
| E051-01 | SELECT DISTINCT | Open |
| E051-02 | GROUP BY clause | Open |
| E051-04 | GROUP BY can contain columns not in select list | Open |
| E051-05 | Select list items can be renamed (AS) | **Done** |
| E051-06 | HAVING clause | Open |
| E051-07 | Qualified `*` in select list (e.g. `t.*`) | Open |
| E051-08 | Correlation names in the FROM clause | **Done** (table aliases in FROM and JOIN clauses) |
| E051-09 | Rename columns in the FROM clause | Open |

## E061 — Basic predicates and search conditions

| ID | Feature | Status |
|----|---------|--------|
| E061-01 | Comparison predicate | **Done** (`=`, `!=`, `<>`, `<`, `>`, `<=`, `>=`) |
| E061-02 | BETWEEN predicate | Open |
| E061-03 | IN predicate with list of values | **Done** (`IN (v1, v2, ...)` and `NOT IN (v1, v2, ...)`; SQL-standard three-valued NULL logic) |
| E061-04 | LIKE predicate | **Done** (`LIKE`, `NOT LIKE`, plus PostgreSQL `ILIKE`/`NOT ILIKE` for case-insensitive matching) |
| E061-05 | LIKE predicate: ESCAPE clause | **Done** (`LIKE pattern ESCAPE char`; single-character escape) |
| E061-06 | NULL predicate (IS NULL) | **Done** (`IS NULL` and `IS NOT NULL`; comparisons with NULL yield NULL per SQL standard) |
| E061-07 | Quantified comparison predicate | Open |
| E061-08 | EXISTS predicate | Open |
| E061-09 | Subqueries in comparison predicate | Open |
| E061-11 | Subqueries in IN predicate | Open |
| E061-12 | Subqueries in quantified comparison predicate | Open |
| E061-13 | Correlated subqueries | Open |
| E061-14 | Search condition (AND, OR, NOT) | **Done** |

## E071 — Basic query expressions

| ID | Feature | Status |
|----|---------|--------|
| E071-01 | UNION DISTINCT table operator | Open |
| E071-02 | UNION ALL table operator | Open |
| E071-03 | EXCEPT DISTINCT table operator | Open |
| E071-05 | Columns combined via table operators need not have exactly the same data type | Open |
| E071-06 | Table operators in subqueries | Open |

## E081 — Basic privileges

| ID | Feature | Status |
|----|---------|--------|
| E081-01 | SELECT privilege | Open |
| E081-02 | DELETE privilege | Open |
| E081-03 | INSERT privilege at the table level | Open |
| E081-04 | UPDATE privilege at the table level | Open |
| E081-05 | UPDATE privilege at the column level | Open |
| E081-06 | REFERENCES privilege at the table level | Open |
| E081-07 | REFERENCES privilege at the column level | Open |
| E081-08 | WITH GRANT OPTION | Open |
| E081-09 | USAGE privilege | Open |
| E081-10 | EXECUTE privilege | Open |

## E091 — Set functions (aggregates)

| ID | Feature | Status |
|----|---------|--------|
| E091-01 | AVG | **Done** (always returns FLOAT, NULL for empty/all-NULL) |
| E091-02 | COUNT | **Done** (COUNT(*) and COUNT(col)) |
| E091-03 | MAX | **Done** |
| E091-04 | MIN | **Done** |
| E091-05 | SUM | **Done** |
| E091-06 | ALL quantifier | Open |
| E091-07 | DISTINCT quantifier | Open |

## E101 — Basic data manipulation

| ID | Feature | Status |
|----|---------|--------|
| E101-01 | INSERT statement | **Done** (single and multi-row) |
| E101-03 | Searched UPDATE statement | **Done** |
| E101-04 | Searched DELETE statement | **Done** |

## E111 — Single row SELECT statement

| ID | Feature | Status |
|----|---------|--------|
| E111 | Single row SELECT (SELECT without FROM) | **Done** (literals, VERSION()) |

## E121 — Basic cursor support

| ID | Feature | Status |
|----|---------|--------|
| E121-01 | DECLARE CURSOR | Open |
| E121-02 | ORDER BY columns need not be in select list | **Done** (ORDER BY references table columns, not select list) |
| E121-03 | Value expressions in ORDER BY clause | **Partial** (column names only; no expressions or ordinal positions) |
| E121-04 | OPEN statement | Open |
| E121-06 | Positioned UPDATE statement | Open |
| E121-07 | Positioned DELETE statement | Open |
| E121-08 | CLOSE statement | Open |
| E121-10 | FETCH statement: implicit NEXT | Open |
| E121-17 | WITH HOLD cursors | Open |

## E131 — Null value support

| ID | Feature | Status |
|----|---------|--------|
| E131 | Null value support (nulls in lieu of values) | **Done** (NULL storage, insertion, IS NULL / IS NOT NULL predicates, and standard NULL comparison semantics) |

## E141 — Basic integrity constraints

| ID | Feature | Status |
|----|---------|--------|
| E141-01 | NOT NULL constraints | **Partial** (implicit on PRIMARY KEY only; no standalone NOT NULL) |
| E141-02 | UNIQUE constraints of NOT NULL columns | **Partial** (via `CREATE UNIQUE INDEX`; no inline column constraint syntax yet) |
| E141-03 | PRIMARY KEY constraints | **Done** (single-column, B-tree indexed) |
| E141-04 | Basic FOREIGN KEY constraint with NO ACTION default | Open |
| E141-06 | CHECK constraints | Open |
| E141-07 | Column defaults | Open |
| E141-08 | NOT NULL inferred on PRIMARY KEY | **Done** |
| E141-10 | Names in a foreign key can be specified in any order | Open |

## E151 — Transaction support

| ID | Feature | Status |
|----|---------|--------|
| E151-01 | COMMIT statement | Open |
| E151-02 | ROLLBACK statement | Open |

## E152 — Basic SET TRANSACTION statement

| ID | Feature | Status |
|----|---------|--------|
| E152-01 | SET TRANSACTION: ISOLATION LEVEL SERIALIZABLE | Open |
| E152-02 | SET TRANSACTION: READ ONLY and READ WRITE | Open |

## E153 — Updatable queries with subqueries

| ID | Feature | Status |
|----|---------|--------|
| E153 | Updatable queries with subqueries | Open |

## E161 — SQL comments using leading double minus

| ID | Feature | Status |
|----|---------|--------|
| E161 | SQL comments (`--`) | **Done** (single-line `--` and nested block `/* */` comments) |

## E171 — SQLSTATE support

| ID | Feature | Status |
|----|---------|--------|
| E171 | SQLSTATE support | **Done** (proper codes: 42601, 42P01, 42P07, 42703, 23505, etc.) |

## E182 — Host language binding

| ID | Feature | Status |
|----|---------|--------|
| E182 | Host language binding | **Done** (PostgreSQL wire protocol v3; compatible with psql, pgx, node-postgres) |

## F021 — Basic information schema

| ID | Feature | Status |
|----|---------|--------|
| F021-01 | COLUMNS view | **Done** (information_schema.columns) |
| F021-02 | TABLES view | **Done** (information_schema.tables) |
| F021-03 | VIEWS view | Open |
| F021-04 | TABLE_CONSTRAINTS view | **Done** (information_schema.table_constraints; also key_column_usage) |
| F021-05 | REFERENTIAL_CONSTRAINTS view | Open |
| F021-06 | CHECK_CONSTRAINTS view | Open |

## F031 — Basic schema manipulation

| ID | Feature | Status |
|----|---------|--------|
| F031-01 | CREATE TABLE statement | **Done** |
| F031-02 | CREATE VIEW statement | Open |
| F031-03 | GRANT statement | Open |
| F031-04 | ALTER TABLE: ADD COLUMN clause | **Done** (ADD COLUMN and DROP COLUMN via ordinal-based storage) |
| F031-13 | DROP TABLE: RESTRICT clause | **Partial** (DROP TABLE works; no RESTRICT/CASCADE semantics) |
| F031-14 | CREATE INDEX statement | **Done** (single-column; both UNIQUE and non-unique; optional index names) |
| F031-15 | DROP INDEX statement | **Done** (`DROP INDEX name ON table`; table-scoped names) |
| F031-16 | DROP VIEW: RESTRICT clause | Open |
| F031-19 | REVOKE statement: RESTRICT clause | Open |

## F041 — Basic joined table

| ID | Feature | Status |
|----|---------|--------|
| F041-01 | Inner join (but not necessarily the INNER keyword) | **Done** (JOIN ... ON with nested-loop execution, table aliases, qualified column refs) |
| F041-02 | INNER keyword | **Done** (INNER JOIN accepted as alias for JOIN) |
| F041-03 | LEFT OUTER JOIN | Open |
| F041-04 | RIGHT OUTER JOIN | Open |
| F041-05 | Outer joins can be nested | Open |
| F041-07 | Inner table in left or right outer join can also be used in inner join | Open |
| F041-08 | All comparison operators are supported (in join conditions) | **Done** (all 6 comparison operators work in ON and WHERE for joins) |

## F051 — Basic date and time

| ID | Feature | Status |
|----|---------|--------|
| F051-01 | DATE data type | Open |
| F051-02 | TIME data type with fractional seconds precision | Open |
| F051-03 | TIMESTAMP data type with fractional seconds precision | **Done** (TIMESTAMP, TIMESTAMPTZ, TIMESTAMP WITH TIME ZONE; UTC-only; microsecond precision; stored as int64 µs since epoch) |
| F051-04 | Comparison predicate on DATE, TIME, and TIMESTAMP | **Partial** (TIMESTAMP comparisons work; DATE and TIME not implemented) |
| F051-05 | Explicit CAST between datetime types and character string types | **Partial** (implicit string→timestamp coercion on INSERT/UPDATE and in WHERE comparisons; `expr::TIMESTAMP` cast syntax supported; no SQL-standard `CAST()` syntax) |
| F051-06 | CURRENT_DATE | Open |
| F051-07 | LOCALTIME | Open |
| F051-08 | LOCALTIMESTAMP | Open |

## F081 — UNION and EXCEPT in views

| ID | Feature | Status |
|----|---------|--------|
| F081 | UNION and EXCEPT in views | Open |

## F131 — Grouped operations

| ID | Feature | Status |
|----|---------|--------|
| F131-01 | WHERE, GROUP BY, and HAVING in grouped views | Open |
| F131-02 | Multiple tables in grouped views | Open |
| F131-03 | Set functions in grouped views | Open |
| F131-04 | Subqueries with GROUP BY and HAVING in grouped views | Open |
| F131-05 | Single row SELECT with GROUP BY and HAVING in grouped views | Open |

## F181 — Multiple module support

| ID | Feature | Status |
|----|---------|--------|
| F181 | Multiple module support | Open |

## F201 — CAST function

| ID | Feature | Status |
|----|---------|--------|
| F201 | CAST function | **Partial** (PostgreSQL-style `expr::type` syntax; supports INTEGER, TEXT, BOOLEAN, FLOAT, TIMESTAMP targets; no SQL-standard `CAST(expr AS type)` syntax yet) |

## F221 — Explicit defaults

| ID | Feature | Status |
|----|---------|--------|
| F221 | Explicit defaults | Open |

## F261 — CASE expression

| ID | Feature | Status |
|----|---------|--------|
| F261-01 | Simple CASE | Open |
| F261-02 | Searched CASE | Open |
| F261-03 | NULLIF | Open |
| F261-04 | COALESCE | Open |

## F311 — Schema definition statement

| ID | Feature | Status |
|----|---------|--------|
| F311-01 | CREATE SCHEMA | Open |
| F311-02 | CREATE TABLE for persistent base tables | **Done** (WAL-backed persistence) |
| F311-03 | CREATE VIEW | Open |
| F311-04 | CREATE VIEW: WITH CHECK OPTION | Open |
| F311-05 | GRANT statement | Open |

## F471 — Scalar subquery values

| ID | Feature | Status |
|----|---------|--------|
| F471 | Scalar subquery values | Open |

## F481 — Expanded NULL predicate

| ID | Feature | Status |
|----|---------|--------|
| F481 | Expanded NULL predicate (IS NOT NULL) | **Done** |

## F501 — Features and conformance views

| ID | Feature | Status |
|----|---------|--------|
| F501-01 | SQL_FEATURES view | Open |
| F501-02 | SQL_SIZING view | Open |

## T321 — Basic SQL-invoked routines

| ID | Feature | Status |
|----|---------|--------|
| T321-01 | User-defined functions with no overloading | Open |
| T321-02 | User-defined stored procedures with no overloading | Open |
| T321-03 | Function invocation | **Partial** (scalar function registry exists; VERSION() implemented) |
| T321-04 | CALL statement | Open |
| T321-05 | RETURN statement | Open |
| T321-06 | ROUTINES view | Open |
| T321-07 | PARAMETERS view | Open |

## T631 — IN predicate with one list element

| ID | Feature | Status |
|----|---------|--------|
| T631 | IN predicate with one list element | **Done** (`x IN (42)` works; single-element list) |

---

## Summary

| Status | Count |
|--------|-------|
| **Done** | ~49 |
| **Partial** | ~9 |
| **Open** | ~120 |

### Strongest areas
- Basic CRUD (CREATE TABLE, INSERT, SELECT, UPDATE, DELETE)
- Primary key constraints with B-tree index
- Secondary indexes (CREATE INDEX, DROP INDEX, query acceleration)
- Identifiers (delimited and case-insensitive)
- Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- ORDER BY (single/multi-column, ASC/DESC, NULLs last)
- INNER JOIN (with table aliases, qualified column references, nested-loop execution)
- Information schema (TABLES, COLUMNS views)
- SQLSTATE error codes
- Wire protocol compatibility (host language binding)

### Biggest gaps to close
1. **Predicates**: BETWEEN, IN
2. **Expressions**: CASE, COALESCE (arithmetic and `::` cast are done; SQL-standard `CAST(expr AS type)` not yet)
3. **GROUP BY / HAVING**: Aggregates currently only work across whole tables
4. **JOINs**: INNER JOIN supported; LEFT/RIGHT/FULL OUTER JOINs not yet
5. **Transactions**: No BEGIN / COMMIT / ROLLBACK
6. **Data types**: No decimal, DATE, or TIME types (TIMESTAMP and FLOAT are done)
7. **Constraints**: UNIQUE via CREATE UNIQUE INDEX; no FOREIGN KEY, CHECK, DEFAULT
8. **Subqueries**: No subquery support anywhere
9. **UNION / EXCEPT**: No set operations
