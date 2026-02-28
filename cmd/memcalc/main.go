// cmd/memcalc calculates the estimated memory consumption of a realistic
// ~1 GB web shop database stored in mulldb's in-memory engine.
//
// It models the Go memory layout of both the old map[int64][]any storage
// and the current dense [][]any array storage, showing the per-row savings
// from eliminating map bucket overhead.
//
// Usage: go run cmd/memcalc/main.go
package main

import "fmt"

// ---------------------------------------------------------------------------
// Go memory layout constants (64-bit)
// ---------------------------------------------------------------------------

const (
	// Map internals: each bucket holds 8 entries.
	// Per entry: tophash(1) + key(8) + value-ptr(8) = 17 bytes.
	// Bucket struct overhead (overflow ptr, padding) amortised ≈ 72 bytes/entry.
	mapEntryOverhead = 72

	// Dense array: each slot is a pointer to []any (8 bytes in the outer slice).
	denseSlotOverhead = 8

	// []any slice header: ptr(8) + len(8) + cap(8).
	sliceHeader = 24

	// any (interface{}) box: type-ptr(8) + data-ptr(8).
	ifaceBox = 16

	// int64 stored in an any: the interface box itself is 16 bytes.
	// Small ints (<256) may be statically allocated; we conservatively
	// assume all are heap-allocated, adding 8 bytes for the backing word.
	int64Overhead = 8

	// string header inside the any data pointer: ptr(8) + len(8).
	stringHeader = 16

	// time.Time struct: wall(8) + ext(8) + loc-ptr(8) = 24 bytes.
	timeTimeSize = 24

	// B-tree entry (btreeEntry): key(16 any) + rowID(8) = 24 bytes.
	btreeEntry = 24

	// Amortised B-tree node overhead per entry.
	// Node has: keys slice(24) + children slice(24) + isLeaf(1) + padding,
	// spread over the default order's capacity (~63 entries).
	btreeNodeOverhead = 10

	// Non-unique (multi) secondary index wraps key in multiKey{key, rowID}
	// which is another struct boxed through the any interface.
	multiKeyExtra = 24
)

// ---------------------------------------------------------------------------
// Schema modelling
// ---------------------------------------------------------------------------

type colType int

const (
	colInt colType = iota
	colText
	colTimestamp
)

type column struct {
	name    string
	typ     colType
	avgSize int // average byte length for TEXT columns
}

type indexDef struct {
	name   string
	unique bool
}

type table struct {
	name    string
	columns []column
	rows    int
	indexes []indexDef // first is always the PK
}

// rawRowSize returns the sum of actual value sizes for one row (no Go overhead).
func rawRowSize(cols []column) int {
	size := 0
	for _, c := range cols {
		switch c.typ {
		case colInt:
			size += 8
		case colText:
			size += c.avgSize
		case colTimestamp:
			size += 8 // raw: 8-byte Unix micros
		}
	}
	return size
}

// goRowOverheadMap returns the per-row Go memory overhead for the old
// map-based storage (map entry, slice header, interface boxes).
func goRowOverheadMap(cols []column) int {
	overhead := mapEntryOverhead + sliceHeader
	for _, c := range cols {
		overhead += ifaceBox // every column is boxed as any
		switch c.typ {
		case colInt:
			overhead += int64Overhead
		case colText:
			overhead += stringHeader // + avgSize counted in raw
		case colTimestamp:
			overhead += timeTimeSize // full struct, not just 8 bytes
		}
	}
	return overhead
}

// goRowOverheadDense returns the per-row Go memory overhead for the
// dense array storage (slot pointer, slice header, interface boxes).
func goRowOverheadDense(cols []column) int {
	overhead := denseSlotOverhead + sliceHeader
	for _, c := range cols {
		overhead += ifaceBox
		switch c.typ {
		case colInt:
			overhead += int64Overhead
		case colText:
			overhead += stringHeader
		case colTimestamp:
			overhead += timeTimeSize
		}
	}
	return overhead
}

// indexEntrySize returns the per-entry memory cost of one index.
func indexEntrySize(unique bool) int {
	size := btreeEntry + btreeNodeOverhead
	if !unique {
		size += multiKeyExtra
	}
	return size
}

// ---------------------------------------------------------------------------
// Web shop schema
// ---------------------------------------------------------------------------

func webShopSchema() []table {
	return []table{
		{
			name: "users",
			columns: []column{
				{"id", colInt, 0},
				{"email", colText, 30},
				{"name", colText, 20},
				{"password_hash", colText, 60},
				{"created_at", colTimestamp, 0},
			},
			rows: 100_000,
			indexes: []indexDef{
				{"pk_id", true},
				{"uq_email", true},
			},
		},
		{
			name: "categories",
			columns: []column{
				{"id", colInt, 0},
				{"name", colText, 20},
				{"parent_id", colInt, 0},
			},
			rows: 500,
			indexes: []indexDef{
				{"pk_id", true},
			},
		},
		{
			name: "products",
			columns: []column{
				{"id", colInt, 0},
				{"name", colText, 40},
				{"description", colText, 200},
				{"price_cents", colInt, 0},
				{"category_id", colInt, 0},
				{"created_at", colTimestamp, 0},
			},
			rows: 50_000,
			indexes: []indexDef{
				{"pk_id", true},
				{"idx_category_id", false},
			},
		},
		{
			name: "orders",
			columns: []column{
				{"id", colInt, 0},
				{"user_id", colInt, 0},
				{"status", colText, 10},
				{"total_cents", colInt, 0},
				{"created_at", colTimestamp, 0},
			},
			rows: 500_000,
			indexes: []indexDef{
				{"pk_id", true},
				{"idx_user_id", false},
			},
		},
		{
			name: "order_items",
			columns: []column{
				{"id", colInt, 0},
				{"order_id", colInt, 0},
				{"product_id", colInt, 0},
				{"quantity", colInt, 0},
				{"unit_price", colInt, 0},
			},
			rows: 2_000_000,
			indexes: []indexDef{
				{"pk_id", true},
				{"idx_order_id", false},
			},
		},
		{
			name: "reviews",
			columns: []column{
				{"id", colInt, 0},
				{"user_id", colInt, 0},
				{"product_id", colInt, 0},
				{"rating", colInt, 0},
				{"comment", colText, 100},
				{"created_at", colTimestamp, 0},
			},
			rows: 200_000,
			indexes: []indexDef{
				{"pk_id", true},
				{"idx_product_id", false},
			},
		},
	}
}

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------

func fmtBytes(b int) string {
	const (
		kb = 1024
		mb = 1024 * kb
		gb = 1024 * mb
	)
	switch {
	case b >= gb:
		return fmt.Sprintf("%.2f GB", float64(b)/float64(gb))
	case b >= mb:
		return fmt.Sprintf("%.2f MB", float64(b)/float64(mb))
	case b >= kb:
		return fmt.Sprintf("%.2f KB", float64(b)/float64(kb))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

func fmtRows(n int) string {
	switch {
	case n >= 1_000_000:
		return fmt.Sprintf("%.1fM", float64(n)/1_000_000)
	case n >= 1_000:
		return fmt.Sprintf("%.0fK", float64(n)/1_000)
	default:
		return fmt.Sprintf("%d", n)
	}
}

// ---------------------------------------------------------------------------
// Print helpers
// ---------------------------------------------------------------------------

func printTable(schema []table, label string, overheadFn func([]column) int) (grandRaw, grandOverhead, grandIndex, grandTotal int) {
	fmt.Printf("%s\n", label)
	fmt.Println(repeat('=', len(label)))
	fmt.Println()

	fmt.Printf("%-14s %8s %10s %10s %10s %10s  %s\n",
		"Table", "Rows", "Raw Data", "Go Ovhd", "Indexes", "Total", "Ratio")
	fmt.Println("-------------- -------- ---------- ---------- ---------- ----------  -----")

	for _, t := range schema {
		raw := rawRowSize(t.columns) * t.rows
		overhead := overheadFn(t.columns) * t.rows
		idxCost := 0
		for _, idx := range t.indexes {
			idxCost += indexEntrySize(idx.unique) * t.rows
		}

		total := raw + overhead + idxCost
		ratio := float64(total) / float64(raw)

		grandRaw += raw
		grandOverhead += overhead
		grandIndex += idxCost
		grandTotal += total

		fmt.Printf("%-14s %8s %10s %10s %10s %10s  %.2fx\n",
			t.name, fmtRows(t.rows),
			fmtBytes(raw), fmtBytes(overhead), fmtBytes(idxCost),
			fmtBytes(total), ratio)
	}

	fmt.Println("-------------- -------- ---------- ---------- ---------- ----------  -----")
	grandRatio := float64(grandTotal) / float64(grandRaw)
	fmt.Printf("%-14s %8s %10s %10s %10s %10s  %.2fx\n",
		"TOTAL", "",
		fmtBytes(grandRaw), fmtBytes(grandOverhead), fmtBytes(grandIndex),
		fmtBytes(grandTotal), grandRatio)
	fmt.Println()

	return grandRaw, grandOverhead, grandIndex, grandTotal
}

func repeat(ch byte, n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = ch
	}
	return string(b)
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

func main() {
	schema := webShopSchema()

	fmt.Println("mulldb Memory Calculator — Web Shop (~1 GB raw data)")
	fmt.Println("=====================================================")
	fmt.Println()

	// Old map-based storage.
	mapRaw, mapOverhead, mapIndex, mapTotal := printTable(
		schema, "Map-Based Storage (old)", goRowOverheadMap)

	// New dense array storage.
	denseRaw, denseOverhead, denseIndex, denseTotal := printTable(
		schema, "Dense Array Storage (current)", goRowOverheadDense)

	// Comparison summary.
	fmt.Println("Comparison")
	fmt.Println("----------")
	fmt.Printf("  %-28s %10s   %10s\n", "", "Map", "Dense Array")
	fmt.Printf("  %-28s %10s   %10s\n", "Raw data:", fmtBytes(mapRaw), fmtBytes(denseRaw))
	fmt.Printf("  %-28s %10s   %10s\n", "Go overhead:", fmtBytes(mapOverhead), fmtBytes(denseOverhead))
	fmt.Printf("  %-28s %10s   %10s\n", "Index overhead:", fmtBytes(mapIndex), fmtBytes(denseIndex))
	fmt.Printf("  %-28s %10s   %10s\n", "Total memory:", fmtBytes(mapTotal), fmtBytes(denseTotal))
	fmt.Printf("  %-28s %9.2fx   %9.2fx\n", "Overhead ratio:",
		float64(mapTotal)/float64(mapRaw),
		float64(denseTotal)/float64(denseRaw))
	fmt.Printf("  %-28s %10s\n", "Savings:", fmtBytes(mapTotal-denseTotal))
	fmt.Printf("  %-28s %d bytes/row\n", "Per-row savings:", mapEntryOverhead-denseSlotOverhead)

	fmt.Println()
	fmt.Println("Assumptions")
	fmt.Println("-----------")
	fmt.Println("  - 64-bit platform, Go 1.22+ map implementation")
	fmt.Println("  - Map entry overhead ~72 bytes (amortised bucket cost)")
	fmt.Println("  - Dense array slot overhead ~8 bytes (pointer in outer slice)")
	fmt.Println("  - All int64 values heap-allocated (conservative; small ints may be inlined)")
	fmt.Println("  - String backing arrays exactly avgSize bytes (no allocator rounding)")
	fmt.Println("  - B-tree order=32 (63 max keys/node), ~10 bytes amortised node overhead")
	fmt.Println("  - No GC metadata, goroutine stacks, or runtime overhead included")
}
