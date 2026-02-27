package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"

	"mulldb/config"
	"mulldb/executor"
	"mulldb/server"
	"mulldb/storage"
)

func main() {
	fmt.Println("mulldb concurrency test")
	fmt.Println("=======================")

	port, shutdown := startServer()
	defer shutdown()

	fmt.Printf("Starting server on port %d...\n\n", port)

	passed, failed := 0, 0
	for _, sc := range []struct {
		name string
		fn   func(int) bool
	}{
		{"Setup", scenarioSetup},
		{"Concurrent reads", scenarioConcurrentReads},
		{"Reads during writes", scenarioReadsDuringWrites},
		{"Concurrent writes", scenarioConcurrentWrites},
	} {
		if sc.fn(port) {
			passed++
		} else {
			failed++
		}
	}

	fmt.Printf("\n%d passed, %d failed\n", passed, failed)
	if failed > 0 {
		os.Exit(1)
	}
}

func startServer() (port int, shutdown func()) {
	tmpDir, err := os.MkdirTemp("", "conctest-*")
	if err != nil {
		fatalf("create temp dir: %v", err)
	}

	eng, err := storage.Open(tmpDir, false)
	if err != nil {
		os.RemoveAll(tmpDir)
		fatalf("open storage: %v", err)
	}

	cfg := &config.Config{
		Port:     0, // OS-assigned
		DataDir:  tmpDir,
		User:     "admin",
		Password: "test",
	}

	exec := executor.New(eng)
	srv := server.New(cfg, exec)

	go func() {
		if err := srv.ListenAndServe(); err != nil {
			fatalf("server: %v", err)
		}
	}()

	// Wait for the listener to be ready.
	for i := 0; i < 100; i++ {
		if addr := srv.Addr(); addr != nil {
			port = addr.(*net.TCPAddr).Port
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if port == 0 {
		eng.Close()
		os.RemoveAll(tmpDir)
		fatalf("server did not start within 1s")
	}

	shutdown = func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
		eng.Close()
		os.RemoveAll(tmpDir)
	}
	return port, shutdown
}

func connect(port int) *pgx.Conn {
	connStr := fmt.Sprintf("host=127.0.0.1 port=%d user=admin password=test sslmode=disable", port)
	cfg, err := pgx.ParseConfig(connStr)
	if err != nil {
		fatalf("parse config: %v", err)
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	conn, err := pgx.ConnectConfig(context.Background(), cfg)
	if err != nil {
		fatalf("connect: %v", err)
	}
	return conn
}

func scenarioSetup(port int) bool {
	start := time.Now()
	conn := connect(port)
	defer conn.Close(context.Background())

	_, err := conn.Exec(context.Background(),
		"CREATE TABLE conc (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		return fail("Setup", "CREATE TABLE: %v", err)
	}

	for i := 1; i <= 100; i++ {
		_, err := conn.Exec(context.Background(),
			fmt.Sprintf("INSERT INTO conc VALUES (%d, 'row%d')", i, i))
		if err != nil {
			return fail("Setup", "INSERT %d: %v", i, err)
		}
	}

	var count int64
	err = conn.QueryRow(context.Background(), "SELECT COUNT(*) FROM conc").Scan(&count)
	if err != nil {
		return fail("Setup", "COUNT: %v", err)
	}
	if count != 100 {
		return fail("Setup", "expected 100 rows, got %d", count)
	}

	return pass("Setup", "created table, inserted 100 rows", time.Since(start))
}

func scenarioConcurrentReads(port int) bool {
	start := time.Now()
	const goroutines = 10
	const queriesPerGoroutine = 50

	var wg sync.WaitGroup
	var errCount atomic.Int64

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn := connect(port)
			defer conn.Close(context.Background())

			for q := 0; q < queriesPerGoroutine; q++ {
				rows, err := conn.Query(context.Background(), "SELECT * FROM conc")
				if err != nil {
					errCount.Add(1)
					continue
				}
				n := 0
				for rows.Next() {
					n++
				}
				rows.Close()
				if rows.Err() != nil || n != 100 {
					errCount.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	errs := errCount.Load()
	total := goroutines * queriesPerGoroutine
	if errs > 0 {
		return fail("Concurrent reads", "%d errors out of %d queries", errs, total)
	}
	return pass("Concurrent reads",
		fmt.Sprintf("%d goroutines × %d queries = %d total, 0 errors", goroutines, queriesPerGoroutine, total),
		time.Since(start))
}

func scenarioReadsDuringWrites(port int) bool {
	start := time.Now()
	const readers = 10

	var wg sync.WaitGroup
	var errCount atomic.Int64
	var minCount, maxCount atomic.Int64
	minCount.Store(999999)

	// Writer goroutine: insert rows 101-200.
	wg.Add(1)
	go func() {
		defer wg.Done()
		conn := connect(port)
		defer conn.Close(context.Background())

		for i := 101; i <= 200; i++ {
			_, err := conn.Exec(context.Background(),
				fmt.Sprintf("INSERT INTO conc VALUES (%d, 'row%d')", i, i))
			if err != nil {
				errCount.Add(1)
			}
		}
	}()

	// Reader goroutines: repeatedly COUNT(*) while writes happen.
	for g := 0; g < readers; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn := connect(port)
			defer conn.Close(context.Background())

			for q := 0; q < 50; q++ {
				var count int64
				err := conn.QueryRow(context.Background(), "SELECT COUNT(*) FROM conc").Scan(&count)
				if err != nil {
					errCount.Add(1)
					continue
				}
				// Update min/max atomically.
				for {
					cur := minCount.Load()
					if count >= cur || minCount.CompareAndSwap(cur, count) {
						break
					}
				}
				for {
					cur := maxCount.Load()
					if count <= cur || maxCount.CompareAndSwap(cur, count) {
						break
					}
				}
			}
		}()
	}
	wg.Wait()

	errs := errCount.Load()
	lo, hi := minCount.Load(), maxCount.Load()

	if errs > 0 {
		return fail("Reads during writes", "%d errors", errs)
	}
	if lo < 100 || hi > 200 {
		return fail("Reads during writes", "counts out of range: [%d..%d]", lo, hi)
	}

	// Verify final count.
	conn := connect(port)
	defer conn.Close(context.Background())
	var finalCount int64
	conn.QueryRow(context.Background(), "SELECT COUNT(*) FROM conc").Scan(&finalCount)
	if finalCount != 200 {
		return fail("Reads during writes", "final count %d, expected 200", finalCount)
	}

	return pass("Reads during writes",
		fmt.Sprintf("100 rows inserted while reading, counts in [%d..%d], 0 errors", lo, hi),
		time.Since(start))
}

func scenarioConcurrentWrites(port int) bool {
	start := time.Now()
	const goroutines = 10
	const rowsPerGoroutine = 10

	var wg sync.WaitGroup
	var errCount atomic.Int64

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			conn := connect(port)
			defer conn.Close(context.Background())

			base := 201 + g*rowsPerGoroutine
			for i := 0; i < rowsPerGoroutine; i++ {
				id := base + i
				_, err := conn.Exec(context.Background(),
					fmt.Sprintf("INSERT INTO conc VALUES (%d, 'row%d')", id, id))
				if err != nil {
					errCount.Add(1)
				}
			}
		}(g)
	}
	wg.Wait()

	errs := errCount.Load()
	if errs > 0 {
		return fail("Concurrent writes", "%d insert errors", errs)
	}

	conn := connect(port)
	defer conn.Close(context.Background())
	var count int64
	conn.QueryRow(context.Background(), "SELECT COUNT(*) FROM conc").Scan(&count)
	if count != 300 {
		return fail("Concurrent writes", "final count %d, expected 300", count)
	}

	return pass("Concurrent writes",
		fmt.Sprintf("%d goroutines × %d rows = %d inserts, final count %d",
			goroutines, rowsPerGoroutine, goroutines*rowsPerGoroutine, count),
		time.Since(start))
}

func pass(name, detail string, d time.Duration) bool {
	fmt.Printf("[PASS] %s: %s (%dms)\n", name, detail, d.Milliseconds())
	return true
}

func fail(name, format string, args ...any) bool {
	fmt.Printf("[FAIL] %s: %s\n", name, fmt.Sprintf(format, args...))
	return false
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "fatal: "+format+"\n", args...)
	os.Exit(2)
}
