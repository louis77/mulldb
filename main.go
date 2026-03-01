package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"mulldb/config"
	"mulldb/executor"
	"mulldb/server"
	"mulldb/storage"
)

func main() {
	cfg := config.Parse()

	eng, err := storage.Open(cfg.DataDir, cfg.Migrate)
	if err != nil {
		log.Fatalf("open storage: %v", err)
	}
	defer eng.Close()

	eng.SetFsync(cfg.Fsync)

	exec := executor.New(eng)
	srv := server.New(cfg, exec)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		log.Printf("received %v, shutting down...", sig)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			log.Printf("shutdown: %v", err)
		}
	}()

	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
